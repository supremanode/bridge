/**
 * Suprema Bridge Adapter — the thin client that connects the relay to coding
 * harnesses. Runs on the machine where the agents live.
 *
 * Loop: poll /v1/pending-question → claim → execute via harness → POST /v1/answer
 *
 * This is the open, thin bridge (PRD-v3.0 open-core): PTY + transport + auth,
 * no intelligence. The relay (closed, fat server) owns routing/budget/orchestration.
 *
 * Usage:
 *   RELAY_URL=http://localhost:4000 BRIDGE_ID=my-laptop pnpm --filter @suprema/bridge dev
 */
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { startStatusServer, getStatus, getApiInfo } from "./statusServer.js";
import { detectHarnesses } from "./harnessDetect.js";
import { discoverSessions } from "./sessionDiscovery.js";
import { sendToAgent } from "./interactiveSession.js";
import { sendViaBroker } from "./brokerClient.js";

const VERSION = "0.0.1";

// Re-mark the task running every HEARTBEAT_MS while it's being processed so the
// relay's auto-timeout timer keeps resetting — a long-but-active agent turn is
// never killed. The bridge's idle-based waitForReply still bounds a truly-stuck
// agent, so the heartbeat only protects ACTIVE work. Keep < relay TASK_TIMEOUT_MS.
const HEARTBEAT_MS = Number(process.env.BRIDGE_HEARTBEAT_MS ?? 25_000);
// Must equal INTERRUPT_SENTINEL in extension/suprema-direct/index.js — the exact
// text the extension maps to ctx.abort() (ESC-in-pi). ponytail: duplicated const,
// two files, no shared package for one string.
const INTERRUPT_SENTINEL = "__SUPREMA_DIRECT_INTERRUPT__";

// A4.5: minimal CLI flags before starting the loop
const arg = process.argv.slice(2);
if (arg.includes("--version") || arg.includes("-v")) { console.log(`@suprema/bridge ${VERSION}`); process.exit(0); }
if (arg.includes("--help") || arg.includes("-h")) {
  console.log(`@suprema/bridge ${VERSION} — the thin client connecting the relay to coding harnesses.

Usage:
  bridge --relay https://relay.supremanode.com --label "My Machine"
  # or via env vars:
  RELAY_URL=https://relay.supremanode.com bridge

  # v3.15: self-serve API key (generate in Settings → Security)
  BRIDGE_KEY=skb_xxx bridge --relay https://relay.supremanode.com

Flags (override env vars):
  --relay <url>      relay endpoint (env RELAY_URL)
  --label <name>     human-readable label (env BRIDGE_LABEL)
  --id <id>          unique bridge id (env BRIDGE_ID)
  --port <port>      status server port, 0=disable (env BRIDGE_PORT)
  --harness <id>     restrict to one harness (env HARNESS)
  --key <key>        bridge API key from Settings (env BRIDGE_KEY)
  -v, --version      print version
  -h, --help         this help`);
  process.exit(0);
}

// Parse --flag value / --flag=value pairs. Overrides env vars.
// ponytail: hand-rolled, no dep. Flags win over env so CLI feels natural.
function parseFlag(name: string): string | undefined {
  for (let i = 0; i < arg.length; i++) {
    const a = arg[i];
    if (a === `--${name}` && i + 1 < arg.length) return arg[i + 1];
    const eq = `--${name}=`;
    if (a.startsWith(eq)) return a.slice(eq.length);
  }
  return undefined;
}
const flagRelay = parseFlag("relay");
const flagLabel = parseFlag("label");
const flagId = parseFlag("id");
const flagPort = parseFlag("port");
const flagHarness = parseFlag("harness");
const flagKey = parseFlag("key");

// Bridge API key: v3.15 self-serve flow (generate in Settings → Security).
// Takes precedence over the legacy BRIDGE_USER_TOKEN env var.
if (flagKey) process.env.BRIDGE_KEY = flagKey;

const run = promisify(execFile);

const RELAY_URL = flagRelay || process.env.RELAY_URL || "http://localhost:4000";
const BRIDGE_ID = flagId || process.env.BRIDGE_ID || `bridge-${randomUUID().slice(0, 8)}`;
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 2000);
// v3.30: long-poll hold (seconds). The relay holds the request until a task
// exists (max 30s server-side). Legacy 2s polling = set 0.
const POLL_WAIT_S = Number(process.env.BRIDGE_POLL_WAIT_S ?? 25);
const HARNESS_FILTER = flagHarness || process.env.HARNESS || undefined; // restrict to one harness

// Capability token issued by the relay on bridge registration. Sent as
// `Authorization: cap <token>` on poll/claim/answer so the relay can verify
// this bridge is authorized to claim the user's tasks (SECURITY-AUDIT.md
// bridge adapter fix). Cached in-memory; re-fetched if the relay 401s.
let capToken: string | null = null;
let capLabel = flagLabel || process.env.BRIDGE_LABEL || BRIDGE_ID;
let detectedHarnessNames: string[] = []; // set in main(), sent on every poll
let lastSessionCount = 0; // updated by the session-push loop; sent in the lightweight poll header

// ─── harness executors ─────────────────────────────────────────────────────

interface HarnessResult { answer: string; error?: string; usage?: { inputTokens: number; outputTokens: number }; filesChanged?: Array<{ path: string; additions: number; deletions: number; diff: string }> }

/** Execute a one-shot prompt via a coding harness CLI (deep integration). */
async function execOneshot(harness: string, prompt: string, cwd?: string, model?: string): Promise<HarnessResult> {
  const args = harnessArgs(harness, prompt, model);
  if (!args) return { answer: "", error: `unsupported harness: ${harness}` };
  try {
    const { stdout } = await run(args.bin, args.args, {
      cwd: cwd || process.cwd(),
      timeout: 90_000,
      maxBuffer: 10 * 1024 * 1024,
      env: { ...process.env, TERM: "dumb" },
    });
    // claude-code --output-format stream-json emits NDJSON; the final `result`
    // line carries usage. Extract both the text answer and token counts.
    const { answer, usage } = parseStreamJson(stdout);
    return { answer: (answer || stdout).trim(), usage };
  } catch (e: any) {
    const out = (e.stdout || "").trim();
    if (out) {
      const { answer, usage } = parseStreamJson(out);
      return { answer: (answer || out).trim(), error: e.stderr?.slice(0, 200), usage };
    }
    return { answer: "", error: e.message?.slice(0, 300) || "harness execution failed" };
  }
}

/** Parse claude-code stream-json NDJSON → { answer, usage }. */
function parseStreamJson(stdout: string): { answer: string; usage?: { inputTokens: number; outputTokens: number } } {
  const lines = stdout.split("\n").filter((l) => l.trim().startsWith("{"));
  let answer = ""; let usage: { inputTokens: number; outputTokens: number } | undefined;
  for (const line of lines) {
    try {
      const msg = JSON.parse(line);
      // result message: { type:"result", result:"...", usage:{...} }
      if (msg.type === "result" && msg.usage) {
        usage = {
          inputTokens: (msg.usage.input_tokens ?? 0) + (msg.usage.cache_creation_input_tokens ?? 0),
          outputTokens: msg.usage.output_tokens ?? 0,
        };
        if (typeof msg.result === "string") answer = msg.result;
      }
      // assistant text deltas (fallback when no result line)
      if (msg.type === "assistant" && Array.isArray(msg.message?.content)) {
        for (const b of msg.message.content) { if (b.type === "text" && typeof b.text === "string") answer += b.text; }
      }
    } catch { /* not json, skip */ }
  }
  return { answer, usage };
}

/** Execute via live PTY stream (PTY integration — captures interactive output). */
async function execPty(harness: string, prompt: string, cwd?: string): Promise<HarnessResult> {
  return new Promise((resolve) => {
    const bin = harnessBin(harness);
    if (!bin) return resolve({ answer: "", error: `harness not found: ${harness}` });
    const child = spawn(bin, [], {
      cwd: cwd || process.cwd(),
      env: { ...process.env, TERM: "dumb", FORCE_COLOR: "0" },
      stdio: ["pipe", "pipe", "pipe"],
    });
    let output = "";
    const timer = setTimeout(() => {
      child.kill("SIGTERM");
      resolve({ answer: output, error: "timeout (90s)" });
    }, 90_000);
    child.stdout.on("data", (d) => (output += d.toString()));
    child.stderr.on("data", (d) => (output += d.toString()));
    child.on("close", () => {
      clearTimeout(timer);
      resolve({ answer: output.trim() });
    });
    // send the prompt + exit
    child.stdin.write(`${prompt}\n`);
    setTimeout(() => child.stdin.write("/exit\n"), 80_000);
    child.stdin.end();
  });
}

/** Map harness id → binary + args for one-shot execution. */
function harnessArgs(harness: string, prompt: string, model?: string): { bin: string; args: string[] } | null {
  const modelArgs = model ? ["--model", model] : [];
  switch (harness) {
    case "claude-code":
      return { bin: "claude", args: ["--print", "--output-format", "stream-json", ...modelArgs, prompt] };
    case "codex":
      return { bin: "codex", args: ["--quiet", ...modelArgs, prompt] };
    case "opencode":
      return { bin: "opencode", args: ["--print", prompt] };
    case "pi":
      return { bin: "pi", args: ["--print", prompt] };
    case "aider":
      return { bin: "aider", args: ["--message", prompt, "--no-auto-commits"] };
    default:
      return null;
  }
}

function harnessBin(harness: string): string | null {
  const map: Record<string, string> = {
    "kilo": "kilo", "gemini": "gemini", "goose": "goose",
    "claude-code": "claude", "codex": "codex",
  };
  return map[harness] ?? null;
}

// ─── relay API helpers ────────────────────────────────────────────────────

/** Register with the relay and cache the capability token. Called on boot.
 * If BRIDGE_KEY (v3.15: self-serve API key from Settings) or BRIDGE_USER_TOKEN
 * (legacy: operator pre-issued JWT) is set, register and get a cap token.
 * Idempotent — safe to call every boot. */
async function registerBridge(): Promise<void> {
  // v3.15: prefer the self-serve bridge API key (long-lived, revocable, scoped).
  const bridgeKey = process.env.BRIDGE_KEY;
  const legacyToken = process.env.BRIDGE_USER_TOKEN;
  const authToken = bridgeKey || legacyToken;
  if (!authToken) return; // soft mode — no registration, relay accepts anon bridges for now
  try {
    const r = await fetch(`${RELAY_URL}/api/bridges`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${authToken}` },
      body: JSON.stringify({ label: capLabel, url: process.env.BRIDGE_URL || `http://localhost:${process.env.BRIDGE_PORT ?? 9764}` }),
    });
    if (r.ok) { const d = await r.json(); if (d.capToken) capToken = d.capToken; if (d.id) { (globalThis as any).__RELAY_BRIDGE_ID = d.id; } }
    // Fetch session scan paths from relay (Settings UI writes these per user).
    // The bridge merges them into its session-store scanner on each poll.
    try {
      const sr = await fetch(`${RELAY_URL}/v1/session-scan-paths`, { headers: { Authorization: `Bearer ${authToken}` } });
      if (sr.ok) { const sd = await sr.json(); if (Array.isArray(sd.paths)) { const { writeScanPaths } = await import("./sessionStoreScanner.js"); writeScanPaths(sd.paths); } }
    } catch { /* best-effort — defaults still work */ }
  } catch { /* soft-fail — bridge still works in soft mode */ }
}

function authHeaders(): Record<string, string> {
  // ponytail: use relay-assigned id after registration; falls back to local id pre-registration.
  const relayId = (globalThis as any).__RELAY_BRIDGE_ID as string | undefined;
  const h: Record<string, string> = { "x-bridge-id": relayId || BRIDGE_ID };
  if (capToken) h["Authorization"] = `cap ${capToken}`;
  // Lightweight status header on EVERY poll — just hostname/harnesses/count.
  // The full session list (can be 200+ entries, 70KB base64) is pushed separately
  // via POST /v1/bridge-sessions every 10s so it never blows Cloudflare's ~8KB
  // single-header limit (was the 431 root cause).
  try {
    const api = getApiInfo();
    const status = {
      hostname: os.hostname(),
      platform: process.platform,
      harnesses: detectedHarnessNames,
      sessionCount: lastSessionCount,
      label: capLabel,
      tailscaleIp: api.ip,
      apiPort: api.port,
      ts: new Date().toISOString(),
    };
    h["x-bridge-status"] = Buffer.from(JSON.stringify(status)).toString("base64");
  } catch { /* best-effort */ }
  return h;
}

/** Push the full session list (live + scanned, can be 200+ entries) to the relay
 * every 10s. Kept off the poll header — base64 of 200 sessions is ~70KB and
 * blows Cloudflare's ~8KB header limit (431 errors). The relay caches this per
 * bridge and serves /api/sessions from it. */
async function pushSessions(): Promise<void> {
  try {
    const sessions = await discoverSessions();
    lastSessionCount = sessions.length;
    await relayFetch(`${RELAY_URL}/v1/bridge-sessions`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...authHeaders() },
      body: JSON.stringify({ sessions }),
    });
  } catch { /* best-effort — relay may be temporarily unreachable */ }
}

/** Push the live history tail for each broker session to the relay (option B —
 *  no task-queue for reads). The relay caches this per sessionName; the web's
 *  /v1/agent-history reads from the cache instantly (no createTask, no 120s
 *  wait, no claim/answer). This mirrors the working pushSessions pattern: the
 *  bridge PUSHES OUT (no inbound tunnel needed — the relay→bridge Tailscale
 *  path is dead), the relay serves from a hot cache. Reads become ~1ms.
 *  Bounded cost: ~10 sessions × 5ms (warm JSONL index) = 50ms per push. */
async function pushHistory(): Promise<void> {
  try {
    const { listBrokerSessions } = await import("./brokerClient.js");
    const { readSessionHistory } = await import("./historyLoader.js");
    const sessions = await listBrokerSessions().catch(() => [] as any[]);
    // Only push history for LIVE pi sessions (skip other bridges/dashboard senders).
    const live = sessions.filter((s: any) => s.name && s.status !== "bridge" && s.status !== "dashboard");
    if (!live.length) return;
    const history: { sessionName: string; messages: any[] }[] = [];
    for (const s of live) {
      try {
        // Push 200 messages so the web can infinite-scroll up (~7 pages
        // of 30+10×7). Configurable via SUPREMA_HISTORY_PUSH_SIZE.
        const pushSize = Number(process.env.SUPREMA_HISTORY_PUSH_SIZE ?? 200);
        const msgs = await readSessionHistory(s.name, pushSize, s.cwd);
        history.push({ sessionName: s.name, messages: msgs });
      } catch { /* skip unreadable */ }
    }
    if (!history.length) return;
    await relayFetch(`${RELAY_URL}/v1/bridge-history`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...authHeaders() },
      body: JSON.stringify({ history }),
    });
  } catch { /* best-effort — relay may be temporarily unreachable */ }
}

// A dedicated undici dispatcher for relay calls. The relay is behind
// Cloudflare/Traefik, which closes idle keep-alive sockets; undici's default
// global Agent keeps those dead sockets in its pool and reuses them on the
// next fetch → throws `fetch failed` every time (NOT transient — all retries
// reuse dead sockets). The fetch `keepalive:false` option does NOT disable this
// (it controls outliving the page, not HTTP keep-alive) — a custom Agent with a
// short keepAliveTimeout does. This was the real root cause of every dropped
// answer (history never loaded, agent replies never surfaced, "Agent timed out").
import { Agent, fetch as undiciFetch, type Dispatcher } from "undici";
// Idle keep-alive sockets the proxy closed are the root cause of persistent
// `fetch failed` after a relay redeploy: undici's default Agent hands a dead
// socket to the next request. keepAliveTimeout shrinks the idle pool lifetime
// so dead sockets are discarded before reuse; headersTimeout/bodyTimeout make a
// stalled socket fail fast so the retry in relayFetch gets a fresh connection.
// IMPORTANT: the Node global `fetch` does NOT accept a `dispatcher` option — it
// rejects with UND_ERR_INVALID_ARG. We must use undici's `fetch` export.
const relayAgent: Dispatcher = new Agent({
  keepAliveTimeout: 4_000,
  keepAliveMaxTimeout: 8_000,
  headersTimeout: 30_000,
  bodyTimeout: 60_000,
});

/** A relay fetch that survives a dead keep-alive socket. Uses relayAgent (short
 *  keepAliveTimeout) + 3x retry. Uses undici's fetch (NOT the global fetch) so the
 *  `dispatcher` option is honoured. Used for ALL relay call paths via relayFetch. */
async function relayFetch(url: string, init: any = {}, attempts = 3): Promise<any> {
  let lastErr: any;
  for (let a = 0; a < attempts; a++) {
    try {
      return await undiciFetch(url, { ...init, dispatcher: relayAgent } as any);
    } catch (e: any) {
      lastErr = e;
      if (a < attempts - 1) await new Promise((r) => setTimeout(r, 400 * (a + 1)));
    }
  }
  throw lastErr;
}

// Poll the relay for user-initiated interrupts (web Stop/ESC) and forward each
// to the live pi session via the broker — the extension maps the sentinel to
// ctx.abort() (the ESC-in-pi interrupt). Runs on its own fast interval so it
// fires even while a tick() is blocked in waitForReply (setInterval overlaps).
async function pollInterrupts(): Promise<void> {
  let r: Response;
  try { r = await relayFetch(`${RELAY_URL}/v1/pending-interrupt`, { headers: authHeaders() }); }
  catch { return; }
  if (!r.ok) return;
  const d = await r.json().catch(() => null);
  const sessions: string[] = Array.isArray(d?.sessions) ? d.sessions : [];
  for (const name of sessions) {
    try { await sendViaBroker(name, INTERRUPT_SENTINEL); console.log(`⏹ interrupt → ${name}`); }
    catch (e) { console.warn(`[interrupt] ${name} failed: ${e instanceof Error ? e.message : e}`); }
  }
}

async function pollPending(): Promise<any | null> {
  // v3.30 long-poll: hold the request up to WAIT_S so the relay can return the
  // INSTANT a task appears (and idle traffic drops ~12x). 0 = legacy instant poll.
  const params: string[] = [];
  if (HARNESS_FILTER) params.push(`harness=${encodeURIComponent(HARNESS_FILTER)}`);
  if (POLL_WAIT_S > 0) params.push(`wait=${POLL_WAIT_S}`);
  const url = `${RELAY_URL}/v1/pending-question${params.length ? `?${params.join("&")}` : ""}`;
  let r: Response;
  try {
    r = await relayFetch(url, { headers: authHeaders() });
  } catch (e) {
    // Network error — relay unreachable. Throttle the error so it doesn't spam.
    if (!pollErrLogged) { console.error(`[bridge] fetch failed: ${e instanceof Error ? e.message : e}`); pollErrLogged = true; }
    return null;
  }
  pollErrLogged = false;
  // Debug: log poll status (throttled — only first + changes)
  if (r.status !== lastPollStatus) { console.log(`[bridge] poll → ${r.status}`); lastPollStatus = r.status; }
  if (r.status === 204) return null; // no work
  if (r.status === 401) {
    if (!capToken) { console.log("[bridge] 401 — registering…"); await registerBridge(); return null; }
    console.error(`[bridge] 401 — capToken rejected, re-registering…`);
    capToken = null; (globalThis as any).__RELAY_BRIDGE_ID = undefined; await registerBridge(); return null;
  }
  if (r.status === 403) { console.error(`[bridge] 403 — forbidden`); return null; }
  if (!r.ok) { console.error(`[bridge] poll error: ${r.status}`); return null; }
  return r.json();
}
let pollErrLogged = false;
let lastPollStatus = 0;

async function claim(taskId: string): Promise<void> {
  // Retry: a dead keep-alive socket throws `fetch failed` and the task is never
  // marked running on the relay → the answer is never expected → dropped.
  for (let a = 0; a < 3; a++) {
    try {
      const r = await relayFetch(`${RELAY_URL}/v1/claim/${taskId}`, { method: "POST", headers: authHeaders() });
      if (r.status < 500) return;
      console.warn(`[bridge] claim ${taskId.slice(0,8)} → ${r.status}, retrying…`);
    } catch (e: any) {
      if (a < 2) console.warn(`[bridge] claim ${taskId.slice(0,8)} threw (${e?.message}) — retry ${a+1}/2`);
      else throw e;
    }
    await new Promise((r) => setTimeout(r, 400 * (a + 1)));
  }
}

async function postAnswer(taskId: string, opts: {
  answer: string;
  error?: string;
  usage?: { inputTokens: number; outputTokens: number };
  byok?: boolean;
  model?: string;
  filesChanged?: Array<{ path: string; additions: number; deletions: number; diff: string }>;
  question?: { question: string; options: string[]; allowCustom?: boolean };
}): Promise<void> {
  // Retry on transient failures — see relayFetch. The bridge often claims a
  // task, then spends seconds streaming session files (history) before
  // answering; during that gap the proxy closes the idle keep-alive connection
  // and the next fetch reuses a dead socket → throws `fetch failed`. Without a
  // retry the answer is silently dropped → empty history / "Agent timed out".
  const body = JSON.stringify(opts);
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const r = await relayFetch(`${RELAY_URL}/v1/answer/${taskId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body,
      });
      if (r.status < 500) { if (!r.ok) console.warn(`[bridge] postAnswer ${taskId.slice(0,8)} → ${r.status}`); return; }
      console.warn(`[bridge] postAnswer ${taskId.slice(0,8)} → ${r.status}, retrying…`);
    } catch (e: any) {
      if (attempt < 2) console.warn(`[bridge] postAnswer ${taskId.slice(0,8)} threw (${e?.message || e}) — retry ${attempt + 1}/2`);
      else { console.error(`[bridge] postAnswer ${taskId.slice(0,8)} FAILED after retries: ${e?.message || e}`); return; }
    }
    await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
  }
}

// ─── main loop ─────────────────────────────────────────────────────────────

const DEEP_HARNESSES = new Set(["claude-code", "codex", "opencode", "pi", "aider"]);

async function tick(): Promise<void> {
  const task = await pollPending();
  if (!task) return;
  console.log(`▶ claimed task ${task.id} (harness=${task.harness}, user=${task.userId}${task.conversationId ? `, conv=${task.conversationId}` : ""})`);
  await claim(task.id);

  // Heartbeat: keep the relay's task-timeout timer from firing while we work.
  const hb = setInterval(() => { claim(task.id).catch(() => {}); }, HEARTBEAT_MS);
  try {
    await processTask(task);
  } finally {
    clearInterval(hb);
  }
}

async function processTask(task: any): Promise<void> {
  // (History is no longer a task — the bridge pushes each session's JSONL tail to
  // the relay every 15s via pushHistory(); the relay serves it from a hot cache.
  // Only sends flow through this poll/claim/answer loop now.)

  // Interactive sessions: route to sendToAgent (uses claude --resume for
  // conversation continuity). Falls through to one-shot for legacy tasks.
  if (task.conversationId) {
    console.log(`  💬 interactive session — using --resume`);
    const result = await sendToAgent({
      conversationId: task.conversationId,
      message: task.prompt,
      cwd: task.cwd,
      model: task.model,
      harness: task.harness,
      sessionId: (task as any).sessionId,
      sessionName: (task as any).sessionName,
      pid: (task as any).pid,
    });
    console.log(`✓ task ${task.id} → ${result.error ? "error" : "answered"} (${result.answer.length} chars)`);
    await postAnswer(task.id, { answer: result.answer, error: result.error, usage: result.usage, byok: !!task.byokKey, model: task.model, filesChanged: result.filesChanged, question: result.question });
    return;
  }

  const useDeep = DEEP_HARNESSES.has(task.harness) && harnessArgs(task.harness, "", task.model) !== null;
  // BYOK: if the task carries a user key, inject it as the provider env var so
  // the harness uses the user's quota, not ours.
  const byokEnv = task.byokKey ? byokEnvFor(task.byokKey) : {};
  const prevByokEnv: Record<string, string | undefined> = {};
  if (Object.keys(byokEnv).length) {
    for (const [k, v] of Object.entries(byokEnv)) {
      prevByokEnv[k] = (process.env as any)[k];
      (process.env as any)[k] = v;
    }
    console.log(`  🔑 BYOK: injected ${task.byokKey.provider} key`);
  }
  // Worktree isolation (one-shot ephemeral only — NOT interactive resume,
  // which is bound to the live session's original cwd). The bridge creates the
  // worktree itself (cwd-swap, not prompt-inject — the agent never sees a
  // worktree instruction, so your prompt stays clean + no risk of the agent
  // mishandling it). Runs the harness in the worktree, captures the diff, cleans
  // up. Default off; gated behind task.worktree === true (relay passes it).
  let execCwd = task.cwd;
  let worktreePath: string | undefined;
  if (task.worktree && task.cwd) {
    try {
      worktreePath = mkdtempSync(join(tmpdir(), `sn-wt-`));
      await run("git", ["worktree", "add", "--detach", worktreePath], { cwd: task.cwd, timeout: 30_000 });
      execCwd = worktreePath;
      console.log(`  🌿 worktree: ${worktreePath}`);
    } catch (e: any) {
      console.warn(`  🌿 worktree create failed — running in place:`, e?.message?.slice(0, 120));
      if (worktreePath) { try { rmSync(worktreePath, { recursive: true, force: true }); } catch {} worktreePath = undefined; }
      execCwd = task.cwd;
    }
  }
  const result = useDeep
    ? await execOneshot(task.harness, task.prompt, execCwd, task.model)
    : await execPty(task.harness, task.prompt, execCwd);
  // Task 5: capture git diffs for the exec dir (worktree if used, else cwd).
  result.filesChanged = await captureGitDiff(execCwd);
  // restore BYOK-injected env vars
  if (Object.keys(byokEnv).length) {
    for (const [k, v] of Object.entries(prevByokEnv)) {
      if (v === undefined) delete (process.env as any)[k];
      else (process.env as any)[k] = v;
    }
  }

  console.log(`✓ task ${task.id} → ${result.error ? "error" : "answered"} (${result.answer.length} chars${result.usage ? `, in=${result.usage.inputTokens} out=${result.usage.outputTokens}` : ""})`);
  await postAnswer(task.id, { answer: result.answer, error: result.error, usage: result.usage, byok: !!task.byokKey, model: task.model, filesChanged: result.filesChanged });
  // Worktree cleanup: drop the worktree + its dir. Best-effort — if the harness
  // still holds a fd the worktree lock may lag; `git worktree remove --force`.
  if (worktreePath) {
    try { await run("git", ["worktree", "remove", "--force", worktreePath], { cwd: task.cwd, timeout: 15_000 }); } catch {}
    try { rmSync(worktreePath, { recursive: true, force: true }); } catch {}
  }
}

/** Map a BYOK provider → the env var the harness reads (e.g. ANTHROPIC_API_KEY). */
function byokEnvFor(key: { provider: string; rawKey: string }): Record<string, string> {
  switch ((key.provider || "").toLowerCase()) {
    case "anthropic": return { ANTHROPIC_API_KEY: key.rawKey };
    case "openai": return { OPENAI_API_KEY: key.rawKey };
    case "openrouter": return { OPENROUTER_API_KEY: key.rawKey };
    case "deepseek": return { DEEPSEEK_API_KEY: key.rawKey };
    case "zai": return { ZAI_API_KEY: key.rawKey };
    case "google": return { GOOGLE_API_KEY: key.rawKey, GEMINI_API_KEY: key.rawKey };
    default: return {};
  }
}

/**
 * Task 5: capture git diffs for changed files after a task runs. Best-effort —
 * returns [] if cwd isn't a repo or git fails. Truncates diffs to bound payload.
 * ponytail: git diff HEAD --stat + per-file diff, 8KB cap each. No diff lib.
 */
async function captureGitDiff(cwd?: string): Promise<Array<{ path: string; additions: number; deletions: number; diff: string }>> {
  if (!cwd) return [];
  try {
    // Get the list of changed files (staged + unstaged, vs HEAD).
    const { stdout: namesRaw } = await run("git", ["diff", "--name-only", "HEAD"], { cwd, timeout: 5_000 });
    const files = namesRaw.split("\n").map((s) => s.trim()).filter(Boolean);
    if (files.length === 0) return [];
    const out = [];
    for (const file of files.slice(0, 20)) { // cap at 20 files
      try {
        const { stdout: numstat } = await run("git", ["diff", "--numstat", "HEAD", "--", file], { cwd, timeout: 5_000 });
        const m = numstat.match(/^(\d+)\s+(\d+)\s+/);
        const additions = m ? Number(m[1]) : 0;
        const deletions = m ? Number(m[2]) : 0;
        let { stdout: diff } = await run("git", ["diff", "HEAD", "--", file], { cwd, timeout: 5_000, maxBuffer: 16 * 1024 });
        if (diff.length > 8_000) diff = diff.slice(0, 8_000) + "\n… (diff truncated)";
        out.push({ path: file, additions, deletions, diff });
      } catch { /* file no longer exists or git error — skip */ }
    }
    return out;
  } catch {
    return []; // not a git repo, or git missing
  }
}

async function main(): Promise<void> {
  // Start the status server (relay federates this for the multi-network dashboard)
  const bridgePort = Number(flagPort ?? process.env.BRIDGE_PORT ?? 9764);
  startStatusServer(BRIDGE_ID, bridgePort);

  // Detect installed harnesses and include in status
  const detected = await detectHarnesses();
  detectedHarnessNames = detected.map((h) => h.name);
  console.log(`  harnesses detected: ${detectedHarnessNames.join(", ") || "none"}`);

  // Warm the JSONL history index on startup (~2-25s, once) so the FIRST history
  // request is instant (8ms warm vs 25s+ cold). Without this the first user who
  // opens a conversation freezes the bridge's event loop + trips the relay's
  // 120s wait → web shows pending forever. Moved to boot time where it's invisible.
  const { warmJsonlIndex } = await import("./historyLoader.js");
  const warmT = Date.now();
  await warmJsonlIndex();
  console.log(`  history index warmed in ${Date.now() - warmT}ms`);

  // Register with the relay to get a capability token (security fix).
  // Soft mode today: if the relay 401s on poll, we re-register lazily.
  await registerBridge();
  if (capToken) console.log(`  bridge token: acquired (90d)`);
  else console.log(`  bridge token: none (soft mode — set BRIDGE_USER_TOKEN to register)`);

  console.log(`▶ Suprema Bridge ${BRIDGE_ID} → ${RELAY_URL}  (${POLL_WAIT_S > 0 ? `long-poll ${POLL_WAIT_S}s` : `poll every ${POLL_INTERVAL_MS}ms`}${HARNESS_FILTER ? `, harness=${HARNESS_FILTER}` : ""})`);
  // v3.30 poll loop — SEQUENTIAL self-scheduling, not setInterval: a 25s
  // long-poll held open while setInterval keeps firing would stack parallel
  // held connections (and the old 2s interval could double-claim while a
  // task was still executing). One poll in flight at a time; short gap when
  // long-polling, POLL_INTERVAL_MS gap in legacy mode; 3s backoff on errors
  // so a relay restart doesn't get hammered.
  let firstPoll = true;
  (async function pollLoop(): Promise<void> {
    for (;;) {
      try {
        if (firstPoll) { console.log(`[bridge] first poll to ${RELAY_URL}/v1/pending-question`); firstPoll = false; }
        await tick();
        await new Promise((r) => setTimeout(r, POLL_WAIT_S > 0 ? 250 : POLL_INTERVAL_MS));
      } catch (e) {
        console.error("[tick]", e instanceof Error ? e.message : e);
        await new Promise((r) => setTimeout(r, 3_000));
      }
    }
  })();
  // session-push loop — full session list every 10s (off the poll header to
  // stay under Cloudflare's header size limit).
  pushSessions().catch(() => {});
  setInterval(() => { pushSessions().catch(() => {}); }, 10_000);
  // interrupt loop — fast poll so Stop/ESC reaches the live session quickly.
  setInterval(() => { pollInterrupts().catch(() => {}); }, 1_500);
  // history-push loop — option B: the bridge pushes the live history tail every
  // 15s so the relay can serve /v1/agent-history from a hot cache (no task queue).
  // Runs after the index is warm so each readSessionHistory is ~5ms.
  setTimeout(() => { pushHistory().catch(() => {}); }, 8_000);
  setInterval(() => { pushHistory().catch(() => {}); }, 15_000);
  // immediate first tick
  tick().catch(() => {});
}

main();
