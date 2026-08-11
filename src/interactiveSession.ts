/**
 * Interactive agent sessions — persistent conversations with claude-code.
 *
 * Replaces the one-shot task model with WhatsApp-style continuity. Each
 * conversation gets a claude session_id; subsequent messages use --resume so
 * the agent remembers the whole thread.
 *
 * Why not attach to a running TUI? A live pi/claude session's stdin is bound
 * to a pseudo-terminal owned by the terminal emulator — writing to it from
 * outside scrambles the TUI. Claude's --resume gives us continuity WITHOUT
 * needing to attach to the live process: same agent, same cwd, full memory,
 * separate conversation thread.
 *
 * Storage: in-memory Map (conversationId → session state). Survives bridge
 * restarts via claude's own session store (the session_id is durable; we just
 * need to remember it. ponytail: persist conversationId↔sessionId to a file
 * so it survives bridge restart).
 */
import { spawn, execFile, type ChildProcess } from "node:child_process";
import { promisify } from "node:util";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { randomUUID } from "node:crypto";
import path from "node:path";
import os from "node:os";
// Broker delivery is the LIVE path: when a pi session is running interactively
// with the suprema-direct extension installed, the web app's message is injected
// straight into that process as the user's own message (instant + no box). Falls
// back to headless `pi -p --session` resume when the live session isn't
// broker-reachable (extension absent, or `-p` headless run). Both paths produce
// clean output; broker is faster + updates the running session live.
import { sendViaBroker, listBrokerSessions } from "./brokerClient.js";
import { findSessionMeta, waitForReply } from "./historyLoader.js";

const run = promisify(execFile);

// Run `pi -p` and resolve on the child's `exit` event with the collected stdout —
// NOT on `close` (stdio pipe closed). pi -p spawns worker processes that inherit
// the stdout pipe and keep it open AFTER pi exits; `promisify(execFile)` waits
// for `close`, which never fires while a grandchild holds the pipe → the promise
// hangs forever → the bridge never posts the answer → "Agent timed out".
// Resolving on `exit` + collecting chunks ourselves is immune to that.
// `detached` + process-group kill on cleanup ensures no worker outlives the turn.
function runHeadless(cmd: string, args: string[], opts: { cwd: string; timeoutMs: number; env: NodeJS.ProcessEnv }): Promise<{ stdout: string; stderr: string; code: number | null; signal: NodeJS.Signals | null }> {
  return new Promise((resolve) => {
    let child: ChildProcess | null = null;
    let stdout = "", stderr = "";
    const timer = setTimeout(() => {
      if (child?.pid) { try { process.kill(-child.pid, "SIGKILL"); } catch { try { child.kill("SIGKILL"); } catch {} } }
      resolve({ stdout, stderr, code: null, signal: "SIGTERM" });
    }, opts.timeoutMs);
    try {
      child = spawn(cmd, args, {
        cwd: opts.cwd, env: opts.env, stdio: ["ignore", "pipe", "pipe"],
        detached: true, // new process group → kill(-pid) reaps workers too
      } as any);
    } catch (e: any) { clearTimeout(timer); resolve({ stdout, stderr: String(e), code: 1, signal: null }); return; }
    child.stdout?.on("data", (d: Buffer) => { stdout += d.toString("utf8"); });
    child.stderr?.on("data", (d: Buffer) => { stderr += d.toString("utf8"); });
    // ⚠ resolve on `exit`, NOT `close` — close never fires if a grandchild holds the pipe.
    child.once("exit", (code, signal) => { clearTimeout(timer); resolve({ stdout, stderr, code, signal }); });
    child.once("error", (err) => { clearTimeout(timer); resolve({ stdout, stderr: String(err), code: 1, signal: null }); });
  });
}

// A coding agent answering a real question reads files, greps, edits — 120s
// was too short and killed pi/claude mid-task (empty stdout → "Command failed").
// ponytail: 5min ceiling; raise via AGENT_EXEC_TIMEOUT_MS if long tasks need it.
const AGENT_TIMEOUT_MS = Number(process.env.AGENT_EXEC_TIMEOUT_MS ?? 300_000);

interface AgentConversation {
  conversationId: string;    // Suprema's conversation id
  claudeSessionId?: string;  // claude-code's session id (for --resume)
  piSessionId?: string;      // pi's session UUID (for --session-id)
  sessionId?: string;        // scanned named session UUID (resumable — supersedes the above)
  sessionName?: string;      // display name — for broker delivery to the LIVE process
  pid?: number;              // live process pid — lets the broker resolve display→registered name
  runtime?: string;          // "claude-code" | "pi" | "codex" — which CLI to dispatch to
  cwd: string;               // working directory
  createdAt: string;
  lastActivity: string;
  busy: boolean;             // true while a turn is executing
}

const SESSIONS_PATH = process.env.AGENT_SESSIONS_PATH || path.join(os.homedir(), ".suprema", "agent-sessions.json");
let conversations = new Map<string, AgentConversation>();

// Load persisted conversation→session mapping on boot.
function loadConversations(): void {
  try {
    if (existsSync(SESSIONS_PATH)) {
      const data = JSON.parse(readFileSync(SESSIONS_PATH, "utf8"));
      for (const [id, conv] of Object.entries(data)) {
        conversations.set(id, conv as AgentConversation);
      }
    }
  } catch { /* corrupt — start fresh */ }
}

function persistConversations(): void {
  try {
    const dir = path.dirname(SESSIONS_PATH);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    const obj: Record<string, AgentConversation> = {};
    for (const [id, conv] of conversations) obj[id] = conv;
    writeFileSync(SESSIONS_PATH, JSON.stringify(obj, null, 2), "utf8");
  } catch { /* best-effort */ }
}

loadConversations();

export interface AgentReply {
  answer: string;
  sessionId?: string;     // claude's session id (captured for --resume)
  usage?: { inputTokens: number; outputTokens: number };
  error?: string;
  filesChanged?: Array<{ path: string; additions: number; deletions: number; diff: string }>;
  question?: { question: string; options: string[]; allowCustom?: boolean }; // agent asked — render as tappable buttons
}

/**
 * Send a message to an agent conversation. First message spawns claude;
 * subsequent messages resume the same session.
 */
export async function sendToAgent(opts: {
  conversationId: string;
  message: string;
  cwd?: string;
  model?: string;
  harness?: string;          // "claude-code" | "pi" | "codex"
  sessionId?: string;        // scanned named session UUID — resume directly into it
  sessionName?: string;      // display name — used for broker delivery to the LIVE process
  pid?: number;              // live process pid — helps broker resolve display→registered name
}): Promise<AgentReply> {
  const cwd = opts.cwd || process.cwd();
  let conv = conversations.get(opts.conversationId);

  // Create the conversation if new.
  if (!conv) {
    conv = {
      conversationId: opts.conversationId,
      cwd,
      runtime: opts.harness || "claude-code",
      sessionId: opts.sessionId, // sticky — first message locks the target session
      createdAt: new Date().toISOString(),
      lastActivity: new Date().toISOString(),
      busy: false,
    };
    conversations.set(opts.conversationId, conv);
  }
  // ponytail: runtime + sessionId + sessionName are sticky — first message locks the target.
  if (!conv.runtime) conv.runtime = opts.harness || "claude-code";
  if (!conv.sessionId && opts.sessionId) conv.sessionId = opts.sessionId;
  if (!conv.sessionName && opts.sessionName) conv.sessionName = opts.sessionName;
  if (opts.pid) conv.pid = opts.pid; // refresh each turn — the live pid can change on restart

  conv.busy = true;
  conv.lastActivity = new Date().toISOString();
  persistConversations();

  // Auto mode (default): for pi sessions, try the live broker delivery FIRST —
  // instant + the running session shows the message + reply. Only fall back to
  // headless resume if the live session isn't broker-reachable (the suprema-direct
  // extension isn't installed, or this is a `-p` run with no live process). Both
  // paths yield clean stderr/stdout; broker wins on speed + live-visibility.
  if (conv.runtime === "pi") {
    // undefined = "not delivered, try headless"; a reply object (even with an
    // error) = "delivered — do NOT also run headless" (would double-execute
    // the message against the same live session / JSONL).
    const brokerReply = await sendToPiBroker(conv, opts).catch(() => undefined);
    if (brokerReply) { conv.busy = false; conv.lastActivity = new Date().toISOString(); persistConversations(); return brokerReply; }
    return sendToPi(conv, opts);
  }
  if (conv.runtime === "codex") return sendToCodex(conv, opts);
  return sendToClaude(conv, opts);
}

/** Pi: deliver to the LIVE process via the broker (instant, full memory) if we know
 *  the session name. Fall back to `pi -p --session` cold-resume only if the broker
 *  can't deliver — e.g. the process died or broker.sock is absent. */
/** pi's headless `-p` stdout is the agent's reply plus runtime noise: worker
 *  cleanup ("[runner] …"), passive extension logs ("[pi-network] …"), and a
 *  trailing status footer the user's setup appends ("---\n_…Quota…_", which is
 *  instructed content that survives even --no-extensions). Strip all of it so
 *  Suprema shows the answer exactly as a natural local pi session would. */
function cleanPiReply(raw: string): string {
  let text = raw
    .split("\n")
    .filter((l) => !/^\s*\[(runner|pi-[\w-]+)\]/.test(l))
    .join("\n")
    .trim();
  // Trailing "---" divider + an underscore-wrapped one-liner (the status footer).
  text = text.replace(/\n+\s*-{3,}\s*\n+_[^\n]*_\s*$/, "").trim();
  return text;
}

// Broker (live) delivery for pi sessions: if the live pi process is registered
// on the broker (suprema-direct extension loaded in it), `send` the plain text
// to it — the extension calls `pi.sendUserMessage`, so the message appears in the
// running session exactly as if you typed it (no box, no mesh footer) and the
// agent turns immediately. The reply is written to the session JSONL by pi → we
// watch+read it via waitForReply. Returns `undefined` when the broker can't
// deliver / session isn't live — that's the ONLY "try headless instead" signal.
// Once `sendViaBroker` reports delivered:true, we always return a reply object
// (answer, question, or error) — never undefined — so the caller can't also run
// headless and double-execute the message against the same live session.
async function sendToPiBroker(conv: AgentConversation, opts: { message: string; model?: string }): Promise<AgentReply | undefined> {
  if (!conv.sessionName) return undefined;
  // Is the target session's suprema-direct receiver currently live on the
  // broker? Matched by the `#suprema-direct`-suffixed name ONLY — the
  // pi-network extension registers the SAME bare session name under the SAME
  // pid (both run in the same pi process), so an unsuffixed name or pid match
  // would be ambiguous between the two extensions' entries. See brokerClient's
  // sendToSession for the full rationale.
  const directName = `${conv.sessionName}#suprema-direct`;
  let reachable = false;
  try { reachable = (await listBrokerSessions()).some((s: any) => s.name === directName || s.name?.toLowerCase() === directName.toLowerCase()); }
  catch { return undefined; }
  if (!reachable) return undefined;

  const sentAt = Date.now();
  let delivered = false;
  try { delivered = await sendViaBroker(conv.sessionName!, opts.message); }
  catch { return undefined; }
  if (!delivered) return undefined;

  // Delivered — the live session now has the message and the agent is turning
  // on it. From here we must NOT let the caller fall back to headless: a second
  // `pi -p --session` resume would run the same message twice and race the
  // live process writing the same JSONL. A timeout past this point is a real
  // error, not a "try headless instead" signal.
  try {
    const reply = await waitForReply(conv.sessionName!, sentAt, AGENT_TIMEOUT_MS, execCwdForConv(conv), opts.message);
    if (reply.answer) return { answer: reply.answer.text, sessionId: conv.sessionId || conv.piSessionId, usage: undefined, question: reply.question };
    if (reply.question) return { answer: "", question: reply.question, sessionId: conv.sessionId || conv.piSessionId };
    return { answer: "", error: "delivered but no reply before timeout", sessionId: conv.sessionId || conv.piSessionId };
  } catch (e: any) {
    return { answer: "", error: e?.message?.slice(0, 200) || "delivered but reply wait failed", sessionId: conv.sessionId || conv.piSessionId };
  }
}

// Resolve the exec cwd for a conversation (the live session's project dir),
// so waitForReply narrows to the right JSONL. Mirrors sendToPi's resolver.
function execCwdForConv(conv: AgentConversation): string | undefined {
  return conv.cwd && conv.cwd !== process.cwd() ? conv.cwd : undefined;
}

async function sendToPi(conv: AgentConversation, opts: { message: string; model?: string }): Promise<AgentReply> {
  // Standalone headless resume — NO pi-network broker. The bridge runs pi's own
  // `-p --session <id>`, so the reply comes straight off stdout: clean (no From:/
  // border/network_comm framing) AND we never miss a tool-call reply (the broker
  // path did → "Agent timed out"). The resumed session carries full agent memory.

  // Resolve the target UUID + project cwd if missing. The web often sends a
  // "remote peer" entry (agent file with no pid → /proc can't enrich it), so
  // sessionId + cwd arrive empty. We must (a) resume the RIGHT session (by name
  // → id from the warmed JSONL index) and (b) run pi from the session's OWN
  // project dir, or pi prompts "Fork this session into current directory?"
  // interactively → hangs headless mode → 2-min timeout.
  let execCwd = conv.cwd;
  if (!conv.sessionId && !conv.piSessionId && conv.sessionName) {
    const meta = await findSessionMeta(conv.sessionName, conv.cwd).catch(() => undefined);
    if (meta?.id) {
      conv.sessionId = meta.id!;
      if (meta.cwd && (!execCwd || execCwd === process.cwd())) execCwd = meta.cwd;
      persistConversations();
    }
  } else if (!execCwd || execCwd === process.cwd()) {
    const meta = await findSessionMeta(conv.sessionName || "", conv.cwd).catch(() => undefined);
    if (meta?.cwd) execCwd = meta.cwd;
  }

  // Still no UUID → start a new session (first message to an unnamed conv).
  if (!conv.sessionId && !conv.piSessionId) {
    conv.piSessionId = randomUUID();
    persistConversations();
  }
  const targetSession = conv.sessionId || conv.piSessionId!;
  try {
    // --session resumes an existing session file; --session-id creates one if missing.
    const flag = conv.sessionId ? "--session" : "--session-id";
    const piArgs = process.env.SUPREMA_AGENT_EXTENSIONS === "1" ? [] : ["--no-extensions"];
    const args = ["-p", ...piArgs, flag, targetSession, opts.message];
    // runHeadless resolves on the child EXIT event (not `close`) — pi's spawned
    // workers keep the stdout pipe open after pi exits, which would hang
    // promisify(execFile) forever. A waiting timeout was the "Agent timed out".
    const { stdout, stderr, code, signal } = await runHeadless("pi", args, {
      cwd: execCwd || process.cwd(), timeoutMs: AGENT_TIMEOUT_MS, env: { ...process.env, TERM: "dumb" },
    });
    const answer = cleanPiReply(stdout);
    if (!answer && (code !== 0 && code !== null)) return { answer: "", error: (stderr || `pi exited ${code ?? signal}`).slice(0, 300) || "pi execution failed", sessionId: conv.sessionId || conv.piSessionId };
    return { answer: answer || "(no output)", sessionId: conv.sessionId || conv.piSessionId };
  } catch (e: any) {
    const out = cleanPiReply(e.stdout || "");
    if (out) return { answer: out, error: e.stderr?.slice(0, 200), sessionId: conv.sessionId || conv.piSessionId };
    return { answer: "", error: e.message?.slice(0, 300) || "pi execution failed" };
  } finally {
    conv.busy = false; conv.lastActivity = new Date().toISOString(); persistConversations();
  }
}

// Claude Code: headless `--print` + `--resume <sessionId>` if scanned, else new.
// Uses runHeadless (resolve on exit) — claude spawns plugin subprocesses that
// inherit the stdout pipe and keep it open after claude exits; promisify(execFile)
// waits on `close` → hangs forever → "Agent timed out" (the same bug pi had).
async function sendToClaude(conv: AgentConversation, opts: { message: string; model?: string }): Promise<AgentReply> {
  const modelArgs = opts.model ? ["--model", opts.model] : [];
  const resumeArgs = (conv.sessionId || conv.claudeSessionId) ? ["--resume", conv.sessionId || conv.claudeSessionId!] : [];
  const args = ["--print", "--output-format", "stream-json", ...resumeArgs, ...modelArgs, opts.message];
  try {
    const { stdout } = await runHeadless("claude", args, {
      cwd: conv.cwd || process.cwd(), timeoutMs: AGENT_TIMEOUT_MS, env: { ...process.env, TERM: "dumb" },
    });
    const parsed = parseStreamJson(stdout);
    if (parsed.sessionId && !conv.claudeSessionId) { conv.claudeSessionId = parsed.sessionId; persistConversations(); }
    return { answer: parsed.answer || stdout.trim(), sessionId: parsed.sessionId, usage: parsed.usage, filesChanged: parsed.filesChanged };
  } catch (e: any) {
    const out = (e.stdout || "").trim();
    if (out) { const parsed = parseStreamJson(out); if (parsed.sessionId && !conv.claudeSessionId) { conv.claudeSessionId = parsed.sessionId; persistConversations(); } return { answer: parsed.answer || out.trim(), error: e.stderr?.slice(0, 200), usage: parsed.usage }; }
    return { answer: "", error: e.message?.slice(0, 300) || "claude execution failed" };
  } finally { if (conv) { conv.busy = false; conv.lastActivity = new Date().toISOString(); persistConversations(); } }
}

/** Codex: headless. The `codex` CLI resume flag is `--resume <id>`; format mirrors
 *  claude. If `codex` isn't installed, runHeadless fails fast (ENOENT) → a clear
 *  error instead of a hang. */
async function sendToCodex(conv: AgentConversation, opts: { message: string; model?: string }): Promise<AgentReply> {
  const modelArgs = opts.model ? ["-m", opts.model] : [];
  const resumeArgs = (conv.sessionId) ? ["--resume", conv.sessionId] : [];
  const args = ["exec", "--json", ...resumeArgs, ...modelArgs, opts.message];
  try {
    const { stdout } = await runHeadless("codex", args, {
      cwd: conv.cwd || process.cwd(), timeoutMs: AGENT_TIMEOUT_MS, env: { ...process.env, TERM: "dumb" },
    });
    const parsed = parseStreamJson(stdout);
    return { answer: parsed.answer || stdout.trim(), sessionId: parsed.sessionId, usage: parsed.usage };
  } catch (e: any) {
    return { answer: "", error: (e.message || "").includes("ENOENT") ? "codex CLI not installed on the bridge machine" : (e.message?.slice(0, 300) || "codex execution failed") };
  } finally { if (conv) { conv.busy = false; conv.lastActivity = new Date().toISOString(); persistConversations(); } }
}

/** Parse claude-code stream-json NDJSON → { answer, usage, sessionId, filesChanged }. */
function parseStreamJson(stdout: string): { answer: string; usage?: { inputTokens: number; outputTokens: number }; sessionId?: string; filesChanged?: any[] } {
  const lines = stdout.split("\n").filter((l) => l.trim().startsWith("{"));
  let answer = "";
  let usage: { inputTokens: number; outputTokens: number } | undefined;
  let sessionId: string | undefined;
  const filesChanged: any[] = [];

  for (const line of lines) {
    try {
      const msg = JSON.parse(line);
      // result message: { type:"result", result:"...", usage:{...}, session_id:"..." }
      if (msg.type === "result") {
        answer = msg.result || answer;
        if (msg.usage) {
          usage = {
            inputTokens: (msg.usage.input_tokens ?? 0) + (msg.usage.cache_creation_input_tokens ?? 0),
            outputTokens: msg.usage.output_tokens ?? 0,
          };
        }
        if (msg.session_id) sessionId = msg.session_id;
      }
      // tool_use with file edits (best-effort capture)
      if (msg.type === "tool_use" && msg.name === "str_replace_editor" && msg.input?.command === "create") {
        filesChanged.push({ path: msg.input.path, additions: (msg.input.file_raw || "").split("\n").length, deletions: 0, diff: "" });
      }
    } catch { /* skip unparseable */ }
  }
  return { answer, usage, sessionId, filesChanged: filesChanged.length ? filesChanged : undefined };
}

/** Get conversation state (for status: busy/idle). */
export function getConversation(conversationId: string): AgentConversation | undefined {
  return conversations.get(conversationId);
}

/** List all conversations (for history loading). */
export function listConversations(): AgentConversation[] {
  return Array.from(conversations.values());
}
