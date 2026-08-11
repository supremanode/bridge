/**
 * Session-store scanner — discovers NAMED sessions from each harness's on-disk
 * session store, not just live /proc processes.
 *
 * Why this exists: /proc only shows running processes. A user's named sessions
 * ("Suprema Node App", "Jobhunt Extension") live in JSONL files on disk with
 * full conversation history. The user expects to message these by name — and
 * pi's `/resume` proves the data is there. This scanner reads it so the bridge
 * can resume any named session via `--session <uuid>`.
 *
 * Each harness stores sessions differently:
 *   - Pi:      ~/.pi/agent/sessions/<dir>/<timestamp>_<uuid>.jsonl
 *              First line: {"type":"session","id":"<uuid>","cwd":"<path>"}
 *              Name: {"type":"session_info","name":"<name>"} (latest wins)
 *   - Claude:  ~/.claude/projects/<encoded-cwd>/<uuid>.jsonl
 *              sessionId = filename. No name field — derive from first user msg.
 *   - Codex:   ~/.codex/sessions/YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl (nested)
 *              First line: {"type":"session_meta","payload":{"session_id":"<uuid>","cwd":"<path>"}}
 *              Name: ~/.codex/session_index.jsonl → {"id":"<uuid>","thread_name":"<name>"}
 *              Messages: {"type":"response_item","payload":{"role":"user|assistant","content":[...]}}
 *
 * Custom scan paths: add via Settings (stored on relay, fetched on poll) or
 * SESSION_SCAN_PATHS env (comma-separated). Each path is scanned as pi-format
 * (JSONL with session_info) — the most common pattern.
 */
import { readFileSync, readdirSync, statSync, existsSync, openSync, fstatSync, readSync, closeSync, type Dirent } from "node:fs";
import path from "node:path";
import os from "node:os";
import type { AgentSession } from "./sessionDiscovery.js";

/** Default session-store roots, per harness. Missing dirs are skipped silently. */
export function defaultScanRoots(): Array<{ runtime: string; dir: string }> {
  const home = os.homedir();
  return [
    { runtime: "pi", dir: path.join(home, ".pi", "agent", "sessions") },
    { runtime: "claude", dir: path.join(home, ".claude", "projects") },
    { runtime: "codex", dir: path.join(home, ".codex", "sessions") },
  ];
}

/** Read custom scan paths from a local config file (written by Settings UI
 * via relay → bridge config sync). Falls back to env var. */
export function customScanPaths(): string[] {
  try {
    const cfg = path.join(homeSupremaDir(), "scan-paths.json");
    if (existsSync(cfg)) {
      const data = JSON.parse(readFileSync(cfg, "utf8"));
      if (Array.isArray(data.paths)) return data.paths.filter((p: string) => typeof p === "string");
    }
  } catch { /* corrupt — ignore */ }
  const env = process.env.SESSION_SCAN_PATHS;
  if (env) return env.split(",").map((s) => s.trim()).filter(Boolean);
  return [];
}

function homeSupremaDir(): string {
  const p = path.join(os.homedir(), ".suprema");
  return p;
}

/** Write scan paths (called by the config-sync fetch from relay). */
export function writeScanPaths(paths: string[]): void {
  try {
    const fs = require("node:fs");
    const dir = homeSupremaDir();
    if (!existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, "scan-paths.json"), JSON.stringify({ paths }, null, 2), "utf8");
  } catch { /* best-effort */ }
}

const MAX_SESSIONS = 200; // ponytail: cap — large stores shouldn't stall the poll
const MAX_NAME_LEN = 60;

/** Scan all configured roots → named sessions with resumable UUIDs.
 * ONLY used for the name→sessionId lookup now (findSessionIdByName). The live
 * session list comes from /proc — dormant on-disk sessions are NOT surfaced. */
export function scanNamedSessions(): AgentSession[] {
  const roots = defaultScanRoots();
  const custom = customScanPaths();
  // Custom paths scan as pi-format (session_info) — the generic JSONL pattern.
  for (const p of custom) roots.push({ runtime: "pi", dir: p });

  const out: AgentSession[] = [];
  for (const { runtime, dir } of roots) {
    if (!existsSync(dir)) continue;
    try {
      if (runtime === "claude") out.push(...scanClaude(dir));
      else if (runtime === "codex") out.push(...scanCodex(dir));
      else out.push(...scanPiFormat(dir, runtime)); // pi + custom use this format
    } catch { /* permission error etc — skip this root */ }
    if (out.length >= MAX_SESSIONS) break;
  }

  // Dedup by sessionId — keep the most recently modified. A session cloned/forked
  // across dirs can otherwise surface twice.
  const bySession = new Map<string, AgentSession>();
  for (const s of out) {
    const key = s.sessionId || s.id;
    const existing = bySession.get(key);
    if (!existing || (s.lastActivity || 0) > (existing.lastActivity || 0)) bySession.set(key, s);
  }
  return [...bySession.values()].sort((a, b) => (b.lastActivity || 0) - (a.lastActivity || 0)).slice(0, MAX_SESSIONS);
}

/** Pi-format scanner: JSONL where first line has {id, cwd} and name comes
 * from the latest {type:"session_info", name:"..."} entry. Works for pi,
 * codex, and any custom path following the same pattern. */
function scanPiFormat(root: string, runtime: string): AgentSession[] {
  const out: AgentSession[] = [];
  let dirs: string[];
  try { dirs = readdirSync(root, { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => path.join(root, d.name)); }
  catch { return out; }

  for (const dir of dirs) {
    let files: string[];
    try { files = readdirSync(dir).filter((f) => f.endsWith(".jsonl")).map((f) => path.join(dir, f)); }
    catch { continue; }
    for (const fp of files) {
      try {
        const parsed = parsePiSession(fp);
        if (!parsed || !parsed.name || !parsed.sessionId) continue; // unnamed — skip
        out.push({
          id: `${runtime}-session-${parsed.sessionId.slice(0, 8)}`,
          pid: 0,
          runtime: runtime as any,
          name: parsed.name.slice(0, MAX_NAME_LEN),
          cwd: parsed.cwd || "",
          model: parsed.model,
          sessionId: parsed.sessionId,
          status: "online",
          startedAt: parsed.startedAt,
          lastActivity: parsed.modified,
        });
      } catch { /* corrupt file — skip */ }
    }
  }
  return out;
}

/** Read first line (session header) + reverse-walk the TAIL for latest
 *  session_info name. Reads only head (2KB) + tail (256KB) — NEVER the whole
 *  file. The old code did readFileSync on the entire file (20MB+); called for
 *  every one of ~336 files, ~9× per pushSessions (every 10s), that was ~6.9GB of
 *  synchronous reads per poll → the event loop froze ~25s, CPU pegged, /api
 *  went HTTP 000, history + sends stalled. Head+tail is bounded + fast. */
function parsePiSession(fp: string): { sessionId: string; cwd?: string; name?: string; model?: string; startedAt: number; modified: number } | null {
  const fd = openSync(fp, "r");
  try {
    const st = fstatSync(fd);
    // Head: first line carries the session header {id, cwd, timestamp}.
    const headLen = Math.min(4096, st.size);
    const headBuf = Buffer.alloc(headLen);
    if (headLen) readSync(fd, headBuf, 0, headLen, 0);
    const firstLine = headBuf.toString("utf8").split("\n")[0];
    if (!firstLine.trim().startsWith("{")) return null;
    let sessionId: string | undefined, cwd: string | undefined, startedAt = 0;
    try {
      const header = JSON.parse(firstLine);
      sessionId = header.id; cwd = header.cwd;
      if (header.timestamp) startedAt = Date.parse(header.timestamp) || 0;
    } catch { return null; }
    if (!sessionId) return null;

    // Tail: last 256KB holds the latest session_info (name) + model_change for
    // any recently-active session. A rename buried mid-file in a huge session is
    // missed here — acceptable: this only enriches a live process with a resume
    // UUID; live messaging routes by name via the broker, not this id.
    const tailSize = Math.min(256 * 1024, st.size);
    const tailBuf = Buffer.alloc(tailSize);
    if (tailSize) readSync(fd, tailBuf, 0, tailSize, Math.max(0, st.size - tailSize));
    const tailLines = tailBuf.toString("utf8").split("\n");
    let name: string | undefined, model: string | undefined;
    for (let i = tailLines.length - 1; i >= 0; i--) {
      const line = tailLines[i].trim();
      if (!line.startsWith("{")) continue;
      try {
        const entry = JSON.parse(line);
        if (!name && entry.type === "session_info" && entry.name?.trim()) name = entry.name.trim();
        if (!model && entry.type === "model_change" && entry.modelId) model = entry.modelId;
        if (name && model) break;
      } catch { /* skip */ }
    }
    return { sessionId, cwd, name, model, startedAt: startedAt || st.mtimeMs, modified: st.mtimeMs };
  } finally { closeSync(fd); }
}

/** Claude Code scanner: ~/.claude/projects/<encoded-cwd>/<uuid>.jsonl
 * Claude has no session_info/name field — derive the display name from the
 * first real user message (not system instructions). */
function scanClaude(root: string): AgentSession[] {
  const out: AgentSession[] = [];
  let dirs: string[];
  try { dirs = readdirSync(root, { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => path.join(root, d.name)); }
  catch { return out; }

  for (const dir of dirs) {
    const cwd = decodeClaudeCwdName(path.basename(dir));
    let files: string[];
    try { files = readdirSync(dir).filter((f) => f.endsWith(".jsonl")).map((f) => path.join(dir, f)); }
    catch { continue; }
    for (const fp of files) {
      try {
        const sessionId = path.basename(fp, ".jsonl");
        const name = deriveClaudeName(fp);
        if (!name) continue; // no user content yet — skip
        const st = statSync(fp);
        out.push({
          id: `claude-session-${sessionId.slice(0, 8)}`,
          pid: 0,
          runtime: "claude",
          name: name.slice(0, MAX_NAME_LEN),
          cwd,
          sessionId,
          status: "online",
          startedAt: st.birthtimeMs || st.mtimeMs,
          lastActivity: st.mtimeMs,
        });
      } catch { /* skip */ }
    }
  }
  return out;
}

/** Claude encodes cwd into the dir name: /home/jamezun/Coding Projects → -home-jamezun-Coding-Projects */
function decodeClaudeCwdName(name: string): string {
  if (!name.startsWith("-")) return name;
  return name.slice(1).replace(/-/g, "/");
}

/** Derive a display name from a claude session file.
 * Priority: custom-title/agent-name (/rename) → first real user message. */
function deriveClaudeName(fp: string): string | undefined {
  // Read first ~16KB — /rename writes custom-title + agent-name near the top,
  // before the first real user message. Skip system-injection caveats.
  const fd = require("node:fs").openSync(fp, "r");
  const buf = Buffer.alloc(16384);
  require("node:fs").readSync(fd, buf, 0, 16384, 0);
  require("node:fs").closeSync(fd);
  const head = buf.toString("utf8");
  let customName: string | undefined;
  for (const line of head.split("\n")) {
    if (!line.trim().startsWith("{")) continue;
    try {
      const entry = JSON.parse(line);
      // /rename writes these — the authoritative name.
      if (entry.type === "custom-title" && entry.customTitle?.trim()) { customName = entry.customTitle.trim(); break; }
      if (!customName && entry.type === "agent-name" && entry.agentName?.trim()) customName = entry.agentName.trim();
    } catch { /* skip */ }
  }
  if (customName) return customName;
  // Fallback: first real user message (skip system-instruction injections).
  for (const line of head.split("\n")) {
    if (!line.trim().startsWith("{")) continue;
    try {
      const entry = JSON.parse(line);
      if (entry.type === "user" && entry.message?.role === "user") {
        const content = typeof entry.message.content === "string"
          ? entry.message.content
          : Array.isArray(entry.message.content)
            ? entry.message.content.find((c: any) => c.type === "text")?.text
            : undefined;
        if (!content) continue;
        if (/^\[System instructions\]/.test(content)) continue;
        if (/^<local-command-caveat>/.test(content)) continue;
        return content.replace(/\n/g, " ").trim();
      }
    } catch { /* skip */ }
  }
  return undefined;
}

/** Codex CLI scanner: ~/.codex/sessions/YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl (nested).
 * Names live in a SEPARATE registry file (~/.codex/session_index.jsonl),
 * not in the session files themselves. First line of each session file is
 * {"type":"session_meta","payload":{"session_id":"<uuid>","cwd":"<path>"}}.
 * Returns only NAMED sessions (those with a thread_name in the index). */
function scanCodex(root: string): AgentSession[] {
  const out: AgentSession[] = [];

  // 1. Load the name registry: id → { thread_name, updated_at }
  const nameMap = new Map<string, { name: string; updated: number }>();
  const idxPath = path.join(path.dirname(root), "session_index.jsonl");
  try {
    const idx = readFileSync(idxPath, "utf8");
    for (const line of idx.split("\n")) {
      const t = line.trim();
      if (!t.startsWith("{")) continue;
      try {
        const e = JSON.parse(t);
        if (e.id && e.thread_name) nameMap.set(e.id, { name: e.thread_name, updated: Date.parse(e.updated_at) || 0 });
      } catch { /* skip */ }
    }
  } catch { /* no index — all sessions unnamed, return empty */ }
  if (nameMap.size === 0) return out;

  // 2. Recursively find all .jsonl under root (nested YYYY/MM/DD).
  const files: string[] = [];
  const walk = (dir: string, depth: number) => {
    if (depth > 4 || files.length >= MAX_SESSIONS * 3) return; // bound recursion
    let entries: Dirent[];
    try { entries = readdirSync(dir, { withFileTypes: true }); }
    catch { return; }
    for (const e of entries) {
      if (files.length >= MAX_SESSIONS * 3) break;
      const fp = path.join(dir, e.name);
      if (e.isDirectory()) walk(fp, depth + 1);
      else if (e.name.endsWith(".jsonl")) files.push(fp);
    }
  };
  walk(root, 0);

  // 3. Parse each: first line has session_meta with id + cwd; match name from index.
  for (const fp of files) {
    try {
      const fd = openSync(fp, "r");
      let sessionId: string | undefined, cwd: string | undefined;
      try {
        const headBuf = Buffer.alloc(65536);
        const n = readSync(fd, headBuf, 0, Math.min(65536, fstatSync(fd).size), 0);
        const firstLine = headBuf.toString("utf8", 0, n).split("\n")[0];
        if (firstLine.trim().startsWith("{")) {
          const meta = JSON.parse(firstLine);
          if (meta.type === "session_meta" && meta.payload) {
            sessionId = meta.payload.session_id;
            cwd = meta.payload.cwd;
          }
        }
      } finally { closeSync(fd); }
      if (!sessionId) continue;
      const named = nameMap.get(sessionId);
      if (!named || !named.name.trim()) continue; // unnamed — skip
      const st = statSync(fp);
      out.push({
        id: `codex-session-${sessionId.slice(0, 8)}`,
        pid: 0,
        runtime: "codex",
        name: named.name.trim().slice(0, MAX_NAME_LEN),
        cwd: cwd || "",
        sessionId,
        status: "online",
        startedAt: named.updated || st.birthtimeMs || st.mtimeMs,
        lastActivity: st.mtimeMs,
      });
    } catch { /* corrupt file — skip */ }
  }
  return out;
}

// name→sessionId resolution now delegates to historyLoader.findSessionMeta — the
// SAME warmed index history reads use. The old version read each JSONL
// head+tail, which missed mid-file renames (the index does a full async scan).
// That double scan also froze the event loop for ~25s every pushSessions.
// historyLoader is the single source of truth; this module just converts a sync
// call (from sessionDiscovery's /proc enrichment) into the async index lookup.
import { findSessionMeta } from "./historyLoader.js";
const nameIdCache = new Map<string, { ts: number; id?: string }>();
const NAME_ID_TTL = 30_000;

export async function findSessionIdByNameAsync(name: string, cwdHint?: string): Promise<string | undefined> {
  const key = `${name}::${cwdHint || ""}`;
  const c = nameIdCache.get(key);
  if (c && Date.now() - c.ts < NAME_ID_TTL) return c.id;
  const meta = await findSessionMeta(name, cwdHint).catch(() => undefined);
  const id = meta?.id;
  nameIdCache.set(key, { ts: Date.now(), id });
  return id;
}

// findSessionIdByName (sync) is gone — it scanned the broken head/tail index.
// sessionDiscovery now enriches /proc sessions with the resumable UUID lazily
// via findSessionIdByNameAsync above. Claude is skipped — fuzzy no-op.
