/**
 * Session discovery — finds running agent processes (pi, claude, codex) on
 * the bridge's machine and reports their state.
 *
 * Standalone — no pi-network dependency. Replaces the broker's register/list
 * with direct /proc introspection. Each running agent becomes a "session"
 * visible in Suprema's Contacts + Messenger.
 *
 * What we extract per process:
 *   - pid, name, runtime (pi/claude/codex/...)
 *   - cwd (current working directory — what project the agent is in)
 *   - model (best-effort, from cmdline if present)
 *   - status: running / sleeping / idle (from /proc/<pid>/stat)
 *   - startedAt, lastActivity (from /proc/<pid>/stat ctime + stat mtime)
 *
 * ponytail: /proc is the source of truth on Linux. No daemon, no socket, no
 * extension hook required. macOS fallback uses `ps` (less detail).
 */
import { readFileSync, readlinkSync, statSync, readdirSync, existsSync } from "node:fs";
import { execSync } from "node:child_process";
import os, { homedir } from "node:os";
import path, { join } from "node:path";
import { scanNamedSessions, findSessionIdByNameAsync } from "./sessionStoreScanner.js";
import { findSessionMeta } from "./historyLoader.js";

/** Authoritative live-session resolver: a running agent is appending to exactly
 *  ONE session file right now — the newest .jsonl in its harness store matching the
 *  live process's /proc/<pid>/cwd. This is robust across pi/claude/codex because
 *  it keys on cwd+mtime, NOT the display name (claude derives names from the
 *  first user message → never matches the generic "Claude Code" live-process
 *  name; pi names can be renamed mid-file → name-match misses). The live process
 *  is the ground truth for "where is this session".
 *  Returns {sessionId, cwd, file} or undefined. */
function resolveLiveByCwd(cwd: string, runtime: string): { sessionId?: string; cwd: string; file?: string } | undefined {
  if (!cwd) return undefined;
  const home = homedir();
  const enc = cwd.replace(/\//g, "-"); // pi + claude both encode cwd as -home-jamezun-...
  let stores: string[] = [];
  if (runtime === "claude") stores = [join(home, ".claude", "projects")];
  else if (runtime === "codex") stores = [join(home, ".codex", "sessions")];
  else stores = [join(home, ".pi", "agent", "sessions")]; // pi (default) + any pi-format
  let best: { file: string; mtime: number; sessionId?: string } | undefined;
  const RECENT_MS = 10 * 60_000; // live process appended within 10min
  for (const store of stores) {
    if (!existsSync(store)) continue;
    let dirs: string[];
    try { dirs = readdirSync(store, { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => join(store, d.name)); }
    catch { continue; }
    for (const dir of dirs) {
      // Match the cwd-encoded dir name (suffix match handles pi's double-dash encoding).
      if (!dir.endsWith(enc) && !dir.includes(enc)) continue;
      let files: string[];
      try { files = readdirSync(dir).filter((f) => f.endsWith(".jsonl")).map((f) => join(dir, f)); }
      catch { continue; }
      for (const fp of files) {
        try {
          const st = statSync(fp);
          if (Date.now() - st.mtimeMs > RECENT_MS) continue; // skip dormant
          if (!best || st.mtimeMs > best.mtime) {
            // pi filenames are `<timestamp>_<uuid>.jsonl`; claude filenames are `<uuid>.jsonl`.
            // pi `--session`/claude `--resume` want the UUID only, so strip the timestamp prefix.
            const basename = fp.split("/").pop()!.replace(/\.jsonl$/, "");
            const sessionId = basename.includes("_") ? basename.split("_").pop()! : basename;
            best = { file: fp, mtime: st.mtimeMs, sessionId };
          }
        } catch { /* skip */ }
      }
    }
  }
  if (best) return { sessionId: best.sessionId, cwd, file: best.file };
  return undefined;
}

export interface AgentSession {
  id: string;            // stable id: "<runtime>-<pid>" or "remote-<name>"
  pid: number;           // 0 for remote/scanned sessions (no local process)
  runtime: "pi" | "claude" | "codex" | "unknown";
  name: string;          // display name: "Pi" / "Claude Code" / "Suprema Node Network"
  cwd: string;           // working directory ("" for remote peers)
  model?: string;        // extracted from cmdline if present
  status: "online" | "busy" | "idle";
  startedAt: number;     // epoch ms
  lastActivity: number;  // epoch ms
  remote?: boolean;      // true = pi intercom peer on another machine (not local)
  sessionId?: string;    // harness session UUID (for --resume / --session). Resolved at discovery.
  unreachable?: boolean; // true = named session with NO resumable JSONL on disk. Web must REFUSE to send
                         //   (do not silently fall back to a random new session — that was the “Other”
                         //   timeout: a stale agent-file registration whose JSONL was gone/mismatched).
}

// Match cmdline patterns → runtime + display name + model extraction.
const PATTERNS: Array<{ test: RegExp; runtime: AgentSession["runtime"]; name: string; modelFrom?: (cmd: string) => string | undefined }> = [
  // Claude Code: "claude" or "node ... claude"
  { test: /(^|\s)claude(\s|$)/, runtime: "claude", name: "Claude Code", modelFrom: (c) => c.match(/--model[=\s]+(\S+)/)?.[1] },
  // Codex
  { test: /(^|\s)codex(\s|$)/, runtime: "codex", name: "Codex" },
  // Pi (the badlogic pi agent, not "pi-network" or "pinetwork")
  { test: /(^|\s|\/)pi(\s|$)/, runtime: "pi", name: "Pi", modelFrom: (c) => c.match(/--model[=\s]+(\S+)/)?.[1] },
];

// Skip these false positives (look like pi/claude but aren't agent sessions).
const SKIP = /pi-observational|pinetwork|pi-network|pi-intercom|context-mode|pi-extension|suprema/i;

/** Scan for running agent sessions. Returns one entry per matching process. */
export async function discoverSessions(): Promise<AgentSession[]> {
  const isLinux = os.platform() === "linux";
  // Sync part: enumerate live /proc processes + remote peers. Async part:
  // lookup the resumable UUID via the warmed JSONL index (findSessionIdByNameAsync).
  const result = isLinux ? await discoverSessionsLinux() : await discoverSessionsMac();
  const remotes = piRemotePeers();
  if (remotes.length) {
    // Remote peers have no /proc-visible pid; enrich them by name lookup too,
    // and mark unreachable if the named peer can't be found on disk.
    for (const r of remotes) {
      const meta = await findSessionMeta(r.name, r.cwd).catch(() => undefined);
      if (meta?.id) { r.sessionId = meta.id; if (!r.cwd && meta.cwd) r.cwd = meta.cwd; }
      else if (r.name) r.unreachable = true;
    }
  }
  return [...result, ...remotes];
}

/**
 * Pi names its agent-network sessions ("Suprema Node Network", "Jobhunt
 * Extension", …) in ~/.pi/agent/bridge/agents/<name>.json, each carrying the
 * process pid + chosen model. /proc only gives us "Pi" + pid, so we join on pid
 * here to surface the real session name + model in the fleet. Best-effort:
 * missing dir / unnamed session → falls back to the generic runtime name.
 */
function piAgentNames(): Map<number, { name: string; model?: string }> {
  const map = new Map<number, { name: string; model?: string }>();
  try {
    const dir = path.join(os.homedir(), ".pi", "agent", "bridge", "agents");
    for (const f of readdirSync(dir)) {
      if (!f.endsWith(".json")) continue;
      // ponytail: pi writes both <Name>.json and session-<pid>.json for the same
      // pid. The named file has the human-readable sessionName; the session file
      // is auto-generated. Prefer named — skip the generic session-<pid>.json so
      // it never clobbers a real name (last-write-wins by directory order bug).
      if (f.startsWith("session-")) continue;
      try {
        const a = JSON.parse(readFileSync(path.join(dir, f), "utf8"));
        const name = a.sessionName || a.name;
        if (typeof a.pid === "number" && name) map.set(a.pid, { name, model: a.model });
      } catch { /* skip malformed agent file */ }
    }
  } catch { /* no pi agents dir on this machine */ }
  return map;
}

/**
 * Pi intercom-discovered REMOTE peers — pi sessions on OTHER machines in the
 * tailnet. They have no local pid (the process runs elsewhere), so /proc can't
 * see them, but pi's broker persists their heartbeat in the same agents dir.
 * We surface them so the fleet shows every machine's agents, not just local.
 * Stale heartbeats (>5min) are skipped — the peer has gone offline.
 */
function piRemotePeers(): AgentSession[] {
  const out: AgentSession[] = [];
  try {
    const dir = path.join(os.homedir(), ".pi", "agent", "bridge", "agents");
    for (const f of readdirSync(dir)) {
      if (!f.endsWith(".json") || f.startsWith("session-")) continue;
      try {
        const a = JSON.parse(readFileSync(path.join(dir, f), "utf8"));
        if (typeof a.pid === "number") continue; // local — /proc handles it
        const name = a.sessionName || a.name;
        if (!name) continue;
        const seen = typeof a.lastSeen === "number" ? a.lastSeen : (typeof a.heartbeatAt === "number" ? a.heartbeatAt : 0);
        if (!seen) continue;
        const ageSec = (Date.now() - seen) / 1000;
        if (ageSec > 300) continue; // stale >5min — peer gone
        out.push({
          id: `remote-${f.replace(/\.json$/, "").replace(/[^a-z0-9-]/gi, "")}`,
          pid: 0,
          runtime: "pi",
          name,
          cwd: "",
          model: a.model,
          status: ageSec < 60 ? "online" : "idle",
          startedAt: seen,
          lastActivity: seen,
          remote: true,
        });
      } catch { /* skip malformed */ }
    }
  } catch { /* no pi agents dir on this machine */ }
  return out;
}

async function discoverSessionsLinux(): Promise<AgentSession[]> {
  const pids = listPids();
  const named = piAgentNames();
  const sessions: AgentSession[] = [];
  for (const pid of pids) {
    try {
      const cmd = readFileSync(`/proc/${pid}/cmdline`, "utf8").replace(/\0/g, " ").trim();
      if (!cmd || SKIP.test(cmd)) continue;
      const match = PATTERNS.find((p) => p.test.test(cmd));
      if (!match) continue;
      const cwd = safeReadlink(`/proc/${pid}/cwd`);
      const stat = readStat(pid);
      const startedAt = stat?.startTime ?? Date.now();
      const lastActivity = stat?.lastCpu ?? Date.now();
      const ageSec = (Date.now() - lastActivity) / 1000;
      const status: AgentSession["status"] = ageSec < 5 ? "busy" : "online";
      const meta = named.get(pid);
      const name = meta?.name || match.name;
      // Resolve the resumable session UUID. Name-match is primary (a NAMED pi
      // session resumes by its session_info name — unambiguous even when many live
      // procs share one cwd). cwd-newest is the fallback for GENERIC live procs
      // (name === match.name, e.g. "Pi"/"Claude Code") where there's no name to
      // match but the live process is appending to exactly one file.
      const hasName = !!meta?.name;
      let sessionId: string | undefined;
      if (hasName) sessionId = await findSessionIdByNameAsync(name, cwd).catch(() => undefined);
      if (!sessionId) sessionId = resolveLiveByCwd(cwd, match.runtime)?.sessionId;
      // unreachable = a NAMED session we can't resolve to any on-disk file. The
      // web refuses to send — no silent fallback to a random new session (that was
      // the "Other" timeout: a stale agent-file whose JSONL was gone).
      const unreachable = hasName && !sessionId;
      sessions.push({
        id: `${match.runtime}-${pid}`, pid, runtime: match.runtime, name,
        cwd, model: meta?.model || match.modelFrom?.(cmd),
        status, startedAt, lastActivity, sessionId,
        ...(unreachable ? { unreachable: true } : {}),
      });
    } catch { /* process died or no access — skip */ }
  }
  return sessions;
}

async function discoverSessionsMac(): Promise<AgentSession[]> {
  // ponytail: `ps` fallback for macOS. Less detail (no cwd/model without lsof).
  try {
    const out = execSync("ps -eo pid,comm,args", { encoding: "utf8", timeout: 3000 });
    const named = piAgentNames();
    const sessions: AgentSession[] = [];
    for (const line of out.split("\n").slice(1)) {
      const trimmed = line.trim();
      if (!trimmed || SKIP.test(trimmed)) continue;
      const match = PATTERNS.find((p) => p.test.test(trimmed));
      if (!match) continue;
      const pid = Number(trimmed.split(/\s+/)[0]);
      if (!pid) continue;
      const meta = named.get(pid);
      const name = meta?.name || match.name;
      const sessionId = name ? (await findSessionIdByNameAsync(name).catch(() => undefined)) : undefined;
      sessions.push({
        id: `${match.runtime}-${pid}`, pid, runtime: match.runtime, name,
        cwd: "", model: meta?.model || match.modelFrom?.(trimmed),
        status: "online", startedAt: Date.now(), lastActivity: Date.now(), sessionId,
      });
    }
    return sessions;
  } catch { return []; }
}

function listPids(): number[] {
  try {
    return execSync("ls /proc", { encoding: "utf8", timeout: 2000 })
      .split("\n")
      .map((s) => Number(s.trim()))
      .filter((n) => Number.isFinite(n) && n > 0);
  } catch { return []; }
}

function safeReadlink(p: string): string {
  try { return readlinkSync(p); } catch { return ""; }
}

// Parse /proc/<pid>/stat for start time + last CPU activity.
function readStat(pid: number): { startTime: number; lastCpu: number } | null {
  try {
    const raw = readFileSync(`/proc/${pid}/stat`, "utf8");
    // Field 22 = starttime in clock ticks since boot. Field 3 = state.
    // ponytail: split carefully — comm can contain spaces + parens.
    const closeParen = raw.lastIndexOf(")");
    const postComm = raw.slice(closeParen + 2).split(" ");
    const starttimeTicks = Number(postComm[19]); // field 22 overall (0-indexed 19 after comm+state)
    const clkTck = 100; // typical Linux
    const bootEpoch = readBootEpoch();
    const startTime = bootEpoch + (starttimeTicks / clkTck) * 1000;
    // lastCpu: stat file mtime is a decent proxy for last activity.
    const lastCpu = statSync(`/proc/${pid}/stat`).mtimeMs;
    return { startTime, lastCpu };
  } catch { return null; }
}

let cachedBoot: number | null = null;
function readBootEpoch(): number {
  if (cachedBoot !== null) return cachedBoot;
  try {
    const uptime = Number(readFileSync("/proc/uptime", "utf8").split(" ")[0]);
    cachedBoot = Date.now() - uptime * 1000;
    return cachedBoot;
  } catch { return Date.now(); }
}
