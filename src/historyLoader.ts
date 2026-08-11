/**
 * Session history loader — reads the tail of a pi session JSONL file and returns
 * the last N messages for display in Suprema's messenger. Vendored pattern from
 * pi-network-dashboard/server/ssh-manager.ts readSessionHistory.
 *
 * Reads ONLY the tail (last N KB) to avoid loading a 22MB file. Each line is a
 * JSON event; we extract user + assistant messages with their text content.
 */
import { openSync, fstatSync, readSync, closeSync, existsSync, watch } from "node:fs";
import { readdir as readdirP, stat as statP, open as openP } from "node:fs/promises";
import { join } from "node:path";
import { homedir } from "node:os";

const SESSIONS_DIR = join(homedir(), ".pi/agent/sessions");

export interface HistoryMessage {
  role: "user" | "assistant" | "system";
  text: string;
  ts?: number;
  model?: string;
}

/** A question the agent is asking — surfaced from an ask_user_question tool call. */
export interface PendingQuestion {
  question: string;
  options: string[];          // selectable choices (may be empty → free-text)
  allowCustom?: boolean;      // true if the user can type a custom answer
  toolCallId?: string;        // the ask_user_question call id
}

// Cached JSONL index — vendored from pi-network-dashboard/server/local-discovery.ts.
// The previous version re-streamed every unnamed candidate line-by-line on
// each lookup → blocked the bridge event loop 49 minutes (measured) → the relay
// timed out → web showed nothing. This version reads each file's full content
// ONCE during the 30s cache rebuild, extracting BOTH cwd AND name upfront. The
// first call after TTL is expensive (~5-10s for ~300 files on an SSD) but every
// subsequent lookup for 30s is a pure O(n) array filter — ZERO disk I/O, ZERO
// line-streaming. This is the proven pi-network-dashboard pattern.
interface JsonlEntry { file: string; mtime: number; name?: string; cwd?: string; id?: string; }
let jsonlIndex: { ts: number; files: JsonlEntry[] } = { ts: 0, files: [] };
let building: Promise<JsonlEntry[]> | null = null; // dedupes concurrent rebuilds
const JSONL_CACHE_TTL = 30_000;

// The CURRENT session name is the LAST `session_info` event, which pi appends on
// every rename — it can sit ANYWHERE (a rename at 12% of a 4MB file is real). So
// head/tail reads miss it. We must scan to EOF, but the previous code did that
// with a SYNC readFileSync + full JSON.parse of every file (766MB across 336
// files) every 30s → the event loop froze for tens of seconds (/api/health
// returned HTTP 000, pushHistory never finished, sends stalled).
//
// This scan is cheap + non-blocking instead:
//   • async chunked reads (fs/promises) — yields to the loop, never freezes;
//   • JSON.parse ONLY the 2-3 lines containing the session/session_info marker
//     (substring pre-filter) — not the whole 766MB;
//   • result cached per (file, mtime) so unchanged files are NEVER re-scanned.
// First build scans everything once (at boot warm); steady-state only re-scans
// the handful of files an active session actually appended to.
const fileMeta = new Map<string, { mtime: number; name?: string; cwd?: string; id?: string }>();

// `size` is a snapshot taken before the scan. We read ONLY up to it and never
// past — session JSONLs are appended live by running agents (this very pi
// session included), so an unbounded `for (;;)` that reads until EOF chases the
// growing tail forever and pegs a core at ~100% CPU (the real freeze cause: boot
// warmup finished by luck on momentarily-idle files; the 30s-TTL rebuild hit an
// actively-written file and spun). A rename that lands after the snapshot is
// picked up on the next rebuild (the file's mtime changes → it re-scans).
async function scanNameCwd(file: string, size: number): Promise<{ name?: string; cwd?: string; id?: string }> {
  const fh = await openP(file, "r");
  try {
    const CHUNK = 256 * 1024;
    const buf = Buffer.alloc(CHUNK);
    let leftover = "", pos = 0;
    let cwd: string | undefined, name: string | undefined, id: string | undefined;
    const consider = (l: string) => {
      if (l.includes('"session_info"')) {
        try { const e = JSON.parse(l); if (e.type === "session_info" && e.name?.trim()) name = e.name.trim(); } catch {}
      } else if (l.includes('"type":"session"')) {
        try { const e = JSON.parse(l); if (e.type === "session") { if (e.cwd) cwd = e.cwd; if (e.id) id = String(e.id); } } catch {}
      }
    };
    while (pos < size) {
      const toRead = Math.min(CHUNK, size - pos); // hard bound — never chase a growing file
      const { bytesRead } = await fh.read(buf, 0, toRead, pos);
      if (bytesRead <= 0) break;
      pos += bytesRead;
      const lines = (leftover + buf.subarray(0, bytesRead).toString("utf8")).split("\n");
      leftover = lines.pop() ?? ""; // last (partial) line carries to next chunk
      for (const l of lines) consider(l);
    }
    if (leftover) consider(leftover);
    return { name, cwd, id };
  } finally { await fh.close(); }
}

function buildJsonlIndex(): Promise<JsonlEntry[]> {
  if (Date.now() - jsonlIndex.ts < JSONL_CACHE_TTL) return Promise.resolve(jsonlIndex.files);
  if (building) return building; // a rebuild is already in flight — join it
  building = (async () => {
    const out: JsonlEntry[] = [];
    try {
      const dirents = await readdirP(SESSIONS_DIR, { withFileTypes: true });
      for (const de of dirents) {
        if (!de.isDirectory()) continue;
        const dir = join(SESSIONS_DIR, de.name);
        let files: string[];
        try { files = (await readdirP(dir)).filter((f) => f.endsWith(".jsonl")); }
        catch { continue; }
        for (const f of files) {
          const file = join(dir, f);
          try {
            const st = await statP(file);
            // Skip files not modified in 90 days to bound the scan.
            if (Date.now() - st.mtimeMs > 90 * 86_400_000) continue;
            let meta = fileMeta.get(file);
            if (!meta || meta.mtime !== st.mtimeMs) {
              meta = { mtime: st.mtimeMs, ...(await scanNameCwd(file, st.size)) };
              fileMeta.set(file, meta);
            }
            out.push({ file, mtime: st.mtimeMs, name: meta.name, cwd: meta.cwd, id: meta.id });
          } catch { /* skip unreadable */ }
        }
      }
    } catch { /* SESSIONS_DIR missing/unreadable */ }
    out.sort((a, b) => b.mtime - a.mtime);
    jsonlIndex = { ts: Date.now(), files: out };
    building = null;
    return out;
  })();
  return building;
}

/** Preemptively build the JSONL index on bridge startup so the FIRST history
 *  request is warm. Now non-blocking (async head/tail reads) — the cold build
 *  no longer freezes the event loop. Safe to call repeatedly (TTL-gated). */
export async function warmJsonlIndex(): Promise<void> {
  try { await buildJsonlIndex(); } catch { /* best-effort */ }
}

/** Find the JSONL file for a named session. O(n) lookup against the cached
 *  index — the first call after the 30s TTL rebuilds it (non-blocking), every
 *  call after is instant. Exported. */
export async function findSessionFile(sessionName: string, cwd?: string): Promise<string | undefined> {
  const idx = await buildJsonlIndex();
  if (!idx.length) return;
  // 1. Exact name match (the fast path — name was extracted during the build).
  for (const c of idx) {
    if (c.name === sessionName && existsSync(c.file)) return c.file;
  }
  // 2. Fallback: newest same-cwd file (best-effort — matches the dashboard).
  if (cwd) {
    const sameCwd = idx.filter((c) => c.cwd === cwd && existsSync(c.file));
    if (sameCwd.length) return sameCwd[0].file;
  }
  return undefined;
}

// Resolve a named session's resumable UUID (pi's `id` from the session header).
// Uses the same warmed index the history reads use — NO separate scan, NO
// head/tail truncation (renames that land mid-file are caught by the full async
// scan). The bridge uses this when the web's sessionId is missing (remote-peer
// entries whose agent file has no pid → /proc can't enrich them).
export async function findSessionMeta(sessionName: string, cwdHint?: string): Promise<{ file?: string; id?: string; cwd?: string } | undefined> {
  const idx = await buildJsonlIndex();
  if (!idx.length) return undefined;
  // 1. Exact name match (the fast path — name was extracted during the build).
  let entry = idx.find((c) => c.name === sessionName && existsSync(c.file));
  // 2. Fallback by cwd hint (e.g. same project).
  if (!entry && cwdHint) entry = idx.find((c) => c.cwd === cwdHint && existsSync(c.file));
  if (!entry) return undefined;
  return { file: entry.file, id: entry.id, cwd: entry.cwd };
}

/** Convenience: just the session id. */
export async function findSessionId(sessionName: string, cwdHint?: string): Promise<string | undefined> {
  return (await findSessionMeta(sessionName, cwdHint))?.id;
}

/** Read the last N messages from a session file. Reads only the tail to stay fast
 *  on 22MB files. Returns oldest→newest. Async — resolves the file path first. */
export async function readSessionHistory(sessionName: string, maxMessages = 30, cwd?: string): Promise<HistoryMessage[]> {
  const file = await findSessionFile(sessionName, cwd);
  if (!file) return [];

  const fd = openSync(file, "r");
  try {
    const st = fstatSync(fd);
    // ponytail: read last 512KB — enough for ~30-50 message turns. If the session
    // is huge we only get the recent tail, which is what the user wants anyway.
    const tailSize = Math.min(512 * 1024, st.size);
    const buf = Buffer.alloc(tailSize);
    readSync(fd, buf, 0, tailSize, Math.max(0, st.size - tailSize));
    const text = buf.toString("utf8");
    const lines = text.split("\n");

    const msgs: HistoryMessage[] = [];
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed.startsWith("{")) continue;
      let e: any;
      try { e = JSON.parse(trimmed); } catch { continue; }

      // pi format: { type:"message", timestamp, message:{ role, content:[{type:"text",text}] } }.
      // Fall back to a flat shape (claude/other) where role+content sit on `e`.
      const msg = e.message || e;
      const role = msg.role || e.role;
      if (role !== "user" && role !== "assistant") continue; // skip toolResult/session/model_change/etc.
      const content = extractText(msg.content ?? msg.text ?? e.content);
      if (content) msgs.push({ role, text: content, ts: parseTs(e.timestamp ?? msg.timestamp), model: msg.model });
    }
    // Take the last N, return oldest→newest.
    return msgs.slice(-maxMessages);
  } finally {
    closeSync(fd);
  }
}

function extractText(content: any): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    // message parts: [{ type: "text", text }, ...]
    return content.filter((p: any) => p?.type === "text" || typeof p === "string")
      .map((p: any) => (typeof p === "string" ? p : p.text || ""))
      .join("")
      .trim();
  }
  if (content?.text) return content.text;
  return "";
}

function parseTs(ts: any): number | undefined {
  if (!ts) return undefined;
  if (typeof ts === "number") return ts;
  const n = Date.parse(ts);
  return isNaN(n) ? undefined : n;
}

export interface ReplyResult {
  answer?: HistoryMessage;   // the agent's final text reply
  question?: PendingQuestion; // the agent asked a question instead
}

/** After sending via the broker, poll the session JSONL for the agent's reply
 *  OR a question. If the agent calls ask_user_question, returns the question so
 *  Suprema can render tappable buttons. Returns the final answer otherwise.
 *
 *  Efficiency: uses fs.watch() to wake on file writes instead of busy-polling —
 *  ~2 reads per message (the actual write moments) instead of ~60. Falls back
 *  to a 5s poll if the watcher dies (some network/overlay FSes don't emit events).
 *
 *  `sentText`, if given, guards against a race: `deliverAs:"steer"` queues our
 *  message into a session that may ALSO be driven live by a human at the
 *  terminal. If a human's own message lands after `sinceTs` too, the next
 *  assistant reply could be answering THEM, not us — matching on ts alone would
 *  hand their answer back to our caller. We track the most recent user-role
 *  entry after sinceTs; once one appears whose text doesn't match ours, replies
 *  are attributed to that foreign turn and withheld until our own text is seen
 *  as a user entry (confirming the live session moved on to processing it). */
export async function waitForReply(
  sessionName: string,
  sinceTs: number,
  timeoutMs = 180_000,
  cwd?: string,
  sentText?: string,
): Promise<ReplyResult> {
  let lastSeenAnswer = sinceTs;
  // Highest entry ts seen so far (any role). Advances on every new JSONL write
  // while the agent works — the "is pi still thinking?" signal for the idle clock.
  let lastActivityTs = sinceTs;
  let file = await findSessionFile(sessionName, cwd);
  // fs.watch the session file; resolve the promise when it changes (or on a 5s
  // fallback poll for watchers that don't fire).
  const checkOnce = (): ReplyResult => {
    if (!file) return {};
    const fd = openSync(file, "r");
    try {
      const st = fstatSync(fd);
      const tailSize = Math.min(512 * 1024, st.size);
      const buf = Buffer.alloc(tailSize);
      readSync(fd, buf, 0, tailSize, Math.max(0, st.size - tailSize));
      const lines = buf.toString("utf8").split("\n");
      let newestQ: PendingQuestion | undefined;
      let newestA: HistoryMessage | undefined;
      // true while the most recent user entry we've scanned belongs to someone
      // else (not our sentText) — assistant replies while this is true answer
      // THEM, not us, so we don't accept them as our answer.
      let foreignTurn = false;
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith("{")) continue;
        let e: any;
        try { e = JSON.parse(trimmed); } catch { continue; }
        const msg = e.message || e;
        const role = msg.role || e.role || e.type;
        const ts = parseTs(e.timestamp) ?? 0;
        if (ts <= sinceTs) continue;
        if (ts > lastActivityTs) lastActivityTs = ts; // any new entry = agent still working
        const content = msg.content || e.content;
        if (role === "user" && sentText) {
          const text = extractText(content);
          foreignTurn = text.trim() !== sentText.trim();
          continue;
        }
        let hasToolCall = false;
        if (Array.isArray(content)) {
          for (const part of content) {
            if (part?.type === "toolCall") {
              hasToolCall = true;
              if (part.name === "ask_user_question") {
                const args = part.arguments || {};
                newestQ = {
                  question: args.question || "Please choose:",
                  options: Array.isArray(args.options) ? args.options.map(String) : [],
                  allowCustom: args.allowCustom ?? false,
                  toolCallId: part.id,
                };
              }
            }
          }
        }
        // Only a text-only assistant message (no pending tool call) is the FINAL
        // answer. Intermediate "let me check…" chunks carry a toolCall — the agent
        // is still working, so we keep waiting rather than surfacing a partial.
        if (role === "assistant" && !hasToolCall && ts > lastSeenAnswer) {
          if (foreignTurn) { lastSeenAnswer = ts; continue; } // belongs to a concurrent human turn — skip, keep waiting
          const text = extractText(content);
          if (text) { newestA = { role: "assistant", text, ts, model: msg.model }; lastSeenAnswer = ts; }
        }
      }
      // A question means the agent is blocked waiting for input — prioritise it.
      if (newestQ) return { question: newestQ };
      if (newestA) return { answer: newestA };
    } finally { closeSync(fd); }
    return {};
  };

  // Idle-based deadline: reset the clock on ANY new JSONL activity so a long-but-
  // active turn never times out — "no timeout while pi is thinking". We only give
  // up after `timeoutMs` of true silence (agent crashed, hung, or genuinely idle).
  // The relay stays alive in parallel via the bridge's periodic re-claim heartbeat.
  let idleDeadline = Date.now() + timeoutMs;
  let seenActivity = sinceTs;
  const bumpIfActive = () => {
    if (lastActivityTs > seenActivity) {
      seenActivity = lastActivityTs;
      idleDeadline = Date.now() + timeoutMs;
    }
  };
  while (Date.now() < idleDeadline) {
    // Re-resolve the file path each iteration in case the session only appeared
    // after we sent (broker delivery can prompt pi to create/append a file).
    file = file || await findSessionFile(sessionName, cwd);
    const immediate = checkOnce();
    if (immediate.answer || immediate.question) return immediate;
    bumpIfActive();
    // Wait for a file-write event (or 5s fallback poll).
    await waitForFileChange(file, Math.min(5_000, Math.max(0, idleDeadline - Date.now())));
    const result = checkOnce();
    if (result.answer || result.question) return result;
    bumpIfActive();
  }
  return {};
}

/** Resolve when the file changes, or after fallbackMs (whichever first). */
function waitForFileChange(file: string | undefined, fallbackMs: number): Promise<void> {
  return new Promise((resolve) => {
    let done = false;
    const finish = () => { if (!done) { done = true; watcher?.close(); clearTimeout(timer); resolve(); } };
    const timer = setTimeout(finish, fallbackMs);
    let watcher: ReturnType<typeof watch> | undefined;
    if (file && existsSync(file)) {
      try {
        watcher = watch(file, () => finish());
        watcher.on("error", finish);
      } catch { /* fall back to timer */ }
    }
  });
}
