/** Lightweight harness detection for the bridge (standalone — no workspace deps). */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
const run = promisify(execFile);

const HARNESS_IDS = [
  { id: "claude-code", name: "Claude Code", bin: "claude" },
  { id: "codex", name: "Codex CLI", bin: "codex" },
  { id: "opencode", name: "OpenCode", bin: "opencode" },
  { id: "kilo", name: "Kilo Code", bin: "kilo" },
  { id: "pi", name: "Pi", bin: "pi" },
  { id: "gemini", name: "Gemini CLI", bin: "gemini" },
  { id: "aider", name: "Aider", bin: "aider" },
  { id: "goose", name: "Goose", bin: "goose" },
];

export async function detectHarnesses() {
  const found = [];
  for (const h of HARNESS_IDS) {
    try { await run("which", [h.bin]); found.push({ id: h.id, name: h.name }); }
    catch { /* not installed */ }
  }
  return found;
}
