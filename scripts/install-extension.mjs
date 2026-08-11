#!/usr/bin/env node
/**
 * Auto-install the suprema-direct pi extension into the user's pi extension dir
 * so live sessions can receive web-app messages. Runs on `npm install` of the
 * bridge (via the "postinstall" script in apps/bridge/package.json).
 *
 * It symlinks apps/bridge/extension/suprema-direct → ~/.pi/agent/extensions/
 * suprema-direct. A symlink keeps the extension in sync with bridge updates
 * (no stale copies). Idempotent + silently skips if pi isn't installed or the
 * target is already a symlink to our source. Never fails the install.
 */
import { existsSync, lstatSync, mkdirSync, readlinkSync, rmSync, symlinkSync, realpathSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = process.env.INIT_CWD && existsSync(join(process.env.INIT_CWD, "extension"))
  ? process.env.INIT_CWD
  : join(fileURLToPath(import.meta.url), "..", "..");

const src = resolve(__dirname, "extension", "suprema-direct");
const destDir = join(homedir(), ".pi", "agent", "extensions");
const dest = join(destDir, "suprema-direct");

try {
  if (!existsSync(src)) { // bridge not fully present (e.g. fresh clone mid-install) — skip
    process.exit(0);
  }
  if (!existsSync(destDir)) mkdirSync(destDir, { recursive: true });
  // Already correct symlink? done. (lstatSync throws on a missing path, so guard it.)
  let destLstat;
  try { destLstat = existsSync(dest) ? lstatSync(dest) : undefined; } catch { destLstat = undefined; }
  if (destLstat?.isSymbolicLink()) {
    let target;
    try { target = realpathSync(dest); } catch { try { target = readlinkSync(dest); } catch {} }
    if (target && realpathSync(src) === target) process.exit(0);
  }
  // Replace an existing entry (old copy or stale symlink) with ours.
  if (destLstat) rmSync(dest, { recursive: true, force: true });
  symlinkSync(src, dest, "dir");
  console.log(`  suprema-direct: installed → ${dest}`);
} catch (e) {
  // Non-fatal: if we can't write to ~/.pi (permissions, read-only fs), the user
  // falls back to direct/headless delivery for this run. Don't break `npm install`.
  console.warn(`  suprema-direct: install skipped (${e instanceof Error ? e.message : e}) — direct mode will be used.`);
}