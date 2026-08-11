/**
 * Bridge status + direct API server.
 *
 * Exposes the machine's running servers + harnesses for the relay to federate
 * (legacy /api/status), AND the two endpoints the web actually needs —
 * /api/history (read the session JSONL directly) + /api/send (inject a message
 * into the LIVE pi session via the broker). This mirrors pi-network-dashboard's
 * working pattern: direct local file reads + broker delivery, NO relay task-queue
 * round-trip. The relay round-trip (task → claim → answer → poll) was the root
 * cause of every "history never loads" / "Agent timed out" failure — a 4-hop
 * queue to read a local file.
 *
 * Listens on BRIDGE_API_PORT (default 9765 — 9764 is taken by pi). Auto-increments
 * on EADDRINUSE. Reports the actual port via the `onReady` callback so the
 * bridge can register it with the relay for web discovery.
 */
import http from "node:http";
import os from "node:os";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { readFileSync } from "node:fs";
import { hostname } from "node:os";

const run = promisify(execFile);

export interface BridgeStatus {
  bridgeId: string;
  hostname: string;
  platform: string;
  arch: string;
  uptime: number;
  tailscaleIp: string | null;
  tailscaleName: string | null;
  lanIp: string | null;
  servers: Array<{
    pid: number;
    name: string;
    port: number;
    role: string;
    ramMb: number;
    cpuPct: number;
    uptimeSec: number;
    cmdline: string;
  }>;
  harnesses: string[];
  ts: string;
}

async function detectTailscale(): Promise<{ ip: string | null; name: string | null }> {
  try {
    const { stdout } = await run("tailscale", ["ip", "-4"], { timeout: 2000 });
    const ip = stdout.trim().split("\n")[0] || null;
    let name = null;
    try {
      const { stdout: s } = await run("tailscale", ["status", "--json"], { timeout: 2000 });
      const d = JSON.parse(s);
      name = d.Self?.DNSName?.replace(/\.$/, "") || null;
    } catch {}
    return { ip, name };
  } catch {
    return { ip: null, name: null };
  }
}

async function getLanIp(): Promise<string | null> {
  const ifs = os.networkInterfaces();
  for (const [name, addrs] of Object.entries(ifs)) {
    if (name === "lo" || name.startsWith("docker") || name.startsWith("br-")) continue;
    for (const a of addrs ?? []) {
      if (a.family === "IPv4" && !a.internal && !a.address.startsWith("172.")) {
        return a.address;
      }
    }
  }
  return null;
}

async function scanServers(): Promise<BridgeStatus["servers"]> {
  // scan listening ports via ss, map to processes
  const out: BridgeStatus["servers"] = [];
  try {
    const { stdout } = await run("ss", ["-tlnpH"], { timeout: 3000 });
    for (const line of stdout.split("\n")) {
      const m = line.match(/(?:\d+\.\d+\.\d+\.\d+|\[::?\]):(\d+)\b.*pid=(\d+)/);
      if (!m) continue;
      const port = Number(m[1]);
      const pid = Number(m[2]);
      if (port < 1024 && port !== 22) continue;
      // get process info
      let name = "unknown", cmdline = "", ramMb = 0, uptimeSec = 0;
      try {
        const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
        const i = stat.lastIndexOf(")");
        name = stat.slice(0, i).split("(")[1] || "unknown";
        const rest = stat.slice(i + 2).split(" ");
        const startClk = Number(rest[19]) || 0;
        const btime = Number(readFileSync("/proc/stat", "utf8").match(/^btime (\d+)/m)?.[1] || 0);
        if (btime) uptimeSec = Math.max(0, Math.floor(Date.now() / 1000 - (btime + startClk / 100)));
        const rssPages = Number(readFileSync(`/proc/${pid}/statm`, "utf8").split(" ")[1]);
        ramMb = Math.round((rssPages * 4096) / 1048576);
        cmdline = readFileSync(`/proc/${pid}/cmdline`, "utf8").replace(/\0/g, " ").trim().slice(0, 120);
      } catch {}
      // simple role detection
      const c = cmdline.toLowerCase();
      const role = /vite|next-server|webpack/.test(c) ? "frontend"
        : /uvicorn|express|fastify|server\.(mjs|js|ts)|relay/.test(c) ? "backend"
        : /postgres|redis|mongod|supabase|kong/.test(c) ? "database"
        : "unknown";
      out.push({ pid, name, port, role, ramMb, cpuPct: 0, uptimeSec, cmdline });
    }
  } catch {}
  return out;
}

let _bridgeId: string;
let _ts: { ip: string | null; name: string | null } = { ip: null, name: null };
let _apiPort: number | null = null;

// Eagerly detect Tailscale IP so the bridge's poll header can include it without
// waiting for the first /api/status fetch. Resolves on module load; getApiInfo()
// returns whatever's available — the first poll may have a null IP but
// detectTailscale resolves within ~100ms so every subsequent poll is correct.
detectTailscale().then((ts) => { _ts = ts; }).catch(() => {});

/** Sync accessor for the bridge's API endpoint info — used by the poll header
 *  so the relay knows the bridge's Tailscale IP + API port for direct-proxy calls. */
export function getApiInfo(): { ip: string | null; port: number | null } {
  return { ip: _ts.ip, port: _apiPort };
}

export async function getStatus(bridgeId: string): Promise<BridgeStatus> {
  if (!_ts.ip && !_ts.name) _ts = await detectTailscale();
  const servers = await scanServers();
  const lanIp = await getLanIp();
  return {
    bridgeId,
    hostname: hostname(),
    platform: process.platform,
    arch: process.arch,
    uptime: Math.floor(process.uptime()),
    tailscaleIp: _ts.ip,
    tailscaleName: _ts.name,
    lanIp,
    servers,
    harnesses: [],  // filled by the bridge's harness detector
    ts: new Date().toISOString(),
  };
}

/** Read the body of a POST request (JSON). */
function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve) => {
    let body = "";
    req.on("data", (c) => { body += c; if (body.length > 1_000_000) req.destroy(); });
    req.on("end", () => resolve(body));
    req.on("error", () => resolve(""));
  });
}

/** Start the status + direct API server. onReady(actualPort) is called once
 *  listening so the bridge can report the API URL to the relay. */
export function startStatusServer(bridgeId: string, port: number, onReady?: (actualPort: number) => void): void {
  _bridgeId = bridgeId;
  const server = http.createServer(async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    if (req.method === "OPTIONS") { res.statusCode = 204; res.end(); return; }

    const url = new URL(req.url || "/", `http://localhost:${port}`);
    const path = url.pathname;

    try {
      // ── Direct history (the instant path — reads JSONL locally, no relay) ──
      if (path === "/api/history" && req.method === "GET") {
        const { readSessionHistory } = await import("./historyLoader.js");
        const session = url.searchParams.get("session") || "";
        const limit = Number(url.searchParams.get("limit")) || 30;
        if (!session) { res.statusCode = 400; res.setHeader("Content-Type", "application/json"); res.end(JSON.stringify({ error: "session required" })); return; }
        const { findBrokerSession } = await import("./brokerClient.js");
        let cwd: string | undefined;
        try { const bs = await findBrokerSession(session); if (bs?.cwd) cwd = bs.cwd; } catch {}
        const messages = await readSessionHistory(session, limit, cwd);
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ messages }));
        return;
      }

      // ── Direct send (broker delivery into the LIVE pi session) ──
      if (path === "/api/send" && req.method === "POST") {
        const body = JSON.parse(await readBody(req) || "{}");
        const { session, message } = body as { session: string; message: string };
        if (!session || !message) { res.statusCode = 400; res.setHeader("Content-Type", "application/json"); res.end(JSON.stringify({ error: "session + message required" })); return; }
        // Fire-and-forget: write to broker.sock and respond immediately. The
        // broker confirms delivery (or times out) in the background — but the
        // caller doesn't need to wait. The web polls /api/history for the
        // agent's reply; a slow broker confirmation would otherwise hold the
        // HTTP request open for 10s+ per message.
        const { sendViaBroker } = await import("./brokerClient.js");
        const sentAt = Date.now();
        setImmediate(() => { sendViaBroker(session, message).then((ok) => console.log(`  [send] broker: ${ok ? "delivered" : "failed/timeout"} "${session.slice(0,30)}"`)).catch(() => {}); });
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ delivered: true, sentAt }));
        return;
      }

      // ── Health + status (legacy federation) ──
      if (path === "/api/health") {
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ ok: true, bridgeId, ts: new Date().toISOString() }));
        return;
      }
      if (path === "/api/status") {
        try {
          const status = await getStatus(bridgeId);
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify(status));
        } catch (e) {
          res.statusCode = 500;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ error: e instanceof Error ? e.message : String(e) }));
        }
        return;
      }

      res.statusCode = 404;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ error: "not found" }));
    } catch (e: any) {
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ error: e?.message || String(e) }));
    }
  });

  // Auto-increment the port if taken — the API server is REQUIRED (the web
  // calls it directly), so we MUST find a free port and report it. Unlike the
  // old status-only server that could skip on EADDRINUSE, the direct API
  // is the primary path now.
  let tryPort = port;
  const tryListen = () => {
    server.listen(tryPort, "0.0.0.0", () => {
      _apiPort = tryPort;
      console.log(`  bridge API: http://0.0.0.0:${tryPort}/api/{history,send,status}`);
      onReady?.(tryPort);
    });
  };
  server.on("error", (e: NodeJS.ErrnoException) => {
    if (e.code === "EADDRINUSE" && tryPort < port + 20) {
      tryPort++; tryListen();
    } else {
      console.warn(`  bridge API: ${e.message}`);
    }
  });
  tryListen();
}