/**
 * pi-network broker client — delivers messages to LIVE pi sessions via the
 * local broker Unix socket. Vendored from pi-network-dashboard/server/broker-sender.ts.
 *
 * This is the instant, full-memory delivery path. The pi-network extension
 * running INSIDE each live pi process listens on broker.sock; when we send a
 * task_route envelope, the extension calls pi.sendUserMessage() — injecting the
 * message straight into the running agent. No cold-resume of a 22MB session file.
 *
 * Wire protocol: length-prefixed JSON (4-byte big-endian length + UTF-8 payload).
 * Socket: ~/.pi/agent/intercom/broker.sock (Unix domain socket).
 */
import { createConnection } from "node:net";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";

const BROKER_SOCK = process.env.PI_BROKER_SOCK || join(homedir(), ".pi/agent/intercom/broker.sock");

function writeMessage(sock: any, obj: any): void {
  const json = JSON.stringify(obj);
  const buf = Buffer.allocUnsafe(4 + Buffer.byteLength(json));
  buf.writeUInt32BE(Buffer.byteLength(json), 0);
  buf.write(json, 4, "utf8");
  sock.write(buf);
}

interface BrokerSession { name: string; id: string; pid?: number; cwd?: string; status?: string; model?: string; }

class BrokerClient extends EventEmitter {
  private socket: any = null;
  private buffer = Buffer.alloc(0);
  private connecting: Promise<void> | null = null;
  private disconnecting = false;

  isConnected(): boolean { return !!this.socket && !this.disconnecting; }

  /** Connect + register as a dashboard sender. */
  async connect(): Promise<void> {
    if (this.isConnected()) return;
    if (this.connecting) return this.connecting;
    this.connecting = new Promise<void>((resolve, reject) => {
      if (!existsSync(BROKER_SOCK)) { reject(new Error("broker.sock not found")); return; }
      const sock = createConnection(BROKER_SOCK);
      const timer = setTimeout(() => { sock.destroy(); reject(new Error("connect timeout")); }, 5000);
      sock.on("connect", () => {
        clearTimeout(timer);
        this.socket = sock;
        this.disconnecting = false;
        writeMessage(sock, {
          type: "register",
          session: {
            // Unique per-process name: every bridge restart used the same
            // "suprema-bridge" name, accumulating stale broker entries →
            // "Multiple sessions named suprema-bridge" broke pi-network mesh
            // replies. A pid-suffixed name never collides + the pid makes stale
            // entries debuggable. The bridge doesn't need a stable discoverable
            // name — the web routes by the TARGET session's name (e.g. "Jobhunt
            // Extension"), not the bridge's own name.
            name: `suprema-bridge-${process.pid}`,
            cwd: "/tmp",
            model: "bridge",
            pid: process.pid,
            startedAt: Date.now(),
            lastActivity: Date.now(),
            status: "bridge",
          },
        });
        resolve();
      });
      sock.on("data", (chunk: Buffer) => this.handleData(chunk));
      sock.on("error", () => this.cleanup());
      sock.on("close", () => { this.emit("disconnected"); this.cleanup(); });
    });
    try { await this.connecting; } finally { this.connecting = null; }
  }

  private cleanup(): void { this.socket = null; this.buffer = Buffer.alloc(0); this.disconnecting = false; }

  private handleData(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.buffer.length >= 4) {
      const len = this.buffer.readUInt32BE(0);
      if (this.buffer.length < 4 + len) break;
      const json = this.buffer.subarray(4, 4 + len).toString("utf8");
      this.buffer = this.buffer.subarray(4 + len);
      let msg: any;
      try { msg = JSON.parse(json); } catch { continue; }
      if (msg.type === "sessions") this.emit("_list", msg);
      else if (msg.type === "delivered") this.emit("_deliver", msg);
      else if (msg.type === "delivery_failed") this.emit("_deliver", { ...msg, delivered: false });
      else if (msg.type === "result") this.emit("_result", msg);  // agent reply text
      else if (msg.type === "message") this.emit("_message", msg);
    }
  }

  /** List all sessions the broker knows about (live pi processes). */
  async listSessions(): Promise<BrokerSession[]> {
    if (!this.isConnected()) return [];
    return new Promise((resolve) => {
      const requestId = randomUUID();
      const timer = setTimeout(() => { this.off("_list", handler); resolve([]); }, 5000);
      const handler = (msg: any) => {
        if (msg.requestId === requestId) {
          clearTimeout(timer);
          this.off("_list", handler);
          resolve(msg.sessions || []);
        }
      };
      this.on("_list", handler);
      writeMessage(this.socket, { type: "list", requestId });
    });
  }

  /**
   * Send a plain message to the suprema-direct receiver registered for a named
   * pi session. The receiving pi process's suprema-direct extension calls
   * pi.sendUserMessage() — triggering the live agent with full memory.
   *
   * Target resolution is name-only, against `${sessionName}#suprema-direct` —
   * NOT pid. The pi-network extension registers the SAME bare session name
   * under the SAME pid (both extensions run inside one pi process), so a
   * pid-based lookup can't tell the two registrations apart and risks routing
   * our message to pi-network's entry instead (wrong framing, mesh reply
   * format). The suffix is unique to suprema-direct, so exact-name match is
   * unambiguous — if it's not found, no suprema-direct receiver is live here.
   */
  async sendToSession(sessionName: string, message: string): Promise<boolean> {
    if (!this.isConnected()) return false;
    const suffixed = `${sessionName}#suprema-direct`;
    let target: string | undefined;
    try {
      const sessions = await this.listSessions();
      target = sessions.find((s) => s.name === suffixed)?.name
        ?? sessions.find((s) => s.name?.toLowerCase() === suffixed.toLowerCase())?.name;
    } catch { /* best-effort */ }
    if (!target) return false; // no live suprema-direct receiver for this session

    const taskId = randomUUID();
    return new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => { this.off("_deliver", handler); resolve(false); }, 10_000);
      const handler = (msg: any) => {
        if (msg.messageId === taskId || msg.taskId === taskId) {
          clearTimeout(timer);
          this.off("_deliver", handler);
          resolve(msg.delivered !== false);
        }
      };
      this.on("_deliver", handler);
      // Plain user message — NOT a task_route envelope. The pi-network extension
      // parses content.text: if it's JSON with type:"task_route" it injects heavy
      // task framing ("Complete the task and return results") which makes the
      // agent reply via the mesh instead of just answering. Plain text lands in
      // the extension's deliverInboundMessage → pi.sendMessage(triggerTurn:true).
      // expectsReply:true triggers an agent turn so the message is processed.
      // The reply goes to the session JSONL → the web's pollDirectReply finds it.
      writeMessage(this.socket, {
        type: "send",
        to: target,
        message: {
          id: taskId,
          timestamp: Date.now(),
          expectsReply: true,
          content: { text: message },
        },
      });
    });
  }

  async disconnect(): Promise<void> {
    if (!this.socket) return;
    this.disconnecting = true;
    try { writeMessage(this.socket, { type: "unregister" }); this.socket.end(); } catch { this.socket?.destroy(); }
    this.cleanup();
  }
}

// Singleton — one persistent broker connection per bridge process.
let sharedClient: BrokerClient | null = null;
let connectingPromise: Promise<BrokerClient> | null = null;

async function getBroker(): Promise<BrokerClient> {
  if (sharedClient?.isConnected()) return sharedClient;
  if (connectingPromise) return connectingPromise;
  connectingPromise = (async () => {
    const c = new BrokerClient();
    await c.connect();
    sharedClient = c;
    connectingPromise = null;
    return c;
  })();
  try { return await connectingPromise; }
  finally { connectingPromise = null; }
}

/** Send a message to a live named pi session via the broker. Returns true if delivered. */
export async function sendViaBroker(sessionName: string, message: string): Promise<boolean> {
  try {
    const client = await getBroker();
    return await client.sendToSession(sessionName, message);
  } catch {
    try { await sharedClient?.disconnect(); } catch {}
    sharedClient = null;
    connectingPromise = null;
    return false;
  }
}

/** Find the broker entry for a named session (by name, or pid if given).
 *  Returns its cwd + pid — used to resolve the JSONL on disk without scanning
 *  every recent file. The broker is authoritative for which process owns a name. */
export async function findBrokerSession(name: string, pid?: number): Promise<BrokerSession | undefined> {
  try {
    const sessions = await listBrokerSessions();
    return sessions.find((s) => (pid ? s.pid === pid : s.name === name)) ||
           sessions.find((s) => s.name?.toLowerCase() === name.toLowerCase());
  } catch { return undefined; }
}

// ponytail: every pi send probed the broker (5s connect + 5s list timeout when
// broker.sock exists but nothing's listening) before falling back to headless —
// paid per message. Cache the list briefly so a burst of sends only pays once.
let sessionsCache: { ts: number; list: BrokerSession[] } | null = null;
const SESSIONS_CACHE_MS = 8_000;

/** List live sessions known to the broker (for discovery + name resolution). */
export async function listBrokerSessions(): Promise<BrokerSession[]> {
  if (sessionsCache && Date.now() - sessionsCache.ts < SESSIONS_CACHE_MS) return sessionsCache.list;
  try {
    const client = await getBroker();
    const list = await client.listSessions();
    sessionsCache = { ts: Date.now(), list };
    return list;
  } catch {
    try { await sharedClient?.disconnect(); } catch {}
    sharedClient = null;
    sessionsCache = null;
    return [];
  }
}

/** Check if the broker socket exists (quick liveness check). */
export function brokerAvailable(): boolean { return existsSync(BROKER_SOCK); }

export async function closeBroker(): Promise<void> {
  try { await sharedClient?.disconnect(); } catch {}
  sharedClient = null;
  connectingPromise = null;
}
