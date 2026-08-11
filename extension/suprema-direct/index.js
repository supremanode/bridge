// suprema-direct — standalone pi extension: lets the Suprema web app send
// messages into a LIVE pi session as the user's own message.
//
// PLAIN JAVASCRIPT, ESM SYNTAX (import + export default) — matches every
// working pi extension (e.g. pi-network). Two prior failure modes that this
// fixes:
//   1. A .ts version with `import type { ExtensionAPI } from "@earendil-works/
//      pi-coding-agent"` — jiti's transform didn't strip the type-only import,
//      silently aborting handler registration.
//   2. A CJS version with `module.exports = function` — pi calls
//      `jiti.import(path, { default: true })` then checks `typeof factory ===
//      "function"`; CJS module.exports returns a namespace object, not the
//      function, so the factory was never called (extension listed, did nothing).
// ESM `export default` is what the loader actually unwraps.
//
// ISOLATED from pi-network: no imports from pi-network, no shared state. Only
// shared surface is the OS-level broker.sock; we open our own connection and
// register under a suffixed name (`<session>#suprema-direct`) that never
// collides with pi-network's bare-name registration.
//
// API surface: `pi` (factory arg) has getSessionName/sendUserMessage/on.
// `ctx` (2nd arg to each handler) has hasUI/ui.notify/cwd — pi.ui is undefined.
//
// Diagnostics: every step logs to /tmp/suprema-direct.log so we can read the
// truth after /reload regardless of TUI toast rendering.
import { createConnection } from "node:net";
import { existsSync, appendFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";

const BROKER_SOCK = process.env.PI_BROKER_SOCK || join(homedir(), ".pi/agent/intercom/broker.sock");
const DIRECT_SUFFIX = "#suprema-direct";
// Control sentinel: when the web presses Stop/ESC the bridge sends this exact
// text (not a real message). We map it to ctx.abort() — the ESC-in-pi interrupt.
// ponytail: string sentinel over a new broker message type — the text channel
// already works end-to-end; keep this string identical in the bridge.
const INTERRUPT_SENTINEL = "__SUPREMA_DIRECT_INTERRUPT__";
const LOG_PATH = join(tmpdir(), "suprema-direct.log");

function log(msg) {
  try { appendFileSync(LOG_PATH, `${new Date().toISOString()} ${msg}\n`); } catch { /* best-effort */ }
}

function writeMsg(sock, obj) {
  const json = JSON.stringify(obj);
  const len = Buffer.byteLength(json);
  const hdr = Buffer.allocUnsafe(4);
  hdr.writeUInt32BE(len, 0);
  sock.write(Buffer.concat([hdr, Buffer.from(json, "utf8")]));
}

export default function (pi) {
  // NOTE: only REGISTRATION methods (pi.on, pi.registerFlag, pi.registerTool)
  // may be called in the factory body. ACTION methods (getSessionName,
  // sendUserMessage, appendEntry, …) are bound to a `notInitialized` placeholder
  // until the runtime is bound AFTER factory loading — calling one here throws
  // "Extension runtime not initialized" and fails the whole extension load.
  // Defer all action calls to inside the handlers (where `ctx` is passed).
  console.error("[suprema-direct] FACTORY ENTRY");
  log(`factory invoked; typeof pi=${typeof pi} hasOn=${typeof pi?.on}`);
  let socket = null;
  let inboundBuf = Buffer.alloc(0);

  function disconnect() {
    try { socket && socket.destroy(); } catch {}
    socket = null;
  }

  function connect(ctx) {
    if (socket) return;
    const hasUI = ctx && ctx.hasUI;
    // getSessionName is an action method — safe here because connect() runs from
    // the session_start handler (runtime bound by then), NOT from the factory.
    const bareName = (pi && pi.getSessionName && pi.getSessionName()) || `suprema-receiver-${process.pid}`;
    log(`connect: hasUI=${hasUI} brokerSock=${existsSync(BROKER_SOCK)} name=${bareName}`);
    if (!hasUI) { ctx && ctx.ui && ctx.ui.notify && ctx.ui.notify("suprema-direct: no UI (headless) — skipping", "info"); return; }
    if (!existsSync(BROKER_SOCK)) { ctx && ctx.ui && ctx.ui.notify && ctx.ui.notify("suprema-direct: no broker.sock — skipping", "info"); return; }

    const sessionName = bareName + DIRECT_SUFFIX;
    try {
      socket = createConnection(BROKER_SOCK);
    } catch (e) {
      const em = e && e.message ? e.message : String(e);
      log(`connect: createConnection threw: ${em}`);
      ctx && ctx.ui && ctx.ui.notify && ctx.ui.notify(`suprema-direct: broker connect failed: ${em}`, "error");
      return;
    }
    socket.on("connect", () => {
      writeMsg(socket, {
        type: "register",
        session: { name: sessionName, pid: process.pid, cwd: ctx && ctx.cwd, model: "suprema-receiver", status: "online", startedAt: Date.now(), lastActivity: Date.now() },
      });
      log(`connect: registered "${sessionName}" cwd=${ctx && ctx.cwd}`);
      ctx && ctx.ui && ctx.ui.notify && ctx.ui.notify(`suprema-direct: registered "${sessionName}"`, "info");
    });
    socket.on("data", (chunk) => {
      inboundBuf = Buffer.concat([inboundBuf, chunk]);
      while (inboundBuf.length >= 4) {
        const len = inboundBuf.readUInt32BE(0);
        if (inboundBuf.length < 4 + len) break;
        const json = inboundBuf.subarray(4, 4 + len).toString("utf8");
        inboundBuf = inboundBuf.subarray(4 + len);
        let msg;
        try { msg = JSON.parse(json); } catch { continue; }
        // The broker forwards to the TARGET as {type:"message", from:<senderInfo>,
        // message:<BrokerMessage>} — "send" is only the sender→broker type. We
        // previously matched "send" here, so every delivery was silently dropped
        // while the broker acked delivered=true to the bridge.
        if (msg.type === "message" && msg.message && msg.message.content && msg.message.content.text) {
          const fromName = (msg.from && (msg.from.name || msg.from.id)) || "";
          log(`recv message from="${fromName}" text="${String(msg.message.content.text).slice(0, 60)}"`);
          if (typeof fromName === "string" && fromName.startsWith("suprema-bridge")) {
            const text = String(msg.message.content.text);
            if (text === INTERRUPT_SENTINEL) {
              // ESC-equivalent: the user pressed Stop in the web. Abort the
              // current agent turn exactly like pressing ESC in the pi TUI.
              log("recv INTERRUPT -> ctx.abort()");
              try { ctx && ctx.abort && ctx.abort(); ctx && ctx.ui && ctx.ui.notify && ctx.ui.notify("suprema-direct: interrupted by web", "info"); }
              catch (e) { log(`abort threw: ${e && e.message ? e.message : e}`); }
              continue;
            }
            try { pi.sendUserMessage(text, { deliverAs: "steer" }); }
            catch (e) {
              const em = e && e.message ? e.message : String(e);
              log(`sendUserMessage threw: ${em}`);
              ctx && ctx.ui && ctx.ui.notify && ctx.ui.notify(`suprema-direct: delivery failed: ${em}`, "error");
            }
          } else {
            ctx && ctx.ui && ctx.ui.notify && ctx.ui.notify(`suprema-direct: ignored send from "${fromName}"`, "info");
          }
        }
      }
    });
    socket.on("error", (e) => { log(`socket error: ${e && e.message ? e.message : e}`); try { socket && socket.destroy(); } catch {} socket = null; });
    socket.on("close", () => { log("socket closed"); socket = null; });
  }

  pi.on("session_start", (e, ctx) => {
    log(`session_start reason=${e && e.reason} hasUI=${ctx && ctx.hasUI}`);
    ctx && ctx.ui && ctx.ui.notify && ctx.ui.notify(`suprema-direct: session_start (reason=${(e && e.reason) || "?"})`, "info");
    disconnect();
    connect(ctx);
  });
  pi.on("session_shutdown", (e, ctx) => {
    log(`session_shutdown reason=${e && e.reason}`);
    ctx && ctx.ui && ctx.ui.notify && ctx.ui.notify(`suprema-direct: session_shutdown (reason=${(e && e.reason) || "?"})`, "info");
    disconnect();
  });
  log("factory handlers registered");
}