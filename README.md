# @supremanode/bridge

The thin client that connects your machine's coding agents (pi, Claude Code, Codex, Cursor) to the [Suprema Node](https://supremanode.com) relay.

Suprema Node is a mobile-first control plane for AI coding agents. The bridge runs on your dev machine, discovers your local agent harnesses, and relays commands + answers between your phone and your agents. This repo is the **open** piece of an open-core product — the relay (smart routing, billing) is closed.

## What it does

- **Live-harness bridge** — runs a coding task on your machine via your installed agent (pi, Claude Code, etc.), streams the answer back to the Suprema app.
- **Interactive sessions** — `--resume` a running named session; deliver messages to a live agent process.
- **Fleet management** — register one bridge per machine; the relay tracks devices + routes tasks by priority.
- **Extension** — optional browser extension pairs with the relay for inline messaging.

## Install

```bash
curl -fsSL https://supremanode.com/install.sh | sh
```

Or from source:

```bash
git clone https://github.com/supremanode/bridge.git
cd bridge
pnpm install
RELAY_URL=https://relay.supremanode.com BRIDGE_ID=my-laptop pnpm start
```

## Requirements

- Node.js 22+
- A coding agent harness installed locally (pi, Claude Code, Codex, or Cursor)
- A Suprema Node account (the relay validates your bridge token)

## How it works

```
Your phone (Suprema app)
       │
       ▼
   Suprema Relay  ◄──── smart routing, metering, billing (closed source)
       │
       ▼
   This bridge  ──►  your local agent (pi / claude / codex)
                          │
                          ▼
                      your codebase
```

The bridge is deliberately thin: it discovers harnesses, forwards prompts, and returns answers. No business logic lives here — that's the relay's job. Read the source; it's small.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `RELAY_URL` | `http://localhost:4000` | Relay endpoint |
| `BRIDGE_ID` | hostname | Friendly device name shown in the app |
| `BRIDGE_TOKEN` | — | Auth token from your Suprema account |

## License

[Apache License 2.0](./LICENSE) — © 2026 Suprema Node.

The patent grant matters: you can use, modify, and distribute this code, and any patent claims the contributors hold that cover it are licensed to you. See §3 of the License.

## Security

Found a vulnerability? Email **security@supremanode.com** — do not open a public issue. See [SECURITY.md](./SECURITY.md) (if present) or the responsible-disclosure policy on supremanode.com/security.
