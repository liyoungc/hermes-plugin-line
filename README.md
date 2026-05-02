# Hermes LINE Plugin

A Hermes Agent gateway plugin for the **LINE Messaging API** (LINE Official Account / consumer LINE — `manager.line.biz`).

> Different from [Unayung/hermes-plugin-lineworks](https://github.com/Unayung/hermes-plugin-lineworks). That plugin targets **LINE WORKS** (Works Mobile, B2B). This one targets the consumer LINE Messaging API used by Official Accounts. The two services use different endpoints, signatures, auth, and message schemas — a single plugin cannot serve both.

## Features

- Inbound webhook at `/webhook/line` (configurable per account)
- `X-Line-Signature` HMAC-SHA256 verification
- **Multi-account** in one process — host e.g. a personal main account and a hospital account on the same gateway, each with its own credentials, webhook path, persona table, and authorization policy
- **Per-group persona routing** via `group_personas: {<bare_group_id>: <skill_name>}` → injected into `MessageEvent.auto_skill` for new sessions
- DM allow-list (`allow_from`) and group allow-list (`group_personas` keys when `group_policy: allowlist`)
- Inbound text, image, audio, sticker. Images/audio downloaded with Bearer auth and cached for vision/voice tools
- Outbound via **Push API only** (`/v2/bot/message/push`) — no reply-token reliance, so cron deliveries and long-running tool calls work the same as live replies
- Markdown stripped on outbound (LINE does not render markdown); messages auto-split at ~4900 chars on whitespace
- Best-effort sender enrichment via `/v2/bot/profile/{userId}` → injected as `channel_prompt` so the LLM knows who is talking
- Zero `line-bot-sdk` dependency — only `aiohttp` for HTTP and stdlib `hmac` for signature

## Out of scope

Reply API (we only push), Flex Messages, rich menus, video, file types, group management events, narrowcast/broadcast, LINE Login/LIFF.

## Install

```bash
hermes plugins install /absolute/path/to/hermes-plugin-line
hermes gateway restart
```

Or once published from GitHub:

```bash
hermes plugins install https://github.com/liyoungc/hermes-plugin-line.git
hermes gateway restart
```

For pip distribution:

```toml
[project.entry-points."hermes_agent.plugins"]
line = "line_platform"
```

## Config

### Single account (env-driven)

```bash
# .env
LINE_CHANNEL_ACCESS_TOKEN=...
LINE_CHANNEL_SECRET=...
LINE_PORT=18791
```

```yaml
# ~/.hermes/config.yaml
gateway:
  platforms:
    line:
      enabled: true
      extra:
        webhook_path: /webhook/line
        default_persona: cattia-line
        dm_policy: allowlist
        group_policy: open
        allow_from:
          - U2299043b96746a5d1f81b7d73fe1e770
        group_personas:
          Cd0e6c2529c0b4514361aec67da55871c: mochi-line
```

### Two accounts (main + Lynx) on one gateway

```yaml
gateway:
  platforms:
    line:
      enabled: true
      extra:
        host: 127.0.0.1
        port: 18791
        accounts:
          main:
            channel_access_token: U6UklOFV...
            channel_secret: 71f7b3ad...
            webhook_path: /webhook/line
            default_persona: cattia-line
            dm_policy: allowlist
            group_policy: open
            allow_from: [U2299043b96746a5d1f81b7d73fe1e770]
            group_personas:
              Cd0e6c2529c0b4514361aec67da55871c: mochi-line
          lynx:
            channel_access_token: jkGtF8dn...
            channel_secret: 787ef75e...
            webhook_path: /webhook/line/lynx
            default_persona: lynx-hospital
            dm_policy: pairing
            group_policy: allowlist
            group_personas:
              Cafa9730279fc00f7c65909a02084955e: lynx-hospital
              C77c9adadb82dc33e3e1e1bde200fe873: lynx-hospital
```

Cloudflare tunnel (one host, two paths — cleaner than two ports):

```yaml
ingress:
  - hostname: hermes-line.example.com
    service: http://localhost:18791
```

In LINE Developers Console:
- Main webhook URL: `https://hermes-line.example.com/webhook/line`
- Lynx webhook URL: `https://hermes-line.example.com/webhook/line/lynx`
- Disable LINE Official Account "Auto-reply" so the bot can answer.

## Authorization policies

| `dm_policy` | Behavior |
|---|---|
| `allowlist` *(default)* | Drop DMs from users not in `allow_from` |
| `open` | Accept all DMs |
| `pairing` | Accept all DMs; GatewayRunner enforces a pairing flow |

| `group_policy` | Behavior |
|---|---|
| `open` *(default)* | Accept all groups; persona = `group_personas.get(gid, default_persona)` |
| `allowlist` | Drop messages from groups whose ID is not a key of `group_personas` |

## Outbound to a specific account (cron / proactive push)

When the gateway needs to push to a specific account (e.g. Lynx-only morning brief), pass `metadata={"account_id": "lynx"}` to `send()`. Without that hint, multi-account sends use the first configured account.

## Tests

```bash
pip install pytest aiohttp
pytest tests/ -v
```

The tests stub out the `gateway.*` modules so they run without the full Hermes runtime.
