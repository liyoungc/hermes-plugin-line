# Hermes LINE Plugin

Hermes Agent 的 gateway plugin，用來接 **LINE Messaging API**（LINE Official Account / 消費版 LINE，後台在 `manager.line.biz`）。

> 跟 [Unayung/hermes-plugin-lineworks](https://github.com/Unayung/hermes-plugin-lineworks) 不一樣 — 那個是給 **LINE WORKS**（Works Mobile，B2B 企業協作版）用的。這個 plugin 是給 LINE 官方帳號用的。兩邊的 endpoint、簽章 header、認證方式、訊息 schema 完全不同，**一個 plugin 不可能同時服務兩種**。

## 特色 / Features

- 在 `/webhook/line` 收 webhook（每個 account 可自訂 path）
- `X-Line-Signature` HMAC-SHA256 驗章
- **Multi-account 同進程運作** — 同一台 gateway 同時跑多個 LINE Official Account，各自有獨立的 credentials、webhook path、persona 路由表、authorization policy
- **每個群組獨立 persona 路由** 透過 `group_personas: {<bare_group_id>: <skill_name>}` → 注入 `MessageEvent.auto_skill`，在新 session 時生效
- DM allow-list (`allow_from`) 與 group allow-list（`group_policy: allowlist` 時，`group_personas` 的 keys 即白名單）
- 支援收進來的訊息類型：text / image / audio / sticker。圖片和語音會用 Bearer auth 下載並 cache，給 vision / voice tool 看
- Outbound 一律走 **Push API** (`/v2/bot/message/push`)：不依賴 reply token，所以 cron 推送、長 tool 計算後再回覆都跟即時回話一樣可行
- Outbound 自動 strip markdown（LINE 不渲染 markdown），長訊息以空白為界自動切成 ~4900 字一段
- Best-effort sender enrichment：透過 `/v2/bot/profile/{userId}` 拉 displayName，塞進 `MessageEvent.channel_prompt`，讓 LLM 知道現在是誰在傳訊
- **零 `line-bot-sdk` 依賴** — 只用 `aiohttp` 跑 HTTP server，搭配 stdlib `hmac` 算簽章

## 不在範圍內 / Out of scope

Reply API（我們只 push）、Flex Messages、Rich Menu、video、file 類型訊息、群組管理事件、narrowcast / broadcast、LINE Login / LIFF。

## 安裝 / Install

```bash
hermes plugins install /absolute/path/to/hermes-plugin-line
hermes gateway restart
```

或直接從 GitHub：

```bash
hermes plugins install https://github.com/liyoungc/hermes-plugin-line.git
hermes gateway restart
```

如果走 pip 散佈，`pyproject.toml` 已經宣告好 entry-point：

```toml
[project.entry-points."hermes_agent.plugins"]
line = "line_platform"
```

## 設定 / Config

### 單帳號（環境變數驅動）

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
        default_persona: my-persona
        dm_policy: allowlist
        group_policy: open
        allow_from:
          - Uxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx   # 你自己的 LINE userId
        group_personas:
          Cxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx: family-persona
```

### 多帳號（同一 gateway 跑兩個以上的 LINE Official Account）

```yaml
gateway:
  platforms:
    line:
      enabled: true
      extra:
        host: 127.0.0.1
        port: 18791
        accounts:
          account1:
            channel_access_token: <token-1>
            channel_secret: <secret-1>
            webhook_path: /webhook/line/account1
            default_persona: account1-persona
            dm_policy: allowlist
            group_policy: open
            allow_from: [Uxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx]
            group_personas:
              Cxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx: account1-group-persona
          account2:
            channel_access_token: <token-2>
            channel_secret: <secret-2>
            webhook_path: /webhook/line/account2
            default_persona: account2-persona
            dm_policy: pairing
            group_policy: allowlist
            group_personas:
              Cyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy: account2-group-persona
              Czzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz: account2-group-persona
```

Cloudflare tunnel（一個 host 跑兩個 path 比兩個 port 乾淨）：

```yaml
ingress:
  - hostname: hermes-line.example.com
    service: http://localhost:18791
```

到 LINE Developers Console 設定（每個 channel 一個 webhook URL）：
- account1 webhook URL：`https://hermes-line.example.com/webhook/line/account1`
- account2 webhook URL：`https://hermes-line.example.com/webhook/line/account2`
- 把每個 LINE Official Account 的 **Auto-reply messages** 關掉，bot 才能正常回話

## 授權策略 / Authorization policies

| `dm_policy` | 行為 |
|---|---|
| `allowlist`（預設） | 不在 `allow_from` 裡的使用者 DM 直接 drop |
| `open` | 任何人 DM 都接受 |
| `pairing` | 全部接受；由 GatewayRunner 跑 pairing flow |

| `group_policy` | 行為 |
|---|---|
| `open`（預設） | 全部群組都接受；persona 走 `group_personas.get(gid, default_persona)` |
| `allowlist` | 群組 ID 不在 `group_personas` keys 裡的訊息直接 drop |

## 指定帳號發送（cron / proactive push）

當 gateway 要推訊息給特定 account（例如只給 `account2` 推某個早班 brief），呼叫 `send()` 時帶 `metadata={"account_id": "account2"}`。沒帶這個 hint 的話，多帳號設定下預設用第一個 account。

## 測試 / Tests

```bash
pip install pytest aiohttp
pytest tests/ -v
```

Test 會 stub 掉 `gateway.*` modules，不需要完整 Hermes runtime 就能跑（16/16 passing）。

## 注意事項 / Heads-up

如果你的 Hermes 已經把這個 plugin 對應的 LINE adapter merge 進 core（`gateway/platforms/line.py` 已存在），核心會搶先註冊 `Platform.LINE`，這個 plugin 會被當 no-op。這種情況下 plugin 留著也沒事，但實質上是核心 adapter 在工作。要驗證是哪一邊在跑，看 gateway log 開頭是 `[line]`（核心）還是 `[line_platform]`（plugin）。

## License

MIT
