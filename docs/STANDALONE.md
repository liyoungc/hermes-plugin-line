# 獨立連接器（不需要 Hermes）

同一個 LINE 官方帳號進多個群組，每個群組有自己的知識庫、記憶、成員名單，
並且可以各自選擇由誰回答：直接呼叫模型（`llm`），或交給 Muse（`muse`）。

```
LINE ──webhook──▶ 連接器 ──▶ 訊息落地（SQLite）──▶ 身分核准 ──▶ 要不要回？
                                                                  │
                                              llm：呼叫 Claude ◀──┤
                                              muse：排進佇列 ◀────┘
Muse ──本機 API（127.0.0.1:18792）──▶ 讀歷史／領問題／送回覆
```

## 為什麼這樣設計

- **先落地再處理**：每則訊息在任何判斷之前先寫進資料庫。摘要、檢索都從這份紀錄算出來，重啟不會遺失。
- **身分只認 userId**：LINE 顯示名稱任何人都能改，所以只當「提示」顯示給管理者，不會成為身分。
- **群組嚴格隔離**：查詢、摘要、成員名單都以群組為界；A 群的提示詞裡不會出現 B 群的任何內容。
- **對外只開 webhook**：公開的通道只接受有 LINE 簽章的請求。Muse 用的介面只聽本機，另外要帶 token。
- **reply-first**：一分鐘內用 reply token 回覆不計費；逾時或被拒才改用計費的 Push。

## 安裝與啟動

```bash
python3 -m venv ~/line-connector/venv
~/line-connector/venv/bin/pip install "hermes-plugin-line[standalone] @ git+https://github.com/liyoungc/hermes-plugin-line"
cp examples/config.example.yaml ~/line-connector/config.yaml && chmod 600 ~/line-connector/config.yaml
```

`~/line-connector/.env`（權限 600，金鑰只放這裡）：

```
LINE_CHANNEL_ACCESS_TOKEN=
LINE_CHANNEL_SECRET=
ANTHROPIC_API_KEY=
LINE_CONNECTOR_API_TOKEN=     # 自己產生：openssl rand -hex 32
```

第一次設定時還不知道管理者的 LINE userId：先啟動服務，用自己的 LINE 加機器人好友並傳一句話，再執行
`python -m line_platform.standalone --config ~/line-connector/config.yaml --seen`，
列出的 `dm  U...` 就是你的 userId。填進 `admins` 後重啟。

systemd 範例見 `examples/line-connector.service`。資料庫在 `$LINE_CONNECTOR_HOME/line_platform.db`（權限 600）。

## LINE 後台檢查清單

- Messaging API 已啟用；Webhook URL 指到 `https://<你的網域>/webhook/line`，按 Verify 顯示成功
- Use webhook：開
- Allow bot to join group chats：開
- Auto-reply messages：關
- Greeting messages：關

## 把巧可加進新群組

1. 在 LINE 把官方帳號邀進群組。
2. 巧可私訊管理者一組設定碼（不含任何群組訊息內容）。
3. 管理者私訊回覆其中一種：
   - `/group add <設定碼> family`：接 LLM，用預設模型
   - `/group add <設定碼> family llm opus`：接 LLM 並指定模型
   - `/group add <設定碼> family muse <Side Chat 代號>`：接 Muse，並綁定一個 Side Chat
4. 成員第一次發言時，巧可私訊管理者核准身分。

設定前，該群組的訊息不會被記錄，巧可也不會回話。

## 管理指令（只在管理者與巧可的私訊有效）

| 指令 | 作用 |
|---|---|
| `/member pending` | 列出待核准成員 |
| `/member approve <碼> <姓名> \| <角色> \| <關係>` | 核准新成員 |
| `/member approve <碼>` | 此人已在別群登記過，沿用身分 |
| `/member deny <碼>` | 拒絕 |
| `/group list` | 各群組的模式、回答來源、人數 |
| `/group add <設定碼> <標籤> [llm [模型] \| muse [Side Chat]]` | 設定新群組 |
| `/group backend <標籤> llm\|muse` | 切換回答來源，立即生效 |
| `/group model <標籤> sonnet\|opus\|haiku\|<模型 ID>` | 指定這個群組用的模型 |
| `/group muse <標籤> <Side Chat 代號>` | 綁定 Muse Side Chat |
| `/group mode <標籤> shadow\|quiet\|active` | 只記錄／只回 @／正常回答 |
| `/group summary [標籤]` | 看目前的滾動摘要 |
| `/kb reload` | 重新載入知識庫與定案事實 |
| `/journal stats` | 各群組收發則數、尚未設定的群組 ID |

## 知識庫與定案事實

每個群組一個資料夾：`$LINE_CONNECTOR_HOME/groups/<帳號>/<標籤>/`

- `kb.md`：知識庫。開頭可寫 `kb_version`。檔案一改就會自動重讀。
- `pinned.md`：手動維護的定案事實。自動摘要不會覆寫它。

知識庫沒有的答案，巧可會說不知道並請對方問管理者。金額、訂位編號、證件號碼、聯絡方式不要放進知識庫；歷史對話裡出現的這類內容，在送進模型前會先被遮蔽。

## Muse 本機 API

全部要帶 `Authorization: Bearer <LINE_CONNECTOR_API_TOKEN>`。

| 方法與路徑 | 用途 |
|---|---|
| `GET /api/v1/groups` | 群組清單、回答來源、綁定的 Side Chat、成員 |
| `POST /api/v1/groups/bind` `{"group": "disney", "muse_chat": "<UUID>"}` | 登記群組綁定的 Side Chat（不會切換回答來源） |
| `GET /api/v1/messages?group=disney&day=2027-02-14` | 某一天的全部訊息 |
| `GET /api/v1/messages?group=disney&q=集合` | 關鍵字搜尋 |
| `GET /api/v1/messages?group=disney&after_seq=120` | 從某則之後增量讀取 |
| `GET /api/v1/summary?group=disney` | 滾動摘要 |
| `GET /api/v1/turns[?muse_chat=<代號>]` | 等 Muse 回答的問題（含提示詞與上下文） |
| `POST /api/v1/turns/{id}/claim` | 領取一題（避免重複回答） |
| `POST /api/v1/turns/{id}/reply` `{"text": "..."}` | 回覆到原群組 |
| `POST /api/v1/send` `{"group": "disney", "text": "..."}` | 主動發訊息 |

訊息預設會遮蔽敏感內容；加 `redact=0` 取得原文。
接 Muse 的群組，題目帶的回答規則和 LLM 群組不同：Muse 可以用綁定的 Side Chat 本身的記憶與資料回答，定案事實與知識庫是補充並在衝突時優先；不提供金額、訂位編號、證件號碼、聯絡方式的規則不變。所以綁定的 Side Chat 裡只放適合全群組成員知道的內容。

Muse 通常超過一分鐘才回覆，所以排進佇列時會先用免費的 reply token 回一句「收到，我查一下再回覆。」（設定檔 `muse.ack_text`，設成空字串可關閉），正式答案改用計費的 Push 送出。

## 測試

```bash
pip install -e ".[test]" && pytest tests -q
```
