"""LINE Messaging API HTTP client, signature check and text helpers."""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import re
from typing import Any, Optional

from .config import LineAccount

logger = logging.getLogger(__name__)

LINE_API_BASE = "https://api.line.me"
LINE_DATA_API_BASE = "https://api-data.line.me"
LINE_SIGNATURE_HEADER = "X-Line-Signature"
TEXT_CHUNK_LIMIT = 4900
# One reply/push call carries at most five message objects.
MAX_MESSAGES_PER_CALL = 5


class LineApiError(RuntimeError):
    def __init__(self, operation: str, status: int):
        super().__init__(f"LINE {operation} failed: {status}")
        self.operation = operation
        self.status = status


def verify_signature(raw_body: bytes, signature_header: Optional[str], channel_secret: str) -> bool:
    """X-Line-Signature = base64( HMAC-SHA256(channel_secret, raw_body) )."""
    if not signature_header or not channel_secret:
        return False
    expected = hmac.new(channel_secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    try:
        got = base64.b64decode(signature_header, validate=True)
    except Exception:
        return False
    return len(got) == len(expected) and hmac.compare_digest(got, expected)


_MARKDOWN_PATTERNS = (
    (re.compile(r"\*\*(.+?)\*\*", re.DOTALL), r"\1"),  # **bold**
    (re.compile(r"(?<!\*)\*([^*\n]+)\*(?!\*)"), r"\1"),  # *italic*
    (re.compile(r"`([^`\n]+)`"), r"\1"),  # `code`
)


def strip_markdown(text: str) -> str:
    out = text or ""
    for pat, repl in _MARKDOWN_PATTERNS:
        out = pat.sub(repl, out)
    return out


def split_text(text: str, max_len: int) -> list[str]:
    """Whitespace-aware splitter — never breaks mid-character.

    LINE caps a single text message at 5000 chars; we use 4900 as headroom.
    """
    if not text:
        return []
    if len(text) <= max_len:
        return [text]
    chunks: list[str] = []
    remaining = text
    while len(remaining) > max_len:
        cut = remaining.rfind("\n", 0, max_len)
        if cut < max_len // 2:
            cut = remaining.rfind(" ", 0, max_len)
        if cut < max_len // 2:
            cut = max_len
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def describe_message(message: dict[str, Any]) -> tuple[str, str, Optional[str]]:
    """Return ``(text, msg_type, content_message_id)`` for a LINE message
    object. ``msg_type`` is LINE's own type string. Unsupported types give
    empty text; the journal still records that they happened."""
    mtype = str(message.get("type") or "")
    message_id = message.get("id")
    if mtype == "text":
        return str(message.get("text") or ""), "text", None
    if mtype == "image":
        return "", "image", str(message_id) if message_id else None
    if mtype == "audio":
        return "", "audio", str(message_id) if message_id else None
    if mtype == "sticker":
        pkg = message.get("packageId") or "?"
        sid = message.get("stickerId") or "?"
        return f"[貼圖: {pkg}/{sid}]", "sticker", None
    return "", mtype or "unknown", None


def is_self_mentioned(message: dict[str, Any]) -> bool:
    mention = message.get("mention")
    if not isinstance(mention, dict):
        return False
    for mentionee in mention.get("mentionees") or []:
        if isinstance(mentionee, dict) and mentionee.get("isSelf") is True:
            return True
    return False


class LineClient:
    """One per account. ``session`` is an ``aiohttp.ClientSession``."""

    def __init__(self, session: Any, account: LineAccount):
        self.session = session
        self.account = account

    @property
    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.account.channel_access_token}"}

    async def _post_messages(self, operation: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        url = f"{LINE_API_BASE}/v2/bot/message/{operation}"
        async with self.session.post(
            url, json=payload, headers={**self._auth_headers, "Content-Type": "application/json"}
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.warning("[line_platform] %s failed status=%s body=%s", operation, resp.status, body[:300])
                raise LineApiError(operation, resp.status)
            try:
                data = await resp.json()
            except Exception:
                return []
        sent = data.get("sentMessages") if isinstance(data, dict) else None
        return [m for m in sent if isinstance(m, dict)] if isinstance(sent, list) else []

    async def push_message(self, to: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return await self._post_messages("push", {"to": to, "messages": messages})

    async def reply_message(self, reply_token: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return await self._post_messages("reply", {"replyToken": reply_token, "messages": messages})

    async def download_content(self, message_id: str) -> bytes:
        url = f"{LINE_DATA_API_BASE}/v2/bot/message/{message_id}/content"
        async with self.session.get(url, headers=self._auth_headers) as resp:
            if resp.status != 200:
                logger.warning("[line_platform] content download failed status=%s", resp.status)
                raise LineApiError("content", resp.status)
            return await resp.read()

    async def _get_json(self, path: str) -> Optional[dict[str, Any]]:
        try:
            async with self.session.get(f"{LINE_API_BASE}{path}", headers=self._auth_headers) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return data if isinstance(data, dict) else None
        except Exception as exc:
            logger.debug("[line_platform] GET %s failed: %s", path.split("/")[3] if path.count("/") > 3 else path, exc)
            return None

    async def get_user_profile(self, user_id: str) -> Optional[dict[str, Any]]:
        return await self._get_json(f"/v2/bot/profile/{user_id}")

    async def get_group_member_profile(self, group_id: str, user_id: str) -> Optional[dict[str, Any]]:
        """Works for group members who have not added the bot as a friend."""
        kind = "room" if group_id.startswith("R") else "group"
        return await self._get_json(f"/v2/bot/{kind}/{group_id}/member/{user_id}")

    async def get_bot_info(self) -> Optional[dict[str, Any]]:
        return await self._get_json("/v2/bot/info")
