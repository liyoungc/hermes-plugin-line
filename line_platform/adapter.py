"""LINE Messaging API platform adapter for Hermes Agent.

Inbound: webhook from LINE Platform — verified by X-Line-Signature
(HMAC-SHA256(channel_secret, body) → base64), parsed into Hermes
``MessageEvent`` and dispatched via ``handle_message``.

Outbound: Push API (``/v2/bot/message/push``) — no reply_token usage,
so any chunked / deferred / cron-driven reply works the same way.

Multi-account: a single adapter instance can host several LINE Official
Accounts. Each account holds its own credentials, webhook path, and
persona routing table; events are dispatched via the matched account's
client. This lets one Hermes deployment serve e.g. a personal Cattia
account and a hospital "Lynx" account at the same time.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Dict, Iterable, Optional

try:
    import aiohttp
    from aiohttp import web

    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised by check_requirements only
    aiohttp = None  # type: ignore[assignment]
    web = None  # type: ignore[assignment]
    AIOHTTP_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_audio_from_bytes,
    cache_image_from_bytes,
)

logger = logging.getLogger(__name__)

LINE_API_BASE = "https://api.line.me"
LINE_DATA_API_BASE = "https://api-data.line.me"
LINE_SIGNATURE_HEADER = "X-Line-Signature"
DEFAULT_WEBHOOK_PATH = "/webhook/line"
DEFAULT_PORT = 18791
TEXT_CHUNK_LIMIT = 4900


# ---------------------------------------------------------------------------
# Account dataclass
# ---------------------------------------------------------------------------


@dataclass
class LineAccount:
    """One LINE Official Account's runtime config."""

    account_id: str = "default"
    enabled: bool = True
    channel_access_token: str = ""
    channel_secret: str = ""
    webhook_path: str = DEFAULT_WEBHOOK_PATH
    # Persona routing: bare LINE group IDs → skill name. DMs and unmatched
    # groups fall back to ``default_persona``. ``auto_skill`` is consumed by
    # GatewayRunner only on new sessions.
    group_personas: dict[str, str] = field(default_factory=dict)
    default_persona: Optional[str] = None
    # Authorization. ``allow_from`` is the DM allow-list (bare LINE user IDs).
    # ``group_personas`` keys double as the group allow-list when
    # ``group_policy == "allowlist"``.
    allow_from: list[str] = field(default_factory=list)
    dm_policy: str = "allowlist"  # "allowlist" | "open" | "pairing"
    group_policy: str = "open"  # "open" | "allowlist"

    @property
    def has_credentials(self) -> bool:
        return bool(self.channel_access_token and self.channel_secret)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def _config_or_env(extra: dict[str, Any], keys: tuple[str, ...], env_key: str, default: str = "") -> str:
    for key in keys:
        value = extra.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    env = os.getenv(env_key, "")
    return env.strip() if env else default


def _normalize_list(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [part.strip() for part in raw.split(",") if part.strip()]
    if isinstance(raw, Iterable):
        return [str(item).strip() for item in raw if str(item).strip()]
    return []


def _normalize_dict(raw: Any) -> dict[str, str]:
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items() if str(k).strip()}
    return {}


def _account_from_extra(extra: dict[str, Any], account_id: str = "default") -> LineAccount:
    """Build a LineAccount from one ``extra`` block.

    Falls back to env vars (LINE_CHANNEL_ACCESS_TOKEN / LINE_CHANNEL_SECRET)
    only for the first/default account. Multi-account setups should put
    credentials inline so each account stays distinct.
    """

    def pick(*keys: str) -> Any:
        for key in keys:
            if key in extra:
                return extra[key]
        return None

    enabled_raw = pick("enabled")
    return LineAccount(
        account_id=account_id,
        enabled=bool(enabled_raw if enabled_raw is not None else True),
        channel_access_token=_config_or_env(
            extra, ("channel_access_token", "channelAccessToken"), "LINE_CHANNEL_ACCESS_TOKEN"
        ),
        channel_secret=_config_or_env(
            extra, ("channel_secret", "channelSecret"), "LINE_CHANNEL_SECRET"
        ),
        webhook_path=str(pick("webhook_path", "webhookPath") or DEFAULT_WEBHOOK_PATH),
        group_personas=_normalize_dict(pick("group_personas", "groupPersonas")),
        default_persona=(str(pick("default_persona", "defaultPersona") or "").strip() or None),
        allow_from=_normalize_list(pick("allow_from", "allowFrom")),
        dm_policy=str(pick("dm_policy", "dmPolicy") or "allowlist").lower(),
        group_policy=str(pick("group_policy", "groupPolicy") or "open").lower(),
    )


def _accounts_from_config(config: PlatformConfig) -> list[LineAccount]:
    extra = getattr(config, "extra", {}) or {}
    accounts_cfg = extra.get("accounts") if isinstance(extra, dict) else None
    accounts: list[LineAccount] = []
    if isinstance(accounts_cfg, dict) and accounts_cfg:
        # Account-level keys override top-level keys (channel creds, policies…)
        for account_id, account_extra in accounts_cfg.items():
            merged = {k: v for k, v in extra.items() if k not in {"accounts", "host", "port"}}
            if isinstance(account_extra, dict):
                merged.update(account_extra)
            account = _account_from_extra(merged, str(account_id))
            if account.enabled:
                accounts.append(account)
    else:
        account = _account_from_extra(extra, "default")
        if account.enabled:
            accounts.append(account)
    # Sanity: webhook_path must be unique per account, otherwise we cannot
    # tell which account a request belongs to.
    seen_paths: dict[str, str] = {}
    for account in accounts:
        existing = seen_paths.get(account.webhook_path)
        if existing:
            logger.warning(
                "[line] account %s shares webhook_path %s with %s — second registration overrides first",
                account.account_id,
                account.webhook_path,
                existing,
            )
        seen_paths[account.webhook_path] = account.account_id
    return accounts


# ---------------------------------------------------------------------------
# Signature verification
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Platform enum lookup (works whether the registry has registered "line" yet)
# ---------------------------------------------------------------------------


def _line_platform():
    try:
        return Platform("line")
    except Exception:
        return SimpleNamespace(value="line", name="LINE")


# ---------------------------------------------------------------------------
# Inbound message parsing
# ---------------------------------------------------------------------------


def _parse_message_content(message: dict[str, Any]) -> tuple[str, MessageType, Optional[str]]:
    """Return ``(text, message_type, content_message_id)``.

    ``content_message_id`` is the ID we use to fetch image/audio bytes via
    the Data API. Returns "" / TEXT / None when the message isn't supported
    (the caller should drop it).
    """
    mtype = message.get("type")
    message_id = message.get("id")
    if mtype == "text":
        return str(message.get("text") or ""), MessageType.TEXT, None
    if mtype == "image":
        return "", MessageType.PHOTO, str(message_id) if message_id else None
    if mtype == "audio":
        return "", MessageType.VOICE, str(message_id) if message_id else None
    if mtype == "sticker":
        pkg = message.get("packageId") or "?"
        sid = message.get("stickerId") or "?"
        return f"[貼圖: {pkg}/{sid}]", MessageType.TEXT, None
    return "", MessageType.TEXT, None


# ---------------------------------------------------------------------------
# Outbound text helpers
# ---------------------------------------------------------------------------


_MARKDOWN_PATTERNS = (
    (re.compile(r"\*\*(.+?)\*\*", re.DOTALL), r"\1"),  # **bold**
    (re.compile(r"(?<!\*)\*([^*\n]+)\*(?!\*)"), r"\1"),  # *italic*
    (re.compile(r"`([^`\n]+)`"), r"\1"),  # `code`
)


def _strip_markdown(text: str) -> str:
    out = text or ""
    for pat, repl in _MARKDOWN_PATTERNS:
        out = pat.sub(repl, out)
    return out


def _split_text(text: str, max_len: int) -> list[str]:
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


# ---------------------------------------------------------------------------
# LINE HTTP client (one per account)
# ---------------------------------------------------------------------------


class LineClient:
    def __init__(self, session: "aiohttp.ClientSession", account: LineAccount):
        self.session = session
        self.account = account

    @property
    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.account.channel_access_token}"}

    async def push_message(self, to: str, messages: list[dict[str, Any]]) -> None:
        url = f"{LINE_API_BASE}/v2/bot/message/push"
        async with self.session.post(
            url,
            json={"to": to, "messages": messages},
            headers={**self._auth_headers, "Content-Type": "application/json"},
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.error("[line] push failed status=%s body=%s", resp.status, body[:300])
                raise RuntimeError(f"LINE push failed: {resp.status}")

    async def download_content(self, message_id: str) -> bytes:
        url = f"{LINE_DATA_API_BASE}/v2/bot/message/{message_id}/content"
        async with self.session.get(url, headers=self._auth_headers) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.warning("[line] content download %s failed status=%s body=%s", message_id, resp.status, body[:200])
                raise RuntimeError(f"LINE content download failed: {resp.status}")
            return await resp.read()

    async def get_user_profile(self, user_id: str) -> Optional[dict[str, Any]]:
        url = f"{LINE_API_BASE}/v2/bot/profile/{user_id}"
        try:
            async with self.session.get(url, headers=self._auth_headers) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception as exc:
            logger.debug("[line] profile fetch failed: %s", exc)
            return None


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class LineAdapter(BasePlatformAdapter):
    MAX_MESSAGE_LENGTH = TEXT_CHUNK_LIMIT

    def __init__(self, config: PlatformConfig):
        super().__init__(config, _line_platform())
        extra = getattr(config, "extra", {}) or {}
        self.host = str(extra.get("host") or os.getenv("LINE_HOST") or "127.0.0.1")
        self.port = int(extra.get("port") or os.getenv("LINE_PORT") or DEFAULT_PORT)
        self.accounts = _accounts_from_config(config)
        self._clients: dict[str, LineClient] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._runner: Optional[web.AppRunner] = None
        self._auto_sethome_done: bool = False

    @property
    def name(self) -> str:
        return "LINE"

    # -- lifecycle ----------------------------------------------------------

    async def connect(self) -> bool:
        if not AIOHTTP_AVAILABLE:
            self._set_fatal_error("MISSING_DEPS", "Install dependencies: pip install aiohttp", retryable=False)
            return False
        if not self.accounts or not any(a.has_credentials for a in self.accounts):
            self._set_fatal_error(
                "MISSING_CREDENTIALS",
                "LINE channel_access_token and channel_secret are required",
                retryable=False,
            )
            return False

        self._session = aiohttp.ClientSession()
        self._clients = {a.account_id: LineClient(self._session, a) for a in self.accounts}

        app = web.Application(client_max_size=2 * 1024 * 1024)
        app.router.add_get("/health", lambda _req: web.Response(text="ok"))
        for account in self.accounts:
            app.router.add_post(account.webhook_path, self._make_webhook_handler(account))
            logger.info(
                "[line] account=%s webhook_path=%s",
                account.account_id,
                account.webhook_path,
            )

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        self._mark_connected()
        logger.info("[line] webhook server listening on %s:%d", self.host, self.port)
        return True

    async def disconnect(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        if self._session:
            await self._session.close()
            self._session = None
        self._clients = {}
        self._mark_disconnected()

    # -- inbound ------------------------------------------------------------

    def _make_webhook_handler(self, account: LineAccount):
        async def handler(request: "web.Request") -> "web.Response":
            raw_body = await request.read()
            signature = request.headers.get(LINE_SIGNATURE_HEADER)
            if not signature:
                return web.Response(status=401, text="missing signature")
            if not verify_signature(raw_body, signature, account.channel_secret):
                logger.warning("[line] invalid signature account=%s remote=%s", account.account_id, request.remote)
                return web.Response(status=403, text="invalid signature")
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except json.JSONDecodeError:
                return web.Response(status=400, text="invalid json")
            events = payload.get("events") or []
            for event in events:
                if not isinstance(event, dict):
                    continue
                # We only act on user messages. Postback / Follow / Join /
                # Leave / Beacon / Unfollow / etc. are silently dropped to
                # match the design v4 contract.
                if event.get("type") != "message":
                    continue
                task = asyncio.create_task(self._process_event(account, event))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
            return web.Response(text="ok")

        return handler

    async def _process_event(self, account: LineAccount, event: dict[str, Any]) -> None:
        try:
            await self._process_event_inner(account, event)
        except Exception as exc:
            logger.error("[line] event processing failed: %s", exc, exc_info=True)

    async def _process_event_inner(self, account: LineAccount, event: dict[str, Any]) -> None:
        source = event.get("source") or {}
        source_type = source.get("type")  # "user" | "group" | "room"
        user_id = source.get("userId")
        group_id = source.get("groupId")
        # "room" (multi-person chats not tied to a group) — treat like a group
        # for routing/auth purposes, using roomId as the chat id.
        room_id = source.get("roomId")
        chat_group_id = group_id or room_id

        message = event.get("message") or {}
        message_id = message.get("id")

        # ---- authorization ----
        if source_type == "user":
            if account.dm_policy == "allowlist" and user_id not in account.allow_from:
                logger.info("[line] drop DM from non-allowlisted user account=%s", account.account_id)
                return
            # "open" and "pairing" pass through (GatewayRunner enforces pairing)
        elif source_type in {"group", "room"}:
            if account.group_policy == "allowlist" and chat_group_id not in account.group_personas:
                logger.info("[line] drop group msg from non-allowlisted group account=%s", account.account_id)
                return
        else:
            return

        # ---- persona routing ----
        if chat_group_id:
            auto_skill = account.group_personas.get(chat_group_id, account.default_persona)
        else:
            auto_skill = account.default_persona

        # ---- message parsing ----
        text, msg_type, content_msg_id = _parse_message_content(message)
        media_urls: list[str] = []
        media_types: list[str] = []
        if content_msg_id and msg_type in {MessageType.PHOTO, MessageType.VOICE}:
            try:
                data = await self._clients[account.account_id].download_content(content_msg_id)
                if msg_type == MessageType.PHOTO:
                    path = cache_image_from_bytes(data, ext=".jpg")
                    media_urls.append(path)
                    media_types.append("image/jpeg")
                else:  # VOICE
                    path = cache_audio_from_bytes(data, ext=".m4a")
                    media_urls.append(path)
                    media_types.append("audio/m4a")
            except Exception as exc:
                logger.warning("[line] media download failed message_id=%s: %s", content_msg_id, exc)

        if not text and not media_urls:
            return

        # ---- session source ----
        chat_id = chat_group_id or user_id
        if not chat_id:
            return
        chat_type = "group" if chat_group_id else "dm"

        # Best-effort sender enrichment — stash display name into channel_prompt
        # so the LLM knows who is talking. Skips silently on any error.
        channel_prompt: Optional[str] = None
        user_name = user_id
        if user_id:
            profile = await self._clients[account.account_id].get_user_profile(user_id)
            if profile and profile.get("displayName"):
                user_name = str(profile["displayName"])
                channel_prompt = f"LINE sender: {user_name}"

        # Auto-sethome: silently designate the first chat we see (preferring DMs)
        # as LINE_HOME_CHANNEL so cron-job results have a default destination
        # and so the gateway's "No home channel set" first-message prompt does
        # not nag the user on every new chat. DMs always take priority over
        # groups; once a DM has been seen, we stop overriding.
        if not self._auto_sethome_done:
            self._maybe_auto_sethome(chat_id, chat_type)

        sess_source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_id,
            chat_type=chat_type,
            user_id=user_id or "unknown",
            user_name=user_name or user_id or "unknown",
            message_id=str(message_id) if message_id else None,
        )
        hermes_event = MessageEvent(
            text=text or "",
            message_type=msg_type,
            source=sess_source,
            raw_message=event,
            message_id=str(message_id) if message_id else None,
            auto_skill=auto_skill,
            media_urls=media_urls,
            media_types=media_types,
            channel_prompt=channel_prompt,
        )
        await self.handle_message(hermes_event)

    def _maybe_auto_sethome(self, chat_id: str, chat_type: str) -> None:
        """Silently set ``LINE_HOME_CHANNEL`` env var on first message.

        DM > group: DMs always overwrite; group chats only fill in if no home
        is set yet. Once a DM has been seen the upgrade flag flips and we
        never touch the env var again from inbound traffic.

        Hermes core (gateway/run.py) checks ``<PLATFORM>_HOME_CHANNEL`` env
        when deciding whether to nag users with the "No home channel is set"
        prompt on first session in a chat. Without auto-sethome, every new
        DM and every new group fires that prompt because each chat has its
        own session and the env var is never populated unless the user runs
        ``/sethome`` (which they often forget or skip).
        """
        cur_home = os.getenv("LINE_HOME_CHANNEL", "").strip()
        if chat_type == "dm":
            if str(chat_id) != cur_home:
                self._save_home_channel(chat_id, label="DM")
            self._auto_sethome_done = True
        elif not cur_home:
            self._save_home_channel(chat_id, label="group fallback")

    @staticmethod
    def _save_home_channel(chat_id: str, *, label: str) -> None:
        try:
            # Lazy import — keeps the plugin importable in environments where
            # the hermes_cli package is not on sys.path (tests, packaging).
            from hermes_cli.config import save_env_value  # type: ignore

            save_env_value("LINE_HOME_CHANNEL", str(chat_id))
            logger.info("[line] Auto-sethome: %s %s set as LINE home channel", label, chat_id)
        except Exception as exc:  # pragma: no cover — best-effort
            logger.warning("[line] Auto-sethome failed: %s", exc)

    # -- outbound -----------------------------------------------------------

    def _client_for_chat(self, chat_id: str) -> LineClient:
        # Single-account: easy. Multi-account: the platform doesn't tag an
        # outbound destination with which account it belongs to, so we
        # default to the first account. Cron deliveries that need a specific
        # account should use ``metadata={"account_id": ...}`` (see ``send``).
        if not self._clients:
            raise RuntimeError("LINE adapter is not connected")
        return next(iter(self._clients.values()))

    def _resolve_client(self, metadata: Optional[Dict[str, Any]]) -> LineClient:
        if metadata and isinstance(metadata, dict):
            account_id = metadata.get("account_id")
            if account_id and account_id in self._clients:
                return self._clients[account_id]
        return self._client_for_chat("")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        try:
            client = self._resolve_client(metadata)
            text = _strip_markdown(self.format_message(content) if hasattr(self, "format_message") else content)
            chunks = _split_text(text, self.MAX_MESSAGE_LENGTH)
            for chunk in chunks:
                await client.push_message(chat_id, [{"type": "text", "text": chunk}])
            return SendResult(success=True)
        except Exception as exc:
            logger.error("[line] send failed: %s", exc, exc_info=True)
            return SendResult(success=False, error=str(exc), retryable=True)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        # LINE Messaging API does not support typing indicators on Push.
        # ``loading-animation`` exists but only works for 1:1 chats and via
        # a separate endpoint; skipping here to avoid surprising group calls.
        return None

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        try:
            if not (image_url.startswith("http://") or image_url.startswith("https://")):
                return SendResult(
                    success=False,
                    error="LINE image messages require a public https URL",
                    retryable=False,
                )
            client = self._resolve_client(metadata)
            await client.push_message(
                chat_id,
                [{"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url}],
            )
            if caption:
                await self.send(chat_id, caption, reply_to=reply_to, metadata=metadata)
            return SendResult(success=True)
        except Exception as exc:
            logger.error("[line] send_image failed: %s", exc, exc_info=True)
            return SendResult(success=False, error=str(exc), retryable=True)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **_kwargs,
    ) -> SendResult:
        # LINE only accepts a public URL; local files cannot be uploaded
        # directly. Caller should host the file behind a public URL first.
        return SendResult(
            success=False,
            error="LINE Messaging API has no file upload endpoint — host the image and pass an https URL",
            retryable=False,
        )

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        chat_type = "group" if chat_id and chat_id.startswith(("C", "R")) else "dm"
        return {"name": chat_id, "type": chat_type, "chat_id": chat_id}


# ---------------------------------------------------------------------------
# Plugin entry-point hooks
# ---------------------------------------------------------------------------


def check_requirements() -> bool:
    return AIOHTTP_AVAILABLE


def validate_config(config: PlatformConfig) -> bool:
    try:
        return any(a.has_credentials for a in _accounts_from_config(config))
    except Exception:
        return False


def is_connected(config: PlatformConfig) -> bool:
    return validate_config(config)


def interactive_setup() -> None:
    from hermes_cli.config import (
        get_env_value,
        print_info,
        print_success,
        print_warning,
        prompt,
        save_env_value,
    )

    print_info("LINE setup needs a Channel from the LINE Developers Console (Messaging API).")
    print_info(
        "Webhook URL: https://<your-public-host>/webhook/line  "
        "(disable LINE Official Account 'Auto-reply' so the bot can answer)."
    )
    values = {
        "LINE_CHANNEL_ACCESS_TOKEN": ("Long-lived Channel Access Token", True),
        "LINE_CHANNEL_SECRET": ("Channel Secret", True),
    }
    for env_key, (label, secret) in values.items():
        current = get_env_value(env_key) or ""
        value = prompt(label, default=current, password=secret)
        if value:
            save_env_value(env_key, value.strip())
        else:
            print_warning(f"{env_key} is required")
    port = prompt("Webhook port", default=get_env_value("LINE_PORT") or str(DEFAULT_PORT))
    if port:
        save_env_value("LINE_PORT", port.strip())
    print_success("LINE env saved. Add gateway.platforms.line.enabled=true and restart gateway.")


def register(ctx) -> None:
    ctx.register_platform(
        name="line",
        label="LINE",
        adapter_factory=lambda cfg: LineAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["LINE_CHANNEL_ACCESS_TOKEN", "LINE_CHANNEL_SECRET"],
        install_hint="pip install aiohttp",
        setup_fn=interactive_setup,
        allowed_users_env="LINE_ALLOWED_USERS",
        allow_all_env="LINE_ALLOW_ALL_USERS",
        max_message_length=TEXT_CHUNK_LIMIT,
        emoji="💚",
        allow_update_command=True,
        platform_hint=(
            "You are chatting via LINE Messaging API (LINE Official Account). "
            "Markdown is not rendered — write plain text. Each message is capped "
            "near 5000 characters; long replies are auto-split. Replies use the "
            "Push API, so you can reply at any time, including after long delays."
        ),
    )
