"""LINE Messaging API platform adapter for Hermes Agent.

Inbound: webhook from LINE Platform — verified by X-Line-Signature
(HMAC-SHA256(channel_secret, body) → base64), parsed into Hermes
``MessageEvent`` and dispatched via ``handle_message``.

Outbound: reply token first (free, valid about a minute), Push API as the
fallback for late or proactive sends. ``reply_first: false`` restores the
pure-Push behaviour.

The pipeline itself (journal, enrollment, triggers, context assembly,
sending) lives in ``engine.py`` and is shared with the standalone service;
this module only translates between it and the Hermes gateway.

Multi-account: a single adapter instance can host several LINE Official
Accounts. Each account holds its own credentials, webhook path, and
persona routing table; events are dispatched via the matched account's
client. This lets one Hermes deployment serve e.g. a personal Cattia
account and a hospital "Lynx" account at the same time.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
from types import SimpleNamespace
from typing import Any, Dict, Optional

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

from .client import (  # noqa: E402  (re-exported for callers and tests)
    LINE_SIGNATURE_HEADER,
    TEXT_CHUNK_LIMIT,
    describe_message,
    split_text as _split_text,
    strip_markdown as _strip_markdown,
    verify_signature,
)
from .config import (  # noqa: E402
    DEFAULT_PORT,
    DEFAULT_WEBHOOK_PATH,
    LineAccount,
    account_from_extra as _account_from_extra,
    accounts_from_extra,
)
from .engine import Engine, Turn  # noqa: E402

logger = logging.getLogger(__name__)

_MSG_TYPES = {"image": "PHOTO", "audio": "VOICE"}


def _accounts_from_config(config: PlatformConfig) -> list[LineAccount]:
    return accounts_from_extra(getattr(config, "extra", {}) or {})


def _line_platform():
    """Platform enum lookup (works whether the registry has "line" yet)."""
    try:
        return Platform("line")
    except Exception:
        return SimpleNamespace(value="line", name="LINE")


def _parse_message_content(message: dict[str, Any]) -> tuple[str, MessageType, Optional[str]]:
    """Return ``(text, message_type, content_message_id)`` in Hermes types."""
    text, msg_type, content_id = describe_message(message)
    return text, getattr(MessageType, _MSG_TYPES.get(msg_type, "TEXT")), content_id


def _save_media(data: bytes, msg_type: str) -> str:
    if msg_type == "image":
        return cache_image_from_bytes(data, ext=".jpg")
    return cache_audio_from_bytes(data, ext=".m4a")


async def _hermes_summarizer(system_prompt: str, user_prompt: str) -> str:
    """Rolling summaries use Hermes' auxiliary model when it is available."""
    from agent.auxiliary_client import async_call_llm  # type: ignore

    response = await async_call_llm(
        task="compression",
        messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        max_tokens=2000,
    )
    return str(response.choices[0].message.content or "")


def _summarizer_or_none():
    try:
        import agent.auxiliary_client  # type: ignore  # noqa: F401
    except Exception:
        return None
    return _hermes_summarizer


class LineAdapter(BasePlatformAdapter):
    MAX_MESSAGE_LENGTH = TEXT_CHUNK_LIMIT

    def __init__(self, config: PlatformConfig):
        super().__init__(config, _line_platform())
        extra = getattr(config, "extra", {}) or {}
        self.host = str(extra.get("host") or os.getenv("LINE_HOST") or "127.0.0.1")
        self.port = int(extra.get("port") or os.getenv("LINE_PORT") or DEFAULT_PORT)
        self.accounts = _accounts_from_config(config)
        self.engine: Optional[Engine] = None
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
        self.engine = Engine(
            self.accounts, dispatch=self._dispatch_turn, summarizer=_summarizer_or_none(), media_saver=_save_media
        )
        self.engine.attach_session(self._session)
        self.engine.load_documents()

        app = web.Application(client_max_size=2 * 1024 * 1024)
        app.router.add_get("/health", lambda _req: web.Response(text="ok"))
        for account in self.accounts:
            app.router.add_post(account.webhook_path, self._make_webhook_handler(account))
            logger.info(
                "[line_platform] account=%s webhook_path=%s reply_first=%s groups=%d",
                account.account_id, account.webhook_path, account.reply_first, len(account.groups),
            )

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        self._mark_connected()
        logger.info("[line_platform] webhook server listening on %s:%d", self.host, self.port)
        return True

    async def disconnect(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        if self.engine:
            await self.engine.drain()
        if self._session:
            await self._session.close()
            self._session = None
        self._mark_disconnected()

    # -- inbound ------------------------------------------------------------

    def _make_webhook_handler(self, account: LineAccount):
        async def handler(request: "web.Request") -> "web.Response":
            raw_body = await request.read()
            signature = request.headers.get(LINE_SIGNATURE_HEADER)
            if not signature:
                return web.Response(status=401, text="missing signature")
            if not verify_signature(raw_body, signature, account.channel_secret):
                logger.warning(
                    "[line_platform] invalid signature account=%s remote=%s", account.account_id, request.remote
                )
                return web.Response(status=403, text="invalid signature")
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except json.JSONDecodeError:
                return web.Response(status=400, text="invalid json")
            for event in payload.get("events") or []:
                if not isinstance(event, dict):
                    continue
                task = asyncio.create_task(self.engine.handle_event(account, event))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
            return web.Response(text="ok")

        return handler

    async def _dispatch_turn(self, turn: Turn) -> None:
        """Hand an engine turn to the Hermes agent as a ``MessageEvent``."""
        if not self._auto_sethome_done:
            self._maybe_auto_sethome(turn.chat_id, turn.chat_type)
        source = self.build_source(
            chat_id=turn.chat_id,
            chat_name=turn.group.name if turn.group else turn.chat_id,
            chat_type=turn.chat_type,
            user_id=turn.user_id,
            user_name=turn.sender_name or turn.user_id,
            message_id=turn.message_id,
        )
        kwargs: dict[str, Any] = {}
        if turn.context is not None:
            # Hosts that separate a stable channel prompt from per-turn
            # context get both; older ones get a single prompt.
            fields = {f.name for f in dataclasses.fields(MessageEvent)} if dataclasses.is_dataclass(MessageEvent) else set()
            if "channel_context" in fields:
                kwargs = {"channel_prompt": turn.context.stable, "channel_context": turn.context.volatile}
            else:
                kwargs = {"channel_prompt": turn.context.combined}
        elif turn.sender_name:
            kwargs = {"channel_prompt": f"LINE sender: {turn.sender_name}"}
        event = MessageEvent(
            text=turn.text or "",
            message_type=getattr(MessageType, _MSG_TYPES.get(turn.msg_type, "TEXT")),
            source=source,
            raw_message=turn.raw_event,
            message_id=turn.message_id,
            auto_skill=turn.persona,
            media_urls=turn.media_paths,
            media_types=turn.media_types,
            **kwargs,
        )
        await self.handle_message(event)

    def _maybe_auto_sethome(self, chat_id: str, chat_type: str) -> None:
        """Silently set ``LINE_HOME_CHANNEL`` env var on first message.

        DM > group: DMs always overwrite; group chats only fill in if no home
        is set yet. Once a DM has been seen the upgrade flag flips and we
        never touch the env var again from inbound traffic.
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
            logger.info("[line_platform] Auto-sethome: %s chat set as LINE home channel", label)
        except Exception as exc:  # pragma: no cover — best-effort
            logger.warning("[line_platform] Auto-sethome failed: %s", exc)

    # -- outbound -----------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if self.engine is None:
            return SendResult(success=False, error="LINE adapter is not connected", retryable=True)
        account_id = metadata.get("account_id") if isinstance(metadata, dict) else None
        text = self.format_message(content) if hasattr(self, "format_message") else content
        outcome = await self.engine.send_text(chat_id, text, account_id=account_id)
        if outcome.ok:
            return SendResult(success=True)
        return SendResult(success=False, error=outcome.error, retryable=outcome.error != "group is in shadow mode")

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        # ``loading-animation`` only works for 1:1 chats; skipped to avoid
        # surprising group calls.
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
            if self.engine is None:
                return SendResult(success=False, error="LINE adapter is not connected", retryable=True)
            account_id = metadata.get("account_id") if isinstance(metadata, dict) else None
            account = self.engine.account_for_chat(chat_id, account_id)
            group = account.group(chat_id)
            if group is not None and group.mode == "shadow":
                return SendResult(success=False, error="group is in shadow mode", retryable=False)
            await self.engine.clients[account.account_id].push_message(
                chat_id,
                [{"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url}],
            )
            if caption:
                await self.send(chat_id, caption, reply_to=reply_to, metadata=metadata)
            return SendResult(success=True)
        except Exception as exc:
            logger.error("[line_platform] send_image failed: %s", exc, exc_info=True)
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


class PluginShadowedError(RuntimeError):
    """Another LINE adapter owns the ``line`` platform name."""


def _assert_active(factory) -> None:
    """Fail loudly unless this plugin is the adapter the gateway will build.

    Recent Hermes ships its own LINE adapter (``plugins/platforms/line``)
    under the same platform name. Two adapters must never both serve LINE,
    and a silently ignored plugin would mean the journal, enrollment gate
    and group isolation are not in force. So after registering we look the
    name up and stop the gateway if someone else holds it.
    """
    try:
        from gateway.platform_registry import platform_registry  # type: ignore
    except Exception:
        # Older Hermes without a registry: core adapters win there, and the
        # only signal is ``gateway/platforms/line.py`` existing.
        try:
            import importlib.util

            if importlib.util.find_spec("gateway.platforms.line") is not None:
                raise PluginShadowedError(
                    "[line_platform] NOT ACTIVE: this Hermes has a core LINE adapter (gateway/platforms/line.py) "
                    "that registers first. Disable it or this plugin; they cannot both serve LINE."
                )
        except PluginShadowedError:
            raise
        except Exception:
            pass
        logger.warning("[line_platform] could not verify platform ownership on this Hermes version")
        return
    entry = platform_registry.get("line")
    if entry is None or getattr(entry, "adapter_factory", None) is not factory:
        owner = getattr(entry, "plugin_name", None) or getattr(entry, "source", None) or "unknown"
        raise PluginShadowedError(
            f"[line_platform] NOT ACTIVE: platform 'line' is owned by {owner!r}. "
            "Disable the bundled LINE plugin (or this one); they cannot both serve LINE."
        )
    logger.info("[line_platform] active: this plugin owns platform 'line' (bundled adapter, if any, is replaced)")


def _adapter_factory(cfg):
    return LineAdapter(cfg)


def register(ctx) -> None:
    ctx.register_platform(
        name="line",
        label="LINE",
        adapter_factory=_adapter_factory,
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
            "near 5000 characters; long replies are auto-split. A reply within about "
            "a minute is free; later or proactive messages use the metered Push API."
        ),
    )
    _assert_active(_adapter_factory)
