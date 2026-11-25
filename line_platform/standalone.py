"""Standalone LINE connector service (no Hermes required).

Two listeners:

* the **webhook** port (default 18791) — the only thing the public tunnel
  points at. It accepts signed LINE webhooks and nothing else.
* the **local API** port (default 18792, bound to 127.0.0.1, bearer token)
  — how another agent on the same machine (Muse) reads LINE history,
  fetches questions waiting for it, and sends replies.

Who answers a triggered question is chosen per group: ``llm`` (this
service calls Claude directly) or ``muse`` (the turn is queued for the
external agent). Admins switch it at runtime by DM:
``/group backend <label> llm|muse``.

Run:  ``python -m line_platform.standalone --config /path/to/config.yaml``
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import logging
import os
import signal
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import aiohttp
from aiohttp import web

from .client import LINE_SIGNATURE_HEADER, verify_signature
from .config import DEFAULT_PORT, LineAccount, accounts_from_extra
from .context import Speakers, redact
from .engine import Engine, Turn
from .store import EventRow, Store, default_data_dir

logger = logging.getLogger("line_platform.standalone")

DEFAULT_API_PORT = 18792
DEFAULT_MODEL = "claude-sonnet-5-5"
FAILURE_REPLY = "我現在沒辦法回答這個問題，請稍後再試，或直接問{owner}。"
DM_REPLY = "私訊目前只提供管理指令。輸入 /help 查看。"

# (system_prompt, user_prompt, model=None) -> text
Completion = Callable[..., Awaitable[str]]


class BackendError(RuntimeError):
    pass


def claude_completion(model: str = DEFAULT_MODEL, effort: str = "low", client: Any = None) -> Completion:
    """Single Messages API call per question. The stable part of the context
    (rules, pinned facts, knowledge base, summary) is the cached system
    prompt; the per-message part goes in the user turn."""
    if client is None:
        import anthropic

        client = anthropic.AsyncAnthropic()

    default_model = model

    async def complete(system_prompt: str, user_prompt: str, model: Optional[str] = None) -> str:
        response = await client.beta.messages.create(
            model=model or default_model,
            max_tokens=16000,
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            output_config={"effort": effort},
            system=[{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user_prompt}],
        )
        if response.stop_reason == "refusal":
            raise BackendError("model declined the request")
        text = "".join(block.text for block in response.content if block.type == "text").strip()
        if not text:
            raise BackendError(f"empty model response (stop_reason={response.stop_reason})")
        return text

    return complete


def user_prompt_for(turn: Turn) -> str:
    volatile = turn.context.volatile if turn.context else ""
    return f"{volatile}\n\n[提問]\n{turn.sender_name or '成員'}：{turn.text}".strip()


def _resolve_env(value: Any) -> str:
    text = str(value or "").strip()
    if text.startswith("env:"):
        return os.getenv(text[4:].strip(), "").strip()
    return text


class Service:
    def __init__(
        self,
        config: dict[str, Any],
        *,
        store: Optional[Store] = None,
        completion: Optional[Completion] = None,
    ):
        self.config = config
        self.host = str(config.get("host") or "127.0.0.1")
        self.port = int(config.get("port") or DEFAULT_PORT)
        api = config.get("api") or {}
        self.api_host = str(api.get("host") or "127.0.0.1")
        self.api_port = int(api.get("port") or DEFAULT_API_PORT)
        self.api_token = _resolve_env(api.get("token"))
        muse = config.get("muse") or {}
        self.muse_notify_url = _resolve_env(muse.get("notify_url"))
        self.muse_notify_token = _resolve_env(muse.get("notify_token"))
        self.turn_ttl_s = int(muse.get("turn_ttl_s") or 900)
        self.owner_label = str(config.get("owner_label") or "禮揚")
        self.accounts: list[LineAccount] = accounts_from_extra(config)
        self.completion = completion
        self.engine = Engine(
            self.accounts, dispatch=self.dispatch, store=store, summarizer=completion,
            media_saver=self._save_media, owner_label=self.owner_label,
        )
        self._session: Optional[aiohttp.ClientSession] = None
        self._runners: list[web.AppRunner] = []

    # -- answering ----------------------------------------------------------

    def _save_media(self, data: bytes, msg_type: str) -> str:
        media_dir = self.engine.store.path.parent / "media"
        media_dir.mkdir(mode=0o700, exist_ok=True)
        fd, path = tempfile.mkstemp(suffix=".jpg" if msg_type == "image" else ".m4a", dir=media_dir)
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        return path

    async def dispatch(self, turn: Turn) -> None:
        aid = turn.account.account_id
        if turn.group is None or turn.context is None:
            if turn.chat_type == "dm":
                await self.engine.send_text(turn.chat_id, DM_REPLY, account_id=aid)
            return
        backend = self.engine.group_backend(turn.account, turn.group)
        if backend == "muse":
            await self._queue_for_muse(turn)
            return
        if self.completion is None:
            logger.error("[line_platform] group %s uses backend llm but no model is configured", turn.group.name)
            return
        try:
            answer = await self.completion(turn.context.stable, user_prompt_for(turn), model=turn.group.model)
        except Exception as exc:
            logger.error("[line_platform] llm backend failed group=%s: %s", turn.group.name, exc)
            answer = FAILURE_REPLY.format(owner=self.owner_label)
        await self.engine.send_text(turn.chat_id, answer, account_id=aid)

    async def _queue_for_muse(self, turn: Turn) -> None:
        assert turn.group is not None and turn.context is not None
        turn_id = self.engine.store.enqueue_turn(
            account_id=turn.account.account_id, group_id=turn.group.group_id, user_id=turn.user_id,
            seq=turn.seq, sender=turn.sender_name, question=turn.text,
            system_prompt=turn.context.stable, context=turn.context.volatile,
        )
        logger.info("[line_platform] queued turn %d for muse group=%s", turn_id, turn.group.name)
        if not self.muse_notify_url or self._session is None:
            return
        headers = {"Authorization": f"Bearer {self.muse_notify_token}"} if self.muse_notify_token else {}
        try:
            async with self._session.post(
                self.muse_notify_url, json={"turn_id": turn_id, "group": turn.group.name, "muse_chat": turn.group.muse_chat or ""}, headers=headers,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status >= 300:
                    logger.warning("[line_platform] muse notify returned %s", resp.status)
        except Exception as exc:
            # The turn stays queued; a poller still picks it up.
            logger.warning("[line_platform] muse notify failed: %s", exc.__class__.__name__)

    # -- webhook ------------------------------------------------------------

    def _webhook_handler(self, account: LineAccount):
        async def handler(request: web.Request) -> web.Response:
            raw_body = await request.read()
            signature = request.headers.get(LINE_SIGNATURE_HEADER)
            if not signature:
                return web.Response(status=401, text="missing signature")
            if not verify_signature(raw_body, signature, account.channel_secret):
                logger.warning("[line_platform] invalid signature account=%s", account.account_id)
                return web.Response(status=403, text="invalid signature")
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                return web.Response(status=400, text="invalid json")
            for event in payload.get("events") or []:
                if isinstance(event, dict):
                    self.engine._spawn(self.engine.handle_event(account, event))
            return web.Response(text="ok")

        return handler

    def webhook_app(self) -> web.Application:
        app = web.Application(client_max_size=2 * 1024 * 1024)
        async def health(_request: web.Request) -> web.Response:
            return web.Response(text="ok")

        app.router.add_get("/health", health)
        for account in self.accounts:
            app.router.add_post(account.webhook_path, self._webhook_handler(account))
        return app

    # -- local API ------------------------------------------------------------

    def _authorized(self, request: web.Request) -> bool:
        header = request.headers.get("Authorization", "")
        return bool(self.api_token) and hmac.compare_digest(header, f"Bearer {self.api_token}")

    def _account(self, request: web.Request, body: Optional[dict[str, Any]] = None) -> LineAccount:
        key = (body or {}).get("account") or request.query.get("account")
        if key:
            if key not in self.engine.accounts:
                raise web.HTTPNotFound(text="unknown account")
            return self.engine.accounts[key]
        if len(self.engine.accounts) != 1:
            raise web.HTTPBadRequest(text="account is required")
        return next(iter(self.engine.accounts.values()))

    def _group(self, account: LineAccount, key: Any):
        group = account.find_group(str(key or ""))
        if group is None:
            raise web.HTTPNotFound(text="unknown group")
        return group

    def _rows_json(self, account: LineAccount, group, rows: list[EventRow], redacted: bool) -> list[dict[str, Any]]:
        speakers = Speakers(
            self.engine.store, account.account_id, group.group_id, group.bot_names[0] if group.bot_names else "bot"
        )
        tz = timezone(timedelta(hours=account.utc_offset_hours))
        return [
            {
                "seq": row.seq,
                "time": datetime.fromtimestamp(row.ts_ms / 1000, tz).isoformat(timespec="seconds"),
                "speaker": speakers.name(row),
                "direction": row.direction,
                "type": row.msg_type,
                "text": redact(row.text) if redacted else row.text,
            }
            for row in rows
        ]

    def api_app(self) -> web.Application:
        store = self.engine.store

        @web.middleware
        async def auth(request: web.Request, handler):
            if not self._authorized(request):
                return web.json_response({"error": "unauthorized"}, status=401)
            return await handler(request)

        async def groups(request: web.Request) -> web.Response:
            out = []
            for account in self.engine.accounts.values():
                for group in account.groups.values():
                    out.append({
                        "account": account.account_id, "group": group.name, "mode": group.mode,
                        "backend": group.backend, "model": group.model or "", "muse_chat": group.muse_chat or "",
                        "members": [m.card() for m in store.group_members(account.account_id, group.group_id)],
                    })
            return web.json_response({"groups": out})

        async def messages(request: web.Request) -> web.Response:
            account = self._account(request)
            group = self._group(account, request.query.get("group"))
            redacted = request.query.get("redact", "1") != "0"
            limit = max(1, min(int(request.query.get("limit", "200")), 1000))
            aid, gid = account.account_id, group.group_id
            if request.query.get("day"):
                try:
                    rows = store.messages_on(aid, gid, request.query["day"], utc_offset_hours=account.utc_offset_hours)
                except ValueError:
                    raise web.HTTPBadRequest(text="day must be YYYY-MM-DD")
            elif request.query.get("q"):
                rows = store.search(aid, gid, request.query["q"].split(), limit=min(limit, 100))
            elif request.query.get("after_seq"):
                rows = store.messages_after(aid, gid, int(request.query["after_seq"]), limit=limit)
            else:
                rows = store.recent_messages(aid, gid, limit=limit)
            return web.json_response({"group": group.name, "messages": self._rows_json(account, group, rows, redacted)})

        async def summary(request: web.Request) -> web.Response:
            account = self._account(request)
            group = self._group(account, request.query.get("group"))
            current = store.get_summary(account.account_id, group.group_id)
            return web.json_response({"group": group.name, "summary": current["summary"] if current else ""})

        def turn_json(row: dict[str, Any]) -> dict[str, Any]:
            account = self.engine.accounts.get(row["account_id"])
            group = account.group(row["group_id"]) if account else None
            return {
                "id": row["id"], "account": row["account_id"], "group": group.name if group else "",
                "muse_chat": (group.muse_chat or "") if group else "",
                "status": row["status"], "sender": row["sender"], "question": row["question"],
                "system_prompt": row["system_prompt"], "context": row["context"],
                "age_s": max(0, int((datetime.now().timestamp() * 1000 - row["created_ms"]) / 1000)),
            }

        async def turns(request: web.Request) -> web.Response:
            store.expire_turns(self.turn_ttl_s * 1000)
            out = [turn_json(r) for r in store.list_turns(request.query.get("status", "pending"))]
            chat = request.query.get("muse_chat")
            if chat:
                out = [t for t in out if t["muse_chat"] == chat]
            return web.json_response({"turns": out})

        async def claim(request: web.Request) -> web.Response:
            turn_id = int(request.match_info["turn_id"])
            if not store.claim_turn(turn_id):
                return web.json_response({"error": "not pending"}, status=409)
            return web.json_response({"turn": turn_json(store.get_turn(turn_id))})

        async def reply(request: web.Request) -> web.Response:
            turn_id = int(request.match_info["turn_id"])
            body = await request.json()
            text = str(body.get("text") or "").strip()
            row = store.get_turn(turn_id)
            if row is None:
                raise web.HTTPNotFound(text="unknown turn")
            if not text:
                raise web.HTTPBadRequest(text="text is required")
            if row["status"] not in {"pending", "claimed"}:
                return web.json_response({"error": f"turn is {row['status']}"}, status=409)
            outcome = await self.engine.send_text(row["group_id"], text, account_id=row["account_id"])
            if not outcome.ok:
                return web.json_response({"error": outcome.error}, status=502)
            store.finish_turn(turn_id, text)
            return web.json_response({"ok": True, "method": outcome.method})

        async def send(request: web.Request) -> web.Response:
            body = await request.json()
            account = self._account(request, body)
            group = self._group(account, body.get("group"))
            text = str(body.get("text") or "").strip()
            if not text:
                raise web.HTTPBadRequest(text="text is required")
            outcome = await self.engine.send_text(group.group_id, text, account_id=account.account_id)
            if not outcome.ok:
                return web.json_response({"error": outcome.error}, status=502)
            return web.json_response({"ok": True, "method": outcome.method})

        app = web.Application(middlewares=[auth], client_max_size=256 * 1024)
        app.router.add_get("/api/v1/groups", groups)
        app.router.add_get("/api/v1/messages", messages)
        app.router.add_get("/api/v1/summary", summary)
        app.router.add_get("/api/v1/turns", turns)
        app.router.add_post("/api/v1/turns/{turn_id:\\d+}/claim", claim)
        app.router.add_post("/api/v1/turns/{turn_id:\\d+}/reply", reply)
        app.router.add_post("/api/v1/send", send)
        return app

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        if not self.accounts or not all(a.has_credentials for a in self.accounts):
            raise SystemExit("every account needs channel_access_token and channel_secret")
        self._session = aiohttp.ClientSession()
        self.engine.attach_session(self._session)
        self.engine.load_documents()
        for app, host, port in ((self.webhook_app(), self.host, self.port),):
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            await web.TCPSite(runner, host, port).start()
            self._runners.append(runner)
        for account in self.accounts:
            logger.info(
                "[line_platform] account=%s webhook_path=%s reply_first=%s groups=%d admins=%d",
                account.account_id, account.webhook_path, account.reply_first, len(account.groups),
                len(account.admins),
            )
        logger.info("[line_platform] webhook listening on %s:%d", self.host, self.port)
        if self.api_token:
            runner = web.AppRunner(self.api_app(), access_log=None)
            await runner.setup()
            await web.TCPSite(runner, self.api_host, self.api_port).start()
            self._runners.append(runner)
            logger.info("[line_platform] local API listening on %s:%d", self.api_host, self.api_port)
        else:
            logger.warning("[line_platform] api.token is not set; the local API is disabled")

    async def stop(self) -> None:
        for runner in self._runners:
            await runner.cleanup()
        self._runners = []
        await self.engine.drain()
        if self._session:
            await self._session.close()
            self._session = None
        self.engine.store.close()


def load_config(path: Path) -> dict[str, Any]:
    import yaml

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise SystemExit(f"{path}: top level must be a mapping")
    return data


async def _amain(config_path: Path) -> None:
    config = load_config(config_path)
    llm = config.get("llm") or {}
    completion: Optional[Completion] = None
    if llm.get("enabled", True):
        completion = claude_completion(
            model=str(llm.get("model") or DEFAULT_MODEL), effort=str(llm.get("effort") or "low")
        )
    service = Service(config, completion=completion)
    await service.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    await service.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone LINE connector")
    parser.add_argument("--config", type=Path, default=default_data_dir() / "config.yaml")
    parser.add_argument(
        "--seen", action="store_true",
        help="list chats that messaged the bot but are not set up yet (IDs only), then exit",
    )
    args = parser.parse_args()
    if args.seen:
        store = Store()
        for account in accounts_from_extra(load_config(args.config)):
            for chat in store.stats(account.account_id)["unjournaled"]:
                print(f"{chat['chat_type']}\t{chat['chat_id']}\t{chat['count']} event(s)")
        store.close()
        return
    logging.basicConfig(level=os.getenv("LINE_CONNECTOR_LOG", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(_amain(args.config))


if __name__ == "__main__":
    main()
