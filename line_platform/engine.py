"""The connector pipeline, independent of any agent runtime.

Inbound, per webhook event::

    authorize → journal → (media) → admin command | enrollment gate
              → trigger decision → context assembly → dispatch(turn)

Outbound: reply token first, Push as fallback, every sent message journaled.

A host supplies ``dispatch`` (what to do with a turn that should be
answered). The Hermes adapter turns it into a ``MessageEvent``; the
standalone service calls a model and sends the answer back.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from .client import (
    MAX_MESSAGES_PER_CALL,
    TEXT_CHUNK_LIMIT,
    LineApiError,
    LineClient,
    describe_message,
    is_self_mentioned,
    split_text,
    strip_markdown,
)
from .config import BACKENDS, GROUP_MODES, GroupConfig, LineAccount, resolve_model, valid_label
from .context import TurnContext, build_turn_context, is_question
from .kb import Document, DocumentCache, group_dir
from .memory import GroupMemory, Summarizer
from .store import EventRow, Store, valid_group_id

logger = logging.getLogger(__name__)

LIFECYCLE_EVENTS = {"join", "leave", "memberJoined", "memberLeft", "follow", "unfollow"}
ADMIN_COMMANDS = ("/member", "/group", "/kb", "/journal", "/help")

HELP_TEXT = """\
管理指令（只在與機器人的私訊中有效）：
/member pending
/member approve <核准碼> <姓名> | <角色> | <與你的關係，可省略>
                                    （新成員也可用空白分隔：<姓名> <角色>）
/member approve <核准碼>            （此人已在別的群登記過，沿用身分）
/member approve <核准碼> <本群稱呼>  （沿用身分，只改本群稱呼）
/member deny <核准碼>
/group list
/group add <設定碼> <群組標籤> [llm [模型] | muse [Side Chat 代號]]
/group backend <群組標籤> llm|muse   （切換回答來源，立即生效）
/group model <群組標籤> sonnet|opus|haiku|<模型 ID>
/group muse <群組標籤> <Side Chat 代號>
/group mode <群組標籤> shadow|quiet|active
/group summary [群組標籤]
/kb reload
/journal stats"""


@dataclass
class Turn:
    """One inbound message that should be answered."""

    account: LineAccount
    chat_id: str
    chat_type: str  # "group" | "dm"
    user_id: str
    text: str
    msg_type: str = "text"
    message_id: Optional[str] = None
    seq: Optional[int] = None
    media_paths: list[str] = field(default_factory=list)
    media_types: list[str] = field(default_factory=list)
    persona: Optional[str] = None
    group: Optional[GroupConfig] = None
    context: Optional[TurnContext] = None
    sender_name: str = ""
    trigger: str = ""
    raw_event: Any = None


@dataclass
class SendOutcome:
    ok: bool
    method: str = ""  # "reply" | "push" | "reply+push"
    error: str = ""


Dispatch = Callable[[Turn], Awaitable[None]]
MediaSaver = Callable[[bytes, str], str]  # (bytes, msg_type) -> local path


class Engine:
    def __init__(
        self,
        accounts: list[LineAccount],
        *,
        dispatch: Dispatch,
        store: Optional[Store] = None,
        summarizer: Optional[Summarizer] = None,
        media_saver: Optional[MediaSaver] = None,
        owner_label: str = "禮揚",
    ):
        self.accounts = {a.account_id: a for a in accounts}
        self.store = store or Store()
        self.dispatch = dispatch
        self.memory = GroupMemory(self.store, summarizer)
        self.media_saver = media_saver
        self.owner_label = owner_label
        self.docs = DocumentCache()
        self.clients: dict[str, LineClient] = {}
        self._reply_tokens: dict[tuple[str, str], tuple[str, float]] = {}
        self._answering: dict[tuple[str, str], str] = {}
        self._chat_accounts: dict[str, str] = {}
        self._tasks: set[asyncio.Task] = set()
        self._load_group_overrides()

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def attach_session(self, session: Any) -> None:
        self.clients = {aid: LineClient(session, acct) for aid, acct in self.accounts.items()}

    def load_documents(self) -> None:
        """Load every configured group's KB and pinned facts (startup)."""
        for account in self.accounts.values():
            for group in account.groups.values():
                kb, pinned = self.group_documents(account, group)
                logger.info(
                    "[line_platform] group=%s mode=%s kb=%s pinned=%s",
                    group.name, group.mode,
                    kb.version if kb.exists else "missing",
                    "ok" if pinned.exists else "missing",
                )

    def _doc_paths(self, account: LineAccount, group: GroupConfig) -> tuple[Path, Path]:
        base = group_dir(account.account_id, group.label or group.group_id, self.store.path.parent)
        kb = Path(group.kb_path) if group.kb_path else base / "kb.md"
        pinned = Path(group.pinned_path) if group.pinned_path else base / "pinned.md"
        return kb, pinned

    def group_documents(
        self, account: LineAccount, group: GroupConfig, *, force: bool = False
    ) -> tuple[Document, Document]:
        kb_path, pinned_path = self._doc_paths(account, group)
        return self.docs.get(kb_path, force=force), self.docs.get(pinned_path, force=force)

    # Groups added or changed by admin DM live in the database, so they
    # survive restarts and need no config-file edit. They are applied on
    # top of the config file.

    @staticmethod
    def _group_key(account_id: str, group_id: str) -> str:
        return f"group:{account_id}:{group_id}"

    @staticmethod
    def _apply_override(group: GroupConfig, data: dict[str, Any]) -> None:
        if valid_label(data.get("label")):
            group.label = data["label"]
        if data.get("mode") in GROUP_MODES:
            group.mode = data["mode"]
        if data.get("backend") in BACKENDS:
            group.backend = data["backend"]
        if "model" in data:
            group.model = resolve_model(data["model"])
        if "muse_chat" in data:
            group.muse_chat = str(data["muse_chat"] or "").strip() or None

    def _load_group_overrides(self) -> None:
        for key, value in self.store.settings_with_prefix("group:"):
            _, aid, gid = key.split(":", 2)
            account = self.accounts.get(aid)
            if account is None:
                continue
            try:
                data = json.loads(value)
            except json.JSONDecodeError:
                continue
            group = account.groups.get(gid)
            if group is None:
                group = account.groups[gid] = GroupConfig(group_id=gid, bot_names=list(account.bot_names))
            self._apply_override(group, data)

    def _save_group_override(self, account: LineAccount, group: GroupConfig, **changes: Any) -> None:
        key = self._group_key(account.account_id, group.group_id)
        data = json.loads(self.store.get_setting(key) or "{}")
        data.update(changes)
        self.store.set_setting(key, json.dumps(data, ensure_ascii=False))
        self._apply_override(group, data)

    async def _announce_group(self, account: LineAccount, group_id: str) -> None:
        """Tell the admins, once, that the bot is in a group nobody has set
        up, with a short code to set it up by DM. No message content."""
        aid = account.account_id
        if self.store.get_setting(f"joincode:{aid}:{group_id}"):
            return
        code = "".join(secrets.choice("ABCDEFGHJKLMNPQRSTUVWXYZ23456789") for _ in range(6))
        self.store.set_setting(f"joincode:{aid}:{group_id}", code)
        self.store.set_setting(f"join:{aid}:{code}", group_id)
        bot = account.bot_names[0] if account.bot_names else "機器人"
        text = "\n".join([
            f"{bot}在一個尚未設定的群組裡。設定碼 {code}",
            f"接 LLM（預設 Sonnet）：/group add {code} 群組標籤",
            f"接 LLM 並指定模型：/group add {code} 群組標籤 llm opus",
            f"接 Muse：/group add {code} 群組標籤 muse Side-Chat代號",
            "設定之前，這個群組的訊息不會被記錄，機器人也不會回話。",
        ])
        logger.info("[line_platform] unconfigured group seen account=%s code=%s", aid, code)
        for admin in account.admins:
            await self.send_text(admin, text, account_id=aid, use_reply_token=False)

    def _spawn(self, coro: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def drain(self) -> None:
        """Wait for background work (tests and shutdown)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # ------------------------------------------------------------------
    # Inbound
    # ------------------------------------------------------------------

    async def handle_event(self, account: LineAccount, event: dict[str, Any]) -> None:
        try:
            await self._handle_event(account, event)
        except Exception as exc:
            logger.error("[line_platform] event processing failed: %s", exc, exc_info=True)

    async def _handle_event(self, account: LineAccount, event: dict[str, Any]) -> None:
        etype = str(event.get("type") or "")
        source = event.get("source") or {}
        source_type = source.get("type")  # "user" | "group" | "room"
        user_id = source.get("userId")
        chat_group_id = source.get("groupId") or source.get("roomId")
        if source_type == "user":
            chat_id, chat_type = user_id, "dm"
        elif source_type in {"group", "room"}:
            chat_id, chat_type = chat_group_id, "group"
        else:
            return
        if not chat_id:
            return
        aid = account.account_id

        # ---- authorization ----
        if chat_type == "dm":
            allowed = (
                account.dm_policy != "allowlist" or user_id in account.allow_from or user_id in account.admins
            )
        else:
            allowed = account.group_allowed(chat_id)
        if not allowed:
            # IDs and a counter only: lets the admin find a new group's ID.
            self.store.note_seen_chat(aid, chat_id, chat_type)
            logger.info("[line_platform] drop %s event from non-allowlisted %s account=%s", etype, chat_type, aid)
            if chat_type == "group" and etype in {"join", "message"}:
                await self._announce_group(account, chat_id)
            return
        if chat_type == "group" and account.group(chat_id) is None and chat_id not in account.group_personas:
            if etype in {"join", "message"}:
                await self._announce_group(account, chat_id)

        ts_ms = int(event.get("timestamp") or time.time() * 1000)
        event_id = event.get("webhookEventId")

        if etype != "message":
            if etype in LIFECYCLE_EVENTS:
                self.store.record_event(
                    account_id=aid, chat_type=chat_type, chat_id=chat_id, user_id=user_id,
                    event_type=etype, event_id=event_id, ts_ms=ts_ms, raw=event,
                )
                logger.info("[line_platform] lifecycle %s in %s account=%s", etype, chat_type, aid)
            return

        message = event.get("message") or {}
        message_id = str(message.get("id")) if message.get("id") else None
        text, msg_type, content_id = describe_message(message)
        quoted = message.get("quotedMessageId")

        # ---- journal first: the single source of truth ----
        seq = self.store.record_event(
            account_id=aid, chat_type=chat_type, chat_id=chat_id, user_id=user_id,
            event_type="message", event_id=event_id, message_id=message_id, msg_type=msg_type,
            text=text, reply_to_message_id=str(quoted) if quoted else None, ts_ms=ts_ms, raw=event,
        )
        if seq is None:
            logger.info("[line_platform] duplicate webhook delivery ignored account=%s", aid)
            return
        self._chat_accounts[chat_id] = aid

        if chat_type == "dm":
            await self._handle_dm(account, event, user_id, text, msg_type, message_id, content_id, seq)
            return

        group = account.group(chat_id)
        if group is None:
            # Legacy pass-through: groups routed only by ``group_personas``.
            if not text and not content_id:
                return
            turn = Turn(
                account=account, chat_id=chat_id, chat_type="group", user_id=user_id or "unknown",
                text=text, msg_type=msg_type, message_id=message_id, seq=seq,
                persona=account.group_personas.get(chat_id, account.default_persona), raw_event=event,
            )
            await self._attach_media(turn, content_id, seq)
            if user_id:
                profile = await self._client(aid).get_user_profile(user_id)
                turn.sender_name = str((profile or {}).get("displayName") or "")
            self._stash_reply_token(aid, chat_id, event, user_id)
            await self.dispatch(turn)
            return

        if self.memory.summarizer is not None and self.memory.due(account, group):
            self._spawn(self.memory.update(account, group))

        trigger = self._trigger(account, group, message, text, user_id, ts_ms)

        # ---- enrollment gate: identity is by approved userId only ----
        if group.require_enrollment:
            if not user_id:
                return
            status = self.store.membership_status(aid, chat_id, user_id)
            if status == "denied":
                return
            if status != "approved":
                await self._enroll(account, group, event, user_id, trigger)
                return

        if group.mode == "shadow" or not trigger:
            return
        if not text and not content_id:
            return

        kb, pinned = self.group_documents(account, group)
        sender = self.store.identity(aid, chat_id, user_id) if user_id else None
        turn = Turn(
            account=account, chat_id=chat_id, chat_type="group", user_id=user_id or "unknown",
            text=text, msg_type=msg_type, message_id=message_id, seq=seq,
            persona=group.persona or account.group_personas.get(chat_id, account.default_persona),
            group=group, trigger=trigger, raw_event=event,
            sender_name=(sender.group_name or sender.local_name) if sender else "",
            context=build_turn_context(
                store=self.store, account=account, group=group, kb=kb, pinned=pinned, sender=sender,
                text=text, current_seq=seq, owner_label=self.owner_label,
                backend=self.group_backend(account, group),
            ),
        )
        await self._attach_media(turn, content_id, seq)
        self._stash_reply_token(aid, chat_id, event, user_id)
        logger.info(
            "[line_platform] dispatch group=%s trigger=%s kb=%s", group.name, trigger, turn.context.kb_version or "none"
        )
        await self.dispatch(turn)

    async def _handle_dm(
        self, account: LineAccount, event: dict[str, Any], user_id: str, text: str, msg_type: str,
        message_id: Optional[str], content_id: Optional[str], seq: int,
    ) -> None:
        aid = account.account_id
        self._stash_reply_token(aid, user_id, event, user_id)
        first_word = text.strip().split(maxsplit=1)[0] if text.strip() else ""
        if user_id in account.admins and first_word in ADMIN_COMMANDS:
            reply = await self.admin_command(account, user_id, text)
            await self.send_text(user_id, reply, account_id=aid)
            return
        if not text and not content_id:
            return
        turn = Turn(
            account=account, chat_id=user_id, chat_type="dm", user_id=user_id, text=text, msg_type=msg_type,
            message_id=message_id, seq=seq, persona=account.default_persona, raw_event=event,
        )
        await self._attach_media(turn, content_id, seq)
        profile = await self._client(aid).get_user_profile(user_id)
        turn.sender_name = str((profile or {}).get("displayName") or "")
        await self.dispatch(turn)

    async def _attach_media(self, turn: Turn, content_id: Optional[str], seq: int) -> None:
        if not content_id or turn.msg_type not in {"image", "audio"} or self.media_saver is None:
            return
        try:
            data = await self._client(turn.account.account_id).download_content(content_id)
            path = self.media_saver(data, turn.msg_type)
        except Exception as exc:
            logger.warning("[line_platform] media download failed: %s", exc)
            return
        turn.media_paths.append(path)
        turn.media_types.append("image/jpeg" if turn.msg_type == "image" else "audio/m4a")
        self.store.set_media_ref(seq, path)

    def _trigger(
        self, account: LineAccount, group: GroupConfig, message: dict[str, Any], text: str,
        user_id: Optional[str], ts_ms: int,
    ) -> str:
        """Why the bot should answer this message; empty string = stay quiet."""
        if is_self_mentioned(message):
            return "mention"
        if group.mode == "quiet":
            return ""
        lowered = text.lower()
        if any(name and name.lower() in lowered for name in group.bot_names):
            return "name"
        quoted = message.get("quotedMessageId")
        if quoted:
            row = self.store.find_message(account.account_id, group.group_id, str(quoted))
            if row is not None and row.direction == "out":
                return "reply-to-bot"
        question = is_question(text)
        if question:
            last = self.store.last_outbound(account.account_id, group.group_id)
            if (
                last is not None and user_id and last.user_id == user_id
                and 0 <= ts_ms - last.ts_ms <= group.followup_window_s * 1000
            ):
                return "follow-up"
        if question or not group.triggers_require_question:
            for pattern in group.compiled_triggers():
                if pattern.search(text):
                    return "rule"
        return ""

    async def _enroll(
        self, account: LineAccount, group: GroupConfig, event: dict[str, Any], user_id: str, trigger: str
    ) -> None:
        """Block an unknown sender before the agent and open a private
        pending request. The blocked message's text is not copied anywhere
        outside the journal."""
        aid = account.account_id
        if self.store.get_request(aid, group.group_id, user_id) is not None:
            return
        profile = await self._client(aid).get_group_member_profile(group.group_id, user_id)
        hint = str((profile or {}).get("displayName") or "")
        request, created = self.store.create_request(aid, group.group_id, user_id, hint)
        if not created:
            return
        logger.info("[line_platform] enrollment pending group=%s code=%s", group.name, request.code)
        known = self.store.get_person(aid, user_id)
        lines = [
            f"新成員待核准（群組：{group.name}）",
            f"LINE 顯示名稱（僅供參考，不能當身分依據）：{hint or '未取得'}",
            f"核准碼 {request.code}",
        ]
        if known:
            lines += [
                f"此人已在其他群組登記為「{known['local_name']}」（{known['role']}）。",
                f"沿用身分：/member approve {request.code}",
                f"沿用身分並指定本群稱呼：/member approve {request.code} 稱呼",
            ]
        else:
            lines.append(f"核准：/member approve {request.code} 姓名 角色 關係（關係可省略，例如：{request.code} 小明 表弟）")
        lines.append(f"拒絕：/member deny {request.code}")
        if not account.admins:
            logger.error("[line_platform] account %s has no admins; pending request cannot be approved", aid)
        for admin in account.admins:
            await self.send_text(admin, "\n".join(lines), account_id=aid, use_reply_token=False)
        acknowledge = group.mode != "shadow" and (
            group.enrollment_ack == "always" or (group.enrollment_ack == "triggered" and bool(trigger))
        )
        if acknowledge:
            self._stash_reply_token(aid, group.group_id, event, user_id)
            await self.send_text(group.group_id, "已通知管理者確認身分；核准後請再傳一次訊息。", account_id=aid)

    # ------------------------------------------------------------------
    # Admin commands (DM only)
    # ------------------------------------------------------------------

    async def admin_command(self, account: LineAccount, admin_id: str, text: str) -> str:
        aid = account.account_id
        parts = text.strip().split(maxsplit=2)
        root = parts[0]
        sub = parts[1].lower() if len(parts) > 1 else ""
        arg = parts[2].strip() if len(parts) > 2 else ""

        if root == "/help":
            return HELP_TEXT

        if root == "/member":
            if sub == "pending":
                pending = self.store.pending_requests(aid)
                if not pending:
                    return "目前沒有待核准的成員。"
                lines = []
                for req in pending:
                    group = account.group(req.group_id)
                    known = self.store.get_person(aid, req.user_id)
                    extra = f"；已登記為 {known['local_name']}" if known else ""
                    lines.append(
                        f"{req.code}　{group.name if group else '未設定群組'}　"
                        f"顯示名稱 {req.display_hint or '未取得'}{extra}"
                    )
                return "\n".join(lines)
            if sub == "approve":
                bits = arg.split(maxsplit=1)
                if not bits:
                    return "用法：/member approve <核准碼> <姓名> | <角色> | <關係>"
                code, rest = bits[0], (bits[1].strip() if len(bits) > 1 else "")
                request = self.store.request_by_code(aid, code)
                if request is None:
                    return "找不到這個核准碼。用 /member pending 查看。"
                rest = rest.replace("｜", "|")
                fields = [f.strip() for f in rest.split("|")] if rest else []
                known = self.store.get_person(aid, request.user_id)
                if known is None and len(fields) == 1 and len(fields[0].split()) >= 2:
                    # New member typed "姓名 角色 [關係]" with spaces instead of "|".
                    fields = fields[0].split(maxsplit=2)
                if any(len(f) > 100 or "\x00" in f for f in fields):
                    return "欄位太長（每欄最多 100 字）。"
                kwargs: dict[str, Any] = {}
                if len(fields) >= 2:
                    if not fields[0] or not fields[1]:
                        return "姓名與角色都要填。"
                    kwargs = {"local_name": fields[0], "role": fields[1]}
                    if len(fields) >= 3:
                        kwargs["relation"] = fields[2]
                elif len(fields) == 1 and fields[0]:
                    if known is None:
                        return f"這是新成員，請給姓名與角色，例如：/member approve {code} 小明 表弟"
                    kwargs = {"group_name": fields[0]}
                elif known is None:
                    return f"這是新成員，請給姓名與角色，例如：/member approve {code} 小明 表弟"
                identity = self.store.approve_request(aid, code, decided_by=admin_id, **kwargs)
                group = account.group(request.group_id)
                logger.info("[line_platform] member approved group=%s", group.name if group else "?")
                return f"已核准：{identity.card()}（群組：{group.name if group else '未設定'}）"
            if sub == "deny":
                if not arg:
                    return "用法：/member deny <核准碼>"
                try:
                    self.store.deny_request(aid, arg.split()[0], decided_by=admin_id)
                except KeyError:
                    return "找不到這個核准碼。"
                return "已拒絕。此人在該群組的訊息不會送到機器人。"
            return "用法：/member pending｜approve｜deny"

        if root == "/group":
            if sub == "list":
                if not account.groups:
                    return "尚未設定任何群組。"
                return "\n".join(
                    f"{g.name}　模式 {g.mode}　{self._backend_text(g)}　"
                    f"成員 {len(self.store.group_members(aid, g.group_id))} 人"
                    for g in account.groups.values()
                )
            if sub == "add":
                bits = arg.split()
                usage = "用法：/group add <設定碼> <群組標籤> [llm [模型] | muse [Side Chat 代號]]"
                if len(bits) < 2 or len(bits) > 4:
                    return usage
                gid = bits[0] if valid_group_id(bits[0]) else self.store.get_setting(f"join:{aid}:{bits[0].upper()}")
                if not gid:
                    return "找不到這個設定碼。"
                if gid in account.groups:
                    return f"這個群組已設定為「{account.groups[gid].name}」。"
                label = bits[1]
                if not valid_label(label):
                    return "群組標籤只能用文字、數字、底線或連字號，最多 32 字。"
                if account.find_group(label) is not None:
                    return f"標籤「{label}」已被使用。"
                changes = self._backend_changes(bits[2:])
                if isinstance(changes, str):
                    return changes
                group = GroupConfig(group_id=gid, bot_names=list(account.bot_names))
                account.groups[gid] = group
                self._save_group_override(account, group, label=label, mode="active", **changes)
                logger.info("[line_platform] group added label=%s backend=%s", label, group.backend)
                return (
                    f"已設定群組「{label}」：{self._backend_text(group)}。\n"
                    "成員第一次發言時，我會私訊你核准身分。"
                )
            if sub == "backend":
                bits = arg.split()
                if len(bits) < 2 or len(bits) > 3:
                    return "用法：/group backend <群組標籤> llm [模型] | muse [Side Chat 代號]"
                group = self._resolve_group(account, bits[0])
                if isinstance(group, str):
                    return group
                changes = self._backend_changes(bits[1:])
                if isinstance(changes, str):
                    return changes
                self._save_group_override(account, group, **changes)
                logger.info("[line_platform] backend for group=%s set to %s", group.name, group.backend)
                return f"群組「{group.name}」：{self._backend_text(group)}。"
            if sub in {"model", "muse", "mode"}:
                bits = arg.split()
                if len(bits) != 2:
                    return f"用法：/group {sub} <群組標籤> <值>"
                group = self._resolve_group(account, bits[0])
                if isinstance(group, str):
                    return group
                if sub == "model":
                    model = resolve_model(bits[1])
                    if model is None:
                        return "模型請填 sonnet、opus、haiku，或完整的模型 ID。"
                    self._save_group_override(account, group, model=model)
                elif sub == "muse":
                    if len(bits[1]) > 200:
                        return "Side Chat 代號太長。"
                    self._save_group_override(account, group, muse_chat=bits[1])
                else:
                    if bits[1].lower() not in GROUP_MODES:
                        return "模式請填 shadow、quiet 或 active。"
                    self._save_group_override(account, group, mode=bits[1].lower())
                    return f"群組「{group.name}」的模式已改為 {group.mode}。"
                return f"群組「{group.name}」：{self._backend_text(group)}。"
            if sub == "summary":
                group = self._resolve_group(account, arg)
                if isinstance(group, str):
                    return group
                current = self.store.get_summary(aid, group.group_id)
                if not current or not current["summary"].strip():
                    return f"群組「{group.name}」還沒有摘要。"
                updated = datetime.fromtimestamp(
                    current["updated_ms"] / 1000, timezone(timedelta(hours=account.utc_offset_hours))
                ).strftime("%m/%d %H:%M")
                return f"群組「{group.name}」摘要（更新於 {updated}）\n{current['summary']}"
            return "用法：/group list｜add｜backend｜model｜muse｜mode｜summary（/help 看完整說明）"

        if root == "/kb":
            if sub == "reload":
                if not account.groups:
                    return "尚未設定任何群組。"
                lines = []
                for group in account.groups.values():
                    kb, pinned = self.group_documents(account, group, force=True)
                    kb_state = kb.error or (f"版本 {kb.version}，{len(kb.body)} 字" if kb.exists else "找不到檔案")
                    pin_state = pinned.error or (f"{len(pinned.body)} 字" if pinned.exists else "找不到檔案")
                    lines.append(f"{group.name}：知識庫 {kb_state}；定案事實 {pin_state}")
                return "\n".join(lines)
            return "用法：/kb reload"

        if root == "/journal":
            if sub == "stats":
                stats = self.store.stats(aid)
                lines = [f"全文檢索：{'FTS5' if stats['fts'] else 'LIKE 後備'}"]
                for chat in stats["chats"]:
                    group = account.group(chat["chat_id"])
                    name = group.name if group else ("私訊" if chat["chat_type"] == "dm" else chat["chat_id"])
                    lines.append(f"{name}：收 {chat['inbound']} 則，發 {chat['outbound']} 則")
                for chat in stats["unjournaled"]:
                    lines.append(
                        f"未設定的{'群組' if chat['chat_type'] == 'group' else '私訊'} {chat['chat_id']}："
                        f"看到 {chat['count']} 次（未記錄內容）"
                    )
                return "\n".join(lines)
            return "用法：/journal stats"

        return HELP_TEXT

    @staticmethod
    def _backend_changes(bits: list[str]) -> dict[str, Any] | str:
        """Parse ``llm [model]`` / ``muse [side chat]`` (empty = llm)."""
        backend = bits[0].lower() if bits else "llm"
        if backend not in BACKENDS:
            return "回答來源請填 llm 或 muse。"
        changes: dict[str, Any] = {"backend": backend}
        if len(bits) > 1:
            if backend == "llm":
                model = resolve_model(bits[1])
                if model is None:
                    return "模型請填 sonnet、opus、haiku，或完整的模型 ID。"
                changes["model"] = model
            else:
                if len(bits[1]) > 200:
                    return "Side Chat 代號太長。"
                changes["muse_chat"] = bits[1]
        return changes

    @staticmethod
    def _backend_text(group: GroupConfig) -> str:
        if group.backend == "muse":
            return f"接 Muse（Side Chat：{group.muse_chat or '尚未綁定'}）"
        return f"接 LLM（模型：{group.model or '預設'}）"

    def group_backend(self, account: LineAccount, group: GroupConfig) -> str:
        return group.backend

    def _resolve_group(self, account: LineAccount, key: str) -> GroupConfig | str:
        if key:
            group = account.find_group(key)
            return group if group else f"找不到群組「{key}」。用 /group list 查看。"
        if len(account.groups) == 1:
            return next(iter(account.groups.values()))
        return "請指定群組：/group summary <群組標籤>（用 /group list 查看）"

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    def _client(self, account_id: str) -> LineClient:
        client = self.clients.get(account_id)
        if client is None:
            raise RuntimeError("LINE connector is not connected")
        return client

    def _stash_reply_token(
        self, account_id: str, chat_id: str, event: dict[str, Any], user_id: Optional[str]
    ) -> None:
        token = event.get("replyToken")
        if user_id:
            self._answering[(account_id, chat_id)] = user_id
        if not token:
            return
        ttl = self.accounts[account_id].reply_token_ttl_s
        self._reply_tokens[(account_id, chat_id)] = (str(token), time.monotonic() + ttl)

    def _take_reply_token(self, account_id: str, chat_id: str) -> Optional[str]:
        """Reply tokens are single-use and expire about a minute after the
        webhook; an expired one is discarded so the send goes to Push."""
        token, expires = self._reply_tokens.pop((account_id, chat_id), ("", 0.0))
        if not token or time.monotonic() >= expires:
            return None
        return token

    def account_for_chat(self, chat_id: str, account_id: Optional[str] = None) -> LineAccount:
        if account_id and account_id in self.accounts:
            return self.accounts[account_id]
        found = self._chat_accounts.get(chat_id) or self.store.account_for_chat(chat_id)
        if found in self.accounts:
            return self.accounts[found]
        if len(self.accounts) > 1:
            logger.warning("[line_platform] no account known for this chat; using the first configured account")
        return next(iter(self.accounts.values()))

    async def send_text(
        self, chat_id: str, content: str, *, account_id: Optional[str] = None, use_reply_token: bool = True
    ) -> SendOutcome:
        account = self.account_for_chat(chat_id, account_id)
        aid = account.account_id
        group = account.group(chat_id)
        if group is not None and group.mode == "shadow":
            logger.warning("[line_platform] refused to send into shadow-mode group %s", group.name)
            return SendOutcome(False, error="group is in shadow mode")
        chunks = split_text(strip_markdown(content), TEXT_CHUNK_LIMIT)
        if not chunks:
            return SendOutcome(True)
        client = self._client(aid)
        messages = [{"type": "text", "text": chunk} for chunk in chunks]
        sent: list[dict[str, Any]] = []
        methods: list[str] = []
        try:
            token = self._take_reply_token(aid, chat_id) if (account.reply_first and use_reply_token) else None
            rest = messages
            if token:
                try:
                    sent += await client.reply_message(token, messages[:MAX_MESSAGES_PER_CALL])
                    methods.append("reply")
                    rest = messages[MAX_MESSAGES_PER_CALL:]
                except LineApiError as exc:
                    logger.info("[line_platform] reply token rejected (%s); falling back to push", exc.status)
            for start in range(0, len(rest), MAX_MESSAGES_PER_CALL):
                sent += await client.push_message(chat_id, rest[start:start + MAX_MESSAGES_PER_CALL])
                if "push" not in methods:
                    methods.append("push")
        except Exception as exc:
            logger.error("[line_platform] send failed: %s", exc)
            return SendOutcome(False, method="+".join(methods), error=str(exc))
        finally:
            answering = self._answering.get((aid, chat_id))
            chat_type = "dm" if chat_id.startswith("U") else "group"
            delivered = len(sent) if sent else (len(chunks) if methods else 0)
            for index, chunk in enumerate(chunks[:delivered]):
                meta = sent[index] if index < len(sent) else {}
                self.store.record_event(
                    account_id=aid, chat_type=chat_type, chat_id=chat_id, user_id=answering, direction="out",
                    message_id=str(meta["id"]) if meta.get("id") else None, msg_type="text", text=chunk,
                )
        logger.info("[line_platform] sent via %s account=%s chunks=%d", "+".join(methods), aid, len(chunks))
        return SendOutcome(True, method="+".join(methods))

    # ------------------------------------------------------------------
    # Read-only access for other tools (nightly review, Muse)
    # ------------------------------------------------------------------

    def day_messages(self, account_id: str, group_key: str, day: str) -> list[EventRow]:
        account = self.accounts[account_id]
        group = account.find_group(group_key)
        if group is None:
            raise KeyError(group_key)
        return self.store.messages_on(
            account_id, group.group_id, day, utc_offset_hours=account.utc_offset_hours
        )
