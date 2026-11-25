"""Shared fixtures. Stubs ``gateway.*`` so the Hermes adapter imports
without a Hermes runtime, and provides a fake LINE API client."""

from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _install_gateway_stubs() -> None:
    if "gateway" in sys.modules:
        return
    gateway = types.ModuleType("gateway")
    config_mod = types.ModuleType("gateway.config")
    platforms_mod = types.ModuleType("gateway.platforms")
    base_mod = types.ModuleType("gateway.platforms.base")

    class _Platform(str):
        def __new__(cls, value):
            return str.__new__(cls, value)

    class _PlatformConfig:
        def __init__(self, extra=None):
            self.extra = extra or {}

    class _MessageType:
        TEXT = "text"
        PHOTO = "photo"
        VOICE = "voice"

    class _SendResult:
        def __init__(self, success=True, error=None, retryable=False):
            self.success = success
            self.error = error
            self.retryable = retryable

    class _MessageEvent:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class _BasePlatformAdapter:
        def __init__(self, config, platform):
            self.config = config
            self.platform = platform
            self._background_tasks = set()
            self.handled = []

        def build_source(self, **kwargs):
            return types.SimpleNamespace(**kwargs)

        def format_message(self, text):
            return text

        async def handle_message(self, event):
            self.handled.append(event)

        def _set_fatal_error(self, *a, **kw):
            pass

        def _mark_connected(self):
            pass

        def _mark_disconnected(self):
            pass

    config_mod.Platform = _Platform
    config_mod.PlatformConfig = _PlatformConfig
    base_mod.BasePlatformAdapter = _BasePlatformAdapter
    base_mod.MessageEvent = _MessageEvent
    base_mod.MessageType = _MessageType
    base_mod.SendResult = _SendResult
    base_mod.cache_image_from_bytes = lambda data, ext=".jpg": f"/tmp/fake_image{ext}"
    base_mod.cache_audio_from_bytes = lambda data, ext=".m4a": f"/tmp/fake_audio{ext}"
    sys.modules["gateway"] = gateway
    sys.modules["gateway.config"] = config_mod
    sys.modules["gateway.platforms"] = platforms_mod
    sys.modules["gateway.platforms.base"] = base_mod


_install_gateway_stubs()

from line_platform.client import LineApiError  # noqa: E402
from line_platform.config import accounts_from_extra  # noqa: E402
from line_platform.engine import Engine  # noqa: E402
from line_platform.store import Store  # noqa: E402

ADMIN = "U" + "a" * 32
ALICE = "U" + "1" * 32
BOB = "U" + "2" * 32
CAROL = "U" + "3" * 32
GROUP_A = "C" + "a" * 32
GROUP_B = "C" + "b" * 32


class FakeClient:
    """Stands in for LineClient; records what would have gone to LINE."""

    def __init__(self):
        self.replies = []  # (token, [text, ...])
        self.pushes = []  # (to, [text, ...])
        self.reply_error = None  # HTTP status to fail replies with
        self.names = {}
        self._n = 0

    def _sent(self, messages):
        out = []
        for _ in messages:
            self._n += 1
            out.append({"id": f"out{self._n}"})
        return out

    async def reply_message(self, token, messages):
        if self.reply_error:
            raise LineApiError("reply", self.reply_error)
        self.replies.append((token, [m["text"] for m in messages]))
        return self._sent(messages)

    async def push_message(self, to, messages):
        self.pushes.append((to, [m["text"] for m in messages]))
        return self._sent(messages)

    async def get_user_profile(self, user_id):
        return {"displayName": self.names.get(user_id, "")}

    async def get_group_member_profile(self, group_id, user_id):
        return {"displayName": self.names.get(user_id, "")}

    async def download_content(self, message_id):
        return b"bytes"

    def texts_to(self, chat_id):
        return [t for to, texts in self.pushes if to == chat_id for t in texts]


class Harness:
    def __init__(self, tmp_path, extra, summarizer=None):
        self.tmp_path = tmp_path
        self.extra = extra
        self.summarizer = summarizer
        self.turns = []
        self.client = FakeClient()
        self._n = 0
        self.open()

    def open(self):
        self.store = Store(self.tmp_path / "data" / "line_platform.db")
        self.accounts = accounts_from_extra(self.extra)
        self.account = self.accounts[0]
        self.engine = Engine(self.accounts, dispatch=self._dispatch, store=self.store, summarizer=self.summarizer)
        self.engine.clients = {a.account_id: self.client for a in self.accounts}

    def restart(self):
        """Simulate a gateway restart: drop all in-memory state."""
        self.store.close()
        self.open()

    async def _dispatch(self, turn):
        self.turns.append(turn)

    def event(self, text, *, user=ALICE, group=GROUP_A, mention=False, quoted=None, ts=None, etype="message"):
        self._n += 1
        source = {"type": "group", "groupId": group, "userId": user} if group else {"type": "user", "userId": user}
        message = {"type": "text", "id": f"m{self._n}", "text": text}
        if mention:
            message["mention"] = {"mentionees": [{"index": 0, "length": 3, "type": "user", "isSelf": True}]}
        if quoted:
            message["quotedMessageId"] = quoted
        event = {
            "type": etype, "webhookEventId": f"ev{self._n}", "timestamp": ts or (1_795_000_000_000 + self._n * 1000),
            "source": source, "replyToken": f"rt{self._n}",
        }
        if etype == "message":
            event["message"] = message
        return event

    async def say(self, text, **kwargs):
        account = kwargs.pop("account", None) or self.account
        event = self.event(text, **kwargs)
        await self.engine.handle_event(account, event)
        await self.engine.drain()
        return event

    async def dm(self, text, user=ADMIN):
        return await self.say(text, user=user, group=None)

    def approve_all(self, group=GROUP_A, users=((ALICE, "阿嬤", "奶奶"), (BOB, "Bob", "uncle"))):
        for user, name, role in users:
            self.store.register_member(self.account.account_id, group, user, local_name=name, role=role)


def base_extra(**group_overrides):
    group = {"label": "disney", "bot_names": ["巧可", "Choco"], "triggers": ["郵輪|行程|cruise"]}
    group.update(group_overrides)
    return {
        "channel_access_token": "test-token",
        "channel_secret": "test-secret",
        "group_policy": "allowlist",
        "admins": [ADMIN],
        "groups": {GROUP_A: group, GROUP_B: {"label": "other", "bot_names": ["巧可"]}},
    }


@pytest.fixture
def harness(tmp_path):
    made = []

    def make(extra=None, summarizer=None):
        h = Harness(tmp_path, extra or base_extra(), summarizer)
        made.append(h)
        return h

    yield make
    for h in made:
        try:
            h.store.close()
        except Exception:
            pass


def run(coro):
    return asyncio.run(coro)
