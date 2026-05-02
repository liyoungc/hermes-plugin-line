"""Lightweight unit tests for the LINE platform plugin.

These tests intentionally avoid the Hermes runtime — they exercise only
the pure helpers (signature verification, message parsing, text chunking,
config parsing). The full adapter requires a running gateway and a real
aiohttp event loop, which is covered by integration tests in the gateway
repo, not here.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import sys
import types
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Stub `gateway.*` so adapter.py imports cleanly outside the hermes runtime.
# We only need the symbols the adapter touches during these unit tests.
# ---------------------------------------------------------------------------


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

        def build_source(self, **kwargs):
            return types.SimpleNamespace(**kwargs)

        def format_message(self, text):
            return text

        def _set_fatal_error(self, *a, **kw):
            pass

        def _mark_connected(self):
            pass

        def _mark_disconnected(self):
            pass

    def _cache_image_from_bytes(data, ext=".jpg"):
        return f"/tmp/fake_image{ext}"

    def _cache_audio_from_bytes(data, ext=".m4a"):
        return f"/tmp/fake_audio{ext}"

    config_mod.Platform = _Platform
    config_mod.PlatformConfig = _PlatformConfig
    base_mod.BasePlatformAdapter = _BasePlatformAdapter
    base_mod.MessageEvent = _MessageEvent
    base_mod.MessageType = _MessageType
    base_mod.SendResult = _SendResult
    base_mod.cache_image_from_bytes = _cache_image_from_bytes
    base_mod.cache_audio_from_bytes = _cache_audio_from_bytes

    sys.modules["gateway"] = gateway
    sys.modules["gateway.config"] = config_mod
    sys.modules["gateway.platforms"] = platforms_mod
    sys.modules["gateway.platforms.base"] = base_mod


_install_gateway_stubs()
# Ensure plugin root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from line_platform import adapter as line_adapter  # noqa: E402


# ---------------------------------------------------------------------------
# Signature verification
# ---------------------------------------------------------------------------


def test_verify_signature_accepts_valid():
    secret = "shhh-secret"
    body = b'{"events":[]}'
    sig = base64.b64encode(
        hmac.new(secret.encode(), body, hashlib.sha256).digest()
    ).decode()
    assert line_adapter.verify_signature(body, sig, secret) is True


def test_verify_signature_rejects_bad():
    body = b'{"events":[]}'
    sig = base64.b64encode(
        hmac.new(b"other", body, hashlib.sha256).digest()
    ).decode()
    assert line_adapter.verify_signature(body, sig, "shhh-secret") is False


def test_verify_signature_rejects_empty():
    assert line_adapter.verify_signature(b"x", None, "s") is False
    assert line_adapter.verify_signature(b"x", "AAAA", "") is False


# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------


def test_parse_text_message():
    text, mtype, mid = line_adapter._parse_message_content({"type": "text", "text": "hi", "id": "m1"})
    assert text == "hi"
    assert mtype == line_adapter.MessageType.TEXT
    assert mid is None  # text needs no media download


def test_parse_image_message():
    text, mtype, mid = line_adapter._parse_message_content({"type": "image", "id": "m2"})
    assert text == ""
    assert mtype == line_adapter.MessageType.PHOTO
    assert mid == "m2"


def test_parse_sticker_message():
    text, mtype, mid = line_adapter._parse_message_content(
        {"type": "sticker", "packageId": "1", "stickerId": "100"}
    )
    assert text == "[貼圖: 1/100]"
    assert mtype == line_adapter.MessageType.TEXT


def test_parse_unsupported_type_drops():
    text, mtype, mid = line_adapter._parse_message_content({"type": "video", "id": "m3"})
    assert text == ""
    assert mid is None


# ---------------------------------------------------------------------------
# Text chunking
# ---------------------------------------------------------------------------


def test_split_text_short():
    assert line_adapter._split_text("hi", 100) == ["hi"]


def test_split_text_empty():
    assert line_adapter._split_text("", 100) == []


def test_split_text_breaks_on_whitespace():
    text = "word " * 50  # 250 chars total
    chunks = line_adapter._split_text(text, 60)
    assert len(chunks) >= 2
    for chunk in chunks:
        assert len(chunk) <= 60
    # No word should be split mid-character
    rejoined = " ".join(chunks).strip()
    assert rejoined.replace("  ", " ") == text.strip()


def test_strip_markdown():
    out = line_adapter._strip_markdown("**bold** and *italic* and `code`")
    assert out == "bold and italic and code"


# ---------------------------------------------------------------------------
# Account / config parsing
# ---------------------------------------------------------------------------


def test_account_from_extra_basic():
    extra = {
        "channel_access_token": "tok",
        "channel_secret": "sec",
        "default_persona": "cattia-line",
        "allow_from": ["U1", "U2"],
        "group_personas": {"Cabc": "mochi-line"},
        "dm_policy": "allowlist",
        "group_policy": "open",
    }
    account = line_adapter._account_from_extra(extra, "main")
    assert account.account_id == "main"
    assert account.has_credentials
    assert account.default_persona == "cattia-line"
    assert account.allow_from == ["U1", "U2"]
    assert account.group_personas == {"Cabc": "mochi-line"}
    assert account.dm_policy == "allowlist"
    assert account.group_policy == "open"


def test_accounts_from_config_multi():
    cfg = line_adapter.PlatformConfig(
        extra={
            "accounts": {
                "main": {
                    "channel_access_token": "t1",
                    "channel_secret": "s1",
                    "webhook_path": "/webhook/line",
                    "default_persona": "cattia-line",
                },
                "lynx": {
                    "channel_access_token": "t2",
                    "channel_secret": "s2",
                    "webhook_path": "/webhook/line/lynx",
                    "default_persona": "lynx-hospital",
                },
            }
        }
    )
    accounts = line_adapter._accounts_from_config(cfg)
    assert {a.account_id for a in accounts} == {"main", "lynx"}
    by_id = {a.account_id: a for a in accounts}
    assert by_id["main"].webhook_path == "/webhook/line"
    assert by_id["lynx"].webhook_path == "/webhook/line/lynx"
    assert by_id["lynx"].default_persona == "lynx-hospital"


def test_accounts_from_config_single_top_level():
    cfg = line_adapter.PlatformConfig(
        extra={"channel_access_token": "tok", "channel_secret": "sec"}
    )
    accounts = line_adapter._accounts_from_config(cfg)
    assert len(accounts) == 1
    assert accounts[0].account_id == "default"
    assert accounts[0].has_credentials


def test_validate_config_rejects_empty():
    cfg = line_adapter.PlatformConfig(extra={})
    assert line_adapter.validate_config(cfg) is False


def test_validate_config_accepts_credentialed():
    cfg = line_adapter.PlatformConfig(
        extra={"channel_access_token": "t", "channel_secret": "s"}
    )
    assert line_adapter.validate_config(cfg) is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
