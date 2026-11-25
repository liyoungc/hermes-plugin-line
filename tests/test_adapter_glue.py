"""Hermes adapter glue: turn → MessageEvent, and shadowing detection."""

from __future__ import annotations

import sys
import types

import pytest

from conftest import ALICE, GROUP_A, FakeClient, base_extra, run

from line_platform import adapter as adapter_mod
from line_platform.adapter import LineAdapter, PluginShadowedError, _assert_active
from line_platform.engine import Engine
from line_platform.store import Store


def test_adapter_dispatches_context_as_channel_prompt(tmp_path):
    from gateway.config import PlatformConfig

    adapter = LineAdapter(PlatformConfig(extra=base_extra()))
    adapter._auto_sethome_done = True
    store = Store(tmp_path / "data" / "line.db")
    adapter.engine = Engine(adapter.accounts, dispatch=adapter._dispatch_turn, store=store)
    adapter.engine.clients = {"default": FakeClient()}
    store.register_member("default", GROUP_A, ALICE, local_name="阿嬤", role="奶奶")
    event = {
        "type": "message", "webhookEventId": "g1", "timestamp": 1_795_000_000_000, "replyToken": "t",
        "source": {"type": "group", "groupId": GROUP_A, "userId": ALICE},
        "message": {"type": "text", "id": "gm1", "text": "巧可 登船日是哪天？"},
    }

    async def scenario():
        await adapter.engine.handle_event(adapter.accounts[0], event)
        await adapter.engine.drain()

    run(scenario())
    assert len(adapter.handled) == 1
    message = adapter.handled[0]
    assert message.text == "巧可 登船日是哪天？"
    assert message.source.chat_id == GROUP_A and message.source.user_name == "阿嬤"
    assert "回答規則" in message.channel_prompt and "阿嬤" in message.channel_prompt
    store.close()


def _registry(owner_factory):
    module = types.ModuleType("gateway.platform_registry")
    entry = types.SimpleNamespace(adapter_factory=owner_factory, plugin_name="line-platform")
    module.platform_registry = types.SimpleNamespace(get=lambda name: entry if name == "line" else None)
    return module


def test_assert_active_raises_when_bundled_adapter_owns_line(monkeypatch):
    monkeypatch.setitem(sys.modules, "gateway.platform_registry", _registry(lambda cfg: None))
    with pytest.raises(PluginShadowedError):
        _assert_active(adapter_mod._adapter_factory)


def test_assert_active_passes_when_this_plugin_owns_line(monkeypatch):
    monkeypatch.setitem(sys.modules, "gateway.platform_registry", _registry(adapter_mod._adapter_factory))
    _assert_active(adapter_mod._adapter_factory)
