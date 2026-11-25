"""Standalone service: webhook, LLM/Muse backends, local API."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sys
import types

import pytest
from aiohttp.test_utils import TestClient, TestServer

from conftest import ADMIN, ALICE, BOB, GROUP_A, GROUP_B, FakeClient, base_extra, run

from line_platform.standalone import Service, claude_completion
from line_platform.store import Store

TOKEN = "local-api-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


class StubModel:
    def __init__(self, answer="登船日是 2/14。"):
        self.answer = answer
        self.calls = []
        self.fail = False

    async def __call__(self, system, user, model=None):
        self.calls.append({"system": system, "user": user, "model": model})
        if self.fail:
            raise RuntimeError("boom")
        return self.answer


def make_service(tmp_path, model=None, **group_overrides):
    config = base_extra(**group_overrides)
    config["api"] = {"token": TOKEN}
    service = Service(config, store=Store(tmp_path / "data" / "line.db"), completion=model)
    service.fake = FakeClient()
    service.engine.clients = {"default": service.fake}
    for user, name, role in ((ALICE, "阿嬤", "奶奶"), (BOB, "Bob", "uncle")):
        service.engine.store.register_member("default", GROUP_A, user, local_name=name, role=role)
    return service


_n = 0


def group_event(text, user=ALICE, group=GROUP_A):
    global _n
    _n += 1
    return {
        "type": "message", "webhookEventId": f"s{_n}", "timestamp": 1_795_000_000_000 + _n * 1000,
        "source": {"type": "group", "groupId": group, "userId": user}, "replyToken": f"tok{_n}",
        "message": {"type": "text", "id": f"sm{_n}", "text": text},
    }


async def feed(service, event):
    await service.engine.handle_event(service.accounts[0], event)
    await service.engine.drain()


def sign(body: bytes, secret="test-secret") -> str:
    return base64.b64encode(hmac.new(secret.encode(), body, hashlib.sha256).digest()).decode()


# ---------------------------------------------------------------- webhook


def test_webhook_rejects_bad_signatures_and_accepts_good_ones(tmp_path):
    model = StubModel()
    service = make_service(tmp_path, model)

    async def scenario():
        async with TestClient(TestServer(service.webhook_app())) as client:
            body = json.dumps({"events": [group_event("巧可 登船日是哪天？")]}).encode()
            assert (await client.post("/webhook/line", data=body)).status == 401
            bad = await client.post("/webhook/line", data=body, headers={"X-Line-Signature": sign(body, "wrong")})
            assert bad.status == 403
            tampered = await client.post(
                "/webhook/line", data=body + b" ", headers={"X-Line-Signature": sign(body)}
            )
            assert tampered.status == 403
            assert service.engine.store.count_messages_after("default", GROUP_A, 0) == 0
            ok = await client.post("/webhook/line", data=body, headers={"X-Line-Signature": sign(body)})
            assert ok.status == 200
            await service.engine.drain()
            # LINE's "Verify" button posts an empty event list.
            empty = b'{"events":[]}'
            assert (await client.post("/webhook/line", data=empty, headers={"X-Line-Signature": sign(empty)})).status == 200
            # Nothing but the webhook and health check is exposed on this port.
            assert (await client.get("/api/v1/groups")).status == 404

    run(scenario())
    assert len(model.calls) == 1
    assert service.fake.replies[-1][1] == ["登船日是 2/14。"]
    service.engine.store.close()


# ------------------------------------------------------------ llm backend


def test_llm_backend_prompt_forbids_guessing_when_kb_lacks_answer(tmp_path):
    model = StubModel("我不知道，請問禮揚。")
    service = make_service(tmp_path, model)
    kb_path, _ = service.engine._doc_paths(service.accounts[0], service.accounts[0].groups[GROUP_A])
    kb_path.parent.mkdir(parents=True, exist_ok=True)
    kb_path.write_text("---\nkb_version: 1\n---\n登船日是 2/14。\n", encoding="utf-8")
    run(feed(service, group_event("巧可 岸上觀光多少錢？")))
    call = model.calls[0]
    assert "登船日是 2/14" in call["system"]
    assert "直接說不知道" in call["system"] and "不得編造" in call["system"] and "禮揚" in call["system"]
    assert "岸上觀光" not in call["system"]
    assert call["user"].endswith("阿嬤：巧可 岸上觀光多少錢？")
    assert call["model"] is None  # service default (Sonnet)
    assert service.fake.replies[-1][1] == ["我不知道，請問禮揚。"]
    service.engine.store.close()


def test_per_group_model_is_passed_to_the_model_call(tmp_path):
    model = StubModel()
    service = make_service(tmp_path, model)
    run(feed(service, {**group_event("/group model disney opus", user=ADMIN), "source": {"type": "user", "userId": ADMIN}}))
    run(feed(service, group_event("巧可 登船日是哪天？")))
    assert model.calls[-1]["model"] == "claude-opus-5-5"
    service.engine.store.close()


def test_llm_failure_sends_a_safe_reply(tmp_path):
    model = StubModel()
    model.fail = True
    service = make_service(tmp_path, model)
    run(feed(service, group_event("巧可 登船日是哪天？")))
    assert "沒辦法回答" in service.fake.replies[-1][1][0]
    service.engine.store.close()


def test_non_admin_dm_gets_no_model_call(tmp_path):
    model = StubModel()
    config_service = make_service(tmp_path, model)
    config_service.accounts[0].dm_policy = "open"
    event = {**group_event("登船日是哪天？"), "source": {"type": "user", "userId": ALICE}}
    run(feed(config_service, event))
    assert model.calls == []
    assert "管理指令" in config_service.fake.replies[-1][1][0]
    config_service.engine.store.close()


def test_claude_completion_request_shape():
    seen = {}

    class Messages:
        async def create(self, **kwargs):
            seen.update(kwargs)
            return types.SimpleNamespace(
                stop_reason="end_turn", content=[types.SimpleNamespace(type="text", text=" 好 ")]
            )

    client = types.SimpleNamespace(beta=types.SimpleNamespace(messages=Messages()))
    complete = claude_completion(client=client)
    assert run(complete("SYS", "USER")) == "好"
    assert seen["model"] == "claude-sonnet-5-5"
    assert seen["system"][0]["text"] == "SYS" and seen["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert seen["messages"] == [{"role": "user", "content": "USER"}]
    run(complete("SYS", "USER", model="claude-opus-5-5"))
    assert seen["model"] == "claude-opus-5-5"


# ------------------------------------------------- muse backend + local API


def test_muse_queue_and_local_api(tmp_path):
    model = StubModel()
    service = make_service(tmp_path, model, backend="muse", muse_chat="side-1")
    store = service.engine.store

    async def scenario():
        await feed(service, group_event("訂位編號 ABX12345，10:30 大廳集合", user=BOB))
        await feed(service, group_event("巧可 登船日是哪天？"))
        assert model.calls == []  # Muse answers, not the model
        assert service.fake.replies == []
        async with TestClient(TestServer(service.api_app())) as client:
            assert (await client.get("/api/v1/groups")).status == 401
            assert (await client.get("/api/v1/groups", headers={"Authorization": "Bearer nope"})).status == 401

            groups = (await (await client.get("/api/v1/groups", headers=AUTH)).json())["groups"]
            disney = next(g for g in groups if g["group"] == "disney")
            assert disney["backend"] == "muse" and disney["muse_chat"] == "side-1"
            assert any("阿嬤" in m for m in disney["members"])

            turns = (await (await client.get("/api/v1/turns", headers=AUTH)).json())["turns"]
            assert len(turns) == 1
            turn = turns[0]
            assert turn["question"] == "巧可 登船日是哪天？" and turn["sender"] == "阿嬤"
            assert turn["muse_chat"] == "side-1" and "回答規則" in turn["system_prompt"]
            other = await client.get("/api/v1/turns", params={"muse_chat": "side-2"}, headers=AUTH)
            assert (await other.json())["turns"] == []

            claim = await client.post(f"/api/v1/turns/{turn['id']}/claim", headers=AUTH)
            assert claim.status == 200
            assert (await client.post(f"/api/v1/turns/{turn['id']}/claim", headers=AUTH)).status == 409
            reply = await client.post(f"/api/v1/turns/{turn['id']}/reply", json={"text": "2/14 登船"}, headers=AUTH)
            assert (await reply.json()) == {"ok": True, "method": "reply"}
            assert service.fake.replies[-1][1] == ["2/14 登船"]
            again = await client.post(f"/api/v1/turns/{turn['id']}/reply", json={"text": "再一次"}, headers=AUTH)
            assert again.status == 409

            # History: redacted by default, raw on request, scoped to one group.
            msgs = (await (await client.get("/api/v1/messages", params={"group": "disney"}, headers=AUTH)).json())["messages"]
            assert [m["speaker"] for m in msgs] == ["Bob", "阿嬤", "巧可"]
            assert "ABX12345" not in msgs[0]["text"] and "10:30" in msgs[0]["text"]
            raw = await client.get("/api/v1/messages", params={"group": "disney", "redact": "0"}, headers=AUTH)
            assert "ABX12345" in (await raw.json())["messages"][0]["text"]
            hits = await client.get("/api/v1/messages", params={"group": "disney", "q": "大廳集合"}, headers=AUTH)
            assert len((await hits.json())["messages"]) == 1
            empty = await client.get("/api/v1/messages", params={"group": "other"}, headers=AUTH)
            assert (await empty.json())["messages"] == []
            assert (await client.get("/api/v1/messages", params={"group": "nope"}, headers=AUTH)).status == 404
            bad_day = await client.get("/api/v1/messages", params={"group": "disney", "day": "yesterday"}, headers=AUTH)
            assert bad_day.status == 400

            sent = await client.post("/api/v1/send", json={"group": "disney", "text": "提醒：帶護照"}, headers=AUTH)
            assert (await sent.json())["method"] == "push"
            summary = await client.get("/api/v1/summary", params={"group": "disney"}, headers=AUTH)
            assert (await summary.json())["summary"] == ""

    run(scenario())
    assert store.get_turn(1)["status"] == "answered"
    store.close()


def test_stale_muse_turns_expire(tmp_path):
    service = make_service(tmp_path, None, backend="muse")
    service.turn_ttl_s = 0

    async def scenario():
        await feed(service, group_event("巧可 登船日是哪天？"))
        async with TestClient(TestServer(service.api_app())) as client:
            assert (await (await client.get("/api/v1/turns", headers=AUTH)).json())["turns"] == []

    run(scenario())
    assert service.engine.store.get_turn(1)["status"] == "expired"
    service.engine.store.close()


def test_api_cannot_send_into_shadow_group(tmp_path):
    service = make_service(tmp_path, None, mode="shadow")

    async def scenario():
        async with TestClient(TestServer(service.api_app())) as client:
            resp = await client.post("/api/v1/send", json={"group": "disney", "text": "嗨"}, headers=AUTH)
            assert resp.status == 502

    run(scenario())
    assert service.fake.pushes == []
    service.engine.store.close()
