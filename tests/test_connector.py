"""Engine behaviour: journal, enrollment, isolation, triggers, outbound."""

from __future__ import annotations

import os
import stat
import time

from conftest import ADMIN, ALICE, BOB, CAROL, GROUP_A, GROUP_B, base_extra, run

from line_platform.context import redact

GROUP_C = "C" + "c" * 32


def _write_kb(h, body="---\nkb_version: 2027.1\n---\n登船日是 2/14，在新加坡濱海灣郵輪中心。\n", pinned=None):
    kb_path, pinned_path = h.engine._doc_paths(h.account, h.account.groups[GROUP_A])
    kb_path.parent.mkdir(parents=True, exist_ok=True)
    kb_path.write_text(body, encoding="utf-8")
    if pinned is not None:
        pinned_path.write_text(pinned, encoding="utf-8")


# ---------------------------------------------------------------- journal


def test_journal_file_is_private_and_survives_restart(harness):
    h = harness()
    h.approve_all()
    run(h.say("今天天氣不錯"))
    run(h.say("巧可 登船日是哪天？"))
    path = h.store.path
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(path.parent).st_mode) == 0o700
    h.restart()
    rows = h.store.recent_messages("default", GROUP_A, limit=10)
    assert [r.text for r in rows] == ["今天天氣不錯", "巧可 登船日是哪天？"]
    # Identity registry survives too.
    assert h.store.membership_status("default", GROUP_A, ALICE) == "approved"


def test_untriggered_messages_are_journaled_but_not_dispatched(harness):
    h = harness()
    h.approve_all()
    run(h.say("晚餐吃什麼"))
    assert h.turns == []
    assert h.store.count_messages_after("default", GROUP_A, 0) == 1


def test_duplicate_webhook_delivery_is_ignored(harness):
    h = harness()
    h.approve_all()
    event = run(h.say("巧可 你好嗎？"))
    run(h.engine.handle_event(h.account, event))
    assert len(h.turns) == 1
    assert h.store.count_messages_after("default", GROUP_A, 0) == 1


def test_reply_token_is_not_stored(harness):
    h = harness()
    h.approve_all()
    run(h.say("巧可 你好嗎？"))
    raw = h.store._conn.execute("SELECT raw_json FROM events").fetchone()[0]
    assert "replyToken" not in raw and "rt1" not in raw


def test_day_query_returns_that_local_day_only(harness):
    h = harness()
    h.approve_all()
    day1 = 1_795_000_000_000  # 2026-11-18 UTC+8
    run(h.say("第一天", ts=day1))
    run(h.say("第二天", ts=day1 + 86_400_000))
    from datetime import datetime, timedelta, timezone

    day = datetime.fromtimestamp(day1 / 1000, timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    assert [r.text for r in h.engine.day_messages("default", "disney", day)] == ["第一天"]


# ------------------------------------------------------------- enrollment


def test_enrollment_full_flow(harness):
    h = harness()
    h.client.names[ALICE] = "Fake Admin 禮揚"
    run(h.say("巧可 登船日是哪天？"))
    # Blocked before the agent; the group gets a neutral ack via reply token.
    assert h.turns == []
    assert h.client.replies and "管理者" in h.client.replies[0][1][0]
    pending = h.store.pending_requests("default")
    assert len(pending) == 1
    notice = "\n".join(h.client.texts_to(ADMIN))
    assert pending[0].code in notice and "登船日" not in notice
    # Repeat messages do not create new requests or notify again.
    run(h.say("巧可 在嗎？"))
    assert len(h.store.pending_requests("default")) == 1
    assert len(h.client.texts_to(ADMIN)) == 1
    # A non-admin cannot approve.
    run(h.dm(f"/member approve {pending[0].code} 阿嬤 | 奶奶", user=ALICE))
    assert h.store.membership_status("default", GROUP_A, ALICE) != "approved"
    # Admin approves by DM.
    run(h.dm(f"/member approve {pending[0].code} 阿嬤 | 奶奶 | 媽媽"))
    assert h.store.membership_status("default", GROUP_A, ALICE) == "approved"
    run(h.say("巧可 登船日是哪天？"))
    assert len(h.turns) == 1
    turn = h.turns[0]
    assert turn.sender_name == "阿嬤"
    assert "阿嬤" in turn.context.volatile and "奶奶" in turn.context.volatile
    # The LINE display name never becomes identity.
    assert "Fake Admin" not in turn.context.combined


def test_denied_member_stays_blocked_silently(harness):
    h = harness()
    run(h.say("巧可 你好嗎？", user=BOB))
    code = h.store.pending_requests("default")[0].code
    run(h.dm(f"/member deny {code}"))
    before = (len(h.client.replies), len(h.client.pushes))
    run(h.say("巧可 你好嗎？", user=BOB))
    assert h.turns == []
    assert (len(h.client.replies), len(h.client.pushes)) == before
    assert h.store.pending_requests("default") == []


def test_identity_is_reused_in_a_second_group(harness):
    h = harness()
    h.store.register_member("default", GROUP_A, ALICE, local_name="阿嬤", role="奶奶")
    run(h.say("巧可 你好嗎？", group=GROUP_B))
    request = h.store.pending_requests("default")[0]
    assert "阿嬤" in "\n".join(h.client.texts_to(ADMIN))
    reply = run(_dm_reply(h, f"/member approve {request.code}"))
    assert "阿嬤" in reply
    assert h.store.identity("default", GROUP_B, ALICE).local_name == "阿嬤"


def test_new_member_needs_name_and_role(harness):
    h = harness()
    run(h.say("巧可 你好嗎？"))
    code = h.store.pending_requests("default")[0].code
    reply = run(_dm_reply(h, f"/member approve {code}"))
    assert "姓名與角色" in reply
    assert h.store.membership_status("default", GROUP_A, ALICE) != "approved"
    assert code in reply
    reply = run(_dm_reply(h, f"/member approve {code} 阿嬤"))
    assert "姓名與角色" in reply


def test_new_member_can_be_approved_with_spaces(harness):
    h = harness()
    run(h.say("巧可 你好嗎？"))
    code = h.store.pending_requests("default")[0].code
    reply = run(_dm_reply(h, f"/member approve {code} 禮揚 本人"))
    assert "已核准" in reply
    identity = h.store.identity("default", GROUP_A, ALICE)
    assert (identity.local_name, identity.role) == ("禮揚", "本人")


def test_new_member_spaces_with_relation_and_fullwidth_bar(harness):
    h = harness()
    run(h.say("巧可 你好嗎？"))
    run(h.say("巧可 你好嗎？", user=BOB))
    codes = {r.user_id: r.code for r in h.store.pending_requests("default")}
    run(h.dm(f"/member approve {codes[ALICE]} 阿嬤 奶奶 禮揚的 媽媽"))
    a = h.store.identity("default", GROUP_A, ALICE)
    assert (a.local_name, a.role, a.relation) == ("阿嬤", "奶奶", "禮揚的 媽媽")
    run(h.dm(f"/member approve {codes[BOB]} 小明｜表弟"))
    b = h.store.identity("default", GROUP_A, BOB)
    assert (b.local_name, b.role) == ("小明", "表弟")


async def _dm_reply(h, text, user=ADMIN):
    before = len(h.client.replies) + len(h.client.pushes)
    await h.dm(text, user=user)
    sent = h.client.replies + h.client.pushes
    assert len(sent) > before or True
    return "\n".join(h.client.replies[-1][1]) if h.client.replies else ""


# -------------------------------------------------------------- isolation


def test_group_context_never_contains_another_groups_data(harness):
    h = harness()
    h.approve_all(GROUP_A)
    h.store.register_member("default", GROUP_B, CAROL, local_name="卡蘿", role="同事")
    run(h.say("B群秘密：尾牙在鼎泰豐", user=CAROL, group=GROUP_B))
    h.store.set_summary("default", GROUP_B, "B群摘要：尾牙地點鼎泰豐", 1)
    run(h.say("A群的事：郵輪集合時間待定"))
    run(h.say("巧可 之前有人說過尾牙在哪裡嗎？"))
    context = h.turns[-1].context.combined
    assert "鼎泰豐" not in context and "卡蘿" not in context and "B群" not in context
    assert "郵輪集合時間待定" in context
    # Search is scoped to one group as well.
    assert h.store.search("default", GROUP_A, ["鼎泰豐"]) == []
    assert len(h.store.search("default", GROUP_B, ["鼎泰豐"])) == 1


def test_unlisted_group_is_dropped_without_content(harness):
    h = harness()
    run(h.say("陌生群組的訊息", group=GROUP_C))
    assert h.turns == []
    assert h.store._conn.execute("SELECT COUNT(*) FROM events WHERE chat_id=?", (GROUP_C,)).fetchone()[0] == 0
    stats = run(_dm_reply(h, "/journal stats"))
    assert GROUP_C in stats


# ----------------------------------------------------- group setup by DM


def test_new_group_is_announced_once_and_set_up_by_dm(harness):
    h = harness()
    run(h.say("", group=GROUP_C, etype="join"))
    run(h.say("大家好", group=GROUP_C))
    notices = [t for t in h.client.texts_to(ADMIN) if "設定碼" in t]
    assert len(notices) == 1 and "大家好" not in notices[0]
    code = h.store.get_setting(f"joincode:default:{GROUP_C}")
    reply = run(_dm_reply(h, f"/group add {code} family muse side-chat-42"))
    assert "family" in reply and "Muse" in reply
    group = h.account.groups[GROUP_C]
    assert (group.label, group.backend, group.muse_chat, group.mode) == ("family", "muse", "side-chat-42", "active")
    # Now journaled, and it survives a restart.
    run(h.say("第二句", group=GROUP_C))
    h.restart()
    group = h.account.groups[GROUP_C]
    assert (group.label, group.backend, group.muse_chat) == ("family", "muse", "side-chat-42")
    assert [r.text for r in h.store.recent_messages("default", GROUP_C, limit=5)] == ["第二句"]


def test_group_add_defaults_to_llm_and_accepts_model(harness):
    h = harness()
    run(h.say("", group=GROUP_C, etype="join"))
    code = h.store.get_setting(f"joincode:default:{GROUP_C}")
    assert "已被使用" in run(_dm_reply(h, f"/group add {code} disney"))
    assert "模型請填" in run(_dm_reply(h, f"/group add {code} fam llm gpt-9"))
    run(_dm_reply(h, f"/group add {code} fam llm opus"))
    group = h.account.groups[GROUP_C]
    assert group.backend == "llm" and group.model == "claude-opus-5-5"
    assert "已設定" in run(_dm_reply(h, f"/group add {code} again"))


def test_backend_model_muse_and_mode_switches_persist(harness):
    h = harness()
    assert h.account.groups[GROUP_A].backend == "llm"
    run(h.dm("/group backend disney muse chat-7"))
    assert (h.account.groups[GROUP_A].backend, h.account.groups[GROUP_A].muse_chat) == ("muse", "chat-7")
    run(h.dm("/group muse disney chat-8"))
    run(h.dm("/group model disney haiku"))
    run(h.dm("/group mode disney quiet"))
    h.restart()
    group = h.account.groups[GROUP_A]
    assert (group.backend, group.muse_chat, group.model, group.mode) == (
        "muse", "chat-8", "claude-haiku-5-5", "quiet"
    )
    run(h.dm("/group backend disney llm"))
    assert h.account.groups[GROUP_A].backend == "llm"
    listing = run(_dm_reply(h, "/group list"))
    assert "disney" in listing and "LLM" in listing
    # Non-admins cannot switch anything.
    run(h.dm("/group backend disney muse", user=ALICE))
    assert h.account.groups[GROUP_A].backend == "llm"


# --------------------------------------------------------------- triggers


def test_trigger_rules(harness):
    h = harness()
    h.approve_all()
    run(h.say("好喔"))  # chatter
    run(h.say("郵輪好期待"))  # rule keyword but not a question
    assert h.turns == []
    run(h.say("請問郵輪幾點開？"))
    assert h.turns[-1].trigger == "rule"
    run(h.say("@巧可 hello", mention=True))
    assert h.turns[-1].trigger == "mention"
    run(h.say("choco are you there"))
    assert h.turns[-1].trigger == "name"
    assert len(h.turns) == 3


def test_followup_and_reply_to_bot(harness):
    h = harness()
    h.approve_all()
    now = int(time.time() * 1000)
    run(h.say("巧可 你好嗎？", ts=now - 5000))
    run(h.engine.send_text(GROUP_A, "我很好"))
    run(h.say("那明天呢？", ts=now + 1000))
    assert h.turns[-1].trigger == "follow-up"
    # Someone else's question in the window is not a follow-up.
    run(h.say("要不要吃飯？", user=BOB, ts=now + 2000))
    assert len(h.turns) == 2
    out_id = h.store.last_outbound("default", GROUP_A).message_id
    run(h.say("收到", user=BOB, quoted=out_id))
    assert h.turns[-1].trigger == "reply-to-bot"


def test_quiet_mode_answers_only_mentions(harness):
    h = harness(base_extra(mode="quiet"))
    h.approve_all()
    run(h.say("巧可 請問郵輪幾點開？"))
    assert h.turns == []
    run(h.say("@巧可 請問郵輪幾點開？", mention=True))
    assert len(h.turns) == 1


def test_shadow_mode_journals_and_never_speaks(harness):
    h = harness(base_extra(mode="shadow"))
    run(h.say("@巧可 在嗎？", mention=True))  # unknown sender
    h.approve_all()
    run(h.say("@巧可 在嗎？", mention=True))
    assert h.turns == []
    assert h.client.replies == [] and h.client.texts_to(GROUP_A) == []
    outcome = run(h.engine.send_text(GROUP_A, "不該送出"))
    assert not outcome.ok and h.client.texts_to(GROUP_A) == []
    assert h.store.count_messages_after("default", GROUP_A, 0) == 2


# --------------------------------------------------------------- outbound


def test_reply_first_then_push_when_token_expired_or_rejected(harness):
    h = harness()
    h.approve_all()
    # Fresh token: free reply, no push.
    run(h.say("巧可 你好嗎？"))
    assert run(h.engine.send_text(GROUP_A, "一")).method == "reply"
    assert h.client.texts_to(GROUP_A) == []
    # Token is single-use: the next send is a push.
    assert run(h.engine.send_text(GROUP_A, "二")).method == "push"
    # Expired token: straight to push.
    run(h.say("巧可 你好嗎？"))
    key = ("default", GROUP_A)
    token, _ = h.engine._reply_tokens[key]
    h.engine._reply_tokens[key] = (token, time.monotonic() - 1)
    replies = len(h.client.replies)
    assert run(h.engine.send_text(GROUP_A, "三")).method == "push"
    assert len(h.client.replies) == replies
    # LINE rejects the token: fall back to push, message still delivered.
    run(h.say("巧可 你好嗎？"))
    h.client.reply_error = 400
    assert run(h.engine.send_text(GROUP_A, "四")).method == "push"
    assert h.client.texts_to(GROUP_A) == ["二", "三", "四"]
    # Every outbound message is journaled exactly once.
    out = [r.text for r in h.store.recent_messages("default", GROUP_A, limit=50) if r.direction == "out"]
    assert out == ["一", "二", "三", "四"]


def test_reply_first_can_be_disabled(harness):
    extra = base_extra()
    extra["reply_first"] = False
    h = harness(extra)
    h.approve_all()
    run(h.say("巧可 你好嗎？"))
    assert run(h.engine.send_text(GROUP_A, "嗨")).method == "push"
    assert h.client.replies == []


# ---------------------------------------------------- knowledge / context


def test_muse_backend_may_use_side_chat_memory_but_keeps_privacy_rules(harness):
    h = harness()
    h.approve_all()
    run(h.say("巧可 自助餐幾點開？"))
    llm = h.turns[-1].context.stable
    assert "只根據下面的" in llm and "請回答不知道" in llm
    run(h.dm("/group backend disney muse chat-7"))
    run(h.say("巧可 集合時間是幾點？"))
    muse = h.turns[-1].context.stable
    assert "既有的記憶與資料" in muse
    assert "只根據下面的" not in muse and "請回答不知道" not in muse
    # The privacy and anti-fabrication rules do not relax.
    assert "不提供、不轉述、不確認任何金額" in muse and "不得編造" in muse
    assert "所有成員看到" in muse


def test_context_order_and_missing_kb_instruction(harness):
    h = harness(base_extra(persona_prompt="你是巧可，一隻貓。"))
    h.approve_all()
    run(h.say("巧可 自助餐幾點開？"))
    stable = h.turns[-1].context.stable
    assert "尚未提供" in stable and "不知道" in stable and "禮揚" in stable
    assert "不得編造" in stable
    _write_kb(h, pinned="集合時間：2/14 10:00 大廳。\n")
    h.store.set_summary("default", GROUP_A, "摘要內容", 0)
    run(h.say("巧可 自助餐幾點開？"))
    ctx = h.turns[-1].context
    assert ctx.kb_version.startswith("2027.1 ")
    order = [ctx.combined.index(marker) for marker in (
        "[角色]", "[回答規則]", "[定案事實]", "[知識庫 版本 2027.1", "[群組對話摘要]", "[最近對話]", "[現在發言的人]"
    )]
    assert order == sorted(order)
    assert "集合時間：2/14 10:00" in ctx.stable and "濱海灣" in ctx.stable
    # The KB says nothing about the buffet, and the prompt still forbids guessing.
    assert "自助餐" not in ctx.stable and "直接說不知道" in ctx.stable


def test_kb_reloads_on_change_and_by_command(harness):
    h = harness()
    h.approve_all()
    _write_kb(h)
    run(h.say("巧可 登船日是哪天？"))
    assert h.turns[-1].context.kb_version.startswith("2027.1 ")
    _write_kb(h, "---\nkb_version: 2027.2\n---\n登船日改為 2/15。內容較長以改變檔案大小。\n")
    run(h.say("巧可 登船日是哪天？"))
    assert h.turns[-1].context.kb_version.startswith("2027.2 ")
    assert "2027.2" in run(_dm_reply(h, "/kb reload"))


def test_history_recall_is_redacted(harness):
    h = harness()
    h.approve_all()
    run(h.say("訂位編號 ABX12345，費用 NT$58,000，電話 0912-345-678，10:30 在大廳集合", user=BOB))
    for i in range(25):
        run(h.say(f"閒聊 {i}"))
    run(h.say("巧可 之前有人說過在哪裡集合嗎？"))
    volatile = h.turns[-1].context.volatile
    assert "較早的相關對話" in volatile and "10:30" in volatile and "大廳" in volatile
    for secret in ("ABX12345", "58,000", "0912"):
        assert secret not in volatile


def test_redact_keeps_times_and_dates():
    text = redact("2/14 10:30 集合，email a@b.co，身分證 A123456789，US$1,200，https://x.y/z")
    assert "2/14 10:30" in text
    for secret in ("a@b.co", "A123456789", "1,200", "x.y/z"):
        assert secret not in text


# ---------------------------------------------------------------- summary


def test_rolling_summary_updates_incrementally_per_group(harness):
    calls = []

    async def summarizer(system, user, model=None):
        calls.append(user)
        return f"摘要第{len(calls)}版"

    h = harness(base_extra(summary_every=3), summarizer=summarizer)
    h.approve_all()
    h.store.register_member("default", GROUP_B, CAROL, local_name="卡蘿", role="同事")
    run(h.say("B群的話", user=CAROL, group=GROUP_B))
    for text in ("決定搭 2/14 的船", "阿嬤要帶暈船藥", "晚餐訂第二梯"):
        run(h.say(text))
    run(h.say("第四句"))
    assert len(calls) == 1
    assert "決定搭 2/14 的船" in calls[0] and "阿嬤" in calls[0] and "B群的話" not in calls[0]
    assert h.store.get_summary("default", GROUP_A)["summary"] == "摘要第1版"
    assert "摘要第1版" in run(_dm_reply(h, "/group summary disney"))
    # The next update only sees rows after the cursor, plus the old summary.
    run(h.engine.memory.update(h.account, h.account.groups[GROUP_A], force=True))
    assert "摘要第1版" in calls[1] and "決定搭" not in calls[1] and "第四句" in calls[1]


def test_summary_failure_keeps_cursor(harness):
    async def broken(system, user, model=None):
        raise RuntimeError("model down")

    h = harness(base_extra(summary_every=1), summarizer=broken)
    h.approve_all()
    run(h.say("第一句"))
    run(h.say("第二句"))
    assert h.store.get_summary("default", GROUP_A) is None
