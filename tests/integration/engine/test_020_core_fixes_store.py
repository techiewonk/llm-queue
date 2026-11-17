"""Core correctness fixes of 0.2.0 (review A, scenarios G2 and G13) on Valkey and Redis.

A late final reply never replaces a stored success; replayed REGISTER, REGISTER_SEND,
RESULT_PUT and SETTLE_DONE scripts answer what the first run did.
"""

from __future__ import annotations

from typing import Any

import orjson
import pytest
import redis.asyncio
from engine_testkit import CHAT, GPT, Make

from llm_queue.engine import requests as requests_module
from llm_queue.engine.envelope import Reply
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import QueueSettings, StreamQueues
from llm_queue.engine.requests import IndexEntry, RequestStore

pytestmark = pytest.mark.integration


def _store(client: redis.asyncio.Redis, prefix: str) -> RequestStore:
    return RequestStore(
        client, Keys(prefix), result_ttl_secs=300, result_dedupe_secs=2, takeover_grace_secs=5
    )


async def _register(store: RequestStore, caller: str = "pod-a", rid: str = "req-1") -> str:
    found = await store.register(
        rid,
        caller=caller,
        user_hash="u",
        params_hash="p",
        model="m",
        priority=False,
        app_id=True,
    )
    return found.outcome


def _entry(keys: Keys) -> IndexEntry:
    return IndexEntry(
        keys=(keys.request_index("m", "h1"), keys.tenant_index("m")),
        tenant_hash="h1",
        keep_ms=60_000,
        cls="n",
    )


# ---------------------------------------------------------------- A-1 (G2)


@pytest.mark.parametrize("late", ["cancelled", "failed"])
async def test_a1_a_late_reply_never_replaces_a_stored_success(
    store: redis.asyncio.Redis, prefix: str, late: str
) -> None:
    """A-1: a late cancel or failure after a success set the claim `cancelled`/`failed`.

    The next registration of the app id then deleted the success and ran the call again.
    """
    keys = Keys(prefix)
    requests = _store(store, prefix)
    assert await _register(requests) == "new"
    await requests.confirm("req-1", keep_secs=120, index=_entry(keys), message_id="1-0")
    assert await store.zscore(keys.request_index("m", "h1"), "req-1") is not None
    ok = Reply(request_id="req-1", ok=True, response={"x": 1})
    assert await requests.put_result(ok, final_state="done", status={}) is True
    assert await store.zscore(keys.request_index("m", "h1"), "req-1") is None  # dropped once
    failure = Reply(request_id="req-1", ok=False, error=late)
    assert await requests.put_result(failure, final_state=late, status={"error": late}) is False
    assert (await requests.job("req-1"))["state"] == "done"
    assert (await requests.status("req-1"))["state"] == "done"
    assert await store.zscore(keys.request_index("m", "h1"), "req-1") is None
    assert await _register(requests, "pod-b") == "done"  # `new` before: it ran again
    taken = await requests.take_result("req-1")
    assert taken is not None
    assert taken.ok
    assert taken.response == {"x": 1}


async def test_a1_a_late_settle_after_a_cancel_reply_keeps_the_success_final(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """Q-1: nothing replaces a `done` claim, also through the one-script settle (SETTLE_DONE)."""
    keys = Keys(prefix)
    requests = _store(store, prefix)
    queues = StreamQueues(store, keys)
    settings = QueueSettings(visibility_secs=60, max_receives=3, max_waiting=0)
    assert await _register(requests) == "new"
    await queues.send("q", b"body", settings)
    [message], _ = await queues.receive("q", 1, settings)
    finish = [prefix, orjson.dumps({"alias": "m", "owner": "o1", "request_id": "req-1"}), ""]
    done = await requests.put_result_settled(
        Reply(request_id="req-1", ok=True, response={"x": 1}),
        final_state="done",
        status={},
        queue_keys=queues.queue_keys("q"),
        message_id=message.id,
        generation=message.generation,
        finish=finish,
    )
    assert done.written
    cancelled = Reply(request_id="req-1", ok=False, error="cancelled")
    assert not await requests.put_result(cancelled, final_state="cancelled", status={})
    assert (await requests.job("req-1"))["state"] == "done"
    assert await _register(requests, "pod-b") == "done"


async def test_a1_a_failure_still_moves_a_waiting_claim(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """The guard is only against final states: a first failure still ends the claim."""
    requests = _store(store, prefix)
    assert await _register(requests) == "new"
    await requests.confirm("req-1", keep_secs=120)
    failure = Reply(request_id="req-1", ok=False, error="provider_error")
    assert await requests.put_result(failure, final_state="failed", status={}) is True
    assert (await requests.job("req-1"))["state"] == "failed"
    assert await _register(requests, "pod-b") == "new"  # Q2: a failure runs again


# ---------------------------------------------------------------- A-5, A-7 (G13)


async def test_a5_a_replayed_register_answers_the_first_outcome(
    store: redis.asyncio.Redis, prefix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A-5: a REGISTER replayed by redis-py found its own record and answered `attached`.

    The caller then sent nothing and waited for the takeover check (~6 s). The same nonce
    now answers the first outcome; another registration still attaches.
    """
    requests = _store(store, prefix)
    monkeypatch.setattr(requests_module.os, "urandom", lambda n: b"\x01" * n)
    assert await _register(requests) == "new"
    assert await _register(requests) == "new"  # the replay: `attached` before
    monkeypatch.undo()
    assert await _register(requests, "pod-b") == "attached"


async def test_a5_a_replayed_register_send_sends_once(
    store: redis.asyncio.Redis, prefix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A-5: a replayed REGISTER_SEND answered `attached` and skipped the tenant index.

    It now answers `new` with the first message id, and the stream holds one message.
    """
    keys = Keys(prefix)
    requests = _store(store, prefix)
    queues = StreamQueues(store, keys)
    monkeypatch.setattr(requests_module.os, "urandom", lambda n: b"\x02" * n)

    async def send() -> Any:
        return await requests.register_send(
            "req-1",
            caller="pod-a",
            user_hash="u",
            params_hash="p",
            model="m",
            priority=False,
            app_id=True,
            queue="q",
            queue_keys=queues.queue_keys("q"),
            bell=queues.bell("q"),
            body=b"{}",
            max_waiting=0,
            retention_secs=3600,
            keep_secs=60,
            index=_entry(keys),
        )

    first = await send()
    again = await send()
    assert first.outcome == "new"
    assert first.message_id
    assert (again.outcome, again.message_id) == ("new", first.message_id)
    assert await store.xlen(keys.queue("q", "s")) == 1


async def test_a7_a_replayed_settle_is_recognised(store: redis.asyncio.Redis, prefix: str) -> None:
    """A-7: a replayed SETTLE_DONE answered all zeros: stale ack and grant lost alarms.

    The replay now says `replayed` (this body is stored); another run's settle does not.
    """
    keys = Keys(prefix)
    requests = _store(store, prefix)
    queues = StreamQueues(store, keys)
    settings = QueueSettings(visibility_secs=60, max_receives=3, max_waiting=0)
    await _register(requests)
    await queues.send("q", b"body", settings)
    [message], _ = await queues.receive("q", 1, settings)
    finish = [prefix, orjson.dumps({"alias": "m", "owner": "o1", "request_id": "req-1"}), ""]
    reply = Reply(request_id="req-1", ok=True, response={"x": 1})

    async def settle(body: Reply) -> Any:
        return await requests.put_result_settled(
            body,
            final_state="done",
            status={},
            queue_keys=queues.queue_keys("q"),
            message_id=message.id,
            generation=message.generation,
            finish=finish,
        )

    first = await settle(reply)
    assert (first.written, first.acked, first.replayed) == (True, True, False)
    replay = await settle(reply)
    assert (replay.written, replay.acked, replay.replayed) == (False, False, True)
    other = await settle(reply.model_copy(update={"response": {"x": 2}}))
    assert (other.written, other.replayed) == (False, False)
    assert await requests.put_result(reply, final_state="done") is True  # a replayed put


async def test_a1_a_direct_run_whose_post_result_step_raises_returns_the_success(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A-1: a direct run that stored its success and then raised told its caller it failed.

    Its failure reply also set the claim `failed`, so the app id ran again.
    """
    engine, _ = await make()
    real = engine._count_target_errors

    async def count(held: Any, *, succeeded: bool) -> None:
        await real(held, succeeded=succeeded)
        if succeeded:
            raise RuntimeError("health write failed")

    monkeypatch.setattr(engine, "_count_target_errors", count)
    reply = await engine.call("small", CHAT, request_id="a1-direct", force_direct=True)
    assert reply.ok
    assert (await engine.requests.job("a1-direct"))["state"] == "done"


# ---------------------------------------------------------------- A-3 (G6)


async def test_a3_the_last_deployment_that_takes_calls_is_never_disabled(make: Make) -> None:
    """A-3: a disabled or blocked deployment counted as usable for the health safeguard.

    r2 is disabled by the registry and r3's region is blocked: r1 is the last deployment that
    takes calls, so its target errors only log (`last`) and calls keep going to it.
    """
    engine, fake = await make(
        {
            "big": {
                "health": {"disable_errors": 1, "probe_after_secs": 600},
                "max_attempts": 1,
                "deployments": {
                    "aws-a": {
                        "confirmed": ["r1", "r2"],
                        "fallback": ["r3"],
                        "overrides": {"r2": {"enabled": False}},
                    }
                },
            }
        },
        {"models": {f"{GPT}@r1": {"force_error": "AccessDeniedException"}}},
    )
    await engine.registry.block(region="r3", on=True, actor="t")
    with pytest.raises(CallFailed):
        await engine.call("big", CHAT)
    assert await engine.health.disabled("big") == {}  # r1 was disabled before
    fake.scenario.patch_model(f"{GPT}@r1", {"force_error": None})
    reply = await engine.call("big", CHAT)
    assert reply.meta.deployment == "aws-a/r1"
