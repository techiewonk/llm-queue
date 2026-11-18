"""Core correctness fixes of 0.2.0 (review A, scenarios G2 and G13) on Valkey and Redis.

A late final reply never replaces a stored success; replayed REGISTER, REGISTER_SEND,
RESULT_PUT and SETTLE_DONE scripts answer what the first run did.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
from typing import Any

import orjson
import pytest
import redis.asyncio
from engine_testkit import CHAT, GPT, SMALL, Make, away_from_minute_end, stats

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
    monkeypatch.setattr(os, "urandom", lambda n: b"\x01" * n)
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
    monkeypatch.setattr(os, "urandom", lambda n: b"\x02" * n)

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


# ---------------------------------------------------------------- A-4 (G3)


async def test_a4_a_cancel_during_an_admission_in_flight_gives_the_grant_back(
    make: Make, store: redis.asyncio.Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A-4: a cancel landing while PICK_TAKE was in flight leaked the grant it made.

    Its slot stayed taken up to a lease and its budget was never refunded. The script runs
    here, then its reply never comes (the cancel lands first); the grant now goes back at once.
    """
    engine, _ = await make({"small": {"rpm": 100, "rate_window": "smooth"}})
    real = engine.admission.pick_take
    granted = asyncio.Event()

    async def pick_take(ask: Any, changes: Any) -> Any:
        decision = await real(ask, changes)
        if ask.request_id == "a4":
            granted.set()
            await asyncio.Event().wait()  # the reply is lost in flight: only a cancel ends it
        return decision

    monkeypatch.setattr(engine.admission, "pick_take", pick_take)
    task = asyncio.create_task(
        engine.call("small", CHAT, request_id="a4", timeout_secs=20, force_direct=True)
    )
    await asyncio.wait_for(granted.wait(), 10)
    rate = engine.keys.rate("small", "aws-a/r1")
    inprog = f"{engine.keys.prefix}:{{m:small}}:inprog"
    assert int(await store.hget(rate, "seen_calls") or 0) == 1
    assert await store.zcard(inprog) == 1
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    loop = asyncio.get_running_loop()
    until = loop.time() + 1.0  # the lease is 3 s: a sweep would free only the slot, later
    while await store.zcard(inprog) or int(await store.hget(rate, "seen_calls") or 0):
        assert loop.time() < until, "the grant was not given back"
        await asyncio.sleep(0.05)
    assert "a4" not in engine._held


# ---------------------------------------------------------------- A-11 (G21), G20

HOME_ONE_RPM: dict[str, Any] = {
    "rpm": 1,
    "rate_window": "smooth",  # no minute boundary to cross mid-test
    "retry_base_secs": 0.05,
    "deployments": {"aws-a": {"confirmed": ["r1"]}},
    "model_fallback": {"on": ["exhausted"], "after_secs": 0, "models": ["small"]},
}


async def test_a11_a_retry_after_a_failed_fallback_attempt_goes_home_first(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A-11 (owner Q-5): a retry after a failed attempt on the fallback model stayed there.

    It was re-admitted as the fallback model's own call and never went back to its model,
    even with capacity again. The retry now asks the call's own model first.
    """
    # A fixed window: a raised rpm counts at once (a smooth one keeps the old spacing).
    home = {**HOME_ONE_RPM, "rate_window": "fixed"}
    engine, _ = await make(
        {"big": home, "small": {"max_attempts": 3, "retry_base_secs": 0.05}},
        {"models": {f"{SMALL}@r1": {"force_error": "InternalServerException"}}},
    )
    await away_from_minute_end(engine.clock.now)
    assert (await engine.call("big", CHAT)).meta.served_model == "big"  # its budget is used
    real = engine._attempt

    async def attempt(view: Any, alias: str, *args: Any, **kwargs: Any) -> Any:
        result = await real(view, alias, *args, **kwargs)
        if alias == "small" and result.outcome != "ok":  # the home model recovers meanwhile
            await engine.registry.set({"rpm": 600}, model="big", actor="t")
        return result

    monkeypatch.setattr(engine, "_attempt", attempt)
    reply = await engine.call("big", CHAT, timeout_secs=10)
    assert reply.meta.served_model == "big"  # provider_error after three tries on small before
    assert reply.meta.attempts == 2


async def test_g20_a_tenant_at_its_quota_is_not_served_by_the_fallback_model(
    make: Make,
) -> None:
    """G20: fairness with model fallback: a tenant is charged once, refused per its own quota.

    The home model is out of budget and the tenant at its quota there: the call fails
    `rate_limited` at once instead of going round the quota on the fallback model; another
    tenant falls back; the refused tenant's one grant is all it was charged.
    """
    engine, fake = await make({"big": {**HOME_ONE_RPM, "fairness": {"mode": "quota", "rpm": 1}}})
    first = await engine.call("big", CHAT, tenant_id="acme")
    assert first.meta.served_model == "big"
    with pytest.raises(CallFailed) as caught:
        await engine.call("big", CHAT, tenant_id="acme", timeout_secs=5)
    assert caught.value.kind == "rate_limited"
    for _ in range(2):  # served by the fallback, never charged to its quota on the home model
        other = await engine.call("big", CHAT, tenant_id="globex", timeout_secs=5)
        assert other.meta.served_model == "small"
    assert stats(fake, f"{SMALL}@r1")["calls"] == 2  # globex only: acme was never run there


# ---------------------------------------------------------------- A-10, A-12 (G17)


async def test_a12_the_stores_expiry_mode_wins_over_a_differing_setting(
    make: Make, store: redis.asyncio.Redis
) -> None:
    """A-12 (owner Q-6): pods with different explicit `LLMQ_KEY_EXPIRY_MODE` flipped the mode.

    Each `native` minute on a `volatile-*` store left evictable keys. Now the store's mode
    wins: an `auto` pod on a volatile store moves it to `swept`; a pod set to `native`
    follows it (and logs a conflict), and nothing moves it back to `native` by itself.
    """
    first, _ = await make()
    second, _ = await make()
    second.settings = dataclasses.replace(second.settings, key_expiry_mode="native")
    first._eviction["state"] = "volatile-lru"  # as read from a shared cache store
    await first._apply_expiry()
    mode_key = first.expiry.mode_key
    assert await store.get(mode_key) == b"swept"
    for _ in range(2):  # every minute's re-publish: it flipped back to native before
        await second._apply_expiry()
        assert await store.get(mode_key) == b"swept"
    assert second.expiry.mode == "swept"
    assert second._expiry_conflict["state"] == "swept"
    first._eviction["state"] = "noeviction"  # `auto` would ask for native now: not by itself
    await first._apply_expiry()
    assert await store.get(mode_key) == b"swept"
    key = f"{second.keys.prefix}:a12"
    async with second.state.pipeline(transaction=False) as pipe:
        pipe.set(key, "1")
        second.expiry.expire(pipe, key, 60)
        await pipe.execute()
    assert await store.pttl(key) == -1  # indexed, not a native (evictable) TTL


async def test_a10_a_mode_change_reaches_every_process_at_once(
    make: Make, store: redis.asyncio.Redis
) -> None:
    """A-10: pipelines followed another process's mode change only at their minute re-read.

    For up to a minute after native -> swept, chunk streams and retry counters got native
    TTLs (evictable mid-stream). The change is now announced and followed at once.
    """
    first, _ = await make()
    second, _ = await make(worker=False)  # a caller-only process follows too
    await store.set(first.expiry.mode_key, "native")
    first.expiry.mode = second.expiry.mode = "native"
    first._eviction["state"] = "volatile-lru"
    await first._apply_expiry()
    loop = asyncio.get_running_loop()
    until = loop.time() + 2.0  # the minute re-read is far off
    while second.expiry.mode != "swept":
        assert loop.time() < until, "the change notice never arrived"
        await asyncio.sleep(0.02)
    # The native window above left native TTLs on purpose: not a swept-mode leak at teardown.
    await store.set(first.expiry.mode_key, "native")
