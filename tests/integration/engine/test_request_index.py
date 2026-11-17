"""The request index per model and tenant (0.2.0 T1) on Valkey and Redis.

Every path that confirms a call indexes it in the same script (queued send in the state store,
the separate-store / replica-wait CONFIRM, the direct run); every final write removes it
(success, failure, expiry, cancel, dead letter). Off, no index key or job field is written.
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, Make, ttl_of

from llm_queue.engine.envelope import Reply
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import QueueSettings, StreamQueues
from llm_queue.engine.requests import IndexEntry, RequestStore
from llm_queue.engine.requests_admin import AdminSettings
from llm_queue.engine.runtime import Engine

pytestmark = pytest.mark.integration

SLOW = {"defaults": {"latency_ms": {"p50": 1500, "p95": 1500}}}
SLOW_MODEL = {"small": {"timeout_secs": 30, "attempt_timeout_secs": 20}}


def _entry(keys: Keys, alias: str = "m", tenant: str = "h1", keep_ms: int = 60_000) -> IndexEntry:
    return IndexEntry(
        keys=(keys.request_index(alias, tenant), keys.tenant_index(alias)),
        tenant_hash=tenant,
        keep_ms=keep_ms,
        cls="n",
    )


async def _members(client: redis.asyncio.Redis, key: str) -> dict[str, float]:
    rows: Any = await client.zrange(key, 0, -1, withscores=True)
    return {(m.decode() if isinstance(m, bytes) else m): float(s) for m, s in rows}


async def _wait_for(check: Any, secs: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    until = loop.time() + secs
    while not await check():
        assert loop.time() < until, "condition not met in time"
        await asyncio.sleep(0.02)


# ------------------------------------------------------------------ the scripts


async def test_register_send_indexes_and_the_result_removes(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    keys = Keys(prefix)
    requests = RequestStore(store, keys, takeover_grace_secs=0)
    queues = StreamQueues(store, keys)
    entry = _entry(keys)
    found = await requests.register_send(
        "r1",
        caller="pod-a",
        user_hash="u",
        params_hash="p",
        model="m",
        priority=False,
        app_id=False,
        queue="q",
        queue_keys=queues.queue_keys("q"),
        bell=queues.bell("q"),
        body=b"{}",
        max_waiting=0,
        retention_secs=3600,
        keep_secs=60,
        index=entry,
    )
    assert found.outcome == "new"
    assert found.message_id
    index = await _members(store, keys.request_index("m", "h1"))
    assert list(index) == ["r1"]
    tenants = await _members(store, keys.tenant_index("m"))
    assert tenants == {"h1": index["r1"]}
    job = await requests.job("r1")
    assert (job["ix"], job["ixh"], job["mid"], job["q"]) == (
        keys.request_index("m", "h1"),
        "h1",
        found.message_id,
        "n",
    )
    secs, micros = await store.time()
    now_ms = int(secs) * 1000 + int(micros) // 1000
    assert 55_000 < index["r1"] - now_ms <= 60_000  # scored by the deadline
    await requests.put_result(Reply(request_id="r1", ok=True), final_state="done")
    assert await _members(store, keys.request_index("m", "h1")) == {}


async def test_queue_full_and_abandon_index_nothing(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    keys = Keys(prefix)
    requests = RequestStore(store, keys, takeover_grace_secs=0)
    queues = StreamQueues(store, keys)
    await store.xadd(keys.queue("q", "s"), {"body": "x"})
    with pytest.raises(Exception, match="full"):
        await requests.register_send(
            "r1",
            caller="pod-a",
            user_hash="u",
            params_hash="p",
            model="m",
            priority=False,
            app_id=False,
            queue="q",
            queue_keys=queues.queue_keys("q"),
            bell=queues.bell("q"),
            body=b"{}",
            max_waiting=1,
            retention_secs=3600,
            keep_secs=60,
            index=_entry(keys),
        )
    await requests.register(
        "r2",
        caller="pod-a",
        user_hash="u",
        params_hash="p",
        model="m",
        priority=False,
        app_id=False,
    )
    await requests.abandon("r2", caller="pod-a")
    assert not await store.exists(keys.request_index("m", "h1"), keys.tenant_index("m"))


async def test_confirm_indexes_and_a_takeover_moves_the_score(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    keys = Keys(prefix)
    requests = RequestStore(store, keys, takeover_grace_secs=0)
    record = {
        "caller": "pod-a",
        "user_hash": "u",
        "params_hash": "p",
        "model": "m",
        "priority": True,
        "app_id": False,
    }
    await requests.register("r1", **record)
    await requests.confirm("r1", keep_secs=60, index=_entry(keys, keep_ms=10_000), message_id="1-1")
    first = (await _members(store, keys.request_index("m", "h1")))["r1"]
    # A takeover (the runner died; its record is unconfirmed again) confirms again, with a new
    # deadline and message id: one entry, its score moved.
    await store.hset(keys.request("r1", "job"), "confirmed", "0")
    await requests.register("r1", **{**record, "caller": "pod-b"})
    await requests.confirm("r1", keep_secs=60, index=_entry(keys, keep_ms=40_000), message_id="2-1")
    index = await _members(store, keys.request_index("m", "h1"))
    assert list(index) == ["r1"]
    assert index["r1"] > first
    assert (await requests.job("r1"))["mid"] == "2-1"
    # A failure removes it as a success does.
    await requests.put_result(
        Reply(request_id="r1", ok=False, error="provider_error"), final_state="failed"
    )
    assert await _members(store, keys.request_index("m", "h1")) == {}


async def test_lapsed_entries_are_pruned_and_ttl_only_grows(
    engine_store: tuple[Engine, Any],
) -> None:
    engine, _ = engine_store
    keys = engine.keys
    requests = engine.requests
    await engine.state.zadd(keys.request_index("m", "h1"), {"old": 1})
    await engine.state.zadd(keys.tenant_index("m"), {"gone": 1})
    record = {
        "caller": "pod-a",
        "user_hash": "u",
        "params_hash": "p",
        "model": "m",
        "priority": False,
        "app_id": False,
    }
    await requests.register("long", **record)
    await requests.confirm("long", keep_secs=600, index=_entry(keys, keep_ms=600_000))
    await requests.register("short", **record)
    await requests.confirm("short", keep_secs=5, index=_entry(keys, keep_ms=5_000))
    assert set(await _members(engine.state, keys.request_index("m", "h1"))) == {"long", "short"}
    assert set(await _members(engine.state, keys.tenant_index("m"))) == {"h1"}
    left = await ttl_of(engine, keys.request_index("m", "h1"))
    assert 600 < left <= 660  # the longest call + 60 s, not shortened by the short one


@pytest.fixture
async def engine_store(make: Make) -> tuple[Engine, Any]:
    return await make(worker=False)


# ------------------------------------------------------------------ the engine paths


async def test_queued_call_indexed_then_removed_on_success(make: Make) -> None:
    engine, _ = await make(SLOW_MODEL, SLOW)
    h = engine.hash_id("acme")
    key = engine.keys.request_index("small", h)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme"))

    async def indexed() -> bool:
        return bool(await engine.state.zcard(key))

    await _wait_for(indexed)
    assert h in await _members(engine.state, engine.keys.tenant_index("small"))
    assert (await task).ok
    assert await engine.state.zcard(key) == 0


async def test_no_tenant_is_dash_and_user_is_not_a_tenant(make: Make) -> None:
    engine, _ = await make(SLOW_MODEL, SLOW)
    key = engine.keys.request_index("small", "-")
    task = asyncio.create_task(engine.call("small", CHAT, user_id="ann"))

    async def indexed() -> bool:
        return bool(await engine.state.zcard(key))

    await _wait_for(indexed)
    assert not await engine.state.exists(engine.keys.request_index("small", engine.hash_id("ann")))
    assert (await task).ok


async def test_direct_call_indexed_with_no_message(make: Make) -> None:
    engine, _ = await make(SLOW_MODEL, SLOW)
    h = engine.hash_id("acme")
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme", force_direct=True))
    key = engine.keys.request_index("small", h)

    async def indexed() -> bool:
        return bool(await engine.state.zcard(key))

    await _wait_for(indexed)
    rid = (await _members(engine.state, key)).popitem()[0]
    job = await engine.requests.job(rid)
    assert (job["mid"], job["direct"]) == ("", "1")
    assert (await task).ok
    assert await engine.state.zcard(key) == 0


async def test_separate_queue_store_confirm_indexes(
    make: Make, store: redis.asyncio.Redis, prefix: str
) -> None:
    """CONFIRM path (replica wait off, queues apart): the send's message id is kept."""
    engine, _ = await make(SLOW_MODEL, SLOW)
    engine._queue_store = store  # queues_shared False: register, send, then CONFIRM
    h = engine.hash_id("acme")
    key = engine.keys.request_index("small", h)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme"))

    async def indexed() -> bool:
        return bool(await engine.state.zcard(key))

    await _wait_for(indexed)
    rid = (await _members(engine.state, key)).popitem()[0]
    job = await engine.requests.job(rid)
    assert job["mid"]
    assert "-" in job["mid"]
    assert (await task).ok
    assert await engine.state.zcard(key) == 0


async def test_failure_expiry_and_cancel_remove_the_entry(make: Make) -> None:
    engine, _fake = await make(
        {"small": {"timeout_secs": 2, "attempt_timeout_secs": 1}},
        {"defaults": {"hang": True}},
    )
    h = engine.hash_id("acme")
    key = engine.keys.request_index("small", h)
    # Expiry: the call hangs past its deadline; the worker's final write removes the entry.
    with pytest.raises(CallFailed):
        await engine.call("small", CHAT, tenant_id="acme")

    async def empty() -> bool:
        return not await engine.state.zcard(key)

    await _wait_for(empty, 15)
    # Cancel: the entry stays while the call ends (`cancelling`), then goes with its final write.
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme", timeout_secs=30))

    async def indexed() -> bool:
        return bool(await engine.state.zcard(key))

    await _wait_for(indexed)
    rid = (await _members(engine.state, key)).popitem()[0]
    await engine.cancel(rid)
    with pytest.raises(CallFailed, match="cancelled"):
        await task
    await _wait_for(empty, 10)


async def test_a_cancel_seen_twice_still_ends_a_hung_running_call(make: Make) -> None:
    """The channel and the key check both cancelled the run task; the second cancel landed in
    its cleanup, so no reply was written and the caller waited out its deadline (a flake of
    the test above, about one run in four)."""
    engine, _ = await make(
        {"small": {"timeout_secs": 30, "attempt_timeout_secs": 20}}, {"defaults": {"hang": True}}
    )
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme", timeout_secs=30))

    async def running() -> bool:
        return any(h.run is not None and not h.run.done() for h in engine._held.values())

    await _wait_for(running, 10)
    rid = next(iter(engine._held))
    started = asyncio.get_running_loop().time()
    await engine.cancel(rid)
    for _ in range(20):  # the heartbeat's key check, racing the channel's notice
        await engine._check_cancels()
        await asyncio.sleep(0)
    with pytest.raises(CallFailed, match="cancelled"):
        await task
    assert asyncio.get_running_loop().time() - started < 5


async def test_dead_letter_removes_the_entry(make: Make) -> None:
    engine, _ = await make(worker=False)
    h = engine.hash_id("acme")
    key = engine.keys.request_index("small", h)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme", timeout_secs=20))

    async def indexed() -> bool:
        return bool(await engine.state.zcard(key))

    await _wait_for(indexed)
    rid = (await _members(engine.state, key)).popitem()[0]
    queue = engine._queue_name(engine.registry.current, "small", False)
    # A worker's view of a dead letter: take the message, then answer its caller.
    messages, _ = await engine.queues.receive(queue, 1, QueueSettings())
    await engine._dead_lettered("small", messages[0].body)
    with pytest.raises(CallFailed, match="dead_lettered"):
        await task
    assert await engine.state.zcard(key) == 0
    assert (await engine.requests.job(rid))["state"] == "failed"


async def test_worker_death_and_redelivery_settle_once(make: Make) -> None:
    """A worker dies mid-call: the message comes back, the next run settles and removes it."""
    engine, _ = await make(SLOW_MODEL, SLOW, worker=False)
    h = engine.hash_id("acme")
    key = engine.keys.request_index("small", h)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme"))

    async def indexed() -> bool:
        return bool(await engine.state.zcard(key))

    await _wait_for(indexed)
    queue = engine._queue_name(engine.registry.current, "small", False)
    # The dead worker: took the message and never answered; its hold lapses (visibility 0.2 s).
    taken, _ = await engine.queues.receive(queue, 1, QueueSettings(visibility_secs=0.2))
    assert taken
    await asyncio.sleep(0.3)
    assert await engine.state.zcard(key) == 1  # not final: still indexed
    worker, _ = await make(SLOW_MODEL, SLOW, caller=False)
    assert (await task).ok
    assert await engine.state.zcard(key) == 0
    assert worker.keys.prefix == engine.keys.prefix


async def test_store_reset_mid_run_raises_nothing(make: Make) -> None:
    engine, _ = await make(SLOW_MODEL, SLOW)
    h = engine.hash_id("acme")
    key = engine.keys.request_index("small", h)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme"))

    async def indexed() -> bool:
        return bool(await engine.state.zcard(key))

    await _wait_for(indexed)
    await engine.state.delete(key, engine.keys.tenant_index("small"))  # lost with the store
    assert (await task).ok  # the final write finds no entry: nothing fails
    assert (await engine.call("small", CHAT, tenant_id="acme")).ok


async def test_index_off_writes_nothing(make: Make) -> None:
    engine, _ = await make()
    engine.settings = dataclasses.replace(engine.settings, admin=AdminSettings(request_index=False))
    reply = await engine.call("small", CHAT, tenant_id="acme")
    assert reply.ok
    assert not await engine.state.exists(
        engine.keys.request_index("small", engine.hash_id("acme")),
        engine.keys.tenant_index("small"),
    )
    job = await engine.requests.job(reply.request_id)
    assert "ix" not in job


async def test_another_tenant_is_untouched(make: Make) -> None:
    engine, _ = await make(SLOW_MODEL, SLOW)
    a = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme"))
    b = asyncio.create_task(engine.call("small", CHAT, tenant_id="beta"))
    ka = engine.keys.request_index("small", engine.hash_id("acme"))
    kb = engine.keys.request_index("small", engine.hash_id("beta"))

    async def both() -> bool:
        return bool(await engine.state.zcard(ka)) and bool(await engine.state.zcard(kb))

    await _wait_for(both)
    assert set(await _members(engine.state, ka)).isdisjoint(await _members(engine.state, kb))
    assert (await a).ok
    assert (await b).ok
