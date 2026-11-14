"""0.1.0b2 speed work (plan B): fewer store round trips and no idle waits on the hot path.

Each test pins one change on Valkey and Redis: a stream joins the shared chunk read at once, a
stream returns on its `end` entry, the folded scripts and the doorbell give-back.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, Make

from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import Message, QueueSettings, StreamQueues
from llm_queue.engine.runtime import _Slots
from llm_queue.engine.streaming import ChunkReader

pytestmark = pytest.mark.integration


async def test_a_new_stream_joins_the_blocked_read_at_once(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """B1: a stream opened while the shared read blocked waited for that read to end."""
    reader = ChunkReader(store, block_ms=5000, wake_key=f"{prefix}:chunkwake:w")
    first = f"{prefix}:{{r:a}}:chunks"
    second = f"{prefix}:{{r:b}}:chunks"
    try:
        reader.open(first)
        await asyncio.sleep(0.2)  # the shared read now blocks on `first` alone
        queue = reader.open(second)
        await asyncio.sleep(0.05)
        await store.xadd(second, {"seq": 1, "att": "a", "t": "end", "d": b"{}"})
        started = time.monotonic()
        entry = await asyncio.wait_for(queue.get(), 2)
        assert entry["t"] == "end"
        assert time.monotonic() - started < 1.0  # not the 5 s block
    finally:
        await reader.aclose()


async def test_stream_returns_on_its_end_entry(make: Make, monkeypatch: pytest.MonkeyPatch) -> None:
    """B2: the stream waited for the call task (result write and read) after its `end`."""
    engine, _ = await make()
    await engine.call("small", CHAT)  # warm the client
    real = engine.requests.put_result

    async def late(*args: Any, **kw: Any) -> bool:
        await asyncio.sleep(1.5)
        return bool(await real(*args, **kw))

    monkeypatch.setattr(engine.requests, "put_result", late)
    started = time.monotonic()
    events = [event async for event in engine.stream("small", CHAT)]
    assert events
    assert time.monotonic() - started < 1.2  # the result is still 1.5 s away
    assert engine._tails  # the call task finishes in the background
    for _ in range(60):
        if not engine._tails:
            break
        await asyncio.sleep(0.05)
    assert not engine._tails


FAST = QueueSettings(visibility_secs=5, max_receives=3)


async def test_one_receive_reads_priority_then_normal(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """B5 (106): the worker read its priority queue, then its normal one: two scripts a poll."""
    queues = StreamQueues(store, Keys(prefix))
    assert await queues.receive_first(["p", "n"], 5, FAST) == ([], [])
    await queues.send("n", b"normal", FAST)
    (normal,), _ = await queues.receive_first(["p", "n"], 5, FAST)
    assert (normal.queue, normal.body) == ("n", b"normal")
    await queues.send("n", b"later", FAST)
    await queues.send("p", b"urgent", FAST)
    taken, _ = await queues.receive_first(["p", "n"], 5, FAST)
    assert [(m.queue, m.body) for m in taken] == [("p", b"urgent")]  # the normal one waits
    # The store's time at receive (112): the wait is measured on the store's clock.
    store_ms = int((await store.time())[0]) * 1000
    assert abs(taken[0].received_ms - store_ms) < 2000
    assert taken[0].received_ms >= taken[0].position_ms


async def test_receive_first_reports_dead_letters_of_each_queue(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """B5 (106): a lapsed hold past max_receives dead-letters from whichever queue it is in."""
    queues = StreamQueues(store, Keys(prefix))
    settings = QueueSettings(visibility_secs=0.05, max_receives=1)
    await queues.send("n", b"doomed", settings)
    assert len((await queues.receive_first(["p", "n"], 1, settings))[0]) == 1
    await asyncio.sleep(0.1)
    taken, dead = await queues.receive_first(["p", "n"], 1, settings)
    assert taken == []
    assert [(d.queue, d.body) for d in dead] == [("n", b"doomed")]


async def test_extend_many_renews_holds_in_one_call(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """B5 (111): the heartbeat extended each held message with its own script call."""
    queues = StreamQueues(store, Keys(prefix))
    await queues.send("a", b"1", FAST)
    await queues.send("b", b"2", FAST)
    (one,), _ = await queues.receive("a", 1, FAST)
    (two,), _ = await queues.receive("b", 1, FAST)
    stale = Message("a", one.id, one.generation + 1, b"")
    assert await queues.extend_many([(one, 60), (two, 60), (stale, 60)]) == [True, True, False]
    vis = await store.zscore(Keys(prefix).queue("b", "vis"), two.id)
    assert vis is not None
    assert vis - two.received_ms > 50_000
    assert await queues.extend_many([]) == []


async def test_a_worker_with_no_free_slot_gives_the_doorbell_back(make: Make) -> None:
    """B5 (107): a worker woken with no free slot took the token: other pods slept on."""
    engine, _ = await make({"small": {"worker_slots": 1}})
    await engine.call("small", CHAT)  # the loop is idle on the doorbell now
    engine._slots["small"] = _Slots(0)  # every slot busy
    view = engine.registry.current
    name = engine._queue_name(view, "small", False)
    await asyncio.sleep(0.2)
    await engine.state.delete(engine.queues.bell(name))  # a token the first call left
    await engine.queues.send(name, b"x", QueueSettings())
    await asyncio.sleep(0.5)
    assert await engine.state.llen(engine.queues.bell(name)) == 1  # given back, not eaten
