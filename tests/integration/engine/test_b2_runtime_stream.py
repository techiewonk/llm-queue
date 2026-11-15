"""0.1.0b2 streaming, results, jobs and queue fixes (RESCAN S2-S11, X10), on Valkey and Redis."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, Make

from llm_queue.engine.envelope import Meta, Reply
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.jobs import JOBS_MAX_WAITING, JobRunner
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import QueueSettings, StreamQueues
from llm_queue.engine.requests import RequestStore
from llm_queue.engine.streaming import ChunkReader

pytestmark = pytest.mark.integration


class Counter:
    """A metrics sink that keeps counter totals."""

    def __init__(self) -> None:
        self.counts: dict[str, float] = {}

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        self.counts[name] = self.counts.get(name, 0.0) + value

    def observe(self, name: str, value: float, **labels: str) -> None:
        """Ignore."""

    def gauge(self, name: str, value: float, **labels: str) -> None:
        """Ignore."""

    def total(self, name: str) -> float:
        return self.counts.get(name, 0.0)


async def test_slow_consumer_reads_what_is_buffered(make: Make) -> None:
    """S2: a consumer slower than the deadline got `deadline_exceeded` on chunks already read."""
    engine, _ = await make()
    events = []
    async for event in engine.stream("small", CHAT, timeout_secs=1):
        events.append(event)
        if len(events) == 1:
            await asyncio.sleep(1.5 + engine.settings.caller_margin_secs)  # past the deadline
    assert len(events) > 1


async def test_stream_end_shortens_its_life(make: Make, store: redis.asyncio.Redis) -> None:
    """S3: a read stream stayed for the result TTL plus the timeout (up to 8 MB each)."""
    engine, _ = await make()
    async for _ in engine.stream("small", CHAT, request_id="s3"):
        pass
    assert 0 < await store.ttl(engine.keys.request("s3", "chunks")) <= 30


async def test_stream_keeps_the_app_id_and_refuses_embeddings(make: Make) -> None:
    """S11: `stream(request_id=)` lost its app-id behaviour; an embedding model yielded nothing."""
    engine, _ = await make()
    async for _ in engine.stream("small", CHAT, request_id="app-1"):
        pass
    assert (await engine.requests.job("app-1")).get("app") == "1"
    with pytest.raises(CallFailed) as info:  # a failed call, not bad config (audit RT-28)
        async for _ in engine.stream("titan", {"inputText": "x"}):
            pass
    assert info.value.kind == "invalid_request"


async def test_chunk_reader_skips_a_bad_entry(store: redis.asyncio.Redis, prefix: str) -> None:
    """S11: one bad entry ended the reader shared by every stream of the process."""
    key = f"{prefix}:{{r:bad}}:chunks"
    await store.xadd(key, {"seq": "x", "t": "chunk", "d": b"[]"})
    await store.xadd(key, {"seq": 1, "att": "a", "t": "end", "d": b"{}"})
    reader = ChunkReader(store, block_ms=50)
    queue = reader.open(key)
    try:
        entry = await asyncio.wait_for(queue.get(), 2)
        assert entry["t"] == "end"
    finally:
        await reader.aclose()


class FlakyOffload:
    """An offload whose first `fail` reads raise."""

    def __init__(self, fail: int) -> None:
        self.fail = fail
        self.data: dict[str, bytes] = {}

    async def put(self, key: str, data: bytes) -> str:
        self.data[key] = data
        return key

    async def get(self, ref: str) -> bytes | None:
        if self.fail:
            self.fail -= 1
            raise OSError("S3 blip")
        return self.data.get(ref)


async def test_offloaded_result_survives_a_failed_read(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """S4: the result was deleted before its offload read; a blip there lost it for good.

    Since R3JE-28 a failed read is tried twice more, then answers the retryable
    `queue_unavailable`; the result survives either way and a later read gets it.
    """
    offload = FlakyOffload(fail=3)  # more failures than one read's retries
    requests = RequestStore(store, Keys(prefix), offload=offload, offload_bytes=10)
    reply = Reply(request_id="big", ok=True, response={"x": "y" * 50}, meta=Meta())
    assert await requests.put_result(reply, final_state="done")
    with pytest.raises(CallFailed) as info:
        await requests.take_result("big")
    assert info.value.kind == "queue_unavailable"
    again = await requests.take_result("big")
    assert again is not None
    assert again.response == {"x": "y" * 50}
    assert not await store.exists(f"{prefix}:{{r:big}}:res")


async def test_callers_set_expires(store: redis.asyncio.Redis, prefix: str) -> None:
    """S9: an attach and a result write left the callers set with no TTL."""
    requests = RequestStore(store, Keys(prefix))
    kw: dict[str, Any] = {
        "user_hash": "u",
        "params_hash": "p",
        "model": "m",
        "priority": False,
        "app_id": False,
    }
    assert (await requests.register("c", caller="one", **kw)).outcome == "new"
    await store.persist(f"{prefix}:{{r:c}}:callers")
    assert (await requests.register("c", caller="two", **kw)).outcome == "attached"
    assert await store.ttl(f"{prefix}:{{r:c}}:callers") > 0
    await store.persist(f"{prefix}:{{r:c}}:callers")
    await requests.put_result(Reply(request_id="c", ok=True, meta=Meta()), final_state="done")
    assert await store.ttl(f"{prefix}:{{r:c}}:callers") > 0


async def test_dead_letters_are_trimmed_on_a_timer(store: redis.asyncio.Redis, prefix: str) -> None:
    """X10: dead letters were trimmed only when a new one came: a quiet queue kept them."""
    queues = StreamQueues(store, Keys(prefix))
    dlq = Keys(prefix).queue("q", "dlq")
    await store.xadd(dlq, {"body": b"old"}, id="1000-0")
    await store.xadd(dlq, {"body": b"new"})
    assert await queues.trim_dead("q", 60) == 1
    assert await store.xlen(dlq) == 1


def _runner(store: redis.asyncio.Redis, prefix: str, **kw: Any) -> JobRunner:
    settings = QueueSettings(visibility_secs=30, max_receives=5, max_waiting=JOBS_MAX_WAITING)
    return JobRunner(StreamQueues(store, Keys(prefix)), settings=settings, **kw)


async def test_job_timeout(store: redis.asyncio.Redis, prefix: str) -> None:
    """S6: a hung job held its worker slot for good."""
    runner = _runner(store, prefix)
    started = asyncio.Event()

    async def hang(payload: dict[str, Any]) -> None:
        started.set()
        await asyncio.sleep(30)

    runner.register("hang", hang, timeout_secs=0.2)
    await runner.send("hang", {})
    await runner.start()
    try:
        await asyncio.wait_for(started.wait(), 3)
        fails = Keys(prefix).queue(runner.queue("hang"), "fails")

        async def failed() -> bool:
            return bool(await store.hlen(fails))

        for _ in range(60):
            if await failed():
                break
            await asyncio.sleep(0.05)
        assert await failed()
    finally:
        await runner.stop(drain_secs=0.1)


async def test_job_stopped_mid_run_is_returned(store: redis.asyncio.Redis, prefix: str) -> None:
    """S5: a job cut by shutdown counted as a failure (and a new one could start after stop)."""
    runner = _runner(store, prefix)
    started = asyncio.Event()

    async def long(payload: dict[str, Any]) -> None:
        started.set()
        await asyncio.sleep(30)

    runner.register("long", long)
    await runner.send("long", {})
    await runner.start()
    await asyncio.wait_for(started.wait(), 3)
    await runner.stop(drain_secs=0.1)
    keys = Keys(prefix)
    assert not await store.hlen(keys.queue(runner.queue("long"), "fails"))
    again, _ = await runner.queues.receive(runner.queue("long"), 1, runner.settings)
    assert len(again) == 1  # visible at once, not after its 30 s hold


async def test_job_ack_is_retried_apart(
    store: redis.asyncio.Redis, prefix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S8: a failed ack counted as a failed job, which then ran again."""
    runner = _runner(store, prefix)
    runs = 0
    done = asyncio.Event()

    async def job(payload: dict[str, Any]) -> None:
        nonlocal runs
        runs += 1

    real = runner.queues.ack
    left = {"fail": 1}

    async def flaky(message: Any) -> bool:
        if left["fail"]:
            left["fail"] -= 1
            raise ConnectionError("blip")
        acked = await real(message)
        done.set()
        return acked

    monkeypatch.setattr(runner.queues, "ack", flaky)
    runner.register("j", job)
    await runner.send("j", {})
    await runner.start()
    try:
        await asyncio.wait_for(done.wait(), 5)
        assert runs == 1
        keys = Keys(prefix)
        assert not await store.hlen(keys.queue(runner.queue("j"), "fails"))
        assert await store.xlen(keys.queue(runner.queue("j"), "s")) == 0
    finally:
        await runner.stop(drain_secs=0.1)


async def test_job_with_no_consumer_is_reported(store: redis.asyncio.Redis, prefix: str) -> None:
    """S7: jobs sent to a name nobody runs waited, unbounded and silent, until lost."""
    sink = Counter()
    runner = _runner(store, prefix, metrics=sink)
    await runner.send("nobody", {})
    assert sink.total("llmq_jobs_no_consumer_total") == 1
    assert JobRunner(runner.queues).settings.max_waiting == JOBS_MAX_WAITING
