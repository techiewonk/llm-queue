"""`RequestsAdmin` (0.2.0 T2) on Valkey and Redis: list, count, get, cancel, reprioritize.

Calls wait in a caller-only engine's queues (no worker), so their stages are set by hand where
a worker would: `held` (taken, waiting for budget), `delayed` (returned with a delay),
`running` (granted). Tenant A's operations never touch tenant B's calls on the same model.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import orjson
import pytest
import redis.asyncio
from engine_testkit import CHAT, Make

from llm_queue.engine.envelope import Reply
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.queue import QueueSettings
from llm_queue.engine.requests_admin import AdminSettings, RequestsAdmin
from llm_queue.engine.runtime import Engine
from llm_queue.errors import AdminRefused, ConfigError

pytestmark = pytest.mark.integration

A, B = "tenant-a", "tenant-b"


def _admin(engine: Engine, **kw: Any) -> RequestsAdmin:
    settings = kw.pop("settings", engine.settings.admin)
    return RequestsAdmin(
        engine.state,
        queue_client=kw.pop("queue_client", None),
        keys=engine.keys,
        registry=engine.registry,
        queue_prefix=engine.settings.queue_prefix,
        hash_id=engine.hash_id,
        settings=settings,
        status_ttl_secs=engine.settings.status_ttl_secs,
        metrics=engine.metrics,
    )


class _Calls:
    """Calls sent by a caller-only engine and left waiting; cancelled at the end."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.tasks: list[asyncio.Task[Any]] = []

    async def send(self, n: int, tenant: str | None, **kw: Any) -> None:
        index = self.engine.keys.request_index(
            "small", self.engine.hash_id(tenant) if tenant else "-"
        )
        before = int(await self.engine.state.zcard(index))
        for _ in range(n):
            self.tasks.append(
                asyncio.create_task(
                    self.engine.call("small", CHAT, tenant_id=tenant, timeout_secs=60, **kw)
                )
            )
        for _ in range(300):
            if int(await self.engine.state.zcard(index)) >= before + n:
                return
            await asyncio.sleep(0.02)
        raise AssertionError("calls not indexed in time")

    async def close(self) -> None:
        for task in self.tasks:
            task.cancel()
        for task in self.tasks:
            with contextlib.suppress(BaseException):
                await task


@pytest.fixture
async def caller(make: Make) -> AsyncIterator[tuple[Engine, _Calls]]:
    engine, _ = await make(worker=False)
    calls = _Calls(engine)
    yield engine, calls
    await calls.close()


def _queue(engine: Engine, priority: bool = False) -> str:
    return engine._queue_name(engine.registry.current, "small", priority)


async def _dump(store: redis.asyncio.Redis, prefix: str) -> dict[str, Any]:
    """Every key under the prefix with its type and content (a dry run must change nothing)."""
    out: dict[str, Any] = {}
    async for key in store.scan_iter(match=f"{prefix}*", count=500):
        if not any(part in key for part in (b"{r:", b":tq", b"{q:", b"audit")):
            continue  # the engine's own beats and sweeps write elsewhere meanwhile
        kind = (await store.type(key)).decode()
        if kind == "hash":
            out[key] = sorted((await store.hgetall(key)).items())
        elif kind == "zset":
            out[key] = await store.zrange(key, 0, -1, withscores=True)
        elif kind == "stream":
            out[key] = await store.xrange(key)
        elif kind == "set":
            out[key] = sorted(await store.smembers(key))
        elif kind == "list":
            out[key] = await store.lrange(key, 0, -1)
        else:
            out[key] = await store.get(key)
    return out


# ------------------------------------------------------------------ reads


async def test_list_and_count_by_tenant(caller: tuple[Engine, _Calls]) -> None:
    engine, calls = caller
    admin = _admin(engine)
    await calls.send(3, A)
    await calls.send(2, B)
    await calls.send(1, None)
    page = await admin.list("small", tenant=A)
    assert len(page.items) == 3
    assert {i.tenant for i in page.items} == {engine.hash_id(A)}
    assert all(i.stage == "queued" and i.state == "waiting" for i in page.items)
    assert all(i.message_id and i.deadline_in_secs and i.deadline_in_secs > 50 for i in page.items)
    assert page.next is None
    nobody = await admin.list("small", tenant_hash="-")
    assert [i.tenant for i in nobody.items] == ["-"]
    every = await admin.list("small")
    assert len(every.items) == 6
    deadlines = [i.deadline_in_secs or 0 for i in every.items]
    assert deadlines == sorted(deadlines)  # merged across tenants by deadline
    counts = await admin.count("small", tenant=B)
    assert (counts.total, counts.by_state, counts.truncated) == (
        2,
        {"waiting": 2, "running": 0},
        False,
    )
    assert counts.by_stage["queued"] == 2
    assert counts.oldest_wait_secs is not None
    assert (await admin.count("small", tenant="nobody")).total == 0
    with pytest.raises(ConfigError):
        await admin.list("small", tenant=A, tenant_hash="-")
    with pytest.raises(ConfigError):
        await admin.list("small", tenant_hash="not-a-hash")
    with pytest.raises(ConfigError):
        await admin.list("small", limit=0)


async def test_pagination_is_stable_while_calls_finish(caller: tuple[Engine, _Calls]) -> None:
    engine, calls = caller
    admin = _admin(engine)
    await calls.send(5, A)
    first = await admin.list("small", tenant=A, limit=2)
    assert len(first.items) == 2
    assert first.next is not None
    # A call of the first page ends meanwhile: the next page is unaffected.
    await engine.requests.put_result(
        Reply(request_id=first.items[0].request_id, ok=True), final_state="done"
    )
    second = await admin.list("small", tenant=A, limit=2, after=first.next)
    third = await admin.list("small", tenant=A, limit=2, after=second.next)
    seen = [i.request_id for i in (*first.items, *second.items, *third.items)]
    assert len(seen) == len(set(seen)) == 5
    assert third.next is None


async def test_stages_held_delayed_running_direct_and_cancelling(
    caller: tuple[Engine, _Calls],
) -> None:
    engine, calls = caller
    admin = _admin(engine)
    await calls.send(4, A)
    queue = _queue(engine)
    held, _ = await engine.queues.receive(queue, 1, QueueSettings(visibility_secs=60))
    delayed, _ = await engine.queues.receive(queue, 1, QueueSettings(visibility_secs=60))
    await engine.queues.return_(delayed[0], delay_secs=60)
    page = await admin.list("small", tenant=A)
    by_mid = {i.message_id: i for i in page.items}
    assert by_mid[held[0].id].stage == "held"
    assert by_mid[delayed[0].id].stage == "delayed"
    # Running: what a grant writes (job state and the in-progress entry).
    run = next(i for i in page.items if i.stage == "queued")
    await engine.state.hset(engine.keys.request(run.request_id, "job"), "state", "running")
    await engine.state.hset(
        engine.keys.model("small", f"inprog:{run.request_id}"), mapping={"dep": "aws-a/r1"}
    )
    other = next(i for i in page.items if i.stage == "queued" and i is not run)
    await engine.cancel(other.request_id)
    page = await admin.list("small", tenant=A, state="running")
    assert [(i.request_id, i.deployment) for i in page.items] == [(run.request_id, "aws-a/r1")]
    waiting = await admin.list("small", tenant=A, state="waiting")
    assert {i.request_id for i in waiting.items if i.cancelling} == {other.request_id}
    counts = await admin.count("small", tenant=A)
    assert counts.by_stage == {"queued": 1, "delayed": 1, "held": 1, "direct": 0, "running": 1}
    assert counts.cancelling == 1
    detail = await admin.get(run.request_id)
    assert detail is not None
    assert detail.summary is not None
    assert (detail.final, detail.summary.stage, detail.callers) == (False, "running", 1)
    assert await admin.get("no-such-id") is None


async def test_direct_stage(make: Make) -> None:
    slow = {"defaults": {"latency_ms": {"p50": 1500, "p95": 1500}}}
    engine, _ = await make({"small": {"timeout_secs": 30, "attempt_timeout_secs": 20}}, slow)
    admin = _admin(engine)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id=A, force_direct=True))
    for _ in range(200):
        page = await admin.list("small", tenant=A)
        if page.items:
            break
        await asyncio.sleep(0.02)
    assert page.items[0].stage in ("direct", "running")
    assert page.items[0].message_id is None
    assert (await task).ok


# ------------------------------------------------------------------ cancel


async def test_cancel_waiting_running_all_and_by_ids(
    caller: tuple[Engine, _Calls], store: redis.asyncio.Redis, prefix: str
) -> None:
    engine, calls = caller
    admin = _admin(engine)
    await calls.send(3, A)
    await calls.send(2, B)
    page = await admin.list("small", tenant=A)
    running = page.items[0].request_id
    await engine.state.hset(engine.keys.request(running, "job"), "state", "running")

    before = await _dump(store, prefix)
    plan = await admin.cancel("small", tenant=A, actor="ops")  # dry run by default (D3)
    assert (plan.dry_run, plan.matched, plan.changed, plan.by_state) == (
        True,
        2,
        0,
        {"waiting": 2},
    )
    assert plan.skipped == {"other_state": 1}
    assert await _dump(store, prefix) == before  # nothing written, no audit

    done = await admin.cancel("small", tenant=A, actor="ops", dry_run=False)
    assert (done.matched, done.changed) == (2, 2)
    assert not await engine.state.exists(engine.keys.request(running, "cancel"))
    # The waiting ones were answered `cancelled` at once: a second run finds nothing to do.
    again = await admin.cancel("small", tenant=A, actor="ops", dry_run=False)
    assert (again.matched, again.changed) == (0, 0)
    run = await admin.cancel("small", tenant=A, state="running", actor="ops", dry_run=False)
    assert (run.matched, run.changed, run.sample_ids) == (1, 1, [running])
    # A running call stays until its worker ends it: cancelling it again is `already_cancelling`.
    twice = await admin.cancel("small", tenant=A, state="all", actor="ops", dry_run=False)
    assert (twice.matched, twice.skipped) == (0, {"already_cancelling": 1})
    # Tenant B untouched.
    b = await admin.list("small", tenant=B)
    assert len(b.items) == 2
    assert not any(i.cancelling for i in b.items)
    # By ids: all states; one unknown id is reported, not refused.
    ids = [i.request_id for i in b.items]
    by_ids = await admin.cancel(request_ids=[*ids, "nope"], state="all", actor="ops", dry_run=False)
    assert (by_ids.matched, by_ids.changed, by_ids.skipped) == (2, 2, {"final": 1})
    # One audit entry per real run, with counts and no tenant name.
    entries: Any = await store.xrange(engine.registry.audit_key)
    ours = [e[1] for e in entries if e[1][b"op"] == b"requests_cancel"]
    assert len(ours) == 3  # a run that changed nothing writes none
    assert all(A.encode() not in e[b"diff"] and B.encode() not in e[b"diff"] for e in ours)
    assert engine.hash_id(A).encode() in ours[0][b"diff"]
    with pytest.raises(ConfigError):
        await admin.cancel(request_ids=["x"], tenant=A, actor="ops")
    with pytest.raises(ConfigError):
        await admin.cancel(actor="ops")


async def test_cancel_waiting_skips_a_call_granted_meanwhile(
    caller: tuple[Engine, _Calls],
) -> None:
    """The race: a call granted between the read and the script stays running (`started`)."""
    engine, calls = caller
    await calls.send(1, A)
    rid = (await _admin(engine).list("small", tenant=A)).items[0].request_id
    await engine.state.hset(engine.keys.request(rid, "job"), "state", "running")
    marked, started = await engine.requests.cancel_counts(
        [rid], ttl_secs=60, channel=engine.keys.cancel_channel, only_state="queued"
    )
    assert (marked, started) == (0, 1)
    assert not await engine.state.exists(engine.keys.request(rid, "cancel"))


async def test_cancelled_waiting_calls_fail_cancelled_at_once(make: Make) -> None:
    """S-T: a cancelled waiting call ends now, not when a worker reaches its message."""
    engine, _ = await make(worker=False)
    calls = _Calls(engine)
    try:
        await calls.send(2, A)
        report = await _admin(engine).cancel("small", tenant=A, actor="ops", dry_run=False)
        assert report.changed == 2
        for task in calls.tasks:  # no worker runs: the admin answered them
            with pytest.raises(CallFailed, match="cancelled"):
                await asyncio.wait_for(task, 3)
        assert await engine.state.zcard(engine.keys.request_index("small", engine.hash_id(A))) == 0
        stream = engine.keys.queue(_queue(engine), "s")
        assert await engine.state.xlen(stream) == 2  # still there: the next worker acks them
        worker, _ = await make(caller=False)
        for _ in range(200):
            if not await engine.state.xlen(stream):
                break
            await asyncio.sleep(0.05)
        assert await engine.state.xlen(stream) == 0
        assert worker.keys.prefix == engine.keys.prefix
    finally:
        await calls.close()


async def test_index_off_refuses(caller: tuple[Engine, _Calls]) -> None:
    engine, _ = caller
    admin = _admin(engine, settings=AdminSettings(request_index=False))
    with pytest.raises(AdminRefused, match="LLMQ_REQUESTS_INDEX_ENABLED"):
        await admin.list("small", tenant=A)
    with pytest.raises(AdminRefused):
        await admin.cancel("small", tenant=A, state="running", actor="ops")


async def test_scan_cap_truncates_counts_and_refuses_writes(
    caller: tuple[Engine, _Calls],
) -> None:
    engine, calls = caller
    await calls.send(3, A)
    admin = _admin(engine)
    admin.settings = AdminSettings(scan_max=2)  # below the env range: a test-only cap
    counts = await admin.count("small", tenant=A)
    assert counts.truncated
    assert counts.total == 3
    with pytest.raises(AdminRefused, match="LLMQ_ADMIN_SCAN_MAX"):
        await admin.cancel("small", tenant=A, actor="ops")


# ------------------------------------------------------------------ reprioritize


async def test_reprioritize_moves_only_that_tenants_queued_calls(
    caller: tuple[Engine, _Calls],
) -> None:
    engine, calls = caller
    admin = _admin(engine)
    await calls.send(4, A)
    await calls.send(2, B)
    await calls.send(1, A, priority=True)  # already there
    queue = _queue(engine)
    _held, _ = await engine.queues.receive(queue, 1, QueueSettings(visibility_secs=60))
    delayed, _ = await engine.queues.receive(queue, 1, QueueSettings(visibility_secs=60))
    await engine.queues.return_(delayed[0], delay_secs=60)
    before = {i.request_id: i for i in (await admin.list("small", tenant=A)).items}

    plan = await admin.reprioritize("small", priority=True, tenant=A, actor="ops")
    assert (plan.dry_run, plan.matched) == (True, 2)
    assert plan.skipped == {"held": 1, "delayed": 1, "already": 1}

    done = await admin.reprioritize("small", priority=True, tenant=A, actor="ops", dry_run=False)
    assert (done.matched, done.changed) == (2, 2)
    after = {i.request_id: i for i in (await admin.list("small", tenant=A)).items}
    moved = list(done.sample_ids)
    for rid in moved:
        assert after[rid].priority
        old = before[rid].message_id
        assert old is not None
        new = after[rid].message_id
        assert new != old
        # The copy keeps its original place: `pos` is the id it was sent with (Q9).
        entry: Any = await engine.state.xrange(
            engine.keys.queue(_queue(engine, True), "s"), min=new, max=new
        )
        assert entry[0][1][b"pos"].decode() == old
        status = await engine.requests.status(rid)
        assert status["priority"] == "1"
    # B untouched; a second run moves nothing more.
    assert not any(i.priority for i in (await admin.list("small", tenant=B)).items)
    again = await admin.reprioritize("small", priority=True, tenant=A, actor="ops", dry_run=False)
    assert again.changed == 0
    assert again.skipped["already"] == 3


async def test_moved_call_keeps_deadline_and_is_served_in_its_new_class(
    make: Make,
) -> None:
    engine, _ = await make(worker=False)
    calls = _Calls(engine)
    try:
        await calls.send(1, A)
        admin = _admin(engine)
        old = (await admin.list("small", tenant=A)).items[0]
        await asyncio.sleep(0.05)
        await admin.reprioritize("small", priority=True, tenant=A, actor="ops", dry_run=False)
        messages, _ = await engine.queues.receive(
            _queue(engine, True), 1, QueueSettings(visibility_secs=0.1)
        )
        assert messages[0].pos == old.message_id
        assert old.message_id is not None
        assert messages[0].position_ms == int(old.message_id.split("-")[0])
        assert messages[0].id != old.message_id
        # A receive replay or RECLAIM naming the old id does nothing: it has no generation.
        source = engine.keys.queue(_queue(engine), "")
        claim = engine.queues.claim_key("test")
        body = orjson.dumps({"n": "x", "b": source, "bell": "", "t": [old.message_id, "1"]})
        await engine.state.set(claim, body)
        assert await engine.queues.reclaim(claim, "x") == 0
        assert (
            await engine.state.zscore(engine.keys.queue(_queue(engine), "vis"), old.message_id)
            is None
        )
        await asyncio.sleep(0.35)  # the 0.1 s hold lapses (review B-26: a 50 ms margin flaked)
        worker, _ = await make(caller=False)
        reply = await asyncio.wait_for(calls.tasks[0], 15)
        assert reply.ok
        assert worker.keys.prefix == engine.keys.prefix
    finally:
        await calls.close()


async def test_reprioritize_stops_when_the_target_is_full(make: Make) -> None:
    engine, _ = await make({"small": {"max_waiting": 2}}, worker=False)
    calls = _Calls(engine)
    try:
        await calls.send(2, A)
        await calls.send(1, B, priority=True)
        await calls.send(1, A, priority=True)
        report = await _admin(engine).reprioritize(
            "small", priority=True, tenant=A, actor="ops", dry_run=False
        )
        assert (report.matched, report.changed, report.skipped.get("full")) == (2, 0, 2)
    finally:
        await calls.close()


async def test_byte_cap_splits_runs_and_separate_store_variant(
    caller: tuple[Engine, _Calls],
) -> None:
    engine, calls = caller
    big = {"messages": [{"role": "user", "content": [{"text": "x" * 40_000}]}]}
    for _ in range(3):
        calls.tasks.append(
            asyncio.create_task(engine.call("small", big, tenant_id=A, timeout_secs=60))
        )
    for _ in range(300):
        if await engine.state.zcard(engine.keys.request_index("small", engine.hash_id(A))) >= 3:
            break
        await asyncio.sleep(0.02)
    # Another client object on the same store: the records are written by a pipeline.
    other = redis.asyncio.Redis(connection_pool=engine.state.connection_pool)
    admin = _admin(
        engine, queue_client=other, settings=AdminSettings(move_bytes=65_536, move_batch=10)
    )
    assert not admin.queues_shared
    report = await admin.reprioritize("small", priority=True, tenant=A, actor="ops", dry_run=False)
    assert report.changed == 3  # one ~40 KB body per run under a 64 KB cap
    for item in (await admin.list("small", tenant=A)).items:
        assert item.priority
        job = await engine.requests.job(item.request_id)
        assert (job["q"], job["mid"]) == ("p", item.message_id)
