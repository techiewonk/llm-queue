"""Review fixes of the request admin (0.2.0 review, RQ-7 to RQ-15; scenarios G14 and G24).

Each test names the finding it proves. Calls wait in a caller-only engine (no worker) unless the
test starts one; records the test needs in a given shape are written by hand.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
from typing import Any

import orjson
import pytest
import redis.asyncio
from engine_testkit import CHAT, SMALL, Make, stats

from llm_queue.engine import requests_admin as requests_admin_module
from llm_queue.engine.fairness import NO_TENANT
from llm_queue.engine.queue import QueueSettings
from llm_queue.engine.requests import IndexEntry
from llm_queue.engine.requests_admin import AdminSettings, RequestsAdmin
from llm_queue.engine.runtime import Engine
from llm_queue.errors import AdminRefused

pytestmark = pytest.mark.integration

A, B = "tenant-a", "tenant-b"


def _admin(engine: Engine, **kw: Any) -> RequestsAdmin:
    return RequestsAdmin(
        engine.state,
        queue_client=kw.pop("queue_client", None),
        keys=engine.keys,
        registry=engine.registry,
        queue_prefix=engine.settings.queue_prefix,
        hash_id=engine.hash_id,
        settings=kw.pop("settings", engine.settings.admin),
        status_ttl_secs=engine.settings.status_ttl_secs,
    )


async def _send(
    engine: Engine, tasks: list[asyncio.Task[Any]], n: int, tenant: str, **kw: Any
) -> None:
    key = engine.keys.request_index("small", engine.hash_id(tenant))
    before = int(await engine.state.zcard(key))
    for _ in range(n):
        call = engine.call("small", CHAT, tenant_id=tenant, timeout_secs=kw.pop("t", 60), **kw)
        tasks.append(asyncio.create_task(call))
    for _ in range(300):
        if int(await engine.state.zcard(key)) >= before + n:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("calls not indexed in time")


async def _close(tasks: list[asyncio.Task[Any]]) -> None:
    for task in tasks:
        task.cancel()
    for task in tasks:
        with contextlib.suppress(BaseException):
            await task


def _queue(engine: Engine, priority: bool = False) -> str:
    return engine._queue_name(engine.registry.current, "small", priority)


async def test_rq7_a_send_short_of_replicas_records_its_message(make: Make) -> None:
    """RQ-7: the message id is kept, so `list` shows it and reprioritize can move it."""
    engine, _ = await make(worker=False)
    engine.replica_wait.replicas = 1  # a standalone store: WAIT always answers 0
    tasks: list[asyncio.Task[Any]] = []
    try:
        await _send(engine, tasks, 1, A)
        item = (await _admin(engine).list("small", tenant=A)).items[0]
        assert item.message_id
        assert item.stage == "queued"
        report = await _admin(engine).reprioritize(
            "small", priority=True, tenant=A, actor="ops", dry_run=False
        )
        assert report.changed == 1
    finally:
        await _close(tasks)


async def test_rq8_moved_records_are_written_again_after_a_store_error(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RQ-8: a separate queue store's MOVE stands; the record write is tried again."""
    engine, _ = await make(worker=False)
    tasks: list[asyncio.Task[Any]] = []
    try:
        await _send(engine, tasks, 2, A)
        other = redis.asyncio.Redis(connection_pool=engine.state.connection_pool)
        admin = _admin(engine, queue_client=other)
        assert not admin.queues_shared
        real = admin._write_moved
        fails = [1]

        async def flaky(moved: Any, cls: str) -> list[str]:
            if fails:
                fails.pop()
                raise redis.exceptions.ConnectionError("blip")
            return await real(moved, cls)

        monkeypatch.setattr(admin, "_write_moved", flaky)
        report = await admin.reprioritize(
            "small", priority=True, tenant=A, actor="ops", dry_run=False
        )
        assert report.changed == 2
        for item in (await admin.list("small", tenant=A)).items:
            job = await engine.requests.job(item.request_id)
            assert (job["q"], item.priority) == ("p", True)
            assert job["mid"] == item.message_id
    finally:
        await _close(tasks)


async def test_rq8_a_move_left_unrecorded_is_repaired_by_the_next_admin_read(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RQ-8: a process that stopped between MOVE and the record write; the marks repair it."""
    engine, _ = await make(worker=False)
    tasks: list[asyncio.Task[Any]] = []
    try:
        await _send(engine, tasks, 2, A)
        other = redis.asyncio.Redis(connection_pool=engine.state.connection_pool)
        crashed = _admin(engine, queue_client=other)

        async def stop(*_: Any) -> list[str]:  # the process dies right after MOVE
            return []

        monkeypatch.setattr(crashed, "_moved_records", stop)
        report = await crashed.reprioritize(
            "small", priority=True, tenant=A, actor="ops", dry_run=False
        )
        assert report.changed == 2
        marks = crashed._queues.moved_key(_queue(engine, priority=True))
        assert int(await engine.state.hlen(marks)) == 2
        expiring = int(await engine.state.pttl(marks)) > 0  # native, or swept: in `P:ttl`
        assert expiring or await engine.state.zscore(f"{engine.keys.prefix}:ttl", marks)
        rids = [rid.decode() for rid in await engine.state.hkeys(marks)]
        for rid in rids:  # the records still name the old message
            assert (await engine.requests.job(rid))["q"] == "n"

        admin = _admin(engine, queue_client=other)  # the next process
        items = (await admin.list("small", tenant=A)).items
        assert sorted(i.request_id for i in items) == sorted(rids)
        for item in items:
            job = await engine.requests.job(item.request_id)
            assert (job["q"], item.priority, item.stage) == ("p", True, "queued")
            assert job["mid"] == item.message_id
            assert await engine.state.hget(engine.keys.status(item.request_id), "priority") == b"1"
        assert not await engine.state.exists(marks)
        entries: Any = await engine.state.xrange(engine.registry.audit_key)
        repairs = [e[1] for e in entries if e[1][b"op"] == b"requests_reprioritize_repair"]
        assert len(repairs) == 1
        assert b"repair@" in repairs[0][b"actor"]
        # Repaired calls are where their records say: a move back finds every one.
        back = await admin.reprioritize(
            "small", priority=False, tenant=A, actor="ops", dry_run=False
        )
        assert (back.changed, back.skipped) == (2, {})
        assert len((await admin.list("small", tenant=A)).items) == 2  # nothing left to repair
        entries = await engine.state.xrange(engine.registry.audit_key)
        assert sum(e[1][b"op"] == b"requests_reprioritize_repair" for e in entries) == 1
    finally:
        await _close(tasks)


async def test_rq8_a_repair_never_puts_back_an_older_message(make: Make) -> None:
    """RQ-8: a stale mark (a later move recorded a newer id) is cleared, the record kept."""
    engine, _ = await make(worker=False)
    tasks: list[asyncio.Task[Any]] = []
    try:
        await _send(engine, tasks, 1, A)
        other = redis.asyncio.Redis(connection_pool=engine.state.connection_pool)
        admin = _admin(engine, queue_client=other)
        rid = (await admin.list("small", tenant=A)).items[0].request_id
        job_key = engine.keys.request(rid, "job")
        mid = (await engine.requests.job(rid))["mid"]
        marks = admin._queues.moved_key(_queue(engine, priority=True))
        await engine.state.hset(marks, rid, "1-0")  # older than the call's own message
        await engine.state.hset(marks, "gone-call", "9999999999999-0")  # no record: not made
        detail = await admin.get(rid)
        assert detail is not None
        assert detail.summary is not None
        assert not detail.summary.priority
        assert (await engine.requests.job(rid))["q"] == "n"
        assert not await engine.state.exists(marks)
        assert not await engine.state.exists(engine.keys.request("gone-call", "job"))
        assert await engine.state.hget(job_key, "mid") == mid.encode()
    finally:
        await _close(tasks)


async def test_rq9_a_takeover_under_another_tenant_leaves_the_old_index(make: Make) -> None:
    """RQ-9: the old tenant's index loses the id (it was listed twice, cancellable by both)."""
    engine, _ = await make(worker=False)
    rid = "rq9-call"
    await engine.requests.register(
        rid, caller="c1", user_hash="u", params_hash="p", model="small", priority=False, app_id=True
    )

    def entry(tenant: str) -> IndexEntry:
        h = engine.hash_id(tenant)
        keys = (engine.keys.request_index("small", h), engine.keys.tenant_index("small"))
        return IndexEntry(keys=keys, tenant_hash=h, keep_ms=60_000, cls="n")

    await engine.requests.confirm(rid, keep_secs=60, index=entry(A), message_id="1-0")
    await engine.requests.confirm(rid, keep_secs=60, index=entry(B), message_id="2-0")
    old = engine.keys.request_index("small", engine.hash_id(A))
    new = engine.keys.request_index("small", engine.hash_id(B))
    assert await engine.state.zscore(old, rid) is None
    assert await engine.state.zscore(new, rid) is not None


async def test_rq10_count_with_the_index_off_refuses(make: Make) -> None:
    """RQ-10 / G25: zeros would read as "nothing waiting" during an incident."""
    engine, _ = await make(worker=False)
    admin = _admin(engine, settings=AdminSettings(request_index=False))
    with pytest.raises(AdminRefused, match="LLMQ_REQUEST_INDEX_ENABLED"):
        await admin.count("small", tenant=A)


async def test_rq11_no_tenant_status_reads_the_fairness_bucket(make: Make) -> None:
    """RQ-11: the no-tenant block read fairness under `-`; fairness writes the no-tenant hash."""
    engine, _ = await make({"small": {"fairness": {"mode": "share"}}}, worker=False)
    bucket = engine.hash_id(NO_TENANT)
    await engine.state.hset(engine.keys.model("small", "tf:w"), bucket, "7")
    report = await engine.status("small", tenant_hash="-")
    assert report["tenant"]["fairness"]["weight"] == 7.0


async def test_rq13_cancel_logs_only_the_calls_it_cancelled(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, _ = await make(worker=False)
    tasks: list[asyncio.Task[Any]] = []
    logged: list[str] = []

    def info(event: str, **fields: Any) -> None:
        if event == "call_cancel":
            logged.append(fields["request_id"])

    monkeypatch.setattr(requests_admin_module.logger, "info", info)
    try:
        await _send(engine, tasks, 2, A)
        admin = _admin(engine)
        ids = [i.request_id for i in (await admin.list("small", tenant=A)).items]
        read = admin._rows_by_id

        async def granted_after_the_read(wanted: Any) -> Any:
            rows = await read(wanted)  # both still waiting here
            await engine.state.hset(engine.keys.request(ids[1], "job"), "state", "running")
            return rows

        monkeypatch.setattr(admin, "_rows_by_id", granted_after_the_read)
        report = await admin.cancel(request_ids=ids, state="waiting", actor="ops", dry_run=False)
        assert report.skipped == {"started": 1}
        assert report.changed == 1
        assert logged == [ids[0]]
    finally:
        await _close(tasks)


async def test_rq14_lapsed_tenants_leave_the_names_hash(make: Make) -> None:
    """RQ-14: `tqn` only grew while traffic went on."""
    engine, _ = await make(worker=False)
    admin_settings = dataclasses.replace(engine.settings.admin, tenant_names=True)
    engine.settings = dataclasses.replace(
        engine.settings, admin=admin_settings, caller_margin_secs=0.2
    )
    names = engine.keys.tenant_names("small")
    tasks: list[asyncio.Task[Any]] = []
    try:
        await _send(engine, tasks, 1, A, t=1)
        assert await engine.state.hexists(names, engine.hash_id(A))
        await asyncio.sleep(1.5)  # A's only call lapses (1 s + 0.2 s margin)
        await _send(engine, tasks, 1, B)
        assert not await engine.state.hexists(names, engine.hash_id(A))
        assert await engine.state.hexists(names, engine.hash_id(B))
    finally:
        await _close(tasks)


async def test_rq15_a_full_target_counts_every_call_left(make: Make) -> None:
    """RQ-15: after a `full` stop, the calls of later batches were not counted."""
    engine, _ = await make({"small": {"max_waiting": 3}}, worker=False)
    tasks: list[asyncio.Task[Any]] = []
    try:
        await _send(engine, tasks, 3, A)
        await _send(engine, tasks, 3, B, priority=True)  # the priority queue is full
        admin = _admin(engine, settings=AdminSettings(move_batch=1))
        report = await admin.reprioritize(
            "small", priority=True, tenant=A, actor="ops", dry_run=False
        )
        assert (report.matched, report.changed, report.skipped) == (3, 0, {"full": 3})
    finally:
        await _close(tasks)


async def test_cancel_answers_waiting_calls_in_its_own_script(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancel answers its waiting calls in the script that marks them, not per id."""
    engine, _ = await make(worker=False)
    tasks: list[asyncio.Task[Any]] = []
    admin = _admin(engine)

    async def one_by_one(*_: Any, **__: Any) -> bool:
        raise AssertionError("one round trip per id")

    monkeypatch.setattr(admin._requests, "put_result", one_by_one)
    try:
        await _send(engine, tasks, 5, A)
        report = await admin.cancel("small", tenant=A, actor="ops", dry_run=False)
        assert report.changed == 5
        for task in tasks:
            with pytest.raises(Exception, match="cancelled"):
                await asyncio.wait_for(task, 3)
    finally:
        await _close(tasks)


async def test_get_of_a_call_on_a_removed_model_is_clean(make: Make) -> None:
    """`get` read the removed model's queue name and raised a bare KeyError."""
    engine, _ = await make(worker=False)
    rid = "on-removed"
    await engine.state.hset(engine.keys.request(rid, "job"), mapping={"state": "queued"})
    await engine.state.hset(engine.keys.status(rid), mapping={"state": "waiting", "model": "gone"})
    detail = await _admin(engine).get(rid)
    assert detail is not None
    assert (detail.final, detail.summary) == (False, None)
    assert detail.status["model"] == "gone"


async def test_g14_a_moved_call_runs_once_after_a_receive_replay_and_reclaim(
    make: Make,
) -> None:
    """G14: reprioritize, then a replayed receive and RECLAIM of the old ids: each call once."""
    engine, _ = await make(worker=False)
    tasks: list[asyncio.Task[Any]] = []
    try:
        await _send(engine, tasks, 2, A)
        admin = _admin(engine)
        old = [i.message_id for i in (await admin.list("small", tenant=A)).items]
        done = await admin.reprioritize(
            "small", priority=True, tenant=A, actor="ops", dry_run=False
        )
        assert done.changed == 2
        source = engine.keys.queue(_queue(engine), "")
        claim = engine.queues.claim_key("g14")
        body = orjson.dumps({"n": "x", "b": source, "bell": "", "t": [*old, "1"]})
        await engine.state.set(claim, body)
        assert await engine.queues.reclaim(claim, "x") == 0
        replay, _ = await engine.queues.receive(_queue(engine), 10, QueueSettings())
        assert replay == []  # the old messages are gone from the normal queue
        _, fake = await make(caller=False)
        replies = await asyncio.wait_for(asyncio.gather(*tasks), 15)
        assert all(r.ok for r in replies)
        assert stats(fake, f"{SMALL}@r1")["calls"] == 2
    finally:
        await _close(tasks)


async def test_g24_paging_across_tenants_has_no_duplicate_or_gap(make: Make) -> None:
    """G24: 3 tenants, 250 calls with one deadline; pages of 100 while calls end and one moves."""
    engine, _ = await make(worker=False)
    admin = _admin(engine)
    secs, _ = await engine.state.time()
    score = (int(secs) + 600) * 1000
    tenants = [engine.hash_id(t) for t in ("t1", "t2", "t3")]
    ids: list[str] = []
    async with engine.state.pipeline(transaction=False) as pipe:
        for n in range(250):
            h = tenants[n % 3]
            rid = f"g24-{n:03d}"
            ids.append(rid)
            ix = engine.keys.request_index("small", h)
            pipe.hset(
                engine.keys.request(rid, "job"),
                mapping={"state": "queued", "ix": ix, "ixh": h, "direct": "1"},
            )
            pipe.hset(
                engine.keys.status(rid),
                mapping={"state": "waiting", "model": "small", "enqueued_at": str(secs)},
            )
            pipe.zadd(ix, {rid: score})
            pipe.zadd(engine.keys.tenant_index("small"), {h: score})
        await pipe.execute()
    first = await admin.list("small", limit=100)
    seen = [i.request_id for i in first.items]
    unseen = [rid for rid in ids if rid not in seen]
    finished = unseen[:10]  # end while paging
    moved = unseen[-1]  # extended: a later deadline, still unread
    for rid in finished:
        await engine.state.hset(engine.keys.request(rid, "job"), "state", "done")
    job = await engine.requests.job(moved)
    await engine.state.zadd(job["ix"], {moved: score + 1000}, xx=True)
    cursor = first.next
    while cursor is not None:
        page = await admin.list("small", limit=100, after=cursor)
        seen += [i.request_id for i in page.items]
        cursor = page.next
    assert len(seen) == len(set(seen))  # no duplicate
    assert set(seen) == set(ids) - set(finished)  # no gap
