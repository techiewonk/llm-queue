"""The request admin's batched reads and cancel (gate G-T2 fixes) on Valkey and Redis.

`list` reads a page with one script merging every tenant's index, records and stages are one
script per batch, and a cancel marks and answers its waiting calls in one script per batch. These
tests pin what the faster paths must keep: page order and cursors, and the cancel's replies.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any

import pytest
from engine_testkit import CHAT, Make

from llm_queue.engine.errors import CallFailed
from llm_queue.engine.requests_admin import AdminSettings, RequestsAdmin
from llm_queue.engine.runtime import Engine

pytestmark = pytest.mark.integration


def _admin(engine: Engine, **settings: Any) -> RequestsAdmin:
    return RequestsAdmin(
        engine.state,
        keys=engine.keys,
        registry=engine.registry,
        queue_prefix=engine.settings.queue_prefix,
        hash_id=engine.hash_id,
        settings=AdminSettings(**settings),
        status_ttl_secs=engine.settings.status_ttl_secs,
    )


async def test_list_pages_follow_deadline_then_byte_order_over_skewed_tenants(
    make: Make,
) -> None:
    """Pages of 7 over tenants of 40, 3 and 1 calls, few deadlines (ties): one sorted run.

    The page script reads each tenant a few entries at a time; ids tie-break in byte order
    (`B` before `a`), as the sorted set orders them, whatever the store's locale.
    """
    engine, _ = await make(worker=False)
    admin = _admin(engine)
    secs, _ = await engine.state.time()
    base = (int(secs) + 600) * 1000
    tenants = {engine.hash_id(t): n for t, n in (("big", 40), ("mid", 3), ("one", 1))}
    expected: list[tuple[int, bytes, str]] = []  # deadline, id bytes, tenant hash
    async with engine.state.pipeline(transaction=False) as pipe:
        for h, n in tenants.items():
            ix = engine.keys.request_index("small", h)
            for i in range(n):
                rid = f"{'aB'[i % 2]}-{h[:4]}-{i:02d}"
                score = base + (i % 4) * 1000
                expected.append((score, rid.encode(), h))
                pipe.hset(
                    engine.keys.request(rid, "job"),
                    mapping={"state": "queued", "ix": ix, "ixh": h, "direct": "1"},
                )
                pipe.hset(
                    engine.keys.status(rid),
                    mapping={"state": "waiting", "model": "small", "enqueued_at": str(secs)},
                )
                pipe.zadd(ix, {rid: score})
            pipe.zadd(engine.keys.tenant_index("small"), {h: base + 4000})
        await pipe.execute()
    seen: list[str] = []
    cursor: str | None = None
    pages = 0
    while True:
        page = await admin.list("small", limit=7, after=cursor)
        seen += [i.request_id for i in page.items]
        pages += 1
        if page.next is None:
            break
        cursor = page.next
    expected.sort()
    assert seen == [rid.decode() for _, rid, _ in expected]
    assert pages == 7  # 44 calls: six full pages and one of 2
    mid = engine.hash_id("mid")
    one = await admin.list("small", tenant="mid", limit=100)
    assert [i.request_id for i in one.items] == [r.decode() for _, r, h in expected if h == mid]


async def test_list_over_10000_tenants_never_holds_the_store_in_one_long_script(
    make: Make,
) -> None:
    """FA-1: 10,000 tenants of one call each, `list(limit=500)`: no page script runs 100 ms.

    One script merged every tenant, scanning all of them per row (seconds, the store serving
    nothing else). Now each run merges at most 256 tenants; the pages keep deadline-then-id
    order and their cursor.
    """
    engine, _ = await make(worker=False)
    admin = _admin(engine)
    secs, _ = await engine.state.time()
    base = (int(secs) + 600) * 1000
    expected: list[tuple[int, str]] = []
    async with engine.state.pipeline(transaction=False) as pipe:
        for i in range(10_000):
            h = engine.hash_id(f"tenant-{i}")
            ix = engine.keys.request_index("small", h)
            rid = f"r-{(i * 7919) % 10_000:05d}"
            score = base + (i % 997) * 10
            expected.append((score, rid))
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
    expected.sort()
    script = admin._page_script
    runs: list[float] = []

    async def timed(*args: Any, **kwargs: Any) -> Any:
        t0 = time.perf_counter()
        try:
            return await script(*args, **kwargs)
        finally:
            runs.append((time.perf_counter() - t0) * 1000)

    admin._page_script = timed  # type: ignore[assignment]
    first = await admin.list("small", limit=500)
    second = await admin.list("small", limit=500, after=first.next)
    assert [i.request_id for i in first.items + second.items] == [r for _, r in expected[:1000]]
    assert max(runs) < 100, f"slowest page script {max(runs):.0f} ms over {len(runs)} runs"


async def test_cancel_answers_every_waiting_caller_in_batches(make: Make) -> None:
    """A cancel of 7 waiting calls in batches of 3: each caller gets `cancelled`, once.

    The calls leave the request index and their status is `cancelled`, written by the cancel's
    own script (no worker runs); a second cancel finds nothing to do.
    """
    engine, _ = await make(worker=False)
    admin = _admin(engine, batch=3)
    tasks = [
        asyncio.create_task(engine.call("small", CHAT, tenant_id="t", timeout_secs=60))
        for _ in range(7)
    ]
    try:
        for _ in range(300):
            if (await admin.count("small")).total == 7:
                break
            await asyncio.sleep(0.02)
        ids = [i.request_id for i in (await admin.list("small")).items]
        report = await admin.cancel("small", actor="ops", dry_run=False)
        assert (report.matched, report.changed, report.skipped) == (7, 7, {})
        for task in tasks:
            with pytest.raises(CallFailed, match="cancelled"):
                await asyncio.wait_for(task, 5)
        assert (await admin.count("small")).total == 0
        for rid in ids:
            status = await engine.requests.status(rid)
            assert (status["state"], status["error"]) == ("cancelled", "cancelled")
        again = await admin.cancel("small", actor="ops", dry_run=False)
        assert (again.matched, again.changed) == (0, 0)
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(BaseException):
                await task
