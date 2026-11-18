"""`RequestsAdmin.extend` (0.2.0 T2, owner D6): one new deadline in all three places.

The records (job `dl`, TTLs, the index score), the worker (a notice on the cancel channel, or
`dl` read when it reaches the deadline it knows) and the waiting caller (its timer moves) all
see it. Without the extend each call below would end `deadline_exceeded` / `expired`.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses

import pytest
from engine_testkit import CHAT, SMALL, Make, stats

from llm_queue.engine.errors import CallFailed
from llm_queue.engine.requests import extend_notice
from llm_queue.engine.runtime import Engine
from llm_queue.errors import AdminRefused, ConfigError

pytestmark = pytest.mark.integration

SLOW = {"defaults": {"latency_ms": {"p50": 3000, "p95": 3000}}}
MODEL = {"small": {"timeout_secs": 30, "attempt_timeout_secs": 20}}


def _short_margin(engine: Engine) -> None:
    """The caller waits 0.5 s past the worker's deadline (5 s by default): a quick test."""
    engine.settings = dataclasses.replace(engine.settings, caller_margin_secs=0.5)
    engine.requests_admin.caller_margin_secs = 0.5


async def _running(engine: Engine, rid: str) -> None:
    for _ in range(300):
        if (await engine.requests.job(rid)).get("state") == "running":
            return
        await asyncio.sleep(0.02)
    raise AssertionError("call never ran")


async def _rid(engine: Engine, tenant: str = "acme") -> str:
    key = engine.keys.request_index("small", engine.hash_id(tenant))
    for _ in range(300):
        found = await engine.state.zrange(key, 0, 0)
        if found:
            return found[0].decode()
        await asyncio.sleep(0.02)
    raise AssertionError("call not indexed")


def test_notice_format() -> None:
    assert extend_notice("\x00x\x00123\x00req:1") == ("req:1", 123)
    assert extend_notice("req-1") is None
    assert extend_notice("\x00x\x00bad\x00r") is None


async def test_caller_and_worker_see_the_new_deadline(make: Make) -> None:
    """The running attempt outlives the old deadline; the caller waits for the reply."""
    engine, _ = await make(MODEL, SLOW)
    _short_margin(engine)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme", timeout_secs=2))
    rid = await _rid(engine)
    report = await engine.requests_admin.extend(rid, timeout_secs=12, actor="ops", dry_run=False)
    assert (report.matched, report.changed) == (1, 1)
    reply = await asyncio.wait_for(task, 15)  # ~3 s of latency: past the old 2.5 s
    assert reply.ok


async def test_a_worker_that_takes_it_later_reads_the_extend(make: Make) -> None:
    caller, _ = await make(worker=False)
    _short_margin(caller)
    task = asyncio.create_task(caller.call("small", CHAT, tenant_id="acme", timeout_secs=2))
    rid = await _rid(caller)
    admin = caller.requests_admin
    plan = await admin.extend(rid, timeout_secs=8, actor="ops")  # dry run: nothing moves
    assert (plan.dry_run, plan.matched, plan.changed) == (True, 1, 0)
    assert "dl" not in await caller.requests.job(rid)
    before = await caller.state.zscore(
        caller.keys.request_index("small", caller.hash_id("acme")), rid
    )
    done = await admin.extend(rid, timeout_secs=8, actor="ops", dry_run=False)
    assert done.changed == 1
    job = await caller.requests.job(rid)
    assert int(job["dl"]) > 0
    after = await caller.state.zscore(
        caller.keys.request_index("small", caller.hash_id("acme")), rid
    )
    assert before is not None
    assert after is not None
    assert after > before
    again = await admin.extend(rid, timeout_secs=8, actor="ops", dry_run=False)
    assert again.skipped == {"already": 1}
    await asyncio.sleep(3)  # past the first deadline (2 s) and the caller's (2.5 s)
    assert not task.done()
    worker, _ = await make(caller=False)  # takes it now: its first deadline is long gone
    reply = await asyncio.wait_for(task, 10)
    assert reply.ok
    assert worker.keys.prefix == caller.keys.prefix
    final = await admin.extend(rid, timeout_secs=9, actor="ops", dry_run=False)
    assert final.skipped == {"final": 1}


async def test_extend_refuses_past_the_models_cap(make: Make) -> None:
    engine, _ = await make(worker=False)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme", timeout_secs=5))
    rid = await _rid(engine)
    cap = float(engine.registry.current.value("small", "timeout_max_secs"))
    with pytest.raises(ConfigError, match="timeout_max_secs"):
        await engine.requests_admin.extend(rid, timeout_secs=cap + 1, actor="ops")
    with pytest.raises(ConfigError):
        await engine.requests_admin.extend(rid, timeout_secs=0, actor="ops")
    gone = await engine.requests_admin.extend("no-such-id", timeout_secs=9, actor="ops")
    assert gone.skipped == {"gone": 1}
    task.cancel()
    with pytest.raises(BaseException):  # noqa: B017 PT011  # cancelled or failed: either ends it
        await task


# G7 (RQ-1 to RQ-4, RQ-12): the five extend cases.


async def test_g7a_a_waiting_call_outlives_its_max_wait(make: Make) -> None:
    """Extended before any worker took it: the worker that takes it later lifts max_wait."""
    caller, _ = await make(worker=False)
    _short_margin(caller)
    task = asyncio.create_task(
        caller.call("small", CHAT, tenant_id="acme", timeout_secs=4, max_wait_secs=1)
    )
    rid = await _rid(caller)
    done = await caller.requests_admin.extend(rid, timeout_secs=12, actor="ops", dry_run=False)
    assert done.changed == 1
    await asyncio.sleep(1.5)  # past max_wait_secs, before the first deadline
    await make(caller=False)
    reply = await asyncio.wait_for(task, 15)
    assert reply.ok


async def test_g7b_a_running_attempt_is_not_cut_at_the_old_deadline(make: Make) -> None:
    """A 3 s call with timeout 2 extended to 8 while it runs: one provider call, no retry."""
    engine, fake = await make(MODEL, SLOW)
    _short_margin(engine)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme", timeout_secs=2))
    rid = await _rid(engine)
    await _running(engine, rid)
    done = await engine.requests_admin.extend(rid, timeout_secs=8, actor="ops", dry_run=False)
    assert done.changed == 1
    reply = await asyncio.wait_for(task, 15)
    assert reply.ok
    assert reply.meta.attempts == 1
    assert stats(fake, f"{SMALL}@r1")["calls"] == 1


async def test_g7c_a_stream_reads_the_extended_deadline(make: Make) -> None:
    """A stream's own deadline moves too, and its chunks live past the new one."""
    engine, fake = await make(MODEL, SLOW)
    _short_margin(engine)
    events: list[dict[str, object]] = []

    async def read() -> None:
        async for event in engine.stream(
            "small", CHAT, tenant_id="acme", timeout_secs=2, request_id="g7c"
        ):
            events.append(event)

    task = asyncio.create_task(read())
    await _running(engine, "g7c")
    done = await engine.requests_admin.extend("g7c", timeout_secs=8, actor="ops", dry_run=False)
    assert done.changed == 1
    await asyncio.wait_for(task, 15)
    assert events
    assert stats(fake, f"{SMALL}@r1")["calls"] == 1


async def test_g7d_an_extend_never_shortens(make: Make) -> None:
    """`extend(60)` on a 120 s call is `already`: nothing written, max_wait stays on."""
    engine, _ = await make(worker=False)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme", timeout_secs=120))
    rid = await _rid(engine)
    report = await engine.requests_admin.extend(rid, timeout_secs=60, actor="ops", dry_run=False)
    assert (report.changed, report.skipped) == (0, {"already": 1})
    assert "dl" not in await engine.requests.job(rid)
    assert await engine.requests.extend(rid, deadline_ms=1, margin_secs=5.0) == "already"
    task.cancel()
    with pytest.raises(BaseException):  # noqa: B017 PT011  # cancelled or failed: either ends it
        await task


async def test_g7e_an_extend_past_the_queue_retention_is_refused(make: Make) -> None:
    """Past `queue.retention_secs` the waiting message would be trimmed: refused."""
    engine, _ = await make({"small": {"queue": {"retention_secs": 60}}}, worker=False)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme", timeout_secs=30))
    rid = await _rid(engine)
    with pytest.raises(ConfigError, match="retention_secs"):
        await engine.requests_admin.extend(rid, timeout_secs=90, actor="ops")
    ok = await engine.requests_admin.extend(rid, timeout_secs=60, actor="ops")
    assert ok.matched == 1
    task.cancel()
    with pytest.raises(BaseException):  # noqa: B017 PT011  # cancelled or failed: either ends it
        await task


async def test_extend_needs_the_request_index(make: Make) -> None:
    engine, _ = await make(worker=False)
    admin = engine.requests_admin
    admin.settings = dataclasses.replace(admin.settings, request_index=False)
    with pytest.raises(AdminRefused, match="request index"):
        await admin.extend("any", timeout_secs=9, actor="ops")


async def test_a_re_registered_id_after_an_extend_runs_with_its_own_status(make: Make) -> None:
    """FA-2: extend an app id, end it, lose its job record, send it again: a new run shows.

    The final write set the job's TTL to the status TTL alone, shorter than the status the
    extend grew, so the job expired first. The re-registered id then kept the old final
    status: its own status writes were refused and admin reads took it for finished. Now the
    job lives as long as its status, and a registration with no job clears a final status.
    """
    engine, _ = await make(worker=False)
    admin = engine.requests_admin
    first = asyncio.create_task(
        engine.call("small", CHAT, tenant_id="acme", request_id="fa2", timeout_secs=5)
    )
    assert await _rid(engine) == "fa2"
    done = await admin.extend("fa2", timeout_secs=300, actor="ops", dry_run=False)
    assert done.changed == 1
    cancelled = await admin.cancel("small", actor="ops", dry_run=False)
    assert cancelled.changed == 1
    with pytest.raises(CallFailed, match="cancelled"):
        await asyncio.wait_for(first, 5)
    job_key, status_key = engine.keys.request("fa2", "job"), engine.keys.status("fa2")
    job_ttl, status_ttl = await engine.state.pttl(job_key), await engine.state.pttl(status_key)
    if job_ttl > 0 and status_ttl > 0:  # native expiry (swept keeps them in its index)
        assert job_ttl >= status_ttl - 1000
    await engine.state.delete(job_key)  # its expiry, or a lost record
    again = asyncio.create_task(
        engine.call("small", CHAT, tenant_id="acme", request_id="fa2", timeout_secs=30)
    )
    try:
        assert await _rid(engine) == "fa2"
        assert (await engine.requests.status("fa2"))["state"] == "waiting"
        page = await admin.list("small", tenant="acme")
        assert [i.request_id for i in page.items] == ["fa2"]
    finally:
        again.cancel()
        with contextlib.suppress(BaseException):
            await again
