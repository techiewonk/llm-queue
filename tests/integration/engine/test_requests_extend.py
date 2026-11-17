"""`RequestsAdmin.extend` (0.2.0 T2, owner D6): one new deadline in all three places.

The records (job `dl`, TTLs, the index score), the worker (a notice on the cancel channel, or
`dl` read when it reaches the deadline it knows) and the waiting caller (its timer moves) all
see it. Without the extend each call below would end `deadline_exceeded` / `expired`.
"""

from __future__ import annotations

import asyncio
import dataclasses

import pytest
from engine_testkit import CHAT, Make

from llm_queue.engine.requests import extend_notice
from llm_queue.engine.runtime import Engine
from llm_queue.errors import ConfigError

pytestmark = pytest.mark.integration

SLOW = {"defaults": {"latency_ms": {"p50": 3000, "p95": 3000}}}
MODEL = {"small": {"timeout_secs": 30, "attempt_timeout_secs": 20}}


def _short_margin(engine: Engine) -> None:
    """The caller waits 0.5 s past the worker's deadline (5 s by default): a quick test."""
    engine.settings = dataclasses.replace(engine.settings, caller_margin_secs=0.5)


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
