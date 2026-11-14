"""200 callers at once on one deployment (RESCAN T7): budgets hold under contention.

Grants never exceed the RPM budget or the concurrency slots, the store's counters equal what
the provider saw, every slot is given back, and every caller gets an answer.
"""

from __future__ import annotations

import asyncio

import pytest
from engine_testkit import CHAT, SMALL, Make, away_from_minute_end, stats

from llm_queue.engine.envelope import Reply
from llm_queue.engine.errors import CallFailed

pytestmark = pytest.mark.integration

CALLERS = 200
RPM = 150
SLOTS = 8


async def test_200_callers_one_deployment(make: Make) -> None:
    """150 grants for 200 callers, never more than 8 in flight, counters equal provider calls."""
    engine, fake = await make(
        {
            "small": {
                "rpm": RPM,
                "max_concurrency": SLOTS,
                "max_wait_secs": 12,  # slots, not budget, must never refuse a call (slow CI)
                "priority_reserve": 0,  # the whole budget for normal calls
            }
        },
        # fakellm throttles a 9th call in flight, so an over-grant shows up as a throttle.
        {"models": {f"{SMALL}@r1": {"max_concurrency": SLOTS, "latency_ms": {"p50": 40}}}},
    )
    await away_from_minute_end(engine.clock.now, margin_secs=25)  # no reset mid-test
    async with asyncio.timeout(60):  # all finish: none hangs
        results = await asyncio.gather(
            *[engine.call("small", CHAT, timeout_secs=20) for _ in range(CALLERS)],
            return_exceptions=True,
        )
    ok = [r for r in results if isinstance(r, Reply) and r.ok]
    failed = [r for r in results if isinstance(r, CallFailed)]
    assert len(ok) + len(failed) == CALLERS  # nothing else (no raw errors)
    assert {f.kind for f in failed} <= {"rate_limited"}
    assert len(ok) == RPM  # the whole budget is used, and not one grant more
    provider = stats(fake, f"{SMALL}@r1")
    assert provider["calls"] == provider["ok"] == RPM
    assert provider["throttled"] == 0
    assert provider["max_concurrent_seen"] <= SLOTS
    status = await engine.status("small")
    dep = status["deployments"][0]
    assert dep["rpm"]["used"] == RPM  # leases add up: the store counted each call once
    assert dep["concurrency"]["in_use"] == 0  # every slot was given back
