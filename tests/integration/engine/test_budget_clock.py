"""Property tests of the admission scripts' RPM windows against a Python model (RESCAN T7).

The scripts read the store clock; `Admission(clock_ms=...)` injects one (tests only), so a
generated sequence of asks at chosen times can be compared grant by grant with the model of
the fixed window and of the smooth (GCRA) window.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import redis.asyncio
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from test_admission import Candidate, rig  # type: ignore[import-not-found]  # sibling module

from llm_queue.engine.admission import Admission, Ask, owner_token
from llm_queue.engine.keys import Keys

pytestmark = pytest.mark.integration
START_MS = 1_700_000_000_000  # 13 digits: Lua's tostring keeps every one of them
R1 = [Candidate("aws-a/r1")]
# Limits that divide a minute: the smooth window's step (60000 / rpm ms) is then a whole
# number, so the model's arithmetic is exact (the script stores the state as text).
LIMITS = st.sampled_from([1, 2, 3, 4, 5, 6, 10, 12, 20, 30, 60])
STEPS = st.lists(st.integers(min_value=0, max_value=20_000), min_size=1, max_size=40)
PROPS = settings(
    database=None,
    deadline=None,
    max_examples=40,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)


def _fixed_model(rpm: int, times: list[int]) -> list[bool]:
    """At most `rpm` grants per clock minute."""
    out: list[bool] = []
    counts: dict[int, int] = {}
    for now in times:
        minute = now // 60_000
        ok = counts.get(minute, 0) < rpm
        if ok:
            counts[minute] = counts.get(minute, 0) + 1
        out.append(ok)
    return out


def _smooth_model(rpm: int, burst_pct: float, times: list[int]) -> list[bool]:
    """GCRA: one grant per 60000 / rpm ms, with up to `burst` grants at once."""
    per = 60_000 // rpm
    burst = max(1, int(rpm * burst_pct))
    out, tat = [], 0
    for now in times:
        start = max(tat, now)
        ok = start + per - now - burst * per <= 0
        if ok:
            tat = start + per
        out.append(ok)
    return out


async def _run(store: redis.asyncio.Redis, model: dict[str, Any], steps: list[int]) -> list[bool]:
    prefix = f"llmqtest:clock:{uuid.uuid4().hex[:12]}"
    try:
        r = await rig(store, prefix, model={"rpm": 10_000, **model})
        clock = [START_MS]
        admission = Admission(store, Keys(prefix), clock_ms=lambda: clock[0])
        out = []
        n = 0  # the call asking now: a refused one asks again (it is the waiter at the head)
        for step in steps:
            clock[0] += step
            rid = f"c{n}"
            ask = Ask("m", rid, "w1", 1, "n", float(n + 1), R1)
            decision = await admission.pick_take(ask, r.changes)
            granted = decision.kind == "granted"
            out.append(granted)
            if granted:  # free the slot; the request stays counted (refund_r 0)
                await admission.finish(
                    "m", owner_token(rid, "w1", 1), rid, refund_r=0, refund_t=0, outcome="ok"
                )
                n += 1
        return out
    finally:
        keys = [k async for k in store.scan_iter(match=f"{prefix}:*", count=500)]
        if keys:
            await store.delete(*keys)


@PROPS
@given(rpm=LIMITS, steps=STEPS)
async def test_fixed_window_matches_the_model(
    store: redis.asyncio.Redis, rpm: int, steps: list[int]
) -> None:
    got = await _run(store, {"rpm": rpm, "rate_window": "fixed"}, steps)
    times = [START_MS + sum(steps[: i + 1]) for i in range(len(steps))]
    assert got == _fixed_model(rpm, times)


@PROPS
@given(rpm=LIMITS, burst=st.sampled_from([0.0, 0.1, 0.5, 1.0]), steps=STEPS)
async def test_smooth_window_matches_the_model(
    store: redis.asyncio.Redis, rpm: int, burst: float, steps: list[int]
) -> None:
    model = {"rpm": rpm, "rate_window": "smooth", "rate_burst": burst}
    got = await _run(store, model, steps)
    times = [START_MS + sum(steps[: i + 1]) for i in range(len(steps))]
    assert got == _smooth_model(rpm, burst, times)
