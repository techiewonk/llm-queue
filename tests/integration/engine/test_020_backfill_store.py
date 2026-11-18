"""Size-aware backfill (plan F1, owner Q3) in PICK_TAKE, on a store with an injected clock.

A small call may pass an older waiter of its class only when that waiter still fits at its
predicted grant afterwards: never past a waiting priority call, never over a budget, and with
the flag off exactly as before.
"""

from __future__ import annotations

from typing import Any

import orjson
import pytest
import redis.asyncio
from test_admission import Candidate, rig  # type: ignore[import-not-found]  # sibling module

from llm_queue.engine.admission import Admission, Ask, Class
from llm_queue.engine.keys import Keys

pytestmark = pytest.mark.integration
MINUTE = 60_000
START_MS = 28_333_334 * MINUTE + 10_000  # 10 s into a clock minute
R1 = [Candidate("aws-a/r1")]


class Clocked:
    """Asks on deployment r1 of model `m` at a chosen store time."""

    def __init__(self, admission: Admission, changes: int, clock: list[int]) -> None:
        self.admission = admission
        self.changes = changes
        self.clock = clock

    async def ask(
        self, rid: str, pos: float, tokens: int, *, cls: Class = "n", backfill: bool = False
    ) -> Any:
        ask = Ask("m", rid, "w1", 1, cls, pos, R1, cost_t=tokens, backfill=backfill)
        return await self.admission.pick_take(ask, self.changes)


async def _clocked(store: redis.asyncio.Redis, prefix: str, model: dict[str, Any]) -> Clocked:
    r = await rig(store, prefix, model={"rate_limits": "tpm", **model})
    clock = [START_MS]
    admission = Admission(store, Keys(prefix), clock_ms=lambda: clock[0])
    return Clocked(admission, r.changes, clock)


async def _waiter(store: redis.asyncio.Redis, prefix: str, rid: str) -> dict[str, Any]:
    raw = await store.hget(f"{prefix}:{{m:m}}:waitinfo", rid)
    assert raw is not None
    loaded: dict[str, Any] = orjson.loads(raw)
    return loaded


async def _used(store: redis.asyncio.Redis, prefix: str) -> int:
    """Tokens counted in r1's fixed window this minute."""
    return int(await store.hget(f"{prefix}:{{m:m}}:rate:aws-a/r1", "tpm_n") or 0)


@pytest.mark.parametrize("backfill", [False, True])
async def test_fixed_window_rest_goes_to_small_calls_only_with_backfill(
    store: redis.asyncio.Redis, prefix: str, backfill: bool
) -> None:
    q = await _clocked(store, prefix, {"tpm": 1000, "rate_window": "fixed"})
    assert (await q.ask("a", 1, 800, backfill=backfill)).kind == "granted"
    big = await q.ask("big", 2, 800, backfill=backfill)  # cannot fit before the next minute
    assert (big.kind, big.reason) == ("wait", "tpm")
    small = await q.ask("s1", 3, 150, backfill=backfill)
    if backfill:
        assert small.kind == "granted"
        assert small.extra.get("bf") is True
        assert "at" in await _waiter(store, prefix, "big")
        # The rest of the minute only: a second small call over the budget still waits.
        assert (await q.ask("s2", 4, 100, backfill=True)).kind == "wait"
        assert await _used(store, prefix) == 950  # never above tpm 1000
    else:
        assert (small.kind, small.reason) == ("wait", "not_head")
        assert "at" not in await _waiter(store, prefix, "big")  # the entry as before
        assert await _used(store, prefix) == 800
    # The big call is granted at its predicted time either way: backfill did not delay it.
    q.clock[0] = (START_MS // MINUTE + 1) * MINUTE
    assert (await q.ask("big", 2, 800, backfill=backfill)).kind == "granted"
    assert await _used(store, prefix) == 800


async def test_smooth_clock_small_call_never_delays_a_waiter_on_the_same_clock(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """Smooth: any spend on the waiter's own clock moves its grant, so nothing passes."""
    q = await _clocked(store, prefix, {"tpm": 6000, "rate_window": "smooth", "rate_burst": 0.5})
    assert (await q.ask("a", 1, 2500, backfill=True)).kind == "granted"
    big = await q.ask("big", 2, 2000, backfill=True)
    assert big.kind == "wait"
    small = await q.ask("s1", 3, 100, backfill=True)
    assert (small.kind, small.reason) == ("wait", "not_head")
    q.clock[0] += int(big.wait_secs * 1000)
    assert (await q.ask("big", 2, 2000, backfill=True)).kind == "granted"


async def test_never_passes_a_waiting_priority_call(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """A priority waiter's reservation stays strict for normal calls: no backfill past it."""
    q = await _clocked(store, prefix, {"tpm": 1000, "rate_window": "fixed"})
    assert (await q.ask("a", 1, 800, backfill=True)).kind == "granted"
    prio = await q.ask("p", 2, 800, cls="p", backfill=True)
    assert prio.kind == "wait"
    small = await q.ask("s1", 3, 150, backfill=True)
    assert small.kind == "wait"
    assert await _used(store, prefix) == 800
    q.clock[0] = (START_MS // MINUTE + 1) * MINUTE
    assert (await q.ask("p", 2, 800, cls="p", backfill=True)).kind == "granted"


async def test_a_priority_arrival_stops_backfill_at_the_next_grant(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    q = await _clocked(store, prefix, {"tpm": 2000, "rate_window": "fixed"})
    assert (await q.ask("a", 1, 1500, backfill=True)).kind == "granted"
    assert (await q.ask("big", 2, 800, backfill=True)).kind == "wait"
    assert (await q.ask("s1", 3, 100, backfill=True)).kind == "granted"  # backfilled
    assert (await q.ask("p", 4, 800, cls="p", backfill=True)).kind == "wait"
    assert (await q.ask("s2", 5, 100, backfill=True)).kind == "wait"  # not past priority


async def test_small_calls_take_the_rest_of_the_minute_up_to_the_budget(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    q = await _clocked(store, prefix, {"tpm": 1000, "rate_window": "fixed"})
    assert (await q.ask("a", 1, 700, backfill=True)).kind == "granted"
    assert (await q.ask("big", 2, 900, backfill=True)).kind == "wait"
    kinds = [(await q.ask(f"s{i}", 3 + i, 50, backfill=True)).kind for i in range(8)]
    assert kinds == ["granted"] * 6 + ["wait"] * 2
    assert await _used(store, prefix) == 1000
