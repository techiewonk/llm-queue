"""A smooth window whose bucket holds one call (`rpm x rate_burst` below 2), on Valkey and Redis.

Bug (0.2.0 scenario review): such a model never granted a call. The GCRA check summed
`tat + c x per - now - burst x per` on store milliseconds (13 digits), where `now + per` rounds
to 1/4096 ms: for a call that exactly fills the bucket the sum came out a few 1e-5 ms above
0, so an idle bucket answered "wait 0.0001 ms" for ever and every call ended `rate_limited` at
`max_wait_secs`. It hit about half of all limits (rpm 900 at rate_burst 0.001, rpm 9 to 19 at the
default 0.1), with or without a priority reserve.
"""

from __future__ import annotations

import asyncio
import collections

import pytest
import redis.asyncio
from engine_testkit import CHAT, Make
from test_admission import Candidate, rig  # type: ignore[import-not-found]  # sibling module

from llm_queue.engine.errors import CallFailed

pytestmark = pytest.mark.integration
ONE = [Candidate("aws-a/r1")]
# rpm 900: 60000 / 900 ms rounds up next to a 13-digit `now`; the normal share (720) too.
BUCKET_OF_ONE = {"rpm": 900, "rate_window": "smooth", "rate_burst": 0.001}


@pytest.mark.parametrize("reserve", [0, 0.2])
async def test_a_bucket_of_one_grants_at_its_rate(
    store: redis.asyncio.Redis, prefix: str, reserve: float
) -> None:
    """An idle bucket of one grants a call; the next waits one interval (67 ms), then fits."""
    r = await rig(store, prefix, model={**BUCKET_OF_ONE, "priority_reserve": reserve})
    assert (await r.ask("a", candidates=ONE)).kind == "granted"
    second = await r.ask("b", candidates=ONE)
    assert second.kind == "wait"
    assert 0 < second.wait_secs <= 0.1
    await asyncio.sleep(0.1)
    assert (await r.ask("b", candidates=ONE)).kind == "granted"


async def test_priority_still_goes_first_on_a_bucket_of_one(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """A waiting priority call keeps the one unit: the normal call behind it waits its turn."""
    r = await rig(store, prefix, model={**BUCKET_OF_ONE, "priority_reserve": 0.2})
    assert (await r.ask("n1", candidates=ONE)).kind == "granted"  # empties the bucket
    assert (await r.ask("p1", cls="p", candidates=ONE)).kind == "wait"
    assert (await r.ask("n2", candidates=ONE)).kind == "wait"
    await asyncio.sleep(0.1)  # the unit is back
    held = await r.ask("n2", candidates=ONE)
    assert (held.kind, held.reason) == ("wait", "not_head")
    assert (await r.ask("p1", cls="p", candidates=ONE)).kind == "granted"
    await asyncio.sleep(0.1)
    assert (await r.ask("n2", candidates=ONE)).kind == "granted"


async def test_waiting_calls_on_a_bucket_of_one_are_all_served(make: Make) -> None:
    """Six calls at once on the queue path: all served at 15 a second, none `rate_limited`."""
    engine, _ = await make({"small": {**BUCKET_OF_ONE, "max_wait_secs": 4}})
    out: collections.Counter[str] = collections.Counter()

    async def one() -> None:
        try:
            await engine.call("small", CHAT, timeout_secs=8)
            out["ok"] += 1
        except CallFailed as exc:
            out[exc.kind] += 1

    await asyncio.gather(*(one() for _ in range(6)))
    assert out == {"ok": 6}
