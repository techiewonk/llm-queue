"""0.1.0b2 admission script fixes (RESCAN A1-A10, P2, S11), each pinned at the script level.

Refund caps and the grant's own budgets, stale latency, dead-owner probe, expired waiters and
fallback, breaker close, shared throttle windows, lost grants, claim owner and claim lifetime.
"""

from __future__ import annotations

import asyncio
import copy
from typing import Any

import pytest
import redis.asyncio
from test_admission import (  # type: ignore[import-not-found]  # sibling test module
    CONFIRMED,
    Candidate,
    rig,
)

from llm_queue.engine.admission import owner_token

pytestmark = pytest.mark.integration
R1 = [Candidate("aws-a/r1")]


async def _now_ms(store: redis.asyncio.Redis) -> int:
    secs, micros = await store.time()
    return int(secs * 1000 + micros // 1000)


def _n(value: Any) -> int:
    return int(value or 0)


def _m(prefix: str, name: str) -> str:
    return f"{prefix}:{{m:m}}:{name}"


async def test_refund_is_capped_at_the_charge(store: redis.asyncio.Redis, prefix: str) -> None:
    """A4: a refund larger than the charge drove `seen_tokens` (and budgets) below zero."""
    r = await rig(store, prefix, model={"tpm": 100_000})
    assert (await r.ask("a", candidates=R1, cost_t=100)).kind == "granted"
    owner = owner_token("a", "w1", 1)
    assert await r.admission.finish("m", owner, "a", refund_r=5, refund_t=500, outcome="ok")
    seen = await store.hget(_m(prefix, "rate:aws-a/r1"), "seen_tokens")
    calls = await store.hget(_m(prefix, "rate:aws-a/r1"), "seen_calls")
    assert _n(seen) == 0
    assert _n(calls) == 0


async def test_settle_gives_back_to_the_grants_budgets(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """A7: a reload between grant and refund (window fixed -> smooth) left the charge in place."""
    r = await rig(store, prefix, model={"tpm": 100_000})
    assert (await r.ask("a", candidates=R1, cost_t=100)).kind == "granted"
    doc = copy.deepcopy(dict(r.store.current.registry.raw))
    doc["models"]["m"]["rate_window"] = "smooth"
    doc["registry_version"] = int(doc["registry_version"]) + 1
    await r.store.publish(doc, actor="test")
    owner = owner_token("a", "w1", 1)
    assert await r.admission.finish("m", owner, "a", refund_r=0, refund_t=100, outcome="ok")
    assert _n(await store.hget(_m(prefix, "rate:aws-a/r1"), "tpm_n")) == 0


async def test_stale_latency_no_longer_marks_slow(store: redis.asyncio.Redis, prefix: str) -> None:
    """A5: a deployment marked slow stayed last for good (its average never aged)."""
    r = await rig(store, prefix)
    now = await _now_ms(store)
    await store.hset(
        _m(prefix, "health"),
        mapping={
            "aws-a/r1|lat": 60_000,
            "aws-a/r1|n": 50,
            "aws-a/r1|lat_at": now - 600_000,  # ten minutes old
            "aws-a/r2|lat": 1000,
            "aws-a/r2|n": 50,
            "aws-a/r2|lat_at": now,
            "aws-a/r3|lat": 1000,
            "aws-a/r3|n": 50,
            "aws-a/r3|lat_at": now,
        },
    )
    all3 = [*CONFIRMED, Candidate("aws-a/r3", tier="fallback")]
    decision = await r.ask("a", candidates=all3)
    assert decision.deployment == "aws-a/r1"
    await store.hset(_m(prefix, "health"), "aws-a/r1|lat_at", now)  # fresh: slow again
    decision = await r.ask("b", candidates=all3)
    assert decision.deployment == "aws-a/r2"


async def test_dead_owner_takeover_frees_the_probe(store: redis.asyncio.Redis, prefix: str) -> None:
    """A8: taking over a dead owner's entry left its breaker probe set: callers waited 60 s."""
    r = await rig(store, prefix, lease_secs=0.4)
    await store.hset(_m(prefix, "cb:aws-a/r1"), mapping={"state": "open", "until": 0})
    assert (await r.ask("a", candidates=R1)).kind == "granted"
    assert (
        await store.hget(_m(prefix, "cb:aws-a/r1"), "probe") == owner_token("a", "w1", 1).encode()
    )
    await asyncio.sleep(0.5)  # the owner's heartbeat is now older than a lease
    decision = await r.ask("a", candidates=R1, worker="w2")
    assert decision.kind == "granted", decision


async def test_expired_waiters_do_not_block_fallback(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """A9: expired waiters past the first 50 counted as waiters and refused every fallback."""
    r = await rig(store, prefix)
    wait_key = f"{prefix}:{{m:other}}:wait:n"
    info_key = f"{prefix}:{{m:other}}:waitinfo"
    for i in range(60):
        await store.zadd(wait_key, {f"old-{i}": i})
        await store.hset(info_key, f"old-{i}", '{"exp": 0, "deps": ["aws-a/r1"]}')
    decision = await r.ask(
        "f", alias="other", candidates=R1, foreign_from="m", claim=False, pos=1e15
    )
    assert decision.kind == "granted", decision


async def test_only_the_probe_closes_an_open_breaker(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """A10: an ok from a call granted before the breaker opened closed it early."""
    r = await rig(store, prefix)
    assert (await r.ask("early", candidates=R1)).kind == "granted"
    now = await _now_ms(store)
    cb = _m(prefix, "cb:aws-a/r1")
    await store.hset(cb, mapping={"state": "open", "until": now + 60_000, "probe": "x|y|1"})
    owner = owner_token("early", "w1", 1)
    assert await r.admission.finish("m", owner, "early", refund_r=0, refund_t=0, outcome="ok")
    assert await store.hget(cb, "state") == b"open"


async def test_throttle_keeps_a_deployment_out_for_every_pod(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """P2: a throttled deployment was retried at once, ignoring Retry-After, by every pod."""
    r = await rig(store, prefix)
    assert (await r.ask("a", candidates=R1)).kind == "granted"
    owner = owner_token("a", "w1", 1)
    await r.admission.finish(
        "m", owner, "a", refund_r=1, refund_t=0, outcome="throttle", retry_after_secs=5
    )
    alone = await r.ask("b", candidates=R1, worker="w2")
    assert alone.kind == "wait"
    assert alone.reason == "throttled"
    assert 3.0 <= alone.wait_secs <= 5.1  # FT-21: time passes between throttle and ask
    other = await r.ask("c", candidates=CONFIRMED, worker="w3")
    assert (other.kind, other.deployment) == ("granted", "aws-a/r2")
    cb = _m(prefix, "cb:aws-a/r2")
    owner = owner_token("c", "w3", 1)
    await r.admission.finish("m", owner, "c", refund_r=1, refund_t=0, outcome="throttle")
    first = _n(await store.hget(cb, "thr_until")) - await _now_ms(store)
    assert 900 <= first <= 2100  # no Retry-After: a jittered 1-2 s backoff
    assert _n(await store.hget(cb, "thr_n")) == 1


@pytest.mark.parametrize(("refund_r", "next_kind"), [(0, "wait"), (1, "granted")])
async def test_a_throttle_finish_keeps_the_request_in_the_fixed_minute(
    store: redis.asyncio.Redis, prefix: str, refund_r: int, next_kind: str
) -> None:
    """BL1: the worker now FINISHes a 429 with `refund_r=0` (the provider counted it).

    The script applies the refund it is given: kept, the minute stays spent and the next call
    waits for the next minute; given back (the old rule), the call was sent again at once.
    """
    r = await rig(store, prefix, model={"rpm": 1})
    assert (await r.ask("a", candidates=R1)).kind == "granted"
    owner = owner_token("a", "w1", 1)
    assert await r.admission.finish(
        "m", owner, "a", refund_r=refund_r, refund_t=0, outcome="throttle"
    )
    await store.hdel(_m(prefix, "cb:aws-a/r1"), "thr_until")  # the budget alone decides
    after = await r.ask("b", candidates=R1, worker="w2")
    assert after.kind == next_kind, after


async def test_success_ends_the_throttle_streak(store: redis.asyncio.Redis, prefix: str) -> None:
    """P2: the backoff grows per throttle in a row and a success starts it again."""
    r = await rig(store, prefix)
    cb = _m(prefix, "cb:aws-a/r1")
    await store.hset(cb, mapping={"thr_n": 3})
    assert (await r.ask("a", candidates=R1)).kind == "granted"
    await r.admission.finish(
        "m", owner_token("a", "w1", 1), "a", refund_r=0, refund_t=0, outcome="ok"
    )
    assert _n(await store.hget(cb, "thr_n")) == 0


async def test_renew_reports_lost_grants_and_refreshes_claims(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """A2/A6: RENEW said only how many it renewed; A1: held calls between grants aged out."""
    r = await rig(store, prefix)
    assert (await r.ask("a", candidates=R1)).kind == "granted"
    kept = ("m", owner_token("a", "w1", 1), "a")
    gone = ("m", owner_token("b", "w1", 1), "b")
    job = f"{prefix}:{{r:c}}:job"
    await store.hset(job, mapping={"state": "running", "owner": "w1@t", "hb": 0})
    renewed, lost = await r.admission.renew_all([kept, gone], jobs=[("c", "w1@t")])
    assert renewed == 1
    assert lost == [gone[1]]
    assert _n(await store.hget(job, "hb")) > 0


async def test_claim_owner_is_the_delivery(store: redis.asyncio.Redis, prefix: str) -> None:
    """A1: a second delivery of a running call to the same pod ran it again between retries."""
    r = await rig(store, prefix)
    assert (await r.ask("a", candidates=R1, tag="m1")).kind == "granted"
    job = f"{prefix}:{{r:a}}:job"
    assert await store.hget(job, "owner") == b"w1@m1"
    assert await store.pttl(job) > 0  # S11: a claim made by the worker has a lifetime
    owner = owner_token("a", "w1@m1", 1)
    await r.admission.finish("m", owner, "a", refund_r=0, refund_t=0, outcome="fail")
    again = await r.ask("a", candidates=R1, tag="m2")
    assert again.kind == "duplicate"


async def test_reregister_skips_finished_calls(store: redis.asyncio.Redis, prefix: str) -> None:
    """A2: re-registering a call whose result is stored brought back a slot nobody frees."""
    r = await rig(store, prefix)
    await store.set(f"{prefix}:{{r:done}}:res", b"{}")
    item = {
        "alias": "m",
        "owner": owner_token("done", "w1", 1),
        "request_id": "done",
        "dep": "aws-a/r1",
        "cls": "n",
        "cost_r": 1,
        "cost_t": 0,
    }
    assert await r.admission.reregister([item]) == 0
    assert not await store.exists(_m(prefix, "inprog:done"))


async def test_reregister_skips_grants_this_process_settled(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """A heartbeat that found a just-settled grant lost put its slot back until the lease ended.

    FINISH (or UNDO) ran between the heartbeat's snapshot of held grants and its RENEW, so the
    RENEW reported the owner lost and REREGISTER brought back a lease nobody would free (a
    `grants_reregistered` in the logs; scenarios R4, P1 ... ended with one lease left).
    """
    r = await rig(store, prefix)
    lease = _m(prefix, "lease:aws-a/r1:n")
    for rid, settle in (("f", "finish"), ("u", "undo")):
        assert (await r.ask(rid, candidates=R1)).kind == "granted"
        owner = owner_token(rid, "w1", 1)
        if settle == "finish":
            await r.admission.finish("m", owner, rid, refund_r=0, refund_t=0, outcome="fail")
        else:
            await r.admission.undo("m", owner, rid)
        assert await r.admission.renew_all([("m", owner, rid)]) == (0, [])  # settled, not lost
        item = {
            "alias": "m",
            "owner": owner,
            "request_id": rid,
            "dep": "aws-a/r1",
            "cls": "n",
            "cost_r": 1,
            "cost_t": 0,
        }
        assert await r.admission.reregister([item]) == 0
        assert await store.zcard(lease) == 0
        assert not await store.exists(_m(prefix, f"inprog:{rid}"))
