"""Per-tenant fairness in PICK_TAKE, FINISH and RENEW on Valkey and Redis (owner Q28, Q29, Q50).

Off writes nothing new; `quota` refuses a tenant past its own rpm / tpm with a wait; `share`
holds back a tenant over its weighted share only while a tenant within its share waits (and
lends otherwise); `fair_queue` orders waiters by round, not by queue position. Every key the
feature writes has an expiry. Plus the engine end to end: a quota refusal is `rate_limited`.
"""

from __future__ import annotations

import asyncio
import copy
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, Make, away_from_minute_end

from llm_queue.engine.admission import Admission, Ask, Candidate, Decision, owner_token
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.fairness import Fairness
from llm_queue.engine.keys import Keys
from llm_queue.registry.store import RegistryStore

pytestmark = pytest.mark.integration

ONE = [Candidate("aws-a/r1")]
BASE: dict[str, Any] = {
    "registry_version": 1,
    "accounts": {"aws-a": {"kind": "aws"}},
    "models": {
        "m": {
            "type": "chat",
            "provider": "bedrock",
            "model_id": "x.model-1:0",
            "rate_window": "fixed",
            "priority_reserve": 0,
            "normal_min_share": 0,
            "deployments": {"aws-a": {"confirmed": ["r1", "r2"], "fallback": ["r3"]}},
        }
    },
}


class Rig:
    """A published registry and an `Admission` over one prefix (as in test_admission)."""

    def __init__(self, admission: Admission, changes: int) -> None:
        self.admission = admission
        self.changes = changes
        self.positions: dict[str, int] = {}  # queue position by request id, first ask first

    async def ask(
        self, rid: str, *, candidates: list[Candidate] | None = None, **kw: Any
    ) -> Decision:
        ask = Ask(
            alias="m",
            request_id=rid,
            worker="w1",
            attempt=1,
            cls="n",
            position=self._position(rid),
            candidates=candidates or [Candidate("aws-a/r1"), Candidate("aws-a/r2")],
            **kw,
        )
        return await self.admission.pick_take(ask, self.changes)

    def _position(self, rid: str) -> float:
        return float(self.positions.setdefault(rid, len(self.positions) + 1))

    async def done(self, rid: str, refund_t: int = 0) -> bool:
        owner = owner_token(rid, "w1", 1)
        return await self.admission.finish(
            "m", owner, rid, refund_r=0, refund_t=refund_t, outcome="ok"
        )


async def rig(
    client: redis.asyncio.Redis,
    prefix: str,
    *,
    model: dict[str, Any] | None = None,
    lease_secs: float = 60,
) -> Rig:
    doc = copy.deepcopy(BASE)
    doc["models"]["m"].update(model or {})
    store = RegistryStore(client, prefix, config=doc)
    await store.boot()
    await away_from_minute_end(margin_secs=5)  # fixed-window counts must not cross a reset
    return Rig(Admission(client, Keys(prefix), lease_secs=lease_secs), store.changes)


def share(tenant: str, weight: float = 1.0) -> Fairness:
    return Fairness("share", tenant, weight=weight)


def quota(tenant: str, rpm: int | None = None, tpm: int | None = None) -> Fairness:
    return Fairness("quota", tenant, rpm=rpm, tpm=tpm, window="fixed")


async def tf_keys(client: redis.asyncio.Redis, prefix: str) -> list[bytes]:
    return [k async for k in client.scan_iter(match=f"{prefix}:*tf:*", count=500)]


async def test_off_writes_nothing_new(store: redis.asyncio.Redis, prefix: str) -> None:
    """Fairness off: no `tf:` key, no `tf_` field on the grant, and the same decisions."""
    r = await rig(store, prefix, model={"rpm": 2})
    kinds = [(await r.ask(f"o{i}", candidates=ONE)).kind for i in range(3)]
    assert kinds == ["granted", "granted", "wait"]
    assert await tf_keys(store, prefix) == []
    entry = await store.hgetall(r.admission.keys.model("m", "inprog:o0"))
    assert not [f for f in entry if str(f).startswith(("tf_", "b'tf_"))]
    assert await r.done("o0")


async def test_quota_rpm_per_tenant(store: redis.asyncio.Redis, prefix: str) -> None:
    """A tenant past its own rpm is refused with a wait; another tenant is not."""
    r = await rig(store, prefix, model={"rpm": 100})
    a = [(await r.ask(f"a{i}", candidates=ONE, fairness=quota("ha", rpm=2))) for i in range(3)]
    assert [d.kind for d in a] == ["granted", "granted", "tenant_limited"]
    assert a[2].reason == "quota"
    assert 0 < a[2].wait_secs <= 60
    b = await r.ask("b0", candidates=ONE, fairness=quota("hb", rpm=2))
    assert b.kind == "granted"


async def test_quota_tpm_refund(store: redis.asyncio.Redis, prefix: str) -> None:
    """FINISH gives back the unused tokens to the tenant's own clock too."""
    r = await rig(store, prefix, model={"tpm": 100_000, "default_output_tokens": 50})
    first = await r.ask("t1", candidates=ONE, cost_t=80, fairness=quota("ht", tpm=100))
    assert first.kind == "granted"
    refused = await r.ask("t2", candidates=ONE, cost_t=80, fairness=quota("ht", tpm=100))
    assert refused.kind == "tenant_limited"
    assert await r.done("t1", refund_t=70)
    again = await r.ask("t3", candidates=ONE, cost_t=80, fairness=quota("ht", tpm=100))
    assert again.kind == "granted"


async def test_share_slots_by_weight_and_lending(store: redis.asyncio.Redis, prefix: str) -> None:
    """Slots split 3:1; the heavy tenant borrows while nobody within its share waits."""
    r = await rig(store, prefix, model={"max_concurrency": 2})  # 2 confirmed: 4 slots
    a, b = share("ha", 3.0), share("hb", 1.0)
    # Alone, A's share is the whole model: it takes all four slots.
    grants = [await r.ask(f"a{i}", fairness=a) for i in range(4)]
    assert [g.kind for g in grants] == ["granted"] * 4
    # B is within its share (1 of 4 slots) but the model is full: it waits, and is "needed".
    waiting = await r.ask("b0", fairness=b)
    assert (waiting.kind, waiting.reason) == ("wait", "slot")
    # One of A's calls ends: A holds 3, its share (3 of 4), so it may not take the free slot.
    assert await r.done("a0")
    held_back = await r.ask("a4", fairness=a)
    assert (held_back.kind, held_back.reason) == ("wait", "tenant")
    assert (await r.ask("b0", fairness=b)).kind == "granted"
    # Another of A's ends: below its share again, A is granted.
    assert await r.done("a1")
    assert (await r.ask("a4", fairness=a)).kind == "granted"


async def test_share_rpm_borrow_then_yield(store: redis.asyncio.Redis, prefix: str) -> None:
    """Over its rpm share a tenant borrows the idle budget, and yields to a waiting tenant."""
    r = await rig(store, prefix, model={"rpm": 6})  # fixed window, 2 confirmed: 12 a minute
    a, b = share("ha"), share("hb")
    kinds = [(await r.ask(f"a{i}", fairness=a)).kind for i in range(12)]
    assert kinds == ["granted"] * 12  # its share clock ran over after the first: it borrowed
    waiting = await r.ask("b0", fairness=b)
    assert waiting.kind == "wait"
    assert waiting.reason in ("rpm", "not_head")
    held_back = await r.ask("a12", fairness=a)
    assert (held_back.kind, held_back.reason) == ("wait", "tenant")
    status = await r_status(store, prefix)
    assert status == (2, 1)  # two active tenants, one waiting within its share


async def r_status(client: redis.asyncio.Redis, prefix: str) -> tuple[int, int]:
    base = f"{prefix}:{{m:m}}:"
    return int(await client.zcard(base + "tf:act")), int(await client.zcard(base + "tf:need"))


async def test_fair_queue_round_robin(store: redis.asyncio.Redis, prefix: str) -> None:
    """A newcomer's call goes ahead of a busy tenant's later waiters (FIFO would not)."""
    r = await rig(store, prefix, model={"max_concurrency": 1})
    fq = {t: Fairness("fair_queue", t) for t in ("hx", "ha", "hb")}
    assert (await r.ask("x0", candidates=ONE, fairness=fq["hx"])).kind == "granted"
    rounds: dict[str, float] = {}
    for rid, tenant in (("a1", "ha"), ("a2", "ha"), ("a3", "ha"), ("b1", "hb")):
        d = await r.ask(rid, candidates=ONE, fairness=fq[tenant])
        assert d.kind == "wait"
        rounds[rid] = float(d.extra["fr"])
    assert rounds == {"a1": 2.0, "a2": 3.0, "a3": 4.0, "b1": 2.0}
    assert await r.done("x0")

    def again(rid: str, tenant: str) -> Fairness:
        return Fairness("fair_queue", tenant, round=rounds[rid])

    # a3 (round 4) sees three waiters ahead of it; b1 (round 2) sees none and takes the slot.
    late = await r.ask("a3", candidates=ONE, fairness=again("a3", "ha"))
    assert late.kind == "wait"
    assert (await r.ask("b1", candidates=ONE, fairness=again("b1", "hb"))).kind == "granted"


async def test_tenant_slot_renewed_and_freed(store: redis.asyncio.Redis, prefix: str) -> None:
    """RENEW keeps the tenant's slot as long as the grant; FINISH removes it."""
    r = await rig(store, prefix, lease_secs=5)
    assert (await r.ask("s0", fairness=share("hs"))).kind == "granted"
    slots = f"{prefix}:{{m:m}}:tf:s:hs"
    before = float(await store.zscore(slots, owner_token("s0", "w1", 1)) or 0)
    await asyncio.sleep(0.05)
    assert await r.admission.renew([("m", owner_token("s0", "w1", 1), "s0")]) == 1
    assert float(await store.zscore(slots, owner_token("s0", "w1", 1)) or 0) > before
    assert await r.done("s0")
    assert await store.zcard(slots) == 0


@pytest.mark.parametrize("mode", ["share", "quota", "fair_queue"])
async def test_every_key_expires(store: redis.asyncio.Redis, prefix: str, mode: str) -> None:
    """Every key the feature writes has an expiry."""
    r = await rig(store, prefix, model={"rpm": 1, "max_concurrency": 1})
    fair = Fairness(mode, "h0", rpm=5 if mode == "quota" else None)  # type: ignore[arg-type]
    for i in range(3):
        await r.ask(f"k{i}", candidates=ONE, fairness=fair)
    keys = await tf_keys(store, prefix)
    assert keys
    for key in keys:
        assert await store.pttl(key) > 0, key


async def test_engine_quota_fails_rate_limited(
    make: Make, store: redis.asyncio.Redis, prefix: str
) -> None:
    """A call past its tenant's quota fails at once as rate_limited, with retry_after_secs."""
    engine, _ = await make({"big": {"fairness": {"mode": "quota", "rpm": 1}}})
    await away_from_minute_end(engine.clock.now)  # the quota's fixed window resets on the minute
    await engine.call("big", CHAT, tenant_id="acme")
    with pytest.raises(CallFailed) as caught:
        await engine.call("big", CHAT, tenant_id="acme", timeout_secs=5)
    # Another tenant has its own quota; with no tenant id the user id is the tenant.
    assert (await engine.call("big", CHAT, tenant_id="globex")).ok
    assert (await engine.call("big", CHAT, user_id="u-1")).ok
    assert caught.value.kind == "rate_limited"
    assert caught.value.retry_after_secs is not None
    assert caught.value.retry_after_secs > 0
    report: dict[str, Any] = await engine.status_reader.model(engine.registry.current, "big")
    assert report["fairness"] == {"mode": "quota", "active_tenants": 3, "waiting_within_share": 0}
    # The store holds the tenants' keyed hashes only, never the raw ids.
    for key in await tf_keys(store, prefix):
        assert b"acme" not in key
        assert b"globex" not in key
        assert b"u-1" not in key
