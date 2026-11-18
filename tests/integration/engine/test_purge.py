"""Purge of removed models, deployments and tenants, and auto purge (0.1.0b3, owner K1-K5).

Against the real stores (Valkey and Redis): each purge kind as a dry run, for real and again
(idempotent), the refusals, the tenant `~sum` fix, the wait timeout and the auto purge grace.
"""

from __future__ import annotations

import asyncio
import copy
import json
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, Make, registry

from llm_queue.engine.cli import amain
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.purge import PurgeService, PurgeSettings
from llm_queue.errors import AdminRefused
from llm_queue.ids import keyed_hash
from llm_queue.registry.store import RegistryStore

pytestmark = pytest.mark.integration
SALT = "purge-test-salt"


async def _service(
    store: redis.asyncio.Redis, prefix: str, doc: dict[str, Any] | None = None, **kw: Any
) -> PurgeService:
    reg = RegistryStore(store, prefix)
    await reg.publish(doc or registry(), actor="test")
    return PurgeService(store, reg, settings=PurgeSettings(**kw), salt=SALT)


def _without(doc: dict[str, Any], version: int, *, model: str | None = None) -> dict[str, Any]:
    """`doc` at `version`, without `model`, or without deployment aws-a/r2 of `big`."""
    out = copy.deepcopy(doc)
    out["registry_version"] = version
    if model:
        del out["models"][model]
    else:
        out["models"]["big"]["deployments"]["aws-a"]["confirmed"] = ["r1"]
    return out


async def _exists(store: redis.asyncio.Redis, keys: list[str]) -> list[int]:
    return [int(await store.exists(k)) for k in keys]


async def _last_audit(store: redis.asyncio.Redis, prefix: str) -> dict[bytes, bytes]:
    entries: Any = await store.xrevrange(f"{prefix}:{{reg}}:audit", count=1)
    return dict(entries[0][1])


async def test_model_purge_dry_run_real_and_again(store: redis.asyncio.Redis, prefix: str) -> None:
    svc = await _service(store, prefix)
    model = [f"{prefix}:{{m:ghost}}:cfg", f"{prefix}:{{m:ghost}}:rate:aws-a/r1"]
    for key in model:
        await store.hset(key, "x", "1")
    await store.zadd(f"{prefix}:{{m:ghost}}:inprog", {"o": 1})
    model.append(f"{prefix}:{{m:ghost}}:inprog")
    dlq, bell = f"{prefix}:{{q:llm-ghost}}:dlq", f"{prefix}:bell:llm-ghost"
    await store.xadd(dlq, {"body": "x"})
    await store.rpush(bell, "1")
    await store.zadd(f"{prefix}:ttl", {model[0]: 1})
    keep = f"{prefix}:{{m:big}}:cfg"  # written by the publish
    assert await store.exists(keep)

    plan = await svc.model("ghost", actor="plan")
    assert (plan.deleted_keys, plan.queues, plan.dry_run) == (5, ("llm-ghost",), True)
    assert await _exists(store, [*model, dlq, bell]) == [1] * 5

    done = await svc.model("ghost", actor="ops", dry_run=False)
    assert (done.deleted_keys, done.queues, done.actor) == (5, ("llm-ghost",), "ops")
    assert await _exists(store, [*model, dlq, bell, keep]) == [0] * 5 + [1]
    assert await store.zscore(f"{prefix}:ttl", model[0]) is None
    audit = await _last_audit(store, prefix)
    assert (audit[b"op"], audit[b"actor"]) == (b"purge_model", b"ops")
    assert done.as_dict()["queues"] == ["llm-ghost"]

    again = await svc.model("ghost", actor="ops", dry_run=False)
    assert (again.deleted_keys, again.queues) == (0, ())


async def test_model_purge_refusals_and_busy_queue(store: redis.asyncio.Redis, prefix: str) -> None:
    svc = await _service(store, prefix)
    with pytest.raises(AdminRefused, match="still in the registry"):
        await svc.model("big", actor="ops", dry_run=False)
    stream = f"{prefix}:{{q:llm-ghost}}:s"
    await store.xadd(stream, {"body": "x"})
    report = await svc.model("ghost", actor="ops", dry_run=False)
    assert report.queues == ()
    assert report.skipped[0][0] == "llm-ghost"
    assert "force" in report.skipped[0][1]
    assert await store.exists(stream)
    forced = await svc.model("ghost", actor="ops", dry_run=False, force=True)
    assert forced.queues == ("llm-ghost",)
    assert not await store.exists(stream)


async def _seed_deployment(store: redis.asyncio.Redis, prefix: str) -> tuple[list[str], list[str]]:
    """State of big's aws-a/r2 (gone) and aws-a/r1 (kept): (r2 keys, r1 keys)."""
    m = f"{prefix}:{{m:big}}:"
    gone = [f"{m}{n}:aws-a/r2" for n in ("rate", "ac", "cb", "terr")]
    gone += [f"{m}lease:aws-a/r2:p", f"{m}thr:aws-a/r2:123", f"{m}sticky:s1"]
    kept = [f"{m}rate:aws-a/r1", f"{m}sticky:s2", f"{m}thr:aws-a/r1:123"]
    for key in gone + kept:
        if "sticky" in key:
            await store.set(key, "aws-a/r2" if key in gone else "aws-a/r1")
        else:
            await store.hset(key, "x", "1")
    await store.hset(f"{m}health", mapping={"aws-a/r2|until": 1, "aws-a/r1|until": 2})
    await store.hset(f"{m}rot", mapping={"rr|aws-a/r2": 1, "cw|aws-a/r1": 2})
    return gone, kept


async def test_deployment_purge(store: redis.asyncio.Redis, prefix: str) -> None:
    doc = registry()
    svc = await _service(store, prefix, doc)
    gone, kept = await _seed_deployment(store, prefix)
    with pytest.raises(AdminRefused):
        await svc.deployment("big", "aws-a/r2", actor="ops", dry_run=False)
    await svc.registry.publish(_without(doc, 2), actor="test")

    plan = await svc.deployment("big", "aws-a/r2", actor="plan", wait=False)
    assert (plan.deleted_keys, plan.deleted_fields, plan.target) == (7, 2, "big/aws-a/r2")
    done = await svc.deployment("big", "aws-a/r2", actor="ops", dry_run=False)
    assert (done.deleted_keys, done.deleted_fields) == (7, 2)
    assert await _exists(store, gone) == [0] * 7
    assert await _exists(store, kept) == [1] * 3
    m = f"{prefix}:{{m:big}}:"
    assert await store.hkeys(f"{m}health") == [b"aws-a/r1|until"]
    assert await store.hkeys(f"{m}rot") == [b"cw|aws-a/r1"]
    assert (await _last_audit(store, prefix))[b"op"] == b"purge_deployment"
    again = await svc.deployment("big", "aws-a/r2", actor="ops", dry_run=False)
    assert (again.deleted_keys, again.deleted_fields) == (0, 0)


async def test_deployment_purge_wait_timeout_skips(store: redis.asyncio.Redis, prefix: str) -> None:
    doc = registry()
    svc = await _service(store, prefix, doc, wait_secs=0.2)
    await svc.registry.publish(_without(doc, 2), actor="test")
    gone, _ = await _seed_deployment(store, prefix)
    await store.hset(f"{prefix}:{{m:big}}:inprog:r-1", mapping={"dep": "aws-a/r2"})
    waited = await svc.deployment(
        "big", "aws-a/r2", actor="ops", dry_run=False
    )  # wait=True by default
    assert waited.deleted_keys == 0
    assert "still running" in waited.skipped[0][1]
    assert await _exists(store, gone) == [1] * 7
    now = await svc.deployment("big", "aws-a/r2", actor="ops", dry_run=False, wait=False)
    assert now.deleted_keys == 7


async def test_tenant_purge_fixes_the_weight_sum(store: redis.asyncio.Redis, prefix: str) -> None:
    svc = await _service(store, prefix)
    h, other = keyed_hash(SALT, "acme"), keyed_hash(SALT, "beta")
    m = f"{prefix}:{{m:big}}:"
    await store.hset(f"{m}tf:w", mapping={h: "2", other: "1", "~sum": "3"})
    await store.zadd(f"{m}tf:act", {h: 1, other: 1})
    await store.hset(f"{m}tf:t:{h}", "r_tat", 1)
    await store.zadd(f"{m}tf:s:{h}", {"o": 1})

    plan = await svc.tenant("acme", actor="plan")
    assert (plan.deleted_keys, plan.deleted_fields, plan.target) == (2, 2, f"tenant:{h}")
    done = await svc.tenant("acme", actor="ops", dry_run=False)
    assert (done.deleted_keys, done.deleted_fields) == (2, 2)
    assert await store.hgetall(f"{m}tf:w") == {other.encode(): b"1", b"~sum": b"1"}
    assert await store.zrange(f"{m}tf:act", 0, -1) == [other.encode()]
    assert await _exists(store, [f"{m}tf:t:{h}", f"{m}tf:s:{h}"]) == [0, 0]
    assert b"acme" not in str(await _last_audit(store, prefix)).encode()  # only the hash
    again = await svc.tenant("acme", actor="ops", dry_run=False)
    assert (again.deleted_keys, again.deleted_fields) == (0, 0)
    await svc.tenant("beta", actor="ops", dry_run=False, models=["big"])
    assert not await store.exists(f"{m}tf:w")  # the last tenant: the hash goes


async def test_tenant_purge_removes_the_tenant_name(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """FA-5: the raw name in `tqn` (`LLMQ_TENANT_NAMES_ENABLED`) goes with the tenant's state.

    It stayed until a later call on the model pruned lapsed tenants, which with ongoing traffic
    and a growing TTL could be never.
    """
    svc = await _service(store, prefix)
    h, other = keyed_hash(SALT, "acme"), keyed_hash(SALT, "beta")
    names = f"{prefix}:{{m:big}}:tqn"
    await store.hset(names, mapping={h: "acme", other: "beta"})
    plan = await svc.tenant("acme", actor="plan")
    assert plan.deleted_fields == 1
    assert await store.hget(names, h) == b"acme"  # a dry run writes nothing
    done = await svc.tenant("acme", actor="ops", dry_run=False)
    assert done.deleted_fields == 1
    assert await store.hgetall(names) == {other.encode(): b"beta"}
    await svc.tenant("beta", actor="ops", dry_run=False, models=["big"])
    assert not await store.exists(names)


async def test_tenant_calls_come_from_the_request_index(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    """T3: a tenant purge of queued / running calls reads the T1 index of each model."""
    off = await _service(store, prefix, request_index=False)
    with pytest.raises(AdminRefused, match="LLMQ_REQUEST_INDEX_ENABLED"):
        await off.tenant("acme", actor="ops", dry_run=False, remove={"keys", "queued"})
    indexed = await _service(store, prefix)
    h = keyed_hash(SALT, "acme")
    other = keyed_hash(SALT, "beta")
    for rid, state, model, tenant in (
        ("q1", "queued", "big", h),
        ("r1", "running", "small", h),
        ("b1", "queued", "big", other),
    ):
        await store.zadd(f"{prefix}:{{m:{model}}}:tq:{tenant}", {rid: 10**15})
        await store.hset(f"{prefix}:{{r:{rid}}}:job", "state", state)
    await store.zadd(f"{prefix}:{{m:big}}:tq:{h}", {"lapsed": 1})  # past its deadline
    plan = await indexed.tenant("acme", actor="plan", remove={"queued"})
    assert plan.cancelled == 1
    done = await indexed.tenant("acme", actor="ops", dry_run=False, remove={"queued"})
    assert done.cancelled == 1
    assert await _exists(store, [f"{prefix}:{{r:q1}}:cancel", f"{prefix}:{{r:r1}}:cancel"]) == [
        1,
        0,
    ]
    everything = await indexed.tenant(
        "acme", actor="ops", dry_run=False, remove={"running", "keys"}
    )
    # q1 was answered `cancelled` by the first purge (final): only r1 is marked now (FA-4).
    assert everything.cancelled == 1
    assert (await store.hgetall(f"{prefix}:{{r:q1}}:status"))[b"state"] == b"cancelled"
    assert await store.hget(f"{prefix}:{{r:r1}}:job", "state") == b"running"  # its worker's
    assert not await store.exists(f"{prefix}:{{r:b1}}:cancel")  # another tenant: untouched
    assert not await store.exists(f"{prefix}:{{r:lapsed}}:cancel")


async def test_removed_purges_models_and_deployments(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    doc = registry()
    svc = await _service(store, prefix, doc)
    gone, kept = await _seed_deployment(store, prefix)
    await store.hset(f"{prefix}:{{m:ghost}}:cfg", "x", 1)
    await svc.registry.publish(_without(doc, 2), actor="test")
    plan = await svc.removed(actor="ops", dry_run=True)
    assert plan.deleted_keys == 8
    assert plan.kind == "removed"
    done = await svc.removed(actor="ops", dry_run=False)
    assert done.deleted_keys == 8
    assert await _exists(store, [*gone, f"{prefix}:{{m:ghost}}:cfg"]) == [0] * 8
    assert await _exists(store, kept) == [1] * 3
    assert (await svc.removed(actor="ops", dry_run=False)).deleted_keys == 0


async def test_auto_purge_grace_rollback_and_due(store: redis.asyncio.Redis, prefix: str) -> None:
    doc = registry()
    svc = await _service(store, prefix, doc, grace_secs=600)
    v1 = await svc.registry.load()
    assert v1 is not None
    await svc.tick(v1)  # first sight: nothing dropped yet
    assert await svc.scheduled() == []
    gone, _ = await _seed_deployment(store, prefix)
    await store.hset(f"{prefix}:{{m:small}}:cfg", "x", 1)
    v2_doc = _without(_without(doc, 2), 2, model="small")
    await svc.registry.publish(v2_doc, actor="test")
    v2 = await svc.registry.load()
    assert v2 is not None
    await svc.tick(v2)
    pending = {row["target"]: row["kind"] for row in await svc.scheduled()}
    assert pending == {"small": "model", "big/aws-a/r2": "deployment"}
    await svc.tick(v2)  # within the grace: nothing purged
    assert await _exists(store, gone) == [1] * 7

    # Rollback within the grace: the entry goes when due, the state stays.
    await store.zadd(f"{prefix}:purge", {"small": 0})
    rolled = copy.deepcopy(doc)
    rolled["registry_version"] = 3
    rolled["models"]["big"]["deployments"]["aws-a"]["confirmed"] = ["r1"]
    await svc.registry.publish(rolled, actor="test")
    v3 = await svc.registry.load()
    assert v3 is not None
    await svc.tick(v3)
    assert [r["target"] for r in await svc.scheduled()] == ["big/aws-a/r2"]
    assert await store.exists(f"{prefix}:{{m:small}}:cfg")

    # Due and still absent: purged, entry removed, audited as `auto`.
    await store.zadd(f"{prefix}:purge", {"big/aws-a/r2": 0})
    await svc.tick(v3)
    assert await svc.scheduled() == []
    assert await _exists(store, gone) == [0] * 7
    assert (await _last_audit(store, prefix))[b"actor"] == b"auto"
    assert not await svc.cancel_scheduled("big/aws-a/r2")


async def test_auto_purge_off_schedules_nothing(store: redis.asyncio.Redis, prefix: str) -> None:
    doc = registry()
    svc = await _service(store, prefix, doc, auto=False)
    v1 = await svc.registry.load()
    assert v1 is not None
    await svc.tick(v1)
    await svc.registry.publish(_without(doc, 2, model="small"), actor="test")
    v2 = await svc.registry.load()
    assert v2 is not None
    await svc.tick(v2)
    assert await svc.scheduled() == []


async def test_engine_has_the_service(make: Make) -> None:
    engine, _ = await make(worker=False)
    assert isinstance(engine.purge, PurgeService)
    assert engine.purge.keys.prefix == engine.keys.prefix
    assert engine.purge._hash("acme") == engine.hash_id("acme")


async def test_cli_purge_dry_run_and_refusal(store: redis.asyncio.Redis, prefix: str) -> None:
    doc = registry()
    port = store.connection_pool.connection_kwargs["port"]
    env = {
        "LLMQ_STORE_URL": f"redis://:localdev@127.0.0.1:{port}/0",
        "LLMQ_ENVIRONMENT": "test",
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_REGISTRY": json.dumps(doc),
    }
    await amain(["registry", "publish", "--yes"], env)
    await store.hset(f"{prefix}:{{m:ghost}}:cfg", "x", 1)
    dry = await amain(["purge", "model", "ghost"], env)
    assert (dry["deleted_keys"], dry["dry_run"]) == (1, True)
    assert await store.exists(f"{prefix}:{{m:ghost}}:cfg")
    off = {**env, "LLMQ_REQUEST_INDEX_ENABLED": "false"}
    with pytest.raises(AdminRefused):
        await amain(["purge", "tenant", "acme", "--remove", "queued"], off)
    done = await amain(["--actor", "ops", "purge", "removed", "--yes"], env)
    assert (done["deleted_keys"], done["actor"]) == (1, "ops")


async def test_engine_purges_a_tenants_calls_from_the_index(make: Make) -> None:
    """T3: a running call of the tenant is found in the request index and cancelled."""
    slow = {"defaults": {"latency_ms": {"p50": 3000, "p95": 3000}}}
    engine, _ = await make({"small": {"timeout_secs": 30, "attempt_timeout_secs": 20}}, slow)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme"))
    h = engine.hash_id("acme")
    key = engine.keys.request_index("small", h)
    for _ in range(100):
        if await engine.state.zcard(key):
            break
        await asyncio.sleep(0.02)
    report = await engine.purge.tenant("acme", actor="ops", dry_run=False, remove={"running"})
    assert report.cancelled == 1
    with pytest.raises(CallFailed, match="cancelled"):
        await task
    assert await engine.state.zcard(key) == 0  # the cancelled call's final write removed it


async def test_a_tenant_purge_answers_its_waiting_callers_at_once(make: Make) -> None:
    """FA-4: a purge of the tenant's queued calls answers their callers `cancelled` at once.

    It only set cancel marks: with no worker reaching the message, the caller waited for its
    deadline (60 s here). Now it is answered as the admin cancel answers (`CANCEL_SETTLE`).
    """
    engine, _ = await make(worker=False)
    task = asyncio.create_task(engine.call("small", CHAT, tenant_id="acme", timeout_secs=60))
    key = engine.keys.request_index("small", engine.hash_id("acme"))
    for _ in range(100):
        if await engine.state.zcard(key):
            break
        await asyncio.sleep(0.02)
    rid = (await engine.state.zrange(key, 0, 0))[0].decode()
    report = await engine.purge.tenant("acme", actor="ops", dry_run=False, remove={"queued"})
    assert report.cancelled == 1
    with pytest.raises(CallFailed, match="cancelled"):
        await asyncio.wait_for(task, 5)
    assert (await engine.requests.status(rid))["state"] == "cancelled"
    assert await engine.state.zcard(key) == 0
    again = await engine.purge.tenant("acme", actor="ops", dry_run=False, remove={"running"})
    assert again.cancelled == 0  # final: nothing marked, nothing counted


async def test_auto_purge_waits_when_the_registry_cannot_be_read(
    store: redis.asyncio.Redis, prefix: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rescan B3: a store error fell back to the pod's copy; now the due purge waits."""
    doc = registry()
    svc = await _service(store, prefix, doc)
    view = await svc.registry.load()
    assert view is not None
    await store.hset(f"{prefix}:{{m:ghost}}:cfg", "x", 1)
    await store.zadd(f"{prefix}:purge", {"ghost": 0})

    async def broken() -> Any:
        raise ConnectionError("store blip")

    monkeypatch.setattr(svc.registry, "_read", broken)
    await svc.tick(view)
    assert [r["target"] for r in await svc.scheduled()] == ["ghost"]
    assert await store.exists(f"{prefix}:{{m:ghost}}:cfg")
