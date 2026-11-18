"""Every admin write's dry run leaves the store as it was (0.2.0 scenarios G15 and G23).

G15: for each operation of the operations table with a dry run, the prefix's keyspace (DUMP of
each key, and whether its TTL grew) is the same before and after `dry_run=True`, with calls
waiting, so there is something each operation would change. G23: `purge.queue` of one tenant
on a live store cancels that tenant's waiting calls, leaves the other's, and audits once.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import AsyncIterator
from typing import Any

import pytest
import redis.asyncio
from engine_testkit import CHAT, Make

from llm_queue.engine.admin import AdminServices
from llm_queue.engine.admin.ops import OPERATIONS
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.runtime import Engine

pytestmark = pytest.mark.integration

A, B = "tenant-a", "tenant-b"
# Keys an idle caller-only engine rewrites on its own (sweep lock, caller liveness and, in
# swept mode, the expiry index the sweeper and the heartbeat move).
_CHURN = (b":lock:", b":caller:", b":ttl")

# Arguments of every operation with a dry run: a new one fails the coverage check below.
ARGS: dict[str, dict[str, Any]] = {
    "registry.set": {"values": {"rpm": 5}, "model": "small"},
    "registry.publish": {},
    "registry.rollback": {"version": 1},
    "models.enable": {"model": "small"},
    "models.disable": {"model": "small"},
    "models.drain": {"model": "small"},
    "deployments.enable": {"model": "small", "deployment": "aws-a/r1"},
    "deployments.disable": {"model": "small", "deployment": "aws-a/r1"},
    "blocks.add": {"region": "r1"},
    "blocks.remove": {"region": "r1"},
    "blocks.kill_switch": {"on": True},
    "requests.cancel": {"model": "small", "tenant": A},
    "requests.reprioritize": {"model": "small", "priority": True},
    "requests.extend": {"request_id": "", "timeout_secs": 30},  # filled with a waiting call
    "dlq.redrive": {"model": "small"},
    "dlq.purge": {"model": "small"},
    "purge.model": {"model": "small", "force": True},
    "purge.deployment": {"model": "small", "deployment": "aws-a/r1", "force": True},
    "purge.tenant": {"tenant": A, "remove": ["keys", "queued"]},
    "purge.queue": {"model": "small", "tenant": A},
    "purge.removed": {},
    "purge.unschedule": {"target": "small"},
}
# Drain is process-local (it pauses an LLMQueue, writes nothing to the store); its dry run is
# covered by test_020_drain.py and test_020_drain_store.py.
DRY = sorted(
    op.id for op in OPERATIONS if op.dry_run_default is not None and not op.id.startswith("drain.")
)


def test_every_dry_run_operation_is_covered() -> None:
    assert sorted(ARGS) == DRY


async def _dump(store: redis.asyncio.Redis, prefix: str) -> dict[bytes, tuple[Any, int]]:
    out: dict[bytes, tuple[Any, int]] = {}
    async for key in store.scan_iter(match=f"{prefix}*", count=500):
        if any(part in key for part in _CHURN):
            continue
        out[key] = (await store.dump(key), int(await store.pttl(key)))
    return out


def _same(before: dict[bytes, tuple[Any, int]], after: dict[bytes, tuple[Any, int]]) -> list[Any]:
    """Keys added, gone, rewritten, or whose TTL grew (a TTL only falls by itself)."""
    changed = sorted(set(before) ^ set(after))
    for key in set(before) & set(after):
        (body, ttl), (body_after, ttl_after) = before[key], after[key]
        if body != body_after or (ttl_after > ttl > 0) or ((ttl > 0) != (ttl_after > 0)):
            changed.append(key)
    return changed


async def _waiting(engine: Engine, tenant: str, n: int) -> list[asyncio.Task[Any]]:
    key = engine.keys.request_index("small", engine.hash_id(tenant))
    tasks = [
        asyncio.create_task(engine.call("small", CHAT, tenant_id=tenant, timeout_secs=60))
        for _ in range(n)
    ]
    for _ in range(300):
        if await engine.state.zcard(key) >= n:
            return tasks
        await asyncio.sleep(0.02)
    raise AssertionError("calls not indexed in time")


@contextlib.asynccontextmanager
async def _calls(engine: Engine) -> AsyncIterator[list[asyncio.Task[Any]]]:
    tasks: list[asyncio.Task[Any]] = []
    try:
        yield tasks
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(BaseException):
                await task


@pytest.mark.parametrize("op_id", DRY)
async def test_g15_a_dry_run_writes_nothing(
    op_id: str, make: Make, store: redis.asyncio.Redis, prefix: str
) -> None:
    engine, _ = await make(worker=False)
    admin = AdminServices.for_engine(engine)
    async with _calls(engine) as tasks:
        tasks += await _waiting(engine, A, 2)
        tasks += await _waiting(engine, B, 1)
        kwargs = dict(ARGS[op_id])
        if op_id == "requests.extend":
            kwargs["request_id"] = (await admin.requests.list("small", tenant=A))["items"][0][
                "request_id"
            ]
        before = await _dump(store, prefix)
        method = admin.method(next(op for op in OPERATIONS if op.id == op_id))
        result = method(**kwargs, actor="ops", dry_run=True)
        if inspect.isawaitable(result):
            result = await result
        after = await _dump(store, prefix)
    assert result["dry_run"] is True
    assert _same(before, after) == [], op_id


async def test_g23_purge_queue_cancels_one_tenants_waiting_calls(make: Make) -> None:
    engine, _ = await make(worker=False)
    admin = AdminServices.for_engine(engine)
    async with _calls(engine) as tasks:
        a_calls = await _waiting(engine, A, 2)
        b_calls = await _waiting(engine, B, 1)
        tasks += a_calls + b_calls
        before = len(await engine.state.xrange(engine.registry.audit_key))
        report = await admin.purge.queue("small", tenant=A, actor="ops", dry_run=False)
        assert (report["matched"], report["changed"]) == (2, 2)
        for task in a_calls:
            with pytest.raises(CallFailed, match="cancelled"):
                await asyncio.wait_for(task, 5)
        entries = await engine.state.xrange(engine.registry.audit_key)
        assert len(entries) == before + 1
        assert entries[-1][1][b"op"] == b"purge_queue"
        assert A.encode() not in entries[-1][1][b"diff"]  # counts and the hash only
        await make(caller=False)  # B's call is still there: a worker serves it
        reply = await asyncio.wait_for(b_calls[0], 15)
        assert reply.ok
