"""Status per tenant (0.2.0 T4) and the metrics allow-list (T6) on Valkey and Redis."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import pytest
from engine_testkit import CHAT, Make

from llm_queue.engine.metrics import NullSink, TenantLabels
from llm_queue.engine.queue import QueueSettings
from llm_queue.errors import ConfigError

pytestmark = pytest.mark.integration

A = "tenant-a"


class _Recorder(NullSink):
    """Every point the engine emits, with its labels."""

    def __init__(self) -> None:
        self.points: list[tuple[str, dict[str, str]]] = []

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        self.points.append((name, labels))

    def observe(self, name: str, value: float, **labels: str) -> None:
        self.points.append((name, labels))


async def test_status_has_tenant_counts_and_a_tenant_block(make: Make) -> None:
    engine, _ = await make(
        {"small": {"fairness": {"mode": "share", "default_weight": 2}}}, worker=False
    )
    tasks = [
        asyncio.create_task(engine.call("small", CHAT, tenant_id=A, timeout_secs=60))
        for _ in range(3)
    ]
    tasks.append(asyncio.create_task(engine.call("small", CHAT, tenant_id="b", timeout_secs=60)))
    try:
        key = engine.keys.request_index("small", engine.hash_id(A))
        for _ in range(300):
            if await engine.state.zcard(key) == 3:
                break
            await asyncio.sleep(0.02)
        queue = engine._queue_name(engine.registry.current, "small", False)
        await engine.queues.receive(queue, 1, QueueSettings(visibility_secs=60))  # one held
        report = await engine.status("small")
        assert report["request_index"] is True
        assert report["tenants"] == {"active": 2, "indexed_calls": 4}
        assert "tenant" not in report
        one = await engine.status("small", tenant=A)
        block = one["tenant"]
        assert block["tenant"] == engine.hash_id(A)  # the hash, never the name
        assert A not in str(one)
        assert (block["waiting"], block["running"], block["cancelling"]) == (3, 0, 0)
        assert block["held"] in (0, 1)  # the one taken may be another tenant's
        assert block["oldest_wait_secs"] is not None
        fair = block["fairness"]
        assert (fair["mode"], fair["weight"], fair["slots_in_use"]) == ("share", 2.0, 0)
        assert fair["used_this_minute"] == {"calls": 0, "tokens": 0}  # rate_window fixed
        nobody = (await engine.status("small", tenant="nobody"))["tenant"]
        assert (nobody["waiting"], nobody["running"], nobody["oldest_wait_secs"]) == (0, 0, None)
        with pytest.raises(ConfigError):
            await engine.status(tenant=A)
        everything = await engine.status()
        small = next(m for m in everything["models"] if m["model"] == "small")
        assert small["tenants"]["active"] == 2
        assert "tenant" not in small
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(BaseException):
                await task


async def test_metrics_carry_the_allow_listed_tenant_label(make: Make) -> None:
    engine, _ = await make()
    sink = _Recorder()
    engine.metrics = sink
    engine._tenant_label = TenantLabels([("acme", "acme"), ("globex", "t2")], engine.hash_id)
    for tenant in ("acme", "globex", "initech", None):
        assert (await engine.call("small", CHAT, tenant_id=tenant)).ok
    for _ in range(100):
        if sum(1 for n, _ in sink.points if n == "llmq_calls_total") >= 4:
            break
        await asyncio.sleep(0.02)
    calls = [labels["tenant"] for n, labels in sink.points if n == "llmq_calls_total"]
    waits = [labels["tenant"] for n, labels in sink.points if n == "llmq_wait_seconds"]
    assert sorted(calls) == sorted(waits) == sorted(["acme", "t2", "other", "-"])
    others: Any = [labels for n, labels in sink.points if n == "llmq_call_seconds"]
    assert all("tenant" not in labels for labels in others)
