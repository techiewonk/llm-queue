"""Round 3 observability: the engine's metric and log call sites (R3OB-1..-20, owner Q5/Q10/Q11).

An `Engine` is built over a store client that never connects (scripts register lazily) and
the full test registry run on its own config; each test drives one method and reads a
recording sink. Each test names its finding and what used to happen.
"""

from __future__ import annotations

import ast
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import redis.asyncio
from prometheus_client import CollectorRegistry

from llm_queue.clients.providers import Providers
from llm_queue.engine.envelope import Envelope, Meta
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.metrics import METRICS, PrometheusSink, spec_of
from llm_queue.engine.runtime import Engine, _Held
from llm_queue.registry.store import RegistryStore

FULL = Path(__file__).parents[1] / "registry" / "fixtures" / "full.json"
SRC = Path(__file__).parents[3] / "src" / "llm_queue"


class Sink:
    """Keeps every point: (kind, name, value, labels)."""

    def __init__(self) -> None:
        self.points: list[tuple[str, str, float, dict[str, str]]] = []

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        self.points.append(("count", name, value, labels))

    def observe(self, name: str, value: float, **labels: str) -> None:
        self.points.append(("observe", name, value, labels))

    def gauge(self, name: str, value: float, **labels: str) -> None:
        self.points.append(("gauge", name, value, labels))

    def named(self, name: str) -> list[tuple[float, dict[str, str]]]:
        return [(v, lb) for _, n, v, lb in self.points if n == name]


def _engine(metrics: Any = None) -> Engine:
    client = redis.asyncio.Redis(host="127.0.0.1", port=1)  # never connects in these tests
    store = RegistryStore(client, "t", config=json.loads(FULL.read_text("utf-8")))
    store.run_on_config()
    return Engine(client, store, Providers(MagicMock()), metrics=metrics or Sink(), worker_id="w1")


def _held(engine: Engine, **over: Any) -> _Held:
    now = engine.clock.now()
    env = Envelope(
        request_id="r1", op="converse", model="gpt20b", params={}, caller="w1", timeout_secs=60
    )
    held = _Held(
        message=None,
        envelope=env,
        first_seen_secs=now - 5,
        position_ms=int((now - 30) * 1000),
        tag="t",
    )
    for key, value in over.items():
        setattr(held, key, value)
    return held


def test_a_failed_call_carries_its_last_deployment(caplog: pytest.LogCaptureFixture) -> None:
    """R3OB-4 / R3OB-8: failures were counted with account and region '-', deployment None."""
    sink = Sink()
    engine = _engine(sink)
    dep = engine.registry.current.deployments("gpt20b")[0]
    held = _held(engine, last_dep=dep.key, attempts=3)
    with caplog.at_level(logging.INFO):
        engine._record(held, "gpt20b", dep.account, dep.region, "rate_limited", None, error="x")
    [(_, labels)] = sink.named("llmq_calls_total")
    assert (labels["account"], labels["region"]) == (dep.account, dep.region)
    record = next(r.llmq for r in caplog.records if r.llmq["event"] == "llmq_call_finished")  # type: ignore[attr-defined]
    assert record["deployment"] == dep.key
    assert record["attempts"] == 3
    assert record["worker"].startswith("w1")  # owner Q6: prefix plus a random part
    assert record["error"] == "x"


def test_waits_and_duration_are_histograms() -> None:
    """R3OB-2 / R3OB-3: no call duration existed, and the wait stopped at receive (hold unseen)."""
    sink = Sink()
    engine = _engine(sink)
    now = engine.clock.now()
    held = _held(engine, granted_secs=now - 2)  # sent 30 s ago, taken 25 s ago, granted 28 s in
    engine._record(held, "gpt20b", "a", "r", "ok", Meta(served_model="gpt20b"))
    [(wait, _)] = sink.named("llmq_wait_seconds")
    [(admit, _)] = sink.named("llmq_admission_wait_seconds")
    [(total, labels)] = sink.named("llmq_call_seconds")
    assert wait == pytest.approx(25, abs=0.5)
    assert admit == pytest.approx(28, abs=0.5)
    assert total == pytest.approx(30, abs=0.5)
    assert labels == {"model": "gpt20b", "outcome": "ok"}


async def test_a_dead_lettered_call_is_counted() -> None:
    """R3OB-5: a dead-lettered call got a reply but no count and no record."""
    sink = Sink()
    engine = _engine(sink)

    async def reply(*args: Any, **kwargs: Any) -> None:
        return None

    engine._reply = reply  # type: ignore[method-assign]
    body = _held(engine).envelope.dumps()
    await engine._dead_lettered("gpt20b", body)
    [(_, labels)] = sink.named("llmq_calls_total")
    assert labels["outcome"] == "dead_lettered"


async def test_caller_side_failures_are_counted() -> None:
    """R3OB-5: queue_full and queue_unavailable raised to callers were in no counter."""
    sink = Sink()
    engine = _engine(sink)

    async def full(*args: Any, **kwargs: Any) -> Any:
        raise CallFailed("queue_full", "full", request_id="r")

    engine._call_in = full  # type: ignore[method-assign]
    with pytest.raises(CallFailed):
        await engine.call("gpt20b", {"messages": []})
    assert sink.named("llmq_caller_failures_total") == [
        (1.0, {"model": "gpt20b", "outcome": "queue_full"})
    ]


def _report(engine: Engine) -> dict[str, Any]:
    dep = engine.registry.current.deployments("gpt20b")[0]
    return {
        "kill_switch": True,
        "blocked_regions": [dep.region],
        "models": [
            {
                "model": "gpt20b",
                "waiting": {"priority": 0, "normal": 1},
                "waiting_for_grant": {"priority": 0, "normal": 9},
                "oldest_waiting_secs": 90.0,
                "in_progress": 2,
                "dead_letters": 0,
                "deployments": [
                    {
                        "name": dep.key,
                        "enabled": True,
                        "account_enabled": True,
                        "blocked": True,
                        "healthy": True,
                        "breaker": "closed",
                        "rpm": {"limit": 100, "used": 5, "left": 95},
                        "tpm": {"limit": None, "used": 0, "left": None},
                        "concurrency": {"limit": 7.5, "in_use": 2},
                        "throttles_1m": 0,
                        "latency_ms": 1500.0,
                    }
                ],
            }
        ],
        "jobs": [{"job": "nightly", "waiting": 0, "dead_letters": 2}],
    }


def test_report_gauges_carry_limits_holds_and_switches() -> None:
    """R3OB-2 / R3OB-16 / R3OB-14 / Q15: no limit, held, usable, switch or dead-job gauge."""
    sink = Sink()
    engine = _engine(sink)
    engine._report_gauges(_report(engine))
    value = {(n, tuple(sorted(lb.items()))): v for k, n, v, lb in sink.points if k == "gauge"}
    dep = engine.registry.current.deployments("gpt20b")[0]
    d = (("deployment", dep.key), ("model", "gpt20b"))
    assert value[("llmq_held_waiting", (("model", "gpt20b"), ("priority", "normal")))] == 9
    assert value[("llmq_rpm_limit", d)] == 100
    assert value[("llmq_concurrency_limit", d)] == 7.5
    assert value[("llmq_deployment_usable", d)] == 0  # blocked
    assert value[("llmq_provider_latency_seconds", d)] == 1.5
    assert value[("llmq_kill_switch", ())] == 1
    assert value[("llmq_region_blocked", (("region", dep.region),))] == 1
    assert value[("llmq_job_dead_letters", (("job", "nightly"),))] == 2
    assert ("llmq_tpm_limit", d) not in value  # no limit set: no series


async def test_a_pod_that_lost_the_alert_lock_drops_its_cluster_gauges() -> None:
    """R3OB-1: a pod kept its last win's alert gauges for good; now cleared on the next tick."""
    registry = CollectorRegistry()
    engine = _engine(PrometheusSink(registry))
    engine._cluster.gauge("llmq_alert_model_unavailable", 1.0, model="gpt20b")
    engine._cluster.finish()

    async def lost() -> bool:
        return False

    engine._alert_lock = lost  # type: ignore[method-assign]
    await engine._alert_tick()
    assert registry.get_sample_value("llmq_alert_model_unavailable", {"model": "gpt20b"}) is None
    assert (
        registry.get_sample_value(
            "llmq_build_info", {"version": pytest.importorskip("llm_queue").__version__}
        )
        == 1
    )


async def test_a_dead_loop_is_logged_counted_and_named_in_health(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """R3OB-20: liveness turned false with no line saying why; the body said only false."""
    sink = Sink()
    engine = _engine(sink)

    async def boom() -> None:
        raise RuntimeError("sweeper bug")

    with caplog.at_level(logging.ERROR):
        engine._loop(boom(), "sweeper")
        await asyncio.sleep(0.01)
    died = [r.llmq for r in caplog.records if r.llmq.get("event") == "background_loop_died"]  # type: ignore[attr-defined]
    assert [d["loop"] for d in died] == ["sweeper"]
    assert sink.named("llmq_loop_died_total") == [(1.0, {"loop": "sweeper"})]
    engine._stores = (time.monotonic(), {"state": "ok", "queue": "ok"})  # no probe
    health = await engine.health_check()
    assert health["live"] is False
    assert health["live_reason"] == "loop died: sweeper"
    assert health["worker"] == engine.worker_id
    assert health["held"] == 0


async def test_cancels_are_counted_by_a_bounded_source() -> None:
    """Owner Q5: every cancel logged with its source; the metric label stays bounded."""
    sink = Sink()
    engine = _engine(sink)

    async def cancel(*args: Any, **kwargs: Any) -> int:
        return 0

    engine.requests.cancel = cancel  # type: ignore[method-assign]
    await engine._cancel_batch(["a", "b"], source="admin")
    await engine._cancel_batch(["c"], source="user-supplied-anything")
    assert sink.named("llmq_cancels_total") == [
        (2.0, {"source": "admin"}),
        (1.0, {"source": "other"}),
    ]


def test_outage_memo_and_readiness_gauges() -> None:
    """Owner Q11 / Q10: the outage memo and readiness on the eviction policy had no metric."""
    sink = Sink()
    engine = _engine(sink)
    engine._remember_outage()
    engine._remember_outage()  # already remembered: counted once
    assert sink.named("llmq_outage_memo_set_total") == [(1.0, {})]
    engine._pod_gauges()
    gauges = {n: v for k, n, v, lb in sink.points if k == "gauge" and not lb}
    assert gauges["llmq_outage_memo_active"] == 1
    assert gauges["llmq_ready"] == 0  # not started


def _emitted_names() -> set[str]:
    """Every metric name passed as a literal to count / observe / gauge in the library."""
    names: set[str] = set()
    for path in SRC.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text("utf-8"))):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in ("count", "observe", "gauge")
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
                and node.args[0].value.startswith("llmq_")
            ):
                names.add(node.args[0].value)
    return names


def test_every_emitted_metric_is_in_the_table() -> None:
    """R3OB-14 / R3OB-18: the metric list was hand-kept and missed names; one table now."""
    names = _emitted_names()
    assert "llmq_calls_total" in names
    assert sorted(n for n in names if spec_of(n) is None) == []
    assert "llmq_provider_latency_ms" not in METRICS  # renamed to _seconds


async def test_http_live_names_the_reason_and_metrics_are_served() -> None:
    """R3OB-20 / R3OB-14: /health/live said only false; no /metrics route existed."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from llm_queue.engine.admin import AdminServices
    from llm_queue.engine.http import status_router

    registry = CollectorRegistry()
    sink = PrometheusSink(registry)
    engine = _engine(sink)

    async def boom() -> None:
        raise RuntimeError("bug")

    engine._loop(boom(), "alerts")
    await asyncio.sleep(0.01)
    app = FastAPI()
    admin = AdminServices.for_engine(engine)
    app.include_router(status_router(admin, insecure_reads=True, metrics_route=True))
    client = TestClient(app)
    live = client.get("/health/live")
    assert (live.status_code, live.json()) == (503, {"live": False, "reason": "loop died: alerts"})
    assert b'llmq_loop_died_total{loop="alerts"} 1.0' in client.get("/metrics").content
