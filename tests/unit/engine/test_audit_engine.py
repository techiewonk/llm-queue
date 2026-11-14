"""Bug-pinning tests for the engine audit (EN-*): each names its finding and the old behaviour.

They use small in-memory fakes of the store client, so they run without the Docker stack.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import orjson
import pytest
from redis.exceptions import ConnectionError as StoreConnectionError

from llm_queue.engine.admission import Admission
from llm_queue.engine.keys import Keys


def events(caplog: pytest.LogCaptureFixture, name: str) -> list[logging.LogRecord]:
    """The captured records of one structured event (`record.llmq["event"]`)."""
    return [r for r in caplog.records if getattr(r, "llmq", {}).get("event") == name]


class FakeScript:
    """A registered script whose reply comes from `run(keys, args)`."""

    def __init__(self, run: Callable[[list[Any], list[Any]], Any]) -> None:
        self.run = run
        self.calls = 0

    async def __call__(self, keys: list[Any], args: list[Any]) -> Any:
        self.calls += 1
        return self.run(keys, args)


class ScriptClient:
    """A store client whose scripts, in registration order, run the given functions."""

    def __init__(self, *runs: Callable[[list[Any], list[Any]], Any]) -> None:
        self.scripts = [FakeScript(run) for run in runs]
        self._next = 0

    def register_script(self, _source: str) -> FakeScript:
        script = self.scripts[self._next] if self._next < len(self.scripts) else FakeScript(_none)
        self._next += 1
        return script


def _none(_keys: list[Any], _args: list[Any]) -> Any:
    return None


def _down(_keys: list[Any], _args: list[Any]) -> Any:
    raise StoreConnectionError("connection reset")


# Admission registers PICK_TAKE, FINISH, UNDO, RENEW, SWEEP, REREGISTER in that order.
def _admission(**runs: Callable[[list[Any], list[Any]], Any]) -> Admission:
    order = ("pick", "finish", "undo", "renew", "sweep", "reregister")
    client = ScriptClient(*(runs.get(name, _none) for name in order))
    return Admission(client, Keys("t"), lease_secs=1.0)  # type: ignore[arg-type]  # fake


class RecordingSink:
    """A metrics sink that keeps every count point."""

    def __init__(self) -> None:
        self.counts: list[tuple[str, float, dict[str, str]]] = []

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        self.counts.append((name, value, labels))

    def observe(self, name: str, value: float, **labels: str) -> None:
        """Ignore."""

    def gauge(self, name: str, value: float, **labels: str) -> None:
        """Ignore."""


def test_en1_prometheus_sink_drops_a_point_with_other_labels(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """EN-1: a second label set for one metric raised `ValueError` into the call; now dropped."""
    from prometheus_client import CollectorRegistry

    from llm_queue.engine.metrics import PrometheusSink

    sink = PrometheusSink(CollectorRegistry())
    sink.count("llmq_test_lost_total", step="renew")
    with caplog.at_level(logging.WARNING):
        sink.count("llmq_test_lost_total", model="m", step="finish")
        sink.count("llmq_test_lost_total", model="m", step="finish")
        sink.observe("llmq_test_seconds", 1.0, model="m")
        sink.observe("llmq_test_seconds", 1.0)
        sink.gauge("llmq_test_depth", 1.0, model="m")
        sink.gauge("llmq_test_depth", 1.0, other="x")
    assert len(events(caplog, "metric_dropped")) == 3  # once per name


async def test_en1_renew_counts_lost_grants_with_the_finish_labels() -> None:
    """EN-1: `renew_all` counted `llmq_grants_lost_total` by step only, `finish` by model too."""
    reply = orjson.dumps({"n": 0, "lost": ["o1", "o2", "o3"]})
    admission = _admission(renew=lambda _k, _a: reply, finish=lambda _k, _a: 0)
    sink = RecordingSink()
    admission.metrics = sink
    items = [("a", "o1", "r1"), ("a", "o2", "r2"), ("b", "o3", "r3")]
    assert (await admission.renew_all(items))[1] == ["o1", "o2", "o3"]
    await admission.finish("a", "o9", "r9", refund_r=0, refund_t=0, outcome="ok")
    labels = {tuple(sorted(lb)) for name, _, lb in sink.counts if name == "llmq_grants_lost_total"}
    assert labels == {("model", "step")}
    per_model = {lb["model"]: v for _, v, lb in sink.counts if lb["step"] == "renew"}
    assert per_model == {"a": 2.0, "b": 1.0}


async def test_en16_undo_skips_on_store_error(caplog: pytest.LogCaptureFixture) -> None:
    """EN-16: `undo` let store errors escape (the runtime suppressed every exception unlogged)."""
    admission = _admission(undo=_down)
    with caplog.at_level(logging.WARNING):
        assert await admission.undo("m", "owner", "rid") is False
    assert len(events(caplog, "undo_skipped")) == 1


def test_en17_resettled_owner_does_not_block_pruning(monkeypatch: pytest.MonkeyPatch) -> None:
    """EN-17: re-settling an owner kept its old place, so pruning stopped at it."""
    import time

    now = [100.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    admission = _admission()  # lease 1 s: owners are kept 2 s
    admission._settle("A")
    now[0] += 0.5
    admission._settle("B")
    now[0] += 4.5
    admission._settle("A")  # recent again: it sat first and stopped B's pruning
    now[0] += 1.0
    admission._settle("C")
    assert list(admission._settled) == ["A", "C"]
