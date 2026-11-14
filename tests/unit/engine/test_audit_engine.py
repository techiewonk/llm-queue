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


def test_en2_raising_estimator_falls_back_to_chars(caplog: pytest.LogCaptureFixture) -> None:
    """EN-2: a custom estimator that raised escaped into the call loop (hot requeue)."""
    from llm_queue.engine import tokens

    tokens.register_token_estimator("en2_broken", lambda *_: 1 // 0)
    params = {"messages": [{"role": "user", "content": [{"text": "x" * 400}]}]}
    with caplog.at_level(logging.WARNING):
        first = tokens.input_tokens("converse", params, "en2_broken")
        second = tokens.input_tokens("converse", params, "en2_broken")
    assert first == second == tokens.input_tokens("converse", params)
    assert len(events(caplog, "hook_failed")) == 1


def test_en2_inference_config_none_is_filled() -> None:
    """EN-2: `"inferenceConfig": None` raised `TypeError` ('NoneType' item assignment)."""
    from llm_queue.engine import inference, tokens

    params: dict[str, Any] = {"inferenceConfig": None, "additionalModelRequestFields": None}
    out = inference.apply(
        "bedrock", "converse", params, {"max_tokens": 1, "extra": {"k": 1}, "top_k": 5}, {}
    )
    assert out["inferenceConfig"] == {"maxTokens": 1}
    assert out["additionalModelRequestFields"] == {"top_k": 5, "k": 1}
    assert params["inferenceConfig"] is None  # the caller's request is not changed
    assert tokens.max_tokens_of("converse", {"inferenceConfig": None}) is None


def test_en15_unknown_estimator_warns_once(caplog: pytest.LogCaptureFixture) -> None:
    """EN-15: an estimator name nobody registered fell back to characters / 4 with no log."""
    from llm_queue.engine import tokens

    with caplog.at_level(logging.WARNING):
        tokens.input_tokens("converse", {"x": "abcd"}, "en15_nope")
        tokens.input_tokens("converse", {"x": "abcd"}, "en15_nope")
    found = events(caplog, "hook_not_registered")
    assert len(found) == 1
    assert found[0].llmq["kind"] == "token_estimator"  # type: ignore[attr-defined]  # extra


def test_en28_tokens_does_not_import_the_provider_stack() -> None:
    """EN-28: `engine.tokens` imported `clients.providers` for one annotation (~60 ms)."""
    import subprocess
    import sys

    code = (
        "import sys, llm_queue.engine.tokens\nprint('llm_queue.clients.providers' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert out == "False"
