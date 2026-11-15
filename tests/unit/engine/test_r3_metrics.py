"""Round 3 observability: the metric sinks (R3OB-1, -11..-14, R3PF-14).

Each test names its finding and what used to happen. No store, no network: the EMF sink writes
into a list and the Prometheus sink into its own `CollectorRegistry`.
"""

from __future__ import annotations

import logging
from typing import Any

import orjson
import pytest
from prometheus_client import CollectorRegistry

from llm_queue.engine.metrics import (
    METRICS,
    EmfSink,
    GaugeSet,
    PrometheusSink,
    _StdoutWriter,
    prometheus_text,
    sink_for,
    unit_of,
)


def _lines(out: list[str]) -> list[dict[str, Any]]:
    return [orjson.loads(line) for line in out]


def test_one_call_is_one_emf_line() -> None:
    """R3PF-14 / R3OB-11: a call wrote 4-5 lines with the same labels; now one, all metrics."""
    out: list[str] = []
    sink = EmfSink(out.append, flush_secs=60)
    labels = {"model": "m", "account": "a", "region": "r"}
    for _ in range(3):
        sink.count("llmq_calls_total", 1.0, outcome="ok", **labels)
    sink.count("llmq_tokens_input_total", 10.0, **labels)
    sink.count("llmq_tokens_input_total", 5.0, **labels)
    sink.observe("llmq_provider_seconds", 0.5, **labels)
    sink.observe("llmq_provider_seconds", 1.5, **labels)
    assert out == []  # added up until the interval ends
    sink.flush()
    lines = _lines(out)
    assert len(lines) == 2  # two label sets (outcome is a label of calls only)
    by_names = {tuple(sorted(k for k in ln if k.startswith("llmq_"))): ln for ln in lines}
    tokens = by_names[("llmq_provider_seconds", "llmq_tokens_input_total")]
    assert tokens["llmq_tokens_input_total"] == 15
    assert tokens["llmq_provider_seconds"] == [0.5, 1.5]
    names = [m["Name"] for m in tokens["_aws"]["CloudWatchMetrics"][0]["Metrics"]]
    assert sorted(names) == ["llmq_provider_seconds", "llmq_tokens_input_total"]
    assert by_names[("llmq_calls_total",)]["llmq_calls_total"] == 3


def test_a_full_value_array_is_written_at_once() -> None:
    """R3OB-11: EMF takes at most 100 values per metric; the 100th sample writes the line."""
    out: list[str] = []
    sink = EmfSink(out.append, flush_secs=60)
    for i in range(100):
        sink.observe("llmq_wait_seconds", float(i), model="m", priority="0")
    assert len(out) == 1
    assert len(orjson.loads(out[0])["llmq_wait_seconds"]) == 100


def test_points_are_flushed_once_the_interval_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    """R3PF-14: the next point after `flush_secs` writes everything pending."""
    now = [100.0]
    monkeypatch.setattr("llm_queue.engine.metrics.time.monotonic", lambda: now[0])
    out: list[str] = []
    sink = EmfSink(out.append, flush_secs=1.0)
    sink.count("llmq_calls_total", model="m")
    assert out == []
    now[0] += 1.5
    sink.count("llmq_calls_total", model="m")
    assert len(out) == 1
    assert orjson.loads(out[0])["llmq_calls_total"] == 2


def test_emf_units_follow_the_name() -> None:
    """R3OB-13: `_ms` went out as unit None, used / in-use counts as None."""
    assert unit_of("llmq_x_ms", "gauge") == "Milliseconds"
    assert unit_of("llmq_x_seconds", "observe") == "Seconds"
    assert unit_of("llmq_rpm_used", "gauge") == "Count"
    assert unit_of("llmq_breaker_open", "gauge") == "None"
    out: list[str] = []
    EmfSink(out.append, flush_secs=0).gauge("llmq_provider_latency_seconds", 0.2, model="m")
    unit = orjson.loads(out[0])["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]["Unit"]
    assert unit == "Seconds"


def test_environment_is_a_dimension_and_the_pod_a_property() -> None:
    """R3OB-12: two environments in one account were summed; no line said which pod."""
    out: list[str] = []
    sink = EmfSink(
        out.append, static={"environment": "prod"}, properties={"pod": "p-1"}, flush_secs=0
    )
    sink.gauge("llmq_registry_on_own_config", 1.0)
    line = orjson.loads(out[0])
    assert line["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [["environment"]]
    assert line["environment"] == "prod"
    assert line["pod"] == "p-1"  # searchable, but not a dimension (no metric per pod)


def test_sink_for_keeps_one_sink_per_environment() -> None:
    """R3OB-12: `sink_for` keyed by name only would share one environment's dimension."""
    first = sink_for("emf", static={"environment": "t-a"})
    assert sink_for("emf", static={"environment": "t-a"}) is first
    assert sink_for("emf", static={"environment": "t-b"}) is not first


def test_dropped_lines_warn_once_a_minute_with_a_count(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """R3OB-11: drops were warned once per process lifetime, then never counted in the logs."""
    now = [0.0]
    monkeypatch.setattr("llm_queue.engine.metrics.time.monotonic", lambda: now[0])
    monkeypatch.setattr(_StdoutWriter, "MAX_LINES", 1)
    writer = _StdoutWriter()
    monkeypatch.setattr(writer, "start", lambda: None)  # no thread: the queue stays full
    with caplog.at_level(logging.WARNING):
        for _ in range(5):
            writer("x")
        now[0] = 61.0
        writer("x")
    warned = [r.llmq for r in caplog.records if r.llmq.get("event") == "emf_lines_dropped"]  # type: ignore[attr-defined]
    assert [(w["dropped"], w["dropped_total"]) for w in warned] == [(1, 1), (4, 5)]


def test_a_long_wait_lands_in_a_finite_bucket() -> None:
    """R3OB-14: the default buckets ended at 10 s, so a 120 s queue wait was only in +Inf."""
    registry = CollectorRegistry()
    sink = PrometheusSink(registry)
    sink.observe("llmq_wait_seconds", 120.0, model="m", priority="0")
    labels = {"model": "m", "priority": "0"}
    assert registry.get_sample_value("llmq_wait_seconds_bucket", {**labels, "le": "120.0"}) == 1
    assert registry.get_sample_value("llmq_wait_seconds_bucket", {**labels, "le": "60.0"}) == 0
    body, content_type = prometheus_text(sink)
    assert b"# HELP llmq_wait_seconds Queue wait" in body  # help text, not the name again
    assert content_type.startswith("text/plain")


def test_every_listed_metric_has_help_and_bounded_labels() -> None:
    """R3OB-14 / R3OB-12: one table; no label that a caller controls (request, user, tenant)."""
    for name, spec in METRICS.items():
        assert spec.help, name
        assert spec.help != name
        assert not {"request_id", "user", "tenant", "session", "pod", "worker"} & set(spec.labels)


def test_a_process_that_lost_the_alert_lock_clears_its_gauges() -> None:
    """R3OB-1: each pod kept the gauges of its last alert-lock win for good (an alert stuck at 1).

    Two pods with their own registries: A evaluates (alert on), then B evaluates (off). A clears
    its series on losing the lock, so no scrape shows A's stale 1.
    """
    reg_a, reg_b = CollectorRegistry(), CollectorRegistry()
    pod_a, pod_b = GaugeSet(PrometheusSink(reg_a)), GaugeSet(PrometheusSink(reg_b))
    labels = {"model": "m", "deployment": "d"}
    pod_a.gauge("llmq_alert_deployment_disabled", 1.0, **labels)
    pod_a.finish()
    assert reg_a.get_sample_value("llmq_alert_deployment_disabled", labels) == 1.0
    pod_b.gauge("llmq_alert_deployment_disabled", 0.0, **labels)
    pod_b.finish()
    pod_a.clear()  # A's next tick: the lock is B's
    assert reg_a.get_sample_value("llmq_alert_deployment_disabled", labels) is None
    assert reg_b.get_sample_value("llmq_alert_deployment_disabled", labels) == 0.0


def test_a_removed_deployment_series_goes_away() -> None:
    """R3OB-1: series of removed models or deployments were never removed."""
    registry = CollectorRegistry()
    gauges = GaugeSet(PrometheusSink(registry))
    for dep in ("d1", "d2"):
        gauges.gauge("llmq_rpm_used", 5.0, model="m", deployment=dep)
    gauges.finish()
    gauges.gauge("llmq_rpm_used", 6.0, model="m", deployment="d1")  # d2 left the registry
    gauges.finish()
    assert registry.get_sample_value("llmq_rpm_used", {"model": "m", "deployment": "d1"}) == 6.0
    assert registry.get_sample_value("llmq_rpm_used", {"model": "m", "deployment": "d2"}) is None


def test_every_metric_is_documented_in_monitor_md() -> None:
    """R3OB-19 / R3OB-18: the docs listed some metrics, without labels or units; now all."""
    from pathlib import Path

    page = Path(__file__).parents[3] / "docs" / "how-to" / "monitor.md"
    text = page.read_text("utf-8")
    missing = [name for name in METRICS if f"| `{name}` |" not in text]
    assert missing == []
