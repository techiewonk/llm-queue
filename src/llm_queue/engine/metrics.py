"""Metric sinks (plan E131): CloudWatch embedded metric format by default, Prometheus optional.

The engine calls `count`, `observe` and `gauge` with labels from the registry (model, account,
region, deployment) and bounded enums (outcome, reason); never a request, user or free text.
`METRICS` is the one table of every metric the library emits: kind, unit, labels and help,
read by both sinks and by the docs. `EmfSink` adds points up per label set and writes one JSON
line per label set per second; `PrometheusSink` keeps `prometheus_client` metrics for a
`/metrics` endpoint (`prometheus_text`).
"""

from __future__ import annotations

import atexit
import contextlib
import queue
import sys
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol

import orjson

from llm_queue._logging import get_logger
from llm_queue.errors import MissingExtra

logger = get_logger(__name__)
NAMESPACE = "llm-queue"

Kind = Literal["counter", "histogram", "gauge"]

# Queue and admission waits run to minutes: the default buckets ended at 10 s, so every long
# wait landed in +Inf (R3OB-14).
WAIT_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0)
CALL_BUCKETS = (0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0)


@dataclass(frozen=True, slots=True)
class MetricSpec:
    """One metric: its kind, labels, unit (EMF), help text and histogram buckets."""

    kind: Kind
    labels: tuple[str, ...]
    help: str
    unit: str = "None"
    buckets: tuple[float, ...] | None = None
    who: str = "worker"  # which process emits it: worker, alerter, caller, every pod


def _c(labels: str, text: str, who: str = "worker") -> MetricSpec:
    return MetricSpec("counter", tuple(labels.split()), text, "Count", None, who)


def _g(labels: str, text: str, unit: str = "Count", who: str = "alerter") -> MetricSpec:
    return MetricSpec("gauge", tuple(labels.split()), text, unit, None, who)


def _p(labels: str, text: str, unit: str = "None") -> MetricSpec:
    return MetricSpec("gauge", tuple(labels.split()), text, unit, None, "every pod")


def _h(labels: str, text: str, buckets: tuple[float, ...]) -> MetricSpec:
    return MetricSpec("histogram", tuple(labels.split()), text, "Seconds", buckets, "worker")


_DEP = "model deployment"
# Every metric the library emits. `who`: `alerter` gauges come from the one process holding the
# alert lock (the others clear theirs, R3OB-1); `every pod` ones describe the process itself.
METRICS: dict[str, MetricSpec] = {
    # ---- per call (worker)
    "llmq_calls_total": _c(
        "model account region outcome",
        "Calls finished by a worker, by outcome (ok or the error kind); account and region "
        "of the last deployment tried, '-' when none was",
    ),
    "llmq_caller_failures_total": _c(
        "model outcome",
        "Calls that failed in the caller before any worker took them (rejected, queue_full, "
        "queue_unavailable, deadline_exceeded, store_down)",
        "caller",
    ),
    "llmq_attempts_total": _c(
        "model account region result",
        "Provider attempts by result: ok, throttled, retryable, model_error, auth, target, "
        "or the request error kind",
    ),
    "llmq_tokens_input_total": _c("model account region", "Input tokens of successful calls"),
    "llmq_tokens_output_total": _c("model account region", "Output tokens of successful calls"),
    "llmq_fallback_total": _c("kind model", "Successful calls served by a fallback"),
    "llmq_wait_seconds": _h(
        "model priority",
        "Queue wait: from send until a worker took the call (budget hold not included)",
        WAIT_BUCKETS,
    ),
    "llmq_admission_wait_seconds": _h(
        "model priority",
        "From send until the call's first grant: queue wait plus the wait for budget",
        WAIT_BUCKETS,
    ),
    "llmq_call_seconds": _h(
        "model outcome", "From send until the call finished, by outcome", CALL_BUCKETS
    ),
    "llmq_provider_seconds": _h(
        "model account region", "Duration of each provider attempt", CALL_BUCKETS
    ),
    "llmq_first_token_seconds": _h(
        "model account region", "Streams: from the attempt's start to its first chunk", CALL_BUCKETS
    ),
    "llmq_admission_refusals_total": _c("model reason", "Calls refused by admission, once each"),
    "llmq_retries_total": _c("model reason", "Retries after a failed attempt, by error kind"),
    "llmq_grants_lost_total": _c("model step", "Grants found gone at renew or finish"),
    "llmq_store_errors_total": _c("where", "Store errors by step", "every pod"),
    "llmq_bad_messages_total": _c("model", "Messages that could not be read (dead-lettered)"),
    "llmq_newer_messages_total": _c("model", "Messages from a newer library, given back"),
    "llmq_swept_total": _c("model", "Lapsed leases and in-progress entries the sweep removed"),
    "llmq_reclaimed_total": _c("model", "Messages taken over from a worker that went quiet"),
    "llmq_stuck_calls_total": _c("model", "Held calls freed because nothing worked on them"),
    "llmq_store_resets_total": _c("", "State store resets (lost keys) this pod detected"),
    "llmq_credentials_failed_total": _c("account region", "Credential or auth failures"),
    "llmq_loop_died_total": _c("loop", "Background loops that ended with an error", "every pod"),
    "llmq_jobs_no_consumer_total": _c("job", "Jobs sent with no consumer group", "caller"),
    "llmq_job_ack_failed_total": _c("job", "Job acknowledgements that failed"),
    "llmq_registry_reload_errors_total": _c("error", "Registry reloads that failed", "every pod"),
    "llmq_registry_refused_total": _c(
        "reason", "Stored registry documents this pod refused (owner Q25)", "every pod"
    ),
    "llmq_send_not_replicated_total": _c(
        "scope", "Writes that reached fewer replicas than asked, sent anyway (owner Q9)", "caller"
    ),
    "llmq_cancels_total": _c(
        "source",
        "Cancels asked, by source: api, caller_task, fail_fast, admin, cli, http, other",
        "caller",
    ),
    "llmq_outage_memo_set_total": _c(
        "", "Times this pod started remembering a store outage (owner Q11)", "every pod"
    ),
    "llmq_store_pool_busy_total": _c("model", "Calls refused because the store pool was full"),
    "llmq_pubsub_resubscribes_total": _c("reader", "Stale subscribers reset", "every pod"),
    "llmq_stale_acks_total": _c("model", "Acks refused: another worker took the message over"),
    "llmq_jobs_total": _c("job outcome", "Jobs finished by outcome (ok, failed, dead, ...)"),
    "llmq_hook_failed_total": _c("kind hook", "App hooks that raised", "every pod"),
    # ---- cluster state (the alert-lock holder, every 30 s)
    "llmq_queue_waiting": _g("model priority", "Calls in the queue no worker has taken"),
    "llmq_held_waiting": _g(
        "model priority", "Calls a worker took that still wait for budget (a grant)"
    ),
    "llmq_in_progress": _g("model", "Calls running at a provider"),
    "llmq_dead_letters": _g("model", "Dead letters, priority and normal queues together"),
    "llmq_oldest_waiting_seconds": _g(
        "model", "Age of the oldest call in the model's queues", "Seconds"
    ),
    "llmq_breaker_open": _g(_DEP, "1 when the deployment's breaker is open", "None"),
    "llmq_deployment_usable": _g(
        _DEP, "1 when the deployment can take calls (enabled, unblocked, healthy)", "None"
    ),
    "llmq_rpm_used": _g(_DEP, "Requests used in the current minute"),
    "llmq_rpm_limit": _g(_DEP, "The deployment's RPM limit"),
    "llmq_tpm_used": _g(_DEP, "Tokens used in the current minute"),
    "llmq_tpm_limit": _g(_DEP, "The deployment's TPM limit"),
    "llmq_concurrency_in_use": _g(_DEP, "Calls holding a concurrency slot"),
    "llmq_concurrency_limit": _g(_DEP, "The adaptive concurrency limit now"),
    "llmq_throttles_1m": _g(_DEP, "Provider throttles, the larger of last and this minute"),
    "llmq_provider_latency_seconds": _g(
        _DEP, "Smoothed provider latency from the health hash", "Seconds"
    ),
    "llmq_kill_switch": _g("", "1 while the routing kill switch is on", "None"),
    "llmq_region_blocked": _g("region", "1 while the region is blocked by routing", "None"),
    "llmq_job_dead_letters": _g("job", "Dead jobs kept, per job name (owner Q15)"),
    "llmq_job_waiting": _g("job", "Jobs waiting, per job name"),
    "llmq_alerts_evaluated_timestamp_seconds": _g(
        "", "Unix time of the last alert evaluation (alarm when it stops moving)", "Seconds"
    ),
    # ---- this pod
    "llmq_registry_invalid_reads": _p("", "Stored registry reads refused as invalid", "Count"),
    "llmq_registry_stored_refused": _p("", "1 when the stored registry was refused"),
    "llmq_registry_lib_too_old": _p("", "1 when the stored registry needs a newer library"),
    "llmq_registry_on_own_config": _p("", "1 when this pod runs on its own config"),
    "llmq_build_info": _p("version", "Always 1; the library version as a label"),
    "llmq_store_up": _p("store", "1 when the store takes writes, 0 when down or read-only"),
    "llmq_held_calls": _p("", "Calls this pod holds (waiting for budget or running)", "Count"),
    "llmq_outage_memo_active": _p("", "1 while this pod remembers a store outage (owner Q11)"),
    "llmq_store_eviction_unsafe": _p(
        "store", "1 when the store's eviction policy may evict queue keys (owner Q10)"
    ),
    "llmq_ready": _p("", "1 when this pod is ready (0: draining, no registry, or evicting store)"),
    "llmq_heartbeat_age_seconds": _p(
        "", "Seconds since this pod's heartbeat last made progress", "Seconds"
    ),
}
# One gauge per alert name, labels model [, deployment] (`AlertEvaluator`).
ALERT_PREFIX = "llmq_alert_"


def spec_of(name: str) -> MetricSpec | None:
    """The table entry of a metric (alert gauges share one), None for a name not listed."""
    if (spec := METRICS.get(name)) is not None:
        return spec
    if name.startswith(ALERT_PREFIX):
        return MetricSpec("gauge", ("model", "deployment"), f"1 while alert {name} is on")
    return None


def unit_of(name: str, kind: str) -> str:
    """EMF unit: the table's, else by suffix (`_seconds`, `_ms`, `_bytes`, `_total`) (R3OB-13)."""
    if (spec := METRICS.get(name)) is not None:
        return spec.unit
    for suffix, unit in (
        ("_seconds", "Seconds"),
        ("_ms", "Milliseconds"),
        ("_bytes", "Bytes"),
        ("_total", "Count"),
    ):
        if name.endswith(suffix):
            return unit
    return "Count" if kind == "count" else "None"


class Metrics(Protocol):
    """What the engine needs from a metrics sink."""

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        """Add to a counter."""
        ...

    def observe(self, name: str, value: float, **labels: str) -> None:
        """Record one sample of a distribution (seconds, tokens)."""
        ...

    def gauge(self, name: str, value: float, **labels: str) -> None:
        """Set a gauge."""
        ...


class NullSink:
    """Drops every point (tests, or metrics turned off)."""

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        """Ignore."""

    def observe(self, name: str, value: float, **labels: str) -> None:
        """Ignore."""

    def gauge(self, name: str, value: float, **labels: str) -> None:
        """Ignore."""

    def remove(self, name: str, **labels: str) -> None:
        """Ignore."""


class _StdoutWriter:
    """Writes lines to stdout from one daemon thread, so a backed-up log pipe never holds the loop.

    `EmfSink`'s default `write` (EN-20). Lines wait in a bounded queue (`MAX_LINES`); when it is
    full a line is dropped and counted (`dropped`), with a warning at most once a minute
    (R3OB-11: once per process hid every later loss). No thread until the first line, so
    importing or building a sink starts nothing. `on_idle` runs on the thread when no line
    came for `IDLE_SECS` (the EMF sink's flush of points added up since its last line).
    """

    MAX_LINES = 10_000
    IDLE_SECS = 1.0
    WARN_EVERY_SECS = 60.0

    def __init__(self) -> None:
        """An empty queue; the thread starts on the first line."""
        self._lines: queue.Queue[str] = queue.Queue(self.MAX_LINES)
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self.dropped = 0
        self._warned_mono = -1e9
        self._warned_count = 0
        self.on_idle: Callable[[], None] | None = None

    def start(self) -> None:
        """Start the writer thread: once, and again when it is gone (a forked child, FC-9)."""
        thread = self._thread
        if thread is None or not thread.is_alive():
            # Not only the first line: after a fork the child has `_thread` set but no thread,
            # and its lines piled up to the cap, then were dropped (FC-9).
            with self._lock:
                if self._thread is None or not self._thread.is_alive():
                    if self._thread is None:
                        # Lines still queued at exit would be lost with the daemon thread.
                        atexit.register(self.flush)
                    else:  # the parent's queue may hold its lines and a held lock: start over
                        self._lines = queue.Queue(self.MAX_LINES)
                    self._thread = threading.Thread(
                        target=self._run, name="llmq-emf-writer", daemon=True
                    )
                    self._thread.start()

    def __call__(self, line: str) -> None:
        """Queue one line; drop it (counted, warned once a minute) when the queue is full."""
        self.start()
        try:
            self._lines.put_nowait(line)
        except queue.Full:
            self.dropped += 1
            now = time.monotonic()
            if now - self._warned_mono >= self.WARN_EVERY_SECS:
                logger.warning(
                    "emf_lines_dropped",
                    reason="stdout is not keeping up",
                    dropped=self.dropped - self._warned_count,
                    dropped_total=self.dropped,
                )
                self._warned_mono, self._warned_count = now, self.dropped

    def _run(self) -> None:
        while True:
            try:
                line = self._lines.get(timeout=self.IDLE_SECS)
            except queue.Empty:
                if self.on_idle is not None:
                    with contextlib.suppress(Exception):  # a sink bug must not end the writer
                        self.on_idle()
                continue
            try:
                sys.stdout.write(line + "\n")
                if self._lines.empty():
                    sys.stdout.flush()
            except Exception:  # a closed or broken stdout must not end the writer
                pass
            finally:
                self._lines.task_done()

    def flush(self, timeout_secs: float = 2.0) -> bool:
        """Wait until every queued line is written, at most `timeout_secs`; True when done."""
        deadline = time.monotonic() + timeout_secs
        while self._lines.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.005)
        return not self._lines.unfinished_tasks


_LabelKey = tuple[tuple[str, str], ...]
_EMF_MAX_METRICS = 100  # per line (EMF limit)
_EMF_MAX_VALUES = 100  # per metric value array (EMF limit)


class EmfSink:
    """CloudWatch embedded metric format, added up per label set and written once a second.

    One line per point cost 17.7 us and 1,322 bytes of stdout per call (4 points; R3PF-14), and
    the alert tick wrote one line per gauge (R3OB-11). Now counters are summed, gauges keep
    their last value and samples go into a value array, per label set; each label set is one
    line with all its metrics, written when `flush_secs` has passed (checked on each point, and
    by the default writer's thread when idle), at exit, or by `flush()`. `flush_secs=0` writes
    one line per point. `properties` (pod, version) ride on every line outside `Dimensions`:
    searchable in Logs Insights at no metric cost (R3OB-12).
    """

    def __init__(
        self,
        write: Callable[[str], Any] | None = None,
        namespace: str = NAMESPACE,
        static: Mapping[str, str] | None = None,
        *,
        flush_secs: float = 1.0,
        properties: Mapping[str, str] | None = None,
    ) -> None:
        """`write` receives each line (default: stdout, via a writer thread); `static` labels too.

        A `write` passed in runs on the event loop for every line, so it must not block. The
        default queues each line for a writer thread (EN-20: a backed-up stdout held the loop).
        """
        self._writer: _StdoutWriter | None = None
        self._write: Callable[[str], Any]
        if write is None:
            self._writer = self._write = _StdoutWriter()
            self._writer.on_idle = self._idle
        else:
            self._write = write
        self.namespace = namespace
        self.static = dict(static or {})
        self.properties = dict(properties or {})
        self.flush_secs = flush_secs
        self._lock = threading.Lock()
        self._pending: dict[_LabelKey, dict[str, tuple[str, Any]]] = {}
        self._flushed_mono = time.monotonic()
        self._started = False

    def _line(self, labels: Mapping[str, str], metrics: Mapping[str, tuple[str, Any]]) -> str:
        body: dict[str, Any] = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": self.namespace,
                        "Dimensions": [sorted(labels)],
                        "Metrics": [{"Name": n, "Unit": unit} for n, (unit, _) in metrics.items()],
                    }
                ],
            },
            **self.properties,
        }
        body.update(labels)
        for name, (_, value) in metrics.items():
            body[name] = value
        return orjson.dumps(body).decode()  # compact; 0.8 us vs 6.2 us for json.dumps

    def _emit(self, kind: str, name: str, value: float, labels: Mapping[str, str]) -> None:
        unit = unit_of(name, kind)
        merged = {**self.static, **labels}
        if self.flush_secs <= 0:
            self._write(self._line(merged, {name: (unit, value)}))
            return
        key = tuple(sorted(merged.items()))
        lines: list[str] = []
        with self._lock:
            group = self._pending.setdefault(key, {})
            if kind == "count":
                old = group.get(name)
                group[name] = (unit, (old[1] if old else 0.0) + value)
            elif kind == "observe":
                old = group.get(name)
                values: list[float] = old[1] if old else []
                values.append(value)
                group[name] = (unit, values)
            else:
                group[name] = (unit, value)
            full = len(group) >= _EMF_MAX_METRICS or (
                kind == "observe" and len(group[name][1]) >= _EMF_MAX_VALUES
            )
            if full:
                lines.append(self._line(dict(key), self._pending.pop(key)))
            if time.monotonic() - self._flushed_mono >= self.flush_secs:
                lines += self._drain()
        if self._writer is not None:
            # Its idle tick writes points no later point flushes; checked on every point, so a
            # forked child gets its own thread back (FC-9).
            self._writer.start()
            if not self._started:
                self._started = True
                atexit.register(self.flush)  # after the writer's: runs first at exit
        for line in lines:
            self._write(line)

    def _drain(self) -> list[str]:
        """Lines for every pending label set (lock held)."""
        lines = [self._line(dict(key), group) for key, group in self._pending.items() if group]
        self._pending.clear()
        self._flushed_mono = time.monotonic()
        return lines

    def _idle(self) -> None:
        with self._lock:
            lines = (
                self._drain() if time.monotonic() - self._flushed_mono >= self.flush_secs else []
            )
        for line in lines:
            self._write(line)

    def flush(self) -> None:
        """Write every pending point now."""
        with self._lock:
            lines = self._drain()
        for line in lines:
            self._write(line)

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        """Add to a counter."""
        self._emit("count", name, value, labels)

    def observe(self, name: str, value: float, **labels: str) -> None:
        """A sample (seconds for `_seconds` names)."""
        self._emit("observe", name, value, labels)

    def gauge(self, name: str, value: float, **labels: str) -> None:
        """A gauge point."""
        self._emit("gauge", name, value, labels)

    def remove(self, name: str, **labels: str) -> None:
        """Nothing to remove: EMF lines are points, not state."""


class PrometheusSink:
    """`prometheus_client` metrics (needs the `metrics` extra); serve them with its exposition."""

    def __init__(self, registry: Any = None) -> None:
        """Create metrics lazily, by name and label set."""
        try:
            import prometheus_client
        except ImportError as exc:
            raise MissingExtra("PrometheusSink", "metrics") from exc

        self._prom = prometheus_client
        self.registry = registry or prometheus_client.REGISTRY
        self._made: dict[str, Any] = {}
        self._warned: set[str] = set()

    def _metric(self, kind: str, name: str, labels: Mapping[str, str]) -> Any:
        if name not in self._made:
            spec = spec_of(name) or spec_of(f"{name}_total")
            text = spec.help if spec is not None else name
            made: Any
            try:
                if kind == "observe":
                    buckets = spec.buckets if spec is not None and spec.buckets else None
                    extra: dict[str, Any] = {"buckets": buckets} if buckets else {}
                    made = self._prom.Histogram(
                        name, text, sorted(labels), registry=self.registry, **extra
                    )
                else:
                    cls = self._prom.Counter if kind == "count" else self._prom.Gauge
                    made = cls(name, text, sorted(labels), registry=self.registry)
                self._made[name] = made
            except ValueError:
                # Already registered by another sink in this process (the sync path's engine
                # made a second one): use that collector instead of failing the call (O5).
                # `_names_to_collectors` is private: if a release drops it, the lookup finds
                # nothing and the point is logged and dropped below, never raised (EN-21).
                found = getattr(self.registry, "_names_to_collectors", {}).get(name)
                if found is None:
                    raise
                self._made[name] = found
        metric = self._made[name]
        return metric.labels(**labels) if labels else metric

    def _dropped(self, name: str, exc: Exception) -> None:
        """Log a point that did not fit its metric, once per name: a metric never fails a call."""
        if name not in self._warned:
            self._warned.add(name)
            logger.warning("metric_dropped", metric=name, error=str(exc))

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        """Add to a counter (a point whose labels do not fit is logged and dropped)."""
        try:
            self._metric("count", name.removesuffix("_total"), labels).inc(value)
        except ValueError as exc:  # label names differ from the metric's first use (EN-1)
            self._dropped(name, exc)

    def observe(self, name: str, value: float, **labels: str) -> None:
        """Observe into a histogram (a point whose labels do not fit is logged and dropped)."""
        try:
            self._metric("observe", name, labels).observe(value)
        except ValueError as exc:
            self._dropped(name, exc)

    def gauge(self, name: str, value: float, **labels: str) -> None:
        """Set a gauge (a point whose labels do not fit is logged and dropped)."""
        try:
            self._metric("gauge", name, labels).set(value)
        except ValueError as exc:
            self._dropped(name, exc)

    def remove(self, name: str, **labels: str) -> None:
        """Remove one gauge series, so a scrape stops showing a value nobody sets (R3OB-1)."""
        metric = self._made.get(name)
        if metric is None:
            return
        if not labels:
            # An unlabelled gauge cannot be removed: it stays at its last value.
            return
        names = getattr(metric, "_labelnames", tuple(sorted(labels)))
        # KeyError / ValueError: never set with these labels, or other label names.
        with contextlib.suppress(KeyError, ValueError):
            metric.remove(*(labels[n] for n in names))


def prometheus_text(sink: Metrics | None = None) -> tuple[bytes, str]:
    """The exposition of a `PrometheusSink` (body, content type) for a `/metrics` route.

    Under a forking server (gunicorn prefork) each worker has its own registry: serve this per
    worker process and let Prometheus scrape each one, or use `prometheus_client`'s
    multiprocess mode (docs/how-to/monitor.md).
    """
    try:
        import prometheus_client
    except ImportError as exc:
        raise MissingExtra("prometheus_text", "metrics") from exc
    registry = getattr(sink, "registry", None) or prometheus_client.REGISTRY
    return prometheus_client.generate_latest(registry), prometheus_client.CONTENT_TYPE_LATEST


class GaugeSet:
    """Gauges one process sets on behalf of the cluster, with stale series removed (R3OB-1).

    With Prometheus every pod's `/metrics` kept the values of its last alert-lock win for good:
    an alert left at 1 on one pod after another evaluated it 0, and removed deployments' series
    never went. Each round `gauge()` records what it set; `finish()` removes what the round did
    not set again; `clear()` removes everything (the process lost the alert lock).
    """

    def __init__(self, sink: Metrics) -> None:
        """Wrap `sink`; nothing set yet."""
        self.sink = sink
        self._set: set[tuple[str, _LabelKey]] = set()
        self._round: set[tuple[str, _LabelKey]] = set()

    def gauge(self, name: str, value: float, **labels: str) -> None:
        """Set a gauge and remember its series."""
        self.sink.gauge(name, value, **labels)
        self._round.add((name, tuple(sorted(labels.items()))))

    def _remove(self, series: set[tuple[str, _LabelKey]]) -> None:
        remove = getattr(self.sink, "remove", None)
        if remove is None:
            return
        for name, labels in series:
            remove(name, **dict(labels))

    def finish(self) -> None:
        """End a round: remove the series set before but not in this round."""
        self._remove(self._set - self._round)
        self._set, self._round = self._round, set()

    def clear(self) -> None:
        """Remove every series this process set (another process evaluates now)."""
        self._remove(self._set | self._round)
        self._set, self._round = set(), set()


_SINKS: dict[tuple[str, tuple[tuple[str, str], ...]], Metrics] = {}


def sink_for(
    name: str,
    *,
    static: Mapping[str, str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> Metrics:
    """The process's sink named by the `metrics_sink` setting: `emf`, `prometheus` or `none`.

    One per name (and EMF dimensions) per process: every engine (async queue, sync calls, a
    second queue) shares it. `static` (EMF only): dimensions on every point, such as the
    environment, so two environments in one AWS account are not summed (R3OB-12);
    `properties`: plain fields on every EMF line (pod, version), never dimensions.
    """
    key = (name, tuple(sorted((static or {}).items())))
    if (sink := _SINKS.get(key)) is None:
        if name == "prometheus":
            sink = PrometheusSink()
        elif name == "none":
            sink = NullSink()
        else:
            sink = EmfSink(static=static, properties=properties)
        sink = _SINKS.setdefault(key, sink)
    return sink
