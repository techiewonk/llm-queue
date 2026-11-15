"""Status of models, deployments and requests, read from the stores (plan §3.12).

`StatusReader.model()` reports, per model: waiting calls per class, calls in progress, and per
deployment the RPM / TPM used and left this minute, concurrency limit and use, breaker, health,
latency and throttles. `request()` reports one call. `AlertEvaluator` turns a report into state
alerts, sent as gauges every sample, with a log line once per change (plan §3.18 R32).
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from llm_queue._logging import get_logger
from llm_queue.engine._codec import text as _text
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import StreamQueues
from llm_queue.registry.settings import ALERT_DEFAULTS

if TYPE_CHECKING:
    import redis.asyncio

    from llm_queue.registry import ResolvedRegistry

logger = get_logger(__name__)

# One model's numbers in one command: TIME, calls in progress, health, and per deployment its
# rate hash, limiter, slots in use, breaker state and throttles this minute. As ~180 separate
# commands per model, client-side packing and parsing made a 600-deployment report 140 ms+.
# KEYS: none. ARGV: model key prefix (`<p>:{m:<alias>}:`), deployment names...
_MODEL_STATUS = """
local base = ARGV[1]
local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000
local minute = math.floor(tonumber(t[1]) / 60)
local out = {t[1], redis.call('ZCOUNT', base .. 'inprog', now_ms, '+inf'),
             redis.call('HGETALL', base .. 'health')}
for i = 2, #ARGV do
  local dep = ARGV[i]
  table.insert(out, {
    redis.call('HGETALL', base .. 'rate:' .. dep),
    redis.call('HGET', base .. 'ac:' .. dep, 'limit') or '',
    redis.call('ZCOUNT', base .. 'lease:' .. dep .. ':p', now_ms, '+inf')
      + redis.call('ZCOUNT', base .. 'lease:' .. dep .. ':n', now_ms, '+inf'),
    redis.call('HGET', base .. 'cb:' .. dep, 'state') or 'closed',
    redis.call('GET', base .. 'thr:' .. dep .. ':' .. minute) or '0',
    redis.call('GET', base .. 'thr:' .. dep .. ':' .. (minute - 1)) or '0',
  })
end
return out
"""


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(_text(value))
    except (TypeError, ValueError):
        return default


class StatusReader:
    """Read-only view of one environment's stores."""

    def __init__(
        self, client: redis.asyncio.Redis, keys: Keys, queues: StreamQueues, queue_prefix: str = ""
    ) -> None:
        """Keep the clients; every report reads the store fresh."""
        self._client = client
        self.keys = keys
        self.queues = queues
        self.queue_prefix = queue_prefix
        self._model_status = client.register_script(_MODEL_STATUS)

    async def model(self, view: ResolvedRegistry, alias: str) -> dict[str, Any]:
        """One model: queues, calls in progress, and every deployment's numbers.

        All deployment reads are one script (one command per model): read one by one, 20
        models x 30 deployments took 6.5 s (plan B91 asks for 200 ms; now 47 ms).
        """
        name = self.queue_prefix + view.queue_name(alias)
        deps = view.deployments(alias)
        base = self.keys.model(alias, "")
        names = (f"{name}-priority", name)
        depths, raw, firsts = await asyncio.gather(
            asyncio.gather(*(self.queues.depth(n) for n in names)),
            self._model_status([], [base, *(d.key for d in deps)]),
            asyncio.gather(*(self._first_id(n) for n in names)),
        )
        classes = ("priority", "normal")
        waiting = {
            cls: max(0, total - held) for cls, (total, held, _) in zip(classes, depths, strict=True)
        }
        # Taken by a worker but still waiting for a grant: counted nowhere before, so a model
        # out of budget showed 0 waiting with up to worker_slots x pods calls held (R3OB-2).
        held_waiting = {cls: held for cls, (_, held, _) in zip(classes, depths, strict=True)}
        dead = sum(letters for _, _, letters in depths)
        now_secs = int(raw[0])
        minute = now_secs // 60
        # The oldest call in either queue (an acked message leaves the stream): a queue whose
        # oldest call keeps ageing is stalled (R3OB-17).
        oldest_ms = min((ms for ms in firsts if ms is not None), default=None)
        inprog, flat = raw[1], raw[2]
        health = {_text(flat[i]): _text(flat[i + 1]) for i in range(0, len(flat), 2)}
        deployments = [
            self._deployment(view, alias, dep, raw[3 + i], health, minute)
            for i, dep in enumerate(deps)
        ]
        report: dict[str, Any] = {
            "model": alias,
            "enabled": bool(view.effective(alias, "enabled").value),
            "waiting": waiting,
            "waiting_for_grant": held_waiting,
            "oldest_waiting_secs": None
            if oldest_ms is None
            else round(max(0.0, now_secs - oldest_ms / 1000), 1),
            "in_progress": int(inprog),
            "dead_letters": dead,
            "deployments": deployments,
        }
        fairness = view.value(alias, "fairness")
        if fairness.get("mode", "off") != "off":
            report["fairness"] = await self._fairness(alias, fairness, now_secs)
        return report

    async def _fairness(
        self, alias: str, setting: Mapping[str, Any], now_secs: int
    ) -> dict[str, Any]:
        """Tenant fairness of one model, as counts only (never a tenant or its hash).

        `active_tenants`: tenants that asked within `active_secs`; `waiting_within_share`:
        refused calls of tenants within their share, which others may not borrow ahead of.
        Read only when the mode is on: one pipelined round trip.
        """
        now_ms = now_secs * 1000
        active_ms = int(float(setting.get("active_secs", 60)) * 1000)
        async with self._client.pipeline(transaction=False) as pipe:
            pipe.zcount(self.keys.model(alias, "tf:act"), now_ms - active_ms, "+inf")
            pipe.zcount(self.keys.model(alias, "tf:need"), now_ms, "+inf")
            active, need = await pipe.execute()
        return {
            "mode": setting["mode"],
            "active_tenants": int(active),
            "waiting_within_share": int(need),
        }

    async def _first_id(self, queue: str) -> int | None:
        """The time (ms) in the id of a queue's oldest message, None when it is empty."""
        client = self.queues.client
        entries = await client.xrange(self.queues.keys.queue(queue, "s"), count=1)
        if not entries:
            return None
        return int(_text(entries[0][0]).split("-", 1)[0])

    def _deployment(
        self,
        view: ResolvedRegistry,
        alias: str,
        deployment: Any,
        replies: list[Any],
        health: Mapping[str, str],
        minute: int,
    ) -> dict[str, Any]:
        flat_rate, raw_limit, in_use, breaker, throttles, throttles_before = replies
        dep = deployment.key
        rate = {_text(flat_rate[i]): _text(flat_rate[i + 1]) for i in range(0, len(flat_rate), 2)}
        fresh = rate.get("seen_win") == str(minute)
        used_calls = int(_num(rate.get("seen_calls"))) if fresh else 0
        used_tokens = int(_num(rate.get("seen_tokens"))) if fresh else 0
        rpm = view.value(alias, "rpm", dep)
        tpm = view.value(alias, "tpm", dep)
        cap = view.value(alias, "max_concurrency", dep) or 200
        limit = _num(raw_limit, cap) if _text(raw_limit) else cap
        disabled_until = health.get(f"{dep}|until")
        routing = view.registry.routing
        return {
            "name": dep,
            "group": deployment.group,
            "enabled": bool(view.value(alias, "enabled", dep)),
            "account_enabled": view.registry.accounts[deployment.account].enabled,
            "blocked": deployment.region in routing.blocked_regions
            or deployment.account in routing.blocked_accounts,
            "healthy": disabled_until is None,
            "next_probe": _num(disabled_until) / 1000 if disabled_until else None,
            "outside_residency": view.outside_residency(deployment),
            "rpm": {
                "limit": rpm,
                "used": used_calls,
                "left": None if rpm is None else max(0, rpm - used_calls),
            },
            "tpm": {
                "limit": tpm,
                "used": used_tokens,
                "left": None if tpm is None else max(0, tpm - used_tokens),
            },
            "concurrency": {"limit": round(limit, 2), "in_use": int(in_use)},
            "breaker": _text(breaker),
            "latency_ms": round(_num(health.get(f"{dep}|lat")), 1)
            if f"{dep}|lat" in health
            else None,
            "samples": int(_num(health.get(f"{dep}|n"))),
            # The larger of the last full minute and this one: a sample early in the minute read
            # near 0, so the gauge and `quota_above_observed` flipped at each rollover (R3OB-6).
            "throttles_1m": int(max(_num(throttles), _num(throttles_before))),
            "throttles_this_minute": int(_num(throttles)),
        }

    async def jobs(self) -> list[dict[str, Any]]:
        """Every job name a worker consumed lately: waiting jobs and dead letters (Q15).

        Names come from the set the job loops keep (`JobRunner.names_key`); dead jobs had no
        place in `status()` and no alert before.
        """
        key = f"{self.queues.keys.prefix}:jobs:{self.queue_prefix}"
        names = sorted(_text(n) for n in await self.queues.client.smembers(key))
        depths = await asyncio.gather(
            *(self.queues.depth(f"{self.queue_prefix}jobs-{name}") for name in names)
        )
        return [
            {"job": name, "waiting": max(0, total - held), "dead_letters": dead}
            for name, (total, held, dead) in zip(names, depths, strict=True)
        ]

    async def report(self, view: ResolvedRegistry) -> dict[str, Any]:
        """Every model and job, the registry version and the routing switches."""
        models, jobs = await asyncio.gather(
            asyncio.gather(*(self.model(view, a) for a in view.registry.models)), self.jobs()
        )
        routing = view.registry.routing
        return {
            "registry_version": view.registry.version,
            "kill_switch": routing.kill_switch,
            "blocked_regions": list(routing.blocked_regions),
            "blocked_accounts": list(routing.blocked_accounts),
            "models": list(models),
            "jobs": jobs,
        }

    async def request(self, request_id: str) -> dict[str, Any]:
        """One call: its status, and its in-progress entry while it runs."""
        status = {
            _text(k): _text(v)
            for k, v in (await self._client.hgetall(self.keys.status(request_id))).items()
        }
        if not status:
            return {}
        model = status.get("served_model") or status.get("model")
        if model and status.get("state") == "waiting":
            entry = await self._client.hgetall(self.keys.model(model, f"inprog:{request_id}"))
            if entry:
                fields = {_text(k): _text(v) for k, v in entry.items()}
                status["state"] = "in_progress"
                status["deployment"] = fields.get("dep", "")
        return {"request_id": request_id, **status}


@dataclass(frozen=True, slots=True)
class Alert:
    """One alert state: on or off, with its details."""

    name: str
    on: bool
    labels: dict[str, str] = field(default_factory=dict)
    detail: str = ""


_ALERT_BATCH = 500  # alert markers per pipeline


def _marker(keys: Keys, alert: Alert) -> str:
    """The store key holding an alert's last state."""
    labels = ",".join(f"{k}={v}" for k, v in sorted(alert.labels.items()))
    return f"{keys.prefix}:alert:{alert.name}:{labels}"


def _state(alert: Alert) -> str:
    return "1" if alert.on else "0"


# Alerts read from per-minute counters: on at once, off only after this many clear samples in
# a row, so a saturated deployment does not flip at each minute rollover (R3OB-6).
_STEADY = frozenset({"quota_near_limit", "quota_above_observed"})
CLEAR_SAMPLES = 2


class AlertEvaluator:
    """State alerts from a report (thresholds from the model's `alerts` setting).

    Gauges are returned every time; `changed()` logs a line only when a state flips, with the
    store marker deciding which process logs it.
    """

    def __init__(self, client: redis.asyncio.Redis, keys: Keys) -> None:
        """Keep the client for the change markers."""
        self._client = client
        self.keys = keys
        self._clear_runs: dict[str, int] = {}  # steady alert marker -> clear samples in a row
        self._on: dict[str, Alert] = {}  # alerts on at the last `changed()`, by marker

    def _steady(self, alert: Alert) -> Alert:
        """Hold a per-minute alert on until `CLEAR_SAMPLES` clear samples in a row."""
        if alert.name not in _STEADY:
            return alert
        marker = _marker(self.keys, alert)
        if alert.on:
            self._clear_runs[marker] = 0
            return alert
        runs = self._clear_runs.get(marker)
        if runs is None:  # not on here
            return alert
        runs += 1
        if runs >= CLEAR_SAMPLES:
            del self._clear_runs[marker]
            return alert
        self._clear_runs[marker] = runs
        return Alert(alert.name, True, alert.labels, f"{alert.detail} (clearing)")

    def evaluate(self, view: ResolvedRegistry, report: Mapping[str, Any]) -> list[Alert]:
        """Every alert state of a report.

        `model_unavailable`: no deployment is enabled (itself and its account), unblocked,
        healthy, inside the residency list and with its breaker not open. `quota_near_limit`:
        RPM or TPM use this minute at or above `alerts.quota_used_pct` (off after
        `CLEAR_SAMPLES` clear samples). `breaker_open`, `kill_switch_on` and `queue_stalled`
        (the oldest call older than `alerts.queue_age_secs`) were missing (R3OB-17).
        """
        alerts: list[Alert] = []
        kill_switch = bool(report.get("kill_switch"))
        for model in report["models"]:
            alias = model["model"]
            thresholds = view.value(alias, "alerts")
            deps = model["deployments"]
            usable = [
                d
                for d in deps
                if d["enabled"]
                and d["account_enabled"]
                and not d["blocked"]
                and d["healthy"]
                and not d["outside_residency"]
                and d["breaker"] != "open"
            ]
            alerts.append(Alert("model_unavailable", not usable, {"model": alias}))
            alerts.append(Alert("kill_switch_on", kill_switch, {"model": alias}))
            oldest = model.get("oldest_waiting_secs")
            age_limit = thresholds.get("queue_age_secs", ALERT_DEFAULTS["queue_age_secs"])
            alerts.append(
                Alert(
                    "queue_stalled",
                    oldest is not None and oldest >= age_limit,
                    {"model": alias},
                    f"oldest call {oldest} s",
                )
            )
            for dep in deps:
                labels = {"model": alias, "deployment": dep["name"]}
                alerts.append(Alert("deployment_disabled", not dep["healthy"], labels))
                alerts.append(Alert("breaker_open", dep["breaker"] == "open", labels))
                used = {
                    budget: 100 * dep[budget]["used"] / dep[budget]["limit"]
                    for budget in ("rpm", "tpm")
                    if dep[budget]["limit"]
                }
                worst = max(used, key=used.__getitem__, default="rpm")
                used_pct = used.get(worst, 0.0)
                alerts.append(
                    Alert(
                        "quota_near_limit",
                        used_pct >= thresholds["quota_used_pct"],
                        labels,
                        f"{worst} {used_pct:.0f}%",
                    )
                )
                limit = dep["rpm"]["limit"]
                quota_left = limit is None or dep["rpm"]["left"] > 0
                alerts.append(
                    Alert(
                        "quota_above_observed",
                        dep["throttles_1m"] > 0 and quota_left,
                        labels,
                        f"{dep['throttles_1m']} throttles with budget left",
                    )
                )
            alerts.append(
                Alert(
                    "dead_letters",
                    model["dead_letters"] >= thresholds["dlq_messages"],
                    {"model": alias},
                )
            )
        for job in report.get("jobs", ()):
            # Any dead job is worth a look: it ran out of retries (Q15).
            alerts.append(
                Alert(
                    "job_dead_letters",
                    job["dead_letters"] > 0,
                    {"job": job["job"]},
                    f"{job['dead_letters']} dead",
                )
            )
        return [self._steady(alert) for alert in alerts]

    async def changed(self, alerts: list[Alert]) -> list[Alert]:
        """Alerts whose state flipped since the last evaluation (logged once, by one process).

        An alert that was on here and left the report (its model or deployment was removed) is
        turned off and logged: its marker used to expire after 24 h with no "off" line.
        """
        markers = {_marker(self.keys, alert) for alert in alerts}
        gone = [
            Alert(a.name, False, a.labels, "left the registry")
            for marker, a in self._on.items()
            if marker not in markers
        ]
        alerts = [*alerts, *gone]
        self._on = {_marker(self.keys, a): a for a in alerts if a.on}
        flipped = []
        # One pipelined `SET ... EX GET` per alert: it was GETSET then EXPIRE, two serial round
        # trips each (~3,700 per sample at 20 models x 30 deployments; GETSET is deprecated
        # since Redis 6.2) (EN-9).
        previous: list[Any] = []
        for start in range(0, len(alerts), _ALERT_BATCH):
            async with self._client.pipeline(transaction=False) as pipe:
                for alert in alerts[start : start + _ALERT_BATCH]:
                    pipe.set(_marker(self.keys, alert), _state(alert), ex=86_400, get=True)
                previous += await pipe.execute()
        for alert, before in zip(alerts, previous, strict=True):
            state = _state(alert)
            if (_text(before) if before is not None else "0") != state:
                flipped.append(alert)
                log = logger.error if alert.on else logger.info
                log(
                    "llmq_alert", alert=alert.name, on=alert.on, detail=alert.detail, **alert.labels
                )
        return flipped
