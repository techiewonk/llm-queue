"""Status of models, deployments and requests, read from the stores (plan §3.12).

`StatusReader.model()` reports, per model: waiting calls per class, calls in progress, and per
deployment the RPM / TPM used and left this minute, concurrency limit and use, breaker, health,
latency and throttles. `request()` reports one call. `AlertEvaluator` turns a report into state
alerts, sent as gauges every sample, with a log line once per change (plan §3.18 R32).
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from llm_queue._logging import get_logger
from llm_queue.engine._codec import text as _text
from llm_queue.engine.expiry import native
from llm_queue.engine.fairness import fairness_for, tenant_hash_of
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import StreamQueues
from llm_queue.engine.requests_admin import NO_TENANT as NO_INDEX_TENANT
from llm_queue.errors import ConfigError
from llm_queue.registry.settings import ALERT_DEFAULTS, HEALTH_SUMMARY_DEFAULTS

if TYPE_CHECKING:
    import redis.asyncio

    from llm_queue.engine.requests_admin import RequestsAdmin
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
        self,
        client: redis.asyncio.Redis,
        keys: Keys,
        queues: StreamQueues,
        queue_prefix: str = "",
        *,
        requests: RequestsAdmin | None = None,
        request_index: bool = True,
        scan_max: int = 100_000,
    ) -> None:
        """Keep the clients; every report reads the store fresh.

        `requests` (the engine's `RequestsAdmin`) counts one tenant's calls for
        `model(..., tenant_hash=...)`; `request_index` says whether calls are indexed
        (`LLMQ_REQUEST_INDEX_ENABLED`); `scan_max` caps the tenants read per model.
        """
        self._client = client
        self.keys = keys
        self.queues = queues
        self.queue_prefix = queue_prefix
        self.requests = requests
        self.request_index = request_index
        self.scan_max = scan_max
        self._model_status = client.register_script(_MODEL_STATUS)

    async def model(
        self, view: ResolvedRegistry, alias: str, *, tenant_hash: str | None = None
    ) -> dict[str, Any]:
        """One model: queues, calls in progress, and every deployment's numbers.

        All deployment reads are one script (20 x 30 took 6.5 s one by one; now 47 ms, B91).
        With `tenant_hash` (`-`: no tenant) a `tenant` block adds that tenant's calls.
        """
        # `tenants` counts the tenants with calls in flight and their indexed calls (0.2.0 T4,
        # hashes never shown); the `tenant` block also has the tenant's fairness numbers.
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
        report["health"] = model_health(view, report)
        report["request_index"] = self.request_index
        report["tenants"] = await self._tenants(alias, now_secs)
        if tenant_hash is not None:
            report["tenant"] = await self.tenant(view, alias, tenant_hash, now_secs)
        return report

    async def _tenants(self, alias: str, now_secs: int) -> dict[str, int]:
        """Tenants with calls in flight on the model and their indexed calls (counts only)."""
        now_ms = now_secs * 1000
        hashes = await self._client.zrangebyscore(
            self.keys.tenant_index(alias), now_ms, "+inf", start=0, num=self.scan_max
        )
        calls = 0
        if hashes:
            async with self._client.pipeline(transaction=False) as pipe:
                for h in hashes:
                    pipe.zcount(self.keys.request_index(alias, _text(h)), now_ms, "+inf")
                calls = sum(int(n) for n in await pipe.execute())
        return {"active": len(hashes), "indexed_calls": calls}

    async def tenant(
        self, view: ResolvedRegistry, alias: str, tenant_hash: str, now_secs: int | None = None
    ) -> dict[str, Any]:
        """One tenant on one model (T4): its calls by stage, oldest wait, fairness numbers.

        `tenant` is the hash (`tenant_name` only with `LLMQ_TENANT_NAMES_ENABLED`); `waiting`
        counts every waiting stage, `held` the waiting ones a worker holds for budget.
        Unknown tenant: zeros. Needs the engine's `RequestsAdmin` (`requests=`).
        """
        if self.requests is None:
            raise ConfigError("a tenant's status needs the request index service (requests=)")
        counts = await self.requests.count(alias, tenant_hash=tenant_hash)
        out: dict[str, Any] = {
            "tenant": tenant_hash,
            "waiting": counts.by_state["waiting"],
            "held": counts.by_stage["held"],
            "running": counts.by_state["running"],
            "cancelling": counts.cancelling,
            "oldest_wait_secs": counts.oldest_wait_secs,
            "truncated": counts.truncated,
        }
        if self.requests.settings.tenant_names:
            name = await self._client.hget(self.keys.tenant_names(alias), tenant_hash)
            out["tenant_name"] = _text(name) if name is not None else None
        setting = view.value(alias, "fairness")
        if setting.get("mode", "off") != "off":
            if now_secs is None:
                now_secs = int((await self._client.time())[0])
            # Calls with no tenant are `-` in the index; fairness counts those with no user
            # either under the hash of its no-tenant name (RQ-11: `-` always read defaults).
            fair_h = tenant_hash
            if tenant_hash == NO_INDEX_TENANT:
                fair_h = tenant_hash_of(None, None, self.requests.hash_id)
            out["fairness"] = await self._tenant_fairness(view, alias, fair_h, now_secs)
        return out

    async def _tenant_fairness(
        self, view: ResolvedRegistry, alias: str, h: str, now_secs: int
    ) -> dict[str, Any]:
        """The tenant's fairness numbers: weight (share), quota and its use, slots, activity."""
        setting = view.value(alias, "fairness")
        window = str(view.value(alias, "rate_window"))
        fair = fairness_for(setting, h, window=window)
        model = self.keys.model
        now_ms = now_secs * 1000
        async with self._client.pipeline(transaction=False) as pipe:
            pipe.hget(model(alias, "tf:w"), h)
            pipe.hmget(model(alias, f"tf:t:{h}"), ["r_win", "r_n", "t_win", "t_n"])
            pipe.zcount(model(alias, f"tf:s:{h}"), now_ms, "+inf")
            pipe.zscore(model(alias, "tf:act"), h)
            weight, (r_win, r_n, t_win, t_n), slots, seen = await pipe.execute()
        minute = now_secs // 60
        used = None
        if window == "fixed":
            used = {
                "calls": int(_num(r_n)) if _num(r_win, -1) == minute else 0,
                "tokens": int(_num(t_n)) if _num(t_win, -1) == minute else 0,
            }
        active_ms = int(float(setting.get("active_secs", 60)) * 1000)
        return {
            "mode": setting["mode"],
            "weight": _num(weight) if weight is not None else (fair.weight if fair else 1.0),
            "quota": {"rpm": fair.rpm if fair else None, "tpm": fair.tpm if fair else None},
            "used_this_minute": used,
            "slots_in_use": int(slots),
            "active": seen is not None and float(seen) >= now_ms - active_ms,
        }

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
                # The provider quota behind the budget (`quota_rpm` x `budget_pct`), or None.
                "quota": view.quota(alias, "rpm", dep),
            },
            "tpm": {
                "limit": tpm,
                "used": used_tokens,
                "left": None if tpm is None else max(0, tpm - used_tokens),
                "quota": view.quota(alias, "tpm", dep),
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
            "health": worst_health(m["health"]["state"] for m in models),
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


HEALTH_STATES = ("healthy", "warning", "critical")  # best to worst


def worst_health(states: Iterable[str]) -> str:
    """The worst of `states` (`healthy` when there are none)."""
    return max(states, key=HEALTH_STATES.index, default="healthy")


def _usable(dep: Mapping[str, Any]) -> bool:
    """The alert rule's usable deployment: enabled, unblocked, healthy, inside residency."""
    return bool(
        dep["enabled"]
        and dep["account_enabled"]
        and not dep["blocked"]
        and dep["healthy"]
        and not dep.get("outside_residency", False)
        and dep["breaker"] != "open"
    )


def _budget_used_pct(deps: list[Mapping[str, Any]]) -> float | None:
    """This minute's use of the usable deployments' budgets, the fuller of rpm and tpm (%)."""
    worst: float | None = None
    for budget in ("rpm", "tpm"):
        limit = sum(d[budget]["limit"] or 0 for d in deps if d[budget].get("limit"))
        if limit:
            used = sum(d[budget]["used"] for d in deps if d[budget].get("limit"))
            pct = 100.0 * used / limit
            worst = pct if worst is None else max(worst, pct)
    return None if worst is None else round(worst, 1)


def model_health(view: ResolvedRegistry, report: Mapping[str, Any]) -> dict[str, Any]:
    """A model's health summary (0.2.0 F2): `healthy`, `warning` or `critical`, and why.

    From numbers the report already has, against the model's `health_summary` thresholds:
    critical when no deployment is usable (an enabled model), or budget use, waiting calls or
    the oldest call's wait reach their critical mark; warning at the warning marks, and for an
    open breaker, an auto-disabled deployment, dead letters (`alerts.dlq_messages`), the kill
    switch or a disabled model. `reasons` names each rule that fired.
    """
    alias = report["model"]
    marks = {**HEALTH_SUMMARY_DEFAULTS, **view.value(alias, "health_summary")}
    dlq_mark = view.value(alias, "alerts").get("dlq_messages", ALERT_DEFAULTS["dlq_messages"])
    deps: list[Mapping[str, Any]] = report["deployments"]
    usable = [d for d in deps if _usable(d)]
    used = _budget_used_pct(usable)
    waiting = sum(report["waiting"].values()) + sum(report.get("waiting_for_grant", {}).values())
    oldest = report.get("oldest_waiting_secs")
    critical: list[str] = []
    warning: list[str] = []
    if not report["enabled"]:
        warning.append("model_disabled")
    elif not usable:
        critical.append("no_usable_deployment")
    for name, value, kind in (
        ("budget_used", used, "pct"),
        ("waiting", waiting, ""),
        ("oldest_wait", oldest, "secs"),
    ):
        key = {"pct": "budget_{}_pct", "": "waiting_{}", "secs": "wait_{}_secs"}[kind]
        if value is None:
            continue
        if value >= marks[key.format("critical")]:
            critical.append(name)
        elif value >= marks[key.format("warning")]:
            warning.append(name)
    if any(d["breaker"] == "open" for d in deps):
        warning.append("breaker_open")
    if any(not d["healthy"] for d in deps):
        warning.append("deployment_disabled")
    if report.get("dead_letters", 0) >= dlq_mark:
        warning.append("dead_letters")
    if view.registry.routing.kill_switch:
        warning.append("kill_switch")
    state = "critical" if critical else "warning" if warning else "healthy"
    return {
        "state": state,
        "reasons": critical + warning,
        "budget_used_pct": used,
        "waiting": waiting,
        "oldest_waiting_secs": oldest,
        "usable_deployments": len(usable),
        "deployments": len(deps),
    }


def health_report(view: ResolvedRegistry, report: Mapping[str, Any]) -> dict[str, Any]:
    """The `status.health` answer: overall state and one summary row per model."""
    rows = [
        {"model": m["model"], **(m.get("health") or model_health(view, m))}
        for m in report["models"]
    ]
    return {
        "health": worst_health(r["state"] for r in rows),
        "registry_version": report.get("registry_version"),
        "models": rows,
    }


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
        self.expiry = native(keys.prefix)  # the store's mode, set by the runtime at start
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

        `model_unavailable`, `quota_near_limit`, `breaker_open`, `kill_switch_on` and
        `queue_stalled`; docs/how-to/monitor.md says when each fires.
        """
        # model_unavailable: no deployment enabled, unblocked, healthy, in residency with a
        # closed breaker. quota_near_limit: use this minute at or above `alerts.quota_used_pct`
        # (off after `CLEAR_SAMPLES` clear samples). queue_stalled: oldest call older than
        # `alerts.queue_age_secs` (R3OB-17).
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
                step = 1
                for alert in alerts[start : start + _ALERT_BATCH]:
                    pipe.set(_marker(self.keys, alert), _state(alert), get=True)
                    step = 1 + self.expiry.expire(pipe, _marker(self.keys, alert), 86_400)
                previous += (await pipe.execute())[::step]  # the SET replies
        for alert, before in zip(alerts, previous, strict=True):
            state = _state(alert)
            if (_text(before) if before is not None else "0") != state:
                flipped.append(alert)
                log = logger.error if alert.on else logger.info
                log(
                    "llmq_alert", alert=alert.name, on=alert.on, detail=alert.detail, **alert.labels
                )
        return flipped
