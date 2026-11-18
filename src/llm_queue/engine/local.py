"""Local execution: every call runs in this process, straight against the provider (0.2.0).

`LocalExecutor` runs one call the way a queue worker does, with this process's own state in
place of the store: token buckets per deployment (and account shared budgets), each the
registry's rpm / tpm divided by `LLMQ_LOCAL_PROCESS_COUNT` (GCRA `smooth` or `fixed` minutes,
as the admission script); a per-model cap of `LLMQ_LOCAL_CONCURRENCY` calls in flight, priority
calls first; breakers, throttle backoff, target-error health and failed credentials per
deployment in memory; the engine's attempt rules (retries, backoff, attempt and call timeouts,
`max_wait_secs`), region and model fallback, sticky sessions and streaming.

It serves two paths: `LocalEngine` (`LLMQ_EXECUTION_MODE=local`: no store client is ever
built, the registry is the process's own document) and the queue engine's
`store_down: static_cap`. Store-only operations of a local engine raise `LocalModeRefused`.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import random
import threading
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, NoReturn

from llm_queue._logging import get_logger
from llm_queue._version import __version__
from llm_queue.clients.pool import CredentialsRefreshSlow, CredentialsUnavailable, route_for
from llm_queue.clients.timing import AttemptTimeout, Timing
from llm_queue.engine import hooks, inference, shapes, tokens
from llm_queue.engine.envelope import Envelope, Meta, Reply
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.metrics import TENANT_NONE, Metrics, NullSink, TenantLabels
from llm_queue.engine.runtime import (
    EngineSettings,
    _api_kwargs,
    _dep_takes_calls,
    _meta_event,
    _served_op,
    _takes_calls,
    build_envelope,
    estimate_of,
    log_outcome,
    may_spill,
    request_params,
    shape_ok,
)
from llm_queue.engine.status import model_health, worst_health
from llm_queue.engine.streaming import text_of
from llm_queue.errors import ConfigError, ErrorKind, LocalModeRefused
from llm_queue.ids import keyed_hash, new_id
from llm_queue.providers.classify import (
    error_family,
    is_auth_error,
    is_target_error,
    request_error_kind,
)
from llm_queue.registry.resolve import ResolvedRegistry
from llm_queue.registry.validate import RegistryError, check, tpm_refuses
from llm_queue.tuning import EngineTimings

if TYPE_CHECKING:
    from llm_queue.clients.providers import Providers
    from llm_queue.engine.purge import PurgeService
    from llm_queue.registry import Deployment
    from llm_queue.registry.store import PublishResult

logger = get_logger(__name__)

Sink = Callable[[Any], Awaitable[None]]
_DONE: dict[str, Any] = {"t": "call_done"}  # the marker a local stream's queue ends with
_STOP_WAIT_SECS = 5.0  # after the drain: how long `stop()` waits for the calls it cancelled


# ================================================================ budgets


class _Bucket:
    """One budget of one deployment (or account) in this process: GCRA or a minute counter.

    The admission script's rules on wall seconds (`smooth` GCRA burst, `fixed` per minute);
    `limit` is per process, so it may be below 1 (L-2).
    """

    # `smooth` lets `max(1, floor(limit x rate_burst))` units through, then one every
    # `60 / limit` s. Below 1, one unit still passes in an empty minute, and `fixed` carries
    # what went over into the next minutes as a debt: no process exceeds its share on average.

    __slots__ = ("burst", "limit", "mode", "n", "tat", "used", "used_min", "win")

    def __init__(self, limit: float, burst: float, mode: str) -> None:
        self.limit, self.burst, self.mode = limit, burst, mode
        self.tat = 0.0  # smooth: the theoretical arrival time
        self.win, self.n = -1, 0.0  # fixed: the minute and its count
        self.used_min, self.used = -1, 0.0  # units taken this minute (status)

    def configure(self, limit: float, burst: float, mode: str) -> None:
        """Follow a reloaded registry; a new window mode starts empty."""
        if mode != self.mode:
            self.tat, self.win, self.n = 0.0, -1, 0.0
        self.limit, self.burst, self.mode = limit, burst, mode

    def _size(self) -> int:
        return max(1, math.floor(self.limit * self.burst))

    def wait(self, cost: float, now: float) -> float:
        """Seconds until `cost` fits (0: it fits now)."""
        if self.limit <= 0:
            return 60.0
        if self.mode == "fixed":
            minute = int(now // 60)
            n = self._carried(minute)
            if n + cost <= self.limit or (n == 0 and cost > self.limit):
                return 0.0
            # Whole minutes until the count (debt paid off by `limit` a minute) lets it in.
            over = n if cost > self.limit else n + cost - self.limit
            return (minute + max(1, math.ceil(over / self.limit))) * 60 - now
        per = 60.0 / self.limit
        tat = max(self.tat, now)
        if cost > self._size():
            return 0.0 if tat <= now else tat - now
        # `tat - now` first, as the script: summed from `tat`, an idle bucket of one rounded
        # above 0 for about half of all limits and never let a call in.
        return max(0.0, (tat - now) + (cost - self._size()) * per)

    def take(self, cost: float, now: float) -> None:
        """Charge `cost` (after `wait` said it fits)."""
        self._count(cost, now)
        if self.mode == "fixed":
            minute = int(now // 60)
            self.n = self._carried(minute) + cost
            self.win = minute
            return
        self.tat = max(self.tat, now) + cost * 60.0 / self.limit

    def _carried(self, minute: int) -> float:
        """`fixed`: the count of `minute`; a later minute starts with what is still owed."""
        if self.win == minute:
            return self.n
        if self.win < 0 or minute < self.win or self.limit <= 0:
            return 0.0
        return max(0.0, self.n - self.limit * (minute - self.win))

    def give(self, units: float, now: float) -> None:
        """Give back `units` (negative: charge more), as FINISH settles a grant."""
        if units == 0 or self.limit <= 0:
            return
        self._count(-units, now)
        if self.mode == "fixed":
            minute = int(now // 60)
            if self.win == minute:
                self.n = max(0.0, self.n - units)
            elif units < 0:  # an overrun is charged to the new minute (on top of any debt)
                self.win, self.n = minute, self._carried(minute) - units
            return
        value = max(self.tat, now) - units * 60.0 / self.limit
        self.tat = max(value, now) if units > 0 else value

    def left(self, now: float) -> float:
        """Units that fit now."""
        if self.limit <= 0:
            return 0.0
        if self.mode == "fixed":
            return max(0.0, self.limit - self._carried(int(now // 60)))
        per = 60.0 / self.limit
        return max(0.0, self._size() - (max(self.tat, now) - now) / per)

    def used_now(self, now: float) -> int:
        """Units taken this minute (after refunds)."""
        return max(0, round(self.used)) if self.used_min == int(now // 60) else 0

    def _count(self, units: float, now: float) -> None:
        minute = int(now // 60)
        if self.used_min != minute:
            self.used_min, self.used = minute, 0.0
        self.used = max(0.0, self.used + units)


@dataclass(frozen=True, slots=True)
class _Budget:
    """One bucket a grant charges: its key, per-process limit, burst, mode and cost kind."""

    key: tuple[str, ...]
    limit: float
    burst: float
    mode: str
    kind: Literal["r", "t"]


@dataclass(slots=True)
class _Breaker:
    """A deployment's breaker and throttle backoff in this process (the FINISH rules)."""

    state: str = "closed"
    failures: int = 0
    fail_at: float = 0.0
    until: float = 0.0  # open until (monotonic)
    probing: bool = False  # a half-open probe is in flight
    thr_n: int = 0  # throttles in a row
    thr_until: float = 0.0  # skipped until (monotonic)


@dataclass(slots=True)
class _Health:
    """Latency, samples, throttles per minute and target errors of one deployment."""

    latency_ms: float | None = None
    samples: int = 0
    throttles: dict[int, int] = field(default_factory=dict)  # minute -> throttles
    target_errors: deque[float] = field(default_factory=deque)
    disabled_until: float = 0.0  # monotonic; 0: enabled


@dataclass(frozen=True, slots=True)
class _Backoff:
    """Credentials that failed for one account and region: unused until `until`."""

    until: float
    secs: float


class LocalLimits:
    """The local executor's state of one process: budgets, breakers, health, sticky, retries.

    Shared by every executor of the process (the started engine and the private engine of
    the sync calls), so a process has one budget, one concurrency cap per model and one
    breaker probe per deployment whichever path its calls take. Plain data under one thread
    lock: no event loop owns it.
    """

    def __init__(
        self,
        *,
        process_count: int = 1,
        concurrency: int = 10,
        timings: EngineTimings | None = None,
    ) -> None:
        """`process_count` divides every rpm / tpm; `concurrency` caps calls per model."""
        self.process_count = max(1, int(process_count))
        self.concurrency = max(1, int(concurrency))
        self.timings = timings or EngineTimings()
        self._slots: dict[str, _PrioritySlots] = {}
        self.lock = threading.Lock()
        self.buckets: dict[tuple[str, ...], _Bucket] = {}
        self.breakers: dict[tuple[str, str], _Breaker] = {}
        self.health: dict[tuple[str, str], _Health] = {}
        self.bad: dict[tuple[str, str], _Backoff] = {}
        self.sticky: dict[tuple[str, str], tuple[str, float]] = {}
        self.turn: dict[str, int] = {}  # round_robin position per model
        self.retries: dict[tuple[str, int], list[int]] = {}  # (model, minute) -> [retries, firsts]
        self.in_flight: dict[tuple[str, str], int] = {}
        self.rng = random.Random()

    # ---------------------------------------------------------------- budgets

    def budgets(self, view: ResolvedRegistry, alias: str, dep: Deployment) -> list[_Budget]:
        """The buckets one grant on `dep` charges, each limit divided by the process count."""
        out: list[_Budget] = []
        key: tuple[str, ...]
        burst = float(view.value(alias, "rate_burst", dep.key))
        mode = str(view.value(alias, "rate_window", dep.key))
        limits = view.value(alias, "rate_limits", dep.key)
        count = self.process_count
        for name, kind in (("rpm", "r"), ("tpm", "t")):
            value = view.value(alias, name, dep.key)
            if value is not None and limits in ("both", name):
                key = ("d", alias, dep.key, name)
                out.append(_Budget(key, float(value) / count, burst, mode, kind))  # type: ignore[arg-type]
        account = view.registry.accounts[dep.account]
        for name, kind in (("shared_rpm", "r"), ("shared_tpm", "t")):
            value = account.settings.get(name)
            if value:
                key = ("a", dep.account, name)
                out.append(_Budget(key, float(value) / count, burst, mode, kind))  # type: ignore[arg-type]
        return out

    def _bucket(self, budget: _Budget) -> _Bucket:
        bucket = self.buckets.get(budget.key)
        if bucket is None:
            bucket = self.buckets[budget.key] = _Bucket(budget.limit, budget.burst, budget.mode)
        elif (bucket.limit, bucket.burst, bucket.mode) != (
            budget.limit,
            budget.burst,
            budget.mode,
        ):
            bucket.configure(budget.limit, budget.burst, budget.mode)
        return bucket

    def try_take(self, budgets: Sequence[_Budget], cost_r: float, cost_t: float) -> float:
        """Charge every budget when all fit and return 0; else the longest wait, nothing taken."""
        with self.lock:
            return self._try_take(budgets, cost_r, cost_t, time.time())

    def _try_take(
        self, budgets: Sequence[_Budget], cost_r: float, cost_t: float, now: float
    ) -> float:
        pairs = [(self._bucket(b), cost_r if b.kind == "r" else cost_t) for b in budgets]
        wait = max((bucket.wait(cost, now) for bucket, cost in pairs), default=0.0)
        if wait > 0:
            return wait
        for bucket, cost in pairs:
            bucket.take(cost, now)
        return 0.0

    def claim(
        self, alias: str, dep: str, budgets: Sequence[_Budget], cost_r: float, cost_t: float
    ) -> tuple[bool, bool, float, bool]:
        """Take a grant on `dep` in one step: (granted, usable, wait secs, is the probe).

        Usable, budget and the half-open probe are decided under one lock, so two engines of
        the process never both take the one probe (review L-4).
        """
        with self.lock:
            ok, secs, probe = self._usable(alias, dep, time.monotonic())
            if not ok:
                return False, False, secs, False
            secs = self._try_take(budgets, cost_r, cost_t, time.time())
            if secs > 0:
                return False, True, secs, False
            if probe:
                self.breaker(alias, dep).probing = True
            return True, True, 0.0, probe

    def slots(self, alias: str) -> _PrioritySlots:
        """The concurrency cap of one model in this process (every engine shares it)."""
        with self.lock:
            slots = self._slots.get(alias)
            if slots is None:
                slots = self._slots[alias] = _PrioritySlots(self.concurrency)
            return slots

    def give(self, budgets: Sequence[_Budget], requests: float, tokens_: float) -> None:
        """Give back what an attempt did not use (a negative amount charges more)."""
        now = time.time()
        with self.lock:
            for budget in budgets:
                self._bucket(budget).give(requests if budget.kind == "r" else tokens_, now)

    def left_fraction(self, view: ResolvedRegistry, alias: str) -> float:
        """The share of the model's rpm budget left now, over its confirmed deployments (0-1)."""
        now = time.time()
        total = left = 0.0
        with self.lock:
            for dep in view.deployments(alias, "confirmed"):
                for budget in self.budgets(view, alias, dep):
                    if budget.kind == "r" and budget.key[0] == "d":
                        bucket = self._bucket(budget)
                        total += budget.limit
                        left += min(budget.limit, bucket.left(now))
        return 1.0 if total <= 0 else left / total

    # ---------------------------------------------------------------- breakers and health

    def breaker(self, alias: str, dep: str) -> _Breaker:
        """The breaker of one deployment (made closed on first use)."""
        return self.breakers.setdefault((alias, dep), _Breaker())

    def health_of(self, alias: str, dep: str) -> _Health:
        """Latency, throttles and target errors of one deployment."""
        return self.health.setdefault((alias, dep), _Health())

    def usable(self, alias: str, dep: str) -> tuple[bool, float, bool]:
        """(usable now, seconds until it may be, whether this use is the breaker's probe)."""
        with self.lock:
            return self._usable(alias, dep, time.monotonic())

    def _usable(self, alias: str, dep: str, now: float) -> tuple[bool, float, bool]:
        health = self.health_of(alias, dep)
        if health.disabled_until > now:
            return False, health.disabled_until - now, False
        cb = self.breaker(alias, dep)
        if cb.thr_until > now:
            return False, cb.thr_until - now, False
        if cb.state == "open":
            if now < cb.until:
                return False, cb.until - now, False
            if cb.probing:
                return False, self.timings.admission_probe_wait_ms / 1000, False
            return True, 0.0, True
        return True, 0.0, False

    def settle(
        self,
        view: ResolvedRegistry,
        alias: str,
        dep: str,
        outcome: str,
        *,
        latency_ms: float = -1.0,
        retry_after: float | None = None,
    ) -> None:
        """Learn from an attempt: `ok`, `fail`, `throttle` or `none` (a probe ends either way)."""
        now = time.monotonic()
        with self.lock:
            cb = self.breaker(alias, dep)
            health = self.health_of(alias, dep)
            probing, cb.probing = cb.probing, False
            if outcome == "ok":
                if cb.state != "open" or probing or cb.until <= now:
                    cb.state, cb.failures = "closed", 0
                cb.thr_n = 0
                health.target_errors.clear()
            elif outcome == "throttle":
                cb.thr_n += 1
                top = self.timings.throttle_max_ms / 1000
                back = min(top, self.timings.throttle_base_ms / 1000 * 2 ** min(cb.thr_n - 1, 20))
                back = min(top, back * (1 + self.rng.random()))
                cb.thr_until = max(cb.thr_until, now + max(retry_after or 0.0, back))
                minute = int(time.time() // 60)
                health.throttles[minute] = health.throttles.get(minute, 0) + 1
                for stale in [m for m in health.throttles if m < minute - 1]:
                    del health.throttles[stale]
            elif outcome == "fail":
                window = float(view.value(alias, "breaker_window_secs", dep))
                if cb.fail_at < now - window:
                    cb.failures = 0
                cb.fail_at = now
                cb.failures += 1
                if cb.failures >= int(view.value(alias, "breaker_threshold", dep)) or probing:
                    cooldown = float(view.value(alias, "breaker_cooldown_secs", dep))
                    cb.state, cb.until = "open", now + cooldown
            if latency_ms >= 0:
                old = health.latency_ms if health.latency_ms is not None else float(latency_ms)
                health.latency_ms = old * 0.8 + latency_ms * 0.2
                health.samples += 1

    def target_error(self, view: ResolvedRegistry, alias: str, dep: str) -> bool:
        """Count a target error; True when the deployment is now disabled until its probe time.

        `health.disable_errors` within `disable_window_secs` disable it for `probe_after_secs`,
        never the model's last usable deployment: usable is queue mode's rule (A-3), one that
        takes calls (`_dep_takes_calls`) and is not disabled here (FA-3: a registry-disabled,
        blocked or out-of-residency one counted, and the real last one went).
        """
        settings = view.value(alias, "health", dep)
        now = time.monotonic()
        with self.lock:
            health = self.health_of(alias, dep)
            window = float(settings["disable_window_secs"])
            health.target_errors.append(now)
            while health.target_errors and health.target_errors[0] < now - window:
                health.target_errors.popleft()
            if len(health.target_errors) < int(settings["disable_errors"]):
                return False
            others = [
                d.key
                for d in view.deployments(alias)
                if d.key != dep
                and _dep_takes_calls(view, alias, d)
                and self.health_of(alias, d.key).disabled_until <= now
            ]
            if not others:
                return False
            health.disabled_until = now + float(settings["probe_after_secs"])
            health.target_errors.clear()
            return True

    # ---------------------------------------------------------------- credentials, sticky

    def credentials_ok(self, account: str, region: str) -> bool:
        """False while the account's credentials failed lately in this region."""
        bad = self.bad.get((account, region))
        return bad is None or bad.until <= time.monotonic()

    def credentials_failed(self, account: str, region: str, *, first: bool) -> None:
        """Back off an account and region: 5 s doubling to 60 s."""
        with self.lock:
            last = self.bad.get((account, region))
            if first or last is None:
                secs = min((last.secs if last else 2.5) * 2, 60.0)
                self.bad[(account, region)] = _Backoff(time.monotonic() + secs, secs)

    def sticky_get(self, alias: str, key: str, ttl_secs: float) -> str | None:
        """The deployment a session used last, while it was used within `ttl_secs`."""
        found = self.sticky.get((alias, key))
        if found is None or found[1] < time.monotonic() - ttl_secs:
            return None
        return found[0]

    def sticky_set(self, alias: str, key: str, dep: str) -> None:
        """Remember the session's deployment (oldest entries go past 10,000 sessions)."""
        with self.lock:
            self.sticky.pop((alias, key), None)
            self.sticky[(alias, key)] = (dep, time.monotonic())
            while len(self.sticky) > 10_000:
                self.sticky.pop(next(iter(self.sticky)))

    def retry_allowed(self, view: ResolvedRegistry, alias: str, *, first: bool) -> bool:
        """Count a first attempt (`first`) or a retry; a retry must fit the model's budget.

        `retry_budget_pct` of the first attempts over a sliding minute, at least
        `retry_budget_min`, as the queue engine counts it in the store.
        """
        now = time.time()
        minute = int(now // 60)
        weight = 1.0 - (now % 60) / 60
        with self.lock:
            counts = self.retries.setdefault((alias, minute), [0, 0])
            counts[1 if first else 0] += 1
            for key in [k for k in self.retries if k[1] < minute - 1]:
                del self.retries[key]
            if first:
                return True
            old = self.retries.get((alias, minute - 1), [0, 0])
        used = counts[0] + old[0] * weight
        firsts = counts[1] + old[1] * weight
        allowed = max(
            float(view.value(alias, "retry_budget_min")),
            firsts * int(view.value(alias, "retry_budget_pct")) / 100,
        )
        return used <= allowed


# ================================================================ concurrency


class _PrioritySlots:
    """A model's calls in flight in this process: `size` at once, priority callers first.

    Shared by the engines of the process, each on its own event loop: the state is under a
    thread lock, and a freed slot is handed to the next waiter on that waiter's loop.
    """

    def __init__(self, size: int) -> None:
        self.size = size
        self.in_use = 0
        self._lock = threading.Lock()
        self._waiters: dict[bool, deque[_Waiter]] = {True: deque(), False: deque()}

    def waiting(self, priority: bool) -> int:
        """Callers of one class waiting for a slot."""
        with self._lock:
            return len(self._waiters[priority])

    @contextlib.asynccontextmanager
    async def held(self, priority: bool) -> AsyncIterator[None]:
        """Hold one slot for the block."""
        await self._acquire(priority)
        try:
            yield
        finally:
            self._release()

    async def _acquire(self, priority: bool) -> None:
        loop = asyncio.get_running_loop()
        with self._lock:
            if self.in_use < self.size and not (self._waiters[True] or self._waiters[False]):
                self.in_use += 1
                return
            waiter: _Waiter = (loop, loop.create_future())
            self._waiters[priority].append(waiter)
        future = waiter[1]
        try:
            await future
        except asyncio.CancelledError:
            with self._lock:
                queued = waiter in self._waiters[priority]
                if queued:
                    self._waiters[priority].remove(waiter)
            if not queued and future.done() and not future.cancelled():
                self._release()  # handed a slot just as it was cancelled: pass it on
            raise  # a hand-off still on its way sees the cancelled future and passes it on

    def _release(self) -> None:
        with self._lock:
            for cls in (True, False):
                queue = self._waiters[cls]
                while queue:
                    loop, future = queue.popleft()
                    try:
                        loop.call_soon_threadsafe(self._hand, future)
                    except RuntimeError:  # that waiter's loop is closed: the next one
                        continue
                    return  # the slot passes on: `in_use` stays
            self.in_use -= 1

    def _hand(self, future: asyncio.Future[None]) -> None:
        """On the waiter's loop: give it the slot, or pass it on when it was cancelled."""
        if future.done():
            self._release()
        else:
            future.set_result(None)


_Waiter = tuple[asyncio.AbstractEventLoop, "asyncio.Future[None]"]


# ================================================================ one call


@dataclass(slots=True)
class _Call:
    """The state of one call on the local path."""

    env: Envelope
    path: str
    start: float  # loop time
    deadline: float  # loop time
    sink: Sink | None = None
    wait_cap: float | None = None  # the longest wait for a budget or breaker (`static_cap`)
    granted: float = 0.0  # loop time of the first grant
    texts: list[str] = field(default_factory=list)  # streamed text (partial_text)
    sent: bool = False  # events reached the caller: no other attempt any more
    broken: bool = False  # no confirmed deployment was usable
    model_retried: bool = False
    refused: bool = False  # an admission refusal was counted
    target_failed: set[tuple[str, str]] = field(default_factory=set)  # (model, deployment)
    last_kind: str = ""
    last_error: BaseException | None = None
    last_dep: tuple[str, str] = ("", "")  # (model, deployment) of the last attempt
    attempts: int = 0
    estimates: dict[str, tokens.Estimate] = field(default_factory=dict)

    def partial(self) -> str | None:
        return "".join(self.texts) or None


@dataclass(frozen=True, slots=True)
class _Grant:
    alias: str
    dep: Deployment
    budgets: list[_Budget]
    estimate: tokens.Estimate
    probe: bool


@dataclass(frozen=True, slots=True)
class _Wait:
    secs: float
    reason: str


@dataclass(frozen=True, slots=True)
class _Result:
    outcome: Literal["ok", "retry", "throttle", "fatal", "invalid", "stream_failed"]
    kind: str = ""
    exc: BaseException | None = None
    retry_after: float | None = None
    reply: Reply | None = None


class LocalExecutor:
    """Runs calls in this process with the local budgets (see the module docstring)."""

    def __init__(
        self,
        providers: Providers,
        limits: LocalLimits,
        *,
        view: Callable[[], ResolvedRegistry],
        worker_id: str = "",
        metrics: Metrics | None = None,
        hash_id: Callable[[str], str] = lambda value: value,
        tenant_label: Callable[[str | None], str] = lambda _: TENANT_NONE,
        params: Callable[[ResolvedRegistry, str, Envelope, str | None], dict[str, Any]]
        | None = None,
    ) -> None:
        """`view` reads the current registry; `params` builds a provider request.

        `tenant_label` maps a tenant hash to its metric label; `limits` (cap and budgets) is
        shared by every engine of the process; `params` defaults to `request_params`.
        """
        self.providers = providers
        self.limits = limits
        self._view = view
        self.worker_id = worker_id
        self.metrics: Metrics = metrics or NullSink()
        self.hash_id = hash_id
        self.tenant_label = tenant_label
        self._params = params or request_params
        # Calls in flight: each call's own end (resolved when `run` returns) -> the task that
        # runs it. The drain waits for the calls, never the caller's task, which may go on
        # long after its call (review L-1); the task is kept only to cancel the call.
        self.running: dict[asyncio.Future[None], asyncio.Task[Any] | None] = {}
        self._shutdown_cancelled: set[asyncio.Future[None]] = set()

    @property
    def concurrency(self) -> int:
        """Calls in flight per model in this process (`LLMQ_LOCAL_CONCURRENCY`)."""
        return self.limits.concurrency

    def slots(self, alias: str) -> _PrioritySlots:
        """The concurrency cap of one model in this process."""
        return self.limits.slots(alias)

    async def wait_running(self, timeout_secs: float) -> bool:
        """Wait up to `timeout_secs` for every call in flight; True when none is left."""
        if self.running:
            await asyncio.wait(set(self.running), timeout=max(0.0, timeout_secs))
        return not self.running

    async def shutdown(self) -> None:
        """Cancel the calls still running (after the drain); each fails `worker_shutdown`.

        Only a call cancelled here fails `worker_shutdown`; a caller's own cancel (a client
        that went away) stays a plain cancel, during a shutdown too (review L-8).
        """
        calls = {end: task for end, task in self.running.items() if task is not None}
        for end, task in calls.items():
            if not task.done():
                self._shutdown_cancelled.add(end)
                task.cancel()
        if calls:
            await asyncio.wait(set(calls), timeout=_STOP_WAIT_SECS)

    # ---------------------------------------------------------------- run

    async def run(
        self,
        view: ResolvedRegistry,
        alias: str,
        env: Envelope,
        *,
        path: str = "local",
        sink: Sink | None = None,
        wait_cap_secs: float | None = None,
    ) -> Reply:
        """Run one call to its reply; `CallFailed` with the queue path's kinds otherwise.

        `env.timeout_secs` bounds it all (slot, budget waits, attempts, backoff); `sink`
        gets each streamed event (the call then streams, and never retries once one was sent).
        `wait_cap_secs` caps the wait for a budget or breaker below `max_wait_secs`.
        """
        loop = asyncio.get_running_loop()
        now = loop.time()
        call = _Call(env, path, now, now + env.timeout_secs, sink, wait_cap=wait_cap_secs)
        task = asyncio.current_task()
        end: asyncio.Future[None] = loop.create_future()
        self.running[end] = task
        timer = asyncio.timeout_at(call.deadline)
        try:
            async with timer, self.slots(alias).held(env.priority):
                return await self._attempts(view, alias, call)
        except TimeoutError as exc:
            if not timer.expired():
                raise
            last = f" (last error: {call.last_kind})" if call.last_kind else ""
            self._fail(call, alias, "deadline_exceeded", f"no reply in time{last}", cause=exc)
        except asyncio.CancelledError:
            if end not in self._shutdown_cancelled or task is None:
                raise
            task.uncancel()
            self._fail(call, alias, "worker_shutdown", "the queue stopped before the call ended")
        finally:
            self.running.pop(end, None)
            self._shutdown_cancelled.discard(end)
            end.set_result(None)

    async def _attempts(self, view: ResolvedRegistry, alias: str, call: _Call) -> Reply:
        """Pick, attempt and retry until a reply or a final failure (`_fail` raises)."""
        loop = asyncio.get_running_loop()
        env = call.env
        failed: set[tuple[str, str]] = set()
        tries = throttles = 0
        self.limits.retry_allowed(view, alias, first=True)
        while True:
            view = self._view()
            pick = self._pick(view, alias, call, frozenset(failed))
            if isinstance(pick, _Wait):
                if not call.refused:
                    call.refused = True
                    self.metrics.count(
                        "llmq_admission_refusals_total", model=alias, reason=pick.reason
                    )
                waited = loop.time() - call.start
                max_wait, why = env.max_wait_secs, "max_wait_secs passed"
                if call.wait_cap is not None and (max_wait is None or call.wait_cap < max_wait):
                    max_wait, why = (
                        call.wait_cap,
                        f"no budget within {call.wait_cap:g} s ({call.path})",
                    )
                if max_wait is not None and waited + min(pick.secs, 1.0) > max_wait:
                    if waited < max_wait:
                        await asyncio.sleep(max_wait - waited)
                        continue
                    self._fail(call, alias, "rate_limited", why, retry_after=pick.secs)
                await asyncio.sleep(max(0.01, min(pick.secs, 1.0)))
                continue
            if pick is None:
                self._none_usable(view, alias, call)
            if not call.granted:
                call.granted = loop.time()
            call.attempts += 1
            result = await self._attempt(view, pick, call)
            dep_key = pick.dep.key
            self.metrics.count(
                "llmq_attempts_total",
                model=pick.alias,
                account=pick.dep.account,
                region=pick.dep.region,
                result="ok" if result.outcome == "ok" else (result.kind or result.outcome),
            )
            if result.outcome == "ok":
                assert result.reply is not None
                return result.reply
            call.last_kind, call.last_error = result.kind, result.exc
            message = str(result.exc or result.kind)
            if result.outcome in ("stream_failed", "invalid"):
                self._fail(call, alias, result.kind, message)
            tried = frozenset(failed | {(pick.alias, dep_key)})
            untried = self._untried(view, pick.alias, call, tried)
            if result.outcome == "throttle":
                throttles += 1
                limit = int(view.value(pick.alias, "throttle_retry_attempts", dep_key))
                budget_ok = self.limits.retry_allowed(view, alias, first=False)
                if throttles > limit or (not budget_ok and not untried):
                    self._fail(
                        call,
                        alias,
                        "rate_limited",
                        "the provider kept throttling",
                        retry_after=result.retry_after,
                    )
                retry_secs = 0.0
            else:
                tries += 1
                if result.outcome == "fatal" or tries >= int(view.value(alias, "max_attempts")):
                    known = result.kind in ("invalid_request", "context_length_exceeded")
                    self._fail(call, alias, result.kind if known else "provider_error", message)
                if result.kind == "target" and not untried:
                    self._fail(call, alias, "provider_error", message)
                if not self.limits.retry_allowed(view, alias, first=False) and not untried:
                    self._fail(call, alias, "retry_budget_exhausted", message)
                quick = result.kind in ("target", "model_error", "auth") and untried
                retry_secs = 0.0 if quick else self._backoff(view, pick.alias, dep_key, tries)
            self.metrics.count("llmq_retries_total", model=alias, reason=str(result.kind))
            failed.add((pick.alias, dep_key))
            if retry_secs:
                await asyncio.sleep(max(0.0, min(retry_secs, call.deadline - loop.time())))

    def _backoff(self, view: ResolvedRegistry, alias: str, dep: str, tries: int) -> float:
        """The engine's retry backoff: `retry_base_secs` x multiplier^(tries-1), spread, capped."""
        base = float(view.value(alias, "retry_base_secs", dep))
        factor = float(view.value(alias, "retry_multiplier", dep))
        spread = float(view.value(alias, "retry_spread", dep))
        low = base * factor ** (tries - 1)
        high = low * (1 + (spread - 1) * self.limits.rng.random())
        return min(high, float(view.value(alias, "retry_max_secs", dep)))

    def _untried(
        self, view: ResolvedRegistry, alias: str, call: _Call, tried: frozenset[tuple[str, str]]
    ) -> bool:
        """True when a deployment of `alias` this call has not tried may still take it."""
        return any((alias, d.key) not in tried for d in self._deployments(view, alias, call))

    # ---------------------------------------------------------------- choosing a deployment

    def _deployments(self, view: ResolvedRegistry, alias: str, call: _Call) -> list[Deployment]:
        """The model's deployments this call may use at all (switches, filters, credentials)."""
        env = call.env
        routing = view.registry.routing
        out = []
        for dep in view.deployments(alias):
            if env.allowed_regions and dep.region not in env.allowed_regions:
                continue
            if env.allowed_accounts and dep.account not in env.allowed_accounts:
                continue
            if not view.value(alias, "enabled", dep.key):
                continue
            if not view.registry.accounts[dep.account].enabled:
                continue
            if dep.region in routing.blocked_regions or dep.account in routing.blocked_accounts:
                continue
            if view.outside_residency(dep):
                continue
            if not self.limits.credentials_ok(dep.account, dep.region):
                continue
            out.append(dep)
        return out

    def _estimate(self, view: ResolvedRegistry, alias: str, call: _Call) -> tokens.Estimate:
        found = call.estimates.get(alias)
        if found is None:
            params = self._params(view, alias, call.env, None)
            found = call.estimates[alias] = estimate_of(view, alias, call.env, params)
        return found

    def _ordered(
        self, view: ResolvedRegistry, alias: str, call: _Call, deps: list[Deployment], age: float
    ) -> list[Deployment]:
        """Deployments in the order the model's `balance` (and `sticky`) prefer."""
        env = call.env
        strategy = str(view.value(alias, "balance"))
        if env.priority and view.value(alias, "balance_priority"):
            strategy = str(view.value(alias, "balance_priority"))
        if strategy == "round_robin" and deps:
            with self.limits.lock:
                turn = self.limits.turn.get(alias, 0)
                self.limits.turn[alias] = turn + 1
            shift = turn % len(deps)
            deps = deps[shift:] + deps[:shift]
        elif strategy == "weighted":
            weights = [max(0.0, float(view.value(alias, "weight", d.key))) for d in deps]
            keyed = [
                (self.limits.rng.random() ** (1 / w) if w > 0 else -1.0, d)
                for w, d in zip(weights, deps, strict=True)
            ]
            deps = [d for _, d in sorted(keyed, key=lambda item: -item[0])]
        elif strategy == "headroom":
            now = time.time()

            def room(dep: Deployment) -> float:
                with self.limits.lock:
                    fractions = [
                        self.limits._bucket(b).left(now) / b.limit
                        for b in self.limits.budgets(view, alias, dep)
                        if b.limit > 0
                    ]
                return min(fractions, default=1.0)

            deps = sorted(deps, key=lambda d: -room(d))
        elif strategy.startswith("custom:"):
            deps = self._custom(view, alias, call, deps, strategy[7:], age)
        sticky = view.value(alias, "sticky")
        key = self._sticky_key(view, alias, call, age) if sticky else None
        if key is not None:
            dep_key = self.limits.sticky_get(alias, key, float(sticky["idle_ttl_secs"]))
            deps = sorted(deps, key=lambda d: d.key != dep_key)
        return deps

    def _custom(
        self,
        view: ResolvedRegistry,
        alias: str,
        call: _Call,
        deps: list[Deployment],
        name: str,
        age: float,
    ) -> list[Deployment]:
        fn = hooks.get("balancer", name)
        if fn is None:
            return deps
        views = [
            hooks.DeploymentView(
                key=d.key,
                account=d.account,
                region=d.region,
                group=d.group,
                rpm=view.value(alias, "rpm", d.key),
                tpm=view.value(alias, "tpm", d.key),
                weight=float(view.value(alias, "weight", d.key)),
                tags=view.value(alias, "tags", d.key) or {},
            )
            for d in deps
        ]
        order = hooks.call_safely(
            "balancer", name, fn, views, self._ctx(alias, call, age), default=None
        )
        if not order:
            return deps
        by_key = {d.key: d for d in deps}
        ranked = [by_key[k] for k in order if k in by_key]
        return ranked + [d for d in deps if d.key not in set(order)]

    def _ctx(self, alias: str, call: _Call, age: float) -> hooks.CallContext:
        env = call.env
        return hooks.CallContext(
            alias, env.request_id, env.priority, env.session_id, env.user_id, env.tenant_id, age
        )

    def _sticky_key(
        self, view: ResolvedRegistry, alias: str, call: _Call, age: float
    ) -> str | None:
        if view.registry.models[alias].type != "chat":
            return None
        sticky = view.value(alias, "sticky")
        key: str | None = call.env.session_id
        if str(sticky["key"]).startswith("custom:"):
            name = str(sticky["key"])[7:]
            fn = hooks.get("sticky_key", name)
            ctx = self._ctx(alias, call, age)
            key = hooks.call_safely("sticky_key", name, fn, ctx, default=None) if fn else None
        return self.hash_id(str(key)) if key else None

    def _pick(
        self,
        view: ResolvedRegistry,
        alias: str,
        call: _Call,
        failed: frozenset[tuple[str, str]],
    ) -> _Grant | _Wait | None:
        """A granted deployment, a wait, or None when nothing can serve the call."""
        env = call.env
        age = asyncio.get_running_loop().time() - call.start
        try:
            estimate = self._estimate(view, alias, call)
        except inference.InferenceRejected as exc:  # `on_exceed: reject` (RV-EN14)
            self._fail(call, alias, "invalid_request", str(exc), cause=exc)
        except Exception as exc:  # a param the estimate or a hook cannot read (EN-2)
            logger.error(
                "request_unusable", request_id=env.request_id, error_type=type(exc).__name__
            )
            message = f"the request could not be prepared ({type(exc).__name__})"
            self._fail(call, alias, "invalid_request", message, cause=exc)
        own = self._deployments(view, alias, call)
        if tpm_refuses(view, alias, (d.key for d in own), estimate.total):
            self._fail(call, alias, "request_too_large", "the call is above the token budget")
        cause = "error" if failed else ("broken" if call.broken else "exhausted")
        found = self._try(view, alias, call, own, failed, age, cause, estimate)
        if isinstance(found, _Grant):
            return found
        wait, usable = found
        if not usable and not call.broken:
            call.broken = True  # nothing confirmed can take calls now: the fallback group may
            found = self._try(view, alias, call, own, failed, age, "broken", estimate)
            if isinstance(found, _Grant):
                return found
            wait = min(wait, found[0])
            cause = "broken"
        # A fallback rule not due yet: the call waits for it (its age reaches `after_secs`).
        for name, modes, there in (
            ("fallback", ("all", "regions"), any(d.group == "fallback" for d in own)),
            ("model_fallback", ("all",), True),
        ):
            rule = view.value(alias, name)
            if rule and there and env.allow_fallback in modes and cause in rule["on"]:
                after = float(rule["after_secs"])
                if age < after:
                    wait = min(wait, after - age)
        grant = self._model_fallback(view, alias, call, age, cause)
        if isinstance(grant, _Grant):
            return grant
        wait = min(wait, grant)
        if math.isfinite(wait):
            return _Wait(wait, "budget" if usable else "breaker")
        return None

    def _try(
        self,
        view: ResolvedRegistry,
        alias: str,
        call: _Call,
        deps: list[Deployment],
        failed: frozenset[tuple[str, str]],
        age: float,
        cause: str,
        estimate: tokens.Estimate,
    ) -> _Grant | tuple[float, bool]:
        """Take the first deployment whose budgets fit; else (shortest wait, any usable)."""
        fallback_ok = may_spill(view, alias, call.env, age, cause, "fallback")
        candidates = [d for d in deps if d.group != "fallback" or fallback_ok]
        candidates = self._ordered(view, alias, call, candidates, age)
        candidates.sort(key=lambda d: (alias, d.key) in failed)  # stable: tried ones last
        wait, usable = math.inf, False
        for dep in candidates:
            budgets = self.limits.budgets(view, alias, dep)
            granted, ok, secs, probe = self.limits.claim(alias, dep.key, budgets, 1, estimate.total)
            if granted:
                return _Grant(alias, dep, budgets, estimate, probe)
            usable = usable or (ok and dep.group != "fallback")
            wait = min(wait, secs)
        return wait, usable

    def _model_fallback(
        self, view: ResolvedRegistry, alias: str, call: _Call, age: float, cause: str
    ) -> _Grant | float:
        """A grant on a fallback model when the rule allows; else the shortest wait there."""
        if not may_spill(view, alias, call.env, age, cause, "model_fallback"):
            return math.inf
        env = call.env
        wait = math.inf
        for item in view.value(alias, "model_fallback")["models"]:
            other = item["model"]
            if other not in view.registry.models or not _takes_calls(view, other):
                continue
            if not shape_ok(view, other, env.op, env.params, stream=env.stream):
                continue
            if item["require_headroom"]:
                pct = int(view.value(other, "require_headroom_pct"))
                if self.limits.left_fraction(view, other) * 100 < pct:
                    continue
            try:
                estimate = self._estimate(view, other, call)
            except Exception as exc:  # this target cannot take the request: the next one
                logger.info("fallback_shape_skipped", model=other, reason=type(exc).__name__)
                continue
            deps = self._deployments(view, other, call)
            found = self._try(view, other, call, deps, frozenset(), age, "exhausted", estimate)
            if isinstance(found, _Grant):
                return found
            wait = min(wait, found[0])
        return wait

    def _none_usable(self, view: ResolvedRegistry, alias: str, call: _Call) -> NoReturn:
        """No deployment can serve the call: say why, as the queue path does."""
        if call.last_error is not None:
            kind = call.last_kind
            known = kind in ("invalid_request", "context_length_exceeded", "rate_limited")
            self._fail(call, alias, kind if known else "provider_error", str(call.last_error))
        deps = list(view.deployments(alias))
        if any(not self.limits.credentials_ok(d.account, d.region) for d in deps):
            self._fail(
                call,
                alias,
                "credentials_unavailable",
                "no account with working credentials",
                retry_after=60,
            )
        if not self._deployments(view, alias, call):
            self._fail(call, alias, "no_deployment", "the call's filter leaves no deployment")
        self._fail(call, alias, "model_unavailable", "no usable deployment", retry_after=60)

    # ---------------------------------------------------------------- one attempt

    async def _attempt(self, view: ResolvedRegistry, grant: _Grant, call: _Call) -> _Result:
        """One provider attempt on a granted deployment, and what it teaches the budgets.

        From the grant to the end, one `finally` settles it: whatever raises (the request
        builder, a cancel), the probe ends and the budget is given back (review L-3).
        """
        alias, dep, estimate = grant.alias, grant.dep, grant.estimate
        call.last_dep = (alias, dep.key)
        state = {"settled": False, "sent": False}

        def settle(outcome: str, refund: tokens.Refund, **learn: Any) -> None:
            state["settled"] = True
            self.limits.give(grant.budgets, refund.requests, refund.tokens)
            self.limits.settle(view, alias, dep.key, outcome, **learn)

        try:
            return await self._attempt_granted(view, grant, call, settle, state)
        finally:
            if not state["settled"]:
                if state["sent"]:  # cancelled at the provider (deadline, shutdown): charged
                    self.limits.settle(view, alias, dep.key, "none")
                else:  # never reached the provider: everything back
                    settle("none", tokens.refund(estimate, "unsent"))

    async def _attempt_granted(
        self,
        view: ResolvedRegistry,
        grant: _Grant,
        call: _Call,
        settle: Callable[..., None],
        state: dict[str, bool],
    ) -> _Result:
        """`_attempt`'s body: `settle` once with what the attempt taught, `state["sent"]`."""
        env = call.env
        alias, dep, estimate = grant.alias, grant.dep, grant.estimate
        try:
            params = self._params(view, alias, env, dep.key)
        except inference.InferenceRejected as exc:
            settle("none", tokens.refund(estimate, "unsent"))
            return _Result("invalid", "invalid_request", exc)
        loop = asyncio.get_running_loop()
        remaining = max(0.1, call.deadline - loop.time())
        budget = min(float(view.value(alias, "attempt_timeout_secs", dep.key)), remaining)
        timing = Timing(budget, float(view.value(alias, "stream_idle_secs")), budget)
        entry = view.registry.models[alias]
        provider = self.providers.get(entry.provider)
        served_op = _served_op(view, alias, env)
        route = route_for(view, dep)
        labels = {"model": alias, "account": dep.account, "region": dep.region}
        started = time.monotonic()
        sink: Sink | None = None
        if call.sink is not None:
            out = call.sink
            first = [True]

            async def sink(event: Any) -> None:
                if first[0]:
                    first[0] = False
                    elapsed = time.monotonic() - started
                    self.metrics.observe("llmq_first_token_seconds", elapsed, **labels)
                if text := text_of(event):
                    call.texts.append(text)
                call.sent = True
                await out(event)

        where = (alias, dep.key)
        api = _api_kwargs(view, alias)
        with self.limits.lock:
            self.limits.in_flight[where] = self.limits.in_flight.get(where, 0) + 1
        state["sent"] = True
        try:
            result = await provider.call(served_op, params, route, timing, sink=sink, **api)
        except Exception as exc:
            return self._failed(view, grant, call, exc, provider, route, started, settle)
        finally:
            with self.limits.lock:
                self.limits.in_flight[where] -= 1
        latency = (time.monotonic() - started) * 1000
        self.metrics.observe("llmq_provider_seconds", latency / 1000, **labels)
        used = tokens.used_tokens(
            result.usage,
            output_weight=float(view.value(alias, "tpm_output_weight", dep.key)),
            count_cached=bool(view.value(alias, "tpm_count_cached", dep.key)),
        )
        settle("ok", tokens.refund(estimate, "ok", used), latency_ms=latency)
        with self.limits.lock:
            self.limits.bad.pop((dep.account, dep.region), None)
        for model, failed_dep in call.target_failed:  # the deployment, not the request
            if model in view.registry.models and self.limits.target_error(view, model, failed_dep):
                logger.error(
                    "deployment_disabled", model=model, deployment=failed_dep, path=call.path
                )
        call.target_failed.clear()
        sticky = view.value(alias, "sticky")
        if sticky and (key := self._sticky_key(view, alias, call, 0.0)) is not None:
            self.limits.sticky_set(alias, key, dep.key)
        meta = Meta(
            served_model=alias,
            deployment=dep.key,
            fallback="model"
            if alias != env.model
            else ("region" if dep.group == "fallback" else None),
            attempts=call.attempts,
            queue_wait_ms=int(max(0.0, call.granted - call.start) * 1000),
            provider_ms=int(latency),
            tokens_in=result.usage.input_tokens,
            tokens_out=result.usage.output_tokens,
            worker=self.worker_id,
            provider_request_id=result.request_id,
        )
        response = shapes.reply(result.response, env.op, served_op, model=alias)
        reply = Reply(request_id=env.request_id, ok=True, response=response, meta=meta)
        self._record(call, alias, dep.account, dep.region, "ok", meta)
        return _Result("ok", reply=reply)

    def _failed(
        self,
        view: ResolvedRegistry,
        grant: _Grant,
        call: _Call,
        exc: Exception,
        provider: Any,
        route: Any,
        started: float,
        settle: Callable[..., None],
    ) -> _Result:
        """Classify a failed attempt as `_attempt` in the queue engine does."""
        alias, dep, estimate = grant.alias, grant.dep, grant.estimate
        latency = (time.monotonic() - started) * 1000
        labels = {"model": alias, "account": dep.account, "region": dep.region}
        self.metrics.observe("llmq_provider_seconds", latency / 1000, **labels)
        info = provider.classify(exc)
        logger.warning(
            "local_attempt_failed",
            model=alias,
            deployment=dep.key,
            request_id=call.env.request_id,
            path=call.path,
            error_type=type(exc).__name__,
            kind=str(info.kind),
        )
        throttle = info.kind is ErrorKind.THROTTLE
        if call.sent:  # events reached the caller: the stream ends here, with its text
            settle("throttle" if throttle else "fail", tokens.refund(estimate, "fail"))
            return _Result("stream_failed", "rate_limited" if throttle else "provider_error", exc)
        if throttle:
            settle(
                "throttle",
                tokens.refund(estimate, "throttle"),  # the provider counted it: request kept
                retry_after=info.retry_after_secs,
            )
            return _Result("throttle", "throttled", exc, retry_after=info.retry_after_secs)
        model_error = error_family(info.code) in ("modelerror", "modeltimeout")
        if info.kind is ErrorKind.RETRYABLE and model_error and not call.model_retried:
            call.model_retried = True  # the model's answer failed, not the region (P9)
            settle("none", tokens.refund(estimate, "fail"))
            return _Result("retry", "model_error", exc)
        if info.kind is ErrorKind.RETRYABLE or isinstance(exc, TimeoutError):
            cut = isinstance(exc, AttemptTimeout) and "chunk" in str(exc)
            settle("fail", tokens.refund(estimate, "cut" if cut else "fail"), latency_ms=latency)
            return _Result("retry", "retryable", exc)
        settle("none", tokens.refund(estimate, "unsent"))  # refused before it ran: all back
        if is_target_error(exc, info):
            call.target_failed.add((alias, dep.key))
            if len(call.target_failed) >= 2:  # two deployments refuse it: the request
                call.target_failed.clear()
                return _Result("fatal", "request", exc)
            return _Result("retry", "target", exc)
        if isinstance(exc, CredentialsUnavailable) or is_auth_error(exc, info):
            slow = exc if isinstance(exc, CredentialsRefreshSlow) else None
            if slow is None:
                self.providers.pool.forget(route)
            self.limits.credentials_failed(
                dep.account, dep.region, first=slow is None or slow.first
            )
            logger.error(
                "account_credentials_failed",
                account=dep.account,
                region=dep.region,
                model=alias,
                request_id=call.env.request_id,
                error_type=type(exc).__name__,
                error=str(exc)[:300],
            )
            self.metrics.count(
                "llmq_credentials_failed_total", account=dep.account, region=dep.region
            )
            return _Result("retry", "auth", exc)
        return _Result("fatal", request_error_kind(exc, info) or "fatal", exc)

    # ---------------------------------------------------------------- outcomes

    def _record(
        self,
        call: _Call,
        alias: str,
        account: str,
        region: str,
        outcome: str,
        meta: Meta | None,
        *,
        error: str | None = None,
    ) -> None:
        env = call.env
        loop = asyncio.get_running_loop()
        priority = "1" if env.priority else "0"
        total = max(0.0, loop.time() - call.start)
        if call.granted:
            admit = max(0.0, call.granted - call.start)
            self.metrics.observe(
                "llmq_admission_wait_seconds", admit, model=env.model, priority=priority
            )
        self.metrics.observe("llmq_call_seconds", total, model=env.model, outcome=outcome)
        log_outcome(
            self.metrics,
            self.worker_id,
            env,
            alias,
            account,
            region,
            outcome,
            meta,
            call.path,
            tenant_label=self.tenant_label(env.tenant_id),
            deployment=(meta.deployment if meta else None) or call.last_dep[1] or None,
            attempts=(meta.attempts if meta else None) or call.attempts or None,
            duration_ms=int(total * 1000),
            error=error[:300] if error else None,
        )

    def _fail(
        self,
        call: _Call,
        alias: str,
        kind: str,
        message: str,
        *,
        retry_after: float | None = None,
        cause: BaseException | None = None,
    ) -> NoReturn:
        """Record the call's failure and raise it as `CallFailed`."""
        view = self._view()
        account = region = ""
        served, dep_key = call.last_dep
        for dep in view.deployments(served) if served in view.registry.models else ():
            if dep.key == dep_key:
                account, region = dep.account, dep.region
        self._record(call, alias, account, region, kind, None, error=message)
        failure = CallFailed(
            kind,
            message,
            retry_after_secs=retry_after,
            partial_text=call.partial(),
            request_id=call.env.request_id,
        )
        raise failure from cause or call.last_error


# ================================================================ local mode


class LocalRegistry:
    """The registry of a local-mode process: its own document, checked; nothing is stored.

    `current` and the read side are as `RegistryStore`'s; live edits, publishes, rollback and
    the audit need the store and raise `LocalModeRefused`.
    """

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        prefix: str = "llmq",
        allowed_hosts: tuple[str, ...] = (),
    ) -> None:
        """Keep the document; it is checked by `boot()` / `run_on_config()`."""
        self.config: dict[str, Any] | None = dict(config)
        self.prefix = prefix
        self.live_edit = "off"
        self.metrics: Metrics | None = None
        self.on_own_config = True
        self.stored_refused = False
        self.lib_too_old = False
        self.invalid_count = 0
        self._hosts = allowed_hosts
        self._current: ResolvedRegistry | None = None

    @property
    def current(self) -> ResolvedRegistry:
        """The checked document; `ConfigError` before `start()`."""
        if self._current is None:
            raise ConfigError("the registry has not been loaded yet; call LLMQueue.start() first")
        return self._current

    @property
    def changes(self) -> int:
        """0 once loaded (a local registry never changes), -1 before."""
        return -1 if self._current is None else 0

    def run_on_config(self) -> ResolvedRegistry:
        """Check the document and use it (`RegistryError` listing every problem)."""
        assert self.config is not None
        registry, errors, warnings = check(self.config, allowed_hosts=self._hosts)
        if registry is None:
            raise RegistryError(errors, warnings)
        self._current = ResolvedRegistry(registry)
        return self._current

    async def boot(self, *, actor: str | None = None) -> ResolvedRegistry:
        """`run_on_config()`: there is no store to publish to."""
        return self.run_on_config()

    async def load(self) -> ResolvedRegistry | None:
        """The loaded registry."""
        return self._current

    async def refresh(self) -> ResolvedRegistry | None:
        """The loaded registry (nothing to reload)."""
        return self._current

    async def run(self, interval_secs: float) -> None:
        """Nothing to reload in local mode."""

    async def diff_with_config(self) -> list[dict[str, Any]]:
        """No live edits exist in local mode."""
        return []

    async def publish(self, doc: Mapping[str, Any], *, actor: str) -> PublishResult:
        """Refused: there is no store to publish to."""
        raise LocalModeRefused("registry publish")

    async def set(
        self, values: Mapping[str, Any], *, actor: str, model: str | None = None
    ) -> ResolvedRegistry:
        """Refused: live edits live in the store (change the document and restart)."""
        raise LocalModeRefused("registry live edit")

    async def set_enabled(self, *args: Any, **kwargs: Any) -> ResolvedRegistry:
        """Refused: live edits live in the store."""
        raise LocalModeRefused("registry live edit")

    async def block(
        self,
        *,
        region: str | None = None,
        account: str | None = None,
        on: bool = True,
        actor: str,
    ) -> ResolvedRegistry:
        """Refused: routing blocks live in the store."""
        raise LocalModeRefused("registry block")

    async def edit(self, *args: Any, **kwargs: Any) -> ResolvedRegistry:
        """Refused: live edits live in the store."""
        raise LocalModeRefused("registry live edit")

    async def rollback(self, version: int, *, actor: str) -> PublishResult:
        """Refused: versions live in the store."""
        raise LocalModeRefused("registry rollback")

    async def log(self, count: int = 20, path_prefix: str = "") -> list[dict[str, Any]]:
        """Refused: the audit lives in the store."""
        raise LocalModeRefused("registry audit log")

    async def versions(self) -> list[int]:
        """Refused: versions live in the store."""
        raise LocalModeRefused("registry versions")


class LocalEngine:
    """The engine of a process in local mode: no store client; calls run on `LocalExecutor`.

    Same surface as `Engine` for `LLMQueue`, the HTTP routes and the embedding helper.
    """

    def __init__(
        self,
        registry: LocalRegistry,
        providers: Providers,
        *,
        settings: EngineSettings | None = None,
        worker_id: str | None = None,
        metrics: Metrics | None = None,
        limits: LocalLimits | None = None,
    ) -> None:
        """Nothing runs until `start`; `limits` shares one process budget between engines."""
        self.settings = settings or EngineSettings()
        self.registry = registry
        self.providers = providers
        self.metrics: Metrics = metrics or NullSink()
        registry.metrics = self.metrics
        tail = new_id()[-14:]
        self.worker_id = f"{worker_id}-{tail[-6:]}" if worker_id else tail
        salt = self.settings.id_hash_salt
        self.hash_id: Callable[[str], str] = lambda value: keyed_hash(salt, value)
        self.limits = limits or LocalLimits(
            process_count=self.settings.local_process_count,
            concurrency=self.settings.local_concurrency,
            timings=self.settings.timings,
        )
        self.executor = LocalExecutor(
            providers,
            self.limits,
            view=lambda: self.registry.current,
            worker_id=self.worker_id,
            metrics=self.metrics,
            hash_id=self.hash_id,
            tenant_label=TenantLabels(self.settings.admin.metrics_tenants, self.hash_id),
        )
        self.queues: Any = None  # no store: jobs are refused by `LLMQueue`
        self.replica_wait: Any = None
        self.ready = False
        self._paused = False
        self._draining_since: float | None = None
        self._started = False
        self._stopped = False

    # ---------------------------------------------------------------- lifecycle

    async def start(self, *, worker: bool = True, caller: bool = True) -> None:
        """Check the registry document; ready unless paused before (roles do not apply)."""
        self.registry.run_on_config()
        self._started = True
        self.ready = not self._paused

    def pause_receiving(self) -> None:
        """Not ready (the load balancer stops sending); calls in flight and new ones still run."""
        self.ready = False
        if self._draining_since is None:
            self._draining_since = time.time()
        self._paused = True

    def resume_receiving(self) -> None:
        """Undo `pause_receiving`; `LLMQueueError` once stopped."""
        from llm_queue.errors import LLMQueueError

        if self._stopped:
            raise LLMQueueError("this engine is stopped; it cannot resume receiving")
        self._paused = False
        self._draining_since = None
        self.ready = self._started

    @property
    def draining(self) -> bool:
        """True between `pause_receiving()` and `resume_receiving()`."""
        return self._paused

    @property
    def draining_since(self) -> float | None:
        """Wall time of the pause, None when not draining."""
        return self._draining_since

    def drain_status(self) -> dict[str, Any]:
        """`GET /drain`: draining, since, held (always 0 here) and running."""
        return {
            "draining": self._paused,
            "since": self._draining_since,
            "held": 0,
            "running": len(self.executor.running),
        }

    async def wait_running(self, timeout_secs: float) -> bool:
        """Wait up to `timeout_secs` for the calls in flight; True when none is left."""
        return await self.executor.wait_running(timeout_secs)

    async def stop(self, drain_secs: float | None = None) -> None:
        """Not ready; let calls in flight finish within `drain_secs`, then cancel the rest."""
        drain = self.settings.drain_secs if drain_secs is None else drain_secs
        self.ready = False
        self._stopped = True
        await self.executor.wait_running(drain)
        await self.executor.shutdown()
        flush = getattr(self.metrics, "flush", None)
        if callable(flush):
            try:
                flush()
            except Exception as exc:
                logger.warning("stop_metrics_flush_failed", error=str(exc))

    async def aclose(self) -> None:
        """Close the provider clients."""
        await self.providers.aclose()

    def _live(self) -> bool:
        return True

    def _not_live_reason(self) -> str:
        return ""

    def _evicting(self) -> str | None:
        return None

    @property
    def purge(self) -> PurgeService:
        """Refused: purges delete store state, and local mode has none."""
        raise LocalModeRefused("purge")

    # ---------------------------------------------------------------- calls

    async def call(
        self,
        model: str,
        params: Mapping[str, Any],
        *,
        priority: bool = False,
        timeout_secs: float | None = None,
        max_wait_secs: float | None = None,
        request_id: str | None = None,
        session_id: str | None = None,
        user_id: str | None = None,
        tenant_id: str | None = None,
        allow_fallback: str = "all",
        allowed_regions: Sequence[str] | None = None,
        allowed_accounts: Sequence[str] | None = None,
        force_direct: bool = False,
        stream: bool = False,
        stream_id: str | None = None,
        vectors: bool = False,
        sink: Sink | None = None,
    ) -> Reply:
        """Run one call in this process (`Engine.call`'s options; `force_direct` is moot)."""
        view = self.registry.current
        alias, envelope = build_envelope(
            view,
            model,
            params,
            caller=self.worker_id,
            metrics=self.metrics,
            priority=priority,
            timeout_secs=timeout_secs,
            max_wait_secs=max_wait_secs,
            request_id=request_id,
            session_id=session_id,
            user_id=user_id,
            tenant_id=tenant_id,
            hash_id=self.hash_id,
            allow_fallback=allow_fallback,
            allowed_regions=allowed_regions,
            allowed_accounts=allowed_accounts,
            stream=stream,
            stream_id=stream_id,
            vectors=vectors,
        )
        try:
            return await self.executor.run(view, alias, envelope, path="local", sink=sink)
        except CallFailed as exc:
            self.metrics.count("llmq_caller_failures_total", model=alias, outcome=str(exc.kind))
            raise

    async def stream(
        self, model: str, params: Mapping[str, Any], *, meta_event: bool = False, **call: Any
    ) -> AsyncIterator[dict[str, Any]]:
        """A chat call's events as the provider sends them (`Engine.stream`'s contract)."""
        view = self.registry.current
        alias = view.registry.alias_of(model)
        kind = view.registry.models[alias].type
        if kind != "chat":
            raise CallFailed(
                "invalid_request",
                f"{alias} is an {kind} model; stream() needs a chat model",
                request_id=call.get("request_id"),
            )
        events: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

        async def sink(event: Any) -> None:
            events.put_nowait(event)

        task = asyncio.create_task(self.call(model, params, stream=True, sink=sink, **call))
        task.add_done_callback(lambda _: events.put_nowait(_DONE))
        ended = False
        try:
            while True:
                event = await events.get()
                if event is _DONE:
                    reply = task.result()  # the call's failure, with its partial text
                    ended = True
                    if meta_event:
                        yield _meta_event(reply.meta.model_dump(), None)
                    return
                yield event
        finally:
            if not ended:
                if not task.done():
                    task.cancel()
                task.add_done_callback(_seen)
                await asyncio.wait({task})

    async def embed(
        self,
        model: str,
        texts: Sequence[str],
        *,
        priority: bool = False,
        timeout_secs: float | None = None,
        input_type: str | None = None,
        **call: Any,
    ) -> list[list[float]]:
        """One vector per text, in order (batched by the model's `embed_batch_size`)."""
        from llm_queue.engine.embed import embed

        return await embed(
            self,
            model,
            texts,
            priority=priority,
            timeout_secs=timeout_secs,
            input_type=input_type,
            **call,
        )

    # ---------------------------------------------------------------- admin

    async def cancel(self, request_id: str, *, source: str = "api") -> None:
        """Refused: cancel by id finds a call through the store (cancel its task instead)."""
        raise LocalModeRefused("cancel by request id")

    async def request_status(self, request_id: str) -> dict[str, Any]:
        """Refused: call records live in the store."""
        raise LocalModeRefused("request status")

    async def status(self, model: str | None = None) -> dict[str, Any]:
        """This process's view: per model, slots and per deployment its local budgets."""
        view = self.registry.current
        if model is not None:
            return self._model_status(view, view.registry.alias_of(model))
        routing = view.registry.routing
        models = [self._model_status(view, alias) for alias in view.registry.models]
        return {
            "mode": "local",
            "registry_version": view.registry.version,
            "kill_switch": routing.kill_switch,
            "blocked_regions": list(routing.blocked_regions),
            "blocked_accounts": list(routing.blocked_accounts),
            "process_count": self.limits.process_count,
            "models": models,
            "health": worst_health(m["health"]["state"] for m in models),
        }

    def _model_status(self, view: ResolvedRegistry, alias: str) -> dict[str, Any]:
        slots = self.executor.slots(alias)
        deps = [self._dep_status(view, alias, dep) for dep in view.deployments(alias)]
        out: dict[str, Any] = {
            "model": alias,
            "mode": "local",
            "enabled": bool(view.effective(alias, "enabled").value),
            "waiting": {"priority": slots.waiting(True), "normal": slots.waiting(False)},
            "in_progress": slots.in_use,
            "dead_letters": 0,
            "concurrency": {"limit": slots.size, "in_use": slots.in_use},
            "deployments": deps,
        }
        out["health"] = model_health(view, out)
        return out

    def _dep_status(self, view: ResolvedRegistry, alias: str, dep: Deployment) -> dict[str, Any]:
        limits = self.limits
        now, mono = time.time(), time.monotonic()
        budgets = {b.key[-1]: b for b in limits.budgets(view, alias, dep) if b.key[0] == "d"}
        with limits.lock:

            def rate(name: str) -> dict[str, Any]:
                budget = budgets.get(name)
                if budget is None:
                    return {"limit": None, "used": 0, "left": None, "quota": None}
                bucket = limits._bucket(budget)
                return {
                    "limit": round(budget.limit, 3),
                    "used": bucket.used_now(now),
                    "left": math.floor(bucket.left(now)),
                    "quota": view.quota(alias, name, dep.key),
                }

            cb = limits.breakers.get((alias, dep.key)) or _Breaker()
            health = limits.health.get((alias, dep.key)) or _Health()
            minute = int(now // 60)
            breaker = cb.state
            if cb.state == "open" and cb.until <= mono:
                breaker = "half_open"
            rpm, tpm = rate("rpm"), rate("tpm")
            in_use = limits.in_flight.get((alias, dep.key), 0)
        routing = view.registry.routing
        disabled = health.disabled_until > mono
        return {
            "name": dep.key,
            "group": dep.group,
            "enabled": bool(view.value(alias, "enabled", dep.key)),
            "account_enabled": view.registry.accounts[dep.account].enabled,
            "blocked": dep.region in routing.blocked_regions
            or dep.account in routing.blocked_accounts,
            "healthy": not disabled,
            "next_probe": time.time() + (health.disabled_until - mono) if disabled else None,
            "outside_residency": view.outside_residency(dep),
            "rpm": rpm,
            "tpm": tpm,
            "concurrency": {"limit": self.executor.concurrency, "in_use": in_use},
            "breaker": breaker,
            "latency_ms": None if health.latency_ms is None else round(health.latency_ms, 1),
            "samples": health.samples,
            "throttles_1m": max(
                health.throttles.get(minute, 0), health.throttles.get(minute - 1, 0)
            ),
            "throttles_this_minute": health.throttles.get(minute, 0),
        }

    async def health_check(self, *, fresh: bool = False) -> dict[str, Any]:
        """Liveness and readiness from this process's state (no store to probe)."""
        loaded = self.registry._current is not None
        out: dict[str, Any] = {
            "live": True,
            "ready": bool(self.ready and loaded),
            "mode": "local",
            "draining": self._paused,
            "draining_since": self._draining_since,
        }
        if self._paused:
            out["not_ready_reason"] = "draining"
        out["worker"] = self.worker_id
        out["version"] = __version__
        current = self.registry._current
        out["registry_version"] = current.registry.version if current is not None else None
        out["held"] = 0
        out["running"] = len(self.executor.running)
        out["stores"] = {}
        out["eviction_policy"] = "none"
        out["key_expiry_mode"] = "none"
        return out


def _seen(task: asyncio.Future[Any]) -> None:
    if not task.cancelled():
        task.exception()
