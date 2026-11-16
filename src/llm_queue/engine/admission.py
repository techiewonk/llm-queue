"""Admission: pick a deployment and take everything a call needs in one store call.

`Admission.pick_take` asks the store script to choose a deployment of a model by its strategy
and take the RPM / TPM budgets, the account budget, a concurrency slot of the call's class and an
in-progress entry, all or nothing, honouring waiter order and the claim. `finish` corrects
tokens and learns; `undo` gives a grant back; `renew` is the heartbeat; `sweep` clears the dead.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import orjson
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as StoreTimeoutError

from llm_queue._logging import get_logger
from llm_queue.engine import _admission_lua as lua
from llm_queue.engine.expiry import prelude
from llm_queue.engine.fairness import Fairness
from llm_queue.engine.keys import Keys
from llm_queue.engine.metrics import Metrics, NullSink
from llm_queue.tuning import EngineTimings

if TYPE_CHECKING:
    import redis.asyncio

Class = Literal["p", "n"]
Outcome = Literal["ok", "throttle", "fail", "none"]

logger = get_logger(__name__)


def owner_token(request_id: str, worker: str, attempt: int) -> str:
    """`request|worker|attempt`: identifies one grant (lease, entry) of one attempt."""
    return f"{request_id}|{worker}|{attempt}"


def position_of(message_id: str) -> float:
    """Queue position as a number (stream id `ms-seq` -> ms * 1000 + seq)."""
    ms, _, seq = message_id.partition("-")
    return float(int(ms) * 1000 + int(seq or 0))


@dataclass(frozen=True, slots=True)
class Candidate:
    """A deployment the call may use: its tier, and whether it is only a last choice."""

    key: str
    tier: Literal["confirmed", "fallback"] = "confirmed"
    last: bool = False


@dataclass(frozen=True, slots=True)
class Ask:
    """One admission request (one attempt of one call)."""

    alias: str
    request_id: str
    worker: str
    attempt: int
    cls: Class
    position: float
    candidates: Sequence[Candidate]
    strategy: str = "ordered"
    pick: str = "percent"
    cost_r: int = 1
    cost_t: int = 0
    claim: bool = True
    sticky_hash: str | None = None
    sticky_ttl_secs: float = 1800.0
    sticky_move_secs: float = 1.0
    foreign_from: str | None = None
    require_pct: int = 0
    tag: str = ""
    # Folded in (one round trip each before): the caller TTL to check the call's callers are
    # alive with (`gone` when none is), the minute of the retry-budget key a first attempt
    # counts in, and whether to report the call's chunk stream length (`streamed`).
    callers_ttl: int | None = None
    count_first: int | None = None
    chunks: bool = False
    # Per-tenant fairness (engine/fairness.py); None (fairness off) sends nothing extra.
    fairness: Fairness | None = None

    @property
    def job_owner(self) -> str:
        """Who runs the call, in its claim record: `worker@tag` (one delivery of one pod)."""
        return f"{self.worker}@{self.tag}" if self.tag else self.worker

    @property
    def owner(self) -> str:
        """The owner token of this attempt's grant (`tag` tells two deliveries apart)."""
        return owner_token(self.request_id, self.job_owner, self.attempt)


@dataclass(frozen=True, slots=True)
class Decision:
    """What the script decided.

    `kind`: granted, wait, duplicate, done, cancelled, stale_config, no_config, none (no
    deployment usable), busy (another model refused a fallback call), gone (every caller of a
    call asked with `callers_ttl` is gone), tenant_limited (the tenant's fairness quota is
    used up: `wait_secs` until it has room). A fair_queue wait carries the call's round in
    `extra["fr"]`.
    """

    kind: str
    deployment: str | None = None
    minute: int | None = None
    wait_secs: float = 0.0
    reason: str = ""
    changes: int | None = None
    tier: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


class Admission:
    """The admission scripts of one environment (key prefix)."""

    def __init__(
        self,
        client: redis.asyncio.Redis,
        keys: Keys,
        *,
        lease_secs: float = 60.0,
        waiter_secs: float = 2.0,
        rng: random.Random | None = None,
        timings: EngineTimings | None = None,
        metrics: Metrics | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        """Register the scripts; no I/O until the first call.

        `timings` (`Tuning.engine`) carries the scripts' pacing: probe and slot waits, the
        limiter's cut, budget key lifetime; each script falls back to the same defaults.
        `metrics` counts grants found lost when settled or renewed. `clock_ms` (tests only)
        replaces the store's clock in the scripts, so budgets can be checked at chosen times.
        """
        self._client = client
        self.keys = keys
        self.lease_ms = int(lease_secs * 1000)
        self.waiter_ms = int(waiter_secs * 1000)
        self._rng = rng or random.Random()
        self.metrics: Metrics = metrics or NullSink()
        self._clock_ms = clock_ms
        # Owners this process settled (FINISH or UNDO sent), oldest first: a heartbeat that
        # found one lost re-registered it, a slot nobody freed until its lease ran out.
        self._settled: dict[str, float] = {}
        self.last_epoch: str | None = None  # the store's epoch, as the last renew read it
        pace = timings or EngineTimings()
        self._t = {
            "probe_wait_ms": pace.admission_probe_wait_ms,
            "slot_wait_ms": pace.admission_slot_wait_ms,
            "slot_jitter_ms": pace.admission_slot_jitter_ms,
            "wait_ms": pace.admission_wait_ms,
            "limiter_cut_ms": pace.limiter_cut_ms,
            "limiter_cut_factor": pace.limiter_cut_factor,
            "budget_key_ms": pace.budget_key_ms,
            "throttle_base_ms": pace.throttle_base_ms,
            "throttle_max_ms": pace.throttle_max_ms,
            "slow_stale_ms": pace.slow_stale_ms,
            "job_ttl_ms": pace.job_ttl_ms,
        }
        pre = prelude(keys.prefix)  # X.expire / X.pttl / X.set (engine/expiry.py)
        self._pick = client.register_script(pre + lua.PICK_TAKE)
        self._finish = client.register_script(pre + lua.FINISH)
        self._undo = client.register_script(pre + lua.UNDO)
        self._renew = client.register_script(pre + lua.RENEW)
        self._sweep = client.register_script(pre + lua.SWEEP)
        self._reregister = client.register_script(pre + lua.REREGISTER)
        self._release = client.register_script(pre + lua.RELEASE)

    async def _call(self, script: Any, body: dict[str, Any]) -> Any:
        args: list[Any] = [self.keys.prefix, orjson.dumps({**body, "t": self._t})]
        if self._clock_ms is not None:
            args.append(int(self._clock_ms()))
        return await script([], args)

    async def pick_take(self, ask: Ask, changes: int) -> Decision:
        """Choose and take, or say how long to wait and why."""
        body: dict[str, Any] = {
            "alias": ask.alias,
            "changes": changes,
            "request_id": ask.request_id,
            "owner": ask.owner,
            "worker": ask.worker,
            "job_owner": ask.job_owner,
            "cls": ask.cls,
            "pos": ask.position,
            "cost_r": ask.cost_r,
            "cost_t": ask.cost_t,
            "candidates": [{"key": c.key, "tier": c.tier, "last": c.last} for c in ask.candidates],
            "strategy": ask.strategy,
            "pick": ask.pick,
            "claim": ask.claim,
            "lease_ms": self.lease_ms,
            "waiter_ms": self.waiter_ms,
            "rand": self._rng.random(),
        }
        if ask.sticky_hash:
            body["sticky"] = {
                "hash": ask.sticky_hash,
                "ttl_ms": int(ask.sticky_ttl_secs * 1000),
                "move_ms": int(ask.sticky_move_secs * 1000),
            }
        if ask.foreign_from is not None:
            body["foreign"] = {"from_alias": ask.foreign_from, "require_pct": ask.require_pct}
        if ask.callers_ttl is not None:
            body["callers"] = ask.callers_ttl
        if ask.count_first is not None:
            body["count_first"] = ask.count_first
        if ask.chunks:
            body["chunks"] = True
        if ask.fairness is not None:
            body["tf"] = ask.fairness.body()
        raw = orjson.loads(await self._call(self._pick, body))
        kind = str(raw.pop("r"))
        return Decision(
            kind=kind,
            deployment=raw.pop("dep", None),
            minute=raw.pop("minute", None),
            wait_secs=float(raw.pop("ms", 0)) / 1000.0,
            reason=str(raw.pop("reason", "")),
            changes=raw.pop("changes", None),
            tier=raw.pop("tier", None),
            extra=raw,
        )

    async def finish(
        self,
        alias: str,
        owner: str,
        request_id: str,
        *,
        refund_r: int,
        refund_t: int,
        outcome: Outcome,
        latency_ms: float = -1,
        retry_after_secs: float | None = None,
    ) -> bool:
        """Settle a grant: give back what was not used, release the slot, learn the outcome.

        `refund_t` may be negative (the call used more than it reserved). A `throttle` keeps the
        deployment out for every pod until max(`retry_after_secs`, the throttle backoff). If the
        store is unreachable this is skipped (False): the grant's lease expires and SWEEP frees
        the slot, so a store blip after the provider answered never throws the answer away.
        """
        self._settle(owner)
        body = {
            "alias": alias,
            "owner": owner,
            "request_id": request_id,
            "refund_r": refund_r,
            "refund_t": refund_t,
            "outcome": outcome,
            "latency_ms": latency_ms,
            "retry_after_ms": int((retry_after_secs or 0) * 1000),
            "rand": self._rng.random(),
        }
        try:
            settled = bool(await self._call(self._finish, body))
        except (StoreConnectionError, StoreTimeoutError, ReadOnlyError, OSError) as exc:
            logger.warning("finish_skipped", model=alias, request_id=request_id, error=str(exc))
            return False
        if not settled:  # swept as dead, taken over, or lost with the store (RESCAN A6)
            self.grant_lost(alias, request_id)
        return settled

    def finish_args(
        self,
        alias: str,
        owner: str,
        request_id: str,
        *,
        refund_r: int,
        refund_t: int,
        outcome: Outcome,
        latency_ms: float = -1,
    ) -> list[Any]:
        """FINISH's arguments (prefix, JSON, clock) for a script that runs it with others.

        `RequestStore.put_result_settled` stores the result, acks and finishes in one round
        trip (FL-22). The owner counts as settled from here, as in `finish`.
        """
        self._settle(owner)
        body = {
            "alias": alias,
            "owner": owner,
            "request_id": request_id,
            "refund_r": refund_r,
            "refund_t": refund_t,
            "outcome": outcome,
            "latency_ms": latency_ms,
            "retry_after_ms": 0,
            "rand": self._rng.random(),
            "t": self._t,
        }
        clock = "" if self._clock_ms is None else int(self._clock_ms())
        return [self.keys.prefix, orjson.dumps(body), clock]

    def grant_lost(self, alias: str, request_id: str) -> None:
        """Log and count a grant found gone when it was settled (swept, taken over, reset)."""
        logger.warning("grant_lost", model=alias, request_id=request_id, step="finish")
        self.metrics.count("llmq_grants_lost_total", model=alias, step="finish")

    async def undo(
        self, alias: str, owner: str, request_id: str, *, keep_claim: bool = False
    ) -> bool:
        """Give a grant back entirely, without learning (lost reply, refused claim).

        The call's claim is released too (FL-1: another pod waited a lease for it), unless
        `keep_claim` says this worker still holds the call. Like `finish`, skipped (False,
        logged) when the store is unreachable: the lease expires and SWEEP frees the slot.
        """
        self._settle(owner)
        body: dict[str, Any] = {"alias": alias, "owner": owner, "request_id": request_id}
        if keep_claim:
            body["keep_claim"] = True
        try:
            return bool(await self._call(self._undo, body))
        except (StoreConnectionError, StoreTimeoutError, ReadOnlyError, OSError) as exc:
            logger.warning("undo_skipped", model=alias, request_id=request_id, error=str(exc))
            return False

    async def release_claims(self, jobs: Sequence[tuple[str, str]]) -> int:
        """Release the claims of calls given back with no grant: (request id, job owner) each.

        The drain's hand-back of waiting calls: their claim stayed `running` and another pod
        got `duplicate` for a lease (FL-1). A claim another owner holds is left alone.
        """
        if not jobs:
            return 0
        body = {"jobs": [{"request_id": r, "owner": o} for r, o in jobs]}
        return int(await self._call(self._release, body))

    async def renew(self, items: Sequence[tuple[str, str, str]]) -> int:
        """Extend leases of held grants: (alias, owner, request id) each; how many were held."""
        renewed, _ = await self.renew_all(items)
        return renewed

    async def renew_all(
        self,
        items: Sequence[tuple[str, str, str]],
        jobs: Sequence[tuple[str, str]] = (),
        *,
        caller: tuple[str, int] | None = None,
    ) -> tuple[int, list[str]]:
        """The heartbeat: extend grants, refresh the claims of `jobs` (request id, job owner).

        Returns how many grants were renewed and the owners whose entry was gone (lost). An
        owner settled while this beat ran is not lost (its entry went with its FINISH or UNDO).
        `caller` (key, ttl secs) also sets the process's caller key in the same script, and
        `last_epoch` keeps the store's epoch it read (FL-21: two round trips of their own).
        """
        if not items and not jobs and caller is None:
            return 0, []
        body: dict[str, Any] = {
            "lease_ms": self.lease_ms,
            "items": [{"alias": a, "owner": o, "request_id": r} for a, o, r in items],
            "jobs": [{"request_id": r, "owner": o} for r, o in jobs],
        }
        if caller is not None:
            body["caller"] = {"key": caller[0], "ttl": int(caller[1])}
        raw = orjson.loads(await self._call(self._renew, body))
        self.last_epoch = str(raw.get("epoch") or "") or None
        lost = raw.get("lost") or []
        lost = (
            [str(o) for o in lost if str(o) not in self._settled] if isinstance(lost, list) else []
        )
        if lost:
            logger.warning("grant_lost", step="renew", count=len(lost))
            # Same label set as `finish` (model, step): prometheus fixes a metric's label
            # names on first use, and the other form raised there (EN-1).
            alias_of = {owner: alias for alias, owner, _ in items}
            per_model: dict[str, int] = {}
            for owner in lost:
                alias = alias_of.get(owner, "*")
                per_model[alias] = per_model.get(alias, 0) + 1
            for alias, n in per_model.items():
                self.metrics.count("llmq_grants_lost_total", float(n), model=alias, step="renew")
        return int(raw.get("n", 0)), lost

    def _settle(self, owner: str) -> None:
        """Remember a settled owner (before its script runs) for two leases, then forget it."""
        now = time.monotonic()
        # Re-settling moves the owner to the end; it kept its old place and stopped the
        # pruning below until it aged out (EN-17).
        self._settled.pop(owner, None)
        self._settled[owner] = now
        horizon = now - 2 * self.lease_ms / 1000
        while (oldest := next(iter(self._settled))) != owner and self._settled[oldest] < horizon:
            del self._settled[oldest]

    async def reregister(self, items: Sequence[dict[str, Any]]) -> int:
        """After a store reset, put back the slots and entries of running calls.

        An owner this process already settled is skipped: its call ended while the heartbeat
        found its entry gone, and putting it back held a slot nobody frees until the lease ends.
        """
        items = [item for item in items if item["owner"] not in self._settled]
        if not items:
            return 0
        body = {"lease_ms": self.lease_ms, "items": list(items)}
        return int(await self._call(self._reregister, body))

    async def sweep(self, alias: str, deployments: Sequence[str] = ()) -> int:
        """Remove in-progress entries of owners that stopped renewing; lapsed leases too.

        `deployments`: the model's deployment keys, whose leases past their end are removed
        even when their in-progress entry is already gone (scenario L4 left such leases).
        """
        body = {"alias": alias, "deps": list(deployments), "lease_ms": self.lease_ms}
        return int(await self._call(self._sweep, body))
