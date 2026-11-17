"""The admin side of requests (0.2.0 T): the request index settings and the `RequestsAdmin` service.

Calls are indexed per model and tenant (`P:{m:ALIAS}:tq:H`, see `requests.py` `_INDEX_FN`), so an
operator can list, count, cancel, reprioritize and extend one tenant's calls on one model
without touching another tenant's. Tenants are named by their keyed hash only
(`LLMQ_ID_HASH_SALT`); a raw name given here is hashed and never stored or logged. Every change
is a dry run unless `dry_run=False` (owner D3), takes an `actor` and writes one audit entry.

`RequestsAdmin` is built from the stores alone (like `PurgeService`), so the CLI, the HTTP
routes and an admin worker share it without an engine (`RequestsAdmin.from_env`).
"""

from __future__ import annotations

import builtins
import contextlib
import heapq
import os
import re
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from typing import TYPE_CHECKING, Any, Literal, Self

import orjson

from llm_queue._logging import get_logger
from llm_queue.engine._codec import text as _text
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import RETENTION_SECS, StreamQueues
from llm_queue.engine.requests import FINAL_STATES, RequestStore
from llm_queue.engine.store_guard import store_op
from llm_queue.errors import AdminRefused, ConfigError
from llm_queue.registry._lua import AUDIT_MAX_ENTRIES

if TYPE_CHECKING:
    import redis.asyncio

    from llm_queue.engine.metrics import Metrics
    from llm_queue.engine.settings import InfraSettings
    from llm_queue.registry.resolve import ResolvedRegistry
    from llm_queue.registry.store import RegistryStore

logger = get_logger(__name__)

State = Literal["waiting", "running", "all"]
Stage = Literal["queued", "delayed", "held", "direct", "running"]
STATES: tuple[State, ...] = ("waiting", "running", "all")
STAGES: tuple[Stage, ...] = ("queued", "delayed", "held", "direct", "running")
NO_TENANT = "-"  # the index's tenant of a call with no tenant id
MAX_CANCEL_IDS = 1_000  # request ids one cancel may name
SAMPLE_IDS = 20  # ids a change report lists
_HASH_RE = re.compile(r"^[0-9a-f]{16}\Z")


@dataclass(frozen=True, slots=True)
class AdminSettings:
    """The request index and admin operation settings (`LLMQ_*`, design 0.2.0 section 7).

    `request_index`: calls are indexed per model and tenant (`LLMQ_REQUEST_INDEX_ENABLED`).
    `tenant_names`: tenant names are stored beside their hashes (`LLMQ_TENANT_NAMES_ENABLED`).
    `batch`: ids per pipeline or cancel script (`LLMQ_ADMIN_BATCH`); `scan_max`: index entries
    or messages one operation reads (`LLMQ_ADMIN_SCAN_MAX`); `list_max`: the largest list page
    (`LLMQ_REQUESTS_LIST_MAX`); `move_batch` / `move_bytes`: messages and bytes per reprioritize
    script run (`LLMQ_REQUESTS_MOVE_BATCH` / `_BYTES`); `metrics_tenants`: (tenant name, label)
    pairs that get a `tenant` metric label of their own (`LLMQ_METRICS_TENANTS`).
    """

    request_index: bool = True
    tenant_names: bool = False
    batch: int = 500
    scan_max: int = 100_000
    list_max: int = 1_000
    move_batch: int = 50
    move_bytes: int = 8_388_608
    metrics_tenants: tuple[tuple[str, str], ...] = ()

    @classmethod
    def from_infra(cls, settings: InfraSettings) -> AdminSettings:
        """The admin part of the process settings."""
        return cls(
            request_index=settings.request_index_enabled,
            tenant_names=settings.tenant_names_enabled,
            batch=int(settings.admin_batch),
            scan_max=int(settings.admin_scan_max),
            list_max=int(settings.requests_list_max),
            move_batch=int(settings.requests_move_batch),
            move_bytes=int(settings.requests_move_bytes),
            metrics_tenants=tuple(settings.metrics_tenants),
        )


# ---------------------------------------------------------------- reports


@dataclass(frozen=True, slots=True)
class RequestSummary:
    """One call in flight, as the index and its records show it (never its prompt).

    `tenant` and `user` are keyed hashes (`-`: no tenant); `tenant_name` only with
    `LLMQ_TENANT_NAMES_ENABLED`. `stage`: `queued` (in its queue, visible), `delayed` (returned
    with a delay still to run), `held` (taken by a worker, waiting for budget), `direct` (runs
    in its caller's process), `running` (granted). `cancelling`: a cancel is pending.
    `deadline_in_secs`: seconds to the call's deadline (None when not indexed).
    """

    request_id: str
    model: str
    tenant: str
    state: Literal["waiting", "running"]
    stage: Stage
    priority: bool
    age_secs: float
    deadline_in_secs: float | None
    deployment: str | None = None
    user: str | None = None
    cancelling: bool = False
    message_id: str | None = None
    tenant_name: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Plain JSON types."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RequestPage:
    """A page of `list`: its items, the cursor of the next page (None: the end), ids read."""

    items: list[RequestSummary]
    next: str | None
    scanned: int

    def as_dict(self) -> dict[str, Any]:
        """Plain JSON types."""
        return {
            "items": [i.as_dict() for i in self.items],
            "next": self.next,
            "scanned": self.scanned,
        }


@dataclass(frozen=True, slots=True)
class RequestCounts:
    """Counts of one model (and tenant) by state and stage.

    `truncated`: more than `LLMQ_ADMIN_SCAN_MAX` calls are indexed; the counts cover the first
    ones (by deadline) and `total` is the index's own count.
    """

    model: str
    tenant: str | None
    total: int
    by_state: dict[str, int]
    by_stage: dict[str, int]
    cancelling: int
    oldest_wait_secs: float | None
    truncated: bool

    def as_dict(self) -> dict[str, Any]:
        """Plain JSON types."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RequestDetail:
    """One call: its summary while in flight (None once final), status record and callers."""

    summary: RequestSummary | None
    status: dict[str, str]
    final: bool
    callers: int
    result_ready: bool

    def as_dict(self) -> dict[str, Any]:
        """Plain JSON types."""
        return {
            "summary": self.summary.as_dict() if self.summary else None,
            "status": dict(self.status),
            "final": self.final,
            "callers": self.callers,
            "result_ready": self.result_ready,
        }


@dataclass(frozen=True, slots=True)
class ChangeReport:
    """What a change did (or, `dry_run`, would do): JSON-ready with `as_dict()`.

    `matched`: calls selected; `changed`: calls changed; `skipped`: reason -> count;
    `by_state`: the matched calls by state; `sample_ids`: at most 20 matched ids; `at`: wall
    time. `tenant` is a hash (never a name).
    """

    op: str
    model: str | None
    tenant: str | None
    dry_run: bool
    actor: str
    matched: int = 0
    changed: int = 0
    skipped: dict[str, int] = field(default_factory=dict)
    by_state: dict[str, int] = field(default_factory=dict)
    sample_ids: list[str] = field(default_factory=list)
    at: float = field(default_factory=time.time)

    def as_dict(self) -> dict[str, Any]:
        """Plain JSON types."""
        return asdict(self)


@dataclass(slots=True)
class _Row:
    """An index entry being read: (deadline ms, request id, tenant hash) and its records."""

    score: float
    rid: str
    tenant: str
    mid: str = ""
    cls: str = ""
    job: dict[str, str] = field(default_factory=dict)
    status: dict[str, str] = field(default_factory=dict)
    cancelling: bool = False
    stage: Stage | None = None  # None: final or gone
    deployment: str | None = None


def _decode(raw: Any) -> dict[str, str]:
    return {_text(k): _text(v) for k, v in (raw or {}).items()}


def _cursor(score: float, rid: str) -> str:
    return f"{int(score)}:{rid}"


def _parse_cursor(after: str | None) -> tuple[float, str]:
    if after is None:
        return float("-inf"), ""
    head, sep, rid = after.partition(":")
    try:
        score = float(int(head))
    except ValueError:
        score = float("nan")
    if not sep or score != score:  # NaN: not a number
        raise ConfigError(f"after must be a cursor from a previous page (`ms:id`), not {after!r}")
    return score, rid


class RequestsAdmin:
    """List, count, get, cancel, reprioritize and extend calls by model and tenant (T2).

    Reads are read-only pipelines; writes are scripts (`CANCEL`, `MOVE`, `EXTEND`), each in
    batches of `LLMQ_ADMIN_BATCH`. Every write is a dry run unless `dry_run=False`.
    """

    def __init__(
        self,
        state: redis.asyncio.Redis,
        *,
        queue_client: redis.asyncio.Redis | None = None,
        keys: Keys,
        registry: RegistryStore,
        queue_prefix: str = "",
        hash_id: Callable[[str], str],
        settings: AdminSettings | None = None,
        status_ttl_secs: int = 1800,
        caller_margin_secs: float = 5.0,
        metrics: Metrics | None = None,
        owner: str | None = None,
    ) -> None:
        """Wire the stores; no I/O until the first call."""
        self._state = state
        self._queue_client = queue_client or state
        self.queues_shared = queue_client is None or queue_client is state
        self.keys = keys
        self.registry = registry
        self.queue_prefix = queue_prefix
        self._hash = hash_id
        self.settings = settings or AdminSettings()
        self.status_ttl_secs = status_ttl_secs
        self.caller_margin_secs = caller_margin_secs
        self._metrics = metrics
        self._owner = owner or f"admin-{os.getpid()}"
        self._queues = StreamQueues(self._queue_client, keys)
        self._requests = RequestStore(state, keys, status_ttl_secs=status_ttl_secs)
        self._owned: builtins.list[Any] = []

    # ---------------------------------------------------------------- construction

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        secrets: Callable[[str], str | None] | None = None,
    ) -> RequestsAdmin:
        """A standalone service from `LLMQ_*` (CLI, admin worker); close it with `aclose()`."""
        from llm_queue.engine.purge import keyed_hash
        from llm_queue.engine.settings import InfraSettings
        from llm_queue.engine.store_client import secret_store_client
        from llm_queue.registry.store import RegistryStore
        from llm_queue.tuning import Tuning

        env = dict(os.environ if environ is None else environ)
        settings = InfraSettings.from_env(env, secrets=secrets)
        cap, options = settings.store_max_connections, Tuning().store
        lookup = secrets or env.get
        state = secret_store_client(settings.store_url, lookup, "LLMQ_STORE_PASSWORD", cap, options)
        queue_client = (
            secret_store_client(
                settings.queue_store_url, lookup, "LLMQ_QUEUE_STORE_PASSWORD", cap, options
            )
            if settings.queue_store_url
            else None
        )
        registry = RegistryStore(
            state,
            settings.key_prefix,
            live_edit=settings.registry_live_edit,
            allowed_hosts=settings.base_url_allowed_hosts,
            audit_retention_days=settings.audit_retention_days,
        )
        salt = settings.id_hash_salt or ""
        service = cls(
            state,
            queue_client=queue_client,
            keys=Keys(settings.key_prefix),
            registry=registry,
            queue_prefix=settings.queue_prefix,
            hash_id=lambda value: keyed_hash(salt, value),
            settings=AdminSettings.from_infra(settings),
            status_ttl_secs=settings.status_ttl_secs,
        )
        service._owned = [c for c in (queue_client, state) if c is not None]
        return service

    async def aclose(self) -> None:
        """Close the clients `from_env` made (none otherwise)."""
        async with contextlib.AsyncExitStack() as stack:
            for client in self._owned:
                stack.push_async_callback(client.aclose)
        self._owned = []

    async def __aenter__(self) -> Self:
        """Use as `async with RequestsAdmin.from_env() as admin:`."""
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close what `from_env` opened."""
        await self.aclose()

    # ---------------------------------------------------------------- reads

    @store_op
    async def list(
        self,
        model: str,
        *,
        tenant: str | None = None,
        tenant_hash: str | None = None,
        state: State = "all",
        limit: int = 100,
        after: str | None = None,
    ) -> RequestPage:
        """Calls in flight of `model` (alias or model id), oldest deadline first.

        `tenant` (a raw name, hashed here) or `tenant_hash` (16 hex, or `-` for calls with no
        tenant; both is a `ConfigError`); neither: every tenant. `state`: `waiting`, `running`
        or `all`. `limit` 1 to `LLMQ_REQUESTS_LIST_MAX`; `after` the `next` of the previous page.
        Reads at most `LLMQ_ADMIN_SCAN_MAX` entries per page (`next` then goes on from there).
        """
        view = await self._view()
        alias = view.registry.alias_of(model)
        h = self._tenant(tenant, tenant_hash)
        self._check_state(state)
        if not 1 <= limit <= self.settings.list_max:
            raise ConfigError(f"limit must be 1 to {self.settings.list_max}")
        start, after_rid = _parse_cursor(after)
        self._need_index(state)
        items: builtins.list[RequestSummary] = []
        scanned = 0
        last: tuple[float, str] | None = None
        names = await self._names(alias)
        chunk = max(10, min(self.settings.batch, limit * 2))
        async for rows in self._rows(view, alias, h, start, after_rid, chunk):
            for row in rows:
                scanned += 1
                last = (row.score, row.rid)
                summary = self._summary(view, alias, row, names)
                if summary is not None and state in ("all", summary.state):
                    items.append(summary)
                    if len(items) >= limit:
                        return RequestPage(items, _cursor(row.score, row.rid), scanned)
                if scanned >= self.settings.scan_max:
                    return RequestPage(items, _cursor(*last), scanned)
        return RequestPage(items, None, scanned)

    @store_op
    async def count(
        self, model: str, *, tenant: str | None = None, tenant_hash: str | None = None
    ) -> RequestCounts:
        """Calls in flight of `model` (and one tenant) by state and stage.

        Reads every index entry up to `LLMQ_ADMIN_SCAN_MAX` (`truncated` above it).
        """
        view = await self._view()
        alias = view.registry.alias_of(model)
        h = self._tenant(tenant, tenant_hash)
        indexed = await self._indexed(alias, h)
        by_state = dict.fromkeys(("waiting", "running"), 0)
        by_stage: dict[str, int] = dict.fromkeys(STAGES, 0)
        cancelling = scanned = 0
        oldest: float | None = None
        now = await self._now()
        async for rows in self._rows(view, alias, h, float("-inf"), "", self.settings.batch):
            for row in rows:
                scanned += 1
                summary = self._summary(view, alias, row, {}, now=now)
                if summary is not None:
                    by_state[summary.state] += 1
                    by_stage[summary.stage] += 1
                    cancelling += int(summary.cancelling)
                    if summary.state == "waiting":
                        oldest = max(oldest or 0.0, summary.age_secs)
                if scanned >= self.settings.scan_max:
                    break
            if scanned >= self.settings.scan_max:
                break
        truncated = indexed > self.settings.scan_max
        return RequestCounts(
            model=alias,
            tenant=h,
            total=indexed if truncated else by_state["waiting"] + by_state["running"],
            by_state=by_state,
            by_stage=by_stage,
            cancelling=cancelling,
            oldest_wait_secs=None if oldest is None else round(oldest, 3),
            truncated=truncated,
        )

    @store_op
    async def get(self, request_id: str) -> RequestDetail | None:
        """One call by id (None when the store knows nothing of it)."""
        k = self.keys
        async with self._state.pipeline(transaction=False) as pipe:
            pipe.hgetall(k.status(request_id))
            pipe.hgetall(k.request(request_id, "job"))
            pipe.exists(k.request(request_id, "cancel"))
            pipe.scard(k.request(request_id, "callers"))
            pipe.exists(k.request(request_id, "res"))
            status_raw, job_raw, cancel, callers, res = await pipe.execute()
        status, job = _decode(status_raw), _decode(job_raw)
        if not status and not job:
            return None
        state = job.get("state") or status.get("state", "")
        final = state in FINAL_STATES or status.get("state") in FINAL_STATES
        summary = None
        alias = status.get("model")
        if not final and job and alias:
            view = await self._view()
            score = None
            if job.get("ix"):
                score = await self._state.zscore(job["ix"], request_id)
            row = _Row(
                float(score) if score is not None else float("nan"),
                request_id,
                job.get("ixh") or NO_TENANT,
                job=job,
                status=status,
                cancelling=bool(cancel),
            )
            await self._stages(view, alias, [row])
            summary = self._summary(view, alias, row, await self._names(alias))
        return RequestDetail(
            summary=summary,
            status=status,
            final=final,
            callers=int(callers),
            result_ready=bool(res),
        )

    # ---------------------------------------------------------------- writes

    @store_op
    async def cancel(
        self,
        model: str | None = None,
        *,
        request_ids: Sequence[str] | None = None,
        tenant: str | None = None,
        tenant_hash: str | None = None,
        state: State = "waiting",
        actor: str,
        dry_run: bool = True,
    ) -> ChangeReport:
        """Cancel calls by id, or by model, tenant and state (default `waiting`, owner D9).

        Cancel wins for every caller of an id. With `state="waiting"` a call granted between
        the read and the write is left running (`skipped.started`); a call already being
        cancelled is skipped (`already_cancelling`), so a second run changes nothing. A dry run
        (the default) writes nothing; a real one writes one audit entry with counts.
        """
        self._check_state(state)
        if request_ids is not None and (tenant is not None or tenant_hash is not None):
            raise ConfigError("give request_ids, or a model and tenant: not both")
        if request_ids is None and model is None:
            raise ConfigError("give a model (and a tenant), or request_ids")
        h: str | None = None
        alias: str | None = None
        view = await self._view() if model is not None else None
        if view is not None and model is not None:
            alias = view.registry.alias_of(model)
        rows: builtins.list[_Row] = []
        skipped: dict[str, int] = {}
        if request_ids is not None:
            ids = list(dict.fromkeys(request_ids))
            if not 1 <= len(ids) <= MAX_CANCEL_IDS:
                raise ConfigError(f"request_ids must name 1 to {MAX_CANCEL_IDS} ids")
            rows = await self._rows_by_id(ids)
        else:
            assert view is not None
            assert alias is not None
            h = self._tenant(tenant, tenant_hash)
            self._need_index(state)
            await self._refuse_above_cap(alias, h)
            async for chunk in self._rows(view, alias, h, float("-inf"), "", self.settings.batch):
                rows += chunk
        chosen: builtins.list[_Row] = []
        for row in rows:
            row_state = self._state_of(row)
            if row_state is None:
                if request_ids is not None:
                    _bump(skipped, "final")
                continue
            if row.cancelling:
                _bump(skipped, "already_cancelling")
                continue
            if state != "all" and row_state != state:
                _bump(skipped, "other_state")
                continue
            chosen.append(row)
        report = ChangeReport(
            op="requests_cancel",
            model=alias,
            tenant=h,
            dry_run=dry_run,
            actor=actor,
            matched=len(chosen),
            skipped=skipped,
            by_state=_by_state(chosen, self._state_of),
            sample_ids=[r.rid for r in chosen[:SAMPLE_IDS]],
        )
        if dry_run or not chosen:
            return report
        only = {"waiting": "queued", "running": "running"}.get(state)
        changed = 0
        ids = [r.rid for r in chosen]
        for start in range(0, len(ids), self.settings.batch):
            batch = ids[start : start + self.settings.batch]
            marked, moved_on = await self._requests.cancel_counts(
                batch,
                ttl_secs=self.status_ttl_secs,
                channel=self.keys.cancel_channel,
                only_state=only,
            )
            changed += marked
            if moved_on:
                _bump(skipped, "started" if state == "waiting" else "not_running", moved_on)
            for rid in batch:
                logger.info("call_cancel", request_id=rid, source="admin", by=actor)
        if self._metrics is not None:
            self._metrics.count("llmq_cancels_total", float(changed), source="admin")
        report = _replace(report, changed=changed, skipped=skipped)
        await self._audit(report)
        return report

    @store_op
    async def reprioritize(
        self,
        model: str,
        *,
        priority: bool,
        tenant: str | None = None,
        tenant_hash: str | None = None,
        limit: int | None = None,
        actor: str,
        dry_run: bool = True,
    ) -> ChangeReport:
        """Move waiting calls of `model` (and a tenant) to the priority (or normal) queue.

        Only calls still in their queue move: a call held by a worker waiting for budget is
        skipped (`held`, owner D7), as are `delayed`, `direct`, `cancelling` ones and those
        already in the target class (`already`). A moved call keeps its deadline,
        `max_wait_secs` and its place among waiting calls by arrival (`pos`, owner Q9).
        `limit`: at most this many (oldest first). Dry run by default.
        """
        view = await self._view()
        alias = view.registry.alias_of(model)
        h = self._tenant(tenant, tenant_hash)
        if limit is not None and limit < 1:
            raise ConfigError("limit must be 1 or more")
        self._need_index("waiting")
        await self._refuse_above_cap(alias, h)
        target = "p" if priority else "n"
        skipped: dict[str, int] = {}
        chosen: builtins.list[_Row] = []
        async for rows in self._rows(view, alias, h, float("-inf"), "", self.settings.batch):
            for row in rows:
                if row.stage is None or row.stage == "running":
                    continue  # final, gone or running: not a waiting call
                if row.cancelling:
                    _bump(skipped, "cancelling")
                elif row.cls == target:
                    _bump(skipped, "already")
                elif row.stage != "queued" or not row.mid:
                    _bump(skipped, row.stage if row.mid else "direct")
                elif limit is None or len(chosen) < limit:
                    chosen.append(row)
        report = ChangeReport(
            op="requests_reprioritize",
            model=alias,
            tenant=h,
            dry_run=dry_run,
            actor=actor,
            matched=len(chosen),
            skipped=skipped,
            by_state={"waiting": len(chosen)} if chosen else {},
            sample_ids=[r.rid for r in chosen[:SAMPLE_IDS]],
        )
        if dry_run or not chosen:
            return report
        queue = view.value(alias, "queue")
        base = self.queue_prefix + view.queue_name(alias)
        source, dest = (base, f"{base}-priority") if priority else (f"{base}-priority", base)
        pairs = [(r.rid, r.mid) for r in chosen]
        changed = 0
        step = self.settings.move_batch
        at = 0
        while at < len(pairs):
            part = pairs[at : at + step]
            result = await self._queues.move(
                source,
                dest,
                part,
                target_class=target,
                retention_secs=int(queue.get("retention_secs", RETENTION_SECS)),
                max_waiting=int(view.value(alias, "max_waiting")),
                max_bytes=self.settings.move_bytes,
                prefix=self.keys.prefix if self.queues_shared else "",
            )
            if result.moved and not self.queues_shared:
                await self._moved_records(result.moved, target)
            for rid, new in result.moved:
                logger.info("request_moved", request_id=rid, model=alias, to=target, mid=new)
            changed += len(result.moved)
            for _, why in result.skipped:
                _bump(skipped, why)
            if any(why == "full" for _, why in result.skipped):
                break
            at += max(1, result.looked)
        report = _replace(report, changed=changed, skipped=skipped)
        await self._audit(report)
        return report

    # ---------------------------------------------------------------- helpers: reading rows

    async def _view(self) -> ResolvedRegistry:
        with contextlib.suppress(ConfigError):
            return self.registry.current
        view = await self.registry.load()
        if view is None:
            raise ConfigError("no registry is published yet (llm-queue registry publish)")
        return view

    def _tenant(self, tenant: str | None, tenant_hash: str | None) -> str | None:
        """The tenant hash asked for (None: every tenant); a raw name is hashed here."""
        if tenant is not None and tenant_hash is not None:
            raise ConfigError("give tenant or tenant_hash, not both")
        if tenant is not None:
            if not tenant:
                raise ConfigError("tenant must not be empty")
            return self._hash(tenant)
        if tenant_hash is not None and tenant_hash != NO_TENANT and not _HASH_RE.match(tenant_hash):
            raise ConfigError("tenant_hash must be 16 hex characters, or - for no tenant")
        return tenant_hash

    @staticmethod
    def _check_state(state: str) -> None:
        if state not in STATES:
            raise ConfigError(f"state must be one of {', '.join(STATES)}")

    def _need_index(self, state: str) -> None:
        if not self.settings.request_index:
            raise AdminRefused(
                f"finding {state} calls by model and tenant needs the request index "
                "(LLMQ_REQUEST_INDEX_ENABLED is off)"
            )

    async def _now(self) -> float:
        secs, micros = await self._state.time()
        return int(secs) + int(micros) / 1_000_000

    async def _tenants(self, alias: str, h: str | None, now_ms: float) -> builtins.list[str]:
        if h is not None:
            return [h]
        raw = await self._state.zrangebyscore(
            self.keys.tenant_index(alias), now_ms, "+inf", start=0, num=self.settings.scan_max
        )
        return [_text(m) for m in raw]

    async def _indexed(self, alias: str, h: str | None) -> int:
        """Index entries still in time for one tenant (or every tenant of the model)."""
        now_ms = (await self._now()) * 1000
        tenants = await self._tenants(alias, h, now_ms)
        if not tenants:
            return 0
        async with self._state.pipeline(transaction=False) as pipe:
            for t in tenants:
                pipe.zcount(self.keys.request_index(alias, t), now_ms, "+inf")
            return sum(int(n) for n in await pipe.execute())

    async def _refuse_above_cap(self, alias: str, h: str | None) -> None:
        indexed = await self._indexed(alias, h)
        if indexed > self.settings.scan_max:
            raise AdminRefused(
                f"{indexed} calls are indexed, above LLMQ_ADMIN_SCAN_MAX "
                f"({self.settings.scan_max}): narrow it to one tenant, or raise the cap"
            )

    async def _rows(
        self,
        view: ResolvedRegistry,
        alias: str,
        h: str | None,
        start: float,
        after_rid: str,
        chunk: int,
    ) -> AsyncIterator[builtins.list[_Row]]:
        """Index entries after (`start`, `after_rid`), oldest deadline first, with records.

        One tenant: its `tq:H` in chunks. Every tenant: the tenants in `tq`, their entries
        merged by (deadline, id). Entries past their deadline are never read.
        """
        now_ms = (await self._now()) * 1000
        tenants = await self._tenants(alias, h, now_ms)
        cursors = {t: (max(start, now_ms), after_rid if start >= now_ms else "") for t in tenants}
        buffers: dict[str, builtins.list[tuple[float, str]]] = {t: [] for t in tenants}
        done: set[str] = set()

        async def refill(names: Sequence[str], num: int = chunk + 1) -> None:
            async with self._state.pipeline(transaction=False) as pipe:
                for t in names:
                    pipe.zrangebyscore(
                        self.keys.request_index(alias, t),
                        cursors[t][0],
                        "+inf",
                        start=0,
                        num=num,
                        withscores=True,
                    )
                replies = await pipe.execute()
            wider = []
            for t, raw in zip(names, replies, strict=True):
                entries = [(float(s), _text(m)) for m, s in raw]
                buffers[t] = [e for e in entries if e > cursors[t]]
                if len(entries) < num:
                    done.add(t)
                elif not buffers[t]:  # a whole read at one score already seen (ms ties)
                    wider.append(t)
            if wider:
                await refill(wider, num * 2)

        await refill(tenants)
        heap = [(buf[0][0], buf[0][1], t, 0) for t, buf in buffers.items() if buf]
        heapq.heapify(heap)
        out: builtins.list[_Row] = []
        while heap:
            score, rid, t, i = heapq.heappop(heap)
            out.append(_Row(score, rid, t))
            cursors[t] = (score, rid)
            if i + 1 < len(buffers[t]):
                nxt = buffers[t][i + 1]
                heapq.heappush(heap, (nxt[0], nxt[1], t, i + 1))
            elif t not in done:
                await refill([t])
                if buffers[t]:
                    heapq.heappush(heap, (buffers[t][0][0], buffers[t][0][1], t, 0))
            if len(out) >= chunk:
                await self._records(out)
                await self._stages(view, alias, out)
                yield out
                out = []
        if out:
            await self._records(out)
            await self._stages(view, alias, out)
            yield out

    async def _rows_by_id(self, ids: Sequence[str]) -> builtins.list[_Row]:
        rows = [_Row(float("nan"), rid, NO_TENANT) for rid in ids]
        await self._records(rows)
        view = await self._view()
        by_model: dict[str, builtins.list[_Row]] = {}
        for row in rows:
            row.tenant = row.job.get("ixh") or NO_TENANT
            model = row.status.get("model")
            if model and model in view.registry.models:
                by_model.setdefault(model, []).append(row)
        for alias, part in by_model.items():
            await self._stages(view, alias, part)
        for row in rows:
            if row.stage is None and self._state_of(row) is not None:
                row.stage = "running" if row.job.get("state") == "running" else "queued"
        return rows

    async def _records(self, rows: Sequence[_Row]) -> None:
        """Fill each row's job, status and cancel mark (one pipelined round trip per batch)."""
        k = self.keys
        for start in range(0, len(rows), self.settings.batch):
            part = rows[start : start + self.settings.batch]
            async with self._state.pipeline(transaction=False) as pipe:
                for row in part:
                    pipe.hgetall(k.request(row.rid, "job"))
                    pipe.hmget(
                        k.status(row.rid),
                        ["state", "model", "priority", "enqueued_at", "served_model"],
                    )
                    pipe.exists(k.request(row.rid, "cancel"))
                replies = await pipe.execute()
            for i, row in enumerate(part):
                job, status, cancel = replies[3 * i : 3 * i + 3]
                row.job = _decode(job)
                names = ("state", "model", "priority", "enqueued_at", "served_model")
                row.status = {n: _text(v) for n, v in zip(names, status, strict=True) if v}
                row.cancelling = bool(cancel)
                row.mid = row.job.get("mid", "")
                row.cls = row.job.get("q", "")

    async def _stages(self, view: ResolvedRegistry, alias: str, rows: Sequence[_Row]) -> None:
        """The stage of each row: from its job, its message's records and its grant."""
        waiting = [
            r
            for r in rows
            if r.job.get("state") == "queued" and r.mid and r.job.get("direct") != "1"
        ]
        running = [r for r in rows if r.job.get("state") == "running"]
        base = self.queue_prefix + view.queue_name(alias)
        if waiting:
            async with self._queue_client.pipeline(transaction=False) as pipe:
                for r in waiting:
                    queue = f"{base}-priority" if r.cls == "p" else base
                    pipe.hget(self.keys.queue(queue, "st"), r.mid)
                    pipe.zscore(self.keys.queue(queue, "vis"), r.mid)
                replies = await pipe.execute()
            now_ms = (await self._now()) * 1000
            for i, r in enumerate(waiting):
                st, vis = replies[2 * i], replies[2 * i + 1]
                if st is not None and _text(st) == "h":
                    r.stage = "held"
                elif st is not None and vis is not None and float(vis) > now_ms:
                    r.stage = "delayed"
                else:
                    r.stage = "queued"
        if running:
            async with self._state.pipeline(transaction=False) as pipe:
                for r in running:
                    served = r.status.get("served_model") or alias
                    pipe.hget(self.keys.model(served, f"inprog:{r.rid}"), "dep")
                deps = await pipe.execute()
            for r, dep in zip(running, deps, strict=True):
                r.stage = "running"
                r.deployment = _text(dep) if dep is not None else None
        for r in rows:
            if r.stage is None and r.job.get("state") == "queued":
                r.stage = "direct" if r.job.get("direct") == "1" else "queued"

    @staticmethod
    def _state_of(row: _Row) -> Literal["waiting", "running"] | None:
        state = row.job.get("state")
        if not row.job or state in FINAL_STATES or row.status.get("state") in FINAL_STATES:
            return None
        return "running" if state == "running" else "waiting"

    def _summary(
        self,
        view: ResolvedRegistry,
        alias: str,
        row: _Row,
        names: Mapping[str, str],
        *,
        now: float | None = None,
    ) -> RequestSummary | None:
        state = self._state_of(row)
        if state is None or row.stage is None:
            return None
        now = time.time() if now is None else now
        enqueued = float(row.status.get("enqueued_at") or 0) or now
        deadline = None if row.score != row.score else round(row.score / 1000 - now, 3)
        return RequestSummary(
            request_id=row.rid,
            model=alias,
            tenant=row.tenant,
            state=state,
            stage=row.stage,
            priority=(row.cls or ("p" if row.status.get("priority") == "1" else "n")) == "p",
            age_secs=round(max(0.0, now - enqueued), 3),
            deadline_in_secs=deadline,
            deployment=row.deployment,
            user=row.job.get("user") or None,
            cancelling=row.cancelling,
            message_id=row.mid or None,
            tenant_name=names.get(row.tenant),
        )

    async def _names(self, alias: str) -> dict[str, str]:
        """Tenant names of the model (only with `LLMQ_TENANT_NAMES_ENABLED`)."""
        if not self.settings.tenant_names:
            return {}
        return _decode(await self._state.hgetall(self.keys.tenant_names(alias)))

    async def _moved_records(self, moved: Sequence[tuple[str, str]], cls: str) -> None:
        """Separate queue store: the moved calls' job `mid` / `q` and status `priority`."""
        async with self._state.pipeline(transaction=False) as pipe:
            for rid, new in moved:
                pipe.hset(self.keys.request(rid, "job"), mapping={"mid": new, "q": cls})
                pipe.hset(self.keys.status(rid), "priority", "1" if cls == "p" else "0")
            await pipe.execute()

    async def _audit(self, report: ChangeReport) -> None:
        """One audit entry per real change: counts only, the tenant as its hash."""
        after = {
            "matched": report.matched,
            "changed": report.changed,
            "skipped": report.skipped,
            "by_state": report.by_state,
            "sample_ids": report.sample_ids,
        }
        path = f"requests.{report.model or '*'}.{report.tenant or '*'}"
        diff = [{"path": path, "before": None, "after": after}]
        await self._state.xadd(
            self.registry.audit_key,
            {
                "actor": report.actor,
                "op": report.op,
                "version": "",
                "diff": orjson.dumps(diff),
                "ts": f"{time.time():.6f}",
            },
            maxlen=AUDIT_MAX_ENTRIES,
            approximate=True,
        )
        logger.info(
            "llmq_admin",
            op=report.op,
            actor=report.actor,
            model=report.model,
            tenant=report.tenant,
            matched=report.matched,
            changed=report.changed,
        )


def _bump(counts: dict[str, int], key: str, by: int = 1) -> None:
    counts[key] = counts.get(key, 0) + by


def _by_state(rows: Sequence[_Row], state_of: Callable[[_Row], str | None]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        state = state_of(row)
        if state is not None:
            _bump(out, state)
    return out


def _replace(report: ChangeReport, **changes: Any) -> ChangeReport:
    return replace(report, **changes)


__all__ = [
    "MAX_CANCEL_IDS",
    "NO_TENANT",
    "STAGES",
    "STATES",
    "AdminSettings",
    "ChangeReport",
    "RequestCounts",
    "RequestDetail",
    "RequestPage",
    "RequestSummary",
    "RequestsAdmin",
    "Stage",
    "State",
]
