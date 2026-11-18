"""The admin side of requests (0.2.0 T): the request index settings and the `RequestsAdmin` service.

The per-model, per-tenant index (`P:{m:ALIAS}:tq:H`) lets an operator list, count, cancel,
reprioritize and extend one tenant's calls; tenants are keyed hashes, a raw name is never stored
or logged. Every change is a dry run unless `dry_run=False` (owner D3), names an `actor` and is
audited. Built from the stores alone (`from_env`), so CLI, HTTP and admin workers share it.
"""

from __future__ import annotations

import asyncio
import builtins
import contextlib
import heapq
import os
import re
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from typing import TYPE_CHECKING, Any, Literal, Self

from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import TimeoutError as StoreTimeoutError

from llm_queue._logging import get_logger
from llm_queue.engine._codec import text as _text
from llm_queue.engine.audit import Audit
from llm_queue.engine.envelope import Meta, Reply
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import RETENTION_SECS, StreamQueues
from llm_queue.engine.requests import FINAL_STATES, RequestStore
from llm_queue.engine.store_guard import store_op
from llm_queue.errors import AdminRefused, ConfigError
from llm_queue.registry.store import check_actor, process_actor

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
MAX_ID_CHARS = 256  # characters of one request id a cancel may name
SAMPLE_IDS = 20  # ids a change report lists
_HASH_RE = re.compile(r"^[0-9a-f]{16}\Z")
# Pauses before the moved calls' records are written again (separate queue store, RQ-8).
_RECORD_RETRY_SECS = (0.2, 0.5, 1.0)
_MOVED_CLASSES = (("", "n"), ("-priority", "p"))  # a model's queues: name suffix, class
_STORE_ERRORS = (StoreConnectionError, StoreTimeoutError, OSError)
_JOB_FIELDS = ("state", "mid", "q", "direct", "user", "ixh")
_STATUS_FIELDS = ("state", "model", "priority", "enqueued_at", "served_model")
_ROW_WIDTH = len(_JOB_FIELDS) + len(_STATUS_FIELDS) + 1
# The most tenants one `_PAGE` run merges, and tenant hashes one read of `tq` returns (FA-1):
# with more, a page is merged here from runs of this many tenants, so no script holds the store
# for longer than about a millisecond, however many tenants a model has.
_PAGE_TENANTS = 256
_TENANT_READ = 1_000

# The records of index rows, one script per batch (gate G-T2): three commands per id in a
# pipeline cost about 25 us each of client packing and parsing, so a 100-row page took 47 ms.
# KEYS: job, status, cancel of each call. ARGV: how many job fields, the job fields, then the
# status fields. Reply, flat: per call its job fields, its status fields, then 1 / 0 for its
# cancel mark (false: no such field). Read-only.
_READ_ROWS = """
local nj = tonumber(ARGV[1])
local job_fields, status_fields = {}, {}
for i = 2, #ARGV do
  if i <= nj + 1 then
    table.insert(job_fields, ARGV[i])
  else
    table.insert(status_fields, ARGV[i])
  end
end
local out = {}
for i = 1, #KEYS / 3 do
  for _, v in ipairs(redis.call('HMGET', KEYS[3 * i - 2], unpack(job_fields))) do
    out[#out + 1] = v
  end
  for _, v in ipairs(redis.call('HMGET', KEYS[3 * i - 1], unpack(status_fields))) do
    out[#out + 1] = v
  end
  out[#out + 1] = redis.call('EXISTS', KEYS[3 * i])
end
return out
"""

# The next index entries of a model, merged by (deadline, id) over its tenants (gate G-T2: a page
# of `list` in one round trip, not three). KEYS: the tenant index `tq` (a tenant's own index is
# `tq:H`, the same hash tag). ARGV: the cursor's deadline ms ('' from the start) and request
# id, how many entries, the most tenants this script reads from `tq` itself, the highest
# deadline wanted ('' none), then the tenant hashes to merge (none: the tenants in `tq`). Each
# tenant is read a few entries at a time (more each time it runs out), so a page reads about
# `count` entries, not `count` of every tenant (2,000 for 20 tenants, sorted: 17 ms of script).
# The merge is a binary heap (FA-1: a scan of every tenant per row held the store for seconds
# with 20,000 tenants), and no run merges more than `_PAGE_TENANTS` tenants. Entries past their
# deadline are never read. Reply: the store's time (ms), 1 when `tq` holds more tenants than
# this run may read (nothing merged: the caller merges them in chunks) else 0, then the
# deadline, request id and tenant hash of each entry. Read-only.
_PAGE = """
-- Byte order, as a sorted set orders members of one score (Lua's `<` follows the locale).
local function before(x, y)
  for i = 1, math.min(#x, #y) do
    local a, b = string.byte(x, i), string.byte(y, i)
    if a ~= b then return a < b end
  end
  return #x < #y
end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local lower, after, count = now, nil, tonumber(ARGV[3])
if ARGV[1] ~= '' and tonumber(ARGV[1]) >= now then
  lower, after = tonumber(ARGV[1]), ARGV[2]
end
local upper = ARGV[5] ~= '' and ARGV[5] or '+inf'
local tenants = {}
if #ARGV >= 6 then
  for i = 6, #ARGV do tenants[#tenants + 1] = ARGV[i] end
else
  local most = tonumber(ARGV[4])
  tenants = redis.call('ZRANGEBYSCORE', KEYS[1], now, '+inf', 'LIMIT', 0, most + 1)
  if #tenants > most then return {now, 1} end
end
local step = math.ceil(count / math.max(1, #tenants)) + 1
-- The tenant's next entry after the cursor (its `score`), reading more when it ran out.
local function advance(s)
  while true do
    if s.pos > #s.buf then
      if s.done then return false end
      s.buf = redis.call('ZRANGEBYSCORE', s.key, lower, upper, 'WITHSCORES', 'LIMIT', s.offset,
        s.step)
      s.pos, s.offset, s.done = 1, s.offset + s.step, #s.buf < 2 * s.step
      s.step = s.step * 2
      if #s.buf == 0 then return false end
    end
    s.score = tonumber(s.buf[s.pos + 1])
    -- Entries at the cursor's own deadline come in id order: those up to its id were read.
    if s.score > lower or after == nil or before(after, s.buf[s.pos]) then return true end
    s.pos = s.pos + 2
  end
end
local function less(a, b)
  return a.score < b.score or (a.score == b.score and before(a.buf[a.pos], b.buf[b.pos]))
end
local heap = {}
local function push(s)
  local i = #heap + 1
  heap[i] = s
  while i > 1 do
    local p = math.floor(i / 2)
    if not less(heap[i], heap[p]) then break end
    heap[i], heap[p] = heap[p], heap[i]
    i = p
  end
end
local function pop()
  local top, n = heap[1], #heap
  heap[1] = heap[n]
  heap[n] = nil
  n = n - 1
  local i = 1
  while true do
    local l, r, m = 2 * i, 2 * i + 1, i
    if l <= n and less(heap[l], heap[m]) then m = l end
    if r <= n and less(heap[r], heap[m]) then m = r end
    if m == i then break end
    heap[i], heap[m] = heap[m], heap[i]
    i = m
  end
  return top
end
for _, h in ipairs(tenants) do
  local s = {key = KEYS[1] .. ':' .. h, h = h, buf = {}, pos = 1, offset = 0, step = step}
  if advance(s) then push(s) end
end
local out = {now, 0}
while #out < 3 * count + 2 and #heap > 0 do
  local s = pop()
  out[#out + 1] = s.buf[s.pos + 1]
  out[#out + 1] = s.buf[s.pos]
  out[#out + 1] = s.h
  s.pos = s.pos + 2
  if advance(s) then push(s) end
end
return out
"""

# Where waiting messages are, one script per batch on the queue store. KEYS: the normal queue's
# `st` and `vis`, then the priority queue's. ARGV: class (`p` / `n`) and message id pairs.
# Reply: the message's `st` and `vis` score per pair (false: none). Read-only.
_READ_STAGES = """
local out = {}
for i = 1, #ARGV / 2 do
  local at = ARGV[2 * i - 1] == 'p' and 2 or 0
  out[2 * i - 1] = redis.call('HGET', KEYS[at + 1], ARGV[2 * i])
  out[2 * i] = redis.call('ZSCORE', KEYS[at + 2], ARGV[2 * i])
end
return out
"""

# The records of calls a reprioritize moved, from `MOVE`'s marks (separate queue store, RQ-8).
# KEYS: job and status of each call. ARGV: the class (`p` / `n`), then each call's new message
# id. A record is written only when it is there (a finished call's expired record is not made
# again, with no expiry) and holds an older message id: stream ids grow, so a repair racing a
# later move never puts back the earlier id. Reply: the 1-based places of the calls written.
_WRITE_MOVED = """
local function before(a, b)
  local am, as = string.match(a, '^(%d+)-(%d+)$')
  local bm, bs = string.match(b, '^(%d+)-(%d+)$')
  if not am or not bm then return a ~= b end
  am, as, bm, bs = tonumber(am), tonumber(as), tonumber(bm), tonumber(bs)
  return am < bm or (am == bm and as < bs)
end
local cls, out = ARGV[1], {}
for i = 1, #KEYS / 2 do
  local job, status, new = KEYS[2 * i - 1], KEYS[2 * i], ARGV[i + 1]
  local cur = redis.call('HGET', job, 'mid')
  if cur and before(cur, new) then
    redis.call('HSET', job, 'mid', new, 'q', cls)
    if redis.call('EXISTS', status) == 1 then
      redis.call('HSET', status, 'priority', cls == 'p' and '1' or '0')
    end
    out[#out + 1] = i
  end
end
return out
"""

# Clear move marks whose records are written. KEYS: the marks (`StreamQueues.moved_key`).
# ARGV: request id / message id pairs. A mark a later move replaced (another id) stays.
_CLEAR_MOVED = """
local n = 0
for i = 1, #ARGV, 2 do
  if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[i + 1] then
    n = n + redis.call('HDEL', KEYS[1], ARGV[i])
  end
end
return n
"""

# One field of many hashes (the running calls' grants). KEYS: the hashes. ARGV: the field.
_READ_FIELD = """
local out = {}
for i = 1, #KEYS do out[i] = redis.call('HGET', KEYS[i], ARGV[1]) end
return out
"""


@dataclass(frozen=True, slots=True)
class AdminSettings:
    """The request index and admin operation settings (`LLMQ_*`, design 0.2.0 section 7).

    Attributes:
        request_index: Index calls per model and tenant (`LLMQ_REQUESTS_INDEX_ENABLED`).
        tenant_names: Store tenant names beside their hashes (`LLMQ_TENANT_NAMES_ENABLED`).
        batch: Ids per pipeline or cancel script (`LLMQ_ADMIN_BATCH`).
        scan_max: Index entries or messages one operation reads (`LLMQ_ADMIN_SCAN_MAX`).
        list_max: The largest list page (`LLMQ_REQUESTS_LIST_MAX`).
        move_batch: Messages per reprioritize script run (`LLMQ_REQUESTS_MOVE_BATCH`).
        move_bytes: Bytes per reprioritize script run (`LLMQ_REQUESTS_MOVE_BYTES`).
        metrics_tenants: (name, label) pairs with their own `tenant` label (`LLMQ_METRICS_TENANTS`).
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
            request_index=settings.requests_index_enabled,
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

    `tenant` and `user` are keyed hashes (`-`: no tenant); `tenant_name` needs
    `LLMQ_TENANT_NAMES_ENABLED`. `stage` is `queued`, `delayed` (returned with a delay), `held`
    (worker waits for budget), `direct` (runs in its caller) or `running`; `cancelling` means a
    cancel is pending; `deadline_in_secs` is None when not indexed.
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
        self.hash_id = hash_id  # the keyed hash of a tenant or user id
        self.settings = settings or AdminSettings()
        self.status_ttl_secs = status_ttl_secs
        self.caller_margin_secs = caller_margin_secs
        self._metrics = metrics
        self._owner = owner or f"admin-{os.getpid()}"
        self._queues = StreamQueues(self._queue_client, keys)
        self._requests = RequestStore(state, keys, status_ttl_secs=status_ttl_secs)
        self._read_rows = state.register_script(_READ_ROWS)
        self._page_script = state.register_script(_PAGE)
        self._read_field = state.register_script(_READ_FIELD)
        self._read_stages = self._queue_client.register_script(_READ_STAGES)
        self._write_moved_script = state.register_script(_WRITE_MOVED)
        self._clear_moved = self._queue_client.register_script(_CLEAR_MOVED)
        self._owned: builtins.list[Any] = []

    # ---------------------------------------------------------------- construction

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        secrets: Callable[[str], str | None] | None = None,
        defaults: Mapping[str, Any] | None = None,
        secrets_names: Mapping[str, str] | None = None,
    ) -> RequestsAdmin:
        """A standalone service from `LLMQ_*` (CLI, admin worker); close it with `aclose()`.

        Read as `LLMQueue.from_env` reads its settings (same arguments): `secrets` when given,
        else the sources `LLMQ_SECRETS_SOURCE` names, then `environ`, then `defaults`.
        """
        from llm_queue.engine.admin.context import AdminContext
        from llm_queue.engine.settings import Layers

        ctx = AdminContext.from_layers(Layers(environ, secrets, defaults, secrets_names))
        assert ctx.requests is not None  # `from_layers` builds it
        service = ctx.requests
        service._owned = ctx.owned
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
        h = self.pick_tenant(tenant, tenant_hash)
        self._check_state(state)
        if not 1 <= limit <= self.settings.list_max:
            raise ConfigError(f"limit must be 1 to {self.settings.list_max}")
        start, after_rid = _parse_cursor(after)
        self._need_index(state)
        await self._finish_moves(view, alias)
        items: builtins.list[RequestSummary] = []
        scanned = 0
        last: tuple[float, str] | None = None
        names = await self._names(alias)
        # Every row in flight is listed with `all`: read the page and no more (rows read twice
        # the page before). A state filter skips some, so it reads ahead.
        chunk = min(self.settings.batch, limit if state == "all" else max(10, limit * 2))
        while True:
            now_ms, rows = await self._page(alias, h, start, after_rid, chunk)
            await self._records(rows)
            await self._stages(view, alias, rows, now_ms)
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
            if len(rows) < chunk or last is None:
                return RequestPage(items, None, scanned)
            start, after_rid = last

    @store_op
    async def count(
        self, model: str, *, tenant: str | None = None, tenant_hash: str | None = None
    ) -> RequestCounts:
        """Calls in flight of `model` (and one tenant) by state and stage.

        Reads every index entry up to `LLMQ_ADMIN_SCAN_MAX` (`truncated` above it). Needs the
        request index: with it off nothing is counted, so `AdminRefused` (not zeros).
        """
        view = await self._view()
        alias = view.registry.alias_of(model)
        h = self.pick_tenant(tenant, tenant_hash)
        self._need_index("in-flight")
        indexed = await self._indexed(alias, h)
        by_state = dict.fromkeys(("waiting", "running"), 0)
        by_stage: dict[str, int] = dict.fromkeys(STAGES, 0)
        cancelling = scanned = 0
        oldest: float | None = None
        now = await self._now()
        async for rows in self._rows(view, alias, h, float("-inf"), "", self.settings.scan_max):
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
    async def tenants(self, model: str) -> dict[str, Any]:
        """Tenants with calls in flight on `model`: hash, calls, seconds to the next deadline.

        Hashes only (`tenant_name` too with `LLMQ_TENANT_NAMES_ENABLED`), most calls first.
        Reads at most `LLMQ_ADMIN_SCAN_MAX` tenants (`truncated` when it reached that).
        """
        view = await self._view()
        alias = view.registry.alias_of(model)
        self._need_index("all")
        now_ms = (await self._now()) * 1000
        hashes = await self._tenants(alias, None, now_ms)
        names = await self._names(alias)
        rows: builtins.list[dict[str, Any]] = []
        if hashes:
            async with self._state.pipeline(transaction=False) as pipe:
                for h in hashes:
                    key = self.keys.request_index(alias, h)
                    pipe.zcount(key, now_ms, "+inf")
                    pipe.zrangebyscore(key, now_ms, "+inf", start=0, num=1, withscores=True)
                replies = await pipe.execute()
            for i, h in enumerate(hashes):
                calls, first = int(replies[2 * i]), replies[2 * i + 1]
                if not calls:
                    continue
                next_in = round((float(first[0][1]) - now_ms) / 1000, 3) if first else None
                row: dict[str, Any] = {"tenant": h, "calls": calls}
                row["next_deadline_in_secs"] = next_in
                if self.settings.tenant_names:
                    row["tenant_name"] = names.get(h)
                rows.append(row)
        rows.sort(key=lambda r: (-r["calls"], r["tenant"]))
        return {
            "model": alias,
            "tenants": rows,
            "truncated": len(hashes) >= self.settings.scan_max,
        }

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
        view = await self._view() if not final and job and alias else None
        # A model removed since the call was sent has no queues to read: no summary, only
        # the records (its lookup raised a bare KeyError before).
        if view is not None and alias is not None and alias in view.registry.models:
            if request_id in await self._finish_moves(view, alias):
                job = _decode(await self._state.hgetall(k.request(request_id, "job")))
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
        op: str = "requests_cancel",
    ) -> ChangeReport:
        """Cancel calls by id, or by model, tenant and state (default `waiting`, owner D9).

        Cancel wins for every caller of an id. With `state="waiting"` a call granted meanwhile
        keeps running (`skipped.started`) and one already cancelling is skipped, so a rerun
        changes nothing. Dry run by default; a real run writes one audit entry named `op`
        (`purge_queue` from `llmq.admin.purge.queue`).
        """
        check_actor(actor)
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
            if not 1 <= len(ids) <= MAX_CANCEL_IDS or any(
                not rid or len(rid) > MAX_ID_CHARS for rid in ids
            ):
                raise ConfigError(
                    f"request_ids must name 1 to {MAX_CANCEL_IDS} ids of 1 to {MAX_ID_CHARS} "
                    "characters"
                )
            rows = await self._rows_by_id(ids)
        else:
            assert view is not None
            assert alias is not None
            h = self.pick_tenant(tenant, tenant_hash)
            self._need_index(state)
            await self._refuse_above_cap(alias, h)
            scan = self.settings.scan_max  # every row at once: records in one round trip
            async for chunk in self._rows(view, alias, h, float("-inf"), "", scan, stages=False):
                rows += chunk  # a cancel reads only the records: state and mark, not stages
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
            op=op,
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
        # A call still `queued` once marked never starts: it is answered `cancelled` in the
        # same script (a waiting call was only stopped when a worker next asked for it, which
        # for a message deep in a queue can be minutes). One that started is left to its worker;
        # with `state="running"` no queued call is marked, so none is answered.
        marked, moved_on, answered = await self._requests.cancel_settle(
            [r.rid for r in chosen],
            ttl_secs=self.status_ttl_secs,
            channel=self.keys.cancel_channel,
            only_state={"waiting": "queued", "running": "running"}.get(state),
            reply=Reply(
                request_id="",
                ok=False,
                error="cancelled",
                message="the call was cancelled by an admin",
                meta=Meta(worker=self._owner),
            ),
            batch=self.settings.batch,
        )
        changed = len(marked)
        if moved_on:
            _bump(skipped, "started" if state == "waiting" else "not_running", moved_on)
        for rid in marked:  # only the calls it cancelled (RQ-13)
            logger.info("call_cancel", request_id=rid, source="admin", by=actor)
        if self._metrics is not None:
            self._metrics.count("llmq_cancels_total", float(changed), source="admin")
        report = _replace(report, changed=changed, skipped=skipped)
        try:
            await self._audit(report)
        finally:
            # Last, after the audit: callers woken in this process (an admin beside an app)
            # run their result reads on this loop, and the audit waited behind 10,000 of them.
            # Not woken (a store error here), they find their results by the result sweep.
            await self._requests.wake(answered, batch=self.settings.batch)
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

        Only queued calls move; `held` (owner D7), `delayed`, `direct`, `cancelling` and
        `already` ones are skipped. A moved call keeps its deadline, `max_wait_secs` and arrival
        order (`pos`, owner Q9). `limit` caps the count, oldest first. Dry run by default.
        """
        check_actor(actor)
        view = await self._view()
        alias = view.registry.alias_of(model)
        h = self.pick_tenant(tenant, tenant_hash)
        if limit is not None and limit < 1:
            raise ConfigError("limit must be 1 or more")
        self._need_index("waiting")
        await self._refuse_above_cap(alias, h)
        target = "p" if priority else "n"
        skipped: dict[str, int] = {}
        chosen: builtins.list[_Row] = []
        async for rows in self._rows(view, alias, h, float("-inf"), "", self.settings.scan_max):
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
                await self._moved_records(result.moved, target, dest)
            for rid, new in result.moved:
                logger.info("request_moved", request_id=rid, model=alias, to=target, mid=new)
            changed += len(result.moved)
            for _, why in result.skipped:
                _bump(skipped, why)
            if any(why == "full" for _, why in result.skipped):
                # The calls of later batches are not moved either (RQ-15: they went uncounted).
                if rest := len(pairs) - at - len(part):
                    _bump(skipped, "full", rest)
                break
            at += max(1, result.looked)
        report = _replace(report, changed=changed, skipped=skipped)
        await self._audit(report)
        return report

    @store_op
    async def extend(
        self, request_id: str, *, timeout_secs: float, actor: str, dry_run: bool = True
    ) -> ChangeReport:
        """Give a call in flight a longer timeout: `timeout_secs` from when it was sent.

        The deadline reaches records and index, holding workers and waiting callers (owner D6).
        Above `timeout_max_secs` or the queue's `retention_secs` (message trimmed) is a
        `ConfigError`; a deadline not later than the current one is `skipped.already`. Needs the
        request index (`AdminRefused` when off). Dry run by default.
        """
        check_actor(actor)
        if not timeout_secs > 0:
            raise ConfigError("timeout_secs must be above 0")
        self._need_index("extended")
        k = self.keys
        async with self._state.pipeline(transaction=False) as pipe:
            pipe.hmget(k.status(request_id), ["model", "enqueued_at", "state"])
            pipe.hmget(k.request(request_id, "job"), ["state", "ixh", "dl", "ix"])
            (model, enqueued, status_state), (state, h, dl, ix) = await pipe.execute()
        first = await self._state.zscore(_text(ix), request_id) if ix is not None else None
        alias = _text(model) if model is not None else None
        report = ChangeReport(
            op="requests_extend",
            model=alias,
            tenant=_text(h) if h is not None else None,
            dry_run=dry_run,
            actor=actor,
        )
        job_state = _text(state) if state is not None else None
        if alias is None or enqueued is None or job_state is None:
            return _replace(report, skipped={"gone": 1})
        if job_state in FINAL_STATES or _text(status_state or b"") in FINAL_STATES:
            return _replace(report, skipped={"final": 1})
        view = await self._view()
        try:
            canonical = view.registry.alias_of(alias)
        except ConfigError:  # the model was removed since the call was sent
            return _replace(report, skipped={"unknown_model": 1})
        cap = float(view.value(canonical, "timeout_max_secs"))
        queue = view.value(canonical, "queue") or {}
        retention = float(queue.get("retention_secs", RETENTION_SECS))
        if timeout_secs > min(cap, retention):
            raise ConfigError(
                f"timeout_secs must be at most the model's timeout_max_secs ({cap:g}) and its "
                f"queue's retention_secs ({retention:g}, past it the message is trimmed)"
            )
        deadline_ms = int((float(_text(enqueued)) + timeout_secs) * 1000)
        current = float(_text(dl)) if dl is not None else 0.0
        if first is not None:  # the index scores the first deadline plus the caller margin
            current = max(current, float(first) - self.caller_margin_secs * 1000)
        if deadline_ms <= current:
            return _replace(report, skipped={"already": 1})
        state_name = "running" if job_state == "running" else "waiting"
        report = _replace(report, matched=1, by_state={state_name: 1}, sample_ids=[request_id])
        if dry_run:
            return report
        outcome = await self._requests.extend(
            request_id, deadline_ms=deadline_ms, margin_secs=self.caller_margin_secs
        )
        if outcome != "ok":
            return _replace(report, skipped={outcome: 1})
        logger.info("request_extended", request_id=request_id, model=alias, deadline_ms=deadline_ms)
        report = _replace(report, changed=1)
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

    def pick_tenant(self, tenant: str | None, tenant_hash: str | None) -> str | None:
        """The tenant hash asked for (None: every tenant); a raw name is hashed here."""
        if tenant is not None and tenant_hash is not None:
            raise ConfigError("give tenant or tenant_hash, not both")
        if tenant is not None:
            if not tenant:
                raise ConfigError("tenant must not be empty")
            return self.hash_id(tenant)
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
                "(LLMQ_REQUESTS_INDEX_ENABLED is off)"
            )

    async def _now(self) -> float:
        secs, micros = await self._state.time()
        return int(secs) + int(micros) / 1_000_000

    async def _tenants(self, alias: str, h: str | None, now_ms: float) -> builtins.list[str]:
        if h is not None:
            return [h]
        # In reads of `_TENANT_READ` (FA-1: one read of 100,000 hashes held the store); a tenant
        # whose place moved between reads may come twice, so each is kept once.
        seen: dict[str, None] = {}
        offset = 0
        while len(seen) < self.settings.scan_max:
            raw = await self._state.zrangebyscore(
                self.keys.tenant_index(alias), now_ms, "+inf", start=offset, num=_TENANT_READ
            )
            seen.update((_text(m), None) for m in raw)
            offset += len(raw)
            if len(raw) < _TENANT_READ:
                break
        return builtins.list(seen)[: self.settings.scan_max]

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
        *,
        stages: bool = True,
    ) -> AsyncIterator[builtins.list[_Row]]:
        """Index entries after (`start`, `after_rid`), oldest deadline first, with records.

        One tenant: its `tq:H` in chunks. Every tenant: the tenants in `tq`, their entries
        merged by (deadline, id). Entries past their deadline are never read. `stages=False`
        leaves `stage` unread (None). Moves left unrecorded are written first (`_finish_moves`).
        """
        await self._finish_moves(view, alias)
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
                if stages:
                    await self._stages(view, alias, out, now_ms)
                yield out
                out = []
        if out:
            await self._records(out)
            if stages:
                await self._stages(view, alias, out, now_ms)
            yield out

    async def _page(
        self, alias: str, h: str | None, start: float, after_rid: str, count: int
    ) -> tuple[float, builtins.list[_Row]]:
        """The store's time (ms) and the next `count` index entries after (`start`, `after_rid`).

        One script (`_PAGE`), merged by (deadline, id) over every tenant (or `h`'s alone), as
        `_rows` merges them: a page of `list` read the time, the tenants and each tenant's
        next entries in three round trips, and parsed `count` entries of every tenant. A model
        with more than `_PAGE_TENANTS` tenants is merged here from one run per that many
        tenants (FA-1), each run reading only entries up to the page's last deadline so far.
        """
        head = ["" if start == float("-inf") else int(start), after_rid, count, _PAGE_TENANTS]
        tq = self.keys.tenant_index(alias)
        raw = await self._page_script([tq], [*head, "", h] if h else head)
        now_ms = float(raw[0])
        if not int(raw[1]):
            return now_ms, self._page_rows(raw)
        tenants = await self._tenants(alias, None, now_ms)
        best: builtins.list[_Row] = []
        for i in range(0, len(tenants), _PAGE_TENANTS):
            upper = int(best[-1].score) if len(best) >= count else ""
            raw = await self._page_script([tq], [*head, upper, *tenants[i : i + _PAGE_TENANTS]])
            best = heapq.nsmallest(
                count, [*best, *self._page_rows(raw)], key=lambda r: (r.score, r.rid)
            )
        return now_ms, best

    @staticmethod
    def _page_rows(raw: Sequence[Any]) -> builtins.list[_Row]:
        return [
            _Row(float(raw[i]), _text(raw[i + 1]), _text(raw[i + 2])) for i in range(2, len(raw), 3)
        ]

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
        fixed: set[str] = set()
        for alias in by_model:
            fixed |= await self._finish_moves(view, alias)
        if fixed:
            await self._records([row for row in rows if row.rid in fixed])
        for alias, part in by_model.items():
            await self._stages(view, alias, part)
        for row in rows:
            if row.stage is None and self._state_of(row) is not None:
                row.stage = "running" if row.job.get("state") == "running" else "queued"
        return rows

    async def _scripted(
        self,
        client: redis.asyncio.Redis,
        script: Any,
        items: Sequence[Any],
        keys: Callable[[Sequence[Any]], builtins.list[str]],
        args: Callable[[Sequence[Any]], builtins.list[Any]],
    ) -> builtins.list[Any]:
        """One read script per `LLMQ_ADMIN_BATCH` items, all in one round trip; replies joined.

        A batch bounds how long one script holds the store; a round trip per batch let 10,000
        rows take 20 (gate G-T2). One batch is called directly: a pipeline of scripts first
        checks they are loaded, a round trip of its own.
        """
        step = self.settings.batch
        if len(items) <= step:
            return builtins.list(await script(keys(items), args(items))) if items else []
        async with client.pipeline(transaction=False) as pipe:
            for at in range(0, len(items), step):
                part = items[at : at + step]
                await script(keys(part), args(part), client=pipe)
            replies = await pipe.execute()
        return [v for reply in replies for v in reply]

    async def _records(self, rows: Sequence[_Row]) -> None:
        """Fill each row's job, status and cancel mark (`_READ_ROWS`, one round trip)."""
        k = self.keys
        flat = await self._scripted(
            self._state,
            self._read_rows,
            rows,
            lambda part: [
                key
                for row in part
                for key in (
                    k.request(row.rid, "job"),
                    k.status(row.rid),
                    k.request(row.rid, "cancel"),
                )
            ],
            lambda _: [len(_JOB_FIELDS), *_JOB_FIELDS, *_STATUS_FIELDS],
        )
        nj = len(_JOB_FIELDS)
        for i, row in enumerate(rows):
            at = i * _ROW_WIDTH
            job, status = flat[at : at + nj], flat[at + nj : at + _ROW_WIDTH - 1]
            row.job = {n: _text(v) for n, v in zip(_JOB_FIELDS, job, strict=True) if v}
            row.status = {n: _text(v) for n, v in zip(_STATUS_FIELDS, status, strict=True) if v}
            row.cancelling = bool(flat[at + _ROW_WIDTH - 1])
            row.mid = row.job.get("mid", "")
            row.cls = row.job.get("q", "")

    async def _stages(
        self,
        view: ResolvedRegistry,
        alias: str,
        rows: Sequence[_Row],
        now_ms: float | None = None,
    ) -> None:
        """The stage of each row: from its job, its message's records and its grant."""
        waiting = [
            r
            for r in rows
            if r.job.get("state") == "queued" and r.mid and r.job.get("direct") != "1"
        ]
        running = [r for r in rows if r.job.get("state") == "running"]
        base = self.queue_prefix + view.queue_name(alias)
        if waiting:
            queues = [
                self.keys.queue(q, part)
                for q in (base, f"{base}-priority")
                for part in ("st", "vis")
            ]
            replies = await self._scripted(
                self._queue_client,
                self._read_stages,
                waiting,
                lambda _: queues,
                lambda part: [v for r in part for v in ("p" if r.cls == "p" else "n", r.mid)],
            )
            if now_ms is None:
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
            deps = await self._scripted(
                self._state,
                self._read_field,
                running,
                lambda part: [
                    self.keys.model(r.status.get("served_model") or alias, f"inprog:{r.rid}")
                    for r in part
                ],
                lambda _: ["dep"],
            )
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

    async def _moved_records(
        self, moved: Sequence[tuple[str, str]], cls: str, queue: str
    ) -> builtins.list[str]:
        """Separate queue store: the moved calls' job `mid` / `q` and status `priority`.

        Written from `MOVE`'s reply, tried again on a store error, then the marks `MOVE` left
        in `queue` are cleared (RQ-8): a record left on the old message showed the call in the
        wrong class and made a later reprioritize skip it as `gone`. Still failing, it is
        logged with the ids, and the marks have the next admin read of the model write them
        (`_finish_moves`). Returns the ids whose records were written.
        """
        for pause in (*_RECORD_RETRY_SECS, None):
            try:
                written = await self._write_moved(moved, cls)
                await self._clear_moved(
                    [self._queues.moved_key(queue)], [v for pair in moved for v in pair]
                )
                return written
            except _STORE_ERRORS as exc:
                if pause is None:
                    logger.error(
                        "request_records_stale",
                        request_ids=[rid for rid, _ in moved][:SAMPLE_IDS],
                        count=len(moved),
                        error_type=type(exc).__name__,
                    )
                    return []
                await asyncio.sleep(pause)
        return []  # not reached: the last pass returns

    async def _write_moved(self, moved: Sequence[tuple[str, str]], cls: str) -> builtins.list[str]:
        """`_WRITE_MOVED` per `LLMQ_ADMIN_BATCH` calls; the ids whose records were written."""
        written: builtins.list[str] = []
        step = self.settings.batch
        for at in range(0, len(moved), step):
            part = moved[at : at + step]
            keys = [
                key
                for rid, _ in part
                for key in (self.keys.request(rid, "job"), self.keys.status(rid))
            ]
            places = await self._write_moved_script(keys, [cls, *(new for _, new in part)])
            written += [part[int(i) - 1][0] for i in places]
        return written

    async def _finish_moves(self, view: ResolvedRegistry, alias: str) -> set[str]:
        """Write the records of calls a reprioritize moved but did not record (RQ-8).

        Separate queue store only (one store writes them in `MOVE` itself). A process that
        stopped between `MOVE` and the record write left the moves marked in the queue store
        (`StreamQueues.moved_key`); every admin read of the model writes them first, so it lists
        and changes the calls where they are. Idempotent: a record already written, a finished
        call's and one a later move changed are left (`_WRITE_MOVED`). A repair that wrote a
        record logs `request_records_repaired` and writes an audit entry
        (`requests_reprioritize_repair`). Returns the ids whose records were written.
        """
        if self.queues_shared or alias not in view.registry.models:
            return set()
        base = self.queue_prefix + view.queue_name(alias)
        queues = [(base + suffix, cls) for suffix, cls in _MOVED_CLASSES]
        async with self._queue_client.pipeline(transaction=False) as pipe:
            for queue, _ in queues:
                pipe.hgetall(self._queues.moved_key(queue))
            marks = await pipe.execute()
        fixed: set[str] = set()
        for (queue, cls), raw in zip(queues, marks, strict=True):
            moved = sorted((_text(rid), _text(new)) for rid, new in (raw or {}).items())
            if moved:
                fixed.update(await self._moved_records(moved, cls, queue))
        if fixed:
            ids = sorted(fixed)
            logger.warning(
                "request_records_repaired",
                model=alias,
                count=len(ids),
                request_ids=ids[:SAMPLE_IDS],
            )
            await self._audit(
                ChangeReport(
                    op="requests_reprioritize_repair",
                    model=alias,
                    tenant=None,
                    dry_run=False,
                    actor=process_actor("repair"),
                    matched=len(ids),
                    changed=len(ids),
                    sample_ids=ids[:SAMPLE_IDS],
                )
            )
        return fixed

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
        await Audit(self._state, self.registry.audit_key).write(
            kind="requests", actor=report.actor, op=report.op, path=path, before=None, after=after
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
