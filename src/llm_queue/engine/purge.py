"""Purge: delete the store state of a removed model or deployment, or of one tenant (0.1.0b3).

One service, `PurgeService`, holds the logic; `LLMQueue.purge`, the CLI and the HTTP routes
call it (owner K5). Every purge but a dry run is audited in the registry audit stream, logged
`llmq_purge` and counted in `llmq_purged_keys_total{kind}`. Auto purge (owner K1) schedules a
model or deployment the registry dropped and purges it after a grace, unless it came back.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import os
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Self, cast

import orjson

from llm_queue._logging import get_logger
from llm_queue.engine._codec import text as _text
from llm_queue.engine.expiry import native
from llm_queue.engine.fairness import tenant_of
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import StreamQueues
from llm_queue.engine.requests import RequestStore
from llm_queue.engine.settings import TENANT_PURGE_PARTS, InfraSettings
from llm_queue.engine.store_guard import store_op
from llm_queue.errors import ConfigError, PurgeRefused
from llm_queue.registry._lua import AUDIT_MAX_ENTRIES

if TYPE_CHECKING:
    import redis.asyncio

    from llm_queue.engine.metrics import Metrics
    from llm_queue.registry.resolve import ResolvedRegistry
    from llm_queue.registry.store import RegistryStore

logger = get_logger(__name__)
Kind = Literal["model", "deployment", "tenant", "removed"]
AUTO_ACTOR = "auto"
_LOCK_MS = 120_000  # `P:lock:purge`: one pod schedules and runs due purges at a time
_POLL_SECS = 1.0  # deployment purge: how often running calls are checked while waiting

# KEYS: tf:t:<h>, tf:s:<h>, tf:act, tf:w, tf:need, P:ttl. ARGV: h, dry ('1'), request ids
# (from the tenant index) to drop from tf:need. Reply: {keys, fields}. `~sum` stays the sum of
# the remaining weights, so the other tenants' shares are right at once.
_TENANT_PURGE = """
local h, dry = ARGV[1], ARGV[2] == '1'
local keys = redis.call('EXISTS', KEYS[1]) + redis.call('EXISTS', KEYS[2])
local w = redis.call('HGET', KEYS[4], h)
local fields = (w and 1 or 0) + (redis.call('ZSCORE', KEYS[3], h) and 1 or 0)
local rids = {}
for i = 3, #ARGV do table.insert(rids, ARGV[i]) end
if dry then return {keys, fields} end
redis.call('UNLINK', KEYS[1], KEYS[2])
redis.call('ZREM', KEYS[6], KEYS[1], KEYS[2])
redis.call('ZREM', KEYS[3], h)
if w then
  local sum = tonumber(redis.call('HGET', KEYS[4], '~sum') or '0') - tonumber(w)
  redis.call('HDEL', KEYS[4], h)
  if redis.call('HLEN', KEYS[4]) <= 1 or sum <= 0 then
    redis.call('HDEL', KEYS[4], '~sum')
    if redis.call('HLEN', KEYS[4]) > 0 then
      -- rounding left nothing: recount from the weights
      local flat, total = redis.call('HGETALL', KEYS[4]), 0
      for i = 1, #flat, 2 do total = total + tonumber(flat[i + 1]) end
      redis.call('HSET', KEYS[4], '~sum', tostring(total))
    end
  else
    redis.call('HSET', KEYS[4], '~sum', tostring(sum))
  end
end
for i = 1, #rids, 500 do
  redis.call('ZREM', KEYS[5], unpack(rids, i, math.min(i + 499, #rids)))
end
return {keys, fields}
"""

# KEYS: lock. ARGV: owner. Deletes the lock only while this owner holds it.
_UNLOCK = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


def _glob_escape(text: str) -> str:
    """`text` matched literally in a SCAN pattern (glob characters and backslash escaped)."""
    return "".join(f"\\{c}" if c in "*?[]\\" else c for c in text)


def keyed_hash(salt: str, value: str) -> str:
    """`Engine.hash_id` without an engine: HMAC-SHA256 of `value` keyed by `salt`, 16 hex."""
    return hmac.new(salt.encode(), value.encode(), hashlib.sha256).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class PurgeSettings:
    """Defaults of the purge methods (`LLMQ_*`); a method argument overrides each.

    `auto` / `grace_secs`: auto purge after a registry drop; `wait` / `wait_secs`: a deployment
    purge waits for its running calls; `batch`: keys per UNLINK pipeline; `tenant_default`:
    what a tenant purge removes; `tenant_index`: the per-tenant request index is written.
    """

    auto: bool = True
    grace_secs: float = 900.0
    wait: bool = True
    wait_secs: float = 300.0
    http_wait_secs: float = 60.0  # LLMQ_HTTP_PURGE_WAIT_SECS: the most an HTTP purge waits
    batch: int = 500
    tenant_default: frozenset[str] = frozenset({"keys"})
    tenant_index: bool = False

    @classmethod
    def from_infra(cls, settings: InfraSettings) -> PurgeSettings:
        """The purge part of the process settings."""
        return cls(
            auto=settings.auto_purge,
            grace_secs=float(settings.purge_grace_secs),
            wait=settings.purge_wait,
            wait_secs=float(settings.purge_wait_secs),
            http_wait_secs=float(settings.http_purge_wait_secs),
            batch=int(settings.purge_batch),
            tenant_default=frozenset(settings.tenant_purge_default.split(",")),
            tenant_index=settings.tenant_index,
        )


@dataclass(frozen=True, slots=True)
class PurgeReport:
    """What a purge deleted (or, `dry_run`, would delete); JSON-ready with `as_dict()`.

    `deleted_keys` counts keys, `deleted_fields` hash / set members of shared keys (health,
    rotation, tenant weights), `queues` the queues deleted, `skipped` (thing, why) pairs,
    `cancelled` calls a tenant purge cancelled; `at` is wall time (seconds).
    """

    target: str
    kind: Kind
    deleted_keys: int = 0
    queues: tuple[str, ...] = ()
    skipped: tuple[tuple[str, str], ...] = ()
    dry_run: bool = False
    actor: str = ""
    at: float = field(default_factory=time.time)
    deleted_fields: int = 0
    cancelled: int = 0

    def as_dict(self) -> dict[str, Any]:
        """The report as plain JSON types (`skipped` as `{thing, why}` objects)."""
        out = asdict(self)
        out["queues"] = list(self.queues)
        out["skipped"] = [{"thing": thing, "why": why} for thing, why in self.skipped]
        return out


class PurgeService:
    """Purges of one environment (key prefix): models, deployments, tenants, auto purge."""

    def __init__(
        self,
        state: redis.asyncio.Redis,
        registry: RegistryStore,
        *,
        queue_client: redis.asyncio.Redis | None = None,
        queue_prefix: str = "",
        settings: PurgeSettings | None = None,
        hash_id: Callable[[str], str] | None = None,
        salt: str = "",
        metrics: Metrics | None = None,
        status_ttl_secs: int = 1800,
        owner: str | None = None,
    ) -> None:
        """Wire the stores; `hash_id` (the engine's) else a keyed hash with `salt`."""
        self._state = state
        self._queue_client = queue_client or state
        self.registry = registry
        self.keys = Keys(registry.prefix)
        self.queue_prefix = queue_prefix
        self.expiry = native(self.keys.prefix)  # the store's mode, set by the runtime
        self.settings = settings or PurgeSettings()
        self._hash = hash_id or (lambda value: keyed_hash(salt, value))
        self._metrics = metrics
        self._status_ttl_secs = status_ttl_secs
        self._owner = owner or f"{os.getpid()}-{id(self):x}"
        self._queues = StreamQueues(self._queue_client, self.keys)
        self._tenant_script = state.register_script(_TENANT_PURGE)
        self._unlock = state.register_script(_UNLOCK)
        self._seen: set[str] | None = None  # targets of the registry the last tick saw
        self._pending: set[str] = set()  # dropped targets not recorded in `P:purge` yet
        self._owned: list[Any] = []  # clients `from_env` made, closed by `aclose`

    # ---------------------------------------------------------------- construction

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        secrets: Callable[[str], str | None] | None = None,
    ) -> PurgeService:
        """A standalone service from `LLMQ_*` (an admin worker); close it with `aclose()`."""
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
        config = None
        if settings.registry or settings.registry_file:
            config = orjson.loads(settings.registry_document())
        registry = RegistryStore(
            state,
            settings.key_prefix,
            config=config,
            live_edit=settings.registry_live_edit,
            allowed_hosts=settings.base_url_allowed_hosts,
            audit_retention_days=settings.audit_retention_days,
        )
        service = cls(
            state,
            registry,
            queue_client=queue_client,
            queue_prefix=settings.queue_prefix,
            settings=PurgeSettings.from_infra(settings),
            salt=settings.id_hash_salt or "",
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
        """Use as `async with PurgeService.from_env() as purge:`."""
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close what `from_env` opened."""
        await self.aclose()

    # ---------------------------------------------------------------- public: purges

    @store_op
    async def model(
        self, alias: str, *, actor: str, force: bool = False, dry_run: bool = False
    ) -> PurgeReport:
        """Delete every key of model `alias`, its queues and doorbells.

        Queues go only when empty unless `force`. `PurgeRefused` while the registry still has
        the model, unless `force`.
        """
        view = await self._view()
        if alias in view.registry.models and not force:
            raise PurgeRefused(
                f"model {alias!r} is still in the registry: remove it first, or pass force=True"
            )
        report = await self._model(view, alias, force=force, dry_run=dry_run, actor=actor)
        await self._done(report)
        return report

    @store_op
    async def deployment(
        self,
        alias: str,
        deployment: str,
        *,
        actor: str,
        force: bool = False,
        wait: bool | None = None,
        wait_secs: float | None = None,
        dry_run: bool = False,
    ) -> PurgeReport:
        """Delete the state of deployment `deployment` (`account/name`) of model `alias`.

        `wait` (default `LLMQ_PURGE_WAIT`) waits up to `wait_secs` (`LLMQ_PURGE_WAIT_SECS`)
        for its running calls to end; still running then, nothing is deleted and the report
        says why. `PurgeRefused` while the registry still has it, unless `force`.
        """
        view = await self._view()
        if deployment in self._deployments(view, alias) and not force:
            raise PurgeRefused(
                f"deployment {alias}/{deployment} is still in the registry: remove it first, "
                "or pass force=True"
            )
        report = await self._deployment(
            alias,
            deployment,
            actor=actor,
            wait=self.settings.wait if wait is None else wait,
            wait_secs=self.settings.wait_secs if wait_secs is None else wait_secs,
            dry_run=dry_run,
        )
        await self._done(report)
        return report

    @store_op
    async def tenant(
        self,
        tenant_id: str,
        *,
        actor: str,
        remove: Iterable[str] | None = None,
        models: Sequence[str] | None = None,
        dry_run: bool = False,
    ) -> PurgeReport:
        """Delete a tenant's fairness state in every model (or `models`); maybe cancel its calls.

        `tenant_id` is the raw id (hashed as admission hashes it). `remove` (default
        `LLMQ_TENANT_PURGE_DEFAULT`): `keys`, `queued` (cancel calls still waiting), `running`
        (cancel every call); the last two need `LLMQ_TENANT_INDEX`, else `PurgeRefused`.
        """
        parts = self.settings.tenant_default if remove is None else frozenset(remove)
        unknown = parts - set(TENANT_PURGE_PARTS)
        if unknown or not parts:
            raise ConfigError(
                f"remove must be a non-empty subset of {', '.join(TENANT_PURGE_PARTS)}"
            )
        if parts & {"queued", "running"} and not self.settings.tenant_index:
            raise PurgeRefused(
                "removing a tenant's queued or running calls needs the per-tenant request "
                "index: set LLMQ_TENANT_INDEX=true (it indexes calls made from then on)"
            )
        view = await self._view()
        aliases = list(models) if models is not None else sorted(view.registry.models)
        report = await self._tenant(tenant_id, parts, aliases, actor=actor, dry_run=dry_run)
        await self._done(report)
        return report

    @store_op
    async def removed(
        self,
        *,
        actor: str,
        force: bool = False,
        wait: bool | None = None,
        wait_secs: float | None = None,
        dry_run: bool = False,
    ) -> PurgeReport:
        """Purge every model and deployment the store has state of and the registry has not.

        What `registry gc` runs. Queues of removed models go only when empty unless `force`;
        `wait` / `wait_secs` as in `deployment` (defaults `LLMQ_PURGE_WAIT[_SECS]`), per
        deployment.
        """
        view = await self._view()
        known = set(view.registry.models)
        aliases = await self._aliases_in_store()
        aliases |= await self._aliases_in_history()
        reports = [
            await self._model(view, alias, force=force, dry_run=dry_run, actor=actor)
            for alias in sorted(aliases - known)
        ]
        for alias in sorted(known):
            live = self._deployments(view, alias)
            for dep in sorted(await self._deployments_in_store(alias) - live):
                reports.append(
                    await self._deployment(
                        alias,
                        dep,
                        actor=actor,
                        wait=self.settings.wait if wait is None else wait,
                        wait_secs=self.settings.wait_secs if wait_secs is None else wait_secs,
                        dry_run=dry_run,
                    )
                )
        report = PurgeReport(
            target="*",
            kind="removed",
            deleted_keys=sum(r.deleted_keys for r in reports),
            deleted_fields=sum(r.deleted_fields for r in reports),
            queues=tuple(sorted({q for r in reports for q in r.queues})),
            skipped=tuple(s for r in reports for s in r.skipped),
            dry_run=dry_run,
            actor=actor,
        )
        await self._done(report, parts=[r.target for r in reports])
        return report

    async def plan_model(self, alias: str, *, force: bool = False) -> PurgeReport:
        """`model(...)` as a dry run: what it would delete."""
        return await self.model(alias, actor="plan", force=force, dry_run=True)

    async def plan_deployment(
        self, alias: str, deployment: str, *, force: bool = False
    ) -> PurgeReport:
        """`deployment(...)` as a dry run (never waits)."""
        return await self.deployment(
            alias, deployment, actor="plan", force=force, wait=False, dry_run=True
        )

    async def plan_tenant(
        self,
        tenant_id: str,
        *,
        remove: Iterable[str] | None = None,
        models: Sequence[str] | None = None,
    ) -> PurgeReport:
        """`tenant(...)` as a dry run."""
        return await self.tenant(
            tenant_id, actor="plan", remove=remove, models=models, dry_run=True
        )

    # ---------------------------------------------------------------- public: auto purge

    @store_op
    async def scheduled(self) -> list[dict[str, Any]]:
        """Auto purges waiting for their grace: `{target, kind, due_at}` (store seconds)."""
        rows = cast(
            "list[tuple[Any, float]]",
            await self._state.zrange(self._schedule_key, 0, -1, withscores=True),
        )
        return [
            {
                "target": _text(member),
                "kind": "deployment" if "/" in _text(member) else "model",
                "due_at": float(score) / 1000,
            }
            for member, score in rows
        ]

    @store_op
    async def cancel_scheduled(self, target: str) -> bool:
        """Drop a scheduled auto purge (`alias` or `alias/account/name`); True when it was there."""
        removed = bool(await self._state.zrem(self._schedule_key, target))
        if removed:
            logger.info("purge_unscheduled", target=target)
        return removed

    async def tick(self, view: ResolvedRegistry) -> None:
        """One auto purge step (the engine's sweep loop): schedule drops, run due purges.

        Every pod notes what left its registry; the one holding `P:lock:purge` records it
        in `P:purge` (due = now + grace) and purges due targets still absent (a rollback
        within the grace only removes the entry).
        """
        targets = self._targets(view)
        if self._seen is not None:
            dropped = self._seen - targets
            # A dropped model's purge covers its deployments.
            self._pending |= {t for t in dropped if "/" not in t or t.split("/")[0] not in dropped}
        self._seen = targets
        if not self.settings.auto:
            self._pending.clear()
            return
        lock = self._lock_key
        if not await self._state.set(lock, self._owner, nx=True, px=_LOCK_MS):
            return
        try:
            now_ms = await self._now_ms()
            if self._pending:
                due = now_ms + int(self.settings.grace_secs * 1000)
                await self._state.zadd(
                    self._schedule_key, dict.fromkeys(self._pending, due), nx=True
                )
                for target in sorted(self._pending):
                    logger.info(
                        "purge_scheduled", target=target, grace_secs=self.settings.grace_secs
                    )
                self._pending.clear()
            due_now = await self._state.zrangebyscore(self._schedule_key, "-inf", now_ms)
            for raw in due_now:
                target = _text(raw)
                if target in targets:
                    logger.info("purge_unscheduled", target=target, reason="back_in_registry")
                elif not await self._auto(target):
                    continue  # kept: tried again at the next tick
                await self._state.zrem(self._schedule_key, target)
        finally:
            await self._unlock([lock], [self._owner])

    # ---------------------------------------------------------------- public: tenant index

    async def index_call(
        self,
        tenant_id: str | None,
        user_id: str | None,
        request_id: str,
        *,
        now_ms: int,
        keep_ms: int,
    ) -> None:
        """Add a call to its tenant's request index (only with `LLMQ_TENANT_INDEX`).

        Scored by the call's deadline (`now_ms + keep_ms`, store ms); entries past their
        deadline are pruned on each write, so the index holds only calls that can still run.
        """
        if not self.settings.tenant_index:
            return
        key = self._index_key(self._hash(tenant_of(tenant_id, user_id)))
        async with self._state.pipeline(transaction=False) as pipe:
            pipe.zadd(key, {request_id: now_ms + keep_ms})
            pipe.zremrangebyscore(key, "-inf", now_ms)
            # Lives as long as its longest call: set when none, else only lengthened.
            self.expiry.expire(pipe, key, (keep_ms + 60_000) / 1000, grow=True)
            await pipe.execute()

    # ---------------------------------------------------------------- the purges

    async def _model(
        self, view: ResolvedRegistry, alias: str, *, force: bool, dry_run: bool, actor: str
    ) -> PurgeReport:
        glob = _glob_escape(self.keys.prefix)
        keys = [
            _text(k)
            async for k in self._state.scan_iter(
                match=f"{glob}:{{m:{_glob_escape(alias)}}}:*", count=500
            )
        ]
        queue_keys: list[str] = []
        queues: list[str] = []
        skipped: list[tuple[str, str]] = []
        for name in sorted(await self._queue_names(view, alias)):
            total, held, _dead = await self._queues.depth(name)
            if (total or held) and not force:
                skipped.append((name, f"{total} messages waiting; pass force=True"))
                continue
            found = [
                _text(k)
                async for k in self._queue_client.scan_iter(
                    match=f"{glob}:{{q:{_glob_escape(name)}}}:*", count=500
                )
            ]
            bell = self._queues.bell(name)
            if await self._queue_client.exists(bell):
                found.append(bell)
            if found:
                queues.append(name)
                queue_keys += found
        deleted = len(keys) + len(queue_keys)
        if not dry_run:
            deleted = await self._unlink(self._state, keys)
            deleted += await self._unlink(self._queue_client, queue_keys)
        return PurgeReport(
            target=alias,
            kind="model",
            deleted_keys=deleted,
            queues=tuple(queues),
            skipped=tuple(skipped),
            dry_run=dry_run,
            actor=actor,
        )

    async def _deployment(
        self,
        alias: str,
        dep: str,
        *,
        actor: str,
        wait: bool,
        wait_secs: float,
        dry_run: bool,
    ) -> PurgeReport:
        target = f"{alias}/{dep}"
        if wait and not dry_run:
            loop = asyncio.get_running_loop()
            until = loop.time() + max(0.0, wait_secs)
            running = await self._running_on(alias, dep)
            while running and loop.time() < until:
                await asyncio.sleep(min(_POLL_SECS, max(0.01, until - loop.time())))
                running = await self._running_on(alias, dep)
            if running:
                why = f"{running} calls still running after {wait_secs:g} s; nothing deleted"
                return PurgeReport(
                    target, "deployment", skipped=((target, why),), dry_run=dry_run, actor=actor
                )
        model = self.keys.model
        exact = [model(alias, f"{n}:{dep}") for n in ("rate", "ac", "cb", "terr")]
        exact += [model(alias, f"lease:{dep}:{c}") for c in ("p", "n")]
        async with self._state.pipeline(transaction=False) as pipe:
            for key in exact:
                pipe.exists(key)
            there = await pipe.execute()
        keys = [k for k, n in zip(exact, there, strict=True) if n]
        glob = _glob_escape(model(alias, ""))
        keys += [
            _text(k)
            async for k in self._state.scan_iter(
                match=f"{glob}thr:{_glob_escape(dep)}:*", count=500
            )
        ]
        keys += await self._sticky_on(alias, dep)
        health, rot = model(alias, "health"), model(alias, "rot")
        h_fields = [
            f for f in map(_text, await self._state.hkeys(health)) if f.rsplit("|", 1)[0] == dep
        ]
        r_fields = [
            f
            for f in map(_text, await self._state.hkeys(rot))
            if f == dep or ("|" in f and f.split("|", 1)[1] == dep)
        ]
        deleted, fields = len(keys), len(h_fields) + len(r_fields)
        if not dry_run:
            deleted = await self._unlink(self._state, keys)
            async with self._state.pipeline(transaction=False) as pipe:
                if h_fields:
                    pipe.hdel(health, *h_fields)
                if r_fields:
                    pipe.hdel(rot, *r_fields)
                fields = sum(int(n) for n in await pipe.execute())
        return PurgeReport(
            target,
            "deployment",
            deleted_keys=deleted,
            deleted_fields=fields,
            dry_run=dry_run,
            actor=actor,
        )

    async def _tenant(
        self,
        tenant_id: str,
        parts: frozenset[str],
        aliases: Sequence[str],
        *,
        actor: str,
        dry_run: bool,
    ) -> PurgeReport:
        h = self._hash(tenant_of(tenant_id, None))
        index = self._index_key(h)
        rids: list[str] = []
        if self.settings.tenant_index:
            rids = [_text(r) for r in await self._state.zrange(index, 0, -1)]
        cancelled = 0
        if parts & {"queued", "running"} and rids:
            cancelled = await self._cancel_tenant(rids, parts, dry_run=dry_run)
        deleted = fields = 0
        if "keys" in parts:
            for alias in aliases:
                model = self.keys.model
                reply = await self._tenant_script(
                    [
                        model(alias, f"tf:t:{h}"),
                        model(alias, f"tf:s:{h}"),
                        model(alias, "tf:act"),
                        model(alias, "tf:w"),
                        model(alias, "tf:need"),
                        self._ttl_key,
                    ],
                    [h, "1" if dry_run else "0", *rids],
                )
                deleted += int(reply[0])
                fields += int(reply[1])
            if rids or await self._state.exists(index):
                deleted += 1 if dry_run else await self._unlink(self._state, [index])
        # The report names the tenant by its hash: the raw id is never stored or logged.
        return PurgeReport(
            target=f"tenant:{h}",
            kind="tenant",
            deleted_keys=deleted,
            deleted_fields=fields,
            dry_run=dry_run,
            actor=actor,
            cancelled=cancelled,
        )

    async def _cancel_tenant(self, rids: list[str], parts: frozenset[str], *, dry_run: bool) -> int:
        """Cancel the tenant's indexed calls: `queued` ones still waiting, `running` all."""
        if "running" in parts:
            chosen = rids
        else:
            async with self._state.pipeline(transaction=False) as pipe:
                for rid in rids:
                    pipe.hget(self.keys.request(rid, "job"), "state")
                states = await pipe.execute()
            chosen = [r for r, s in zip(rids, states, strict=True) if s and _text(s) == "queued"]
        if dry_run or not chosen:
            return len(chosen)
        store = RequestStore(self._state, self.keys, status_ttl_secs=self._status_ttl_secs)
        done = 0
        for start in range(0, len(chosen), self.settings.batch):
            batch = chosen[start : start + self.settings.batch]
            done += await store.cancel(
                batch, ttl_secs=self._status_ttl_secs, channel=self.keys.cancel_channel
            )
            for rid in batch:
                logger.info("call_cancel", request_id=rid, source="purge", by=self._owner)
        if self._metrics is not None:
            self._metrics.count("llmq_cancels_total", float(len(chosen)), source="admin")
        return done

    async def _auto(self, target: str) -> bool:
        """Run one due auto purge (the grace was the wait); False keeps it for the next tick.

        Decided on the registry as stored now, read strictly: a store error, no stored
        registry, a stored one this library refuses, or a process on its own config skips the
        purge (a stale or fallback copy could purge a model that was live again; rescan B3).
        """
        alias, _, dep = target.partition("/")
        try:
            doc, _meta = await self.registry._read()  # raises on a store error
            view = await self._view() if doc is not None else None
            if view is None or self.registry.stored_refused or self.registry.on_own_config:
                logger.warning("purge_deferred", target=target, reason="registry_not_readable")
                return False
            if target in self._targets(view):
                logger.info("purge_unscheduled", target=target, reason="back_in_registry")
                return True
            if dep:
                report = await self._deployment(
                    alias, dep, actor=AUTO_ACTOR, wait=False, wait_secs=0, dry_run=False
                )
            else:
                report = await self._model(
                    view, alias, force=False, dry_run=False, actor=AUTO_ACTOR
                )
            await self._done(report)
        except Exception as exc:
            logger.warning("purge_failed", target=target, error=repr(exc))
            return False
        return True

    # ---------------------------------------------------------------- helpers

    @property
    def _schedule_key(self) -> str:
        return f"{self.keys.prefix}:purge"

    @property
    def _lock_key(self) -> str:
        return f"{self.keys.prefix}:lock:purge"

    @property
    def _ttl_key(self) -> str:
        return f"{self.keys.prefix}:ttl"

    def _index_key(self, h: str) -> str:
        return f"{self.keys.prefix}:tenant:{h}:reqs"

    async def _view(self) -> ResolvedRegistry:
        """The registry as stored now (an admin purge must not act on a stale copy)."""
        view = await self.registry.load()
        if view is None:
            raise ConfigError("no registry is published yet (llm-queue registry publish)")
        return view

    @staticmethod
    def _deployments(view: ResolvedRegistry, alias: str) -> set[str]:
        if alias not in view.registry.models:
            return set()
        return {d.key for d in view.deployments(alias)}

    def _targets(self, view: ResolvedRegistry) -> set[str]:
        """Auto purge targets of a registry: `alias` and `alias/account/name`."""
        out: set[str] = set()
        for alias in view.registry.models:
            out.add(alias)
            out |= {f"{alias}/{dep}" for dep in self._deployments(view, alias)}
        return out

    async def _now_ms(self) -> int:
        secs, micros = await self._state.time()
        return int(secs) * 1000 + int(micros) // 1000

    async def _unlink(self, client: redis.asyncio.Redis, keys: Sequence[str]) -> int:
        """UNLINK in pipelined batches, each key also dropped from the expiry index `P:ttl`."""
        done = 0
        size = self.settings.batch
        for start in range(0, len(keys), size):
            chunk = list(keys[start : start + size])
            async with client.pipeline(transaction=False) as pipe:
                for key in chunk:
                    pipe.unlink(key)
                done += sum(int(n) for n in await pipe.execute())
            await client.zrem(self._ttl_key, *chunk)  # that store's own index (review B3-4)
        return done

    async def _aliases_in_store(self) -> set[str]:
        glob = _glob_escape(self.keys.prefix)
        return {
            _text(k).split("{m:", 1)[1].split("}", 1)[0]
            async for k in self._state.scan_iter(match=f"{glob}:{{m:*", count=500)
        }

    async def _aliases_in_history(self) -> set[str]:
        """Aliases of the published versions kept for rollback (their queues may remain)."""
        out: set[str] = set()
        for raw in (await self._state.hgetall(self.registry.history_key)).values():
            models = orjson.loads(raw).get("models") or {}
            out |= {a for a in models if isinstance(a, str)}
        return out

    async def _queue_names(self, view: ResolvedRegistry, alias: str) -> set[str]:
        """A model's queues (prefixed, both classes).

        `llm-<alias>` and its `queue.name` now or in a kept version; never a name another live
        model uses.
        """
        names = {f"llm-{alias}"}
        if alias in view.registry.models:
            names.add(view.queue_name(alias))
        for raw in (await self._state.hgetall(self.registry.history_key)).values():
            body = (orjson.loads(raw).get("models") or {}).get(alias)
            if isinstance(body, dict) and (body.get("queue") or {}).get("name"):
                names.add(str(body["queue"]["name"]))
        live = {view.queue_name(a) for a in view.registry.models if a != alias}
        out = {self.queue_prefix + n for n in names - live}
        return out | {f"{n}-priority" for n in out}

    async def _deployments_in_store(self, alias: str) -> set[str]:
        """Deployment keys that have state in model `alias` (keys and hash fields)."""
        base = self.keys.model(alias, "")
        found: set[str] = set()
        async for raw in self._state.scan_iter(match=f"{_glob_escape(base)}*", count=500):
            name = _text(raw)[len(base) :]
            kind, _, rest = name.partition(":")
            if not rest:
                continue
            if kind in ("rate", "ac", "cb", "terr"):
                found.add(rest)
            elif kind == "lease" and rest[-2:] in (":p", ":n"):
                found.add(rest[:-2])
            elif kind == "thr" and ":" in rest:
                found.add(rest.rsplit(":", 1)[0])
        for f in map(_text, await self._state.hkeys(self.keys.model(alias, "health"))):
            if "|" in f:
                found.add(f.rsplit("|", 1)[0])
        for f in map(_text, await self._state.hkeys(self.keys.model(alias, "rot"))):
            if "|" in f:
                found.add(f.split("|", 1)[1])
        return found

    async def _running_on(self, alias: str, dep: str) -> int:
        """Calls of model `alias` in progress on deployment `dep` (its `inprog:*` entries)."""
        glob = _glob_escape(self.keys.model(alias, "inprog:"))
        entries = [k async for k in self._state.scan_iter(match=f"{glob}*", count=500)]
        if not entries:
            return 0
        async with self._state.pipeline(transaction=False) as pipe:
            for key in entries:
                pipe.hget(key, "dep")
            deps = await pipe.execute()
        return sum(1 for d in deps if d is not None and _text(d) == dep)

    async def _sticky_on(self, alias: str, dep: str) -> list[str]:
        """Sticky session keys of model `alias` that point at deployment `dep`."""
        glob = _glob_escape(self.keys.model(alias, "sticky:"))
        keys = [_text(k) async for k in self._state.scan_iter(match=f"{glob}*", count=500)]
        if not keys:
            return []
        async with self._state.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.get(key)
            values = await pipe.execute()
        return [k for k, v in zip(keys, values, strict=True) if v is not None and _text(v) == dep]

    async def _done(self, report: PurgeReport, parts: Sequence[str] = ()) -> None:
        """Audit, log and count a real purge (dry runs leave no trace)."""
        if report.dry_run:
            return
        counts = {
            "deleted_keys": report.deleted_keys,
            "deleted_fields": report.deleted_fields,
            "queues": list(report.queues),
            "skipped": len(report.skipped),
            "cancelled": report.cancelled,
        }
        if parts:
            counts["targets"] = list(parts)
        diff = [{"path": f"purge.{report.target}", "before": None, "after": counts}]
        await self._state.xadd(
            self.registry.audit_key,
            {
                "actor": report.actor,
                "op": f"purge_{report.kind}",
                "version": "",
                "diff": orjson.dumps(diff),
                "ts": f"{time.time():.6f}",  # wall time, like the Lua audit entries
            },
            maxlen=AUDIT_MAX_ENTRIES,
            approximate=True,
        )
        logger.info(
            "llmq_purge",
            kind=report.kind,
            target=report.target,
            actor=report.actor,
            deleted_keys=report.deleted_keys,
            deleted_fields=report.deleted_fields,
            queues=list(report.queues),
            skipped=[thing for thing, _ in report.skipped],
            cancelled=report.cancelled,
        )
        if self._metrics is not None and report.deleted_keys:
            self._metrics.count(
                "llmq_purged_keys_total", float(report.deleted_keys), kind=report.kind
            )
