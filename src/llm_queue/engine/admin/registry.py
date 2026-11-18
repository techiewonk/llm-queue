"""Admin services over the registry: the document, audit, models, deployments, blocks.

Every write is a dry run unless `dry_run=False`: the dry run runs every check the edit runs
(`RegistryStore.plan_edit` / `plan_publish`) and returns the diff it would make; nothing is
written and nothing is audited. Real edits are audited by the registry's own script.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from llm_queue.engine.admin.context import AdminContext
from llm_queue.engine.fairness import hash_tenant_names, hash_tenant_path
from llm_queue.engine.store_guard import store_op
from llm_queue.errors import ConfigError
from llm_queue.registry.store import SECRET_FIELDS, audit_diff, check_actor

if TYPE_CHECKING:
    from llm_queue.registry.resolve import ResolvedRegistry

MAX_AUDIT_COUNT = 1000
# Audit entries `blocks.list` reads back to say who blocked a name and since when.
_BLOCK_HISTORY = 200


def redacted(doc: Any) -> Any:
    """The registry with role ARNs and secret references hidden (lists walked too)."""
    if isinstance(doc, Mapping):
        return {k: "[redacted]" if k in SECRET_FIELDS else redacted(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [redacted(v) for v in doc]
    return doc


Change = Callable[[dict[str, Any]], None]


@store_op
async def _edit(ctx: AdminContext, change: Change, *, actor: str, dry_run: bool) -> dict[str, Any]:
    """Apply (or plan) one registry edit: `{diff, version, dry_run}`.

    `store_op`: the read of the document before the edit (`RegistryStore._read`) is not an
    admin method, so a store outage there reached the app as redis-py's `ConnectionError`
    instead of `StoreUnavailable` (scenario X2: an edit during a store block).
    """
    store = ctx.registry
    if dry_run:
        diff = await store.plan_edit(change, actor=actor)
        version = await store.stored_version()
    else:
        doc, _ = await store._read()
        view = await store.edit(change, actor=actor)
        diff = audit_diff(doc or {}, dict(view.registry.raw))
        version = view.registry.version
    return {"diff": redacted(diff), "version": version, "dry_run": dry_run}


class RegistryAdmin:
    """The registry document: read, live edits, publish and rollback."""

    def __init__(self, ctx: AdminContext) -> None:
        """Over one environment's registry store."""
        self._ctx = ctx

    @property
    def current(self) -> ResolvedRegistry:
        """The registry this process runs on (read-only; `ConfigError` before it is loaded)."""
        return self._ctx.registry.current

    @property
    def changes(self) -> int:
        """The registry's change counter as this process last loaded it (publishes and edits)."""
        return self._ctx.registry.changes

    async def get(self, model: str | None = None) -> dict[str, Any]:
        """The live registry (or one model's entry), secrets hidden, and its live edits."""
        view = await self._ctx.view()
        doc: Any = dict(view.registry.raw)
        if model is not None:
            doc = doc["models"][view.registry.alias_of(model)]
        live_edits = await self._ctx.registry.diff_with_config()
        return {
            "registry": redacted(doc),
            "changes": self._ctx.registry.changes,
            "live_edits": redacted(live_edits),
        }

    async def versions(self) -> dict[str, Any]:
        """Versions kept for rollback (oldest first) and the stored version."""
        store = self._ctx.registry
        versions, current = await asyncio.gather(store.versions(), store.stored_version())
        return {"versions": versions, "current": current}

    async def diff(self) -> dict[str, Any]:
        """What is live but not in this process's registry document (a deploy would drop it)."""
        return {"live_edits": redacted(await self._ctx.registry.diff_with_config())}

    def schema(self) -> dict[str, Any]:
        """The registry document's JSON Schema (no store needed)."""
        from llm_queue.registry.json_schema import registry_schema

        return registry_schema()

    async def set(
        self,
        values: Mapping[str, Any],
        *,
        model: str | None = None,
        actor: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Live edit of dotted paths (`{"rpm": 400}` with `model=`); None deletes a path.

        Tenant names in `fairness.weights` / `quotas` paths are hashed here (RQ-5: the raw
        name was stored and never matched a call); a 16-hex name is refused.
        """
        if not values:
            raise ConfigError("give at least one path=value")
        hash_id = self._ctx.hash_id
        hashed = dict(
            pair
            for path, value in values.items()
            for pair in hash_tenant_path(path, value, hash_id)
        )
        edit = self._ctx.registry.set_change(hashed, model=model)

        def change(doc: dict[str, Any]) -> None:
            edit(doc)
            # The whole document after the edit: a name set into a hashed block is refused.
            fixed = hash_tenant_names(doc, hash_id)
            doc.clear()
            doc.update(fixed)

        return await _edit(self._ctx, change, actor=actor, dry_run=dry_run)

    async def publish(self, *, actor: str, dry_run: bool = True) -> dict[str, Any]:
        """Publish this process's registry document by the version rule (`LLMQ_REGISTRY`)."""
        doc = self._ctx.config_document()
        if doc is None:
            raise ConfigError("LLMQ_REGISTRY (or LLMQ_REGISTRY_FILE) is not set")
        # Hashed at load already; an exported (marked) document is never hashed twice.
        doc = hash_tenant_names(doc, self._ctx.hash_id)
        return await self._publish(doc, actor=actor, dry_run=dry_run, rollback=None)

    async def rollback(self, version: int, *, actor: str, dry_run: bool = True) -> dict[str, Any]:
        """Publish version `version`'s content as the next version (live blocks are kept)."""
        store = self._ctx.registry
        kept = await store.kept(version)
        if hash_tenant_names(kept, self._ctx.hash_id) != kept:
            # Only a version stored before names were hashed on every write holds names.
            raise ConfigError(
                f"version {version} holds tenant names, not hashes: publish a corrected document"
            )
        doc = {**kept, "registry_version": await store.stored_version() + 1}
        return await self._publish(doc, actor=actor, dry_run=dry_run, rollback=version)

    async def _publish(
        self, doc: dict[str, Any], *, actor: str, dry_run: bool, rollback: int | None
    ) -> dict[str, Any]:
        store = self._ctx.registry
        plan = await store.plan_publish(doc)
        out = {**plan, "diff": redacted(plan["diff"]), "dry_run": dry_run}
        if dry_run:
            return out
        if rollback is None:
            result = await store.publish(doc, actor=actor)
        else:
            result = await store.rollback(rollback, actor=actor)
        return {**out, "action": result.action, "version": result.version,
                "carried": list(result.carried)}  # fmt: skip


class AuditAdmin:
    """The audit stream: registry edits, publishes, purges and every admin write."""

    def __init__(self, ctx: AdminContext) -> None:
        """Over one environment's audit stream."""
        self._ctx = ctx

    async def list(
        self, count: int = 20, *, path: str = "", op: str | None = None
    ) -> dict[str, Any]:
        """Latest entries, newest first; `path` keeps entries touching that prefix, `op` one op.

        Best effort: it looks at the newest `count * 5` entries.
        """
        if not 1 <= count <= MAX_AUDIT_COUNT:
            raise ConfigError(f"count must be 1-{MAX_AUDIT_COUNT}")
        entries = await self._ctx.registry.log(count * 5 if op else count, path)
        if op is not None:
            entries = [e for e in entries if e.get("op") == op][:count]
        return {"entries": entries}


class ModelsAdmin:
    """Models: list, read, enable / disable and drain (create / delete: publish, purge)."""

    def __init__(self, ctx: AdminContext) -> None:
        """Over one environment's registry and status."""
        self._ctx = ctx

    async def list(self) -> dict[str, Any]:
        """Every model: type, provider, queues, waiting, in progress, deployments, usable ones."""
        view = await self._ctx.view()
        report = await self._ctx.status.report(view)
        out = []
        for status in report["models"]:
            alias = status["model"]
            entry = view.model(alias)
            out.append(
                {
                    "model": alias,
                    "type": entry.type,
                    "provider": entry.provider,
                    "enabled": status["enabled"],
                    "queue": {
                        "normal": self._ctx.queue_name(view, alias, False),
                        "priority": self._ctx.queue_name(view, alias, True),
                    },
                    "waiting": status["waiting"],
                    "in_progress": status["in_progress"],
                    "dead_letters": status["dead_letters"],
                    "deployments": len(status["deployments"]),
                    "usable_deployments": sum(usable(d) for d in status["deployments"]),
                }
            )
        return {"models": out}

    async def get(self, model: str) -> dict[str, Any]:
        """One model: type, provider, resolved settings (with their level) and live status."""
        view = await self._ctx.view()
        alias = view.registry.alias_of(model)
        entry = view.model(alias)
        live = await self._ctx.status.model(view, alias)
        return {
            "model": alias,
            "type": entry.type,
            "provider": entry.provider,
            "enabled": live["enabled"],
            "settings": _table(view, alias),
            "live": live,
        }

    async def enable(self, model: str, *, actor: str, dry_run: bool = True) -> dict[str, Any]:
        """Take new calls for `model` again."""
        return await self._switch(model, True, actor=actor, dry_run=dry_run)

    async def disable(self, model: str, *, actor: str, dry_run: bool = True) -> dict[str, Any]:
        """Refuse new calls for `model` (waiting calls are still served)."""
        return await self._switch(model, False, actor=actor, dry_run=dry_run)

    async def _switch(
        self, model: str, enabled: bool, *, actor: str, dry_run: bool
    ) -> dict[str, Any]:
        view = await self._ctx.view()
        alias = view.registry.alias_of(model)
        change = self._ctx.registry.set_change({"enabled": enabled}, model=alias)
        out = await _edit(self._ctx, change, actor=actor, dry_run=dry_run)
        return {"model": alias, "enabled": enabled, **out}

    async def drain(
        self, model: str, *, wait_secs: float = 0.0, actor: str, dry_run: bool = True
    ) -> dict[str, Any]:
        """Disable `model`, then wait up to `wait_secs` for its queues to empty.

        `drained`: True once both queues are empty, False when the wait ran out, None when
        not waited for. Undo with `models.enable`.
        """
        if wait_secs < 0:
            raise ConfigError("wait_secs must be 0 or more")
        check_actor(actor)
        out = await self._switch(model, False, actor=actor, dry_run=dry_run)
        alias = out["model"]
        out["waiting"] = await self._waiting(alias)
        out["drained"] = None
        if dry_run:
            return out
        await self._ctx.audit.write(
            kind="models", actor=actor, op="model_drain", path=f"models.{alias}", before=None,
            after={"enabled": False, "waiting": out["waiting"], "wait_secs": wait_secs},
        )  # fmt: skip
        if wait_secs <= 0:
            return out
        loop = asyncio.get_running_loop()
        until = loop.time() + wait_secs
        while True:
            out["waiting"] = await self._waiting(alias)
            if out["waiting"] == 0:
                out["drained"] = True
                return out
            if loop.time() >= until:
                out["drained"] = False
                return out
            await asyncio.sleep(min(1.0, max(0.0, until - loop.time())))

    async def _waiting(self, alias: str) -> int:
        view = await self._ctx.view()
        depths = await asyncio.gather(
            *(self._ctx.queues.depth(self._ctx.queue_name(view, alias, p)) for p in (True, False))
        )
        return sum(total for total, _, _ in depths)


def usable(dep: Mapping[str, Any]) -> bool:
    """The `model_unavailable` alert's rule: a deployment that can take a call now."""
    return bool(
        dep["enabled"]
        and dep["account_enabled"]
        and not dep["blocked"]
        and dep["healthy"]
        and not dep["outside_residency"]
        and dep["breaker"] != "open"
    )


def _table(view: ResolvedRegistry, alias: str, deployment: str | None = None) -> dict[str, Any]:
    table = view.table(alias, deployment)
    out: dict[str, Any] = redacted(
        {name: {"value": e.value, "level": e.level} for name, e in table.items()}
    )
    return out


class DeploymentsAdmin:
    """Deployments of a model: live state, settings, enable / disable."""

    def __init__(self, ctx: AdminContext) -> None:
        """Over one environment's registry and status."""
        self._ctx = ctx

    async def list(self, model: str) -> dict[str, Any]:
        """Every deployment of `model` with budgets, concurrency, breaker, health, `usable`."""
        view = await self._ctx.view()
        alias = view.registry.alias_of(model)
        live = await self._ctx.status.model(view, alias)
        rows = [{**d, "usable": usable(d)} for d in live["deployments"]]
        return {"model": alias, "deployments": rows}

    async def get(self, model: str, deployment: str) -> dict[str, Any] | None:
        """One deployment (`account/name`): its live state and resolved settings; None if none."""
        listed = await self.list(model)
        view = await self._ctx.view()
        for row in listed["deployments"]:
            if row["name"] == deployment:
                return {**row, "settings": _table(view, listed["model"], deployment)}
        return None

    async def enable(
        self, model: str, deployment: str, *, actor: str, dry_run: bool = True
    ) -> dict[str, Any]:
        """Route calls to `deployment` again."""
        return await self._switch(model, deployment, True, actor=actor, dry_run=dry_run)

    async def disable(
        self, model: str, deployment: str, *, actor: str, dry_run: bool = True
    ) -> dict[str, Any]:
        """Route no new calls to `deployment` (running ones finish)."""
        return await self._switch(model, deployment, False, actor=actor, dry_run=dry_run)

    async def _switch(
        self, model: str, deployment: str, enabled: bool, *, actor: str, dry_run: bool
    ) -> dict[str, Any]:
        view = await self._ctx.view()
        alias = view.registry.alias_of(model)
        if deployment not in {d.key for d in view.deployments(alias)}:
            raise ConfigError(f"model {alias} has no deployment {deployment!r} (account/name)")
        change = self._ctx.registry.enabled_change(alias, deployment, enabled)
        out = await _edit(self._ctx, change, actor=actor, dry_run=dry_run)
        return {"model": alias, "deployment": deployment, "enabled": enabled, **out}


class BlocksAdmin:
    """Routing blocks of regions and accounts, and the kill switch."""

    def __init__(self, ctx: AdminContext) -> None:
        """Over one environment's registry."""
        self._ctx = ctx

    async def list(self) -> dict[str, Any]:
        """What is blocked, and by whom and since when (from the audit; best effort)."""
        view = await self._ctx.view()
        routing = view.registry.routing
        entries = await self._ctx.registry.log(_BLOCK_HISTORY, "routing")

        def who(field: str, name: str | None = None) -> dict[str, Any]:
            for entry in entries:  # newest first: the latest time it was turned on
                for row in entry["diff"]:
                    if row.get("path") != f"routing.{field}":
                        continue
                    after, before = row.get("after"), row.get("before")
                    if name is None:
                        turned_on = after is True and before is not True
                    else:
                        turned_on = _has(after, name) and not _has(before, name)
                    if turned_on:
                        return {"since": float(entry.get("ts") or 0), "by": entry.get("actor")}
            return {"since": None, "by": None}

        return {
            "regions": [{"name": r, **who("blocked_regions", r)} for r in routing.blocked_regions],
            "accounts": [
                {"name": a, **who("blocked_accounts", a)} for a in routing.blocked_accounts
            ],
            "kill_switch": {
                "on": routing.kill_switch,
                **(who("kill_switch") if routing.kill_switch else {"since": None, "by": None}),
            },
        }

    async def add(
        self,
        *,
        region: str | None = None,
        account: str | None = None,
        actor: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Block a region or an account for every model (an unknown name is refused)."""
        return await self._block(region, account, True, actor=actor, dry_run=dry_run)

    async def remove(
        self,
        *,
        region: str | None = None,
        account: str | None = None,
        actor: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Lift a region or account block."""
        return await self._block(region, account, False, actor=actor, dry_run=dry_run)

    async def _block(
        self, region: str | None, account: str | None, on: bool, *, actor: str, dry_run: bool
    ) -> dict[str, Any]:
        await self._ctx.view()  # loaded: the name checks read the registry
        change = self._ctx.registry.block_change(region=region, account=account, on=on)
        out = await _edit(self._ctx, change, actor=actor, dry_run=dry_run)
        routing = (await self._ctx.view()).registry.routing
        return {
            "blocked_regions": list(routing.blocked_regions),
            "blocked_accounts": list(routing.blocked_accounts),
            **out,
        }

    async def kill_switch(self, on: bool, *, actor: str, dry_run: bool = True) -> dict[str, Any]:
        """Turn the kill switch on (every call refused) or off."""

        def change(doc: dict[str, Any]) -> None:
            doc.setdefault("routing", {})["kill_switch"] = bool(on)

        out = await _edit(self._ctx, change, actor=actor, dry_run=dry_run)
        return {"kill_switch": bool(on), **out}


def _has(value: Any, name: str) -> bool:
    return isinstance(value, list) and name in value
