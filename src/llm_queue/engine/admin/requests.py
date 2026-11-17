"""The requests operations (`llmq.admin.requests`): the one module between them and the service.

Every method delegates to `RequestsAdmin` (design T2, `engine/requests_admin.py`) over the
per-model, per-tenant request index (T1): the engine's own (`Engine.requests_admin`) inside a
running queue, else one built over the admin context's stores (`AdminServices.from_env`, the
CLI). Results are the service's reports as JSON-ready dicts.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, cast

from llm_queue.engine.admin.context import AdminContext
from llm_queue.engine.requests_admin import AdminSettings, RequestsAdmin

State = Literal["waiting", "running", "all"]


def _service(ctx: AdminContext) -> RequestsAdmin:
    """The requests service of `ctx`: the engine's, the context's, or one over its stores."""
    if ctx.engine is not None:
        return ctx.engine.requests_admin
    if ctx.requests is None:
        settings = ctx.settings
        ctx.requests = RequestsAdmin(
            ctx.state,
            queue_client=ctx.queue_client,
            keys=ctx.keys,
            registry=ctx.registry,
            queue_prefix=ctx.queue_prefix,
            hash_id=ctx.hash_id,
            settings=AdminSettings.from_infra(settings) if settings is not None else None,
            status_ttl_secs=ctx.status_ttl_secs,
        )
    return ctx.requests


class RequestsAdapter:
    """`llmq.admin.requests`: calls in flight by id, or by model, tenant and state."""

    def __init__(self, ctx: AdminContext) -> None:
        """Over one environment's stores."""
        self._ctx = ctx
        self._svc = _service(ctx)

    async def list(
        self,
        model: str,
        *,
        tenant: str | None = None,
        tenant_hash: str | None = None,
        state: State = "all",
        limit: int = 100,
        after: str | None = None,
    ) -> dict[str, Any]:
        """A page of `model`'s calls in flight (of one tenant), oldest deadline first."""
        page = await self._svc.list(
            model, tenant=tenant, tenant_hash=tenant_hash, state=state, limit=limit, after=after
        )
        return _plain(page)

    async def count(
        self, model: str, *, tenant: str | None = None, tenant_hash: str | None = None
    ) -> dict[str, Any]:
        """Counts of `model`'s calls in flight by state and stage (of one tenant)."""
        return _plain(await self._svc.count(model, tenant=tenant, tenant_hash=tenant_hash))

    async def get(self, request_id: str) -> dict[str, Any] | None:
        """One call: summary, status, `final`, callers; None when unknown or expired."""
        found = await self._svc.get(request_id)
        return None if found is None else _plain(found)

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
    ) -> dict[str, Any]:
        """Cancel calls by `request_ids`, or by `model` (and tenant) in `state`.

        `state` (default `waiting`) keeps calls that already run out of a cancel: `running`
        or `all` must be asked for. A call already final or already cancelling is skipped
        (`skipped` says why); a second run is a no-op.
        """
        report = await self._svc.cancel(
            model, request_ids=request_ids, tenant=tenant, tenant_hash=tenant_hash,
            state=state, actor=actor, dry_run=dry_run,
        )  # fmt: skip
        return _plain(report)

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
    ) -> dict[str, Any]:
        """Move `model`'s waiting calls (of one tenant) to the `priority` class or back."""
        report = await self._svc.reprioritize(
            model, priority=priority, tenant=tenant, tenant_hash=tenant_hash, limit=limit,
            actor=actor, dry_run=dry_run,
        )  # fmt: skip
        return _plain(report)

    async def extend(
        self, request_id: str, *, timeout_secs: float, actor: str, dry_run: bool = True
    ) -> dict[str, Any]:
        """Give one call `timeout_secs` more (its caller, worker and records)."""
        report = await self._svc.extend(
            request_id, timeout_secs=timeout_secs, actor=actor, dry_run=dry_run
        )
        return _plain(report)

    async def tenants(self, model: str) -> dict[str, Any]:
        """Tenants with calls in flight on `model` (hashes), for `llmq.admin.tenants.list`."""
        return await self._svc.tenants(model)

    async def purge_queue(
        self,
        model: str,
        *,
        tenant: str | None = None,
        tenant_hash: str | None = None,
        actor: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Cancel `model`'s queued calls (of one tenant), for `llmq.admin.purge.queue`."""
        report = await self._svc.cancel(
            model, tenant=tenant, tenant_hash=tenant_hash, state="waiting", actor=actor,
            dry_run=dry_run, op="purge_queue",
        )  # fmt: skip
        return _plain(report)

    def tenant_hash(self, tenant: str | None, tenant_hash: str | None) -> str | None:
        """The tenant hash asked for (a raw name hashed; None: no tenant asked for)."""
        return self._svc.pick_tenant(tenant, tenant_hash)


def _plain(value: Any) -> dict[str, Any]:
    """A service result as JSON types (`as_dict()` of the service's dataclasses)."""
    as_dict = getattr(value, "as_dict", None)
    return cast("dict[str, Any]", as_dict() if callable(as_dict) else value)
