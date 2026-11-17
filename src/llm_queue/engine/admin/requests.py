"""The requests operations (`llmq.admin.requests`): the one module between them and the service.

The per-model, per-tenant requests service (design T2: `RequestsAdmin` over the T1 index, list
/ count / cancel / reprioritize / extend) is built on its own branch. Until it lands, this
adapter serves what works without the index (one call by id: `get`, and `cancel` by request
ids) and refuses the rest with `AdminRefused(UNAVAILABLE)`. Wiring the service is a change to
`_service()` only: every method below already has the service's signature.
"""

from __future__ import annotations

import builtins
import time
from collections.abc import Sequence
from typing import Any, Literal, cast

from llm_queue._logging import get_logger
from llm_queue.engine._codec import text as _text
from llm_queue.engine.admin.context import AdminContext
from llm_queue.engine.requests import RequestStore
from llm_queue.errors import AdminRefused, ConfigError

logger = get_logger(__name__)
State = Literal["waiting", "running", "all"]
STATES: tuple[str, ...] = ("waiting", "running", "all")
UNAVAILABLE = "requests service not available"
MAX_CANCEL_IDS = 1000  # one cancel by ids at most
MAX_ID_CHARS = 256
SAMPLE_IDS = 20
_FINAL = frozenset({"done", "failed", "expired", "cancelled"})
# `{r:ID}:job` state -> admin state.
_ADMIN_STATE = {"queued": "waiting", "running": "running"}


def _service(ctx: AdminContext) -> Any:
    """The requests service of `ctx`, or None while it is not built (see the module doc)."""
    return None


class RequestsAdapter:
    """`llmq.admin.requests`: calls in flight, by id now, by model and tenant with T1."""

    def __init__(self, ctx: AdminContext) -> None:
        """Over one environment's stores."""
        self._ctx = ctx
        self._svc = _service(ctx)

    def _need(self, what: str) -> Any:
        if self._svc is None:
            raise AdminRefused(f"{UNAVAILABLE}: {what} needs the per-model request index")
        return self._svc

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
        svc = self._need("requests list")
        page = await svc.list(
            model, tenant=tenant, tenant_hash=tenant_hash, state=state, limit=limit, after=after
        )
        return _plain(page)

    async def count(
        self, model: str, *, tenant: str | None = None, tenant_hash: str | None = None
    ) -> dict[str, Any]:
        """Counts of `model`'s calls in flight by state and stage (of one tenant)."""
        svc = self._need("requests count")
        return _plain(await svc.count(model, tenant=tenant, tenant_hash=tenant_hash))

    async def get(self, request_id: str) -> dict[str, Any] | None:
        """One call: `{status, final}`; None when unknown or expired."""
        if self._svc is not None:
            found = await self._svc.get(request_id)
            return None if found is None else _plain(found)
        _check_ids([request_id])
        status = await self._ctx.status.request(request_id)
        if not status:
            return None
        return {"status": status, "final": status.get("state") in _FINAL}

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
        if state not in STATES:
            raise ConfigError(f"state must be one of {', '.join(STATES)}")
        if self._svc is not None:
            report = await self._svc.cancel(
                model, request_ids=request_ids, tenant=tenant, tenant_hash=tenant_hash,
                state=state, actor=actor, dry_run=dry_run,
            )  # fmt: skip
            return _plain(report)
        if request_ids is None:
            self._need("cancel by model and tenant")
        if model is not None or tenant is not None or tenant_hash is not None:
            raise ConfigError("cancel by request ids takes no model or tenant")
        ids = list(dict.fromkeys(request_ids or ()))
        _check_ids(ids)
        return await self._cancel_ids(ids, state, actor=actor, dry_run=dry_run)

    async def _cancel_ids(
        self, ids: Sequence[str], state: str, *, actor: str, dry_run: bool
    ) -> dict[str, Any]:
        ctx = self._ctx
        async with ctx.state.pipeline(transaction=False) as pipe:
            for rid in ids:
                pipe.hget(ctx.keys.request(rid, "job"), "state")
                pipe.exists(ctx.keys.request(rid, "cancel"))
            replies = await pipe.execute()
        chosen: builtins.list[str] = []
        skipped: dict[str, int] = {}
        by_state: dict[str, int] = {}
        for i, rid in enumerate(ids):
            raw, marked = replies[2 * i], replies[2 * i + 1]
            job_state = _text(raw) if raw is not None else None
            admin_state = _ADMIN_STATE.get(job_state or "")
            if admin_state is None:
                reason = "final" if job_state in _FINAL else "unknown"
            elif marked:
                reason = "already_cancelling"
            elif state != "all" and admin_state != state:
                reason = "started" if state == "waiting" else "waiting"
            else:
                chosen.append(rid)
                by_state[admin_state] = by_state.get(admin_state, 0) + 1
                continue
            skipped[reason] = skipped.get(reason, 0) + 1
        changed = 0
        if not dry_run and chosen:
            store = RequestStore(ctx.state, ctx.keys, status_ttl_secs=ctx.status_ttl_secs)
            changed = await store.cancel(
                chosen, ttl_secs=ctx.status_ttl_secs, channel=ctx.keys.cancel_channel
            )
            for rid in chosen:
                logger.info("call_cancel", request_id=rid, source="admin", by=actor)
            await ctx.audit.write(
                actor=actor, op="requests_cancel", path="requests.*.*", before=None,
                after={"matched": len(chosen), "changed": changed, "by_state": by_state},
            )  # fmt: skip
        return {
            "op": "requests_cancel",
            "model": None,
            "tenant": None,
            "dry_run": dry_run,
            "actor": actor,
            "matched": len(chosen),
            "changed": changed,
            "skipped": skipped,
            "by_state": by_state,
            "sample_ids": chosen[:SAMPLE_IDS],
            "at": time.time(),
        }

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
        svc = self._need("requests reprioritize")
        report = await svc.reprioritize(
            model, priority=priority, tenant=tenant, tenant_hash=tenant_hash, limit=limit,
            actor=actor, dry_run=dry_run,
        )  # fmt: skip
        return _plain(report)

    async def extend(
        self, request_id: str, *, timeout_secs: float, actor: str, dry_run: bool = True
    ) -> dict[str, Any]:
        """Give one call `timeout_secs` more (its caller, worker and records)."""
        svc = self._need("requests extend")
        report = await svc.extend(
            request_id, timeout_secs=timeout_secs, actor=actor, dry_run=dry_run
        )
        return _plain(report)

    async def tenants(self, model: str) -> dict[str, Any]:
        """Tenants with calls in flight on `model` (hashes), for `llmq.admin.tenants.list`."""
        svc = self._need("tenants list")
        return _plain(await svc.tenants(model))

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
        svc = self._need("purge queue")
        report = await svc.cancel(
            model, tenant=tenant, tenant_hash=tenant_hash, state="waiting", actor=actor,
            dry_run=dry_run,
        )  # fmt: skip
        return _plain(report)


def _check_ids(ids: Sequence[str]) -> None:
    if not 1 <= len(ids) <= MAX_CANCEL_IDS or any(not r or len(r) > MAX_ID_CHARS for r in ids):
        raise ConfigError(
            f"give 1-{MAX_CANCEL_IDS} request ids of at most {MAX_ID_CHARS} characters"
        )


def _plain(value: Any) -> dict[str, Any]:
    """A service result as JSON types (`as_dict()` of the service's dataclasses)."""
    as_dict = getattr(value, "as_dict", None)
    return cast("dict[str, Any]", as_dict() if callable(as_dict) else value)
