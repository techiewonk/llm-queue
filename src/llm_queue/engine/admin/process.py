"""Process-local admin: drain, describe, and the operations index with its live summary."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

import orjson

from llm_queue._version import __version__
from llm_queue.engine.admin.context import AdminContext
from llm_queue.engine.admin.ops import OPERATIONS, op_row
from llm_queue.engine.admin.registry import usable
from llm_queue.engine.settings import InfraSettings
from llm_queue.engine.store_guard import STORE_DOWN
from llm_queue.errors import LLMQueueError


class DrainAdmin:
    """Drain of the process this admin runs in (pods with several processes: the drain file)."""

    def __init__(self, ctx: AdminContext) -> None:
        """Acts on `ctx.process`, the `LLMQueue`."""
        self._ctx = ctx

    def get(self) -> dict[str, Any]:
        """`draining`, `since`, `held` (calls waiting for budget), `running` (calls and jobs)."""
        return dict(self._ctx.needs_process("drain").drain_status())

    async def start(self, *, actor: str) -> dict[str, Any]:
        """Stop taking calls and jobs here (readiness fails); held calls go back at once."""
        process = self._ctx.needs_process("drain")
        process.pause_receiving()
        await self._audit(actor, "drain")
        return dict(process.drain_status())

    async def resume(self, *, actor: str) -> dict[str, Any]:
        """Take calls and jobs again; `LLMQueueError` once the queue is stopping."""
        process = self._ctx.needs_process("resume")
        process.resume_receiving()
        await self._audit(actor, "resume")
        return dict(process.drain_status())

    async def _audit(self, actor: str, op: str) -> None:
        worker = self._ctx.engine.worker_id if self._ctx.engine is not None else "?"
        # Best effort: the drain happened; a store outage must not hide that from the caller.
        with contextlib.suppress(LLMQueueError, *STORE_DOWN):
            await self._ctx.audit.write(
                actor=actor, op=op, path=f"drain.{worker}", before=None, after={"op": op}
            )


def describe_settings(settings: InfraSettings | None, worker_id: str | None) -> dict[str, Any]:
    """Settings for a log line: no password, salt, token or registry body."""
    out: dict[str, Any] = {"version": __version__, "worker_id": worker_id}
    s = settings
    if s is None:
        return out
    doc = orjson.loads(s.registry_document()) if (s.registry or s.registry_file) else {}
    out["execution_mode"] = s.execution_mode
    if s.execution_mode == "local":  # no store: its settings do not apply
        out.update(
            environment=s.environment,
            local_process_count=s.local_process_count,
            local_concurrency=s.local_concurrency,
            metrics_sink=s.metrics_sink,
            default_tenant=s.default_tenant,
            admin_writes_enabled=s.admin_writes_enabled,
            registry_version=doc.get("registry_version"),
            models=sorted(doc.get("models") or {}),
        )
        return out
    store = urlsplit(s.store_url)
    out.update(
        mode=s.mode,
        environment=s.environment,
        store=f"{store.scheme}://{store.hostname}:{store.port or 6379}",
        separate_queue_store=s.queue_store_url is not None,
        key_prefix=s.key_prefix,
        queue_prefix=s.queue_prefix,
        key_expiry=s.key_expiry,
        store_max_connections=s.store_max_connections,
        metrics_sink=s.metrics_sink,
        default_tenant=s.default_tenant,
        admin_writes_enabled=s.admin_writes_enabled,
        registry_version=doc.get("registry_version"),
        models=sorted(doc.get("models") or {}),
    )
    return out


class OpsAdmin:
    """The operations table and the `GET /` live summary."""

    def __init__(self, ctx: AdminContext) -> None:
        """Over one environment (and its process, when there is one)."""
        self._ctx = ctx

    def list(self) -> list[dict[str, Any]]:
        """Every operation: id, resource, verb, Python call, CLI command, route, guard."""
        return [op_row(op) for op in OPERATIONS]

    async def index(self, routes: Sequence[dict[str, Any]] | None = None) -> dict[str, Any]:
        """The live summary (library, process, stores, registry, models, jobs, purges).

        `routes`: the routes a router mounted (`GET /` passes them); default every operation.
        A part that cannot be read (the store is down) is None, never an error.
        """
        ctx = self._ctx
        engine = ctx.engine
        health: dict[str, Any] | None = None
        if engine is not None:
            health = await engine.health_check()
        settings = ctx.settings
        out: dict[str, Any] = {
            "library": {
                "version": __version__,
                "worker": engine.worker_id if engine is not None else None,
                "mode": settings.mode if settings else None,
                "execution_mode": "queue",
                "environment": settings.environment if settings else None,
            },
            "process": None,
            "stores": None,
            "registry": None,
            "models": None,
            "jobs": None,
            "purges": None,
            "writes": any(r.get("writes") for r in (routes or ())),
            "routes": list(routes) if routes is not None else self.list(),
        }
        if health is not None:
            out["process"] = {k: health.get(k) for k in (
                "live", "ready", "not_ready_reason", "draining", "draining_since", "held",
                "heartbeat_age_secs",
            )}  # fmt: skip
            out["stores"] = {
                "state": health["stores"].get("state"),
                "queue": health["stores"].get("queue"),
                "key_expiry": health.get("key_expiry"),
                "eviction_policy": health.get("eviction_policy"),
            }
        try:
            view = await ctx.view()
            report, live_edits, scheduled = await asyncio.gather(
                ctx.status.report(view), ctx.registry.diff_with_config(), ctx.purge.scheduled()
            )
        except (LLMQueueError, *STORE_DOWN):
            return out
        routing = view.registry.routing
        out["registry"] = {
            "version": view.registry.version,
            "changes": ctx.registry.changes,
            "live_edits": len(live_edits),
            "kill_switch": routing.kill_switch,
            "blocked_regions": list(routing.blocked_regions),
            "blocked_accounts": list(routing.blocked_accounts),
        }
        out["models"] = [
            {
                "model": m["model"],
                "enabled": m["enabled"],
                "waiting": sum(m["waiting"].values()),
                "waiting_for_grant": sum(m["waiting_for_grant"].values()),
                "in_progress": m["in_progress"],
                "oldest_waiting_secs": m["oldest_waiting_secs"],
                "dead_letters": m["dead_letters"],
                "deployments": len(m["deployments"]),
                "usable_deployments": sum(usable(d) for d in m["deployments"]),
                "open_breakers": sum(d["breaker"] == "open" for d in m["deployments"]),
            }
            for m in report["models"]
        ]
        out["jobs"] = report.get("jobs", [])
        out["purges"] = {
            "scheduled": len(scheduled),
            "next_due_at": min((row["due_at"] for row in scheduled), default=None),
        }
        return out
