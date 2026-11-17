"""Admin services over the queues: status, queues, dead letters, jobs, purges, tenants."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import orjson

from llm_queue.engine._codec import text as _text
from llm_queue.engine.admin.context import AdminContext
from llm_queue.engine.admin.requests import RequestsAdapter
from llm_queue.engine.queue import RETENTION_SECS
from llm_queue.engine.status import health_report
from llm_queue.errors import ConfigError

MAX_DLQ_COUNT = 10_000
MAX_DLQ_IDS = 1000


class StatusAdmin:
    """Live status of every model and job, or of one model."""

    def __init__(self, ctx: AdminContext, requests: RequestsAdapter) -> None:
        """Over one environment's stores and request index service."""
        self._ctx = ctx
        self._requests = requests

    async def get(
        self,
        model: str | None = None,
        *,
        tenant: str | None = None,
        tenant_hash: str | None = None,
    ) -> dict[str, Any]:
        """`StatusReport` of everything, or `ModelStatus` of `model`.

        With `tenant` (a name, hashed here) or `tenant_hash` (`-`: calls with no tenant), the
        model's report adds a `tenant` block: its calls by stage and its fairness numbers.
        """
        h = self._requests.tenant_hash(tenant, tenant_hash)
        if h is not None and model is None:
            raise ConfigError("a tenant's status needs a model")
        view = await self._ctx.view()
        if model is not None:
            alias = view.registry.alias_of(model)
            return await self._ctx.status.model(view, alias, tenant_hash=h)
        return await self._ctx.status.report(view)

    async def health(self, model: str | None = None) -> dict[str, Any]:
        """Health summary (0.2.0 F2): overall and per model `healthy` / `warning` / `critical`.

        Each row has its `reasons`, budget use this minute, waiting calls, the oldest wait and
        usable deployments; thresholds are the model's `health_summary` registry setting.
        """
        view = await self._ctx.view()
        if model is not None:
            alias = view.registry.alias_of(model)
            row = await self._ctx.status.model(view, alias)
            report: dict[str, Any] = {"registry_version": view.registry.version, "models": [row]}
        else:
            report = await self._ctx.status.report(view)
        return health_report(view, report)


class QueuesAdmin:
    """Every model and job queue."""

    def __init__(self, ctx: AdminContext) -> None:
        """Over one environment's queue store."""
        self._ctx = ctx

    async def list(self) -> dict[str, Any]:
        """Per queue: class, messages, waiting, held, dead letters, oldest wait, consumer."""
        ctx = self._ctx
        view = await ctx.view()
        rows: list[dict[str, Any]] = []
        for alias in view.registry.models:
            for priority in (False, True):
                name = ctx.queue_name(view, alias, priority)
                rows.append({"queue": name, "model": alias, "job": None,
                             "class": "priority" if priority else "normal"})  # fmt: skip
        for job in await _job_names(ctx):
            rows.append({"queue": ctx.job_queue(job), "model": None, "job": job, "class": "job"})
        now = await _now_secs(ctx)
        details = await asyncio.gather(*(_queue_state(ctx, r["queue"], now) for r in rows))
        return {"queues": [{**r, **d} for r, d in zip(rows, details, strict=True)]}


async def _job_names(ctx: AdminContext) -> list[str]:
    key = f"{ctx.keys.prefix}:jobs:{ctx.queue_prefix}"
    return sorted(_text(n) for n in await ctx.queues.client.smembers(key))


async def _now_secs(ctx: AdminContext) -> float:
    secs, micros = await ctx.queues.client.time()
    return float(secs) + float(micros) / 1e6


async def _queue_state(ctx: AdminContext, queue: str, now: float) -> dict[str, Any]:
    (total, held, dead), first, consumer = await asyncio.gather(
        ctx.queues.depth(queue), ctx.status._first_id(queue), ctx.queues.has_consumer(queue)
    )
    return {
        "messages": total,
        "waiting": max(0, total - held),
        "held": held,
        "dead_letters": dead,
        "oldest_wait_secs": None if first is None else round(max(0.0, now - first / 1000), 1),
        "consumer": consumer,
    }


class DlqAdmin:
    """Dead letters of a model's queue (normal or priority) or of a job name."""

    def __init__(self, ctx: AdminContext) -> None:
        """Over one environment's queue store."""
        self._ctx = ctx

    async def _queue(self, model: str | None, job: str | None, priority: bool) -> tuple[str, int]:
        """The queue and its retention (seconds): give exactly one of `model` and `job`."""
        if (model is None) == (job is None):
            raise ConfigError("give exactly one of model and job")
        if job is not None:
            if priority:
                raise ConfigError("priority is for a model's queue, not a job's")
            return self._ctx.job_queue(job), RETENTION_SECS
        view = await self._ctx.view()
        alias = view.registry.alias_of(model or "")
        keep = int(view.value(alias, "queue")["retention_secs"])
        return self._ctx.queue_name(view, alias, priority), keep

    def _letter(self, letter: dict[str, Any], *, body: bool) -> dict[str, Any]:
        """A dead letter for output: ids from its body, the tenant only as its hash."""
        out = {k: v for k, v in letter.items() if k != "body"}
        try:
            parsed = orjson.loads(letter["body"])
        except (orjson.JSONDecodeError, TypeError):
            parsed = None
        if isinstance(parsed, dict):
            if "request_id" in parsed:  # a call's envelope
                out["request_id"] = parsed.get("request_id")
            else:  # a job's body
                out["job_id"] = parsed.get("id")
            tenant = parsed.get("tenant_id")
            out["tenant"] = self._ctx.hash_id(tenant) if isinstance(tenant, str) else None
        if body:
            out["body"] = bytes(letter["body"]).decode(errors="replace")
        return out

    async def list(
        self,
        *,
        model: str | None = None,
        job: str | None = None,
        priority: bool = False,
        count: int = 10,
    ) -> dict[str, Any]:
        """The oldest `count` dead letters: id, source id, failures, request id, tenant hash."""
        if not 1 <= count <= MAX_DLQ_COUNT:
            raise ConfigError(f"count must be 1-{MAX_DLQ_COUNT}")
        queue, _ = await self._queue(model, job, priority)
        letters = await self._ctx.queues.dead_letters(queue, count)
        return {"queue": queue, "letters": [self._letter(x, body=False) for x in letters]}

    async def show(
        self,
        entry_id: str,
        *,
        model: str | None = None,
        job: str | None = None,
        priority: bool = False,
        unsafe: bool = False,
    ) -> dict[str, Any] | None:
        """One dead letter; its body (prompts, payloads) only with `unsafe`. None if gone."""
        queue, _ = await self._queue(model, job, priority)
        key = self._ctx.keys.queue(queue, "dlq")
        entries = await self._ctx.queues.client.xrange(key, entry_id, entry_id, count=1)
        if not entries:
            return None
        entry_key, fields = entries[0]
        row = {_text(k): v for k, v in (fields or {}).items()}
        letter = {
            "id": _text(entry_key),
            "src": _text(row.get("src", b"")),
            "fails": int(row.get("fails", 0)),
            "body": row.get("body", b""),
        }
        return {"queue": queue, **self._letter(letter, body=unsafe)}

    async def redrive(
        self,
        *,
        model: str | None = None,
        job: str | None = None,
        priority: bool = False,
        count: int = 1000,
        actor: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Send up to `count` dead letters back to their queue (a body-less one is dropped)."""
        if not 1 <= count <= MAX_DLQ_COUNT:
            raise ConfigError(f"count must be 1-{MAX_DLQ_COUNT}")
        queue, keep = await self._queue(model, job, priority)
        _, _, dead = await self._ctx.queues.depth(queue)
        if dry_run:
            return {"queue": queue, "moved": min(dead, count), "dead_letters": dead,
                    "dry_run": True}  # fmt: skip
        moved = await self._ctx.queues.redrive(queue, count, keep)
        await self._ctx.audit.write(
            actor=actor, op="dlq_redrive", path=f"dlq.{queue}", before={"dead_letters": dead},
            after={"moved": moved},
        )  # fmt: skip
        return {"queue": queue, "moved": moved, "dead_letters": dead - moved, "dry_run": False}

    async def purge(
        self,
        *,
        model: str | None = None,
        job: str | None = None,
        priority: bool = False,
        entry_ids: Sequence[str] | None = None,
        actor: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Delete every dead letter of a queue, or only `entry_ids`."""
        queue, _ = await self._queue(model, job, priority)
        client = self._ctx.queues.client
        key = self._ctx.keys.queue(queue, "dlq")
        if entry_ids is not None:
            ids = list(dict.fromkeys(entry_ids))
            if not 1 <= len(ids) <= MAX_DLQ_IDS:
                raise ConfigError(f"give 1-{MAX_DLQ_IDS} entry ids")
            found = [i for i in ids if await client.xrange(key, i, i, count=1)]
            if dry_run:
                return {"queue": queue, "deleted": len(found), "dry_run": True}
            deleted = int(await client.xdel(key, *found)) if found else 0
        else:
            _, _, dead = await self._ctx.queues.depth(queue)
            if dry_run:
                return {"queue": queue, "deleted": dead, "dry_run": True}
            await self._ctx.queues.purge_dead(queue)
            deleted = dead
        await self._ctx.audit.write(
            actor=actor, op="dlq_purge", path=f"dlq.{queue}", before=None,
            after={"deleted": deleted, "by_id": entry_ids is not None},
        )  # fmt: skip
        return {"queue": queue, "deleted": deleted, "dry_run": False}


class JobsAdmin:
    """Job names seen in this environment (send jobs with `llmq.send_job`)."""

    def __init__(self, ctx: AdminContext) -> None:
        """Over one environment's queue store."""
        self._ctx = ctx

    async def list(self) -> dict[str, Any]:
        """Per job name: waiting, held, dead letters, and whether a worker consumes it."""
        ctx = self._ctx
        names = await _job_names(ctx)
        now = await _now_secs(ctx)
        states = await asyncio.gather(*(_queue_state(ctx, ctx.job_queue(n), now) for n in names))
        return {
            "jobs": [
                {"job": name, "waiting": s["waiting"], "held": s["held"],
                 "dead_letters": s["dead_letters"], "consumer": s["consumer"]}
                for name, s in zip(names, states, strict=True)
            ]
        }  # fmt: skip


class PurgeAdmin:
    """Purges of removed models and deployments, tenants and queues (`PurgeService`)."""

    def __init__(self, ctx: AdminContext, requests: RequestsAdapter) -> None:
        """Over one environment's `PurgeService`."""
        self._ctx = ctx
        self._requests = requests

    async def model(
        self, model: str, *, force: bool = False, actor: str, dry_run: bool = True
    ) -> dict[str, Any]:
        """Delete every key of a removed model, its queues and doorbells (`force`: in use too)."""
        report = await self._ctx.purge.model(model, actor=actor, force=force, dry_run=dry_run)
        return report.as_dict()

    async def deployment(
        self,
        model: str,
        deployment: str,
        *,
        force: bool = False,
        wait: bool | None = None,
        wait_secs: float | None = None,
        actor: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Delete a removed deployment's state; waits for its running calls (`wait`)."""
        report = await self._ctx.purge.deployment(
            model, deployment, actor=actor, force=force, wait=wait, wait_secs=wait_secs,
            dry_run=dry_run,
        )  # fmt: skip
        return report.as_dict()

    async def tenant(
        self,
        tenant: str,
        *,
        remove: Sequence[str] | None = None,
        models: Sequence[str] | None = None,
        actor: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Delete a tenant's fairness state in every model (or `models`); maybe its calls."""
        report = await self._ctx.purge.tenant(
            tenant, actor=actor, remove=remove, models=models, dry_run=dry_run
        )
        return report.as_dict()

    async def queue(
        self,
        model: str,
        *,
        tenant: str | None = None,
        tenant_hash: str | None = None,
        actor: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Cancel a model's queued calls (of one tenant): needs the request service (T1)."""
        return await self._requests.purge_queue(
            model, tenant=tenant, tenant_hash=tenant_hash, actor=actor, dry_run=dry_run
        )

    async def removed(
        self,
        *,
        force: bool = False,
        wait: bool | None = None,
        wait_secs: float | None = None,
        actor: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Purge every model and deployment the store has state of and the registry has not."""
        report = await self._ctx.purge.removed(
            actor=actor, force=force, wait=wait, wait_secs=wait_secs, dry_run=dry_run
        )
        return report.as_dict()

    async def scheduled(self) -> dict[str, Any]:
        """Auto purges waiting for their grace: `{target, kind, due_at}`."""
        return {"scheduled": await self._ctx.purge.scheduled()}

    async def unschedule(self, target: str, *, actor: str, dry_run: bool = True) -> dict[str, Any]:
        """Drop one scheduled auto purge (`alias` or `alias/account/name`)."""
        if dry_run:
            listed = {row["target"] for row in await self._ctx.purge.scheduled()}
            return {"target": target, "cancelled": target in listed, "dry_run": True}
        cancelled = await self._ctx.purge.cancel_scheduled(target)
        if cancelled:
            await self._ctx.audit.write(
                actor=actor, op="purge_unschedule", path=f"purge.{target}", before="scheduled",
                after=None,
            )  # fmt: skip
        return {"target": target, "cancelled": cancelled, "dry_run": False}


class TenantsAdmin:
    """Tenants, by keyed hash only (names are never stored or listed)."""

    def __init__(self, ctx: AdminContext, requests: RequestsAdapter) -> None:
        """Over one environment's request index (T1) and id hash."""
        self._ctx = ctx
        self._requests = requests

    async def list(self, model: str) -> dict[str, Any]:
        """Tenants with calls in flight on `model`: hash, calls, next deadline."""
        return await self._requests.tenants(model)

    def hash(self, tenant: str) -> dict[str, Any]:
        """The keyed hash of `tenant` (`LLMQ_ID_HASH_SALT`), as admin views show it."""
        if not tenant:
            raise ConfigError("give a tenant name")
        return {"tenant_hash": self._ctx.hash_id(tenant)}
