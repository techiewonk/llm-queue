"""`llmq.admin.requests` / `tenants` / `purge.queue` / `status` over `RequestsAdmin` (design T2).

The adapter (`engine/admin/requests.py`) hands every call to the request index service and
returns its report as a dict. A recording fake stands in for the service here; the service
itself runs against real stores in `tests/integration/engine/test_requests_admin.py`.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from llm_queue.engine.admin import AdminContext, AdminServices
from llm_queue.engine.audit import Audit
from llm_queue.engine.keys import Keys
from llm_queue.engine.requests_admin import (
    AdminSettings,
    ChangeReport,
    RequestCounts,
    RequestPage,
    RequestsAdmin,
    RequestSummary,
)
from llm_queue.errors import AdminRefused, ConfigError

H = "0123456789abcdef"


class _Service:
    """Records `(method, args, kwargs)`; answers the dataclasses `RequestsAdmin` returns."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def pick_tenant(self, tenant: str | None, tenant_hash: str | None) -> str | None:
        return H if tenant is not None else tenant_hash

    def _record(self, name: str, *args: Any, **kw: Any) -> None:
        self.calls.append((name, args, kw))

    async def list(self, *args: Any, **kw: Any) -> RequestPage:
        self._record("list", *args, **kw)
        item = RequestSummary(
            request_id="r1", model="m", tenant=H, state="waiting", stage="queued",
            priority=False, age_secs=1.0, deadline_in_secs=9.0,
        )  # fmt: skip
        return RequestPage([item], "9:r1", 1)

    async def count(self, *args: Any, **kw: Any) -> RequestCounts:
        self._record("count", *args, **kw)
        return RequestCounts(
            model="m", tenant=H, total=3, by_state={"waiting": 2, "running": 1},
            by_stage={"queued": 2, "running": 1}, cancelling=0, oldest_wait_secs=1.5,
            truncated=False,
        )  # fmt: skip

    async def _change(self, name: str, *args: Any, **kw: Any) -> ChangeReport:
        self._record(name, *args, **kw)
        op = kw.get("op", f"requests_{name}")
        return ChangeReport(
            op=op, model="m", tenant=H, dry_run=kw["dry_run"], actor=kw["actor"], matched=2
        )

    async def cancel(self, *args: Any, **kw: Any) -> ChangeReport:
        return await self._change("cancel", *args, **kw)

    async def reprioritize(self, *args: Any, **kw: Any) -> ChangeReport:
        return await self._change("reprioritize", *args, **kw)

    async def extend(self, *args: Any, **kw: Any) -> ChangeReport:
        return await self._change("extend", *args, **kw)

    async def tenants(self, model: str) -> dict[str, Any]:
        self._record("tenants", model)
        return {"model": model, "tenants": [{"tenant": H, "calls": 2}], "truncated": False}


def _admin(service: Any = None, status: Any = None) -> AdminServices:
    client = MagicMock()
    ctx = AdminContext(
        state=client, queue_client=None, keys=Keys("llmq"), registry=MagicMock(),
        queues=MagicMock(), status=status or MagicMock(), purge=MagicMock(),
        audit=Audit(client, "llmq:cfg:audit"), hash_id=lambda v: v, queue_prefix="",
        status_ttl_secs=60, requests=service,
    )  # fmt: skip
    return AdminServices(ctx)


async def test_list_pages_by_tenant_and_state() -> None:
    """`requests.list(model, tenant=, state=)` returns `RequestPage.as_dict()` with `next`."""
    svc = _Service()
    page = await _admin(svc).requests.list("m", tenant="acme", state="waiting", limit=5)
    assert page["next"] == "9:r1"
    assert page["items"][0]["request_id"] == "r1"
    assert svc.calls == [
        ("list", ("m",), {"tenant": "acme", "tenant_hash": None, "state": "waiting",
                          "limit": 5, "after": None}),
    ]  # fmt: skip


async def test_count_by_state_and_stage() -> None:
    """`requests.count(model, tenant=)` returns `RequestCounts.as_dict()`."""
    svc = _Service()
    counts = await _admin(svc).requests.count("m", tenant_hash=H)
    assert (counts["total"], counts["by_state"]) == (3, {"waiting": 2, "running": 1})
    assert svc.calls[0][2] == {"tenant": None, "tenant_hash": H}


async def test_cancel_by_model_tenant_and_state_is_dry_by_default() -> None:
    """`requests.cancel(model, tenant=, state="waiting")`: a report first, then the cancel."""
    svc = _Service()
    admin = _admin(svc)
    dry = await admin.requests.cancel("m", tenant="acme", actor="ann")
    assert (dry["dry_run"], dry["op"], dry["matched"]) == (True, "requests_cancel", 2)
    done = await admin.requests.cancel("m", tenant="acme", state="all", actor="ann", dry_run=False)
    assert done["dry_run"] is False
    assert [c[2]["state"] for c in svc.calls] == ["waiting", "all"]
    by_ids = await admin.requests.cancel(request_ids=["a", "b"], actor="ann")
    assert by_ids["dry_run"] is True
    assert svc.calls[-1][2]["request_ids"] == ["a", "b"]


async def test_reprioritize_moves_only_that_tenants_waiting_calls() -> None:
    """`requests.reprioritize(model, priority=True, tenant=)` returns a `ChangeReport`."""
    svc = _Service()
    report = await _admin(svc).requests.reprioritize(
        "m", priority=True, tenant="acme", limit=10, actor="ann"
    )
    assert (report["op"], report["dry_run"]) == ("requests_reprioritize", True)
    assert svc.calls[0][2] == {
        "priority": True, "tenant": "acme", "tenant_hash": None, "limit": 10, "actor": "ann",
        "dry_run": True,
    }  # fmt: skip


async def test_extend_gives_one_call_more_time() -> None:
    """`requests.extend(id, timeout_secs=)` (owner D6: in 0.2.0)."""
    svc = _Service()
    report = await _admin(svc).requests.extend("r1", timeout_secs=30, actor="ann", dry_run=False)
    assert (report["op"], report["dry_run"]) == ("requests_extend", False)
    assert svc.calls == [
        ("extend", ("r1",), {"timeout_secs": 30, "actor": "ann", "dry_run": False})
    ]


async def test_tenants_list_and_purge_queue_use_the_index() -> None:
    """`tenants.list(model)` and `purge.queue(model, tenant=)` read the T1 index."""
    svc = _Service()
    admin = _admin(svc)
    listed = await admin.tenants.list("m")
    assert listed["tenants"] == [{"tenant": H, "calls": 2}]
    report = await admin.purge.queue("m", tenant="acme", actor="ann")
    assert (report["op"], report["dry_run"]) == ("purge_queue", True)
    name, args, kw = svc.calls[-1]
    assert (name, args, kw["state"], kw["op"]) == ("cancel", ("m",), "waiting", "purge_queue")


async def test_status_of_one_tenant_on_a_model() -> None:
    """`status.get(model, tenant=)` hashes the name and asks for that tenant's block."""
    status = MagicMock()
    status.model = AsyncMock(return_value={"model": "m", "tenant": {"tenant": H}})
    admin = _admin(_Service(), status)
    admin.context.registry.current.registry.alias_of.return_value = "m"
    admin.context.engine = MagicMock()  # view() reads the loaded registry
    report = await admin.status.get("m", tenant="acme")
    assert report["tenant"] == {"tenant": H}
    assert status.model.await_args.kwargs == {"tenant_hash": H}
    with pytest.raises(ConfigError, match="needs a model"):
        await admin.status.get(tenant="acme")


def _off() -> AdminServices:
    registry = MagicMock()
    service = RequestsAdmin(
        MagicMock(), keys=Keys("llmq"), registry=registry, hash_id=lambda v: v,
        settings=AdminSettings(request_index=False),
    )  # fmt: skip
    return _admin(service)


@pytest.mark.parametrize(
    "call",
    [
        lambda a: a.requests.list("m"),
        lambda a: a.requests.cancel("m", tenant="t", actor="x"),
        lambda a: a.tenants.list("m"),
        lambda a: a.purge.queue("m", actor="x"),
    ],
)
async def test_index_off_refuses_lookups_by_model_and_tenant(call: Any) -> None:
    with pytest.raises(AdminRefused, match="LLMQ_REQUESTS_INDEX_ENABLED"):
        await call(_off())
