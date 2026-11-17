"""`llmq.admin.requests` before the requests service lands (design T2).

The adapter (`engine/admin/requests.py`) refuses the index-backed operations with a clear
`AdminRefused`; the skipped tests below pin the behaviour to switch on when
`feat/020-requests-engine` is wired into `_service()`.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from llm_queue.engine.admin import AdminContext, AdminServices
from llm_queue.engine.audit import Audit
from llm_queue.engine.keys import Keys
from llm_queue.errors import AdminRefused

NEEDS_SERVICE = pytest.mark.skip(
    reason="needs the per-model requests service (feat/020-requests-engine, design T2)"
)


def _admin() -> AdminServices:
    client = MagicMock()
    ctx = AdminContext(
        state=client, queue_client=None, keys=Keys("llmq"), registry=MagicMock(),
        queues=MagicMock(), status=MagicMock(), purge=MagicMock(),
        audit=Audit(client, "llmq:cfg:audit"), hash_id=lambda v: v, queue_prefix="",
        status_ttl_secs=60,
    )  # fmt: skip
    return AdminServices(ctx)


@pytest.mark.parametrize(
    ("call", "what"),
    [
        (lambda a: a.requests.list("m"), "requests list"),
        (lambda a: a.requests.count("m"), "requests count"),
        (lambda a: a.requests.reprioritize("m", priority=True, actor="x"), "requests reprioritize"),
        (lambda a: a.requests.extend("r", timeout_secs=5, actor="x"), "requests extend"),
        (lambda a: a.requests.cancel("m", tenant="t", actor="x"), "cancel by model"),
        (lambda a: a.tenants.list("m"), "tenants list"),
        (lambda a: a.purge.queue("m", actor="x"), "purge queue"),
    ],
)
async def test_index_operations_refuse_until_the_service_lands(call: Any, what: str) -> None:
    with pytest.raises(AdminRefused, match=f"requests service not available: {what}"):
        await call(_admin())


@NEEDS_SERVICE
async def test_list_pages_by_tenant_and_state() -> None:
    """`requests.list(model, tenant=, state=)` returns `RequestPage.as_dict()` with `next`."""


@NEEDS_SERVICE
async def test_count_by_state_and_stage() -> None:
    """`requests.count(model, tenant=)` returns `RequestCounts.as_dict()`."""


@NEEDS_SERVICE
async def test_cancel_by_model_tenant_and_state_is_dry_by_default() -> None:
    """`requests.cancel(model, tenant=, state="waiting")`: a report first, then the cancel."""


@NEEDS_SERVICE
async def test_reprioritize_moves_only_that_tenants_waiting_calls() -> None:
    """`requests.reprioritize(model, priority=True, tenant=)` returns a `ChangeReport`."""


@NEEDS_SERVICE
async def test_extend_gives_one_call_more_time() -> None:
    """`requests.extend(id, timeout_secs=)` (owner D6: in 0.2.0)."""


@NEEDS_SERVICE
async def test_tenants_list_and_purge_queue_use_the_index() -> None:
    """`tenants.list(model)` and `purge.queue(model, tenant=)` read the T1 index."""
