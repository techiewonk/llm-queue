"""One audit writer and one actor check on every admin write (0.2.0 review B-06, B-16).

Only registry writes checked the actor; dead letters, purges, unschedule, drains, model drains
and request changes took any text (a newline could forge a second audit row or log line), and
three copies of the audit write drifted. Every writer now refuses a bad actor before it acts,
and every entry goes through `Audit.write` (one `llmq_admin` line, `kind` naming the service).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from llm_queue.engine import audit as audit_module
from llm_queue.engine.admin import AdminServices
from llm_queue.engine.admin.context import AdminContext
from llm_queue.engine.audit import Audit
from llm_queue.engine.keys import Keys
from llm_queue.engine.purge import PurgeService
from llm_queue.engine.requests_admin import RequestsAdmin
from llm_queue.errors import ConfigError
from llm_queue.registry.store import ACTOR_MAX_CHARS

BAD = ("", " ", "ann\nop=forged", "ann\x1b[2J", "x" * (ACTOR_MAX_CHARS + 1))


class _Untouchable(MagicMock):
    """A store client any call to which fails the test: a refused write touches nothing."""

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_") or name in ("register_script",):
            return super().__getattr__(name)
        raise AssertionError(f"the store was touched: {name}")


def _admin() -> AdminServices:
    client = _Untouchable()
    registry = MagicMock()
    registry.prefix = "llmq"
    registry.audit_key = "llmq:cfg:audit"
    requests = RequestsAdmin(client, keys=Keys("llmq"), registry=registry, hash_id=str)
    purge = PurgeService(client, registry)
    ctx = AdminContext(
        state=client, queue_client=None, keys=Keys("llmq"), registry=registry,
        queues=_Untouchable(), status=MagicMock(), purge=purge,
        audit=Audit(client, "llmq:cfg:audit"), hash_id=str, queue_prefix="",
        status_ttl_secs=60, requests=requests, process=MagicMock(),
    )  # fmt: skip
    return AdminServices(ctx)


Write = Callable[[AdminServices, str], Awaitable[Any]]
WRITES: dict[str, Write] = {
    "requests.cancel": lambda a, actor: a.requests.cancel(request_ids=["r"], actor=actor),
    "requests.reprioritize": lambda a, actor: a.requests.reprioritize(
        "m", priority=True, actor=actor
    ),
    "requests.extend": lambda a, actor: a.requests.extend("r", timeout_secs=5, actor=actor),
    "dlq.redrive": lambda a, actor: a.dlq.redrive(model="m", actor=actor),
    "dlq.purge": lambda a, actor: a.dlq.purge(model="m", actor=actor),
    "purge.model": lambda a, actor: a.purge.model("m", actor=actor),
    "purge.deployment": lambda a, actor: a.purge.deployment("m", "a/r", actor=actor),
    "purge.tenant": lambda a, actor: a.purge.tenant("t", actor=actor),
    "purge.removed": lambda a, actor: a.purge.removed(actor=actor),
    "purge.queue": lambda a, actor: a.purge.queue("m", actor=actor),
    "purge.unschedule": lambda a, actor: a.purge.unschedule("m", actor=actor),
    "models.drain": lambda a, actor: a.models.drain("m", actor=actor),
    "drain.start": lambda a, actor: a.drain.start(actor=actor),
    "drain.resume": lambda a, actor: a.drain.resume(actor=actor),
}


@pytest.mark.parametrize("op", sorted(WRITES))
@pytest.mark.parametrize("actor", BAD, ids=["empty", "blank", "newline", "escape", "long"])
async def test_every_write_refuses_a_bad_actor_before_it_acts(op: str, actor: str) -> None:
    admin = _admin()
    with pytest.raises(ConfigError, match="actor"):
        await WRITES[op](admin, actor)
    admin.context.process.pause_receiving.assert_not_called()


async def test_audit_write_checks_the_actor_and_logs_one_admin_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    client.xadd = AsyncMock()
    lines: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(audit_module.logger, "info", lambda e, **kw: lines.append((e, kw)))
    audit = Audit(client, "k")
    with pytest.raises(ConfigError, match="control characters"):
        await audit.write(kind="dlq", actor="a\nb", op="x", path="p", before=None, after=None)
    client.xadd.assert_not_awaited()
    longest = "a" * ACTOR_MAX_CHARS
    await audit.write(kind="dlq", actor=longest, op="x", path="p", before=None, after={"n": 1})
    client.xadd.assert_awaited_once()
    assert lines == [
        ("llmq_admin", {"kind": "dlq", "actor": longest, "op": "x", "path": "p", "after": {"n": 1}})
    ]


def test_one_actor_limit() -> None:
    assert ACTOR_MAX_CHARS == 128
