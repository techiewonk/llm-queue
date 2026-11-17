"""Admin HTTP input limits (review B-07, B-08): dead letter ids, store refusals, body size.

A fake `AdminServices` stands in for the services (`admin_testkit.FakeAdmin`).
"""

from __future__ import annotations

from typing import Any

import pytest
from admin_testkit import FakeAdmin, allow, client
from redis.exceptions import ResponseError

from llm_queue.engine.admin.queues import DlqAdmin
from llm_queue.engine.http import ADMIN_HEADER, MAX_VALUE_BYTES, error_response
from llm_queue.engine.settings import InfraSettings
from llm_queue.errors import ConfigError

ADMIN = {ADMIN_HEADER: "1"}


def _writes(admin: FakeAdmin, **kw: Any) -> Any:
    return client(admin, dependencies=[allow], allow_writes=True, actor=lambda r: "ann", **kw)


async def test_b07_a_dead_letter_id_that_is_not_a_stream_id_is_refused() -> None:
    """B-07: `GET /dlq/abc` reached XRANGE, whose `ResponseError` answered 500."""
    dlq = object.__new__(DlqAdmin)  # the check comes before any store read
    with pytest.raises(ConfigError, match="dead letter id"):
        await dlq.show("abc", model="m")
    with pytest.raises(ConfigError, match="dead letter id"):
        await dlq.purge(model="m", entry_ids=["1-0", "1-x"], actor="ann")


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (ResponseError("ERR Invalid stream ID specified as stream command argument"), 400),
        (ResponseError("BUSY Redis is busy running a script."), 503),
        (ResponseError("NOREPLICAS Not enough good replicas to write."), 503),
    ],
)
def test_b07_a_store_refusal_maps_to_a_status_code(error: Exception, code: int) -> None:
    """B-07: a raw redis `ResponseError` was re-raised by `error_response` (a 500)."""
    assert error_response(error)[0] == code
    reply = _writes(FakeAdmin(error)).get("/dlq/1-0", params={"model": "m"})
    assert reply.status_code == code


def test_b08_a_body_above_the_cap_is_413() -> None:
    """B-08: POST bodies were read whole with no cap (`LLMQ_ADMIN_BODY_MAX_BYTES`)."""
    admin = FakeAdmin()
    http = _writes(admin, body_max_bytes=2048)
    small = {"values": {"rpm": 5}, "model": "m"}
    assert http.post("/registry", json=small, headers=ADMIN).status_code == 200
    big = {"values": {"x": "y" * 4096}, "model": "m"}
    assert http.post("/registry", json=big, headers=ADMIN).status_code == 413
    assert admin.calls[-1][0] == "registry.set"  # only the small body reached the service
    assert len(admin.calls) == 1


def test_b08_a_body_value_above_its_cap_is_400() -> None:
    """B-08: only dict keys were bounded; a `values` entry could be any size."""
    admin = FakeAdmin()
    huge = {"values": {"models.m.tags": {"note": "y" * (MAX_VALUE_BYTES + 1)}}, "model": "m"}
    assert _writes(admin).post("/registry", json=huge, headers=ADMIN).status_code == 400
    assert admin.calls == []


def test_b08_the_cap_is_a_setting() -> None:
    """`LLMQ_ADMIN_BODY_MAX_BYTES`: 1 MiB by default, bounded."""
    env = {"LLMQ_STORE_URL": "redis://localhost:6379/0", "LLMQ_ENVIRONMENT": "dev"}
    assert InfraSettings.from_env(env).admin_body_max_bytes == 1_048_576
    set_ = InfraSettings.from_env({**env, "LLMQ_ADMIN_BODY_MAX_BYTES": "4096"})
    assert set_.admin_body_max_bytes == 4096
    with pytest.raises(ConfigError, match="ADMIN_BODY_MAX_BYTES"):
        InfraSettings.from_env({**env, "LLMQ_ADMIN_BODY_MAX_BYTES": "10"})
