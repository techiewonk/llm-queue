"""0.2.0 core: worker id, default tenant, job settings from secrets, admin token, describe,
and a context reset in another context.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import pytest

from llm_queue import LLMQueue, request_context
from llm_queue.core import _default_worker_id
from llm_queue.engine.settings import InfraSettings, Layers

DEV = {
    "LLMQ_STORE_URL": "redis://localhost:6379/0",
    "LLMQ_ENVIRONMENT": "dev",
    "LLMQ_REGISTRY": '{"registry_version": 1, "models": {}}',
}


def test_new_settings_defaults_and_validation() -> None:
    settings = InfraSettings.from_env(DEV)
    assert (settings.worker_id, settings.default_tenant, settings.admin_header) == (
        "",
        None,
        "X-LLMQ-Token",
    )
    set_ = InfraSettings.from_env(
        {**DEV, "LLMQ_WORKER_ID": "api-1:7", "LLMQ_DEFAULT_TENANT": "acme",
         "LLMQ_ADMIN_HEADER": "X-Admin-Token"}
    )  # fmt: skip
    assert (set_.worker_id, set_.default_tenant, set_.admin_header) == (
        "api-1:7",
        "acme",
        "X-Admin-Token",
    )
    for name, value in (("LLMQ_WORKER_ID", "a b"), ("LLMQ_ADMIN_HEADER", "bad header")):
        with pytest.raises(Exception, match=name):
            InfraSettings.from_env({**DEV, name: value})


def test_default_worker_id_is_pod_and_pid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POD_NAME", "api-7f9c")
    assert _default_worker_id() == f"api-7f9c:{os.getpid()}"


def test_layers_mapping_reads_secrets_before_env() -> None:
    def secret(name: str) -> str | None:
        return "99" if name == "LLMQ_JOB_DEDUPE_SECS" else None

    merged = Layers({"LLMQ_JOB_DEDUPE_SECS": "10", "OTHER": "x"}, secret)
    assert merged.get("LLMQ_JOB_DEDUPE_SECS") == "99"
    assert merged.get("OTHER") == "x"
    assert merged.get("MISSING") is None


def test_from_env_worker_id_tenant_admin_and_describe(monkeypatch: pytest.MonkeyPatch) -> None:
    secrets = {"LLMQ_ADMIN_TOKEN": "s3cret", "LLMQ_ADMIN_READ_TOKEN": "r3ad"}
    llmq = LLMQueue.from_env(
        {**DEV, "LLMQ_WORKER_ID": "pod-a:1", "LLMQ_DEFAULT_TENANT": "acme"},
        secrets=secrets.get,
    )
    assert llmq._engine.worker_id.startswith("pod-a:1-")
    assert llmq._call_kwargs(None)["tenant_id"] == "acme"
    with request_context(tenant_id="other"):
        assert llmq._call_kwargs(None)["tenant_id"] == "other"
    assert llmq._admin_token() == "s3cret"
    secrets["LLMQ_ADMIN_TOKEN"] = "rotated"  # read on every request
    assert llmq._admin_token() == "rotated"
    assert llmq._admin_read_token() == "r3ad"
    described = llmq.describe()
    text = repr(described)
    assert described["store"] == "redis://localhost:6379"
    assert described["default_tenant"] == "acme"
    assert "rotated" not in text
    assert "s3cret" not in text
    assert "r3ad" not in text
    assert described == llmq.admin.describe()


def test_admin_token_guards_routes_but_not_probes() -> None:
    pytest.importorskip("fastapi")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    token = {"value": "t1"}
    llmq = LLMQueue.from_env(DEV)
    app = FastAPI()
    app.include_router(llmq.http_router(admin_token=lambda: token["value"]))
    client = TestClient(app)
    assert client.get("/status").status_code == 403
    assert client.get("/status", headers={"X-LLMQ-Token": "wrong"}).status_code == 403
    assert client.get("/health/live").status_code != 403  # probes stay open
    token["value"] = ""  # an empty token refuses everything (fail closed)
    assert client.get("/status", headers={"X-LLMQ-Token": ""}).status_code == 403


def test_env_tokens_and_settings_reach_the_router() -> None:
    """Q4: the read token reads, the admin token writes; LLMQ_ADMIN_* settings are defaults."""
    pytest.importorskip("fastapi")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    secrets = {"LLMQ_ADMIN_TOKEN": "w", "LLMQ_ADMIN_READ_TOKEN": "r"}
    env = {**DEV, "LLMQ_ADMIN_WRITES_ENABLED": "true", "LLMQ_ADMIN_HEADER": "X-Admin-Token"}
    llmq = LLMQueue.from_env(env, secrets=secrets.get)
    app = FastAPI()
    app.include_router(llmq.http_router())
    client = TestClient(app)
    # Not loaded: the read passes the guard and answers 503, not 403.
    assert client.get("/registry", headers={"X-Admin-Token": "r"}).status_code == 503
    assert client.get("/registry", headers={"X-LLMQ-Token": "r"}).status_code == 403
    write = client.post("/blocks", json={"region": "r1"}, headers={"X-Admin-Token": "r"})
    assert write.status_code == 403  # the read token cannot write
    write = client.post("/blocks", json={"region": "r1"}, headers={"X-Admin-Token": "w"})
    assert write.status_code == 503  # writes are mounted (LLMQ_ADMIN_WRITES_ENABLED)


async def test_a_context_reset_in_another_context_is_tolerated() -> None:
    """An async generator closed by another task raised ValueError from the reset (0.1.1)."""

    async def stream() -> Any:
        with request_context(tenant_id="inside"):
            yield 1
            yield 2

    gen = stream()
    assert await gen.__anext__() == 1
    # The generator's `finally` now runs in another task's context: before 0.2.0 the
    # ContextVar reset raised "created in a different Context" out of aclose().
    await asyncio.create_task(gen.aclose())


def test_registry_conflict_is_retryable() -> None:
    """FU6: running the same publish or edit again can succeed once other writes are done."""
    from llm_queue.registry.store import RegistryConflict

    error = RegistryConflict("lost the race")
    assert error.category == "config_error"
    assert error.retryable
