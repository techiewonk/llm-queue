"""0.2.0 `install()` on a real store (plan H2, B4): a queue built from the environment starts
around the app, its probes answer under the prefix, the drain file watcher runs once, and a
queued call carries the middleware's tenant. No real LLM is called.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis.asyncio
from aiohttp.test_utils import TestServer
from engine_testkit import CHAT, FakeEngine, Scenario, create_app
from engine_testkit import registry as registry_doc
from fastapi import FastAPI

from llm_queue import LLMQueue, LLMQueueError, current_context
from llm_queue.fastapi import install

pytestmark = pytest.mark.integration


@pytest.fixture
async def env(store: redis.asyncio.Redis, prefix: str) -> AsyncIterator[dict[str, str]]:
    """`LLMQ_*` settings on this test's key prefix and its own fakellm."""
    server = TestServer(create_app(engine=FakeEngine(Scenario.from_mapping({}))), host="127.0.0.1")
    await server.start_server()
    port = store.connection_pool.connection_kwargs["port"]
    key_prefix = prefix.replace(":", "-")
    yield {
        "LLMQ_STORE_URL": f"redis://:localdev@127.0.0.1:{port}/0",
        "LLMQ_ENVIRONMENT": "test",
        "LLMQ_KEY_PREFIX": key_prefix,
        "LLMQ_REGISTRY": json.dumps(registry_doc()),
        "LLMQ_BEDROCK_ENDPOINT_URL": str(server.make_url("")).rstrip("/"),
        "LLMQ_METRICS_SINK": "none",
    }
    await server.close()
    keys = [k async for k in store.scan_iter(match=f"{key_prefix}:*", count=500)]
    if keys:
        await store.delete(*keys)


def _queue(env: dict[str, str], **extra: str) -> LLMQueue:
    return LLMQueue.from_env(
        {**env, **extra},
        credentials=lambda account: {
            "aws_access_key_id": account.name,
            "aws_secret_access_key": "x",
        },
    )


def _ready(llmq: LLMQueue) -> bool:
    return llmq.ready  # read through a call: mypy would narrow the property after an assert


async def test_install_runs_the_queue_around_the_app(env: dict[str, str], tmp_path: Path) -> None:
    llmq = _queue(env, LLMQ_DRAIN_FILE=str(tmp_path / "drain"), LLMQ_DRAIN_POLL_SECS="0.1")
    seen: list[Any] = []

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        seen.append(("app_start", llmq.ready))  # the queue is already up
        yield
        seen.append(("app_stop", llmq.started))  # and still up while the app shuts down

    app = FastAPI(lifespan=lifespan)

    @app.post("/ask")
    async def ask() -> dict[str, Any]:
        reply = await llmq.call("small", CHAT)
        return {"ok": reply.ok, "tenant": current_context().tenant_id}

    install(app, llmq, admin_token="s3cret", context={"tenant_id": "acme"})
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://app") as client,
    ):
        assert llmq.ready is True
        watcher = llmq._drain_watcher
        assert watcher is not None
        assert watcher.running  # started by LLMQueue.start(), once
        ready = await client.get("/internal/llm-queue/health/ready")
        assert ready.status_code == 200
        status = await client.get("/internal/llm-queue/status")
        assert status.status_code == 403  # the token guards everything but the probes
        assert (await client.post("/ask")).json() == {"ok": True, "tenant": "acme"}
        drain = await client.post(
            "/internal/llm-queue/drain", headers={"X-LLMQ-Token": "s3cret", "X-LLMQ-Admin": "1"}
        )
        assert drain.status_code == 200  # drain routes mounted: a token guards them
        assert _ready(llmq) is False
    assert seen == [("app_start", True), ("app_stop", True)]
    assert llmq.started is False
    assert _ready(llmq) is False


async def test_a_failed_start_with_on_start_error_log_keeps_the_app_up(
    env: dict[str, str], caplog: pytest.LogCaptureFixture
) -> None:
    """A registry the start refuses: the app serves, calls fail at once, readiness is false."""
    llmq = _queue(env, LLMQ_REGISTRY='{"registry_version": 1, "models": {"x": {}}}')
    app = FastAPI()
    install(app, llmq, on_start_error="log")
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://app") as client,
    ):
        assert llmq.started is False
        assert llmq.ready is False
        assert (await client.get("/internal/llm-queue/health/ready")).status_code == 503
        with pytest.raises(LLMQueueError, match="stopped"):
            await llmq.call("small", CHAT)
    assert any("llmq_start_failed" in r.getMessage() for r in caplog.records)
