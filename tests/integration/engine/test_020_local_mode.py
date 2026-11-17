"""0.2.0 local execution mode (`LLMQ_EXECUTION_MODE=local`) against fakellm, with no store.

Every queue here is built with an `LLMQ_STORE_URL` that nothing listens on, and the store
client factories are replaced by ones that fail the test: a call that reached for the store
would fail. fakellm runs in-process (aiohttp), one per test, so each scenario is its own.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from aiohttp.test_utils import TestServer
from engine_testkit import CHAT, GPT, SMALL, FakeEngine, Scenario, create_app, stats
from engine_testkit import registry as registry_doc
from fastapi import FastAPI

from llm_queue import CallFailed, LLMQueue, LocalModeRefused, request_context
from llm_queue.fastapi import install

pytestmark = pytest.mark.integration

# Nothing listens on port 1: a client that connected there would fail at once.
BOGUS_STORE = "redis://:nope@127.0.0.1:1/0"


@pytest.fixture(autouse=True)
def no_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail the test if anything builds a store client."""

    def refused(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("local mode built a store client")

    monkeypatch.setattr("llm_queue.core.secret_store_client", refused)
    monkeypatch.setattr("llm_queue.engine.store_client.store_client", refused)
    return


class Rig:
    """A fakellm server and the environment of a local-mode queue in front of it."""

    def __init__(self, server: TestServer, fake: FakeEngine) -> None:
        self.server = server
        self.fake = fake
        self.queues: list[LLMQueue] = []

    def env(self, models: dict[str, Any] | None = None, **extra: str) -> dict[str, str]:
        return {
            "LLMQ_EXECUTION_MODE": "local",
            "LLMQ_STORE_URL": BOGUS_STORE,
            "LLMQ_ENVIRONMENT": "test",
            "LLMQ_REGISTRY": json.dumps(registry_doc(models)),
            "LLMQ_BEDROCK_ENDPOINT_URL": str(self.server.make_url("")).rstrip("/"),
            "LLMQ_METRICS_SINK": "none",
            **extra,
        }

    def queue(self, models: dict[str, Any] | None = None, **extra: str) -> LLMQueue:
        llmq = LLMQueue.from_env(
            self.env(models, **extra),
            credentials=lambda account: {
                "aws_access_key_id": account.name,
                "aws_secret_access_key": "x",
            },
        )
        self.queues.append(llmq)
        return llmq

    async def started(self, models: dict[str, Any] | None = None, **extra: str) -> LLMQueue:
        llmq = self.queue(models, **extra)
        await llmq.start()
        return llmq


async def _rig(scenario: dict[str, Any] | None = None) -> AsyncIterator[Rig]:
    fake = FakeEngine(Scenario.from_mapping(scenario or {}))
    server = TestServer(create_app(engine=fake), host="127.0.0.1")
    await server.start_server()
    rig = Rig(server, fake)
    try:
        yield rig
    finally:
        for llmq in rig.queues:
            if llmq.started:
                await llmq.stop(drain_secs=1)
            else:
                await asyncio.to_thread(llmq.close_sync)
        await server.close()


@pytest.fixture
async def rig() -> AsyncIterator[Rig]:
    async for value in _rig():
        yield value


@pytest.fixture
async def slow_rig() -> AsyncIterator[Rig]:
    """Every call takes about 400 ms."""
    async for value in _rig({"defaults": {"latency_ms": {"p50": 400, "p95": 400}}}):
        yield value


# ---------------------------------------------------------------- the public calls


async def test_converse_and_chat_run_in_process_without_a_store(rig: Rig) -> None:
    llmq = await rig.started()
    assert llmq.execution_mode == "local"
    reply = await llmq.converse("big", CHAT)
    assert reply.ok
    assert reply.meta.served_model == "big"
    assert reply.meta.deployment == "aws-a/r1"
    assert reply.meta.attempts == 1
    assert reply.meta.provider_request_id  # the provider's own id comes through
    chat = await llmq.chat("small", CHAT, request_id="mine-1")
    assert chat.ok
    assert chat.request_id == "mine-1"
    assert stats(rig.fake, f"{GPT}@r1")["ok"] == 1
    assert stats(rig.fake, f"{SMALL}@r1")["ok"] == 1


async def test_streams_yield_events_and_the_meta_event(rig: Rig) -> None:
    llmq = await rig.started()
    events = [e async for e in llmq.converse_stream("big", CHAT, meta_event=True)]
    assert len(events) > 2
    assert any("contentBlockDelta" in e for e in events)
    last = events[-1]["llmq"]
    assert last["meta"]["deployment"] == "aws-a/r1"
    assert last["usage"]["outputTokens"]
    chunks = [e async for e in llmq.chat_stream("small", CHAT)]
    assert any("contentBlockDelta" in e for e in chunks)


async def test_a_stream_of_an_embedding_model_is_refused(rig: Rig) -> None:
    llmq = await rig.started()
    with pytest.raises(CallFailed) as info:
        [e async for e in llmq.converse_stream("titan", CHAT)]
    assert info.value.kind == "invalid_request"


async def test_embed_batches_and_one_text_async_and_sync(rig: Rig) -> None:
    llmq = await rig.started()
    result = await llmq.embed("titan", ["a", "b", "c"])  # embed_batch_size 1: three calls
    assert len(result.vectors) == 3
    assert {len(v) for v in result.vectors} == {256}
    assert result.batches == 3
    cohere = await llmq.embed("cohere", ["a", "b", "c"])  # one batch of 96
    assert cohere.batches == 1
    assert len(cohere.vectors) == 3
    one = (await llmq.embed("titan", ["only"])).vectors[0]  # embed_one's path
    assert len(one) == 256
    sync = await asyncio.to_thread(llmq.embed_sync, "titan", ["s"])  # on the started loop
    assert len(sync.vectors[0]) == 256


async def test_sync_calls_without_start_use_a_private_local_engine(rig: Rig) -> None:
    """Not started: the sync calls build a private local engine (from a worker thread here)."""
    llmq = rig.queue()
    reply = await asyncio.to_thread(llmq.converse_sync, "small", CHAT)
    assert reply.ok
    assert reply.meta.deployment == "aws-a/r1"
    vector = (await asyncio.to_thread(llmq.embed_sync, "titan", ["x"])).vectors[0]
    assert len(vector) == 256


async def test_langchain_adapters_run_locally(rig: Rig) -> None:
    pytest.importorskip("langchain_aws")
    from llm_queue.langchain import QueuedBedrockEmbeddings, QueuedChatBedrockConverse

    llmq = await rig.started()
    model = QueuedChatBedrockConverse(model_alias="big", llmq=llmq)  # type: ignore[call-arg]
    message = await model.ainvoke("hello")
    assert str(message.content).startswith("fakellm")
    assert message.response_metadata["llmq"]["deployment"] == "aws-a/r1"
    streaming = QueuedChatBedrockConverse(model_alias="big", llmq=llmq, queue_streaming=True)  # type: ignore[call-arg]
    chunks = [chunk async for chunk in streaming.astream("hello")]
    assert len(chunks) > 2
    emb = QueuedBedrockEmbeddings(
        model_alias="cohere", model_id="cohere.embed-english-v3", llmq=llmq
    )
    assert len(await emb.aembed_documents(["a", "b"])) == 2


# ---------------------------------------------------------------- budgets and slots


async def test_rpm_is_divided_by_the_process_count(rig: Rig) -> None:
    """rpm 4 over 2 processes: 2 a minute here; smooth burst 1, so a second call waits 30 s."""
    llmq = await rig.started(
        {"small": {"rpm": 4, "rate_window": "smooth"}}, LLMQ_LOCAL_PROCESS_COUNT="2"
    )
    assert (await llmq.converse("small", CHAT)).ok
    started = time.monotonic()
    with pytest.raises(CallFailed) as info:
        await llmq.converse("small", CHAT, max_wait_secs=0.5)
    assert info.value.kind == "rate_limited"
    assert info.value.retry_after_secs is not None
    assert info.value.retry_after_secs > 20
    assert time.monotonic() - started < 3
    status = await llmq.status("small")
    dep = status["deployments"][0]
    assert dep["rpm"]["limit"] == 2.0
    assert dep["rpm"]["used"] == 1
    assert dep["rpm"]["left"] == 0
    assert stats(rig.fake, f"{SMALL}@r1")["ok"] == 1  # the refused call never reached it


async def test_the_concurrency_cap_holds_calls_and_priority_goes_first(slow_rig: Rig) -> None:
    llmq = await slow_rig.started(LLMQ_LOCAL_CONCURRENCY="1")
    order: list[str] = []

    async def one(name: str, priority: bool | None) -> None:
        await llmq.converse("small", CHAT, priority=priority)
        order.append(name)

    first = asyncio.create_task(one("first", False))
    await asyncio.sleep(0.1)
    normal = asyncio.create_task(one("normal", False))
    await asyncio.sleep(0.05)
    with request_context(priority=True):  # the task copies the context
        urgent = asyncio.create_task(one("urgent", None))
    await asyncio.sleep(0.05)
    status = await llmq.status("small")
    assert status["in_progress"] == 1
    assert status["waiting"] == {"priority": 1, "normal": 1}
    await asyncio.gather(first, normal, urgent)
    assert order == ["first", "urgent", "normal"]


async def test_concurrency_status_counts_calls_in_flight(slow_rig: Rig) -> None:
    llmq = await slow_rig.started(LLMQ_LOCAL_CONCURRENCY="2")
    calls = [asyncio.create_task(llmq.converse("small", CHAT)) for _ in range(3)]
    await asyncio.sleep(0.15)
    status = await llmq.status("small")
    assert status["concurrency"] == {"limit": 2, "in_use": 2}
    assert status["deployments"][0]["concurrency"]["in_use"] == 2
    assert all(r.ok for r in await asyncio.gather(*calls))


# ---------------------------------------------------------------- retries and fallbacks


async def _scenario(rig: Rig, models: dict[str, dict[str, Any]]) -> None:
    for key, patch in models.items():
        rig.fake.scenario.patch_model(key, patch)


async def test_a_throttle_moves_to_another_deployment_and_backs_off(rig: Rig) -> None:
    await _scenario(rig, {f"{GPT}@r1": {"force_error": "ThrottlingException"}})
    llmq = await rig.started()
    reply = await llmq.converse("big", CHAT)
    assert reply.meta.deployment == "aws-a/r2"
    assert reply.meta.attempts == 2
    r1 = (await llmq.status("big"))["deployments"][0]
    assert r1["throttles_this_minute"] == 1
    again = await llmq.converse("big", CHAT)
    assert again.meta.attempts == 1
    assert again.meta.deployment == "aws-a/r2"


async def test_throttles_on_the_only_deployment_end_rate_limited(rig: Rig) -> None:
    await _scenario(rig, {f"{SMALL}@r1": {"force_error": "ThrottlingException"}})
    llmq = await rig.started({"small": {"throttle_retry_attempts": 1}})
    with pytest.raises(CallFailed) as info:
        await llmq.converse("small", CHAT, timeout_secs=8)
    assert info.value.kind == "rate_limited"
    assert stats(rig.fake, f"{SMALL}@r1")["calls"] == 2  # tried, backed off, tried again


async def test_a_5xx_is_retried_with_backoff_on_the_same_deployment(rig: Rig) -> None:
    await _scenario(rig, {f"{SMALL}@r1": {"error_rate_5xx": 1.0}})
    llmq = await rig.started({"small": {"max_attempts": 2, "retry_base_secs": 0.05}})
    with pytest.raises(CallFailed) as info:
        await llmq.converse("small", CHAT)
    assert info.value.kind == "provider_error"
    assert stats(rig.fake, f"{SMALL}@r1")["calls"] == 2


async def test_region_fallback_serves_when_confirmed_regions_fail(rig: Rig) -> None:
    await _scenario(
        rig,
        {
            f"{GPT}@r1": {"force_error": "ServiceUnavailableException"},
            f"{GPT}@r2": {"force_error": "ServiceUnavailableException"},
        },
    )
    llmq = await rig.started(
        {"big": {"fallback": {"on": ["error", "broken"], "after_secs": 0}, "max_attempts": 4}}
    )
    reply = await llmq.converse("big", CHAT)
    assert reply.meta.deployment == "aws-a/r3"
    assert reply.meta.fallback == "region"


async def test_model_fallback_serves_when_the_model_is_out_of_budget(rig: Rig) -> None:
    llmq = await rig.started(
        {
            "small": {
                "rpm": 1,
                "rate_window": "smooth",
                "model_fallback": {"on": ["exhausted"], "after_secs": 0, "models": ["big"]},
            }
        }
    )
    assert (await llmq.converse("small", CHAT)).meta.served_model == "small"
    reply = await llmq.converse("small", CHAT, max_wait_secs=2)
    assert reply.meta.served_model == "big"
    assert reply.meta.fallback == "model"


async def test_an_attempt_timeout_moves_on(rig: Rig) -> None:
    await _scenario(rig, {f"{GPT}@r1": {"hang": True}})
    llmq = await rig.started({"big": {"attempt_timeout_secs": 0.5}})
    started = time.monotonic()
    reply = await llmq.converse("big", CHAT)
    assert reply.meta.deployment == "aws-a/r2"
    assert time.monotonic() - started < 3


async def test_the_call_timeout_ends_the_call(rig: Rig) -> None:
    await _scenario(rig, {f"{SMALL}@r1": {"hang": True}})
    llmq = await rig.started()
    with pytest.raises(CallFailed) as info:
        await llmq.converse("small", CHAT, timeout_secs=1)
    assert info.value.kind == "deadline_exceeded"


async def test_a_request_error_fails_at_once(rig: Rig) -> None:
    await _scenario(rig, {f"{SMALL}@r1": {"force_error": "ValidationException"}})
    llmq = await rig.started()
    with pytest.raises(CallFailed) as info:
        await llmq.converse("small", CHAT)
    assert info.value.kind == "invalid_request"
    assert stats(rig.fake, f"{SMALL}@r1")["calls"] == 1


async def test_the_breaker_opens_after_failures(rig: Rig) -> None:
    await _scenario(rig, {f"{SMALL}@r1": {"error_rate_5xx": 1.0}})
    llmq = await rig.started(
        {"small": {"breaker_threshold": 2, "max_attempts": 2, "retry_base_secs": 0.01}}
    )
    with pytest.raises(CallFailed):
        await llmq.converse("small", CHAT)
    assert (await llmq.status("small"))["deployments"][0]["breaker"] == "open"
    with pytest.raises(CallFailed) as info:
        await llmq.converse("small", CHAT, max_wait_secs=0.3)
    assert info.value.kind == "rate_limited"  # waited for the breaker, never sent


# ---------------------------------------------------------------- lifecycle, admin


async def test_describe_and_health_say_local(rig: Rig) -> None:
    llmq = await rig.started(LLMQ_LOCAL_PROCESS_COUNT="4", LLMQ_MODE="worker")
    described = llmq.describe()
    assert described["execution_mode"] == "local"
    assert described["local_process_count"] == 4
    assert "store" not in described
    health = await llmq.health()
    assert health["mode"] == "local"
    assert health["ready"] is True
    assert health["stores"] == {}
    assert llmq.ready


async def test_store_only_operations_are_refused(rig: Rig) -> None:
    llmq = await rig.started()
    admin = llmq.admin
    for operation in (
        admin.requests.cancel(request_ids=["x"], actor="t", dry_run=False),
        admin.requests.list("small", tenant="acme"),
        admin.tenants.list("small"),
        llmq.request_status("x"),
        llmq.send_job("j", {}),
        admin.registry.set({"rpm": 3}, model="small", actor="t", dry_run=False),
        admin.purge.tenant("acme", actor="t"),
        admin.dlq.list(job="j"),
        admin.status.get("small", tenant="acme"),
    ):
        with pytest.raises(LocalModeRefused) as info:
            await operation
        assert info.value.category == "config_error"
    with pytest.raises(LocalModeRefused):
        llmq.register_job("j", lambda payload: asyncio.sleep(0))
    # What the process itself knows is served.
    assert (await admin.status.get("small"))["mode"] == "local"
    assert len(admin.tenants.hash("acme")["tenant_hash"]) == 16
    assert (await admin.registry.get("small"))["live_edits"] == []
    index = await admin.ops.index()
    assert index["library"]["execution_mode"] == "local"
    assert index["stores"] is None
    assert "small" in [m["model"] for m in index["models"]]
    assert admin.describe()["execution_mode"] == "local"


async def test_install_mounts_every_route_with_local_answers(rig: Rig, tmp_path: Path) -> None:
    drain = tmp_path / "drain"
    llmq = rig.queue(
        {"small": {"rpm": 60}},
        LLMQ_DRAIN_FILE=str(drain),
        LLMQ_DRAIN_POLL_SECS="0.1",
        LLMQ_LOCAL_PROCESS_COUNT="3",
    )
    app = FastAPI()

    @app.post("/ask")
    async def ask() -> dict[str, Any]:
        reply = await llmq.call("small", CHAT)
        return {"ok": reply.ok}

    install(
        app,
        llmq,
        admin_token="s3cret",
        allow_writes=True,
        actor=lambda request: "tester",
    )
    head = {"X-LLMQ-Token": "s3cret", "X-LLMQ-Admin": "1"}
    base = "/internal/llm-queue"
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://app") as client,
    ):
        ready = await client.get(f"{base}/health/ready")
        assert ready.status_code == 200
        assert ready.json()["mode"] == "local"
        assert (await client.get(f"{base}/health/live")).status_code == 200
        assert (await client.post("/ask")).json() == {"ok": True}
        status = (await client.get(f"{base}/status", headers=head)).json()
        assert status["mode"] == "local"
        assert status["process_count"] == 3
        small = next(m for m in status["models"] if m["model"] == "small")
        assert small["deployments"][0]["rpm"] == {
            "limit": 20.0,
            "used": 1,
            "left": 19,  # the test registry's windows are `fixed` minutes
            "quota": None,
        }
        assert (await client.get(f"{base}/registry", headers=head)).status_code == 200
        index = (await client.get(f"{base}/", headers=head)).json()
        assert index["library"]["execution_mode"] == "local"
        assert index["process"]["ready"] is True
        assert (await client.get(f"{base}/describe", headers=head)).json()[
            "execution_mode"
        ] == "local"
        edit = {"values": {"defaults.rpm": 5}, "dry_run": False}
        refused = [
            await client.get(f"{base}/requests/abc", headers=head),
            await client.get(f"{base}/requests", params={"model": "small"}, headers=head),
            await client.get(f"{base}/tenants", params={"model": "small"}, headers=head),
            await client.get(f"{base}/dlq", params={"model": "small"}, headers=head),
            await client.post(f"{base}/registry", json=edit, headers=head),
            await client.post(f"{base}/blocks", json={"region": "r1"}, headers=head),
            await client.post(f"{base}/purge/model", json={"model": "x"}, headers=head),
        ]
        for answer in refused:
            assert answer.status_code == 503, answer.text
            assert answer.json()["reason"] == "local_mode"
        # The drain file: readiness 503 while a call in flight finishes.
        slow = rig.fake.scenario
        slow.patch_model(f"{SMALL}@r1", {"latency_ms": {"p50": 500, "p95": 500}})
        running = asyncio.create_task(llmq.converse("small", CHAT))
        await asyncio.sleep(0.1)
        drain.touch()
        os.utime(drain, (time.time() + 2, time.time() + 2))
        for _ in range(30):
            if not llmq.ready:
                break
            await asyncio.sleep(0.05)
        not_ready = await client.get(f"{base}/health/ready")
        assert not_ready.status_code == 503
        assert not_ready.json()["not_ready_reason"] == "draining"
        assert (await running).ok  # the call in flight finished
        drained = (await client.get(f"{base}/drain", headers=head)).json()
        assert drained["draining"] is True
        assert drained["running"] == 0


async def test_stop_drains_then_fails_calls_still_running(rig: Rig) -> None:
    await _scenario(rig, {f"{SMALL}@r1": {"hang": True}})
    llmq = await rig.started()
    call = asyncio.create_task(llmq.converse("small", CHAT, timeout_secs=30))
    await asyncio.sleep(0.2)
    started = time.monotonic()
    await llmq.stop(drain_secs=0.5)
    with pytest.raises(CallFailed) as info:
        await call
    assert info.value.kind == "worker_shutdown"
    assert time.monotonic() - started < 3


def test_settings_validate_the_local_variables() -> None:
    from llm_queue import ConfigError
    from llm_queue.engine.settings import InfraSettings

    env = {"LLMQ_ENVIRONMENT": "test"}
    local = InfraSettings.from_env({**env, "LLMQ_EXECUTION_MODE": "local"})
    assert local.execution_mode == "local"
    assert local.store_url == ""
    assert (local.local_process_count, local.local_concurrency) == (1, 10)
    with pytest.raises(ConfigError, match=r"LLMQ_STORE_URL \(or LLMQ_STORE_HOST\) is required"):
        InfraSettings.from_env(env)  # queue mode still needs the store
    with pytest.raises(ConfigError, match="LLMQ_EXECUTION_MODE must be queue or local"):
        InfraSettings.from_env({**env, "LLMQ_EXECUTION_MODE": "direct"})
