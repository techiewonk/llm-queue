"""0.2.0 H1 / FU1: `LLMQ_ENABLED=false` switches the library off, on every surface.

Python (`DisabledQueue`: calls refused as `config_error`, admin store operations refused like
local mode, factories None, drain and readiness), CLI (`llm-queue` refuses store commands,
`describe` answers) and HTTP (`install()` mounts the two probes only). No store is needed.
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from llm_queue import LLMQueue, QueueDisabled, queue_ready
from llm_queue.disabled import DisabledQueue
from llm_queue.engine.cli import main as cli_main
from llm_queue.engine.http import error_response
from llm_queue.engine.settings import InfraSettings
from llm_queue.errors import ConfigError
from llm_queue.fastapi import install
from llm_queue.langchain import chat_model, embeddings, set_default_queue

OFF = {"LLMQ_ENABLED": "false"}


def _off(**extra: str) -> DisabledQueue:
    queue = LLMQueue.from_env({**OFF, **extra})
    assert isinstance(queue, DisabledQueue)
    return queue


# ---------------------------------------------------------------- settings


def test_enabled_by_default_and_off_needs_no_other_setting() -> None:
    on = InfraSettings.from_env({"LLMQ_STORE_URL": "redis://h:6379", "LLMQ_ENVIRONMENT": "dev"})
    assert on.enabled is True
    # Outside dev a queue needs a store, a salt and a key prefix; switched off it needs none.
    off = InfraSettings.from_env({**OFF, "LLMQ_ENVIRONMENT": "prod", "LLMQ_LEASE_SECS": "x"})
    assert off.enabled is False
    assert off.environment == "prod"


def test_bad_switch_value_is_refused() -> None:
    with pytest.raises(ConfigError, match="LLMQ_ENABLED must be true or false"):
        InfraSettings.from_env({"LLMQ_ENABLED": "maybe", "LLMQ_ENVIRONMENT": "dev"})


def test_bad_drain_poll_is_refused_when_off() -> None:
    with pytest.raises(ConfigError, match="LLMQ_DRAIN_POLL_SECS"):
        InfraSettings.from_env({**OFF, "LLMQ_DRAIN_POLL_SECS": "0"})


def test_switch_reads_secrets_and_defaults_like_every_setting() -> None:
    def secrets(name: str) -> str | None:
        return "false" if name == "LLMQ_ENABLED" else None

    assert isinstance(LLMQueue.from_env({}, secrets=secrets), DisabledQueue)
    assert isinstance(LLMQueue.from_env({}, defaults={"LLMQ_ENABLED": False}), DisabledQueue)


# ---------------------------------------------------------------- Python


async def test_calls_are_refused_as_config_error() -> None:
    queue = _off()
    attempts: list[Any] = [
        lambda: queue.converse("m", {}),
        lambda: queue.chat("m", {}),
        lambda: queue.call("m", {}),
        lambda: queue.embed("m", ["a"]),
        lambda: queue.embed_one("m", "a"),
        lambda: queue.send_job("j", {}),
        lambda: queue.status(),
        lambda: queue.request_status("r"),
    ]
    for attempt in attempts:
        with pytest.raises(QueueDisabled) as info:
            await attempt()
        assert info.value.category == "config_error"
        assert not info.value.retryable
        assert "LLMQ_ENABLED=false" in str(info.value)
    with pytest.raises(QueueDisabled):
        async for _ in queue.converse_stream("m", {}):
            pass
    for sync in (
        lambda: queue.converse_sync("m", {}),
        lambda: queue.embed_sync("m", ["a"]),
        lambda: queue.register_job("j", lambda ctx: None),
    ):
        with pytest.raises(QueueDisabled):
            sync()


async def test_admin_refuses_store_operations_and_serves_the_process() -> None:
    queue = _off()
    admin = queue.admin
    for call in (
        lambda: admin.status.get(),
        lambda: admin.requests.list("m"),
        lambda: admin.purge.model("m", actor="a", dry_run=True),
        lambda: admin.registry.get(),
        lambda: admin.models.list(),
    ):
        with pytest.raises(QueueDisabled):
            await call()
    assert admin.registry.schema()["type"] == "object"
    assert admin.describe()["enabled"] is False
    index = await admin.ops.index(routes=[])
    assert index["library"]["enabled"] is False
    assert index["process"]["ready"] is True
    assert (await admin.drain.start(actor="a"))["draining"] is True
    assert queue.ready is False
    assert (await admin.drain.resume(actor="a"))["draining"] is False


async def test_start_stop_health_ready_and_describe() -> None:
    queue = _off(LLMQ_ENVIRONMENT="prod")
    assert queue.enabled is False
    assert queue.execution_mode == "disabled"
    assert queue.describe() == {
        "version": queue.describe()["version"],
        "instance_id": None,
        "enabled": False,
        "environment": "prod",
        "drain_file": False,
    }
    set_default_queue(None)
    await queue.start()
    try:
        health = await queue.health()
        assert health["enabled"] is False
        assert health["live"]
        assert health["ready"]
        assert queue_ready() is True  # the default queue, switched off: the direct path serves
        queue.pause_receiving()
        assert queue_ready() is False
        assert (await queue.health())["not_ready_reason"] == "draining"
        queue.resume_receiving()
        assert queue_ready(queue) is True
    finally:
        await queue.stop()
    assert queue_ready() is False  # stopped: no default queue any more


async def test_drain_file_still_fails_readiness(tmp_path: Path) -> None:
    path = tmp_path / "drain"
    queue = _off(LLMQ_DRAIN_FILE=str(path), LLMQ_DRAIN_POLL_SECS="0.1")
    await queue.start()
    try:
        assert queue.ready
        path.touch()
        os.utime(path, (time.time() + 2, time.time() + 2))
        for _ in range(50):
            if not queue.ready:
                break
            await asyncio.sleep(0.05)
        assert queue.ready is False
        assert queue.draining
    finally:
        await queue.stop()


def test_factories_return_none(monkeypatch: pytest.MonkeyPatch) -> None:
    queue = _off()
    assert chat_model("any-model", registry=queue) is None
    assert embeddings("any-model", registry=queue) is None
    monkeypatch.setenv("LLMQ_ENABLED", "off")
    monkeypatch.delenv("LLMQ_REGISTRY", raising=False)
    monkeypatch.delenv("LLMQ_REGISTRY_FILE", raising=False)
    assert chat_model("any-model") is None  # no registry read at all


# ---------------------------------------------------------------- HTTP


def test_install_mounts_the_probes_only() -> None:
    app = FastAPI()
    queue = install(app, _off(), admin_token="t" * 40, allow_writes=True)
    assert set(app.openapi()["paths"]) == {
        "/internal/llm-queue/health/live",
        "/internal/llm-queue/health/ready",
    }
    with TestClient(app) as client:
        assert client.get("/internal/llm-queue/health/live").json() == {
            "live": True,
            "enabled": False,
        }
        ready = client.get("/internal/llm-queue/health/ready")
        assert ready.status_code == 200
        assert ready.json()["enabled"] is False
        assert client.get("/internal/llm-queue/status").status_code == 404
        queue.pause_receiving()
        assert client.get("/internal/llm-queue/health/ready").status_code == 503


def test_refusal_maps_to_503_disabled() -> None:
    code, body = error_response(QueueDisabled("status.get"))
    assert code == 503
    assert body["reason"] == "disabled"
    assert body["enabled"] is False


# ---------------------------------------------------------------- CLI


def test_cli_refuses_store_commands_and_describes(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli_main(["status"], environ=OFF) == 1
    assert "LLMQ_ENABLED=false" in capsys.readouterr().err
    assert cli_main(["--actor", "a", "purge", "model", "m"], environ=OFF) == 1
    capsys.readouterr()
    assert cli_main(["describe"], environ=OFF) == 0
    assert '"enabled": false' in capsys.readouterr().out
