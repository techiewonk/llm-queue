"""`LLMQ_CONFIG_ERROR_MODE` and the off switch that never touches a secret.

`LLMQ_ENABLED=false` (environment, then `defaults=`) returns a `DisabledQueue` without building
or reading a secrets source. With `LLMQ_CONFIG_ERROR_MODE=disable`, a `ConfigError` at
`from_env` (a bad setting, a secrets source that cannot be read) is logged once as
`llmq_config_failed`, without values, and a `DisabledQueue` with `disabled_reason:
config_error` is returned: probes, drain file, factories None. `raise` (the default) raises.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from llm_queue import LLMQueue, QueueDisabled
from llm_queue import secrets as secrets_module
from llm_queue.disabled import DisabledQueue
from llm_queue.errors import ConfigError
from llm_queue.fastapi import install
from llm_queue.langchain import chat_model, embeddings
from llm_queue.secrets import SecretReader

SALT = "salt-value-never-logged"
TOKEN = "admin-token-value-never-logged-0123456789"
PASSWORD = "store-password-never-logged"
AWS = "aws-secretsmanager:prod/app/llmq"
DISABLE = {"LLMQ_CONFIG_ERROR_MODE": "disable"}


class _ClientError(Exception):
    """botocore's ClientError shape: the AWS error code in `response`."""

    def __init__(self, code: str) -> None:
        super().__init__(f"An error occurred ({code}) with {PASSWORD}")
        self.response = {"Error": {"Code": code}}


class EndpointConnectionError(Exception):
    """botocore's network failure (only the class name may be shown)."""


class _Client:
    def __init__(self, reply: Any) -> None:
        self.reply = reply

    def get_secret_value(self, SecretId: str) -> Mapping[str, Any]:
        if isinstance(self.reply, BaseException):
            raise self.reply
        return dict(self.reply)


def _aws(monkeypatch: pytest.MonkeyPatch, reply: Any) -> list[str]:
    """Secrets Manager answering `reply` (an exception raises); returns the regions asked."""
    made: list[str] = []

    def factory(region: str | None, timeout_secs: float) -> _Client:
        made.append(str(region))
        return _Client(reply)

    monkeypatch.setattr(secrets_module, "_botocore_client", factory)
    return made


def _never_touched(monkeypatch: pytest.MonkeyPatch) -> None:
    """Any secrets source built or read fails the test."""

    def touched(*_: Any, **__: Any) -> Any:
        raise AssertionError("a secrets source was touched")

    monkeypatch.setattr(secrets_module, "secrets_from_config", touched)
    monkeypatch.setattr(secrets_module, "_botocore_client", touched)


def _failed_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if "llmq_config_failed" in r.getMessage()]


def _assert_no_values(text: str) -> None:
    for value in (SALT, TOKEN, PASSWORD):
        assert value not in text


# ---------------------------------------------------------------- the off switch


def test_off_switch_never_builds_or_reads_a_secrets_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _never_touched(monkeypatch)

    def code_secrets(name: str) -> str | None:
        raise AssertionError(f"secrets= was asked for {name}")

    env = {"LLMQ_ENABLED": "false", "LLMQ_SECRETS_SOURCE": AWS, "LLMQ_ENVIRONMENT": "prod"}
    queue = LLMQueue.from_env(env)
    assert isinstance(queue, DisabledQueue)
    assert queue.disabled_reason == "switch"
    assert queue.describe()["environment"] == "prod"
    assert isinstance(LLMQueue.from_env(env, secrets=code_secrets), DisabledQueue)
    # The app's own default (`defaults={"LLMQ_ENABLED": False}`) is read before a secret too.
    off = LLMQueue.from_env({"LLMQ_SECRETS_SOURCE": AWS}, defaults={"LLMQ_ENABLED": False})
    assert isinstance(off, DisabledQueue)


def test_a_secret_cannot_switch_the_library(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = {"LLMQ_ENABLED": "false", "LLMQ_CONFIG_ERROR_MODE": "disable"}
    _aws(monkeypatch, {"SecretString": json.dumps(secret)})
    env = {"LLMQ_SECRETS_SOURCE": AWS, "LLMQ_ENVIRONMENT": "dev"}
    with pytest.raises(ConfigError, match="LLMQ_STORE_URL"):  # neither key was taken
        LLMQueue.from_env(env)
    for name in ("LLMQ_ENABLED", "LLMQ_CONFIG_ERROR_MODE"):
        with pytest.raises(ConfigError, match=f"{name} is read from the environment"):
            LLMQueue.from_env({**env, "LLMQ_SECRETS_NAMES": json.dumps({name: "k"})})


# ---------------------------------------------------------------- raise (the default)


def test_raise_is_the_default(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ConfigError, match="LLMQ_LEASE_SECS"):
        LLMQueue.from_env({"LLMQ_ENVIRONMENT": "dev", "LLMQ_LEASE_SECS": "x"})
    with pytest.raises(ConfigError, match="LLMQ_LEASE_SECS"):
        LLMQueue.from_env(
            {"LLMQ_ENVIRONMENT": "dev", "LLMQ_LEASE_SECS": "x", "LLMQ_CONFIG_ERROR_MODE": "raise"}
        )
    _aws(monkeypatch, _ClientError("AccessDeniedException"))
    with pytest.raises(ConfigError, match="AccessDeniedException"):
        LLMQueue.from_env({"LLMQ_SECRETS_SOURCE": AWS})


def test_an_invalid_mode_raises_naming_it() -> None:
    with pytest.raises(ConfigError, match="LLMQ_CONFIG_ERROR_MODE must be raise or disable"):
        LLMQueue.from_env({"LLMQ_CONFIG_ERROR_MODE": "log", "LLMQ_ENVIRONMENT": "dev"})


# ---------------------------------------------------------------- disable


def test_a_bad_setting_is_logged_once_and_disables(caplog: pytest.LogCaptureFixture) -> None:
    env = {
        **DISABLE,
        "LLMQ_ENVIRONMENT": "prod",
        "LLMQ_STORE_URL": f"rediss://:{PASSWORD}@store:6379/0",
        "LLMQ_ID_HASH_SALT": SALT,
        "LLMQ_ADMIN_TOKEN": TOKEN,
        "LLMQ_LEASE_SECS": "x",
    }
    with caplog.at_level(logging.INFO, logger="llm_queue"):
        queue = LLMQueue.from_env(env)
    assert isinstance(queue, DisabledQueue)
    assert queue.disabled_reason == "config_error"
    lines = _failed_lines(caplog)
    assert len(lines) == 1
    assert "LLMQ_LEASE_SECS" in lines[0]
    assert [r.levelno for r in caplog.records if "llmq_config_failed" in r.getMessage()] == [
        logging.ERROR
    ]
    _assert_no_values("\n".join(r.getMessage() for r in caplog.records))
    described = queue.describe()
    assert described["enabled"] is False
    assert described["disabled_reason"] == "config_error"
    assert described["environment"] == "prod"
    _assert_no_values(json.dumps(described))


@pytest.mark.parametrize(
    ("reply", "shown"),
    [
        (_ClientError("AccessDeniedException"), "AccessDeniedException"),
        (_ClientError("ResourceNotFoundException"), "ResourceNotFoundException"),
        (EndpointConnectionError(f"cannot reach {PASSWORD}"), "EndpointConnectionError"),
        ({"SecretString": "{not json " + SALT}, "not valid JSON"),
        ({"SecretBinary": b"x"}, "no SecretString"),
    ],
)
def test_a_secrets_source_that_cannot_be_read_disables(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, reply: Any, shown: str
) -> None:
    _aws(monkeypatch, reply)
    env = {**DISABLE, "LLMQ_SECRETS_SOURCE": AWS, "AWS_REGION": "ap-south-1"}
    with caplog.at_level(logging.INFO, logger="llm_queue"):
        queue = LLMQueue.from_env(env)
    assert isinstance(queue, DisabledQueue)
    assert queue.disabled_reason == "config_error"
    lines = _failed_lines(caplog)
    assert len(lines) == 1
    assert shown in lines[0]
    assert AWS in lines[0]
    _assert_no_values("\n".join(r.getMessage() for r in caplog.records))


def test_a_code_secret_reader_that_fails_disables(caplog: pytest.LogCaptureFixture) -> None:
    class Broken:
        kind = "app"
        label = "app:vault"

        def load(self) -> Mapping[str, str]:
            raise TimeoutError(f"vault timed out holding {SALT}")

    with caplog.at_level(logging.INFO, logger="llm_queue"):
        queue = LLMQueue.from_env(
            {**DISABLE, "LLMQ_ENVIRONMENT": "dev"}, secrets=SecretReader([Broken()])
        )
    assert isinstance(queue, DisabledQueue)
    assert queue.disabled_reason == "config_error"
    line = _failed_lines(caplog)[0]
    assert "TimeoutError" in line
    _assert_no_values(line)


def test_mode_is_read_from_defaults_too(monkeypatch: pytest.MonkeyPatch) -> None:
    _aws(monkeypatch, _ClientError("AccessDeniedException"))
    queue = LLMQueue.from_env(
        {"LLMQ_SECRETS_SOURCE": AWS}, defaults={"LLMQ_CONFIG_ERROR_MODE": "disable"}
    )
    assert isinstance(queue, DisabledQueue)
    assert queue.disabled_reason == "config_error"


# ---------------------------------------------------------------- the disabled queue


def _config_failed(**extra: str) -> DisabledQueue:
    queue = LLMQueue.from_env({**DISABLE, "LLMQ_LEASE_SECS": "x", **extra})
    assert isinstance(queue, DisabledQueue)
    return queue


async def test_health_calls_and_drain_file(tmp_path: Path) -> None:
    path = tmp_path / "drain"
    queue = _config_failed(LLMQ_DRAIN_FILE=str(path), LLMQ_DRAIN_POLL_SECS="0.1")
    with pytest.raises(QueueDisabled):
        await queue.converse("m", {})
    await queue.start()
    try:
        health = await queue.health()
        assert health["enabled"] is False  # type: ignore[typeddict-item]
        assert health["disabled_reason"] == "config_error"
        assert health["ready"]
        path.touch()
        os.utime(path, (time.time() + 2, time.time() + 2))
        for _ in range(50):
            if not queue.ready:
                break
            await asyncio.sleep(0.05)
        assert queue.ready is False
    finally:
        await queue.stop()


def test_a_bad_drain_poll_still_disables() -> None:
    queue = _config_failed(LLMQ_DRAIN_POLL_SECS="0")
    assert queue.settings.drain_poll_secs == 1.0


def test_factories_return_none_and_install_mounts_the_probes_only() -> None:
    queue = _config_failed()
    assert chat_model("any-model", registry=queue) is None
    assert embeddings("any-model", registry=queue) is None
    app = FastAPI()
    install(app, queue, admin_token="t" * 40, allow_writes=True)
    assert set(app.openapi()["paths"]) == {
        "/internal/llm-queue/health/live",
        "/internal/llm-queue/health/ready",
    }
    with TestClient(app) as client:
        live = client.get("/internal/llm-queue/health/live").json()
        assert live == {"live": True, "enabled": False, "disabled_reason": "config_error"}
        ready = client.get("/internal/llm-queue/health/ready")
        assert ready.status_code == 200
        assert ready.json()["disabled_reason"] == "config_error"
