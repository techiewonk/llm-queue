"""0.2.0 D2: one `llmq_configured` line at start with the log-safe `describe()`.

Local mode starts without a store. The line carries no salt, token or tenant name (the
default tenant only as its keyed hash), and a broken `describe()` never fails the start.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pytest

from llm_queue import LLMQueue

DOC = {
    "registry_version": 7,
    "residency": {"allowed_regions": ["us-east-1"]},
    "accounts": {"aws-0": {"kind": "aws"}},
    "models": {
        "chat": {
            "type": "chat",
            "provider": "bedrock",
            "model_id": "openai.gpt-oss-20b-1:0",
            "rpm": 60,
            "deployments": {"aws-0": {"confirmed": ["us-east-1"]}},
        }
    },
}
LOCAL = {
    "LLMQ_EXECUTION_MODE": "local",
    "LLMQ_ENVIRONMENT": "dev",
    "LLMQ_REGISTRY": json.dumps(DOC),
    "LLMQ_ID_HASH_SALT": "salt-value-not-logged",
    "LLMQ_DEFAULT_TENANT": "acme-corp",
    "LLMQ_ADMIN_TOKEN": "token-value-not-logged",
}


def _lines(caplog: pytest.LogCaptureFixture, event: str) -> list[str]:
    return [r.getMessage() for r in caplog.records if event in r.getMessage()]


async def test_configured_is_logged_once_without_secrets(caplog: pytest.LogCaptureFixture) -> None:
    llmq = LLMQueue.from_env(LOCAL)
    with caplog.at_level(logging.INFO, logger="llm_queue"):
        await llmq.start()
        await llmq.start()  # a second start does nothing
        await llmq.stop(drain_secs=0)
    lines = _lines(caplog, "llmq_configured")
    assert len(lines) == 1
    line = lines[0]
    assert "registry_version" in line
    assert llmq._engine.hash_id("acme-corp") in line
    for secret in ("salt-value-not-logged", "token-value-not-logged", "acme-corp"):
        assert secret not in line


async def test_a_failing_describe_does_not_fail_the_start(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    llmq = LLMQueue.from_env(LOCAL)

    def broken() -> dict[str, Any]:
        raise ValueError("registry file gone")

    monkeypatch.setattr(llmq, "describe", broken)
    with caplog.at_level(logging.INFO, logger="llm_queue"):
        await llmq.start()
        assert llmq.started
        await llmq.stop(drain_secs=0)
    assert _lines(caplog, "llmq_configured_failed")
    assert not [m for m in _lines(caplog, "llmq_configured") if "failed" not in m]
