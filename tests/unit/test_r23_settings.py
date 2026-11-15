"""Strict-mode settings the owner decided: an explicit key prefix (Q26), https endpoints (Q30)."""

from __future__ import annotations

import pytest

from llm_queue import ConfigError
from llm_queue.engine.settings import InfraSettings

TLS = "rediss://cache.example.com:6379/0"
STRICT = {"LLMQ_STORE_URL": TLS, "LLMQ_ID_HASH_SALT": "s", "LLMQ_KEY_PREFIX": "app-prod"}


def test_strict_mode_needs_an_explicit_key_prefix() -> None:
    """Q26 (R3SE-9): two environments that both left the default `llmq` shared one store.

    Staging workers ran prod prompts with staging credentials. Strict mode now requires
    `LLMQ_KEY_PREFIX`; dev and test keep `llmq`.
    """
    with pytest.raises(ConfigError, match="LLMQ_KEY_PREFIX is required"):
        InfraSettings.from_env({"LLMQ_STORE_URL": TLS, "LLMQ_ID_HASH_SALT": "s"})
    dev = InfraSettings.from_env({"LLMQ_STORE_URL": TLS, "LLMQ_ENVIRONMENT": "test"})
    assert dev.key_prefix == "llmq"
    assert InfraSettings.from_env(STRICT).key_prefix == "app-prod"


@pytest.mark.parametrize("name", ["LLMQ_BEDROCK_ENDPOINT_URL", "LLMQ_STS_ENDPOINT_URL"])
def test_endpoint_overrides_are_https_only_when_strict(name: str) -> None:
    """Q30 (R3SE-6): an `http://` override sent prompts, SigV4 and STS keys in clear."""
    with pytest.raises(ConfigError, match=f"{name} must use https"):
        InfraSettings.from_env({**STRICT, name: "http://x:8080"})
    assert InfraSettings.from_env({**STRICT, name: "https://any.host.example"}).strict
    dev = InfraSettings.from_env(
        {"LLMQ_STORE_URL": TLS, "LLMQ_ENVIRONMENT": "dev", name: "http://x"}
    )
    assert dev.strict is False
