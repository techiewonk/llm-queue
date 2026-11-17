"""0.2.0 registry placeholders never read a credential (review B-09, owner Q11), and the names
they read are logged, never the values.
"""

from __future__ import annotations

import json
import logging

import pytest

from llm_queue import ConfigError
from llm_queue.engine.settings import InfraSettings
from llm_queue.registry.interpolate import credential_name, expand

SECRET = "v3ry-s3cret-value"
DEV = {"LLMQ_STORE_URL": "redis://localhost:6379/0", "LLMQ_ENVIRONMENT": "dev"}

REFUSED = [
    "LLMQ_STORE_PASSWORD",
    "LLMQ_QUEUE_STORE_PASSWORD",
    "LLMQ_ADMIN_TOKEN",
    "LLMQ_ADMIN_READ_TOKEN",
    "LLMQ_ID_HASH_SALT",
    "LLMQ_SECRET_NAMES",
    "LLMQ_SECRETS_SOURCE",
    "LLMQ_SECRETS_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "llmq_store_password",
]
ALLOWED = ["ENV", "AWS_REGION", "LLMQ_KEY_PREFIX", "VLLM_HOST", "LLMQ_TOKENS_FOR_X"]


@pytest.mark.parametrize("name", REFUSED)
def test_credential_names_are_refused_even_with_a_default(name: str) -> None:
    assert credential_name(name)
    looked: list[str] = []

    def lookup(key: str) -> str:
        looked.append(key)
        return SECRET

    for text in (f"${{{name}}}", f"${{{name}:-x}}", f"pre-${{{name}}}"):
        with pytest.raises(ConfigError, match=f"`{name}` \\(at a.b\\) names a credential") as info:
            expand({"a": {"b": text}}, lookup)
        assert SECRET not in str(info.value)
    assert looked == []  # never even read


@pytest.mark.parametrize("name", ALLOWED)
def test_other_names_expand(name: str) -> None:
    assert not credential_name(name)
    used: set[str] = set()
    assert expand({"a": f"${{{name}}}"}, lambda key: "ok", used=used) == {"a": "ok"}
    assert used == {name}


def test_settings_log_the_names_used_never_the_values(caplog: pytest.LogCaptureFixture) -> None:
    doc = {"registry_version": 1, "models": {}, "routing": {"blocked_regions": "${BLOCKED}"}}
    env = {**DEV, "LLMQ_REGISTRY": json.dumps(doc), "BLOCKED": f'["{SECRET}"]'}
    with caplog.at_level(logging.INFO, logger="llm_queue"):
        InfraSettings.from_env(env)
    assert "registry_placeholders" in caplog.text
    assert "BLOCKED" in caplog.text
    assert SECRET not in caplog.text


def test_settings_refuse_a_registry_that_reads_the_store_password() -> None:
    doc = {"registry_version": 1, "models": {}, "accounts": {"a": {"kind": "aws",
           "role_arn": "${LLMQ_STORE_PASSWORD}"}}}  # fmt: skip
    env = {**DEV, "LLMQ_REGISTRY": json.dumps(doc), "LLMQ_STORE_PASSWORD": SECRET}
    with pytest.raises(ConfigError, match="LLMQ_STORE_PASSWORD") as info:
        InfraSettings.from_env(env)
    assert SECRET not in str(info.value)
