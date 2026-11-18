"""An `LLMQ_*` name no setting reads is reported, never silently ignored (review L-6, B-15).

A typo, or a renamed or removed setting, refuses a strict start and logs `setting_unknown` in
dev; there is no per-name history (owner: no backward compatibility in 0.2.0).
"""

from __future__ import annotations

import logging

import pytest

from llm_queue import ConfigError
from llm_queue.engine.settings import InfraSettings

DEV = {
    "LLMQ_STORE_URL": "redis://localhost:6379/0",
    "LLMQ_ENVIRONMENT": "dev",
    "LLMQ_REGISTRY": '{"registry_version": 1, "models": {}}',
}
STRICT = {
    "LLMQ_STORE_URL": "rediss://store.example.com:6380/0",
    "LLMQ_ENVIRONMENT": "prod",
    "LLMQ_REGISTRY": '{"registry_version": 1, "models": {}}',
    "LLMQ_ID_HASH_SALT": "s" * 16,
    "LLMQ_KEY_PREFIX": "app-prod",
}


def test_an_unknown_name_is_logged_in_dev(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="llm_queue"):
        settings = InfraSettings.from_env({**DEV, "LLMQ_LOCAL_LIMIT_CAP": "4"})
    assert settings.local_concurrency == 10  # never read
    line = next(r.getMessage() for r in caplog.records if "setting_unknown" in r.getMessage())
    assert "LLMQ_LOCAL_LIMIT_CAP" in line
    assert "4" not in line  # the value is never logged


@pytest.mark.parametrize("name", ["LLMQ_TENANT_INDEX", "LLMQ_PURGE_BATCH", "LLMQ_LEASE_SEC"])
def test_an_unknown_name_is_refused_when_strict(name: str) -> None:
    with pytest.raises(ConfigError, match=name):
        InfraSettings.from_env({**STRICT, name: "1"})


def test_job_account_and_other_names_are_not_reported() -> None:
    env = {
        **STRICT,
        "LLMQ_JOB_CONCURRENCY": "2",
        "LLMQ_ACCOUNT_VLLM_API_KEY": "k",
        "OTHER_APP_SETTING": "x",
    }
    InfraSettings.from_env(env)


def test_an_unknown_name_in_defaults_is_reported_and_an_empty_one_is_not(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="llm_queue"):
        InfraSettings.from_env({**DEV, "LLMQ_TENANT_INDEX": ""})
        assert "setting_unknown" not in caplog.text
        InfraSettings.from_env(DEV, defaults={"LLMQ_TENANT_INDEX": "true"})
    assert "setting_unknown" in caplog.text
