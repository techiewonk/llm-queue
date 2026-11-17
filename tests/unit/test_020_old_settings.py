"""0.2.0 renamed and removed settings are reported, never silently ignored (review L-6, B-15;
owner RV4: `LLMQ_LOCAL_CONCURRENCY` is the one name for local mode and the store-down cap).
"""

from __future__ import annotations

import logging

import pytest

from llm_queue import ConfigError
from llm_queue.engine.settings import REMOVED, RENAMED, InfraSettings

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


def test_the_old_local_cap_is_reported_as_renamed_in_dev(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="llm_queue"):
        settings = InfraSettings.from_env({**DEV, "LLMQ_LOCAL_LIMIT_CAP": "4"})
    assert settings.local_concurrency == 10  # never read: the new name decides
    line = next(r.getMessage() for r in caplog.records if "setting_renamed" in r.getMessage())
    assert "use=LLMQ_LOCAL_CONCURRENCY" in line
    assert "4" not in line  # the value is never logged


@pytest.mark.parametrize("name", sorted({**RENAMED, **REMOVED}))
def test_an_old_name_is_refused_when_strict(name: str) -> None:
    with pytest.raises(ConfigError, match=name):
        InfraSettings.from_env({**STRICT, name: "1"})


def test_an_old_name_in_defaults_is_reported_and_an_empty_one_is_not(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="llm_queue"):
        InfraSettings.from_env({**DEV, "LLMQ_TENANT_INDEX": ""})
        assert "setting_removed" not in caplog.text
        InfraSettings.from_env(DEV, defaults={"LLMQ_TENANT_INDEX": "true"})
    assert "setting_removed" in caplog.text


def test_the_tables_name_only_llmq_settings_and_new_names_are_not_old() -> None:
    assert all(n.startswith("LLMQ_") for n in (*RENAMED, *REMOVED, *RENAMED.values()))
    assert not set(RENAMED.values()) & (set(RENAMED) | set(REMOVED))
    assert RENAMED["LLMQ_LOCAL_LIMIT_CAP"] == "LLMQ_LOCAL_CONCURRENCY"
