"""Purge settings and the purge report without a store (0.1.0b3).

The purge routes are generated from the operations table (tests/unit/test_admin_http.py); the
purges themselves are tested against the real stores in tests/integration/engine/test_purge.py.
"""

from __future__ import annotations

import pytest

from llm_queue.engine.purge import PurgeReport, PurgeSettings
from llm_queue.engine.settings import InfraSettings
from llm_queue.errors import ConfigError

DEV = {"LLMQ_STORE_URL": "redis://localhost:6379", "LLMQ_ENVIRONMENT": "dev"}


def test_purge_settings_defaults_and_validation() -> None:
    settings = InfraSettings.from_env(DEV)
    purge = PurgeSettings.from_infra(settings)
    assert purge == PurgeSettings()  # one set of defaults, env and code
    custom = InfraSettings.from_env(
        {
            **DEV,
            "LLMQ_AUTO_PURGE": "off",
            "LLMQ_TENANT_INDEX": "on",
            "LLMQ_TENANT_PURGE_DEFAULT": "running, keys",
            "LLMQ_PURGE_BATCH": "50",
        }
    )
    got = PurgeSettings.from_infra(custom)
    assert (got.auto, got.tenant_index, got.batch) == (False, True, 50)
    assert got.tenant_default == frozenset({"keys", "running"})
    for name, value in (
        ("LLMQ_AUTO_PURGE", "maybe"),
        ("LLMQ_TENANT_PURGE_DEFAULT", "keys,everything"),
        ("LLMQ_PURGE_BATCH", "0"),
    ):
        with pytest.raises(ConfigError, match=name):
            InfraSettings.from_env({**DEV, name: value})


def test_report_as_dict() -> None:
    report = PurgeReport("m", "model", 3, ("q",), (("q2", "busy"),), True, "ann", 1.0)
    assert report.as_dict() == {
        "target": "m",
        "kind": "model",
        "deleted_keys": 3,
        "queues": ["q"],
        "skipped": [{"thing": "q2", "why": "busy"}],
        "dry_run": True,
        "actor": "ann",
        "at": 1.0,
        "deleted_fields": 0,
        "cancelled": 0,
    }


def test_key_expiry_and_purge_switches_are_validated() -> None:
    """Each new setting has a default and refuses a bad value (owner: default + configurable)."""
    settings = InfraSettings.from_env(DEV)
    assert (settings.key_expiry, settings.http_purge_wait_secs) == ("auto", 60.0)
    assert (settings.ttl_sweep_batch, settings.ttl_sweep_rounds) == (500, 100)
    for name, value in (
        ("LLMQ_KEY_EXPIRY", "sometimes"),
        ("LLMQ_HTTP_PURGE_WAIT_SECS", "4000"),
        ("LLMQ_PURGE_WAIT", "maybe"),
        ("LLMQ_TENANT_INDEX", "maybe"),
        ("LLMQ_TTL_SWEEP_BATCH", "6000"),
    ):
        with pytest.raises(ConfigError, match=name):
            InfraSettings.from_env({**DEV, name: value})
