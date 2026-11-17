"""0.2.0 T settings without a store: defaults, ranges, the metrics allow-list, hashed names.

Every behaviour has a default and a setting (owner rule); tenant names in the boot registry are
hashed when it is read (D2), so only hashes reach the store.
"""

from __future__ import annotations

import json

import pytest

from llm_queue.engine.metrics import TenantLabels
from llm_queue.engine.requests_admin import AdminSettings
from llm_queue.engine.settings import InfraSettings, parse_metrics_tenants
from llm_queue.errors import ConfigError
from llm_queue.ids import keyed_hash

DEV = {"LLMQ_STORE_URL": "redis://localhost:6379", "LLMQ_ENVIRONMENT": "dev"}


def test_defaults_match_the_code() -> None:
    assert AdminSettings.from_infra(InfraSettings.from_env(DEV)) == AdminSettings()
    assert AdminSettings().request_index is True  # D8: on by default
    assert AdminSettings().tenant_names is False  # Q5: names opt-in


def test_each_setting_is_read() -> None:
    env = {
        **DEV,
        "LLMQ_REQUEST_INDEX_ENABLED": "off",
        "LLMQ_TENANT_NAMES_ENABLED": "on",
        "LLMQ_ADMIN_BATCH": "7",
        "LLMQ_ADMIN_SCAN_MAX": "2000",
        "LLMQ_REQUESTS_LIST_MAX": "9",
        "LLMQ_REQUESTS_MOVE_BATCH": "3",
        "LLMQ_REQUESTS_MOVE_BYTES": "70000",
        "LLMQ_METRICS_TENANTS": "acme, globex=t2",
    }
    got = AdminSettings.from_infra(InfraSettings.from_env(env))
    assert got == AdminSettings(
        request_index=False,
        tenant_names=True,
        batch=7,
        scan_max=2000,
        list_max=9,
        move_batch=3,
        move_bytes=70000,
        metrics_tenants=(("acme", "acme"), ("globex", "t2")),
    )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("LLMQ_REQUEST_INDEX_ENABLED", "maybe"),
        ("LLMQ_TENANT_NAMES_ENABLED", "2"),
        ("LLMQ_ADMIN_SCAN_MAX", "999"),
        ("LLMQ_REQUESTS_MOVE_BATCH", "501"),
        ("LLMQ_REQUESTS_MOVE_BYTES", "1024"),
        ("LLMQ_METRICS_TENANTS", "acme=has space"),
        ("LLMQ_METRICS_TENANTS", "acme=other"),
        ("LLMQ_METRICS_TENANTS", "a=x,b=x"),
    ],
)
def test_bad_values_are_refused(name: str, value: str) -> None:
    with pytest.raises(ConfigError, match=name):
        InfraSettings.from_env({**DEV, name: value})


def test_metrics_tenants_cap() -> None:
    many = ",".join(f"t{i}" for i in range(51))
    _, problems = parse_metrics_tenants(many)
    assert any("at most 50" in p for p in problems)
    pairs, problems = parse_metrics_tenants(" a , b=lb ,, ")
    assert (pairs, problems) == ((("a", "a"), ("b", "lb")), [])


def test_tenant_metric_labels() -> None:
    """T6: listed tenants get their label, others `other`, no tenant `-`; names not kept."""
    labels = TenantLabels([("acme", "acme"), ("globex", "t2")], lambda v: f"h-{v}")
    assert labels("h-acme") == "acme"
    assert labels("h-globex") == "t2"
    assert labels("h-initech") == "other"
    assert labels(None) == "-"
    assert labels("") == "-"
    assert "acme" not in repr(labels.__slots__)


def test_registry_tenant_names_are_hashed_when_read() -> None:
    doc = {
        "registry_version": 1,
        "accounts": {"aws-a": {"kind": "aws"}},
        "defaults": {"fairness": {"mode": "share", "weights": {"acme": 3}}},
        "models": {
            "m": {
                "type": "chat",
                "provider": "bedrock",
                "model_id": "x",
                "fairness": {"mode": "quota", "rpm": 10, "quotas": {"acme": {"rpm": 50}}},
                "deployments": {"aws-a": {"confirmed": ["r1"]}},
            }
        },
    }
    settings = InfraSettings.from_env(
        {**DEV, "LLMQ_REGISTRY": json.dumps(doc), "LLMQ_ID_HASH_SALT": "s" * 16}
    )
    text = settings.registry_document()
    assert "acme" not in text
    out = json.loads(text)
    h = keyed_hash("s" * 16, "acme")
    assert out["defaults"]["fairness"]["weights"] == {h: 3}
    assert out["models"]["m"]["fairness"]["quotas"] == {h: {"rpm": 50}}
    plain = InfraSettings.from_env({**DEV, "LLMQ_REGISTRY": json.dumps({"models": {}})})
    assert plain.registry_document() == json.dumps({"models": {}})  # nothing to hash: as is
