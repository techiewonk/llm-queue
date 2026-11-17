"""One keyed hash, one boolean reader, tenant names hashed once (0.2.0 review B-24, RQ-5).

Every place that hashes a tenant, user or session id must agree, or a tenant splits in two:
the engine, the local executor, the admin services, the boot registry and `keyed_hash` itself.
A registry document is hashed on every write path, and never twice.
"""

from __future__ import annotations

import hashlib
import hmac

import pytest

from llm_queue.engine.fairness import hash_tenant_names, hash_tenant_path
from llm_queue.engine.settings import InfraSettings
from llm_queue.errors import ConfigError
from llm_queue.ids import keyed_hash

SALT = "s" * 16
STORE = {"LLMQ_STORE_URL": "redis://x:6379/0", "LLMQ_ID_HASH_SALT": SALT, "LLMQ_ENVIRONMENT": "dev"}


def _h(value: str) -> str:
    return keyed_hash(SALT, value)


def test_keyed_hash_is_hmac_sha256_of_the_salt() -> None:
    expected = hmac.new(SALT.encode(), b"acme", hashlib.sha256).hexdigest()[:16]
    assert keyed_hash(SALT, "acme") == expected


def test_the_boot_registry_hashes_with_keyed_hash() -> None:
    doc = '{"registry_version": 1, "defaults": {"fairness": {"weights": {"acme": 2}}}}'
    settings = InfraSettings.from_env({**STORE, "LLMQ_REGISTRY": doc})
    assert _h("acme") in settings.registry_document()


@pytest.mark.parametrize("name", ["LLMQ_REQUEST_INDEX_ENABLED", "LLMQ_TENANT_NAMES_ENABLED"])
def test_admin_switches_use_the_one_bool_reader(name: str) -> None:
    InfraSettings.from_env({**STORE, name: "yes"})  # `yes` is true, as for every switch
    with pytest.raises(ConfigError, match=name):
        InfraSettings.from_env({**STORE, name: "maybe"})


def test_an_exported_document_is_not_hashed_twice() -> None:
    """RQ-5: a published document (hashed, marked) published again keeps its hashes."""
    doc = {"defaults": {"fairness": {"mode": "share", "weights": {"acme": 2}}}}
    once = hash_tenant_names(doc, _h)
    assert once["defaults"]["fairness"] == {
        "mode": "share",
        "weights": {_h("acme"): 2},
        "tenant_keys": "hashed",
    }
    assert hash_tenant_names(once, _h) == once


def test_a_hash_in_a_block_of_names_and_a_name_in_a_hashed_block_are_refused() -> None:
    with pytest.raises(ConfigError, match="looks like a tenant hash"):
        hash_tenant_names({"models": {"m": {"fairness": {"weights": {_h("a"): 1}}}}}, _h)
    marked = {"weights": {"acme": 1}, "tenant_keys": "hashed"}
    with pytest.raises(ConfigError, match="not a tenant hash"):
        hash_tenant_names({"models": {"m": {"fairness": marked}}}, _h)


def test_live_edit_paths_hash_the_tenant_and_mark_the_block() -> None:
    assert hash_tenant_path("fairness.weights.acme", 3, _h) == [
        (f"fairness.weights.{_h('acme')}", 3),
        ("fairness.tenant_keys", "hashed"),
    ]
    assert hash_tenant_path("models.m.fairness.quotas.a.b.tpm", 9, _h)[0] == (
        f"models.m.fairness.quotas.{_h('a.b')}.tpm",
        9,
    )
    assert hash_tenant_path("rpm", 400, _h) == [("rpm", 400)]
