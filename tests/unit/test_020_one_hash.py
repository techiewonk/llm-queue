"""One keyed hash and one boolean reader (0.2.0 review B-24).

Every place that hashes a tenant, user or session id must agree, or a tenant splits in two:
the engine, the local executor, the admin services, the boot registry and `keyed_hash` itself.
"""

from __future__ import annotations

import hashlib
import hmac

import pytest

from llm_queue.engine.settings import InfraSettings
from llm_queue.errors import ConfigError
from llm_queue.ids import keyed_hash

SALT = "s" * 16
STORE = {"LLMQ_STORE_URL": "redis://x:6379/0", "LLMQ_ID_HASH_SALT": SALT, "LLMQ_ENVIRONMENT": "dev"}


def test_keyed_hash_is_hmac_sha256_of_the_salt() -> None:
    expected = hmac.new(SALT.encode(), b"acme", hashlib.sha256).hexdigest()[:16]
    assert keyed_hash(SALT, "acme") == expected


def test_the_boot_registry_hashes_with_keyed_hash() -> None:
    doc = '{"registry_version": 1, "defaults": {"fairness": {"weights": {"acme": 2}}}}'
    settings = InfraSettings.from_env({**STORE, "LLMQ_REGISTRY": doc})
    assert keyed_hash(SALT, "acme") in settings.registry_document()


@pytest.mark.parametrize("name", ["LLMQ_REQUEST_INDEX_ENABLED", "LLMQ_TENANT_NAMES_ENABLED"])
def test_admin_switches_use_the_one_bool_reader(name: str) -> None:
    InfraSettings.from_env({**STORE, name: "yes"})  # `yes` is true, as for every switch
    with pytest.raises(ConfigError, match=name):
        InfraSettings.from_env({**STORE, name: "maybe"})
