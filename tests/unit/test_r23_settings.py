"""Strict-mode settings the owner decided: an explicit key prefix (Q26), https endpoints (Q30)."""

from __future__ import annotations

import pytest

from llm_queue import ConfigError
from llm_queue.engine.settings import InfraSettings

TLS = "rediss://cache.example.com:6379/0"
STRICT = {
    "LLMQ_STORE_URL": TLS,
    "LLMQ_ID_HASH_SALT": "test-salt-0123456789",
    "LLMQ_KEY_PREFIX": "app-prod",
}


def test_strict_mode_needs_an_explicit_key_prefix() -> None:
    """Q26 (R3SE-9): two environments that both left the default `llmq` shared one store.

    Staging workers ran prod prompts with staging credentials. Strict mode now requires
    `LLMQ_KEY_PREFIX`; dev and test keep `llmq`.
    """
    with pytest.raises(ConfigError, match="LLMQ_KEY_PREFIX is required"):
        InfraSettings.from_env({"LLMQ_STORE_URL": TLS, "LLMQ_ID_HASH_SALT": "test-salt-0123456789"})
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


def test_fo13_strict_salt_has_a_minimum_length() -> None:
    """FO-13: strict mode took any non-empty salt (`x`), which does not stop brute force."""
    with pytest.raises(ConfigError, match="at least 16 characters"):
        InfraSettings.from_env({**STRICT, "LLMQ_ID_HASH_SALT": "x"})
    dev = InfraSettings.from_env({"LLMQ_STORE_URL": TLS, "LLMQ_ENVIRONMENT": "dev"})
    assert dev.id_hash_salt is None


def test_fo19_store_url_message_names_every_scheme() -> None:
    """FO-19: the message said redis:// or rediss:// though valkey(s):// are accepted."""
    with pytest.raises(ConfigError, match="valkeys://"):
        InfraSettings.from_env({**STRICT, "LLMQ_STORE_URL": "http://x"})


@pytest.mark.parametrize(
    ("name", "value"),
    [("LLMQ_KEY_PREFIX", "a{m:x}"), ("LLMQ_KEY_PREFIX", "a*"), ("LLMQ_QUEUE_PREFIX", "q:1")],
)
def test_fo20_prefixes_are_plain(name: str, value: str) -> None:
    """FO-20: a prefix holding `{m:` or glob characters made `registry gc` mis-attribute keys."""
    with pytest.raises(ConfigError, match=name):
        InfraSettings.from_env({**STRICT, name: value})


def test_fo20_a_key_prefix_may_hold_colons() -> None:
    """FO-20 follow-up: `llmq:prod:v3` (the documented, deployed form) was refused at start."""
    settings = InfraSettings.from_env({**STRICT, "LLMQ_KEY_PREFIX": "llmq:prod:v3"})
    assert settings.key_prefix == "llmq:prod:v3"


def test_r3cf13_allowed_hosts_are_bare_names() -> None:
    """R3CF-13: `https://api.openai.com` never matched, and the error did not name the entry."""
    with pytest.raises(ConfigError, match=r"`https://api.openai.com` must be a bare host"):
        InfraSettings.from_env(
            {**STRICT, "LLMQ_BASE_URL_ALLOWED_HOSTS": "https://api.openai.com, api.x.com"}
        )
    ok = InfraSettings.from_env({**STRICT, "LLMQ_BASE_URL_ALLOWED_HOSTS": "api.x.com, [::1]"})
    assert ok.base_url_allowed_hosts == ("api.x.com", "[::1]")


@pytest.mark.parametrize(
    ("env", "fragment"),
    [
        ({"LLMQ_CFG_RELOAD_SECS": "1e-9"}, "LLMQ_CFG_RELOAD_SECS must be at least 1"),
        ({"LLMQ_RESULT_TTL_SECS": "9" * 23}, "LLMQ_RESULT_TTL_SECS must be at most"),
        ({"LLMQ_STORE_MAX_CONNECTIONS": "1"}, "LLMQ_STORE_MAX_CONNECTIONS must be at least 4"),
        ({"LLMQ_STATUS_TTL_SECS": "0"}, "LLMQ_STATUS_TTL_SECS must be at least 1"),
        ({"LLMQ_QUEUE_WAIT_REPLICAS": "99"}, "LLMQ_QUEUE_WAIT_REPLICAS must be at most 16"),
        ({"LLMQ_STORE_MAX_CONNECTIONS": "200.0"}, "must be a whole number"),
    ],
)
def test_r3cf14_process_numbers_have_ranges(env: dict[str, str], fragment: str) -> None:
    """R3CF-14 / R3CF-17: numbers had a lower bound only, and `200.0` was 'not a number'."""
    with pytest.raises(ConfigError, match=fragment):
        InfraSettings.from_env({**STRICT, **env})


def test_r3cf14_empty_means_unset_and_drain_may_be_zero() -> None:
    """R3CF-14: `LLMQ_LEASE_SECS=` (compose `${VAR:-}`) was an error; DRAIN_SECS=0 refused."""
    settings = InfraSettings.from_env({**STRICT, "LLMQ_LEASE_SECS": "", "LLMQ_DRAIN_SECS": "0"})
    assert (settings.lease_secs, settings.drain_secs) == (60.0, 0.0)


def test_r3cf1_nan_registry_is_a_config_error() -> None:
    """R3CF-1: a NaN in LLMQ_REGISTRY raised a raw orjson.JSONDecodeError from from_env."""
    with pytest.raises(ConfigError, match="NaN"):
        InfraSettings.from_env({**STRICT, "LLMQ_REGISTRY": '{"registry_version": NaN}'})
