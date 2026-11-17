"""0.2.0 D1, D3, D7, H7, H8: settings layers, the store from parts, the pool size `auto`,
secrets under other names and `embed_one`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import pytest

import llm_queue.core as core
from llm_queue import ConfigError, LLMQueue
from llm_queue.core import EmbedResult
from llm_queue.engine.envelope import Meta
from llm_queue.engine.settings import STORE_POOL_MARGIN, InfraSettings, Layers

GPT = "openai.gpt-oss-120b-1:0"
TITAN = "amazon.titan-embed-text-v2:0"
DOC: dict[str, Any] = {
    "registry_version": 1,
    "accounts": {"aws-a": {"kind": "aws"}},
    "models": {
        "gpt120b": {
            "type": "chat",
            "provider": "bedrock",
            "model_id": GPT,
            "worker_slots": 30,
            "deployments": {"aws-a": {"confirmed": ["ap-south-1"]}},
        },
        "titan": {
            "type": "embedding",
            "provider": "bedrock",
            "model_id": TITAN,
            "deployments": {"aws-a": {"confirmed": ["ap-south-1"]}},
        },
    },
}
DEV = {"LLMQ_ENVIRONMENT": "dev", "LLMQ_REGISTRY": json.dumps(DOC)}
URL = "redis://localhost:6379/0"
STRICT = {"LLMQ_ENVIRONMENT": "prod", "LLMQ_ID_HASH_SALT": "s" * 16, "LLMQ_KEY_PREFIX": "app"}


def _problems(**kw: Any) -> str:
    with pytest.raises(ConfigError) as info:
        InfraSettings.from_env(**kw)
    return str(info.value)


# D1: layers -----------------------------------------------------------------------------------


def test_layers_order_is_secret_then_environ_then_defaults() -> None:
    layers = Layers(
        {"LLMQ_A": "env", "LLMQ_B": "env", "LLMQ_E": ""},
        {"LLMQ_A": "secret"}.get,
        {"LLMQ_A": "default", "LLMQ_B": "default", "LLMQ_C": "default", "LLMQ_D": True},
    )
    assert layers.get("LLMQ_A") == "secret"
    assert layers.get("LLMQ_B") == "env"
    assert layers.get("LLMQ_C") == "default"
    assert layers.get("LLMQ_D") == "true"  # a bool reads as a switch value
    assert layers.get("LLMQ_E", "x") == "x"  # empty counts as unset
    assert set(layers) == {"LLMQ_A", "LLMQ_B", "LLMQ_E", "LLMQ_C", "LLMQ_D"}
    with pytest.raises(KeyError):
        layers["LLMQ_MISSING"]


def test_defaults_fill_settings_below_environ_and_secrets() -> None:
    settings = InfraSettings.from_env(
        {"LLMQ_STORE_URL": URL, **DEV, "LLMQ_LEASE_SECS": "30"},
        secrets={"LLMQ_KEY_PREFIX": "from-secret"}.get,
        defaults={
            "LLMQ_LEASE_SECS": 90,
            "LLMQ_KEY_PREFIX": "from-default",
            "LLMQ_DEFAULT_TENANT": "acme",
        },
    )
    assert (settings.lease_secs, settings.key_prefix) == (30, "from-secret")
    assert settings.default_tenant == "acme"


def test_defaults_and_secret_names_must_name_llmq_settings() -> None:
    assert "'KEY_PREFIX' is not an LLMQ_* setting" in _problems(
        environ={"LLMQ_STORE_URL": URL, **DEV}, defaults={"KEY_PREFIX": "x"}
    )
    text = _problems(
        environ={"LLMQ_STORE_URL": URL, **DEV}, secret_names={"STORE_HOST": "h", "LLMQ_X": ""}
    )
    assert "'STORE_HOST' is not an LLMQ_* setting" in text
    assert "LLMQ_X must name a secret" in text


def test_a_higher_registry_file_drops_a_lower_registry(tmp_path: Path) -> None:
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(DOC), encoding="utf-8")
    settings = InfraSettings.from_env(
        {"LLMQ_STORE_URL": URL, "LLMQ_ENVIRONMENT": "dev", "LLMQ_REGISTRY_FILE": str(path)},
        defaults={"LLMQ_REGISTRY": '{"registry_version": 1}'},
    )
    assert (settings.registry, settings.registry_file) == (None, str(path))
    # And the other way: a secret LLMQ_REGISTRY drops the environment's file.
    settings = InfraSettings.from_env(
        {"LLMQ_STORE_URL": URL, "LLMQ_ENVIRONMENT": "dev", "LLMQ_REGISTRY_FILE": str(path)},
        secrets={"LLMQ_REGISTRY": json.dumps(DOC)}.get,
    )
    assert settings.registry_file is None
    assert settings.registry == json.dumps(DOC)
    # Both in one layer is refused, as before.
    assert "not both" in _problems(
        environ={"LLMQ_STORE_URL": URL, **DEV, "LLMQ_REGISTRY_FILE": str(path)}
    )


def test_store_url_and_store_parts_are_one_conflict_group() -> None:
    # A secret host beats the environment's URL; the URL is ignored, not refused.
    settings = InfraSettings.from_env(
        {"LLMQ_STORE_URL": "redis://other:1/0", **DEV, "LLMQ_STORE_TLS_ENABLED": "false"},
        secrets={"LLMQ_STORE_HOST": "valkey.internal"}.get,
    )
    assert settings.store_url == "redis://valkey.internal:6379/0"
    # The environment's URL beats parts in `defaults`.
    settings = InfraSettings.from_env(
        {"LLMQ_STORE_URL": URL, **DEV},
        defaults={"LLMQ_STORE_HOST": "x", "LLMQ_STORE_PORT": 1, "LLMQ_STORE_TLS_ENABLED": False},
    )
    assert settings.store_url == URL
    assert (settings.store_host, settings.store_port, settings.store_tls_enabled) == (
        None,
        6379,
        True,
    )
    # A part in a higher layer than the URL picks the parts: the URL is dropped, a host needed.
    text = _problems(environ={**DEV, "LLMQ_STORE_PORT": "6380"}, defaults={"LLMQ_STORE_URL": URL})
    assert "LLMQ_STORE_URL (or LLMQ_STORE_HOST) is required" in text
    # One layer with both is refused.
    text = _problems(environ={"LLMQ_STORE_URL": URL, **DEV, "LLMQ_STORE_HOST": "h"})
    assert "set LLMQ_STORE_URL or the store parts (LLMQ_STORE_HOST" in text


# H7: secret_names ------------------------------------------------------------------------------


def test_secret_names_read_settings_from_secrets_under_other_names() -> None:
    asked: list[str] = []
    vault = {"valkey_host": "cache.internal", "valkey_port": "6380", "LLMQ_STORE_HOST": "wrong"}

    def secrets(name: str) -> str | None:
        asked.append(name)
        return vault.get(name)

    settings = InfraSettings.from_env(
        {**DEV, "LLMQ_STORE_TLS_ENABLED": "false", "LLMQ_STORE_PORT": "1"},
        secrets=secrets,
        secret_names={"LLMQ_STORE_HOST": "valkey_host", "LLMQ_STORE_PORT": "valkey_port"},
    )
    assert settings.store_url == "redis://cache.internal:6380/0"  # the secret beats the env
    assert "LLMQ_STORE_HOST" not in asked  # the mapped name only


def test_secret_names_apply_to_the_queue_settings_password_and_admin_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients: list[tuple[str, str | None]] = []
    real = core.secret_store_client

    def spy(url: str, lookup: Any, name: str, *args: Any) -> Any:
        clients.append((name, lookup(name)))
        return real(url, lookup, name, *args)  # lazy: no connection is made

    monkeypatch.setattr(core, "secret_store_client", spy)
    vault = {"vk_pass": "pw", "admin": "tok", "jobs": "7"}
    llmq = LLMQueue.from_env(
        {"LLMQ_STORE_URL": URL, **DEV},
        secrets=vault.get,
        secret_names={
            "LLMQ_STORE_PASSWORD": "vk_pass",
            "LLMQ_ADMIN_TOKEN": "admin",
            "LLMQ_JOB_CONCURRENCY": "jobs",
        },
    )
    assert clients == [("LLMQ_STORE_PASSWORD", "pw")]
    assert llmq._admin_token() == "tok"
    assert llmq._jobs.limits.concurrency == 7


# D3: the store from parts ----------------------------------------------------------------------


def test_store_parts_build_the_url_with_an_escaped_password() -> None:
    settings = InfraSettings.from_env(
        {
            **DEV,
            "LLMQ_STORE_HOST": "cache.internal",
            "LLMQ_STORE_PORT": "6380",
            "LLMQ_STORE_DB": "3",
            "LLMQ_STORE_TLS_ENABLED": "false",
            "LLMQ_STORE_PASSWORD": "p@ss/w:rd%",
        }
    )
    parts = urlsplit(settings.store_url)
    assert (parts.scheme, parts.hostname, parts.port, parts.path) == (
        "redis",
        "cache.internal",
        6380,
        "/3",
    )
    assert unquote(parts.password or "") == "p@ss/w:rd%"
    assert (settings.store_host, settings.store_port, settings.store_db) == (
        "cache.internal",
        6380,
        3,
    )
    ipv6 = InfraSettings.from_env({**DEV, "LLMQ_STORE_HOST": "::1"})
    assert ipv6.store_url.startswith("rediss://[::1]:6379/0")


def test_store_parts_follow_the_strict_tls_rule() -> None:
    text = _problems(
        environ={**STRICT, "LLMQ_STORE_HOST": "cache.example.com", "LLMQ_STORE_TLS_ENABLED": "no"}
    )
    assert "the store from LLMQ_STORE_HOST must use TLS (rediss://) outside dev" in text
    settings = InfraSettings.from_env({**STRICT, "LLMQ_STORE_HOST": "cache.example.com"})
    assert settings.store_url.startswith("rediss://cache.example.com:6379/0?")
    assert "ssl_cert_reqs=required" in settings.store_url
    assert "LLMQ_STORE_TLS_ENABLED must be true or false" in _problems(
        environ={**DEV, "LLMQ_STORE_HOST": "h", "LLMQ_STORE_TLS_ENABLED": "maybe"}
    )


def test_store_password_from_the_environment_reaches_the_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """0.1.1 ignored LLMQ_STORE_PASSWORD from plain env when `secrets` was None (D3)."""
    lookups: list[Any] = []
    real = core.secret_store_client

    def spy(url: str, lookup: Any, name: str, *args: Any) -> Any:
        lookups.append(lookup)
        return real(url, lookup, name, *args)

    monkeypatch.setattr(core, "secret_store_client", spy)
    LLMQueue.from_env({"LLMQ_STORE_URL": URL, **DEV, "LLMQ_STORE_PASSWORD": "env-pw"})
    assert lookups[0]("LLMQ_STORE_PASSWORD") == "env-pw"
    rotated = {"LLMQ_STORE_PASSWORD": "v1"}
    LLMQueue.from_env(
        {"LLMQ_STORE_URL": URL, **DEV, "LLMQ_STORE_PASSWORD": "env-pw"}, secrets=rotated.get
    )
    rotated["LLMQ_STORE_PASSWORD"] = "v2"  # read on every new connection (FreshPassword)
    assert lookups[1]("LLMQ_STORE_PASSWORD") == "v2"


def test_store_password_from_env_logs_in_through_a_credential_provider() -> None:
    from llm_queue.engine.store_client import FreshPassword, secret_store_client
    from llm_queue.tuning import StoreOptions

    layers = Layers({"LLMQ_STORE_PASSWORD": "env-pw"})
    client = secret_store_client(URL, layers.get, "LLMQ_STORE_PASSWORD", 4, StoreOptions())
    provider = client.connection_pool.connection_kwargs["credential_provider"]
    assert isinstance(provider, FreshPassword)
    assert provider.get_credentials() == ("env-pw",)


# D7: pool size auto ------------------------------------------------------------------------------


def test_store_max_connections_auto_is_worker_slots_plus_margin() -> None:
    settings = InfraSettings.from_env(
        {"LLMQ_STORE_URL": URL, **DEV, "LLMQ_STORE_MAX_CONNECTIONS": "AUTO"}
    )
    # 30 + 20 (the default worker_slots), plus the margin.
    assert settings.store_max_connections == 50 + STORE_POOL_MARGIN
    assert settings.store_max_connections_auto
    plain = InfraSettings.from_env({"LLMQ_STORE_URL": URL, **DEV})
    assert (plain.store_max_connections, plain.store_max_connections_auto) == (200, False)
    assert "LLMQ_STORE_MAX_CONNECTIONS=auto needs LLMQ_REGISTRY" in _problems(
        environ={"LLMQ_STORE_URL": URL, "LLMQ_ENVIRONMENT": "dev",
                 "LLMQ_STORE_MAX_CONNECTIONS": "auto"}
    )  # fmt: skip
    assert "needs a valid registry" in _problems(
        environ={"LLMQ_STORE_URL": URL, "LLMQ_ENVIRONMENT": "dev",
                 "LLMQ_REGISTRY": '{"registry_version": 1, "models": {"x": {}}}',
                 "LLMQ_STORE_MAX_CONNECTIONS": "auto"}
    )  # fmt: skip


# H8: embed_one -------------------------------------------------------------------------------


class _Embedder(LLMQueue):
    def __init__(self) -> None:
        self.seen: list[tuple[Any, ...]] = []

    async def embed(self, model: str, texts: Any, **kw: Any) -> EmbedResult:  # type: ignore[override]
        self.seen.append((model, list(texts), kw))
        return EmbedResult([[0.5, 0.25]], Meta())

    def embed_sync(self, model: str, texts: Any, **kw: Any) -> EmbedResult:  # type: ignore[override]
        self.seen.append((model, list(texts), kw))
        return EmbedResult([[1.0]], Meta())


async def test_embed_one_returns_the_one_vector() -> None:
    llmq = _Embedder()
    assert await llmq.embed_one("titan", "hello", input_type="search_query") == [0.5, 0.25]
    assert llmq.embed_one_sync("titan", "hi", priority=True) == [1.0]
    assert llmq.seen == [
        ("titan", ["hello"], {"priority": None, "input_type": "search_query"}),
        ("titan", ["hi"], {"priority": True}),
    ]
