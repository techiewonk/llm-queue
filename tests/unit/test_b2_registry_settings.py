"""0.1.0b2 process settings (RESCAN X1, X5, X6, X8, X10, R7, R10; plan C3 drain default).

Each case is an environment 0.1.0b1 accepted although it ran without its safety checks, or a
bad value it let through to a raw error later.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from llm_queue import ConfigError, Tuning
from llm_queue.engine.settings import InfraSettings

# What strict mode needs besides TLS: the id salt and an explicit key prefix (Q26).
SALT = {"LLMQ_ID_HASH_SALT": "s", "LLMQ_KEY_PREFIX": "app-prod"}
TLS = "rediss://cache.example.com:6379/0"


def _settings(**env: str) -> InfraSettings:
    return InfraSettings.from_env({"LLMQ_STORE_URL": TLS, **SALT, **env})


def _refused(fragment: str, env: dict[str, str]) -> None:
    with pytest.raises(ConfigError, match=fragment):
        InfraSettings.from_env(env)


def test_unset_environment_is_strict() -> None:
    """X1: no LLMQ_ENVIRONMENT meant dev: no TLS and no salt checks."""
    _refused("LLMQ_ID_HASH_SALT is required", {"LLMQ_STORE_URL": "redis://localhost:6379/0"})
    _refused("must use TLS", {"LLMQ_STORE_URL": "redis://cache.example.com:6379/0", **SALT})
    dev = InfraSettings.from_env(
        {"LLMQ_STORE_URL": "redis://cache.example.com:6379/0", "LLMQ_ENVIRONMENT": "ci"}
    )
    assert dev.strict is False
    assert _settings().environment == "unset"
    assert _settings().strict is True


@pytest.mark.parametrize(
    "query",
    [
        "ssl_cert_reqs=none&ssl_cert_reqs=required",  # redis-py reads the first one
        "ssl_check_hostname=f",  # to_bool: F is false
        "ssl_check_hostname=N",
        "ssl_cert_reqs=OPTIONAL",
    ],
)
def test_tls_check_reads_the_url_like_redis(query: str) -> None:
    """X5: the check read the last repeated value and only knew false/0/no."""
    _refused("LLMQ_STORE_URL", {"LLMQ_STORE_URL": f"{TLS}?{query}", **SALT})


def test_strict_tls_is_written_into_the_url() -> None:
    settings = _settings()
    assert "ssl_cert_reqs=required" in settings.store_url
    assert "ssl_check_hostname=true" in settings.store_url
    queue = _settings(LLMQ_QUEUE_STORE_URL="valkeys://q.example.com:6379/0?ssl_ca_certs=/ca.pem")
    assert queue.queue_store_url is not None
    assert queue.queue_store_url.startswith("valkeys://q.example.com:6379/0?ssl_ca_certs=")
    assert "ssl_check_hostname=true" in queue.queue_store_url


def _openai_registry() -> str:
    return json.dumps(
        {
            "registry_version": 1,
            "accounts": {
                "v": {"kind": "openai", "base_url": "https://llm.example.com/v1"},
            },
            "models": {},
        }
    )


def test_openai_account_needs_allowed_hosts_outside_dev() -> None:
    """X6: without an allow-list, a registry edit could point base_url anywhere."""
    with pytest.raises(ConfigError, match="LLMQ_BASE_URL_ALLOWED_HOSTS is required"):
        _settings(LLMQ_REGISTRY=_openai_registry())
    ok = _settings(LLMQ_REGISTRY=_openai_registry(), LLMQ_BASE_URL_ALLOWED_HOSTS="llm.example.com")
    assert ok.base_url_allowed_hosts == ("llm.example.com",)


@pytest.mark.parametrize("environment", ["uat", "staging", "prod", ""])
def test_debug_capture_refused_outside_dev(environment: str) -> None:
    """X8: only the exact name `prod` refused debug capture (prompt text)."""
    with pytest.raises(ConfigError, match="DEBUG_CAPTURE_FROM_CONFIG is refused"):
        _settings(LLMQ_ENVIRONMENT=environment, LLMQ_DEBUG_CAPTURE_FROM_CONFIG="true")


def test_salt_is_not_in_repr() -> None:
    assert "pepper" not in repr(_settings(LLMQ_ID_HASH_SALT="pepper"))


def test_caller_ttl_must_outlive_a_heartbeat() -> None:
    """R7: lease 90 s beats every 30 s; a 30 s caller key expired between beats."""
    with pytest.raises(ConfigError, match="LLMQ_CALLER_TTL_SECS"):
        _settings(LLMQ_LEASE_SECS="90", LLMQ_CALLER_TTL_SECS="30")
    assert _settings().caller_ttl_secs == 30
    assert _settings().drain_secs == 25.0


def test_registry_source_problems_are_config_errors(tmp_path: Path) -> None:
    """R10: a bad LLMQ_REGISTRY_FILE raised a raw OSError; repeated keys passed silently."""
    with pytest.raises(ConfigError, match="LLMQ_REGISTRY_FILE cannot be read"):
        _settings(LLMQ_REGISTRY_FILE=str(tmp_path / "missing.json"))
    with pytest.raises(ConfigError, match="repeats keys"):
        _settings(LLMQ_REGISTRY='{"registry_version": 1, "registry_version": 2}')
    path = tmp_path / "reg.json"
    path.write_text('{"registry_version": 1}', encoding="utf-8")
    settings = _settings(LLMQ_REGISTRY_FILE=str(path))
    path.unlink()
    with pytest.raises(ConfigError, match="cannot be read"):
        settings.registry_document()


@pytest.mark.parametrize("part", ["store", "bedrock", "engine", "registry"])
def test_tuning_part_of_the_wrong_type_is_a_problem(part: str) -> None:
    """R10: `Tuning(store=None)` raised a raw KeyError instead of a listed problem."""
    bad: dict[str, Any] = {part: None}
    problems = Tuning(**bad).problems()
    assert problems == [
        f"{part} must be {type(getattr(Tuning(), part)).__name__}(...), not NoneType"
    ]
