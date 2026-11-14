"""Process-level settings (plan §3.18 infrastructure table), read from the environment.

Everything about models lives in the registry; these are the stores, prefixes, the registry's
source and a few process limits. Every problem is reported at once.
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import orjson
from redis.connection import parse_url, to_bool

from llm_queue.errors import ConfigError
from llm_queue.registry.validate import parse_document

PREFIX = "LLMQ_"
LOCAL_HOSTS = ("localhost", "127.0.0.1", "::1")
STORE_SCHEMES = ("redis", "rediss", "valkey", "valkeys")
TLS_SCHEMES = ("rediss", "valkeys")
METRICS_SINKS = ("emf", "prometheus", "none")
# The only environments where the store may skip verified TLS, ids may go unkeyed and debug
# capture may run. Anything else, `LLMQ_ENVIRONMENT` unset included, is treated as holding real
# data (fail closed): a forgotten variable must not switch the checks off.
DEV_ENVIRONMENTS = ("dev", "local", "test", "ci")
# What strict mode asks of a TLS store connection, written into the URL so every client built
# from it (the queue's, the CLI's) verifies the certificate and the host name explicitly.
_TLS_REQUIRED = {"ssl_cert_reqs": "required", "ssl_check_hostname": "true"}


def _store_url(name: str, url: str, strict: bool) -> tuple[str, list[str]]:
    """The store URL to use and what is wrong with it; `strict` requires verified TLS.

    The URL is read with redis-py's own parser (`parse_url`, `to_bool`), so the check sees what
    the client will do: redis-py takes the first of repeated query names, which a check of the
    last one missed, so repeats are refused outright.
    """
    parts = urlsplit(url)
    if parts.scheme not in STORE_SCHEMES:
        return url, [f"{name} must be a redis:// or rediss:// URL"]
    names = [key for key, _ in parse_qsl(parts.query, keep_blank_values=True)]
    if len(names) != len(set(names)):
        return url, [f"{name} repeats a query parameter"]
    if not strict:
        return url, []
    redis_url = "redis" + url[len("valkey") :] if url.startswith("valkey") else url
    try:
        options = parse_url(redis_url)  # type: ignore[no-untyped-call]  # redis-py untyped
    except ValueError as exc:
        return url, [f"{name}: {exc}"]
    if parts.scheme not in TLS_SCHEMES:
        if options.get("host", "") not in LOCAL_HOSTS:
            return url, [f"{name} must use TLS (rediss://) outside dev"]
        return url, []
    if str(options.get("ssl_cert_reqs", "required")).lower() in ("none", "optional"):
        return url, [f"{name} must verify the store's certificate outside dev (ssl_cert_reqs)"]
    hostname = to_bool(options.get("ssl_check_hostname", "true"))  # type: ignore[no-untyped-call]
    if hostname is False:
        return url, [f"{name} must check the store's host name outside dev (ssl_check_hostname)"]
    query = [*parse_qsl(parts.query, keep_blank_values=True)]
    query += [(k, v) for k, v in _TLS_REQUIRED.items() if k not in names]
    return urlunsplit(parts._replace(query=urlencode(query))), []


def _read_document(registry: str | None, registry_file: str | None) -> str:
    """The registry JSON from the variable or the file, refused when it repeats a key."""
    if registry:
        text = registry
    elif registry_file:
        try:
            text = Path(registry_file).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise ConfigError(f"LLMQ_REGISTRY_FILE cannot be read: {exc}") from exc
    else:
        raise ConfigError("no registry: set LLMQ_REGISTRY or LLMQ_REGISTRY_FILE")
    parse_document(text)  # RegistryError (a ConfigError) for bad JSON or a repeated key
    return text


@dataclass(frozen=True, slots=True)
class InfraSettings:
    """The process settings of one environment."""

    store_url: str = field(repr=False)  # holds the password
    queue_store_url: str | None = field(default=None, repr=False)
    store_max_connections: int = 200
    key_prefix: str = "llmq"
    queue_prefix: str = ""
    registry: str | None = None
    registry_file: str | None = None
    registry_live_edit: str = "safe"
    base_url_allowed_hosts: tuple[str, ...] = ()
    cfg_reload_secs: float = 30.0
    result_ttl_secs: int = 300
    result_dedupe_secs: int = 0
    status_ttl_secs: int = 1800
    audit_retention_days: int = 90
    lease_secs: float = 60.0
    caller_ttl_secs: int = 30
    drain_secs: float = 25.0  # inside the default Kubernetes grace period of 30 s
    local_limit_cap: int = 10
    metrics_sink: str = "emf"
    mode: str = "both"
    bedrock_endpoint_url: str | None = None
    sts_endpoint_url: str | None = None
    id_hash_salt: str | None = field(default=None, repr=False)
    environment: str = "unset"
    queue_failover_secs: float = 5.0
    queue_wait_replicas: int = 0
    queue_wait_ms: int = 100
    debug_capture_from_config: bool = False

    @property
    def strict(self) -> bool:
        """True outside the dev environments: verified TLS, keyed ids, no debug capture."""
        return self.environment not in DEV_ENVIRONMENTS

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        secrets: Callable[[str], str | None] | None = None,
    ) -> InfraSettings:
        """Read `LLMQ_*` from `environ` (default `os.environ`), secrets first when given."""
        env = dict(os.environ if environ is None else environ)

        def get(name: str, default: Any = None) -> Any:
            if secrets is not None:
                value = secrets(PREFIX + name)
                if value:
                    return value
            return env.get(PREFIX + name, default)

        problems: list[str] = []
        store = get("STORE_URL")
        environment = str(get("ENVIRONMENT", "") or "").strip().lower() or "unset"
        if environment == "production":
            environment = "prod"
        strict = environment not in DEV_ENVIRONMENTS
        if not store:
            problems.append("LLMQ_STORE_URL is required (redis:// or rediss://)")
        urls: dict[str, str | None] = {}
        for name in ("STORE_URL", "QUEUE_STORE_URL"):
            raw = get(name)
            urls[name] = None
            if raw:
                urls[name], found = _store_url(PREFIX + name, str(raw), strict)
                problems += found
        registry, registry_file = get("REGISTRY"), get("REGISTRY_FILE")
        hosts = tuple(
            h.strip() for h in str(get("BASE_URL_ALLOWED_HOSTS", "")).split(",") if h.strip()
        )
        if registry and registry_file:
            problems.append("set one of LLMQ_REGISTRY and LLMQ_REGISTRY_FILE, not both")
        elif registry or registry_file:
            try:
                document = orjson.loads(_read_document(registry, registry_file))
            except ConfigError as exc:
                problems.append(str(exc))
            else:
                accounts = document.get("accounts") if isinstance(document, dict) else None
                kinds = [a.get("kind") for a in (accounts or {}).values() if isinstance(a, dict)]
                if strict and "openai" in kinds and not hosts:
                    problems.append(
                        "LLMQ_BASE_URL_ALLOWED_HOSTS is required outside dev when the registry "
                        f"has an `openai` account ({environment})"
                    )
        salt = get("ID_HASH_SALT")
        if strict and not salt:
            problems.append(f"LLMQ_ID_HASH_SALT is required outside dev ({environment})")
        debug_capture = str(get("DEBUG_CAPTURE_FROM_CONFIG", "false")).strip().lower()
        if debug_capture not in ("true", "false", "1", "0", "yes", "no", "on", "off"):
            problems.append("LLMQ_DEBUG_CAPTURE_FROM_CONFIG must be true or false")
        capture = debug_capture in ("true", "1", "yes", "on")
        if capture and strict:
            problems.append(
                f"LLMQ_DEBUG_CAPTURE_FROM_CONFIG is refused outside dev ({environment}): it "
                "stores prompt text"
            )
        sink = str(get("METRICS_SINK", "emf"))
        if sink not in METRICS_SINKS:
            problems.append("LLMQ_METRICS_SINK must be emf, prometheus or none")
        live_edit = str(get("REGISTRY_LIVE_EDIT", "safe"))
        if live_edit not in ("safe", "all", "off"):
            problems.append("LLMQ_REGISTRY_LIVE_EDIT must be safe, all or off")
        mode = str(get("MODE", "both"))
        if mode not in ("both", "caller", "worker"):
            problems.append("LLMQ_MODE must be both, caller or worker")

        def number(name: str, default: float, kind: type = float, *, zero: bool = False) -> Any:
            raw = get(name, default)
            try:
                value = kind(raw)
            except (TypeError, ValueError):
                problems.append(f"LLMQ_{name} must be a number")
                return default
            if not math.isfinite(value):
                problems.append(f"LLMQ_{name} must be a finite number")
                return default
            if value < 0 or (value == 0 and not zero):
                problems.append(f"LLMQ_{name} must be {'0 or more' if zero else 'above 0'}")
            return value

        lease = number("LEASE_SECS", 60.0)
        caller_ttl = number("CALLER_TTL_SECS", 30, int)
        # The caller key is renewed every lease / 3; it must outlive one beat with room for a
        # slow store, or a live caller is taken for gone and its calls are failed.
        beat = lease / 3
        if caller_ttl < beat + max(5.0, beat / 2):
            problems.append(
                f"LLMQ_CALLER_TTL_SECS ({caller_ttl}) must be at least "
                f"{math.ceil(beat + max(5.0, beat / 2))}: the heartbeat renews it every "
                f"LLMQ_LEASE_SECS / 3 ({beat:g} s), plus a margin"
            )
        settings = cls(
            store_url=urls["STORE_URL"] or str(store or ""),
            queue_store_url=urls["QUEUE_STORE_URL"],
            store_max_connections=number("STORE_MAX_CONNECTIONS", 200, int),
            key_prefix=str(get("KEY_PREFIX", "llmq")),
            queue_prefix=str(get("QUEUE_PREFIX", "")),
            registry=registry,
            registry_file=registry_file,
            registry_live_edit=live_edit,
            base_url_allowed_hosts=hosts,
            cfg_reload_secs=number("CFG_RELOAD_SECS", 30.0),
            result_ttl_secs=number("RESULT_TTL_SECS", 300, int),
            result_dedupe_secs=number("RESULT_DEDUPE_SECS", 0, int, zero=True),
            status_ttl_secs=number("STATUS_TTL_SECS", 1800, int),
            audit_retention_days=number("AUDIT_RETENTION_DAYS", 90, int),
            lease_secs=lease,
            caller_ttl_secs=caller_ttl,
            drain_secs=number("DRAIN_SECS", 25.0),
            local_limit_cap=number("LOCAL_LIMIT_CAP", 10, int),
            metrics_sink=sink,
            mode=mode,
            bedrock_endpoint_url=get("BEDROCK_ENDPOINT_URL"),
            sts_endpoint_url=get("STS_ENDPOINT_URL"),
            id_hash_salt=salt,
            environment=environment,
            queue_failover_secs=number("QUEUE_FAILOVER_SECS", 5.0, zero=True),
            queue_wait_replicas=number("QUEUE_WAIT_REPLICAS", 0, int, zero=True),
            queue_wait_ms=number("QUEUE_WAIT_MS", 100, int),
            debug_capture_from_config=capture,
        )
        if problems:
            raise ConfigError("invalid LLMQ settings:\n  " + "\n  ".join(problems))
        return settings

    def registry_document(self) -> str:
        """The registry JSON from `LLMQ_REGISTRY` or the file `LLMQ_REGISTRY_FILE`.

        `ConfigError` when neither is set, the file cannot be read, the text is not JSON or an
        object repeats a key.
        """
        return _read_document(self.registry, self.registry_file)
