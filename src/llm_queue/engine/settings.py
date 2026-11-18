"""Process-level settings (plan §3.18 infrastructure table), read from the environment.

Everything about models lives in the registry; these are the stores, prefixes, the registry's
source and a few process limits. Every problem is reported at once.
"""

from __future__ import annotations

import math
import os
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import orjson
from redis.connection import parse_url, to_bool

from llm_queue._logging import get_logger
from llm_queue.engine.expiry import KEY_EXPIRY_MODES
from llm_queue.engine.store_guard import WAIT_SCOPES, parse_scopes
from llm_queue.errors import ConfigError
from llm_queue.ids import keyed_hash
from llm_queue.registry.interpolate import YAML_SUFFIXES, expand, load_yaml, to_json
from llm_queue.registry.validate import parse_document

logger = get_logger(__name__)

PREFIX = "LLMQ_"
LOCAL_HOSTS = ("localhost", "127.0.0.1", "::1")
STORE_SCHEMES = ("redis", "rediss", "valkey", "valkeys")
TLS_SCHEMES = ("rediss", "valkeys")
METRICS_SINKS = ("emf", "prometheus", "none")
EXECUTION_MODES = ("queue", "local")  # LLMQ_EXECUTION_MODE
# The only environments where the store may skip verified TLS, ids may go unkeyed and debug
# capture may run. Anything else, `LLMQ_ENVIRONMENT` unset included, is treated as holding real
# data (fail closed): a forgotten variable must not switch the checks off.
DEV_ENVIRONMENTS = ("dev", "local", "test", "ci")
# What strict mode asks of a TLS store connection, written into the URL so every client built
# from it (the queue's, the CLI's) verifies the certificate and the host name explicitly.
_TLS_REQUIRED = {"ssl_cert_reqs": "required", "ssl_check_hostname": "true"}
_SCHEMES_TEXT = "a redis://, rediss://, valkey:// or valkeys:// URL"
# `hash_id` keys ids such as phone numbers with this salt to stop brute force; a one-character
# salt does not (and the hash is cut to 64 bits), so strict mode wants at least this (FO-13).
MIN_SALT_CHARS = 16
# Key and queue prefixes become part of every store key; `purge removed` splits keys on `{m:` /
# `{q:`, so braces and glob characters in a prefix mis-attributed keys (FO-20). A colon is
# fine in the key prefix (`llmq:prod:v3` is the documented and deployed form: every key is
# `<prefix>:...`); the queue prefix, which goes inside `{q:...}` hash tags, stays plain.
_PREFIX_RE = re.compile(r"^[A-Za-z0-9_.-]{0,64}\Z")
_KEY_PREFIX_RE = re.compile(r"^[A-Za-z0-9_.:-]{0,64}\Z")
INT32_MAX = 2**31 - 1  # the longest TTL `EXPIRE` takes on every store engine (R3CF-14)
# Store connections a worker needs beyond its slots: the receive loop, the result subscriber,
# heartbeats and status reads (R3PF-8). Unverified: an estimate from the loops it runs.
STORE_POOL_MARGIN = 10
# The store given as parts (0.2.0 D3) instead of `LLMQ_STORE_URL` (names without `LLMQ_`).
STORE_PARTS = ("STORE_HOST", "STORE_PORT", "STORE_DB", "STORE_TLS_ENABLED")
# Settings that name one thing two ways (0.2.0 D1): the highest layer that sets any name of a
# group picks the side, and the other side's names are dropped from every layer, so a default
# `LLMQ_REGISTRY` never clashes with an environment's `LLMQ_REGISTRY_FILE`. One layer that
# sets both sides is refused.
CONFLICT_GROUPS: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...] = (
    (("STORE_URL",), STORE_PARTS),
    (("REGISTRY",), ("REGISTRY_FILE",)),
)
_TRUE, _FALSE = ("true", "1", "yes", "on"), ("false", "0", "no", "off")
# Settings that no longer exist (review B-15, owner RV4). One still set in the environment or
# `defaults=` is reported at start: a `ConfigError` when strict, else a `setting_renamed` /
# `setting_removed` warning, so an old manifest never silently gets the default. Old name ->
# what to do instead.
REMOVED: dict[str, str] = {
    "LLMQ_TENANT_INDEX": "tenant purges read each model's request index "
    "(LLMQ_REQUEST_INDEX_ENABLED)",
}
# Old name -> new name: `RENAMED` is reported as above. `PENDING_RENAMES` is the names standard
# (`LLMQ_<AREA>_<THING>[_<UNIT>]`, environment.md "Names") not applied yet: the code still reads
# its old names. `scripts/rename_settings.py` rewrites every read, doc, test, sample and manifest
# from it and moves it into `RENAMED`; it rewrites this block, so keep it plain.
# rename-table: begin
RENAMED: dict[str, str] = {
    "LLMQ_LOCAL_LIMIT_CAP": "LLMQ_LOCAL_CONCURRENCY",
    "LLMQ_PURGE_BATCH": "LLMQ_ADMIN_BATCH",
}
PENDING_RENAMES: dict[str, str] = {
    "LLMQ_AUTO_PURGE": "LLMQ_PURGE_AUTO_ENABLED",
    "LLMQ_PURGE_WAIT": "LLMQ_PURGE_WAIT_ENABLED",
    "LLMQ_HTTP_PURGE_WAIT_SECS": "LLMQ_ADMIN_HTTP_WAIT_SECS",
    "LLMQ_TENANT_PURGE_DEFAULT": "LLMQ_PURGE_TENANT_REMOVE",
    "LLMQ_QUEUE_WAIT_REPLICAS": "LLMQ_REPLICA_WAIT_COUNT",
    "LLMQ_QUEUE_WAIT_MS": "LLMQ_REPLICA_WAIT_MS",
    "LLMQ_QUEUE_WAIT_SCOPES": "LLMQ_REPLICA_WAIT_SCOPES",
    "LLMQ_QUEUE_FAILOVER_SECS": "LLMQ_QUEUE_STORE_FAILOVER_SECS",
    "LLMQ_CFG_RELOAD_SECS": "LLMQ_REGISTRY_RELOAD_SECS",
    "LLMQ_KEY_EXPIRY": "LLMQ_KEY_EXPIRY_MODE",
    "LLMQ_TTL_SWEEP_BATCH": "LLMQ_KEY_EXPIRY_SWEEP_BATCH",
    "LLMQ_TTL_SWEEP_ROUNDS": "LLMQ_KEY_EXPIRY_SWEEP_ROUNDS",
    "LLMQ_REGISTRY_LIVE_EDIT": "LLMQ_REGISTRY_LIVE_EDIT_MODE",
    "LLMQ_DEBUG_CAPTURE_FROM_CONFIG": "LLMQ_DEBUG_CAPTURE_ENABLED",
    "LLMQ_BASE_URL_ALLOWED_HOSTS": "LLMQ_HTTP_ALLOWED_HOSTS",
    "LLMQ_MODE": "LLMQ_ROLE",
    "LLMQ_WORKER_ID": "LLMQ_INSTANCE_ID",
    "LLMQ_DEFAULT_TENANT": "LLMQ_TENANT_DEFAULT",
    "LLMQ_ADMIN_DLQ_BODIES_ENABLED": "LLMQ_ADMIN_DEAD_LETTER_BODIES_ENABLED",
    "LLMQ_SECRET_NAMES": "LLMQ_SECRETS_NAMES",
}
# rename-table: end
# Every process setting (`LLMQ_JOB_*` and the `LLMQ_ACCOUNT_*` secrets aside): environment.md
# documents exactly these, a test checks each is read, and a registry `*_ref` may not name one.
SETTING_NAMES = frozenset(
    {
        "LLMQ_ADMIN_ACTOR_HEADER",
        "LLMQ_ADMIN_BATCH",
        "LLMQ_ADMIN_BODY_MAX_BYTES",
        "LLMQ_ADMIN_CSRF_HEADER",
        "LLMQ_ADMIN_DLQ_BODIES_ENABLED",
        "LLMQ_ADMIN_HEADER",
        "LLMQ_ADMIN_INDEX_CACHE_SECS",
        "LLMQ_ADMIN_READ_TOKEN",
        "LLMQ_ADMIN_SCAN_MAX",
        "LLMQ_ADMIN_TOKEN",
        "LLMQ_ADMIN_WRITES_ENABLED",
        "LLMQ_AUDIT_RETENTION_DAYS",
        "LLMQ_AUTO_PURGE",
        "LLMQ_BASE_URL_ALLOWED_HOSTS",
        "LLMQ_BEDROCK_ENDPOINT_URL",
        "LLMQ_CALLER_TTL_SECS",
        "LLMQ_CFG_RELOAD_SECS",
        "LLMQ_DEBUG_CAPTURE_FROM_CONFIG",
        "LLMQ_DEFAULT_TENANT",
        "LLMQ_DRAIN_FILE",
        "LLMQ_DRAIN_HANDBACK_ENABLED",
        "LLMQ_DRAIN_POLL_SECS",
        "LLMQ_DRAIN_SECS",
        "LLMQ_ENABLED",
        "LLMQ_ENVIRONMENT",
        "LLMQ_EXECUTION_MODE",
        "LLMQ_HTTP_PURGE_WAIT_SECS",
        "LLMQ_ID_HASH_SALT",
        "LLMQ_KEY_EXPIRY",
        "LLMQ_KEY_PREFIX",
        "LLMQ_LEASE_SECS",
        "LLMQ_LOCAL_CONCURRENCY",
        "LLMQ_LOCAL_PROCESS_COUNT",
        "LLMQ_METRICS_SINK",
        "LLMQ_METRICS_TENANTS",
        "LLMQ_MODE",
        "LLMQ_PURGE_GRACE_SECS",
        "LLMQ_PURGE_WAIT",
        "LLMQ_PURGE_WAIT_SECS",
        "LLMQ_QUEUE_BACKFILL_ENABLED",
        "LLMQ_QUEUE_FAILOVER_SECS",
        "LLMQ_QUEUE_PREFIX",
        "LLMQ_QUEUE_STORE_PASSWORD",
        "LLMQ_QUEUE_STORE_URL",
        "LLMQ_QUEUE_WAIT_MS",
        "LLMQ_QUEUE_WAIT_REPLICAS",
        "LLMQ_QUEUE_WAIT_SCOPES",
        "LLMQ_REGISTRY",
        "LLMQ_REGISTRY_FILE",
        "LLMQ_REGISTRY_LIVE_EDIT",
        "LLMQ_REQUESTS_LIST_MAX",
        "LLMQ_REQUESTS_MOVE_BATCH",
        "LLMQ_REQUESTS_MOVE_BYTES",
        "LLMQ_REQUEST_INDEX_ENABLED",
        "LLMQ_RESULT_DEDUPE_SECS",
        "LLMQ_RESULT_TTL_SECS",
        "LLMQ_SECRETS_CACHE_SECS",
        "LLMQ_SECRETS_REGION",
        "LLMQ_SECRETS_SOURCE",
        "LLMQ_SECRETS_TIMEOUT_SECS",
        "LLMQ_SECRET_NAMES",
        "LLMQ_STATIC_CAP_WAIT_SECS",
        "LLMQ_STATUS_TTL_SECS",
        "LLMQ_STORE_DB",
        "LLMQ_STORE_HOST",
        "LLMQ_STORE_MAX_CONNECTIONS",
        "LLMQ_STORE_PASSWORD",
        "LLMQ_STORE_PORT",
        "LLMQ_STORE_TLS_ENABLED",
        "LLMQ_STORE_URL",
        "LLMQ_STS_ENDPOINT_URL",
        "LLMQ_SYNC_STORE_MAX_CONNECTIONS",
        "LLMQ_TENANT_NAMES_ENABLED",
        "LLMQ_TENANT_PURGE_DEFAULT",
        "LLMQ_TTL_SWEEP_BATCH",
        "LLMQ_TTL_SWEEP_ROUNDS",
        "LLMQ_WORKER_ID",
    }
)
# Where secrets come from is never itself read from a secret (llm_queue.secrets.SETTINGS).
_SECRETS_SETTINGS = frozenset(
    PREFIX + n
    for n in (
        "SECRETS_SOURCE",
        "SECRET_NAMES",
        "SECRETS_REGION",
        "SECRETS_CACHE_SECS",
        "SECRETS_TIMEOUT_SECS",
    )
)


def _set(value: Any) -> str | None:
    """`value` as a setting's text, None when unset or empty (an empty value counts as unset)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value)
    return text if text.strip() else None


class Layers(Mapping[str, str]):
    """`LLMQ_*` values from three layers: secrets, then `environ`, then `defaults` (0.2.0 D1).

    Secrets are `secrets`, else `LLMQ_SECRETS_SOURCE`'s sources, read on every lookup (a
    rotation needs no restart); docs/reference/environment.md "Where values come from".
    """

    # `secret_names` and `LLMQ_SECRET_NAMES` (merged, the argument wins) map a setting to its
    # key in the secrets (0.2.0 H7). The secrets settings themselves are read from the
    # environment and the defaults only. Settings that name one thing two ways follow
    # `CONFLICT_GROUPS`. A mapping, so `JobSettings.from_env` reads it.

    def __init__(
        self,
        environ: Mapping[str, str] | None = None,
        secrets: Callable[[str], str | None] | None = None,
        defaults: Mapping[str, Any] | None = None,
        secret_names: Mapping[str, str] | None = None,
    ) -> None:
        """Read `environ` (default `os.environ`, copied now) and the other layers.

        A configured secrets source is read once here: `ConfigError` names one that cannot be.
        """
        from llm_queue import secrets as sources

        problems = [
            f"defaults: {name!r} is not an LLMQ_* setting"
            for name in defaults or {}
            if not str(name).startswith(PREFIX)
        ]
        self._env = dict(os.environ if environ is None else environ)
        raw_defaults = dict(defaults or {})

        def setting(name: str) -> Any:
            value = _set(self._env.get(name))
            return value if value is not None else raw_defaults.get(name)

        names, found = sources.parse_secret_names(setting(sources.NAMES_VAR))
        problems += found
        for name, secret in (secret_names or {}).items():
            if not str(secret or "").strip():
                problems.append(f"secret_names: {name} must name a secret")
            else:  # stripped as LLMQ_SECRET_NAMES is: `" valkey_host"` names `valkey_host`
                names[str(name).strip()] = str(secret).strip()
        for name in names:
            if not name.startswith(PREFIX):
                problems.append(f"secret names: {name!r} is not an LLMQ_* setting")
            elif name in sources.SETTINGS:
                problems.append(f"secret names: {name} is read from the environment, not a secret")
        cache = sources.read_number(
            setting(sources.CACHE_VAR),
            sources.CACHE_VAR,
            sources.CACHE_SECS,
            0,
            sources.MAX_CACHE_SECS,
            problems,
        )
        timeout = sources.read_number(
            setting(sources.TIMEOUT_VAR),
            sources.TIMEOUT_VAR,
            sources.TIMEOUT_SECS,
            sources.MIN_TIMEOUT_SECS,
            sources.MAX_TIMEOUT_SECS,
            problems,
        )
        spec = str(_set(setting(sources.SOURCE_VAR)) or "none").strip()
        region = _set(setting(sources.REGION_VAR))
        if problems:
            raise ConfigError("invalid LLMQ settings:\n  " + "\n  ".join(problems))
        self.secrets_source = spec
        self.secrets_region = region.strip() if region else None
        self.secrets_cache_secs = cache
        self.secrets_timeout_secs = timeout
        if secrets is None:
            # Code `secrets=` wins over LLMQ_SECRETS_SOURCE (which is then not read at all).
            try:
                secrets = sources.secrets_from_config(
                    spec,
                    environ=self._env,
                    region=self.secrets_region,
                    cache_secs=cache,
                    timeout_secs=timeout,
                )
            except ConfigError as exc:
                raise ConfigError(f"invalid LLMQ settings:\n  {exc}") from None
        self._secrets = secrets
        self._defaults = {name: _set(value) for name, value in raw_defaults.items()}
        self._names = names

    @property
    def environ(self) -> dict[str, str]:
        """The environment layer (the copy taken at construction)."""
        return self._env

    @property
    def secrets(self) -> Callable[[str], str | None] | None:
        """The secret layer's lookup (by the secret's own key), None when there is none."""
        return self._secrets

    @property
    def secret_names(self) -> dict[str, str]:
        """Settings read from the secrets under another key (merged names)."""
        return dict(self._names)

    def invalidate_secrets(self) -> None:
        """Have the secret layer read its sources again (`SecretReader.invalidate`), if it can."""
        invalidate = getattr(self._secrets, "invalidate", None)
        if callable(invalidate):
            invalidate()

    def describe_secrets(self) -> dict[str, Any]:
        """Where secrets come from (kinds and ids) and which settings are mapped: no values."""
        reader = self._secrets
        describe = getattr(reader, "describe", None)
        if reader is None:
            shown: dict[str, Any] = {"sources": []}
        elif callable(describe):
            shown = dict(describe())
        else:
            shown = {"sources": ["code"]}  # an app's own `secrets=` callable
        shown["names"] = sorted(self._names)
        return shown

    def secret(self, name: str) -> str | None:
        """Secret `name` (under its mapped key), None when unset or empty."""
        if self._secrets is None or name in _SECRETS_SETTINGS:
            return None
        return _set(self._secrets(self._names.get(name, name)))

    def lookup(self, name: str) -> str | None:
        """`name` from the secrets, else the environment (placeholders, account secrets)."""
        return self.secret(name) or _set(self._env.get(name))

    def _values(self, name: str) -> tuple[str | None, str | None, str | None]:
        return self.secret(name), _set(self._env.get(name)), self._defaults.get(name)

    def _deciding(self, sides: tuple[tuple[str, ...], ...]) -> list[bool] | None:
        """Which sides of a group the highest layer that sets any of its names sets."""
        for layer in range(3):
            found = [
                any(self._values(PREFIX + n)[layer] is not None for n in side) for side in sides
            ]
            if any(found):
                return found
        return None

    def _dropped(self, name: str) -> bool:
        """True when `name` is on the side of its conflict group that a higher layer overrode."""
        short = name.removeprefix(PREFIX)
        for sides in CONFLICT_GROUPS:
            if any(short in side for side in sides):
                found = self._deciding(sides)
                if found is None or all(found):
                    return False  # both in one layer: reported by `conflicts`, nothing dropped
                return short not in sides[found.index(True)]
        return False

    def conflicts(self) -> list[str]:
        """The groups whose deciding layer sets both sides, by their first name (no prefix)."""
        out = []
        for sides in CONFLICT_GROUPS:
            found = self._deciding(sides)
            if found is not None and all(found):
                out.append(sides[0][0])
        return out

    def get(self, name: str, default: Any = None) -> Any:
        """The value of `name` from the highest layer that sets it, else `default`."""
        if self._dropped(name):
            return default
        for value in self._values(name):
            if value is not None:
                return value
        return default

    def __getitem__(self, name: str) -> str:
        """The value of `name`; `KeyError` when no layer sets it."""
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return str(value)

    def __iter__(self) -> Iterator[str]:
        """The names of the environment and the defaults (secrets cannot be listed)."""
        return iter(dict.fromkeys([*self._env, *self._defaults]))

    def __len__(self) -> int:
        """How many names `__iter__` yields."""
        return len(dict.fromkeys([*self._env, *self._defaults]))


def old_names(layers: Layers, *, strict: bool) -> list[str]:
    """Renamed or removed settings still set (`RENAMED`, `REMOVED`): problems when strict.

    Outside strict mode each is logged (`setting_renamed` / `setting_removed`) and `[]` is
    returned. Only the environment and `defaults=` are looked at (secrets cannot be listed).
    """
    problems: list[str] = []
    for name in sorted(set(layers)):
        if name in RENAMED:
            event, text = "setting_renamed", f"{name} was renamed {RENAMED[name]}: set that instead"
        elif name in REMOVED:
            event, text = "setting_removed", f"{name} was removed: {REMOVED[name]}"
        else:
            continue
        if layers.get(name) is None:
            continue
        if strict:
            problems.append(text)
        else:
            logger.warning(event, setting=name, use=RENAMED.get(name), hint=text)
    return problems


def read_bool(layers: Mapping[str, Any], name: str, default: bool, problems: list[str]) -> bool:
    """Switch `LLMQ_<name>` (`true` / `false`, `on` / `off`, `yes` / `no`, `1` / `0`).

    A bad value is added to `problems` and the default is returned.
    """
    raw = str(layers.get(PREFIX + name, "true" if default else "false")).strip().lower()
    if raw not in _TRUE + _FALSE:
        problems.append(f"{PREFIX}{name} must be true or false (on / off)")
        return default
    return raw in _TRUE


def _parts_url(host: str, port: int, db: int, tls: bool, password: str | None) -> str:
    """The store URL from its parts; the password percent-escaped (`@`, `/`, `:` are data)."""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"  # an IPv6 address
    auth = f":{quote(password, safe='')}@" if password else ""
    return f"{'rediss' if tls else 'redis'}://{auth}{host}:{port}/{db}"


def _auto_pool(document: Any, hosts: tuple[str, ...]) -> tuple[int, str | None]:
    """`LLMQ_STORE_MAX_CONNECTIONS=auto`: the boot registry's worker slots + `STORE_POOL_MARGIN`."""
    from llm_queue.registry.validate import load_registry

    try:
        view = load_registry(document, allowed_hosts=hosts)
    except ConfigError as exc:
        return 200, f"LLMQ_STORE_MAX_CONNECTIONS=auto needs a valid registry: {exc}"
    slots = sum(int(view.value(alias, "worker_slots")) for alias in view.registry.models)
    return min(max(slots + STORE_POOL_MARGIN, 4), 100_000), None


def _store_url(name: str, url: str, strict: bool) -> tuple[str, list[str]]:
    """The store URL to use and what is wrong with it; `strict` requires verified TLS.

    The URL is read with redis-py's own parser (`parse_url`, `to_bool`), so the check sees what
    the client will do: redis-py takes the first of repeated query names, which a check of the
    last one missed, so repeats are refused outright.
    """
    parts = urlsplit(url)
    if parts.scheme not in STORE_SCHEMES:
        return url, [f"{name} must be {_SCHEMES_TEXT}"]
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


TENANT_PURGE_PARTS = ("keys", "queued", "running")


def _purge_settings(
    get: Callable[..., Any], number: Callable[..., Any], problems: list[str]
) -> dict[str, Any]:
    """The purge settings (`LLMQ_AUTO_PURGE` ... `LLMQ_TENANT_PURGE_DEFAULT`) as fields.

    Kept apart from `from_env`'s body so the purge feature reads as one block.
    """
    out: dict[str, Any] = {}
    for name, field_name, default in (
        ("AUTO_PURGE", "auto_purge", "true"),
        ("PURGE_WAIT", "purge_wait", "true"),
    ):
        raw = str(get(name, default)).strip().lower()
        if raw not in _TRUE + _FALSE:
            problems.append(f"LLMQ_{name} must be true or false (on / off)")
            raw = default
        out[field_name] = raw in _TRUE
    out["purge_grace_secs"] = number("PURGE_GRACE_SECS", 900.0, zero=True, hi=604_800)
    out["purge_wait_secs"] = number("PURGE_WAIT_SECS", 300.0, zero=True, hi=86_400)
    out["http_purge_wait_secs"] = number("HTTP_PURGE_WAIT_SECS", 60.0, zero=True, hi=3_600)
    parts = {p.strip().lower() for p in str(get("TENANT_PURGE_DEFAULT", "keys")).split(",")}
    parts.discard("")
    if not parts or parts - set(TENANT_PURGE_PARTS):
        problems.append(
            "LLMQ_TENANT_PURGE_DEFAULT must be a comma list of " + ", ".join(TENANT_PURGE_PARTS)
        )
        parts = {"keys"}
    out["tenant_purge_default"] = ",".join(p for p in TENANT_PURGE_PARTS if p in parts)
    return out


# `LLMQ_METRICS_TENANTS`: at most this many tenants get a label of their own (cardinality).
METRICS_TENANTS_MAX = 50
_LABEL_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}\Z")


def parse_metrics_tenants(text: str) -> tuple[tuple[tuple[str, str], ...], list[str]]:
    """`LLMQ_METRICS_TENANTS` (`acme,globex=t2`) as (tenant name, label) pairs, and problems.

    A bare name is its own label. Labels are `[A-Za-z0-9_.-]{1,64}`, distinct, and never `other`
    or `-` (the values of unlisted tenants and of calls with no tenant).
    """
    pairs: list[tuple[str, str]] = []
    problems: list[str] = []
    for item in (part.strip() for part in text.split(",")):
        if not item:
            continue
        name, _, label = item.partition("=")
        name, label = name.strip(), (label.strip() or name.strip())
        if not name or not _LABEL_RE.match(label) or label in ("other", "-"):
            problems.append(
                f"LLMQ_METRICS_TENANTS entry `{item}` must be `name` or `name=label` with a label "
                "of 1-64 letters, digits, `_`, `.` or `-` (not `other` or `-`)"
            )
            continue
        pairs.append((name, label))
    if len(pairs) > METRICS_TENANTS_MAX:
        problems.append(f"LLMQ_METRICS_TENANTS must list at most {METRICS_TENANTS_MAX} tenants")
    names = [n for n, _ in pairs]
    labels = [label for _, label in pairs]
    if len(set(names)) != len(names) or len(set(labels)) != len(labels):
        problems.append("LLMQ_METRICS_TENANTS must not repeat a tenant or a label")
    return tuple(pairs), problems


def _admin_settings(
    layers: Mapping[str, Any],
    get: Callable[..., Any],
    number: Callable[..., Any],
    problems: list[str],
) -> dict[str, Any]:
    """The request index and admin settings (design 0.2.0 T1, T2, T6) as `InfraSettings` fields."""
    tenants, found = parse_metrics_tenants(str(get("METRICS_TENANTS", "") or ""))
    problems.extend(found)
    return {
        "request_index_enabled": read_bool(layers, "REQUEST_INDEX_ENABLED", True, problems),
        "tenant_names_enabled": read_bool(layers, "TENANT_NAMES_ENABLED", False, problems),
        "admin_batch": number("ADMIN_BATCH", 500, int, lo=1, hi=10_000),
        "admin_scan_max": number("ADMIN_SCAN_MAX", 100_000, int, lo=1_000, hi=10_000_000),
        "requests_list_max": number("REQUESTS_LIST_MAX", 1_000, int, lo=1, hi=10_000),
        "requests_move_batch": number("REQUESTS_MOVE_BATCH", 50, int, lo=1, hi=500),
        "requests_move_bytes": number(
            "REQUESTS_MOVE_BYTES", 8_388_608, int, lo=65_536, hi=67_108_864
        ),
        "metrics_tenants": tenants,
    }


def read_document(
    registry: str | None,
    registry_file: str | None,
    lookup: Callable[[str], str | None] | None = None,
    used: set[str] | None = None,
) -> str:
    """The registry JSON from the variable or the file, refused when it repeats a key.

    `${NAME}` placeholders are expanded with `lookup` after parsing (credentials refused);
    a `.yaml` / `.yml` file is read as YAML. `used` collects the names read.
    """
    # Parsing first lets the repeated-key check see the document as written; the expanded
    # text is checked again. Text without placeholders is returned as it is.
    yaml_file = False
    if registry:
        text = registry
    elif registry_file:
        yaml_file = Path(registry_file).suffix.lower() in YAML_SUFFIXES
        try:
            text = Path(registry_file).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise ConfigError(f"LLMQ_REGISTRY_FILE cannot be read: {exc}") from exc
    else:
        raise ConfigError("no registry: set LLMQ_REGISTRY or LLMQ_REGISTRY_FILE")
    # RegistryError (a ConfigError) for bad JSON or a repeated key; YAML has its own check.
    document = load_yaml(text) if yaml_file else parse_document(text)
    if "${" not in text and not yaml_file:
        return text
    return to_json(expand(document, lookup or os.environ.get, used=used))


@dataclass(frozen=True, slots=True)
class InfraSettings:
    """The process settings of one environment."""

    store_url: str = field(repr=False)  # holds the password
    # LLMQ_ENABLED (0.2.0 H1, FU1): false switches the library off (a disabled queue: probes,
    # the drain file and `describe()` only; every other setting is then not read or checked).
    enabled: bool = True
    queue_store_url: str | None = field(default=None, repr=False)
    store_max_connections: int = 200
    # LLMQ_STORE_MAX_CONNECTIONS=auto (0.2.0 D7): the size above is the boot registry's worker
    # slots + STORE_POOL_MARGIN.
    store_max_connections_auto: bool = False
    # LLMQ_SYNC_STORE_MAX_CONNECTIONS: the private engine of sync calls when no queue is started
    # (Celery workers, scripts) is caller-only and needs few (D1); capped by the pool above.
    sync_store_max_connections: int = 20
    # The store as parts (0.2.0 D3), when `LLMQ_STORE_URL` is not set; `store_url` is built from
    # them (the password from LLMQ_STORE_PASSWORD, escaped).
    store_host: str | None = None
    store_port: int = 6379
    store_db: int = 0
    store_tls_enabled: bool = True
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
    # Drain (0.2.0 A1, A4): a pause hands held calls back at once (owner Q2); a file whose
    # appearance pauses every process of the pod (empty: off), polled this often.
    drain_handback_enabled: bool = True  # LLMQ_DRAIN_HANDBACK_ENABLED
    drain_file: str = ""  # LLMQ_DRAIN_FILE
    drain_poll_secs: float = 1.0  # LLMQ_DRAIN_POLL_SECS
    # Size-aware backfill (plan F1, owner Q3) for every model that leaves `queue.backfill` unset.
    queue_backfill_enabled: bool = False  # LLMQ_QUEUE_BACKFILL_ENABLED
    # LLMQ_EXECUTION_MODE (0.2.0): `queue` sends calls through the store; `local` runs every
    # call in this process against the provider and never contacts the store (engine/local.py).
    execution_mode: str = "queue"
    # The local executor's budgets: each deployment's rpm / tpm divided by this (pods x
    # processes), and calls in flight per model and process (`local` mode and `static_cap`).
    local_process_count: int = 1  # LLMQ_LOCAL_PROCESS_COUNT
    local_concurrency: int = 10  # LLMQ_LOCAL_CONCURRENCY
    static_cap_wait_secs: float = 5.0  # LLMQ_STATIC_CAP_WAIT_SECS: a `static_cap` budget wait
    metrics_sink: str = "emf"
    mode: str = "both"
    bedrock_endpoint_url: str | None = None
    sts_endpoint_url: str | None = None
    id_hash_salt: str | None = field(default=None, repr=False)
    environment: str = "unset"
    queue_failover_secs: float = 5.0
    queue_wait_replicas: int = 0
    queue_wait_ms: int = 100
    # What the replica wait covers (owner Q8 / Q51), LLMQ_QUEUE_WAIT_SCOPES: all by default.
    queue_wait_scopes: frozenset[str] = frozenset(WAIT_SCOPES)
    debug_capture_from_config: bool = False
    worker_id: str = ""  # LLMQ_WORKER_ID: this process's id prefix (default pod:pid)
    default_tenant: str | None = None  # LLMQ_DEFAULT_TENANT: when a call names none
    admin_header: str = "X-LLMQ-Token"  # LLMQ_ADMIN_HEADER: the admin tokens' header
    # Admin surfaces (0.2.0 R): write routes mounted by default, the claimed-actor header of a
    # token guard (audited `token:<value>`), the `GET /` summary cache, dead-letter bodies.
    admin_writes_enabled: bool = False  # LLMQ_ADMIN_WRITES_ENABLED
    admin_actor_header: str = "X-LLMQ-Actor"  # LLMQ_ADMIN_ACTOR_HEADER
    admin_csrf_header: str = "X-LLMQ-Admin"  # LLMQ_ADMIN_CSRF_HEADER: a write route needs it
    admin_index_cache_secs: float = 2.0  # LLMQ_ADMIN_INDEX_CACHE_SECS
    admin_dlq_bodies_enabled: bool = False  # LLMQ_ADMIN_DLQ_BODIES_ENABLED
    admin_body_max_bytes: int = 1_048_576  # LLMQ_ADMIN_BODY_MAX_BYTES: a larger POST is a 413
    # How keys expire (LLMQ_KEY_EXPIRY): `auto` picks native TTLs on a `noeviction` store and the
    # expiry index on a `volatile-*` one (owner decision K4, 0.1.0b3); engine/expiry.py.
    key_expiry: str = "auto"
    ttl_sweep_batch: int = 500  # LLMQ_TTL_SWEEP_BATCH: keys per sweep script run
    ttl_sweep_rounds: int = 100  # LLMQ_TTL_SWEEP_ROUNDS: full batches per sweep, at most
    # Purge of removed models / deployments and tenants (0.1.0b3, owner K1-K3): see `PurgeSettings`.
    auto_purge: bool = True
    purge_grace_secs: float = 900.0
    purge_wait: bool = True
    purge_wait_secs: float = 300.0
    http_purge_wait_secs: float = 60.0  # LLMQ_HTTP_PURGE_WAIT_SECS
    tenant_purge_default: str = "keys"  # comma list of TENANT_PURGE_PARTS
    # The request index and admin operations (0.2.0 T): see `AdminSettings` (requests_admin.py).
    request_index_enabled: bool = True  # LLMQ_REQUEST_INDEX_ENABLED
    tenant_names_enabled: bool = False  # LLMQ_TENANT_NAMES_ENABLED: names stored beside hashes
    admin_batch: int = 500  # LLMQ_ADMIN_BATCH: ids per pipeline / script in admin ops and purges
    admin_scan_max: int = 100_000  # LLMQ_ADMIN_SCAN_MAX
    requests_list_max: int = 1_000  # LLMQ_REQUESTS_LIST_MAX
    requests_move_batch: int = 50  # LLMQ_REQUESTS_MOVE_BATCH
    requests_move_bytes: int = 8_388_608  # LLMQ_REQUESTS_MOVE_BYTES
    # LLMQ_METRICS_TENANTS: (tenant name, label) pairs; raw names stay in this process only.
    metrics_tenants: tuple[tuple[str, str], ...] = field(default=(), repr=False)
    # Where secrets come from (llm_queue.secrets): the source spec as written, its sources
    # (kind and id; `code` for a `secrets=` callable), the settings mapped to other secret keys
    # (names only), the region, the cache time and the AWS timeouts.
    secrets_source: str = "none"
    secret_sources: tuple[str, ...] = ()
    secret_names: tuple[str, ...] = ()
    secrets_region: str | None = None
    secrets_cache_secs: float = 300.0
    secrets_timeout_secs: float = 10.0
    # Where registry placeholders are read (the secrets first, then the environment `from_env`
    # read); None: `os.environ`. Never shown or compared.
    placeholder_lookup: Callable[[str], str | None] | None = field(
        default=None, repr=False, compare=False
    )

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
        defaults: Mapping[str, Any] | None = None,
        secret_names: Mapping[str, str] | None = None,
    ) -> InfraSettings:
        """Read `LLMQ_*`: the secrets first, then `environ` (`os.environ`), then `defaults`.

        The secrets are `secrets` when given, else the sources `LLMQ_SECRETS_SOURCE` names.
        See `Layers`; `secret_names` maps a setting to the key of the secret that holds it.
        """
        return cls.from_layers(Layers(environ, secrets, defaults, secret_names))

    @classmethod
    def from_layers(cls, layers: Layers) -> InfraSettings:
        """Read `LLMQ_*` from built `Layers` (so one secrets source serves every reader)."""

        def get(name: str, default: Any = None) -> Any:
            # An empty value counts as unset everywhere: compose's `${VAR:-}` writes one, and
            # `LLMQ_LEASE_SECS=` was an error while an empty LLMQ_ENVIRONMENT was not (R3CF-14).
            return layers.get(PREFIX + name, default)

        # A registry placeholder names the variable in full (no LLMQ_ prefix): the secrets,
        # then the environment.
        placeholder = layers.lookup
        secrets_shown = layers.describe_secrets()
        problems: list[str] = []
        if not read_bool(layers, "ENABLED", True, problems) and not problems:
            return cls._disabled(layers)
        store = get("STORE_URL")
        execution_mode = str(get("EXECUTION_MODE", "queue")).strip().lower()
        if execution_mode not in EXECUTION_MODES:
            problems.append("LLMQ_EXECUTION_MODE must be queue or local")
        local = execution_mode == "local"
        environment = str(get("ENVIRONMENT", "") or "").strip().lower() or "unset"
        if environment == "production":
            environment = "prod"
        strict = environment not in DEV_ENVIRONMENTS
        problems += old_names(layers, strict=strict)
        host = str(get("STORE_HOST", "") or "").strip() or None
        if store and "STORE_URL" in layers.conflicts():
            problems.append(
                "set LLMQ_STORE_URL or the store parts ("
                + ", ".join(PREFIX + n for n in STORE_PARTS)
                + ") in one place, not both"
            )
        elif not store and not host and not local:  # local mode never contacts a store
            problems.append(f"LLMQ_STORE_URL (or LLMQ_STORE_HOST) is required ({_SCHEMES_TEXT})")
        registry, registry_file = get("REGISTRY"), get("REGISTRY_FILE")
        hosts = tuple(
            h.strip() for h in str(get("BASE_URL_ALLOWED_HOSTS", "")).split(",") if h.strip()
        )
        for host in hosts:
            # A scheme, port or path never matched a host, and every `openai` account then
            # failed with a message that did not point at this entry (R3CF-13).
            if any(c in host for c in "/:@?#") and not host.strip("[]").count(":") > 1:
                problems.append(
                    f"LLMQ_BASE_URL_ALLOWED_HOSTS entry `{host}` must be a bare host name "
                    "(no scheme, port or path), e.g. api.openai.com"
                )
        boot: Any = None  # the boot registry document, when it reads
        if registry and registry_file:
            problems.append("set one of LLMQ_REGISTRY and LLMQ_REGISTRY_FILE, not both")
        elif registry or registry_file:
            used: set[str] = set()
            try:
                text = read_document(registry, registry_file, placeholder, used)
                document = boot = orjson.loads(text)
            except ConfigError as exc:
                problems.append(str(exc))
            except ValueError as exc:  # orjson refuses what `json` took (NaN): R3CF-1
                problems.append(f"registry is not valid JSON: {exc}")
            else:
                if used:  # the names only: a value never reaches a log line
                    logger.info("registry_placeholders", names=sorted(used))
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
        elif strict and len(str(salt)) < MIN_SALT_CHARS:
            problems.append(
                f"LLMQ_ID_HASH_SALT must be at least {MIN_SALT_CHARS} characters outside dev "
                f"({environment}): a short key does not stop brute-forcing hashed ids"
            )
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
        worker_id = str(get("WORKER_ID", "") or "").strip()
        if worker_id and not _PREFIX_RE.match(worker_id.replace(":", "_")):
            problems.append("LLMQ_WORKER_ID must be letters, digits, `_`, `.`, `:` or `-`")
        default_tenant = str(get("DEFAULT_TENANT", "") or "").strip() or None
        admin_header = str(get("ADMIN_HEADER", "X-LLMQ-Token") or "").strip()
        actor_header = str(get("ADMIN_ACTOR_HEADER", "X-LLMQ-Actor") or "").strip()
        csrf_header = str(get("ADMIN_CSRF_HEADER", "X-LLMQ-Admin") or "").strip()
        for name, header in (
            ("ADMIN_HEADER", admin_header),
            ("ADMIN_ACTOR_HEADER", actor_header),
            ("ADMIN_CSRF_HEADER", csrf_header),
        ):
            if not re.fullmatch(r"[A-Za-z0-9-]{1,64}", header):
                problems.append(f"LLMQ_{name} must be a header name (letters, digits, `-`)")
        switches = {
            name: read_bool(layers, name, False, problems)
            for name in ("ADMIN_WRITES_ENABLED", "ADMIN_DLQ_BODIES_ENABLED")
        }
        hand_back = read_bool(layers, "DRAIN_HANDBACK_ENABLED", True, problems)
        backfill = read_bool(layers, "QUEUE_BACKFILL_ENABLED", False, problems)
        drain_file = str(get("DRAIN_FILE", "") or "").strip()
        mode = str(get("MODE", "both"))
        if mode not in ("both", "caller", "worker"):
            problems.append("LLMQ_MODE must be both, caller or worker")
        key_expiry = str(get("KEY_EXPIRY", "auto")).strip().lower()
        if key_expiry not in KEY_EXPIRY_MODES:
            problems.append("LLMQ_KEY_EXPIRY must be auto, native or swept")
        key_prefix = get("KEY_PREFIX")
        if strict and not key_prefix and not local:
            # Owner decision Q26 (R3SE-9): two environments on one store that both left the
            # default `llmq` shared queues and registry (staging workers ran prod prompts).
            problems.append(
                f"LLMQ_KEY_PREFIX is required outside dev ({environment}): name it per app and "
                "environment, e.g. myapp-prod"
            )
        queue_prefix = str(get("QUEUE_PREFIX", ""))
        for name, value, need, rule, allowed in (
            ("KEY_PREFIX", str(key_prefix or "llmq"), True, _KEY_PREFIX_RE, "`_`, `.`, `:`"),
            ("QUEUE_PREFIX", queue_prefix, False, _PREFIX_RE, "`_`, `.`"),
        ):
            if not rule.match(value) or (need and not value):
                problems.append(
                    f"LLMQ_{name} must be {'1' if need else '0'}-64 letters, digits, {allowed} "
                    "or `-` (no braces or glob characters: it is part of every key)"
                )
        endpoints: dict[str, Any] = {"BEDROCK_ENDPOINT_URL": None, "STS_ENDPOINT_URL": None}
        for name in endpoints:
            value = get(name)
            endpoints[name] = value
            if strict and value and not str(value).lower().startswith("https://"):
                # Owner decision Q30 (R3SE-6): plain http sent prompts, SigV4 headers and the
                # session token (and STS role keys) in clear. Any host is allowed.
                problems.append(f"LLMQ_{name} must use https outside dev ({environment})")

        def number(
            name: str,
            default: float,
            kind: type = float,
            *,
            zero: bool = False,
            lo: float | None = None,
            hi: float = INT32_MAX,
        ) -> Any:
            raw = get(name, default)
            try:
                value = kind(raw)
            except (TypeError, ValueError):
                # `200.0` and `1e3` are numbers: the rule is a whole number (R3CF-17).
                what = "a whole number" if kind is int else "a number"
                problems.append(f"LLMQ_{name} must be {what}")
                return default
            if not math.isfinite(value):
                problems.append(f"LLMQ_{name} must be a finite number")
                return default
            # A range for every variable, not only a lower bound: `LLMQ_CFG_RELOAD_SECS=1e-9`
            # spun the reload loop, a TTL past int32 failed at the first write (R3CF-14).
            low = lo if lo is not None else 0
            if value < low or (value == 0 and not zero and lo is None):
                floor = (
                    f"at least {low:g}" if lo is not None else ("0 or more" if zero else "above 0")
                )
                problems.append(f"LLMQ_{name} must be {floor}")
            elif value > hi:
                problems.append(f"LLMQ_{name} must be at most {hi:g}")
                return default
            return value

        scopes, scope_problems = parse_scopes(str(get("QUEUE_WAIT_SCOPES", "all")))
        problems.extend(scope_problems)
        lease = number("LEASE_SECS", 60.0, hi=3600)
        caller_ttl = number("CALLER_TTL_SECS", 30, int, hi=86_400)
        # The caller key is renewed every lease / 3; it must outlive one beat with room for a
        # slow store, or a live caller is taken for gone and its calls are failed.
        beat = lease / 3
        if caller_ttl < beat + max(5.0, beat / 2):
            problems.append(
                f"LLMQ_CALLER_TTL_SECS ({caller_ttl}) must be at least "
                f"{math.ceil(beat + max(5.0, beat / 2))}: the heartbeat renews it every "
                f"LLMQ_LEASE_SECS / 3 ({beat:g} s), plus a margin"
            )
        purge = _purge_settings(get, number, problems)
        port = number("STORE_PORT", 6379, int, lo=1, hi=65_535)
        db = number("STORE_DB", 0, int, zero=True)
        tls = read_bool(layers, "STORE_TLS_ENABLED", True, problems)
        urls: dict[str, str | None] = {}
        for name in ("STORE_URL", "QUEUE_STORE_URL"):
            raw = get(name)
            urls[name] = None
            if raw and not local:  # never connected to in local mode: not checked
                urls[name], found = _store_url(PREFIX + name, str(raw), strict)
                problems += found
        if not store and host and not local:
            # The password goes in the URL so every client built from it (the CLI's too) can
            # log in; `LLMQueue.from_env` still reads LLMQ_STORE_PASSWORD fresh per connection.
            parts = _parts_url(host, int(port), int(db), tls, get("STORE_PASSWORD"))
            urls["STORE_URL"], found = _store_url("the store from LLMQ_STORE_HOST", parts, strict)
            problems += found
        pool_raw = str(get("STORE_MAX_CONNECTIONS", "") or "").strip().lower()
        pool_auto = pool_raw == "auto"
        if pool_auto:
            pool = 200
            if not (registry or registry_file):
                problems.append("LLMQ_STORE_MAX_CONNECTIONS=auto needs LLMQ_REGISTRY(_FILE)")
            elif boot is not None:
                pool, found_pool = _auto_pool(boot, hosts)
                problems += [found_pool] if found_pool else []
        else:
            pool = number("STORE_MAX_CONNECTIONS", 200, int, lo=4, hi=100_000)
        admin = _admin_settings(layers, get, number, problems)
        settings = cls(
            store_url=urls["STORE_URL"] or str(store or ""),
            queue_store_url=urls["QUEUE_STORE_URL"],
            store_max_connections=pool,
            store_max_connections_auto=pool_auto,
            sync_store_max_connections=number(
                "SYNC_STORE_MAX_CONNECTIONS", 20, int, lo=1, hi=100_000
            ),
            store_host=host,
            store_port=port,
            store_db=db,
            store_tls_enabled=tls,
            key_prefix=str(key_prefix or "llmq"),
            queue_prefix=queue_prefix,
            registry=registry,
            registry_file=registry_file,
            registry_live_edit=live_edit,
            base_url_allowed_hosts=hosts,
            cfg_reload_secs=number("CFG_RELOAD_SECS", 30.0, lo=1, hi=3600),
            result_ttl_secs=number("RESULT_TTL_SECS", 300, int),
            result_dedupe_secs=number("RESULT_DEDUPE_SECS", 0, int, zero=True),
            # At least 1 s; that it outlives a call is the start cross-check's job (R3CF-10).
            status_ttl_secs=number("STATUS_TTL_SECS", 1800, int, lo=1),
            audit_retention_days=number("AUDIT_RETENTION_DAYS", 90, int, hi=3650),
            lease_secs=lease,
            caller_ttl_secs=caller_ttl,
            drain_secs=number(
                "DRAIN_SECS", 25.0, zero=True, hi=3600
            ),  # stop(drain_secs=0) is valid
            drain_handback_enabled=hand_back,
            drain_file=drain_file,
            drain_poll_secs=number("DRAIN_POLL_SECS", 1.0, lo=0.1, hi=60),
            queue_backfill_enabled=backfill,
            execution_mode=execution_mode,
            local_process_count=number("LOCAL_PROCESS_COUNT", 1, int, hi=100_000),
            local_concurrency=number("LOCAL_CONCURRENCY", 10, int, hi=100_000),
            static_cap_wait_secs=number("STATIC_CAP_WAIT_SECS", 5.0, zero=True, hi=3600),
            metrics_sink=sink,
            mode=mode,
            bedrock_endpoint_url=endpoints["BEDROCK_ENDPOINT_URL"],
            sts_endpoint_url=endpoints["STS_ENDPOINT_URL"],
            id_hash_salt=salt,
            environment=environment,
            queue_failover_secs=number("QUEUE_FAILOVER_SECS", 5.0, zero=True, hi=3600),
            queue_wait_replicas=number("QUEUE_WAIT_REPLICAS", 0, int, zero=True, hi=16),
            queue_wait_ms=number("QUEUE_WAIT_MS", 100, int, hi=60_000),
            queue_wait_scopes=scopes,
            debug_capture_from_config=capture,
            worker_id=worker_id,
            default_tenant=default_tenant,
            admin_header=admin_header,
            admin_writes_enabled=switches["ADMIN_WRITES_ENABLED"],
            admin_actor_header=actor_header,
            admin_csrf_header=csrf_header,
            admin_index_cache_secs=number("ADMIN_INDEX_CACHE_SECS", 2.0, zero=True, hi=60),
            admin_dlq_bodies_enabled=switches["ADMIN_DLQ_BODIES_ENABLED"],
            admin_body_max_bytes=number(
                "ADMIN_BODY_MAX_BYTES", 1_048_576, int, lo=1024, hi=67_108_864
            ),
            key_expiry=key_expiry,
            ttl_sweep_batch=number("TTL_SWEEP_BATCH", 500, int, lo=1, hi=5_000),
            ttl_sweep_rounds=number("TTL_SWEEP_ROUNDS", 100, int, lo=1, hi=10_000),
            **purge,
            **admin,
            secrets_source=layers.secrets_source,
            secret_sources=tuple(secrets_shown["sources"]),
            secret_names=tuple(secrets_shown["names"]),
            secrets_region=layers.secrets_region,
            secrets_cache_secs=layers.secrets_cache_secs,
            secrets_timeout_secs=layers.secrets_timeout_secs,
            placeholder_lookup=placeholder,
        )
        if problems:
            raise ConfigError("invalid LLMQ settings:\n  " + "\n  ".join(problems))
        return settings

    @classmethod
    def _disabled(cls, layers: Layers) -> InfraSettings:
        """`LLMQ_ENABLED=false`: only what a disabled queue uses (environment and drain file).

        No store, registry or salt is needed; the other `LLMQ_*` values are not read, so an
        app can switch the library off without removing them (or before they exist).
        """
        problems: list[str] = []
        environment = str(layers.get(PREFIX + "ENVIRONMENT", "") or "").strip().lower()
        environment = {"": "unset", "production": "prod"}.get(environment, environment)
        poll_raw = layers.get(PREFIX + "DRAIN_POLL_SECS", 1.0)
        try:
            poll = float(poll_raw)
        except (TypeError, ValueError):
            poll = 1.0
            problems.append("LLMQ_DRAIN_POLL_SECS must be a number")
        if not (math.isfinite(poll) and 0.1 <= poll <= 60):
            problems.append("LLMQ_DRAIN_POLL_SECS must be at least 0.1 and at most 60")
        if problems:
            raise ConfigError("invalid LLMQ settings:\n  " + "\n  ".join(problems))
        return cls(
            store_url="",
            enabled=False,
            environment=environment,
            drain_file=str(layers.get(PREFIX + "DRAIN_FILE", "") or "").strip(),
            drain_poll_secs=poll,
            secrets_source=layers.secrets_source,
        )

    def registry_document(self) -> str:
        """The registry JSON from `LLMQ_REGISTRY` or the file `LLMQ_REGISTRY_FILE`.

        Placeholders expanded; tenant names in `fairness` replaced by their keyed hashes (D2),
        so only hashes reach the store. `ConfigError` when it cannot be read or parsed.
        """
        from llm_queue.engine.fairness import hash_tenant_names

        text = read_document(self.registry, self.registry_file, self.placeholder_lookup)
        document = parse_document(text)
        salt = self.id_hash_salt or ""
        hashed = hash_tenant_names(document, lambda value: keyed_hash(salt, value))
        return text if hashed == document else to_json(hashed)
