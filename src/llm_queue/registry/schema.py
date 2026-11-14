"""The registry document (`LLMQ_REGISTRY`) as frozen data, built with every problem collected.

`build()` turns the JSON document into a `Registry` and lists all problems at once, each with
its path, so one fix-and-rerun is enough. Cross-field checks that need resolved values live in
`registry.validate`. Unknown names are errors for the process's own config (`strict`) and
warnings for a stored document (read leniently, so an older pod still starts).
"""

from __future__ import annotations

import difflib
import ipaddress
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any
from urllib.parse import parse_qsl, urlparse

from llm_queue._version import __version__
from llm_queue.errors import ConfigError, UnknownModel
from llm_queue.registry.settings import SETTINGS, A, D, M, R, settings_at

NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
# Queue (stream) names: short and plain, so key names and metric labels stay readable.
QUEUE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,71}$")  # leaves room for "-priority"
# Account kind -> the provider it serves. `register_account_kind` adds more.
ACCOUNT_KINDS: dict[str, str] = {"aws": "bedrock", "openai": "openai"}
MODEL_TYPES = ("chat", "embedding")
LIVE_EDIT_MODES = ("safe", "all", "off")
# The only hosts a plain-http `base_url` may name (no single-label or `.localhost` names).
LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "host.docker.internal"})
# An account's secrets are environment variables (or secrets) under this prefix only, so a
# registry edit can never name the store password or the pod's own AWS keys.
SECRET_REF_PREFIX = "LLMQ_ACCOUNT_"
_SECRET_REF_RE = re.compile(rf"^{SECRET_REF_PREFIX}[A-Z0-9_]+$")
# Query names that carry a credential: a key in a URL ends up in logs and proxies.
_URL_SECRET_PARAMS = frozenset(
    {"api_key", "apikey", "key", "token", "access_token", "password", "secret", "sig", "signature"}
)
# AWS instance metadata, DNS and time over IPv6 (fd00:ec2::/32) and link-local everywhere.
_METADATA_NET = ipaddress.ip_network("fd00:ec2::/32")
_VERSION_RE = re.compile(r"^(\d+(?:\.\d+)*)(?:(a|b|rc)(\d+))?(?:\.post(\d+))?(?:\.dev(\d+))?$")

_TOP = ("registry_version", "min_lib_version", "live_edit", "defaults", "routing", "residency")
_TOP_KEYS = frozenset((*_TOP, "accounts", "models"))
_ACCOUNT_FIELDS = frozenset(
    {"kind", "enabled", "role_arn", "external_id_ref", "base_url", "api_key_ref", "settings"}
)
_MODEL_FIELDS = frozenset({"type", "provider", "model_id", "deployments", "residency"})
_DEPLOYMENT_FIELDS = frozenset({"confirmed", "fallback", "overrides"})
_ROUTING_FIELDS = frozenset({"kill_switch", "allow_users", "blocked_regions", "blocked_accounts"})
_PROFILE_FIELDS = frozenset({"name", "region", "profile_id", "routes_to"})


def register_account_kind(kind: str, provider: str) -> None:
    """Allow accounts of `kind` to serve models of `provider` (for a custom provider)."""
    if not NAME_RE.match(kind) or not NAME_RE.match(provider):
        raise ConfigError("kind and provider must be lowercase names")
    ACCOUNT_KINDS[kind] = provider


def version_key(text: str) -> tuple[Any, ...] | None:
    """A sortable key of a PEP 440 version (`0.1.0b2`, `1.2.dev3`); None when it is not one."""
    found = _VERSION_RE.match(text.strip().lower().removeprefix("v"))
    if not found:
        return None
    release, pre, pre_n, post, dev = found.groups()
    parts = [int(p) for p in release.split(".")]
    while len(parts) > 1 and parts[-1] == 0:
        parts.pop()
    if pre:
        stage = ({"a": 0, "b": 1, "rc": 2}[pre], int(pre_n))
    elif dev is not None and post is None:
        stage = (-1, 0)  # 1.0.dev1 comes before 1.0a1
    else:
        stage = (3, 0)
    post_n = -1 if post is None else int(post)
    return (tuple(parts), stage, post_n, math.inf if dev is None else int(dev))


def needs_newer_lib(min_version: str | None) -> bool:
    """True when a registry's `min_lib_version` is above this library's version.

    An unreadable version counts as newer: it can only come from a library this one predates.
    """
    if not min_version:
        return False
    wanted, own = version_key(min_version), version_key(__version__)
    return wanted is None or own is None or wanted > own


Address = ipaddress.IPv4Address | ipaddress.IPv6Address
_INET_PART_RE = re.compile(r"^(0x[0-9a-f]+|[0-9]+)$")


def host_address(host: str) -> Address | None:
    """The IP address a host names, in every form a resolver accepts, else None.

    `0x7f.1`, `2130706433` and `0177.0.0.1` are 127.0.0.1 to `inet_aton`, and `::ffff:a.b.c.d`
    is the IPv4 address it maps: each is read as that address.
    """
    text = host.strip("[]").lower().split("%", 1)[0]
    try:
        address: Address = ipaddress.ip_address(text)
    except ValueError:
        parts = text.split(".")
        if not 1 <= len(parts) <= 4 or not all(_INET_PART_RE.match(p) for p in parts):
            return None
        try:
            numbers = [
                int(p, 16)
                if p.startswith("0x")
                else int(p, 8 if len(p) > 1 and p[0] == "0" else 10)
                for p in parts
            ]
        except ValueError:  # 8 or 9 in an octal part
            return None
        *head, last = numbers
        if any(n > 255 for n in head) or last >= 256 ** (4 - len(head)):
            return None
        value = sum(n << (8 * (3 - i)) for i, n in enumerate(head)) + last
        address = ipaddress.IPv4Address(value)
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        return address.ipv4_mapped
    return address


def normal_host(host: str) -> str:
    """A host as compared with the local and allowed lists: the address, or the lowercase name."""
    address = host_address(host)
    return str(address) if address is not None else host.lower().rstrip(".")


def _frozen(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True)
class Account:
    """Where calls are made from: an AWS account (pod identity or assumed role) or an endpoint."""

    name: str
    kind: str
    enabled: bool = True
    role_arn: str | None = None
    external_id_ref: str | None = None
    base_url: str | None = None
    api_key_ref: str | None = None
    settings: Mapping[str, Any] = field(default_factory=dict)

    @property
    def provider(self) -> str:
        """The provider this account serves."""
        return ACCOUNT_KINDS[self.kind]


@dataclass(frozen=True, slots=True)
class RegionEntry:
    """One region (or inference profile) of a model in one account: a deployment before resolve.

    `name` is the entry's name (the region, or the profile's own name); `region` is where the
    client calls; `settings` are its deployment-level values (profile fields and `overrides`).
    """

    name: str
    region: str
    group: str
    profile_id: str | None = None
    routes_to: tuple[str, ...] = ()
    settings: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ModelEntry:
    """One model (alias): its provider model id, model-level settings and region entries."""

    alias: str
    type: str
    provider: str
    model_id: str
    settings: Mapping[str, Any]
    deployments: Mapping[str, tuple[RegionEntry, ...]]
    residency: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class Routing:
    """Top-level routing switches: kill switch, users always queued, live blocks."""

    kill_switch: bool = False
    allow_users: tuple[str, ...] = ()
    blocked_regions: tuple[str, ...] = ()
    blocked_accounts: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Registry:
    """The whole registry: version, defaults, routing, residency, accounts and models.

    `warnings` lists non-fatal findings (unknown names in a stored document, unused fallback
    regions, deployments outside the residency list).
    """

    version: int
    defaults: Mapping[str, Any]
    routing: Routing
    accounts: Mapping[str, Account]
    models: Mapping[str, ModelEntry]
    residency: tuple[str, ...] | None = None
    min_lib_version: str | None = None
    live_edit: str | None = None  # None: the process's `LLMQ_REGISTRY_LIVE_EDIT` applies
    warnings: tuple[str, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)
    # Provider model id -> alias, built once: `alias_of` scanned every model on each call
    # (CO-25; 55 models, the last one's id: 2674 -> 241 ns per call, Python 3.12.13, 2026-11-15).
    _by_model_id: Mapping[str, str] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Index the models by provider model id (the first model listed wins, as before)."""
        by_id: dict[str, str] = {}
        for entry in self.models.values():
            by_id.setdefault(entry.model_id, entry.alias)
        object.__setattr__(self, "_by_model_id", MappingProxyType(by_id))

    def alias_of(self, model: str) -> str:
        """The alias for `model`, given as an alias or as a provider model id (`UnknownModel`)."""
        if model in self.models:
            return model
        alias = self._by_model_id.get(model)
        if alias is None:
            raise UnknownModel(model)
        return alias


class _Collector:
    """Gathers errors and warnings with their paths while the document is walked."""

    def __init__(self, strict: bool) -> None:
        self.strict = strict
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def error(self, path: str, message: str) -> None:
        self.errors.append(f"{path}: {message}")

    def warn(self, path: str, message: str) -> None:
        self.warnings.append(f"{path}: {message}")

    def unknown(self, path: str, name: str, known: frozenset[str], where: str) -> None:
        """An unknown name: error in strict mode, warning (value ignored) otherwise."""
        close = difflib.get_close_matches(name, sorted(known), n=1)
        hint = f"; did you mean `{close[0]}`?" if close else ""
        if name in SETTINGS:
            message = f"`{name}` cannot be set {where}"
            self.error(path, message)
            return
        message = f"unknown name `{name}`{hint}"
        if self.strict:
            self.error(path, message)
        else:
            self.warn(path, message + " (ignored)")

    def mapping(self, path: str, value: Any) -> dict[str, Any] | None:
        if isinstance(value, dict):
            return value
        self.error(path, "must be an object")
        return None

    def names(self, path: str, value: Any) -> tuple[str, ...]:
        if isinstance(value, list) and all(isinstance(v, str) for v in value):
            return tuple(value)
        self.error(path, "must be a list of strings")
        return ()


def _settings(
    out: _Collector,
    path: str,
    raw: Mapping[str, Any],
    level: str,
    skip: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Check the settings of one level; return the valid ones."""
    allowed = settings_at(level)
    where = {D: "in defaults", A: "in account settings", M: "on a model", R: "on a deployment"}
    kept: dict[str, Any] = {}
    for name, value in raw.items():
        if name in skip:
            continue
        if name not in allowed:
            out.unknown(f"{path}.{name}", name, allowed | skip, where[level])
            continue
        problem = SETTINGS[name].check(value)
        if problem:
            out.error(f"{path}.{name}", problem)
            continue
        if name == "queue" and level != M and "name" in value:
            out.error(f"{path}.queue.name", "a queue name can only be set on a model")
            continue
        kept[name] = value
    return kept


def _base_url_problem(url: str, allowed_hosts: tuple[str, ...]) -> str | None:
    """What is wrong with an `openai` account's `base_url`, or None."""
    parsed = urlparse(url)
    try:
        host = parsed.hostname or ""
    except ValueError:
        return "must be an http(s) URL"
    if parsed.scheme not in ("https", "http") or not host:
        return "must be an http(s) URL"
    if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
        return "must not carry a user or password (use `api_key_ref`)"
    if any(name.lower() in _URL_SECRET_PARAMS for name, _ in parse_qsl(parsed.query, True)):
        return "must not carry a key in its query (use `api_key_ref`)"
    address = host_address(host)
    # Instance metadata lives on link-local addresses (169.254.169.254, fe80::) and, on AWS
    # over IPv6, in fd00:ec2::/32; `0.0.0.0` and multicast are never an endpoint.
    if address is not None and (
        address.is_link_local
        or address.is_unspecified
        or address.is_multicast
        or (isinstance(address, ipaddress.IPv6Address) and address in _METADATA_NET)
    ):
        return "a link-local, metadata or unspecified address is refused"
    name = normal_host(host)
    if parsed.scheme == "http" and name not in LOCAL_HOSTS:
        return f"must use https unless the host is local ({', '.join(sorted(LOCAL_HOSTS))})"
    if allowed_hosts and name not in {normal_host(h) for h in allowed_hosts}:
        return f"host `{host}` is not in the allowed hosts"
    return None


def _account(
    out: _Collector, name: str, raw: Any, allowed_hosts: tuple[str, ...]
) -> Account | None:
    path = f"accounts.{name}"
    if not NAME_RE.match(name):
        out.error(path, "account names are lowercase letters, digits, `_`, `.`, `-`")
    body = out.mapping(path, raw)
    if body is None:
        return None
    for key in body:
        if key not in _ACCOUNT_FIELDS:
            out.unknown(f"{path}.{key}", key, _ACCOUNT_FIELDS, "on an account")
    kind = body.get("kind")
    if kind not in ACCOUNT_KINDS:
        out.error(f"{path}.kind", f"must be one of {', '.join(sorted(ACCOUNT_KINDS))}")
        return None
    texts: dict[str, str | None] = {}
    for key in ("role_arn", "external_id_ref", "base_url", "api_key_ref"):
        value = body.get(key)
        if value is not None and not (isinstance(value, str) and value):
            out.error(f"{path}.{key}", "must be a non-empty string")
            value = None
        texts[key] = value
    if kind != "aws":
        for key in ("role_arn", "external_id_ref"):
            if texts[key]:
                out.error(f"{path}.{key}", f"only an `aws` account takes `{key}`")
    if texts["external_id_ref"] and not texts["role_arn"]:
        out.error(f"{path}.external_id_ref", "needs `role_arn`")
    for key in ("external_id_ref", "api_key_ref"):
        ref = texts[key]
        if ref and not _SECRET_REF_RE.match(ref):
            # Any other name could be the store password or the pod's own AWS keys, sent as a
            # Bearer token to a base_url the registry author chose.
            out.error(
                f"{path}.{key}",
                f"`{ref}` is not an account secret: an upper-case name starting with "
                f"`{SECRET_REF_PREFIX}` (never a process secret)",
            )
    if kind == "aws":
        for key in ("base_url", "api_key_ref"):
            if texts[key]:
                out.error(f"{path}.{key}", f"an `aws` account does not take `{key}`")
    if kind == "openai":
        url = texts["base_url"]
        if not url:
            out.error(f"{path}.base_url", "an `openai` account needs `base_url`")
        elif problem := _base_url_problem(url, allowed_hosts):
            out.error(f"{path}.base_url", problem)
    enabled = body.get("enabled", True)
    if not isinstance(enabled, bool):
        out.error(f"{path}.enabled", "must be true or false")
        enabled = True
    settings_raw = out.mapping(f"{path}.settings", body.get("settings", {})) or {}
    settings = _settings(out, f"{path}.settings", settings_raw, A)
    return Account(
        name=name,
        kind=kind,
        enabled=enabled,
        role_arn=texts["role_arn"],
        external_id_ref=texts["external_id_ref"],
        base_url=texts["base_url"],
        api_key_ref=texts["api_key_ref"],
        settings=_frozen(settings),
    )


def _region_entry(out: _Collector, path: str, raw: Any, group: str) -> RegionEntry | None:
    if isinstance(raw, str):
        if not NAME_RE.match(raw):
            out.error(path, f"`{raw}` is not a valid region name")
            return None
        return RegionEntry(name=raw, region=raw, group=group)
    body = out.mapping(path, raw)
    if body is None:
        return None
    name, region = body.get("name"), body.get("region")
    for key, value in (("name", name), ("region", region)):
        if not (isinstance(value, str) and NAME_RE.match(value)):
            out.error(f"{path}.{key}", "needs a valid name")
    profile = body.get("profile_id")
    if not (isinstance(profile, str) and profile):
        out.error(f"{path}.profile_id", "an inference profile needs `profile_id`")
    routes = out.names(f"{path}.routes_to", body.get("routes_to", []))
    extra = {k: v for k, v in body.items() if k not in _PROFILE_FIELDS}
    settings = _settings(out, path, extra, R)
    if not (isinstance(name, str) and isinstance(region, str) and isinstance(profile, str)):
        return None
    return RegionEntry(
        name=name,
        region=region,
        group=group,
        profile_id=profile,
        routes_to=routes,
        settings=_frozen(settings),
    )


def _deployments(
    out: _Collector, path: str, raw: Any, accounts: Mapping[str, Account]
) -> dict[str, tuple[RegionEntry, ...]]:
    result: dict[str, tuple[RegionEntry, ...]] = {}
    body = out.mapping(path, raw)
    if not body:
        if body is not None:
            out.error(path, "needs at least one account")
        return result
    for account_name, spec_raw in body.items():
        apath = f"{path}.{account_name}"
        if account_name not in accounts:
            close = difflib.get_close_matches(account_name, sorted(accounts), n=1)
            hint = f"; did you mean `{close[0]}`?" if close else ""
            out.error(apath, f"unknown account `{account_name}`{hint}")
        spec = out.mapping(apath, spec_raw)
        if spec is None:
            continue
        for key in spec:
            if key not in _DEPLOYMENT_FIELDS:
                out.unknown(f"{apath}.{key}", key, _DEPLOYMENT_FIELDS, "in deployments")
        entries: list[RegionEntry] = []
        for group in ("confirmed", "fallback"):
            listed = spec.get(group, [])
            if not isinstance(listed, list):
                out.error(f"{apath}.{group}", "must be a list")
                continue
            for i, item in enumerate(listed):
                entry = _region_entry(out, f"{apath}.{group}[{i}]", item, group)
                if entry:
                    entries.append(entry)
        seen: dict[str, str] = {}
        regions = {e.region for e in entries if e.profile_id is None}
        for entry in entries:
            if entry.name in seen:
                where = (
                    "both confirmed and fallback" if seen[entry.name] != entry.group else "twice"
                )
                out.error(f"{apath}", f"`{entry.name}` is listed {where}")
            seen[entry.name] = entry.group
            if entry.profile_id is not None and entry.name in regions:
                out.error(f"{apath}", f"profile name `{entry.name}` equals a region name")
        overrides = out.mapping(f"{apath}.overrides", spec.get("overrides", {})) or {}
        by_name = {e.name: e for e in entries}
        for target, values_raw in overrides.items():
            opath = f"{apath}.overrides.{target}"
            if target not in by_name:
                out.error(opath, f"`{target}` is not a region or profile of this account")
                continue
            values = out.mapping(opath, values_raw)
            if values is None:
                continue
            entry = by_name[target]
            merged = {**entry.settings, **_settings(out, opath, values, R)}
            by_name[target] = RegionEntry(
                name=entry.name,
                region=entry.region,
                group=entry.group,
                profile_id=entry.profile_id,
                routes_to=entry.routes_to,
                settings=_frozen(merged),
            )
        result[account_name] = tuple(by_name[e.name] for e in entries if e.name in by_name)
    return result


def _residency(out: _Collector, path: str, raw: Any) -> tuple[str, ...] | None:
    if raw is None:
        return None
    body = out.mapping(path, raw)
    if body is None:
        return None
    for key in body:
        if key != "allowed_regions":
            out.unknown(f"{path}.{key}", key, frozenset({"allowed_regions"}), "in residency")
    return out.names(f"{path}.allowed_regions", body.get("allowed_regions", []))


def _model(
    out: _Collector, alias: str, raw: Any, accounts: Mapping[str, Account]
) -> ModelEntry | None:
    path = f"models.{alias}"
    if not NAME_RE.match(alias):
        out.error(path, "an alias is lowercase letters, digits, `_`, `.`, `-`")
    body = out.mapping(path, raw)
    if body is None:
        return None
    kind = body.get("type")
    if kind not in MODEL_TYPES:
        out.error(f"{path}.type", "must be chat or embedding")
    provider = body.get("provider")
    if provider not in ACCOUNT_KINDS.values():
        known = ", ".join(sorted(set(ACCOUNT_KINDS.values())))
        out.error(f"{path}.provider", f"must be one of {known}")
    model_id = body.get("model_id")
    if not (isinstance(model_id, str) and model_id):
        out.error(f"{path}.model_id", "is required")
    settings = _settings(out, path, body, M, skip=_MODEL_FIELDS)
    deployments = _deployments(out, f"{path}.deployments", body.get("deployments"), accounts)
    if not any(e.group == "confirmed" for entries in deployments.values() for e in entries):
        out.error(f"{path}.deployments", "needs at least one confirmed region")
    residency = _residency(out, f"{path}.residency", body.get("residency"))
    if kind not in MODEL_TYPES or not isinstance(provider, str) or not isinstance(model_id, str):
        return None
    return ModelEntry(
        alias=alias,
        type=kind,
        provider=provider,
        model_id=model_id,
        settings=_frozen(settings),
        deployments=MappingProxyType(deployments),
        residency=residency,
    )


def _routing(out: _Collector, raw: Any) -> Routing:
    body = out.mapping("routing", raw if raw is not None else {})
    if body is None:
        return Routing()
    for key in body:
        if key not in _ROUTING_FIELDS:
            out.unknown(f"routing.{key}", key, _ROUTING_FIELDS, "in routing")
    kill = body.get("kill_switch", False)
    if not isinstance(kill, bool):
        out.error("routing.kill_switch", "must be true or false")
        kill = False
    return Routing(
        kill_switch=kill,
        allow_users=out.names("routing.allow_users", body.get("allow_users", [])),
        blocked_regions=out.names("routing.blocked_regions", body.get("blocked_regions", [])),
        blocked_accounts=out.names("routing.blocked_accounts", body.get("blocked_accounts", [])),
    )


def build(
    raw: Mapping[str, Any], *, strict: bool = True, allowed_hosts: tuple[str, ...] = ()
) -> tuple[Registry | None, list[str], list[str]]:
    """Build a `Registry` from the document; return it (None on errors), errors and warnings.

    Only structural checks run here; `registry.validate.check` adds the cross-field ones.
    """
    out = _Collector(strict)
    for key in raw:
        if key not in _TOP_KEYS:
            out.unknown(key, key, _TOP_KEYS, "at the top level")
    version = raw.get("registry_version")
    if not (isinstance(version, int) and not isinstance(version, bool) and version >= 1):
        out.error("registry_version", "is required: an integer of at least 1")
        version = 0
    min_lib = raw.get("min_lib_version")
    if min_lib is not None and not (isinstance(min_lib, str) and version_key(min_lib)):
        out.error("min_lib_version", "must be a version such as 0.1.0b2")
        min_lib = None
    elif strict and needs_newer_lib(min_lib):
        # The process's own config: this library cannot honour it. A stored one is read and
        # the store keeps the pod on its own config instead (RegistryStore.lib_too_old).
        out.error("min_lib_version", f"needs llm-queue {min_lib} or later (this is {__version__})")
    live_edit = raw.get("live_edit")
    if live_edit is not None and live_edit not in LIVE_EDIT_MODES:
        out.error("live_edit", f"must be one of {', '.join(LIVE_EDIT_MODES)}")
        live_edit = None
    defaults_raw = out.mapping("defaults", raw.get("defaults", {})) or {}
    defaults = _settings(out, "defaults", defaults_raw, D)
    accounts_raw = out.mapping("accounts", raw.get("accounts")) or {}
    if not accounts_raw:
        out.error("accounts", "needs at least one account")
    accounts = {
        name: account
        for name, body in accounts_raw.items()
        if (account := _account(out, name, body, allowed_hosts)) is not None
    }
    models_raw = out.mapping("models", raw.get("models")) or {}
    if not models_raw:
        out.error("models", "needs at least one model")
    models = {
        alias: model
        for alias, body in models_raw.items()
        if (model := _model(out, alias, body, accounts)) is not None
    }
    registry = Registry(
        version=version,
        defaults=_frozen(defaults),
        routing=_routing(out, raw.get("routing")),
        accounts=MappingProxyType(accounts),
        models=MappingProxyType(models),
        residency=_residency(out, "residency", raw.get("residency")),
        min_lib_version=min_lib,
        live_edit=live_edit,
        raw=_frozen(raw),
    )
    if out.errors:
        return None, out.errors, out.warnings
    return registry, out.errors, out.warnings
