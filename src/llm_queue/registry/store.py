"""The registry in the state store: publish by version, live edits, audit, rollback, reload.

Every write is one Lua call that stores the document, its version record, an audit entry (path
diffs, secrets redacted) and each model's resolved numbers together; publishes and live edits
are compare-and-set on the change counter. Routing blocks and the kill switch set live survive
a publish. Readers keep the last good registry when the stored one is unreadable, invalid or
needs a newer library; a pod that has none runs on its own document. The reload loop
republishes the config when the stored registry was lost or invalid (and not newer).
"""

from __future__ import annotations

import asyncio
import difflib
import hashlib
import os
import random
import socket
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import orjson
from redis.exceptions import RedisError

from llm_queue._logging import get_logger
from llm_queue._version import __version__
from llm_queue.engine.store_guard import store_op
from llm_queue.errors import ConfigError, LLMQueueError, StoreUnavailable
from llm_queue.registry import _lua
from llm_queue.registry.resolve import ResolvedRegistry
from llm_queue.registry.schema import LIVE_EDIT_MODES, needs_newer_lib
from llm_queue.registry.settings import SETTINGS
from llm_queue.registry.validate import RegistryError, check

if TYPE_CHECKING:
    import redis.asyncio

    from llm_queue.engine.metrics import Metrics
    from llm_queue.registry.schema import Registry

logger = get_logger(__name__)

AUDIT_RETENTION_DAYS = 90
_SECRET_FIELDS = frozenset({"role_arn", "external_id_ref", "api_key_ref"})
_ACTOR_MAX = 200
# First retry of a lost publish / edit waits up to 40 ms, doubling per try (jittered, CO-10).
_CONFLICT_BACKOFF_SECS = 0.02
# ... and never more than this: uncapped, try 21 of a publish could wait 0.02 * 2**21 s, about
# 11.6 h (NB-3). `Tuning.registry.edit_retries` is also bounded (10).
_CONFLICT_BACKOFF_MAX_SECS = 2.0
# A reload loop that failed waits up to this long (doubling from its interval) before the next.
_RELOAD_BACKOFF_MAX_SECS = 30.0
# Live routing switches a publish carries over from the stored document (an emergency block must
# not be undone by the next deploy; only a live unblock lifts it).
_CARRIED_LISTS = ("blocked_regions", "blocked_accounts")
# Why each setting marked `protected` in `SETTINGS` is (a unit test keeps the two in step).
_PROTECTED_WHY = {
    "debug_capture": "it stores prompt text",
    "model_fallback": "it sets where calls may go",
    "params": "deployment params are sent with each call",
}
# Settings whose value goes into a model's resolved numbers (read by the admission script).
ADMISSION_SETTINGS = (
    "enabled",
    "rpm",
    "tpm",
    "rate_limits",
    "rate_window",
    "rate_burst",
    "tpm_output_weight",
    "max_concurrency",
    "weight",
    "attempt_timeout_secs",
    "breaker_threshold",
    "breaker_cooldown_secs",
    "breaker_window_secs",
    "breaker_probe_secs",
)
MODEL_ADMISSION_SETTINGS = (
    "balance",
    "balance_priority",
    "balance_opts",
    "priority_reserve",
    "normal_min_share",
    "require_headroom_pct",
    "max_waiting",
    "health",
)


class RegistryConflict(ConfigError):
    """A publish or live edit lost the race with other writes on every try it had.

    The number of tries is `Tuning.registry.edit_retries`.
    """


class LiveEditRefused(ConfigError):
    """A live edit touched fields that change only by deploy (or live edits are off)."""


Path = tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PublishResult:
    """What a publish did: `published`, `kept`, `not_bumped` or `ignored`, and the version live.

    `carried` lists the live routing switches kept from the stored document (blocked regions
    and accounts, the kill switch), e.g. after a rollback (Q41).
    """

    action: str
    version: int
    carried: tuple[str, ...] = ()


def content_hash(doc: Mapping[str, Any]) -> str:
    """Stable hash of a document (sorted keys), to spot a config change without a version bump."""
    return hashlib.sha256(orjson.dumps(doc, option=orjson.OPT_SORT_KEYS)).hexdigest()


def diff(before: Any, after: Any, path: Path = ()) -> list[tuple[Path, Any, Any]]:
    """Leaf changes between two documents as (path, before, after); lists compare whole."""
    if isinstance(before, dict) and after is None:
        after = {}
    if isinstance(after, dict) and before is None:
        before = {}
    if isinstance(before, dict) and isinstance(after, dict):
        out: list[tuple[Path, Any, Any]] = []
        for key in sorted(set(before) | set(after)):
            out += diff(before.get(key), after.get(key), (*path, key))
        return out
    return [] if before == after else [(path, before, after)]


def _redact(path: Path, value: Any) -> Any:
    if value is not None and path and path[-1] in _SECRET_FIELDS:
        return "[redacted]"
    if isinstance(value, dict):
        return {k: _redact((*path, k), v) for k, v in value.items()}
    return value


def audit_diff(before: Mapping[str, Any], after: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The audit form of a change: one entry per changed path, secrets redacted."""
    return [
        {"path": ".".join(p), "before": _redact(p, b), "after": _redact(p, a)}
        for p, b, a in diff(dict(before), dict(after))
    ]


def _setting_of(path: Path) -> str | None:
    """The registry setting a changed path sets (`defaults.rpm`, `models.m.rpm`, ...), or None.

    Settings sit under `defaults`, an account's `settings`, a model, and a deployment's
    `overrides.<region>`.
    """
    name: str | None = None
    if path[:1] == ("defaults",) and len(path) >= 2:
        name = path[1]
    elif path[:1] == ("accounts",) and len(path) >= 4 and path[2] == "settings":
        name = path[3]
    elif path[:1] == ("models",) and len(path) >= 3:
        if path[2] != "deployments":
            name = path[2]
        elif len(path) >= 7 and path[4] == "overrides":
            name = path[6]
    return name if name in SETTINGS else None


def _deploy_only(path: Path) -> str | None:
    """Why a changed path needs a deploy (a new `registry_version`), or None.

    Settings marked `deploy_only` in `SETTINGS` come from that table (CO-11: the flag was
    set but never read, and the same rule was written out here by name); the document's
    structure (versions, models, model fields, queue names) is listed here.
    """
    if path[:1] in (("registry_version",), ("min_lib_version",), ("live_edit",)):
        return "changes with a deploy"
    if path[:1] == ("models",) and len(path) == 2:
        return "adding or removing a model needs a new registry_version"
    if path[:1] == ("models",) and len(path) >= 3:
        if path[2] in ("type", "provider", "model_id"):
            return f"`{path[2]}` changes only with a deploy"
        if path[2:4] == ("queue", "name"):
            return "a queue name changes only with a deploy"
    name = _setting_of(path)
    if name is not None and SETTINGS[name].deploy_only:
        return f"`{name}` changes only with a deploy"
    return None


def _protected(path: Path) -> str | None:
    """Why a changed path is protected under `registry_live_edit=safe`, or None.

    Settings marked `protected` in `SETTINGS` come from that table (CO-11); the rest are
    structural (residency, accounts, region lists) or part of one setting (`inference.extra`).
    """
    name = _setting_of(path)
    if name is not None and SETTINGS[name].protected:
        return f"`{name}` changes only with a deploy ({_PROTECTED_WHY[name]})"
    if "residency" in path:
        return "residency changes only with a deploy"
    if "inference" in path and path[path.index("inference") + 1 :][:1] == ("extra",):
        return "inference.extra (fields sent to the provider) changes only with a deploy"
    if path[:1] == ("accounts",):
        if len(path) == 2:
            return "adding or removing an account changes only with a deploy"
        if len(path) >= 3 and path[2] in ("kind", "base_url", *_SECRET_FIELDS):
            return f"account `{path[2]}` changes only with a deploy"
    in_deployments = path[:1] == ("models",) and len(path) >= 4 and path[2] == "deployments"
    if in_deployments and (len(path) == 4 or path[4] in ("confirmed", "fallback")):
        return "region lists change only with a deploy"
    return None


def refused_edits(before: Mapping[str, Any], after: Mapping[str, Any], mode: str) -> list[str]:
    """Reasons a live edit from `before` to `after` is refused under `registry_live_edit=mode`."""
    if mode not in LIVE_EDIT_MODES:  # ConfigError: one error family (CO-24)
        raise ConfigError(f"registry_live_edit must be one of {', '.join(LIVE_EDIT_MODES)}")
    changes = diff(dict(before), dict(after))
    if mode == "off" and changes:
        return ["live edits are off (registry_live_edit=off)"]
    reasons = []
    whole = []
    for section, what in (("models", "a model"), ("accounts", "an account")):
        old, new = set(before.get(section, {})), set(after.get(section, {}))
        for name in sorted(old ^ new):
            whole.append((section, name))
            reasons.append(
                f"{section}.{name}: adding or removing {what} needs a new registry_version"
            )
    for path, _, _ in changes:
        if path[:2] in whole:
            continue
        why = _deploy_only(path) or (_protected(path) if mode == "safe" else None)
        if why:
            reasons.append(f"{'.'.join(path)}: {why}")
    return reasons


def protected_mismatches(
    config: Mapping[str, Any], stored: Mapping[str, Any]
) -> list[tuple[str, str]]:
    """(path, why) of every protected or deploy-only field where `stored` differs from `config`.

    The same rules as a live edit under `registry_live_edit=safe` (`_deploy_only`,
    `_protected`, models and accounts added or removed), plus `live_edit` itself. The version
    fields and the live routing switches are not compared.
    """
    out: list[tuple[str, str]] = []
    for path, _, _ in diff(dict(config), dict(stored)):
        if path[:1] in (("registry_version",), ("min_lib_version",)):
            continue
        why = _deploy_only(path) or _protected(path)
        if why is None and path[:1] in (("models",), ("accounts",)) and len(path) == 2:
            why = "adding or removing a model or an account changes only with a deploy"
        if why is not None:
            out.append((".".join(path), why))
    return out


def carried_routing(doc: Mapping[str, Any], merged: Mapping[str, Any]) -> tuple[str, ...]:
    """The live routing switches `merged` keeps that `doc` did not have (Q41), as text.

    `routing.blocked_regions: r1`, `routing.kill_switch`: what a publish or rollback kept
    from the stored document.
    """
    raw_given, raw_routing = doc.get("routing"), merged.get("routing")
    given: Mapping[str, Any] = raw_given if isinstance(raw_given, Mapping) else {}
    routing: Mapping[str, Any] = raw_routing if isinstance(raw_routing, Mapping) else {}
    out = [
        f"routing.{name}: {value}"
        for name in _CARRIED_LISTS
        for value in routing.get(name) or ()
        if value not in (given.get(name) or ())
    ]
    if routing.get("kill_switch") is True and given.get("kill_switch") is not True:
        out.append("routing.kill_switch")
    return tuple(out)


def check_actor(actor: str) -> str:
    """The audit actor, refused (`ConfigError`) when empty, too long or holding control codes.

    Audit rows and log lines print it as given; a newline or escape sequence there could forge
    a second entry or rewrite a terminal.
    """
    if not actor.strip() or len(actor) > _ACTOR_MAX:
        raise ConfigError(f"the audit actor must be 1 to {_ACTOR_MAX} characters")
    if any(ord(c) < 32 or 127 <= ord(c) < 160 for c in actor):
        raise ConfigError("the audit actor must not contain control characters")
    return actor


def process_actor(op: str) -> str:
    """The actor of a write the process makes itself: `boot@host:pid`, `restore@host:pid`."""
    return f"{op}@{socket.gethostname()}:{os.getpid()}"


def carry_routing(stored: Mapping[str, Any] | None, doc: Mapping[str, Any]) -> dict[str, Any]:
    """`doc` with the stored document's live blocks and kill switch added (R5).

    Blocked regions and accounts are the union of both (stored first), and the kill switch is on
    when either has it on. Malformed stored values are skipped (the stored document may be the
    invalid one being replaced).
    """
    out = dict(doc)
    old = stored.get("routing") if isinstance(stored, Mapping) else None
    if not isinstance(old, Mapping):
        return out
    routing = dict(out.get("routing") or {})
    for name in _CARRIED_LISTS:
        before = old.get(name)
        if isinstance(before, list) and all(isinstance(v, str) for v in before):
            own = routing.get(name, [])
            mine = own if isinstance(own, list) else []
            routing[name] = [*before, *(v for v in mine if v not in before)]
    if old.get("kill_switch") is True:
        routing["kill_switch"] = True
    if routing:
        out["routing"] = routing
    return out


def _new_path_errors(before: Mapping[str, Any], after: Mapping[str, Any], hosts: Any) -> list[str]:
    """Strict-mode problems at paths a live edit adds or changes (R2).

    A stored document is read leniently (an older pod skips names a newer one wrote), but an
    edit's own new names must be known: a typo such as `rmp` was accepted and did nothing.
    """
    registry, errors, _ = check(after, strict=True, allowed_hosts=hosts)
    if registry is not None:
        return []
    changed = [".".join(path) for path, _, _ in diff(dict(before), dict(after))]
    found = []
    for error in errors:
        where = error.split(": ", 1)[0]
        if any(where == path or where.startswith(path + ".") for path in changed):
            found.append(error)
    return found


def _set_path(doc: dict[str, Any], path: Sequence[str], value: Any) -> None:
    """Set (or delete, value None) one path, creating objects on the way."""
    node = doc
    for key in path[:-1]:
        child = node.get(key)
        if not isinstance(child, dict):
            child = {}
            node[key] = child
        node = child
    if value is None:
        node.pop(path[-1], None)
    else:
        node[path[-1]] = value


def split_path(doc: Mapping[str, Any], dotted: str) -> list[str]:
    """Split a dotted path, keeping names that contain dots (`deployments.aws-main.x.y`) whole.

    At each level the longest run of segments that names an existing key wins.
    """
    parts = dotted.split(".")
    out: list[str] = []
    node: Any = doc
    i = 0
    while i < len(parts):
        chosen = 1
        if isinstance(node, Mapping):
            for j in range(len(parts), i, -1):
                if ".".join(parts[i:j]) in node:
                    chosen = j - i
                    break
        key = ".".join(parts[i : i + chosen])
        out.append(key)
        node = node.get(key) if isinstance(node, Mapping) else None
        i += chosen
    return out


def admission_numbers(view: ResolvedRegistry, alias: str) -> dict[str, Any]:
    """A model's resolved numbers for the admission script: model values, one row per deployment."""
    model = {name: view.value(alias, name) for name in MODEL_ADMISSION_SETTINGS}
    deployments = []
    for dep in view.deployments(alias):
        row = {name: view.value(alias, name, dep.key) for name in ADMISSION_SETTINGS}
        # A model disabled as a whole still serves its waiting calls (drain, plan §3.18).
        if view.effective(alias, "enabled", dep.key).level == "model":
            row["enabled"] = True
        account = view.registry.accounts[dep.account]
        routing = view.registry.routing
        row.update(
            key=dep.key,
            account=dep.account,
            group=dep.group,
            region=dep.region,
            outside_residency=view.outside_residency(dep),
            account_enabled=account.enabled,
            blocked=dep.region in routing.blocked_regions
            or dep.account in routing.blocked_accounts,
            shared_rpm=account.settings.get("shared_rpm"),
            shared_tpm=account.settings.get("shared_tpm"),
            account_priority_reserve=account.settings.get("priority_reserve", 0),
        )
        deployments.append(row)
    return {"model": model, "deployments": deployments}


class RegistryStore:
    """The registry of one environment (key prefix) in the state store.

    `current` is the last good `ResolvedRegistry`; hot paths read it without I/O. `config` is
    this process's own document, used by `boot()` and to restore a lost registry. For metrics:
    `invalid_count` counts refused reads, `stored_refused` is True while the stored document is
    refused (invalid or needing a newer library), `lib_too_old` while it needs a newer library
    (this pod never publishes over it), `on_own_config` while the pod runs on its own document.
    """

    def __init__(
        self,
        client: redis.asyncio.Redis,
        prefix: str,
        *,
        config: Mapping[str, Any] | None = None,
        live_edit: str = "safe",
        allowed_hosts: tuple[str, ...] = (),
        audit_retention_days: int = AUDIT_RETENTION_DAYS,
        history_size: int = 20,
        edit_retries: int = 3,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Keep the client and settings; nothing is read or written until `boot` or `load`."""
        if live_edit not in LIVE_EDIT_MODES:
            raise ConfigError(f"registry_live_edit must be one of {', '.join(LIVE_EDIT_MODES)}")
        self._client = client
        self.prefix = prefix
        self._history_size = history_size  # `Tuning.registry`
        self._edit_retries = edit_retries
        self.config = dict(config) if config is not None else None
        self.live_edit = live_edit
        self._hosts = allowed_hosts
        self._retention_ms = audit_retention_days * 86_400_000
        self._sleep = sleep
        self._publish = client.register_script(_lua.PUBLISH)
        self._edit = client.register_script(_lua.EDIT)
        self._current: ResolvedRegistry | None = None
        self._changes = -1
        # The engine's metrics sink (set by `Engine`): reload failures are counted (CO-4).
        self.metrics: Metrics | None = None
        self._version = 0
        self.invalid_count = 0
        self.stored_refused = False
        self.lib_too_old = False
        self.on_own_config = False
        self._warned: set[str] = set()
        self._loading: asyncio.Future[ResolvedRegistry | None] | None = None

    # ---------------------------------------------------------------- keys

    @property
    def doc_key(self) -> str:
        """The stored document (JSON string)."""
        return f"{self.prefix}:{{reg}}:doc"

    @property
    def meta_key(self) -> str:
        """Version record: `version`, `config_hash`, `changes`, `published`."""
        return f"{self.prefix}:{{reg}}:meta"

    @property
    def audit_key(self) -> str:
        """Audit stream: one entry per publish or edit, path diffs with secrets redacted."""
        return f"{self.prefix}:{{reg}}:audit"

    @property
    def history_key(self) -> str:
        """Published versions kept for rollback (hash version -> document)."""
        return f"{self.prefix}:{{reg}}:history"

    def numbers_key(self, alias: str) -> str:
        """A model's resolved numbers and the change counter they belong to."""
        return f"{self.prefix}:{{m:{alias}}}:cfg"

    # ---------------------------------------------------------------- reading

    @property
    def current(self) -> ResolvedRegistry:
        """Last good registry; raises `ConfigError` before the first successful load."""
        if self._current is None:
            raise ConfigError(
                "the registry has not been loaded yet; call LLMQueue.start() "
                "(or RegistryStore.boot / load) first"
            )
        return self._current

    @property
    def changes(self) -> int:
        """Change counter of the loaded registry (-1 before the first load)."""
        return self._changes

    async def _read(self) -> tuple[dict[str, Any] | None, dict[str, str]]:
        # One transaction: a publish between the two reads paired a document with another
        # document's change counter (a torn read).
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.get(self.doc_key)
            pipe.hgetall(self.meta_key)
            raw, meta = await pipe.execute()
        meta_text = {_text(k): _text(v) for k, v in (meta or {}).items()}
        if raw is None:
            return None, meta_text
        try:
            doc = orjson.loads(raw)
        except orjson.JSONDecodeError:
            return {"__invalid__": _text(raw)[:200]}, meta_text
        if not isinstance(doc, dict):
            return {"__invalid__": _text(raw)[:200]}, meta_text
        return doc, meta_text

    def _readable(self, doc: Mapping[str, Any]) -> Registry | None:
        """The stored document as a registry, or None (logged once) when this pod refuses it.

        A pod that holds its own config also refuses a stored document whose protected fields
        (accounts and their roles, model ids, residency, region lists, `live_edit`, ...) differ
        from it (owner decision Q25, R3SE-3): anyone who could write the key could send prompts
        to another AWS account or model after a schema check only.
        """
        registry, errors, _ = check(doc, strict=False, allowed_hosts=self._hosts)
        if registry is not None and needs_newer_lib(registry.min_lib_version):
            errors = [
                f"min_lib_version: needs llm-queue {registry.min_lib_version} or later "
                f"(this is {__version__}); this pod keeps its registry and never publishes"
            ]
            registry = None
        mismatched = (
            protected_mismatches(self.config, doc)
            if registry is not None and self.config is not None
            else []
        )
        if mismatched:
            errors = [
                f"{path}: differs from this pod's deploy config ({why})" for path, why in mismatched
            ]
            registry = None
            self._count("llmq_registry_refused_total", reason="protected_mismatch")
        if registry is None:
            key = "|".join(errors)
            if key not in self._warned:
                self._warned.add(key)
                logger.warning("registry_invalid", problems=errors[:20], count=len(errors))
        return registry

    def _accept(self, doc: Mapping[str, Any], meta: Mapping[str, str]) -> bool:
        registry = self._readable(doc)
        # Admission reads the numbers stored with this counter whatever the pod thinks of the
        # document: keeping an older counter made every call loop on `stale_config` (R1).
        self._changes = int(meta.get("changes", "0"))
        if registry is None:
            self.invalid_count += 1
            self.stored_refused = True
            self.lib_too_old = isinstance(doc.get("min_lib_version"), str) and needs_newer_lib(
                doc["min_lib_version"]
            )
            return False
        for warning in registry.warnings:
            if warning not in self._warned:
                self._warned.add(warning)
                logger.warning("registry_warning", warning=warning)
        self._current = ResolvedRegistry(registry)
        self._version = registry.version
        self.stored_refused = self.lib_too_old = self.on_own_config = False
        return True

    def live_edit_mode(self, doc: Mapping[str, Any]) -> str:
        """The live-edit mode of a stored document: its `live_edit`, else the process setting."""
        mode = doc.get("live_edit")
        return mode if mode in LIVE_EDIT_MODES else self.live_edit

    async def load(self) -> ResolvedRegistry | None:
        """Read the stored registry; keep the last good one when it is unreadable or refused.

        Returns None when there is no stored registry (nothing published yet, or it was lost).
        Concurrent calls share one read: every call that saw `stale_config` used to reload.
        """
        pending = self._loading
        if pending is None or pending.done():
            pending = self._loading = asyncio.ensure_future(self._load())
        return await asyncio.shield(pending)

    async def _load(self) -> ResolvedRegistry | None:
        try:
            doc, meta = await self._read()
        except (RedisError, OSError) as exc:
            logger.warning("registry_unreachable", error=str(exc))
            return self._current
        if doc is None:
            return None
        self._accept(doc, meta)
        return self._current

    # ---------------------------------------------------------------- publishing

    def _numbers(self, view: ResolvedRegistry) -> tuple[list[str], list[str]]:
        aliases = sorted(view.registry.models)
        keys = [self.numbers_key(a) for a in aliases]
        values = [orjson.dumps(admission_numbers(view, a)).decode() for a in aliases]
        return keys, values

    @store_op
    async def publish(self, doc: Mapping[str, Any], *, actor: str) -> PublishResult:
        """Publish a document by the version rule; raise `RegistryError` when it is invalid.

        The stored document's routing blocks and kill switch are carried over (R5). A stored
        document this library cannot read is treated as missing when its version is not above
        `doc`'s (R1); one that needs a newer library is never replaced (`lib_too_old`, C5).
        """
        check_actor(actor)
        registry, errors, warnings = check(doc, strict=True, allowed_hosts=self._hosts)
        if registry is None:
            raise RegistryError(errors, warnings)
        tries = self._edit_retries + 2
        for attempt in range(tries):
            if attempt:
                await self._backoff(attempt)
            stored, meta = await self._read()
            replace = False
            if stored is not None:
                min_lib = stored.get("min_lib_version")
                if isinstance(min_lib, str) and needs_newer_lib(min_lib):
                    self.lib_too_old = True
                    stored_version = int(meta.get("version", "0"))
                    logger.warning("registry_needs_newer_library", needs=min_lib, have=__version__)
                    await self._load()
                    return PublishResult("lib_too_old", stored_version)
                replace = self._readable(stored) is None
            merged = carry_routing(None if replace else stored, doc)
            merged_registry, errors, warnings = check(
                merged, strict=True, allowed_hosts=self._hosts
            )
            if merged_registry is None:  # cannot happen for valid routing; never publish it
                raise RegistryError(errors, warnings)
            changes = orjson.dumps(audit_diff(stored or {}, merged)).decode()
            keys, values = self._numbers(ResolvedRegistry(merged_registry))
            gone = [self.numbers_key(a) for a in _removed_models(stored, merged_registry)]
            reply = await self._publish(
                [self.doc_key, self.meta_key, self.audit_key, self.history_key, *keys, *gone],
                [
                    orjson.dumps(merged).decode(),
                    registry.version,
                    content_hash(doc),  # of the config as given: a redeploy of it is `kept`
                    actor,
                    changes,
                    self._retention_ms,
                    self._history_size,
                    int(meta.get("changes", "0")),
                    "1" if replace else "0",
                    len(gone),
                    *values,
                ],
            )
            action, version = _text(reply[0]), int(reply[1])
            if action == "conflict":
                continue
            if action == "not_bumped":
                logger.warning("registry_version_not_bumped", version=version)
            elif action == "ignored":
                logger.info("registry_older_ignored", stored=version, config=registry.version)
            elif action == "published":
                if replace:
                    logger.error("registry_invalid_replaced", version=version, actor=actor)
                logger.info("registry_published", version=version, actor=actor)
                logger.info("llmq_registry_audit", actor=actor, op="publish", diff=changes)
            await self._load()  # a fresh read, never one that started before the write
            return PublishResult(action, version, carried_routing(doc, merged))
        raise RegistryConflict(
            f"the registry kept changing during a publish ({tries} tries); try again"
        )

    def run_on_config(self) -> ResolvedRegistry:
        """Run on this process's own document (the stored one is unreachable or refused).

        Also the engine's fallback when `boot` runs out of time at start (a blackholed store).
        """
        assert self.config is not None
        registry, errors, warnings = check(self.config, allowed_hosts=self._hosts)
        if registry is None:
            raise RegistryError(errors, warnings)
        self._current = ResolvedRegistry(registry)
        self._version = registry.version
        self.on_own_config = True
        return self._current

    async def boot(self, *, actor: str | None = None) -> ResolvedRegistry:
        """Publish this process's config by the version rule, then load what is live.

        When the store cannot be reached, or the live document is one this pod refuses (invalid
        and newer, or needing a newer library), the process starts on its own config (plan
        §3.18, R1). The audit actor defaults to `boot@host:pid`.
        """
        if self.config is None:
            raise ConfigError("RegistryStore.boot needs the process's own registry (config)")
        try:
            await self.publish(self.config, actor=actor or process_actor("boot"))
        except (RedisError, OSError) as exc:
            logger.warning("registry_store_unreachable_at_boot", error=str(exc))
            return self.run_on_config()
        if self._current is None:
            logger.error("registry_stored_refused_running_on_config", lib_too_old=self.lib_too_old)
            return self.run_on_config()
        return self.current

    # ---------------------------------------------------------------- live edits

    @store_op
    async def edit(
        self, change: Callable[[dict[str, Any]], None], *, actor: str
    ) -> ResolvedRegistry:
        """Apply `change` to the stored document as a live edit: validated, checked, audited.

        Compare-and-set on the change counter; on a race the document is read again and the
        change re-applied, up to `Tuning.registry.edit_retries` tries with a jittered backoff
        between them; `RegistryConflict` when every try lost.
        """
        check_actor(actor)
        for attempt in range(self._edit_retries):
            if attempt:
                await self._backoff(attempt)
            doc, meta = await self._read()
            if doc is None:
                raise ConfigError("no registry is stored; publish one first")
            min_lib = doc.get("min_lib_version")
            if isinstance(min_lib, str) and needs_newer_lib(min_lib):
                raise ConfigError(
                    f"the registry needs llm-queue {min_lib} or later (this is {__version__}); "
                    "edit it from a newer version"
                )
            after = orjson.loads(orjson.dumps(doc))
            change(after)
            reasons = refused_edits(doc, after, self.live_edit_mode(doc))
            if reasons:
                logger.warning("registry_edit_refused", actor=actor, reasons=reasons)
                raise LiveEditRefused("; ".join(reasons))
            registry, errors, warnings = check(after, strict=False, allowed_hosts=self._hosts)
            if registry is None:
                raise RegistryError(errors, warnings)
            if new_errors := _new_path_errors(doc, after, self._hosts):
                raise RegistryError(new_errors, warnings)
            changes = orjson.dumps(audit_diff(doc, after)).decode()
            keys, values = self._numbers(ResolvedRegistry(registry))
            reply = await self._edit(
                [self.doc_key, self.meta_key, self.audit_key, self.history_key, *keys],
                [
                    orjson.dumps(after).decode(),
                    int(meta.get("changes", "0")),
                    actor,
                    changes,
                    self._retention_ms,
                    *values,
                ],
            )
            if _text(reply[0]) == "ok":
                logger.info("llmq_registry_audit", actor=actor, op="edit", diff=changes)
                loaded = await self._load()
                if loaded is None:  # was an assert: AssertionError, or None under -O (CO-12)
                    raise StoreUnavailable(
                        "the registry vanished right after the edit (was the store flushed?)"
                    )
                return loaded
        raise RegistryConflict(
            f"the registry kept changing ({self._edit_retries} tries); try again"
        )

    async def _backoff(self, attempt: int) -> None:
        """Wait a jittered, doubling time before try `attempt` (1 = the first retry).

        Retrying a lost compare-and-set at once made concurrent editors collide again and use
        up their tries together (CO-10).
        """
        cap = min(_CONFLICT_BACKOFF_MAX_SECS, _CONFLICT_BACKOFF_SECS * 2**attempt)
        await self._sleep(random.uniform(0, cap))

    @store_op
    async def set(
        self, values: Mapping[str, Any], *, actor: str, model: str | None = None
    ) -> ResolvedRegistry:
        """Set dotted paths (`{"rpm": 400}` with `model=`, or from the root); None deletes."""

        def change(doc: dict[str, Any]) -> None:
            base = doc
            prefix: list[str] = []
            if model is not None:
                alias = self.current.registry.alias_of(model) if self._current else model
                prefix = ["models", alias]
                base = doc.get("models", {}).get(alias, {})
            for dotted, value in values.items():
                _set_path(doc, [*prefix, *split_path(base, dotted)], value)

        return await self.edit(change, actor=actor)

    @store_op
    async def set_enabled(
        self, model: str, deployment: str, enabled: bool, *, actor: str
    ) -> ResolvedRegistry:
        """Enable or disable one deployment (`account/name`) of a model."""
        account, _, name = deployment.partition("/")
        alias = self.current.registry.alias_of(model)
        path = ["models", alias, "deployments", account, "overrides", name, "enabled"]

        def change(doc: dict[str, Any]) -> None:
            _set_path(doc, path, None if enabled else False)

        return await self.edit(change, actor=actor)

    @store_op
    async def block(
        self, *, region: str | None = None, account: str | None = None, on: bool, actor: str
    ) -> ResolvedRegistry:
        """Block (or unblock) a region or an account for every model.

        `ConfigError` when neither is given, or when blocking a name the registry does not use
        (a typo would otherwise block nothing and look done).
        """
        if not region and not account:
            raise ConfigError("give a region or an account to block")
        if on:
            view = self.current
            accounts = set(view.registry.raw.get("accounts", {}))
            regions = {d.region for alias in view.registry.models for d in view.deployments(alias)}
            if account and account not in accounts:
                raise ConfigError(f"unknown account {account!r}{_known(account, accounts)}")
            if region and region not in regions:
                raise ConfigError(f"no deployment uses region {region!r}{_known(region, regions)}")

        def change(doc: dict[str, Any]) -> None:
            routing = doc.setdefault("routing", {})
            for field, value in (("blocked_regions", region), ("blocked_accounts", account)):
                if value is None:
                    continue
                listed = [v for v in routing.get(field, []) if v != value]
                routing[field] = [*listed, value] if on else listed

        return await self.edit(change, actor=actor)

    # ---------------------------------------------------------------- history, rollback, diff

    @store_op
    async def log(self, count: int = 20, path_prefix: str = "") -> list[dict[str, Any]]:
        """Latest audit entries, newest first; `path_prefix` keeps entries that touch it.

        Best effort: it looks at the newest `count * 5` entries, so a narrow `path_prefix` can
        return fewer than `count`; unreadable entries are skipped (logged).
        """
        entries = await self._client.xrevrange(self.audit_key, count=count * 5)
        out: list[dict[str, Any]] = []
        for entry_id, fields in entries or ():
            row = {_text(k): _text(v) for k, v in (fields or {}).items()}
            try:
                changes = orjson.loads(row.get("diff", "[]"))
                paths = [str(c["path"]) for c in changes]
            except (orjson.JSONDecodeError, TypeError, KeyError):
                # One malformed entry (a manual XADD, an older format) failed the whole read
                # with a non-LLMQueueError (CO-19).
                logger.warning("audit_entry_unreadable", id=_text(entry_id))
                continue
            if path_prefix and not any(p.startswith(path_prefix) for p in paths):
                continue
            out.append({"id": _text(entry_id), **row, "diff": changes})
            if len(out) == count:
                break
        return out

    @store_op
    async def versions(self) -> list[int]:
        """Published versions still kept for rollback, oldest first."""
        return sorted(int(_text(v)) for v in await self._client.hkeys(self.history_key))

    @store_op
    async def rollback(self, version: int, *, actor: str) -> PublishResult:
        """Publish the content of an earlier version as a new, higher version.

        The new version is the stored one plus one; when a deploy publishes in between (the
        rollback comes back `ignored` or `not_bumped`), it is read and tried again, up to
        `Tuning.registry.edit_retries` tries, then `RegistryConflict`. Each try is a whole
        `publish`, which itself retries `edit_retries + 2` times on a lost race: at most
        `edit_retries * (edit_retries + 2)` publishes in all (3 x 5 by default; 10 x 12, about
        135 s of backoff, at the maximum), not the 12.5 s bound of one publish (FC-16).

        The live routing blocks and kill switch are kept, as for any publish, and the result's
        `carried` lists them (Q41): a rollback never lifts an emergency block.
        """
        check_actor(actor)
        raw = await self._client.hget(self.history_key, str(version))
        if raw is None:
            raise ConfigError(f"version {version} is not kept any more")
        try:
            kept = orjson.loads(raw)
        except orjson.JSONDecodeError as exc:
            raise ConfigError(f"version {version} is stored unreadable: {exc}") from exc
        if not isinstance(kept, dict):
            raise ConfigError(f"version {version} is stored unreadable: not a JSON object")
        note = f" (rollback to {version})"
        composed = actor[: _ACTOR_MAX - len(note)] + note  # a long actor went past the limit
        for attempt in range(self._edit_retries):
            if attempt:
                await self._backoff(attempt)
            _, meta = await self._read()
            doc = {**kept, "registry_version": int(meta.get("version", "0")) + 1}
            result = await self.publish(doc, actor=composed)
            # A version read before a deploy's publish lost to it and did nothing (CO-18).
            if result.action not in ("ignored", "not_bumped"):
                return result
        raise RegistryConflict(
            f"the registry kept changing during a rollback ({self._edit_retries} tries); try again"
        )

    @store_op
    async def diff_with_config(self) -> list[dict[str, Any]]:
        """What is live but not in this process's config (live edits a deploy would drop)."""
        doc, _ = await self._read()
        if self.config is None or doc is None:
            return []
        return [
            change
            for change in audit_diff(self.config, doc)
            if change["path"] != "registry_version"
        ]

    # ---------------------------------------------------------------- reload loop

    async def refresh(self) -> ResolvedRegistry | None:
        """One reload: load, and restore the config when the stored registry was lost.

        A missing or older stored registry while this process holds a newer one means the store
        was flushed or failed over: republish by the boot rule and alert (live edits are lost).
        A stored registry this pod refuses as invalid counts as missing when the pod's own
        version is not below it (R1); one that needs a newer library is left alone (C5).
        """
        held = self._version
        try:
            doc, meta = await self._read()
        except (RedisError, OSError) as exc:
            logger.warning("registry_unreachable", error=str(exc))
            return self._current
        if doc is not None:
            self._accept(doc, meta)
        if self.config is None:
            return self._current if doc is not None else None
        stored = int(meta.get("version", "0")) if doc is not None else 0
        own = self.config.get("registry_version")
        own_version = own if isinstance(own, int) else 0
        if doc is None or (not self.stored_refused and stored < held):
            logger.error("registry_reset_live_edits_lost", stored=stored, held=held)
            await self._restore(self.config)
        elif self.stored_refused and not self.lib_too_old and own_version >= stored:
            logger.error("registry_invalid_restoring_config", stored=stored, own=own_version)
            await self._restore(self.config)
        return self._current

    async def _restore(self, config: Mapping[str, Any]) -> None:
        """Republish this process's config; a failure is logged and the next reload retries.

        `refresh()` also runs on the call path (admission on `no_config`), where a
        `RegistryConflict` or `StoreUnavailable` from the publish reached the caller as a
        config error instead of `CallFailed` (CO-9).
        """
        try:
            await self.publish(config, actor=process_actor("restore"))
        except (LLMQueueError, RedisError, OSError) as exc:
            logger.error("registry_restore_failed", error=repr(exc))

    async def run(self, interval_secs: float) -> None:
        """Reload every `interval_secs` until cancelled; errors never end the loop.

        After a failed reload the wait doubles, up to 30 s (or the interval when longer),
        and goes back to the interval after a good one.
        """
        failures = 0
        while True:
            try:
                await self.refresh()
                failures = 0
            except (RedisError, OSError, ConfigError) as exc:
                failures += 1
                logger.warning("registry_reload_failed", error=repr(exc), failures=failures)
                self._count_reload_error(exc)
            except Exception as exc:
                # Anything else (a corrupt meta counter, a bug in a check) ended the task for
                # good, silently: the pod never saw a live edit or kill switch again (CO-4).
                failures += 1
                logger.error(
                    "registry_reload_failed", error=repr(exc), failures=failures, exc_info=True
                )
                self._count_reload_error(exc)
            await self._sleep(_reload_wait(interval_secs, failures))

    def _count_reload_error(self, exc: BaseException) -> None:
        """Count a failed reload by error type; a failing sink never stops the loop."""
        self._count("llmq_registry_reload_errors_total", error=type(exc).__name__)

    def _count(self, name: str, **labels: str) -> None:
        """Count one event on the engine's sink, if any; a failing sink is only logged."""
        if self.metrics is None:
            return
        try:
            self.metrics.count(name, 1.0, **labels)
        except Exception as sink_exc:
            logger.debug("registry_metric_failed", error=str(sink_exc))


def _removed_models(stored: Mapping[str, Any] | None, registry: Registry) -> list[str]:
    """Aliases the stored document has and `registry` does not (their numbers are deleted).

    Read leniently: the stored document may be the invalid one being replaced.
    """
    models = stored.get("models") if isinstance(stored, Mapping) else None
    if not isinstance(models, Mapping):
        return []
    return sorted(a for a in models if isinstance(a, str) and a not in registry.models)


def _reload_wait(interval_secs: float, failures: int) -> float:
    """The reload loop's next wait: the interval, doubling per failure in a row up to a cap."""
    if not failures:
        return interval_secs
    cap = max(interval_secs, _RELOAD_BACKOFF_MAX_SECS)
    return float(min(interval_secs * 2 ** min(failures, 16), cap))


def _known(name: str, names: Any) -> str:
    """`; did you mean ...?; known: ...` for a name that is not among `names` (CO-27)."""
    ordered = sorted(names)
    close = difflib.get_close_matches(name, ordered, n=1)
    hint = f"; did you mean {close[0]!r}?" if close else ""
    return f"{hint}; known: {', '.join(ordered) or '(none)'}"


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
