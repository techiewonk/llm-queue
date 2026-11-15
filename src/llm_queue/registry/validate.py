"""Cross-field checks of a registry and the one entry point that loads it.

`load_registry` parses (`schema.build`), resolves and checks the document and raises one
`RegistryError` listing every problem. The checks here need effective values, so they run on
the resolved registry (plan §3.9 and §3.19).
"""

from __future__ import annotations

import dataclasses
import json
import math
from collections.abc import Iterable, Mapping
from typing import Any

from llm_queue.errors import ConfigError
from llm_queue.registry.resolve import Deployment, ResolvedRegistry
from llm_queue.registry.schema import Registry, build
from llm_queue.registry.settings import CHAT_INFERENCE, EMBED_INFERENCE


class RegistryError(ConfigError):
    """The registry document is invalid; `problems` lists every problem with its path."""

    def __init__(self, problems: list[str], warnings: list[str] | None = None) -> None:
        """Keep the full lists; the message shows them one per line."""
        self.problems = list(problems)
        self.warnings = list(warnings or [])
        lines = "\n  ".join(self.problems)
        super().__init__(f"invalid registry ({len(self.problems)} problems):\n  {lines}")


def _budget_numbers(view: ResolvedRegistry, alias: str, dep: Deployment) -> tuple[Any, Any]:
    return view.value(alias, "rpm", dep.key), view.value(alias, "tpm", dep.key)


def _at(path: str, dep: Deployment) -> str:
    """The dotted path of a deployment's overrides, as `settings set` takes it (R3CF-17)."""
    return f"{path}.deployments.{dep.account}.overrides.{dep.name}"


def _shown(view: ResolvedRegistry, alias: str, name: str, dep: str | None = None) -> str:
    """`name` = value (level), so a message says which value and where it came from."""
    found = view.effective(alias, name, dep)
    return f"`{name}` {found.value} ({found.level})"


def _usable(view: ResolvedRegistry, alias: str, dep: Deployment) -> bool:
    """A deployment a call can reach: enabled, its account enabled, not blocked, in residency."""
    routing = view.registry.routing
    account = view.registry.accounts.get(dep.account)
    return (
        bool(view.value(alias, "enabled", dep.key))
        and account is not None
        and account.enabled
        and dep.account not in routing.blocked_accounts
        and dep.region not in routing.blocked_regions
        and not view.outside_residency(dep)
    )


# Fairness settings each mode reads; any other one set is a warning (it does nothing).
_FAIRNESS_READS = {
    "share": {"weights", "default_weight", "active_secs"},
    "quota": {"rpm", "tpm", "quotas", "active_secs"},
    "fair_queue": {"active_secs"},
}


def _check_fairness(
    view: ResolvedRegistry, alias: str, errors: list[str], warnings: list[str]
) -> None:
    """Cross-field checks of `fairness` (owner Q28, Q29, Q50): what the chosen mode needs.

    `quota` needs a cap (`rpm`, `tpm` or `quotas`). A cap above the model's whole budget never
    binds, and a `tpm` cap below one call's output reservation lets a tenant one call at a
    time (an oversized call is admitted only on an idle clock): both warnings.
    """
    path = f"models.{alias}.fairness"
    value = view.value(alias, "fairness")
    mode = value.get("mode", "off")
    if mode == "off":
        return
    entry = view.registry.models[alias]
    reads = _FAIRNESS_READS[mode]
    # The library defaults (`default_weight`, `active_secs`) are always there: only the
    # tenant settings a document sets are worth a warning.
    for name in sorted(k for k in ("weights", "rpm", "tpm", "quotas") if k in value):
        if name in reads:
            continue
        warnings.append(f"{path}.{name}: not used in `{mode}` mode")
    if mode != "quota":
        return
    caps = [value.get("rpm"), value.get("tpm")]
    quotas = value.get("quotas") or {}
    if all(c is None for c in caps) and not any(quotas.values()):
        errors.append(f"{path}: `quota` mode needs `rpm`, `tpm` or `quotas`")
        return
    confirmed = view.deployments(alias, "confirmed")
    totals: dict[str, int | None] = {}
    for name in ("rpm", "tpm"):
        numbers = [view.value(alias, name, d.key) for d in confirmed]
        totals[name] = None if any(n is None for n in numbers) else sum(numbers)
    output = (view.value(alias, "inference") or {}).get("max_tokens")
    output = output or view.value(alias, "default_output_tokens")
    reserve = math.ceil(output * view.value(alias, "tpm_output_weight"))
    every: list[tuple[str, Mapping[str, Any]]] = [
        ("", {"rpm": value.get("rpm"), "tpm": value.get("tpm")})
    ]
    every += [(f".quotas.{tenant}", caps_of) for tenant, caps_of in quotas.items()]
    for where, cap in every:
        for name in ("rpm", "tpm"):
            limit, total = cap.get(name), totals[name]
            if limit is not None and total is not None and limit > total:
                warnings.append(
                    f"{path}{where}.{name}: {limit} is above the model's {name} {total}; "
                    "it never binds"
                )
        tpm = cap.get("tpm")
        if entry.type == "chat" and tpm is not None and tpm < reserve:
            warnings.append(
                f"{path}{where}.tpm: {tpm} is below one call's output reservation ({reserve}); "
                "the tenant runs one call at a time"
            )


def _check_model(
    view: ResolvedRegistry, alias: str, errors: list[str], warnings: list[str]
) -> None:
    entry = view.registry.models[alias]
    path = f"models.{alias}"
    deployments = view.deployments(alias)
    confirmed = [d for d in deployments if d.group == "confirmed"]

    # Accounts: their kind must serve the model's provider; enabled flags only.
    for dep in deployments:
        account = view.registry.accounts.get(dep.account)
        if account is not None and account.provider != entry.provider:
            errors.append(
                f"{path}.deployments.{dep.account}: a `{account.kind}` account serves "
                f"`{account.provider}`, not `{entry.provider}`"
            )

    # Timeouts.
    timeout = view.value(alias, "timeout_secs")
    if timeout > view.value(alias, "timeout_max_secs"):
        errors.append(
            f"{path}: {_shown(view, alias, 'timeout_secs')} is above "
            f"{_shown(view, alias, 'timeout_max_secs')}"
        )
    for dep in deployments:
        if view.value(alias, "attempt_timeout_secs", dep.key) >= view.value(
            alias, "timeout_secs", dep.key
        ):
            errors.append(
                f"{_at(path, dep)}: {_shown(view, alias, 'attempt_timeout_secs', dep.key)} must "
                f"be below {_shown(view, alias, 'timeout_secs', dep.key)}"
            )
        # A probe still running when its slot is freed lets a second probe through.
        if view.value(alias, "breaker_probe_secs", dep.key) < view.value(
            alias, "attempt_timeout_secs", dep.key
        ):
            errors.append(
                f"{_at(path, dep)}: {_shown(view, alias, 'breaker_probe_secs', dep.key)} must be "
                f"at least {_shown(view, alias, 'attempt_timeout_secs', dep.key)}"
            )
        # The retry wait starts at the base and grows to the cap: a base above the cap is
        # always the cap (R3CF-10).
        if view.value(alias, "retry_base_secs", dep.key) > view.value(
            alias, "retry_max_secs", dep.key
        ):
            warnings.append(
                f"{_at(path, dep)}: {_shown(view, alias, 'retry_base_secs', dep.key)} is above "
                f"{_shown(view, alias, 'retry_max_secs', dep.key)}; every wait is the cap"
            )
        health = view.value(alias, "health", dep.key)
        if health["probe_after_secs"] > health["probe_max_secs"]:
            warnings.append(
                f"{_at(path, dep)}.health: `probe_after_secs` {health['probe_after_secs']} is "
                f"above `probe_max_secs` {health['probe_max_secs']}"
            )
    max_wait = view.value(alias, "max_wait_secs")
    for rule in ("fallback", "model_fallback"):
        value = view.value(alias, rule)
        if not value:
            continue
        if max_wait is not None and value["after_secs"] >= max_wait:
            errors.append(
                f"{path}.{rule}.after_secs: {value['after_secs']} must be below "
                f"{_shown(view, alias, 'max_wait_secs')}"
            )
        elif max_wait is None and value["after_secs"] >= timeout:
            # With no max_wait the rule can only fire inside the call's deadline (R3CF-10).
            errors.append(
                f"{path}.{rule}.after_secs: {value['after_secs']} must be below "
                f"{_shown(view, alias, 'timeout_secs')}; it could never fire"
            )
        causes = value.get("on", [])
        if len(set(causes)) != len(causes):
            warnings.append(f"{path}.{rule}.on: lists a cause twice")
    queue = view.value(alias, "queue")
    if queue["retention_secs"] < timeout:
        # A message the queue trims before the call's deadline is lost while it waits (R3CF-10).
        warnings.append(
            f"{path}.queue.retention_secs: {queue['retention_secs']} is below "
            f"{_shown(view, alias, 'timeout_secs')}; a waiting call can be trimmed"
        )

    # Shares.
    reserve = view.value(alias, "priority_reserve")
    share = view.value(alias, "normal_min_share")
    if reserve + share >= 1:
        errors.append(
            f"{path}: {_shown(view, alias, 'priority_reserve')} + "
            f"{_shown(view, alias, 'normal_min_share')} must be below 1"
        )

    # Budgets named by rate_limits must have a number.
    for dep in deployments:
        limits = view.effective(alias, "rate_limits", dep.key)
        rpm, tpm = _budget_numbers(view, alias, dep)
        if limits.level != "library":
            if limits.value == "rpm" and rpm is None:
                errors.append(f"{_at(path, dep)}: `rate_limits` is rpm but no `rpm` is set")
            if limits.value == "tpm" and tpm is None:
                errors.append(f"{_at(path, dep)}: `rate_limits` is tpm but no `tpm` is set")
        # One call reserves its output tokens x the weight up front: a TPM below that admits
        # nothing, and every call waited until its deadline.
        if entry.type == "chat" and tpm is not None and limits.value in ("both", "tpm"):
            output = (view.value(alias, "inference", dep.key) or {}).get("max_tokens")
            output = output or view.value(alias, "default_output_tokens", dep.key)
            reserve = math.ceil(output * view.value(alias, "tpm_output_weight", dep.key))
            if tpm < reserve:
                errors.append(
                    f"{_at(path, dep)}: `tpm` {tpm} is below one call's output reservation "
                    f"({output} max tokens x tpm_output_weight = {reserve})"
                )

    # Strategies that need quota numbers.
    for name in ("balance", "balance_priority"):
        if view.value(alias, name) == "headroom":
            bare = [d.key for d in confirmed if _budget_numbers(view, alias, d) == (None, None)]
            if bare:
                errors.append(f"{path}.{name}: headroom needs `rpm` or `tpm` on {', '.join(bare)}")
    # Weight 0 does not keep traffic away when every usable deployment has it: Lua's total is
    # 0 and the first candidate always wins. Both settings, usable deployments only (R3CF-12).
    for name in ("balance", "balance_priority"):
        if view.value(alias, name) != "weighted":
            continue
        weights = [view.value(alias, "weight", d.key) for d in confirmed if _usable(view, alias, d)]
        if weights and not any(weights):
            errors.append(
                f"{path}.{name}: every confirmed deployment has weight 0 (counting usable ones)"
            )
    # Nothing to call (R3CF-11): a warning, since a planned disable is valid.
    if confirmed and not any(_usable(view, alias, d) for d in confirmed):
        warnings.append(
            f"{path}: no usable confirmed deployment (disabled, account disabled, blocked or "
            "outside residency); calls wait or fall back"
        )

    # Inference parameters by model type and provider; defaults within their limits.
    wrong = EMBED_INFERENCE if entry.type == "chat" else CHAT_INFERENCE
    for dep_key in (None, *(d.key for d in deployments)):
        inference = view.value(alias, "inference", dep_key) or {}
        where = path if dep_key is None else f"{path} @ {dep_key}"
        for name in inference:
            if name in wrong:
                errors.append(f"{where}.inference.{name}: not a parameter of a {entry.type} model")
        if "seed" in inference and entry.provider == "bedrock":
            errors.append(f"{where}.inference.seed: not supported by Bedrock")
        caps = view.value(alias, "inference_limits", dep_key) or {}
        if "max_tokens" in caps and inference.get("max_tokens", 0) > caps["max_tokens"]:
            errors.append(
                f"{where}.inference.max_tokens: {inference['max_tokens']} is above "
                f"`inference_limits.max_tokens` {caps['max_tokens']}"
            )
        misplaced = [n for n in caps.get("locked", ()) if n in wrong]
        if misplaced:  # R3CF-16
            warnings.append(
                f"{where}.inference_limits.locked: {', '.join(misplaced)} is not a parameter of "
                f"a {entry.type} model"
            )

    # Rules that depend on the model type.
    for name in ("embed_batch_size", "embed_max_chars"):
        if entry.type == "chat" and name in entry.settings:
            errors.append(f"{path}.{name}: only for embedding models")
    # Settings the model's type or provider never reads: valid, but they mislead (R3CF-16).
    unread = []
    if entry.type == "embedding":
        unread += ["stream_idle_secs", "converse_api", "supports_tools"]
    if entry.provider != "bedrock":
        unread.append("converse_api")
    for name in sorted({n for n in unread if n in entry.settings}):
        warnings.append(f"{path}.{name}: not used by a {entry.provider} {entry.type} model")
    rule = view.value(alias, "model_fallback")
    if rule and entry.type == "embedding":
        errors.append(f"{path}.model_fallback: an embedding model cannot change model")
    elif rule:
        named = [item["model"] for item in rule["models"]]
        if len(set(named)) != len(named):
            warnings.append(f"{path}.model_fallback.models: lists a model twice")  # R3CF-16
        for item in rule["models"]:
            other = item["model"]
            if other == alias:
                errors.append(f"{path}.model_fallback: a model cannot list itself")
                continue
            if other not in view.registry.models:
                errors.append(f"{path}.model_fallback: unknown model `{other}`")
                continue
            target = view.registry.models[other]
            if target.type != "chat":
                errors.append(f"{path}.model_fallback: `{other}` is not a chat model")
            if target.provider != entry.provider:
                # Allowed (owner decision); the request is not converted between shapes yet,
                # so say so where it is configured (R3CF-6a).
                warnings.append(
                    f"{path}.model_fallback: `{other}` is an {target.provider} model; this "
                    f"{entry.provider} model's requests reach it unchanged"
                )
            allowed = view.residency(alias)
            if allowed is not None:
                # A fallback must not carry this model's calls where its residency forbids.
                outside = sorted(
                    d.key
                    for d in view.deployments(other)
                    if not view.outside_residency(d)
                    and (
                        (d.profile_id is not None and not d.routes_to)
                        or not set(d.routes_to if d.profile_id else (d.region,)) <= set(allowed)
                    )
                )
                if outside:
                    errors.append(
                        f"{path}.model_fallback: `{other}` reaches {', '.join(outside)}, "
                        "outside this model's residency"
                    )
            usable = [
                d
                for d in view.deployments(other)
                if view.value(other, "enabled", d.key) and view.registry.accounts[d.account].enabled
            ]
            if not view.value(other, "enabled") or not usable:
                warnings.append(f"{path}.model_fallback: `{other}` is disabled; it takes no calls")
            if item["require_headroom"]:
                bare = [
                    d.key
                    for d in view.deployments(other, "confirmed")
                    if _budget_numbers(view, other, d) == (None, None)
                ]
                if bare:
                    errors.append(
                        f"{path}.model_fallback: require_headroom on `{other}` needs `rpm` or "
                        f"`tpm` on {', '.join(bare)}"
                    )

    # Warnings (not fatal).
    fallback_deps = [d for d in deployments if d.group == "fallback"]
    if fallback_deps and not view.value(alias, "fallback"):
        warnings.append(f"{path}: fallback regions are listed but there is no `fallback` rule")
    if view.value(alias, "fallback") and not fallback_deps:
        warnings.append(f"{path}.fallback: there are no fallback regions to move to")  # R3CF-16
    for dep in deployments:
        if view.outside_residency(dep):
            reason = "routes_to is missing" if dep.profile_id and not dep.routes_to else "region"
            warnings.append(f"{_at(path, dep)}: outside the residency list ({reason}); unused")
    if confirmed and all(view.outside_residency(d) for d in confirmed):
        errors.append(f"{path}: the residency list leaves no confirmed deployment to call")
    for dep in deployments:
        rpm = view.value(alias, "rpm", dep.key)
        shared = view.registry.accounts[dep.account].settings.get("shared_rpm")
        if shared is not None and rpm is not None and shared < rpm:
            warnings.append(
                f"accounts.{dep.account}.settings.shared_rpm: {shared} is below the `rpm` {rpm} "
                f"of {_at(path, dep)}"
            )


def _fallback_cycles(view: ResolvedRegistry) -> list[str]:
    """Model fallback cycles (A -> B -> ... -> A) with a step that does not need headroom.

    Such a cycle passes a call around while every model is short of quota. Each cycle is found
    once, from its smallest alias.
    """
    edges: dict[str, list[tuple[str, bool]]] = {}
    for alias in view.registry.models:
        rule = view.value(alias, "model_fallback") or {"models": []}
        edges[alias] = [
            (m["model"], bool(m["require_headroom"]))
            for m in rule["models"]
            if m["model"] in view.registry.models and m["model"] != alias
        ]
    errors: list[str] = []

    def walk(start: str, node: str, path: list[str], guarded: bool) -> None:
        for target, headroom in edges[node]:
            if target == start and not (guarded and headroom):
                if len(path) == 2:
                    errors.append(
                        f"models.{start}.model_fallback: `{start}` and `{path[1]}` list each "
                        "other; both must use require_headroom"
                    )
                else:
                    errors.append(
                        f"models.{start}.model_fallback: fallback cycle "
                        f"{' -> '.join([*path, start])}; every step must use require_headroom"
                    )
            elif target > start and target not in path:
                walk(start, target, [*path, target], guarded and headroom)

    for alias in sorted(edges):
        walk(alias, alias, [alias], True)
    return errors


def tpm_refuses(
    view: ResolvedRegistry, alias: str, deployments: Iterable[str], tokens: int
) -> bool:
    """True when every listed deployment enforces a TPM below `tokens` (never admitted).

    The runtime compares a call's whole estimate (input and output reservation) with it before
    asking admission, and fails the call as too large instead of letting it wait.
    """
    listed = list(deployments)
    for key in listed:
        tpm = view.value(alias, "tpm", key)
        if view.value(alias, "rate_limits", key) not in ("both", "tpm") or tpm is None:
            return False
        if tpm >= tokens:
            return False
    return bool(listed)


def _check_registry(view: ResolvedRegistry) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    registry = view.registry
    ids: dict[str, str] = {}
    queues: dict[str, str] = {}
    for alias, entry in registry.models.items():
        if entry.model_id in ids:
            errors.append(
                f"models.{alias}.model_id: `{entry.model_id}` already belongs to "
                f"`{ids[entry.model_id]}` (one alias per model id)"
            )
        ids.setdefault(entry.model_id, alias)
        name = view.queue_name(alias)
        for queue in (name, f"{name}-priority"):
            if queue in queues:
                errors.append(f"models.{alias}: queue `{queue}` is also used by {queues[queue]}")
            queues[queue] = alias
    for alias in registry.models:
        if alias in ids and ids[alias] != alias:
            errors.append(f"models.{alias}: the alias equals the model id of `{ids[alias]}`")
        _check_model(view, alias, errors, warnings)
        _check_fairness(view, alias, errors, warnings)
    errors += _fallback_cycles(view)
    for account in registry.routing.blocked_accounts:
        if account not in registry.accounts:
            warnings.append(f"routing.blocked_accounts: `{account}` is not an account")
    regions = {d.region for alias in registry.models for d in view.deployments(alias)}
    for region in registry.routing.blocked_regions:
        if region not in regions:
            warnings.append(f"routing.blocked_regions: no deployment uses `{region}`")
    # One shared account budget, one window: a fixed and a smooth window (or two bursts) on the
    # same counter each enforced their own view of it, and 4 shared RPM let 8 calls through.
    for name, account_entry in registry.accounts.items():
        settings = account_entry.settings
        if settings.get("shared_rpm") is None and settings.get("shared_tpm") is None:
            continue
        windows = {
            (view.value(alias, "rate_window", d.key), view.value(alias, "rate_burst", d.key))
            for alias in registry.models
            for d in view.deployments(alias)
            if d.account == name
        }
        if len(windows) > 1:
            errors.append(
                f"accounts.{name}: deployments sharing `shared_rpm` / `shared_tpm` must use one "
                "`rate_window` and `rate_burst` (set them on the account)"
            )
    return errors, warnings


def parse_document(text: str | bytes) -> Any:
    """Parse registry JSON; `RegistryError` when it is not JSON or repeats a key in one object.

    A repeated key is legal JSON and parsers keep the last one, so `"rpm": 10, ..., "rpm": 1000`
    would silently drop the first value.
    """
    repeated: list[str] = []

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in items:
            if key in out:
                # The other keys of the object say which one it is (R3CF-17).
                siblings = sorted({k for k, _ in items if k != key})[:4]
                beside = f" (in the object with {', '.join(siblings)})" if siblings else ""
                repeated.append(f"`{key}`{beside}")
            out[key] = value
        return out

    def constant(name: str) -> Any:
        # `json` reads NaN / Infinity, which pass every range check and are written back as
        # `null` (orjson), so the stored document then failed on every reader (R3CF-1).
        raise ValueError(f"{name} is not a JSON number")

    try:
        doc = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
    except ValueError as exc:  # JSONDecodeError, NaN / Infinity and bad UTF-8
        raise RegistryError([f"registry is not valid JSON: {exc}"]) from exc
    if repeated:
        names = "; ".join(sorted(set(repeated)))
        raise RegistryError([f"registry repeats keys inside one object: {names}"])
    return doc


def check(
    raw: Mapping[str, Any] | str | bytes,
    *,
    strict: bool = True,
    allowed_hosts: tuple[str, ...] = (),
) -> tuple[Registry | None, list[str], list[str]]:
    """Parse, resolve and check a document; return (registry or None, errors, warnings)."""
    if isinstance(raw, str | bytes):
        try:
            raw = parse_document(raw)
        except RegistryError as exc:
            return None, exc.problems, []
    if not isinstance(raw, Mapping):
        return None, ["registry must be a JSON object"], []
    registry, errors, warnings = build(raw, strict=strict, allowed_hosts=allowed_hosts)
    if registry is None:
        return None, errors, warnings
    more_errors, more_warnings = _check_registry(ResolvedRegistry(registry))
    warnings = warnings + more_warnings
    if more_errors:
        return None, more_errors, warnings
    return _with_warnings(registry, warnings), [], warnings


def _with_warnings(registry: Registry, warnings: list[str]) -> Registry:
    return dataclasses.replace(registry, warnings=tuple(warnings))


def load_registry(
    raw: Mapping[str, Any] | str | bytes,
    *,
    strict: bool = True,
    allowed_hosts: tuple[str, ...] = (),
) -> ResolvedRegistry:
    """Load and check a registry document; raise `RegistryError` with every problem.

    `strict=False` is for a document read back from the store: unknown names become warnings.
    """
    registry, errors, warnings = check(raw, strict=strict, allowed_hosts=allowed_hosts)
    if registry is None:
        raise RegistryError(errors, warnings)
    return ResolvedRegistry(registry)
