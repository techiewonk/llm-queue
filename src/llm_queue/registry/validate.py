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
        errors.append(f"{path}: `timeout_secs` is above `timeout_max_secs`")
    for dep in deployments:
        if view.value(alias, "attempt_timeout_secs", dep.key) >= view.value(
            alias, "timeout_secs", dep.key
        ):
            errors.append(
                f"{path} @ {dep.key}: `attempt_timeout_secs` must be below `timeout_secs`"
            )
        # A probe still running when its slot is freed lets a second probe through.
        if view.value(alias, "breaker_probe_secs", dep.key) < view.value(
            alias, "attempt_timeout_secs", dep.key
        ):
            errors.append(
                f"{path} @ {dep.key}: `breaker_probe_secs` must be at least `attempt_timeout_secs`"
            )
    max_wait = view.value(alias, "max_wait_secs")
    for rule in ("fallback", "model_fallback"):
        value = view.value(alias, rule)
        if value and max_wait is not None and value["after_secs"] >= max_wait:
            errors.append(f"{path}.{rule}.after_secs: must be below `max_wait_secs`")

    # Shares.
    if view.value(alias, "priority_reserve") + view.value(alias, "normal_min_share") >= 1:
        errors.append(f"{path}: `priority_reserve` + `normal_min_share` must be below 1")

    # Budgets named by rate_limits must have a number.
    for dep in deployments:
        limits = view.effective(alias, "rate_limits", dep.key)
        rpm, tpm = _budget_numbers(view, alias, dep)
        if limits.level != "library":
            if limits.value == "rpm" and rpm is None:
                errors.append(f"{path} @ {dep.key}: `rate_limits` is rpm but no `rpm` is set")
            if limits.value == "tpm" and tpm is None:
                errors.append(f"{path} @ {dep.key}: `rate_limits` is tpm but no `tpm` is set")
        # One call reserves its output tokens x the weight up front: a TPM below that admits
        # nothing, and every call waited until its deadline.
        if entry.type == "chat" and tpm is not None and limits.value in ("both", "tpm"):
            output = (view.value(alias, "inference", dep.key) or {}).get("max_tokens")
            output = output or view.value(alias, "default_output_tokens", dep.key)
            reserve = math.ceil(output * view.value(alias, "tpm_output_weight", dep.key))
            if tpm < reserve:
                errors.append(
                    f"{path} @ {dep.key}: `tpm` {tpm} is below one call's output reservation "
                    f"({output} max tokens x tpm_output_weight = {reserve})"
                )

    # Strategies that need quota numbers.
    for name in ("balance", "balance_priority"):
        if view.value(alias, name) == "headroom":
            bare = [d.key for d in confirmed if _budget_numbers(view, alias, d) == (None, None)]
            if bare:
                errors.append(f"{path}.{name}: headroom needs `rpm` or `tpm` on {', '.join(bare)}")
    if view.value(alias, "balance") == "weighted":
        weights = [view.value(alias, "weight", d.key) for d in confirmed]
        if weights and not any(weights):
            errors.append(f"{path}: every confirmed deployment has weight 0")

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
            errors.append(f"{where}.inference.max_tokens: above `inference_limits.max_tokens`")

    # Rules that depend on the model type.
    for name in ("embed_batch_size", "embed_max_chars"):
        if entry.type == "chat" and name in entry.settings:
            errors.append(f"{path}.{name}: only for embedding models")
    rule = view.value(alias, "model_fallback")
    if rule and entry.type == "embedding":
        errors.append(f"{path}.model_fallback: an embedding model cannot change model")
    elif rule:
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
    for dep in deployments:
        if view.outside_residency(dep):
            reason = "routes_to is missing" if dep.profile_id and not dep.routes_to else "region"
            warnings.append(f"{path} @ {dep.key}: outside the residency list ({reason}); unused")
    if confirmed and all(view.outside_residency(d) for d in confirmed):
        errors.append(f"{path}: the residency list leaves no confirmed deployment to call")
    for dep in deployments:
        rpm = view.value(alias, "rpm", dep.key)
        shared = view.registry.accounts[dep.account].settings.get("shared_rpm")
        if shared is not None and rpm is not None and shared < rpm:
            warnings.append(f"accounts.{dep.account}: `shared_rpm` is below {alias} @ {dep.key}")


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
                repeated.append(key)
            out[key] = value
        return out

    try:
        doc = json.loads(text, object_pairs_hook=pairs)
    except ValueError as exc:  # JSONDecodeError and bad UTF-8
        raise RegistryError([f"registry is not valid JSON: {exc}"]) from exc
    if repeated:
        names = ", ".join(f"`{k}`" for k in sorted(set(repeated)))
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
