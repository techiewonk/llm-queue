"""Named hooks chosen by name in the registry: balancer, fallback policy, admission, sticky key.

Plan §3.10. Hooks are plain functions registered in code; the registry names them (`balance:
"custom:cheapest"`, `fallback_policy: "office_hours"`). A hook that raises is logged once per
name and the built-in behaviour is used, so a bug in a hook never fails a call. The admission
hook is the exception: it is a gate (tenants, entitlements), so a failing or unregistered one
refuses the call (`rejected`) instead of admitting it (FO-4). Every failure is counted as
`llmq_hook_failed_total{kind,hook}` when the caller passes its metrics sink.

Hooks run synchronously on the event loop of the call: they must not block (no network or disk
I/O, no `time.sleep`), or every call of the process waits on them (FO-22).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from llm_queue._logging import get_logger
from llm_queue.errors import ConfigError

if TYPE_CHECKING:
    from llm_queue.engine.metrics import Metrics

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class DeploymentView:
    """What a custom balancer sees of one candidate deployment."""

    key: str
    account: str
    region: str
    group: str
    rpm: int | None
    tpm: int | None
    weight: float
    tags: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CallContext:
    """What hooks see of a call (no prompt text)."""

    model: str
    request_id: str
    priority: bool
    session_id: str | None
    user_id: str | None
    tenant_id: str | None
    waited_secs: float = 0.0


@dataclass(frozen=True, slots=True)
class Reject:
    """Returned by an admission hook to refuse a call before it is sent."""

    reason: str


Balancer = Callable[[Sequence[DeploymentView], CallContext], Sequence[str]]
FallbackPolicy = Callable[[str, float, Mapping[str, Any], CallContext], bool]
AdmissionHook = Callable[[CallContext], "Reject | None"]
StickyKey = Callable[[CallContext], "str | None"]
# fn(alias, op) -> the params a health probe sends, or None for the built-in probe (a 1-token
# chat or one short embedding) (FO-22, R3AP-12).
Probe = Callable[[str, str], "Mapping[str, Any] | None"]

_HOOKS: dict[str, dict[str, Callable[..., Any]]] = {
    "balancer": {},
    "fallback_policy": {},
    "admission": {},
    "sticky_key": {},
    "probe": {},
}
_WARNED: set[tuple[str, str, str]] = set()  # (event, kind, name): each event warns once


def _register(kind: str, name: str, fn: Callable[..., Any]) -> None:
    if not name or not callable(fn):
        raise ConfigError(f"{kind} hook needs a name and a callable")
    _HOOKS[kind][name] = fn


def register_balancer(name: str, fn: Balancer) -> None:
    """`balance: "custom:<name>"`: fn(candidates, ctx) -> deployment keys, most preferred first."""
    _register("balancer", name, fn)


def register_fallback_policy(name: str, fn: FallbackPolicy) -> None:
    """`fallback_policy: <name>`: fn(cause, waited_secs, rule, ctx) -> may this call spill now."""
    _register("fallback_policy", name, fn)


def register_admission(name: str, fn: AdmissionHook) -> None:
    """`admission: <name>`: fn(ctx) -> Reject(reason) to refuse the call, None to admit it.

    A hook that raises, or a name the registry uses that is not registered in this process,
    refuses the call (`CallFailed("rejected")`) and counts `llmq_hook_failed_total` (FO-4).
    """
    _register("admission", name, fn)


def register_sticky_key(name: str, fn: StickyKey) -> None:
    """`sticky.key: "custom:<name>"`: fn(ctx) -> the key a call sticks by (None: no sticking)."""
    _register("sticky_key", name, fn)


def register_probe(name: str, fn: Probe) -> None:
    """`probe: <name>`: fn(alias, op) -> the params a health probe of that model sends.

    `op` is the provider operation (`converse`, `chat`, `embeddings`, `invoke_model`). Return
    None (or raise) to send the built-in probe instead. Called on the event loop: no blocking.
    """
    _register("probe", name, fn)


def get(kind: str, name: str | None) -> Callable[..., Any] | None:
    """A registered hook, or None (unknown names fall back to the built-in behaviour).

    An unknown admission name returns a stand-in that fails, so the call is refused (FO-4).
    """
    if not name:
        return None
    fn = _HOOKS[kind].get(name)
    if fn is None:
        warn_missing(kind, name)
        if kind == "admission":
            return _unregistered(name)
    return fn


def _unregistered(name: str) -> Callable[..., Any]:
    """A stand-in admission hook that raises, so `call_safely` refuses the call (FO-4)."""

    def missing(*_: Any) -> Any:
        raise LookupError(f"admission hook `{name}` is not registered in this process")

    return missing


def warn_missing(kind: str, name: str) -> None:
    """Log once per (kind, name) that a name the registry uses is not registered."""
    if ("missing", kind, name) not in _WARNED:
        _WARNED.add(("missing", kind, name))
        logger.warning("hook_not_registered", kind=kind, name=name)


def call_safely(
    kind: str,
    name: str,
    fn: Callable[..., Any],
    *args: Any,
    default: Any,
    metrics: Metrics | None = None,
) -> Any:
    """Run a hook; on an exception count it, log once per (kind, name) and return `default`.

    A failing admission hook returns `Reject` instead of `default`: admitting on a broken gate
    silently dropped a tenant or entitlement check after the first failure (FO-4).
    """
    try:
        return fn(*args)
    except Exception as exc:  # a hook bug must never fail a call (admission: refuse it)
        if metrics is not None:
            metrics.count("llmq_hook_failed_total", kind=kind, hook=name)
        if ("failed", kind, name) not in _WARNED:
            _WARNED.add(("failed", kind, name))
            logger.warning("hook_failed", kind=kind, name=name, error=str(exc))
        if kind == "admission":
            return Reject(f"admission hook `{name}` failed")
        return default
