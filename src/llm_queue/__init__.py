"""llm-queue: LLM calls through a queue, balanced across accounts and regions.

This is the public front door. `LLMQueue` (loaded on first use, so importing the package has
no side effects) runs calls through each model's queue under RPM / TPM budgets per deployment
(model x account x region), with fallback regions and models, health and status. The registry
(`LLMQ_REGISTRY`) describes accounts, models and settings; the extension points are plain
functions registered by name.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from llm_queue._version import __version__
from llm_queue.context import RequestContext, current_context, request_context
from llm_queue.engine.errors import KINDS, CallFailed
from llm_queue.engine.hooks import (
    CallContext,
    DeploymentView,
    Reject,
    register_admission,
    register_balancer,
    register_fallback_policy,
    register_probe,
    register_sticky_key,
)
from llm_queue.errors import (
    ConfigError,
    LLMQueueError,
    MissingExtra,
    StoreUnavailable,
    UnknownModel,
)
from llm_queue.tuning import (
    BedrockOptions,
    EngineTimings,
    RegistryOptions,
    StoreOptions,
    Tuning,
)

if TYPE_CHECKING:
    from llm_queue.clients.providers import register_provider
    from llm_queue.core import EmbedResult, LLMQueue
    from llm_queue.engine.envelope import Meta, Reply
    from llm_queue.engine.tokens import register_token_estimator
    from llm_queue.registry import RegistryError, register_account_kind

# Loaded on first access: these pull in redis, pydantic models or the SDK layer.
_LAZY = {
    "LLMQueue": "llm_queue.core",
    "EmbedResult": "llm_queue.core",
    "Reply": "llm_queue.engine.envelope",
    "Meta": "llm_queue.engine.envelope",
    "RegistryError": "llm_queue.registry",
    "register_account_kind": "llm_queue.registry",
    "register_provider": "llm_queue.clients.providers",
    "register_token_estimator": "llm_queue.engine.tokens",
}

__all__ = [
    "KINDS",
    "BedrockOptions",
    "CallContext",
    "CallFailed",
    "ConfigError",
    "DeploymentView",
    "EmbedResult",
    "EngineTimings",
    "LLMQueue",
    "LLMQueueError",
    "Meta",
    "MissingExtra",
    "RegistryError",
    "RegistryOptions",
    "Reject",
    "Reply",
    "RequestContext",
    "StoreOptions",
    "StoreUnavailable",
    "Tuning",
    "UnknownModel",
    "__version__",
    "current_context",
    "register_account_kind",
    "register_admission",
    "register_balancer",
    "register_fallback_policy",
    "register_probe",
    "register_provider",
    "register_sticky_key",
    "register_token_estimator",
    "request_context",
]


def __getattr__(name: str) -> Any:
    """Import `LLMQueue` and the heavier names on first use."""
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module), name)
    globals()[name] = value
    return value
