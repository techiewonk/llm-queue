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
from llm_queue.engine.errors import KINDS, CallFailed, Kind
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
    OpenAIOptions,
    RegistryOptions,
    StoreOptions,
    Tuning,
    WarmOptions,
)

if TYPE_CHECKING:
    from llm_queue.clients.hub import run
    from llm_queue.clients.pool import Route
    from llm_queue.clients.providers import Provider, register_provider
    from llm_queue.core import (
        ConcurrencyStatus,
        DeploymentStatus,
        EmbedResult,
        Health,
        LLMQueue,
        ModelStatus,
        RateStatus,
        RequestStatus,
        StatusReport,
    )
    from llm_queue.engine.envelope import Meta, Reply
    from llm_queue.engine.jobs import JobContext, JobFn, JobSettings, current_job
    from llm_queue.engine.metrics import EmfSink, Metrics, NullSink, PrometheusSink
    from llm_queue.engine.offload import ResultOffload, S3Offload
    from llm_queue.engine.store_client import Secrets
    from llm_queue.engine.tokens import register_token_estimator
    from llm_queue.providers.embeddings import EmbeddingCodec, register_embedding_family
    from llm_queue.registry import RegistryError, register_account_kind
    from llm_queue.registry.store import (
        LiveEditRefused,
        PublishResult,
        RegistryConflict,
        RegistryStore,
    )

# Loaded on first access: these pull in redis, pydantic models or the SDK layer.
_LAZY = {
    "LLMQueue": "llm_queue.core",
    "EmbedResult": "llm_queue.core",
    "Health": "llm_queue.core",
    "ModelStatus": "llm_queue.core",
    "RequestStatus": "llm_queue.core",
    "StatusReport": "llm_queue.core",
    # The rows of a status report, so an app can annotate them (R3AP-3).
    "DeploymentStatus": "llm_queue.core",
    "RateStatus": "llm_queue.core",
    "ConcurrencyStatus": "llm_queue.core",
    "Reply": "llm_queue.engine.envelope",
    "Meta": "llm_queue.engine.envelope",
    "Metrics": "llm_queue.engine.metrics",
    "EmfSink": "llm_queue.engine.metrics",
    "NullSink": "llm_queue.engine.metrics",
    "PrometheusSink": "llm_queue.engine.metrics",
    "ResultOffload": "llm_queue.engine.offload",
    "S3Offload": "llm_queue.engine.offload",
    "RegistryError": "llm_queue.registry",
    "RegistryStore": "llm_queue.registry.store",
    "RegistryConflict": "llm_queue.registry.store",
    "LiveEditRefused": "llm_queue.registry.store",
    "register_account_kind": "llm_queue.registry",
    "register_provider": "llm_queue.clients.providers",
    "run": "llm_queue.clients.hub",
    "register_token_estimator": "llm_queue.engine.tokens",
    # Embedding families an app adds without a release (Cohere v4, Nova, ...).
    "EmbeddingCodec": "llm_queue.providers.embeddings",
    "register_embedding_family": "llm_queue.providers.embeddings",
    # Types an app writes against (owner decision Q35): a custom provider, its route, a job
    # function, a secrets lookup, and what a publish or rollback did.
    "Provider": "llm_queue.clients.providers",
    "Route": "llm_queue.clients.pool",
    "JobFn": "llm_queue.engine.jobs",
    "JobContext": "llm_queue.engine.jobs",
    "JobSettings": "llm_queue.engine.jobs",
    "current_job": "llm_queue.engine.jobs",
    "Secrets": "llm_queue.engine.store_client",
    "PublishResult": "llm_queue.registry.store",
}

__all__ = [
    "KINDS",
    "BedrockOptions",
    "CallContext",
    "CallFailed",
    "ConcurrencyStatus",
    "ConfigError",
    "DeploymentStatus",
    "DeploymentView",
    "EmbedResult",
    "EmbeddingCodec",
    "EmfSink",
    "EngineTimings",
    "Health",
    "JobContext",
    "JobFn",
    "JobSettings",
    "Kind",
    "LLMQueue",
    "LLMQueueError",
    "LiveEditRefused",
    "Meta",
    "Metrics",
    "MissingExtra",
    "ModelStatus",
    "NullSink",
    "OpenAIOptions",
    "PrometheusSink",
    "Provider",
    "PublishResult",
    "RateStatus",
    "RegistryConflict",
    "RegistryError",
    "RegistryOptions",
    "RegistryStore",
    "Reject",
    "Reply",
    "RequestContext",
    "RequestStatus",
    "ResultOffload",
    "Route",
    "S3Offload",
    "Secrets",
    "StatusReport",
    "StoreOptions",
    "StoreUnavailable",
    "Tuning",
    "UnknownModel",
    "WarmOptions",
    "__version__",
    "current_context",
    "current_job",
    "register_account_kind",
    "register_admission",
    "register_balancer",
    "register_embedding_family",
    "register_fallback_policy",
    "register_probe",
    "register_provider",
    "register_sticky_key",
    "register_token_estimator",
    "request_context",
    "run",
]


def __getattr__(name: str) -> Any:
    """Import `LLMQueue` and the heavier names on first use."""
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module), name)
    globals()[name] = value
    return value
