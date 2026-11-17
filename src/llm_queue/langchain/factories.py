"""Model factories (0.2.0 E1): the queued LangChain class for a model, or None.

`chat_model(model_id)` / `embeddings(model_id)` return the queued adapter when the *boot*
registry knows the model, else None: a model outside the registry stays on the caller's own
path, so an app writes `chat_model(model_id, temperature=0) or ChatBedrockConverse(...)`. No
langchain module is imported until an adapter is built.

The boot registry is the document a process starts with (`LLMQ_REGISTRY` /
`LLMQ_REGISTRY_FILE`, placeholders expanded), read without a store. A model added later by a
live edit stays on the direct path until the next restart; a model a live edit removed is
still built, and its calls raise `UnknownModel`.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import orjson

from llm_queue.errors import ConfigError, UnknownModel

if TYPE_CHECKING:
    from llm_queue.registry.schema import Registry

# (model type, provider) -> (adapter class, the constructor field that takes the model id).
_ADAPTERS = {
    ("chat", "bedrock"): ("QueuedChatBedrockConverse", "model_id"),
    ("embedding", "bedrock"): ("QueuedBedrockEmbeddings", "model_id"),
    ("chat", "openai"): ("QueuedChatOpenAI", "model"),
    ("embedding", "openai"): ("QueuedOpenAIEmbeddings", "model"),
}


def _is_queue(registry: Any) -> bool:
    return registry is not None and not isinstance(registry, (Mapping, str, bytes, os.PathLike))


def boot_registry(registry: Any = None) -> Registry:
    """The registry a process boots with, parsed and checked, without a store.

    `registry`: a document (a mapping, or JSON text starting with `{`), a path (`str` or
    `Path`; `.yaml` / `.yml` read as YAML, placeholders expanded), an `LLMQueue` (its boot
    document), or None: `LLMQ_REGISTRY` / `LLMQ_REGISTRY_FILE` from the environment.
    `RegistryError` (a `ConfigError`) when it does not read or check.
    """
    from llm_queue.engine.settings import _read_document
    from llm_queue.registry.validate import load_registry

    document: Any
    if isinstance(registry, Mapping):
        document = registry
    elif isinstance(registry, (str, bytes)) and registry.lstrip()[:1] in ("{", b"{"):
        document = _read_document(
            registry.decode() if isinstance(registry, bytes) else registry, None
        )
    elif isinstance(registry, (str, os.PathLike)):
        document = _read_document(None, os.fspath(registry))
    elif registry is None:
        text = os.environ.get("LLMQ_REGISTRY") or None
        path = os.environ.get("LLMQ_REGISTRY_FILE") or None
        if text and path:
            raise ConfigError("set one of LLMQ_REGISTRY and LLMQ_REGISTRY_FILE, not both")
        document = _read_document(text, path)
    else:  # an LLMQueue: the document it booted with, else its current registry
        settings = getattr(registry, "settings", None)
        if settings is not None and (settings.registry or settings.registry_file):
            document = settings.registry_document()
        else:
            return registry.admin.registry.current.registry  # type: ignore[no-any-return]
    if isinstance(document, str):
        document = orjson.loads(document)
    return load_registry(document).registry


def aliases(registry: Any = None) -> dict[str, str]:
    """Provider model id -> registry alias in the boot registry (first alias wins on a repeat).

    `registry` as in `boot_registry`.
    """
    out: dict[str, str] = {}
    for entry in boot_registry(registry).models.values():
        out.setdefault(entry.model_id, entry.alias)
    return out


def _build(kind: str, model_id: str, registry: Any, kw: dict[str, Any]) -> Any:
    queue = registry if _is_queue(registry) else None
    boot = boot_registry(registry)
    try:
        alias = boot.alias_of(model_id)
    except UnknownModel:
        return None
    entry = boot.models[alias]
    if entry.type != kind:
        raise ConfigError(
            f"{model_id!r} is a {entry.type} model in the registry (alias {alias!r}); "
            f"use {'chat_model' if entry.type == 'chat' else 'embeddings'}()"
        )
    adapter = _ADAPTERS.get((kind, entry.provider))
    if adapter is None:
        raise ConfigError(
            f"no LangChain adapter for provider {entry.provider!r} (model {alias!r}); build "
            "the adapter yourself"
        )
    import llm_queue.langchain as lc

    name, id_field = adapter
    values: dict[str, Any] = {"model_alias": alias, id_field: entry.model_id}
    if queue is not None:
        values["llmq"] = queue
    values.update(kw)
    return getattr(lc, name)(**values)


def chat_model(model_id: str, /, *, registry: Any = None, **kw: Any) -> Any:
    """The queued chat model for `model_id` (an alias or a provider model id), or None.

    None when the boot registry does not know the model (the caller's own path then).
    Bedrock models give `QueuedChatBedrockConverse`, OpenAI-compatible ones
    `QueuedChatOpenAI`, with `model_alias` and the model id set; `**kw` goes to the
    constructor and wins (`model_id` is positional-only, so `model_id=` in `**kw` reaches the
    class). `registry` as in `boot_registry`; an `LLMQueue` is also passed as the adapter's
    `llmq`. `ConfigError` for an embedding model or a provider with no adapter.
    """
    return _build("chat", model_id, registry, kw)


def embeddings(model_id: str, /, *, registry: Any = None, **kw: Any) -> Any:
    """The queued embeddings for `model_id`, or None; as `chat_model`, for embedding models.

    Bedrock models give `QueuedBedrockEmbeddings`, OpenAI-compatible ones
    `QueuedOpenAIEmbeddings`.
    """
    return _build("embedding", model_id, registry, kw)


__all__ = ["aliases", "boot_registry", "chat_model", "embeddings"]
