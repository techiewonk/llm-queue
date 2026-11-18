"""Model factories (0.2.0 E1): the queued LangChain class for a model, or None.

`chat_model(model_id)` / `embeddings(model_id)` return the queued adapter when the *boot*
registry knows the model, else None: a model outside the registry, or every model with the
library switched off, stays on the caller's own path, so an app writes
`chat_model(model_id, temperature=0) or ChatBedrockConverse(...)`. No langchain module is
imported until an adapter is built.

Without `registry=`, the factories follow the started queue (`LLMQueue.start()` registers it):
its off switch (`LLMQ_ENABLED`, `LLMQ_CONFIG_ERROR_MODE=disable`) and the boot registry its
settings read (secrets, the environment, `defaults=`). Before any queue has started they read
`LLMQ_ENABLED` and `LLMQ_REGISTRY` / `LLMQ_REGISTRY_FILE` from the environment.

The boot registry is the document a process starts with (placeholders expanded), read once
and without a store. A model added later by a live edit stays on the direct path until the
next restart; a model a live edit removed is still built, and its calls raise `UnknownModel`.
"""

from __future__ import annotations

import contextlib
import os
import weakref
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

# Each queue's boot registry, parsed once: a queue's settings never change (FB-1).
_QUEUE_BOOT: weakref.WeakKeyDictionary[Any, Registry] = weakref.WeakKeyDictionary()
# Registries read from text, a path or the environment, keyed on (text, path, the file's
# mtime and size) and the placeholder values read. Small and bounded.
_READ_BOOT: dict[tuple[Any, ...], Registry] = {}
_READ_BOOT_MAX = 16


def _is_queue(registry: Any) -> bool:
    return registry is not None and not isinstance(registry, (Mapping, str, bytes, os.PathLike))


def _source(registry: Any) -> Any:
    """`registry`, else the started queue (`LLMQueue.start()` registers it), else None."""
    if registry is None:
        from llm_queue.langchain import _client

        return _client._default_queue
    return registry


def _file_key(path: str | None) -> tuple[int, int] | None:
    if not path:
        return None
    try:
        info = os.stat(path)
    except OSError:
        return None  # `read_document` raises the ConfigError
    return info.st_mtime_ns, info.st_size


def _read_boot(text: str | None, path: str | None) -> Registry:
    """The registry from JSON `text` or the file `path`, placeholders from `os.environ`.

    Cached on the text, the path, the file's mtime and size and the values of the
    placeholders it read: a changed file or variable is read again, nothing else is.
    """
    from llm_queue.engine.settings import read_document
    from llm_queue.registry.validate import load_registry

    source = (text, path, _file_key(path))
    for (cached, names), registry in _READ_BOOT.items():
        if cached == source and all(os.environ.get(n) == v for n, v in names):
            return registry
    used: set[str] = set()
    registry = load_registry(orjson.loads(read_document(text, path, None, used))).registry
    if len(_READ_BOOT) >= _READ_BOOT_MAX:
        _READ_BOOT.pop(next(iter(_READ_BOOT)))
    _READ_BOOT[(source, tuple((n, os.environ.get(n)) for n in sorted(used)))] = registry
    return registry


def _queue_boot(queue: Any) -> Registry:
    """A queue's boot registry: the document its settings read, parsed once per queue."""
    from llm_queue.registry.validate import load_registry

    settings = getattr(queue, "settings", None)
    if settings is None or not (settings.registry or settings.registry_file):
        return queue.admin.registry.current.registry  # type: ignore[no-any-return]
    with contextlib.suppress(KeyError, TypeError):  # TypeError: no weak reference
        return _QUEUE_BOOT[queue]
    registry = load_registry(orjson.loads(settings.registry_document())).registry
    with contextlib.suppress(TypeError):
        _QUEUE_BOOT[queue] = registry
    return registry


def boot_registry(registry: Any = None) -> Registry:
    """The registry a process boots with, parsed and checked, without a store.

    `registry`: a document (a mapping, or JSON text starting with `{`), a path (`str` or
    `Path`; `.yaml` / `.yml` read as YAML, placeholders expanded), an `LLMQueue` (the boot
    document its settings read, parsed once per queue), or None: the started queue's, else
    `LLMQ_REGISTRY` / `LLMQ_REGISTRY_FILE` from the environment (cached on the file's mtime).
    `RegistryError` (a `ConfigError`) when it does not read or check.
    """
    from llm_queue.registry.validate import load_registry

    registry = _source(registry)
    if isinstance(registry, Mapping):
        return load_registry(registry).registry
    if isinstance(registry, (str, bytes)) and registry.lstrip()[:1] in ("{", b"{"):
        return _read_boot(registry.decode() if isinstance(registry, bytes) else registry, None)
    if isinstance(registry, (str, os.PathLike)):
        return _read_boot(None, os.fspath(registry))
    if registry is None:
        text = os.environ.get("LLMQ_REGISTRY") or None
        path = os.environ.get("LLMQ_REGISTRY_FILE") or None
        if text and path:
            raise ConfigError("set one of LLMQ_REGISTRY and LLMQ_REGISTRY_FILE, not both")
        return _read_boot(text, path)
    return _queue_boot(registry)


def aliases(registry: Any = None) -> dict[str, str]:
    """Provider model id -> registry alias in the boot registry (first alias wins on a repeat).

    `registry` as in `boot_registry`.
    """
    out: dict[str, str] = {}
    for entry in boot_registry(registry).models.values():
        out.setdefault(entry.model_id, entry.alias)
    return out


def _disabled(registry: Any) -> bool:
    """True when the library is switched off: the queue's `enabled`, else `LLMQ_ENABLED`."""
    if _is_queue(registry):
        return not getattr(registry, "enabled", True)
    if registry is None:  # no queue started yet: the environment
        from llm_queue.engine.settings import _FALSE

        return str(os.environ.get("LLMQ_ENABLED", "")).strip().lower() in _FALSE
    return False


def _build(kind: str, model_id: str, registry: Any, kw: dict[str, Any]) -> Any:
    explicit = registry is not None
    registry = _source(registry)
    if _disabled(registry):  # switched off: the app's own path (0.2.0 H1)
        return None
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
    if explicit and _is_queue(registry):  # else the adapter finds the default queue per call
        values["llmq"] = registry
    values.update(kw)
    return getattr(lc, name)(**values)


def chat_model(model_id: str, /, *, registry: Any = None, **kw: Any) -> Any:
    """The queued chat model for `model_id` (an alias or a provider model id), or None.

    None when the boot registry does not know it or the queue is switched off; else
    `QueuedChatBedrockConverse` or `QueuedChatOpenAI` (`**kw` wins). Without `registry=`, the
    started queue decides, else the environment (see the module). `ConfigError` for an
    embedding model or no adapter.
    """
    # `model_id` is positional-only, so `model_id=` in `**kw` reaches the class. `registry` as
    # in `boot_registry`; an `LLMQueue` is also passed as the adapter's `llmq`.
    return _build("chat", model_id, registry, kw)


def embeddings(model_id: str, /, *, registry: Any = None, **kw: Any) -> Any:
    """The queued embeddings for `model_id`, or None; as `chat_model`, for embedding models.

    Bedrock models give `QueuedBedrockEmbeddings`, OpenAI-compatible ones
    `QueuedOpenAIEmbeddings`.
    """
    return _build("embedding", model_id, registry, kw)


__all__ = ["aliases", "boot_registry", "chat_model", "embeddings"]
