"""LangChain adapters that send model calls through the LLM queue (extras `langchain-*`).

The adapter classes are loaded on first attribute access, so importing this package (for
example to call `set_default_queue` from `LLMQueue.start()`) never imports langchain, boto3 or
httpx. `QueuedChatBedrockConverse` / `QueuedBedrockEmbeddings` need `[langchain-aws]`;
`QueuedChatOpenAI` / `QueuedOpenAIEmbeddings` need `[openai,langchain-openai]`.

`chat_model(model_id)` / `embeddings(model_id)` pick the queued class for a registry model, or
return None when the queue is off or the model is not in the boot registry (`factories`).
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from llm_queue.errors import MissingExtra
from llm_queue.langchain._client import (
    EmbedResultLike,
    QueueClient,
    ReplyLike,
    get_default_queue,
    set_default_queue,
)
from llm_queue.langchain.factories import aliases, boot_registry, chat_model, embeddings

if TYPE_CHECKING:
    from llm_queue.langchain.bedrock import (
        QueuedBedrockEmbeddings,
        QueuedChatBedrockConverse,
    )
    from llm_queue.langchain.openai import QueuedChatOpenAI, QueuedOpenAIEmbeddings

# Lazy exports: attribute name -> (module, extra that provides its dependencies).
_LAZY = {
    "QueuedChatBedrockConverse": ("bedrock", "langchain-aws"),
    "QueuedBedrockEmbeddings": ("bedrock", "langchain-aws"),
    "QueuedChatOpenAI": ("openai", "openai,langchain-openai"),
    "QueuedOpenAIEmbeddings": ("openai", "openai,langchain-openai"),
}

__all__ = [
    "EmbedResultLike",
    "QueueClient",
    "QueuedBedrockEmbeddings",
    "QueuedChatBedrockConverse",
    "QueuedChatOpenAI",
    "QueuedOpenAIEmbeddings",
    "ReplyLike",
    "aliases",
    "boot_registry",
    "chat_model",
    "embeddings",
    "get_default_queue",
    "set_default_queue",
]


def __getattr__(name: str) -> Any:
    """Import an adapter's module on first use, naming the extra to install if it is missing."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module, extra = _LAZY[name]
    try:
        value = getattr(import_module(f"{__name__}.{module}"), name)
    except ImportError as exc:
        raise MissingExtra(name, extra) from exc
    globals()[name] = value
    return value
