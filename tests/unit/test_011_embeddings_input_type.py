"""0.1.1: the Cohere `input_type` of the LangChain embeddings adapter is configurable.

An index whose documents were embedded with `embed_query` (`search_query`) kept matching only
if documents went in the same way; the adapter always sent `search_document` for documents.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("langchain_aws")

from llm_queue.langchain import QueuedBedrockEmbeddings


class _Result:
    def __init__(self, n: int) -> None:
        self.vectors = [[1.0, 0.0]] * n


class _Queue:
    """Records the `input_type` of each embed call."""

    def __init__(self) -> None:
        self.input_types: list[str | None] = []

    def embed_sync(self, model: str, texts: list[str], **kw: Any) -> _Result:
        self.input_types.append(kw.get("input_type"))
        return _Result(len(texts))

    async def embed(self, model: str, texts: list[str], **kw: Any) -> _Result:
        self.input_types.append(kw.get("input_type"))
        return _Result(len(texts))

    # The rest of the queue surface the adapter checks for (unused here).
    async def converse(self, *_: Any, **__: Any) -> None: ...
    def converse_sync(self, *_: Any, **__: Any) -> None: ...
    async def chat(self, *_: Any, **__: Any) -> None: ...


def _embeddings(queue: _Queue, **kw: Any) -> QueuedBedrockEmbeddings:
    return QueuedBedrockEmbeddings(model_id="cohere.embed-english-v3", llmq=queue, **kw)


async def test_defaults_query_and_document_types() -> None:
    queue = _Queue()
    emb = _embeddings(queue)
    emb.embed_query("q")
    emb.embed_documents(["a"])
    await emb.aembed_query("q")
    await emb.aembed_documents(["a"])
    assert queue.input_types == ["search_query", "search_document"] * 2


async def test_documents_can_be_embedded_as_queries() -> None:
    queue = _Queue()
    emb = _embeddings(queue, document_input_type="search_query")
    emb.embed_documents(["a", "b"])
    await emb.aembed_documents(["a"])
    assert queue.input_types == ["search_query", "search_query"]
