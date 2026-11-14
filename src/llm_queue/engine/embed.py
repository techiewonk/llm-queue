"""Embeddings over the queue (plan step A13): batching, provider bodies, vectors in order.

`embed()` splits texts into batches of the model's `embed_batch_size` (capped by what the
provider accepts: Titan 1, Cohere 96, OpenAI 2048), sends each batch as one call (one request
against RPM; tokens only when token limiting is on for the model), runs them concurrently and
returns one vector per text in the input order. Inference defaults (dimensions, input_type,
...) are added by the worker from the registry.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from llm_queue.clients.providers import decode_invoke_body
from llm_queue.engine.errors import CallFailed
from llm_queue.providers.embeddings import batches, max_batch_size, parse_embeddings

if TYPE_CHECKING:
    from llm_queue.engine.runtime import Engine


def batch_size(provider: str, model_id: str, setting: int) -> int:
    """Texts per call: the model's `embed_batch_size`, never above the provider's limit."""
    return max(1, min(int(setting), max_batch_size(provider, model_id)))


def request_for(
    provider: str, model_id: str, texts: Sequence[str], input_type: str | None = None
) -> dict[str, Any]:
    """The provider-native request of one batch (defaults are merged by the worker).

    `input_type` (Cohere: `search_query` / `search_document`) wins over the registry default.
    """
    if provider == "openai":
        return {"input": list(texts)}
    if "cohere" in model_id.lower():
        body: dict[str, Any] = {"texts": list(texts)}
        if input_type:
            body["input_type"] = input_type
        return {"body": body}
    return {"body": {"inputText": texts[0]}}


def vectors_of(provider: str, response: dict[str, Any]) -> list[list[float]]:
    """The vectors of one reply, in the order of its texts."""
    if provider == "openai":
        data = sorted(response.get("data", []), key=lambda item: item.get("index", 0))
        return [list(item["embedding"]) for item in data]
    return parse_embeddings(decode_invoke_body(response))


async def embed(
    engine: Engine,
    model: str,
    texts: Sequence[str],
    *,
    priority: bool = False,
    timeout_secs: float | None = None,
    concurrency: int = 16,
    input_type: str | None = None,
    **call: Any,
) -> list[list[float]]:
    """One vector per text, in order; the batches run through the queue concurrently.

    All or nothing: the first failed batch cancels the rest and raises (`embed_partial` keeps
    what succeeded).
    """
    vectors, _ = await embed_partial(
        engine,
        model,
        texts,
        priority=priority,
        timeout_secs=timeout_secs,
        concurrency=concurrency,
        input_type=input_type,
        partial=False,
        **call,
    )
    return vectors


async def embed_partial(
    engine: Engine,
    model: str,
    texts: Sequence[str],
    *,
    priority: bool = False,
    timeout_secs: float | None = None,
    concurrency: int = 16,
    input_type: str | None = None,
    partial: bool = True,
    request_id: str | None = None,
    **call: Any,
) -> tuple[list[list[float]], dict[int, CallFailed]]:
    """Vectors in order, and the error of each text whose batch failed (its vector is `[]`).

    Empty texts fail the whole call first (`invalid_request`); at most `concurrency` batches
    exist at once (100,000 texts no longer made every task up front). With `request_id`, batch
    n is `<request_id>:<n>`: one id for every batch had each later batch rejected as a
    duplicate (A1). `partial=False` raises the first failure instead.
    """
    for index, text in enumerate(texts):
        if not text.strip():
            raise CallFailed("invalid_request", f"text {index} is empty", request_id=request_id)
    if not texts:
        return [], {}
    view = engine.registry.current
    alias = view.registry.alias_of(model)
    entry = view.registry.models[alias]
    size = batch_size(entry.provider, entry.model_id, int(view.value(alias, "embed_batch_size")))
    groups = list(batches(list(texts), size))

    async def one(n: int, group: list[str]) -> list[list[float]]:
        reply = await engine.call(
            alias,
            request_for(entry.provider, entry.model_id, group, input_type),
            priority=priority,
            timeout_secs=timeout_secs,
            request_id=f"{request_id}:{n}" if request_id else None,
            **call,
        )
        if reply.response is None:
            raise CallFailed(
                "provider_error", f"{alias}: no embedding body", request_id=reply.request_id
            )
        try:
            vectors = vectors_of(entry.provider, reply.response)
        except (KeyError, ValueError, TypeError, IndexError, AttributeError) as exc:
            # A raw parse error is not a `CallFailed`: with `partial` it aborted every batch,
            # and the app got an exception outside `LLMQueueError` (EN-6).
            raise CallFailed(
                "provider_error", f"{alias}: unreadable embedding body", request_id=reply.request_id
            ) from exc
        if len(vectors) != len(group):
            message = f"{alias}: {len(group)} texts gave {len(vectors)} vectors"
            raise CallFailed("provider_error", message, request_id=reply.request_id)
        return vectors

    results: list[list[list[float]] | CallFailed | None] = [None] * len(groups)
    running: dict[asyncio.Task[list[list[float]]], int] = {}
    pending = iter(enumerate(groups))
    try:
        while True:
            while len(running) < max(1, concurrency):  # a window, not every batch up front
                if (item := next(pending, None)) is None:
                    break
                running[asyncio.ensure_future(one(*item))] = item[0]
            if not running:
                break
            done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                n = running.pop(task)
                exc = task.exception()
                if exc is None:
                    results[n] = task.result()
                elif partial and isinstance(exc, CallFailed):
                    results[n] = exc
                else:
                    raise exc
    except BaseException:
        # One batch failed: stop the rest, which would only spend budget on a lost result.
        for task in running:
            task.cancel()
        await asyncio.gather(*running, return_exceptions=True)
        raise
    vectors: list[list[float]] = []
    errors: dict[int, CallFailed] = {}
    for group, result in zip(groups, results, strict=True):
        if isinstance(result, CallFailed):
            errors.update({len(vectors) + i: result for i in range(len(group))})
            vectors.extend([] for _ in group)
        else:
            vectors.extend(result or [])
    return vectors, errors
