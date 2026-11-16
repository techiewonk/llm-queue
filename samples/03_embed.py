"""Embed texts with Titan and Cohere (search indexing, retrieval, follow-up cards).

`embed` returns an `EmbedResult`: one vector per text, in order, plus what it cost (input
tokens in `meta.tokens_in`, provider calls in `batches`, their `request_ids`). The library
splits the texts into batches of the model's `embed_batch_size` (Titan takes 1 text per
request, Cohere up to 96), and every batch goes through the queue and the model's RPM budget.
Cohere needs `input_type`: `search_document` when indexing, `search_query` when searching.

A model family the library does not know gets a codec with `register_embedding_family`,
before `start()`; a registry model then names it in `family`. The one below is registered but
no sample model uses it, so the run is the same with or without it.

    python scripts/samples.py 03_embed [--live]
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Mapping, Sequence
from typing import Any

from llm_queue import EmbeddingCodec, LLMQueue, register_embedding_family

TEXTS = [
    "Nifty 50 closed higher on banking gains.",
    "Banking stocks lifted the Nifty index today.",
    "The monsoon arrived early in Kerala.",
]


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


def my_request(texts: Sequence[str], options: Mapping[str, Any]) -> dict[str, Any]:
    """The InvokeModel body of a made-up family: {"inputs": [...]}."""
    return {"inputs": list(texts), **options}


def my_reply(body: Any) -> list[list[float]]:
    """Its reply: {"vectors": [[...], ...]}, one per text, in order."""
    return [[float(x) for x in v] for v in body["vectors"]]


async def main() -> None:
    # In the registry: "my-model": {"type": "embedding", ..., "family": "my-family"}
    register_embedding_family(
        "my-family", EmbeddingCodec("my-family", my_request, my_reply, max_batch=16)
    )
    llmq = LLMQueue.from_env()
    await llmq.start()
    try:
        for model, input_type in (("titan", None), ("cohere", "search_document")):
            result = await llmq.embed(model, TEXTS, input_type=input_type)
            vectors = result.vectors
            assert len(vectors) == len(TEXTS), f"{model}: {len(vectors)} vectors"
            close, far = cosine(vectors[0], vectors[1]), cosine(vectors[0], vectors[2])
            print(
                f"{model}: {len(vectors)} vectors of {len(vectors[0])} dims; "
                f"similar pair {close:.3f}, unrelated pair {far:.3f}"
            )
            print(
                f"  {result.meta.tokens_in} input tokens in {result.batches} batch(es), "
                f"{len(result.request_ids)} request id(s)"
            )
            assert result.batches == len(result.request_ids) > 0
        query = await llmq.embed("cohere", ["bank index rally"], input_type="search_query")
        print(f"cohere query vector: {len(query.vectors[0])} dims")
    finally:
        await llmq.stop()


if __name__ == "__main__":
    asyncio.run(main())
