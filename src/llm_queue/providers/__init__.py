"""Provider helpers shared by the clients: error classification, embedding codecs, host checks.

The providers themselves (one attempt on a route) live in `llm_queue.clients`. Importing
this package never imports aiobotocore or openai.
"""

from __future__ import annotations

from llm_queue.providers.classify import (
    StreamTruncated,
    classify_bedrock,
    classify_openai,
    error_family,
    is_auth_error,
    is_connection_reset,
    is_target_error,
    parse_retry_after,
    request_error_kind,
)
from llm_queue.providers.embeddings import (
    COHERE_MAX_BATCH,
    OPENAI_DEFAULT_CHUNK_SIZE,
    TITAN_MAX_BATCH,
    EmbeddingCodec,
    batches,
    codec_for,
    embedding_families,
    register_embedding_family,
    request_body,
    vector,
)

__all__ = [
    "COHERE_MAX_BATCH",
    "OPENAI_DEFAULT_CHUNK_SIZE",
    "TITAN_MAX_BATCH",
    "EmbeddingCodec",
    "StreamTruncated",
    "batches",
    "classify_bedrock",
    "classify_openai",
    "codec_for",
    "embedding_families",
    "error_family",
    "is_auth_error",
    "is_connection_reset",
    "is_target_error",
    "parse_retry_after",
    "register_embedding_family",
    "request_body",
    "request_error_kind",
    "vector",
]
