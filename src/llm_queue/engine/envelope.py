"""Message format version 2 (plan E80) and the reply stored for the caller.

A call's deadline is not an absolute pod time: the message carries `timeout_secs` and the
worker measures it from the queue position (the store's clock), so pod clock skew does not
matter (plan §3.18). Version 2 only: a newer message is returned to the queue, never dropped.
Bytes (Converse images, documents, redacted reasoning) travel as `{"__llmq_b64__": "..."}` and
are decoded back to bytes on read, so botocore gets the bytes it expects (plan C1).
"""

from __future__ import annotations

import base64
import binascii
import hashlib
from typing import Any, Literal

import orjson
from pydantic import BaseModel, ConfigDict, Field, model_validator

from llm_queue.errors import UnsupportedEnvelopeError

VERSION = 2
Op = Literal["converse", "invoke_model", "chat", "embeddings"]
AllowFallback = Literal["all", "regions", "none"]
# A key no request field uses: `{"__b64__": ...}` written by a caller as data was decoded too,
# and invalid base64 there failed the message on the worker (FO-25).
B64 = "__llmq_b64__"
# Set on a dumped message or reply that holds encoded bytes, so readers walk it only then (a
# walk of every reply would cost a full pass over large embedding responses).
_HAS_BYTES = "b64"
# The message size cap's default (`max_message_bytes`): a 4 MB message stays well inside the
# store's limits and Bedrock's own request cap for inline images and documents.
MAX_MESSAGE_BYTES = 4 * 1024 * 1024


class _Encoder:
    """orjson `default` that turns bytes into `{B64: ...}` and remembers it did."""

    __slots__ = ("used",)

    def __init__(self) -> None:
        self.used = False

    def __call__(self, value: Any) -> Any:
        if isinstance(value, bytes | bytearray | memoryview):
            self.used = True
            return {B64: base64.b64encode(value).decode("ascii")}
        raise TypeError(f"{type(value).__name__} is not JSON serializable")


def json_default(value: Any) -> Any:
    """The orjson `default` for params and replies with bytes (streaming chunks use it too)."""
    return _Encoder()(value)


def decode_bytes(node: Any) -> Any:
    """`node` with every `{B64: ...}` turned back into bytes (invalid base64 is left as is)."""
    if isinstance(node, dict):
        if len(node) == 1 and isinstance(node.get(B64), str):
            try:
                return base64.b64decode(node[B64], validate=True)
            except (binascii.Error, ValueError):
                return dict(node)
        return {k: decode_bytes(v) for k, v in node.items()}
    if isinstance(node, list):
        return [decode_bytes(v) for v in node]
    return node


def _dumps(fields: dict[str, Any]) -> bytes:
    encoder = _Encoder()
    data = orjson.dumps(fields, default=encoder)
    # The flag is appended to the object rather than dumped again (one pass over the params).
    return data[:-1] + b',"' + _HAS_BYTES.encode() + b'":true}' if encoder.used else data


def message_size_bytes(params: Any) -> int:
    """The JSON size of a call's params as they travel (bytes base64-encoded): the size cap.

    Checked against `max_message_bytes` before a call is sent; above it the call fails with
    `CallFailed("request_too_large")` instead of reaching the store or Bedrock.
    """
    return len(orjson.dumps(params, default=json_default))


class Envelope(BaseModel):
    """One LLM call on the queue. `params` are provider-native and travel inside the message."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    v: int = VERSION
    request_id: str
    op: Op
    model: str
    params: dict[str, Any]
    priority: bool = False
    caller: str
    timeout_secs: float = Field(gt=0)
    max_wait_secs: float | None = Field(default=None, gt=0)
    allow_fallback: AllowFallback = "all"
    allowed_regions: tuple[str, ...] | None = None
    allowed_accounts: tuple[str, ...] | None = None
    session_id: str | None = None
    user_id: str | None = None
    tenant_id: str | None = None
    app_request_id: bool = False
    stream: bool = False

    def dumps(self) -> bytes:
        """JSON bytes for the queue (fields directly: `model_dump` re-walked the params)."""
        return _dumps({k: v for k, v in self.__dict__.items() if v is not None})


def parse(data: bytes | str) -> Envelope:
    """Decode a message; `UnsupportedEnvelopeError` for a newer version (return it, do not drop)."""
    raw = orjson.loads(data)
    if not isinstance(raw, dict) or not isinstance(raw.get("v"), int):
        raise ValueError("not an envelope")
    if raw["v"] > VERSION:
        raise UnsupportedEnvelopeError(f"message version {raw['v']} is newer than {VERSION}")
    if raw["v"] < VERSION:
        raise ValueError(f"message version {raw['v']} is not supported (this library reads 2)")
    if raw.pop(_HAS_BYTES, False):
        raw["params"] = decode_bytes(raw.get("params"))
    return Envelope.model_validate(raw)


def params_hash(params: Any) -> str:
    """Stable hash of a call's params (duplicate request ids must carry the same params)."""
    data = orjson.dumps(params, option=orjson.OPT_SORT_KEYS, default=json_default)
    return hashlib.sha256(data).hexdigest()[:32]


class Meta(BaseModel):
    """How a call was served: deployment, fallback, attempts, timings, tokens."""

    model_config = ConfigDict(frozen=True)

    served_model: str = ""
    deployment: str = ""
    fallback: Literal["region", "model"] | None = None
    attempts: int = 0
    queue_wait_ms: int = 0
    provider_ms: int = 0
    tokens_in: int | None = None
    tokens_out: int | None = None
    worker: str = ""


class Reply(BaseModel):
    """The stored result of a call: the provider reply, or an error kind with its details."""

    model_config = ConfigDict(frozen=True)

    request_id: str
    ok: bool
    response: dict[str, Any] | None = None
    error: str | None = None
    message: str = ""
    retry_after_secs: float | None = None
    partial_text: str | None = None
    meta: Meta = Field(default_factory=Meta)

    @model_validator(mode="before")
    @classmethod
    def _bytes_back(cls, data: Any) -> Any:
        """A stored reply that held bytes (redacted reasoning) gets them back as bytes."""
        if isinstance(data, dict) and data.get(_HAS_BYTES):
            data = {k: v for k, v in data.items() if k != _HAS_BYTES}
            data["response"] = decode_bytes(data.get("response"))
        return data

    def dumps(self) -> bytes:
        """JSON bytes for the store (fields directly: `model_dump` re-walked the response)."""
        out = {k: v for k, v in self.__dict__.items() if v is not None}
        out["meta"] = {k: v for k, v in self.meta.__dict__.items() if v is not None}
        return _dumps(out)
