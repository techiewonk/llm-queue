"""Embeddings over the queue (plan step A13): batching, provider bodies, vectors in order.

`embed()` splits texts into batches of the model's `embed_batch_size` (capped by its family's
`EmbeddingCodec`: Titan 1, Cohere 96, OpenAI 2048), sends each batch as one call (one request
against RPM; tokens only when token limiting is on for the model), runs them concurrently and
returns one vector per text in the input order. Inference defaults (dimensions, input_type,
...) are added by the worker from the registry. A batch the provider refuses is split and sent
again until only the bad text fails; texts over the model's length limit fail before any call.
"""

from __future__ import annotations

import asyncio
import base64
import itertools
import sys
from array import array
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from llm_queue.clients.providers import decode_invoke_body
from llm_queue.engine.errors import CallFailed
from llm_queue.errors import ConfigError
from llm_queue.providers.embeddings import EmbeddingCodec, batches, codec_for, request_body

if TYPE_CHECKING:
    from llm_queue.engine.runtime import Engine


# A refusal of the batch's content: the batch is split in two and each half sent again, so
# one bad text no longer failed (and hid among) up to 2,047 good ones (R3JE-17, Q19).
SPLIT_KINDS = frozenset({"invalid_request", "context_length_exceeded", "request_too_large"})


def batch_size(codec: EmbeddingCodec, setting: int) -> int:
    """Texts per call: the model's `embed_batch_size`, never above its family's limit."""
    return max(1, min(int(setting), codec.max_batch))


def request_for(
    provider: str, codec: EmbeddingCodec, texts: Sequence[str], input_type: str | None = None
) -> dict[str, Any]:
    """The provider-native request of one batch (defaults are merged by the worker).

    `input_type` (Cohere: `search_query` / `search_document`) wins over the registry default.
    """
    options: Mapping[str, Any] = {"input_type": input_type} if input_type else {}
    return request_body(provider, codec, texts, options)


def vectors_of(provider: str, codec: EmbeddingCodec, response: dict[str, Any]) -> list[list[float]]:
    """The vectors of one reply in the order of its texts (an InvokeModel body decoded first).

    A reply the worker packed (`pack_reply`) is unpacked instead; an older worker's raw body
    is read as before.
    """
    packed = response.get(PACKED)
    if packed is not None:
        return _unpack(packed)
    body = response if provider == "openai" else decode_invoke_body(response)
    return codec.parse_reply(body)


# The reply field of vectors a worker packed for `embed()` (the envelope's `vectors` flag).
PACKED = "llmq_vectors"
_PARSE_ERRORS = (KeyError, ValueError, TypeError, IndexError, AttributeError, OverflowError)


def pack_reply(
    provider: str, codec: EmbeddingCodec, response: dict[str, Any]
) -> dict[str, Any] | None:
    """`response` as its vectors in binary (base64), or None when its body cannot be read.

    A Cohere batch of 96 x 1024 floats was stored as base64 of its JSON body, 3.1 MB a
    result; with the reply copies and client buffers around it, 300 such calls peaked at
    +105 MB of store memory (gate B97, +64 MB allowed). Packed it is 1.05 MB (float64), or
    0.52 MB when every value is exactly a float32: lossless either way. The worker packs only
    for `embed()`, which reads nothing else of the body; a raw InvokeModel call keeps its body.
    """
    try:
        vectors = vectors_of(provider, codec, response)
        dims = len(vectors[0]) if vectors else 0
        if any(len(v) != dims for v in vectors):
            return None  # the caller's own check reports it, from the raw body
        flat = list(itertools.chain.from_iterable(vectors))
        try:
            data = array("f", flat)
            exact = data.tolist() == flat
        except OverflowError:  # above float32's range
            exact = False
        if not exact:
            data = array("d", flat)
    except _PARSE_ERRORS:
        return None  # stored raw: the caller fails it as an unreadable body, as before
    if sys.byteorder == "big":
        data.byteswap()  # stored little-endian, like `vector()` reads base64 float32
    blob = base64.b64encode(data.tobytes()).decode("ascii")
    return {PACKED: {"type": data.typecode, "count": len(vectors), "dims": dims, "data": blob}}


def _unpack(packed: Any) -> list[list[float]]:
    """The vectors of a `pack_reply` field; `ValueError` / `TypeError` when it is malformed."""
    if not isinstance(packed, Mapping) or packed.get("type") not in ("f", "d"):
        raise TypeError("packed vectors of an unknown type")
    count, dims = int(packed["count"]), int(packed["dims"])
    data = array("d" if packed["type"] == "d" else "f")
    data.frombytes(base64.b64decode(packed["data"], validate=True))
    if len(data) != count * dims:
        raise ValueError("packed vectors of the wrong length")
    if sys.byteorder == "big":
        data.byteswap()
    flat = data.tolist()
    return [flat[i * dims : (i + 1) * dims] for i in range(count)]


def check_texts(texts: object, request_id: str | None = None) -> None:
    """Refuse what is not a list of non-empty strings, before any call (`invalid_request`).

    A single `str` was iterated letter by letter (ten one-letter calls for "helloworld"), and
    a `None` in the list raised a bare `AttributeError` (R3JE-19).
    """
    if isinstance(texts, str | bytes | bytearray) or not isinstance(texts, Sequence):
        raise CallFailed(
            "invalid_request", "texts must be a list of strings", request_id=request_id
        )
    for index, text in enumerate(texts):
        if not isinstance(text, str):
            message = f"text {index} is {type(text).__name__}, not a string"
            raise CallFailed("invalid_request", message, request_id=request_id)
        if not text.strip():
            raise CallFailed("invalid_request", f"text {index} is empty", request_id=request_id)


@dataclass(slots=True)
class EmbedOutcome:
    """What one `embed` produced: vectors in order, errors by text index, and the usage.

    `tokens_in` sums the input tokens the replies reported (None when none did), `batches`
    counts provider calls (splits included) and `request_ids` lists the answered ones (Q21).
    """

    vectors: list[list[float]]
    errors: dict[int, CallFailed] = field(default_factory=dict)
    tokens_in: int | None = None
    batches: int = 0
    request_ids: list[str] = field(default_factory=list)
    provider_request_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _Part:
    """The results of one batch (and its splits): a vector or an error per text."""

    items: list[list[float] | CallFailed]
    tokens_in: int | None = None
    batches: int = 0
    request_ids: list[str] = field(default_factory=list)
    provider_request_ids: list[str] = field(default_factory=list)

    def add(self, other: _Part) -> None:
        """Append another part's results and usage."""
        self.items += other.items
        self.batches += other.batches
        self.request_ids += other.request_ids
        self.provider_request_ids += other.provider_request_ids
        if other.tokens_in is not None:
            self.tokens_in = (self.tokens_in or 0) + other.tokens_in


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
    outcome = await embed_detailed(
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
    return outcome.vectors


async def embed_partial(
    engine: Engine,
    model: str,
    texts: Sequence[str],
    *,
    partial: bool = True,
    **kwargs: Any,
) -> tuple[list[list[float]], dict[int, CallFailed]]:
    """Vectors in order, and the error of each text that failed (its vector is `[]`)."""
    outcome = await embed_detailed(engine, model, texts, partial=partial, **kwargs)
    return outcome.vectors, outcome.errors


def _max_chars(view: Any, alias: str, codec: EmbeddingCodec) -> int:
    """The model's `embed_max_chars`, else its family's limit; 0 when there is none."""
    setting = view.value(alias, "embed_max_chars")
    if setting is not None:
        return int(setting)
    return codec.max_chars or 0


async def embed_detailed(
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
) -> EmbedOutcome:
    """Vectors in order, the error of each text that failed (its vector is `[]`), and usage.

    Before any call (`invalid_request`, naming the text's index): empty, non-string or too
    long texts, an `input_type` the family does not take, and a model with no family. At most
    `concurrency` batches exist at once. With `request_id`, batch n is `<request_id>:<n>` (one
    id for all was refused as a duplicate: A1), its halves `<n>.0` / `<n>.1`. A batch refused
    for its content is split until the bad text alone fails, with its index in the message.
    `partial=False` raises the first failure instead. Vectors of different lengths fail the
    call (`provider_error`).
    """
    check_texts(texts, request_id)
    if not texts:
        return EmbedOutcome([])
    view = engine.registry.current
    alias = view.registry.alias_of(model)
    entry = view.registry.models[alias]
    try:
        codec = codec_for(entry.provider, entry.model_id, getattr(entry, "family", None))
    except ConfigError as exc:  # the registry refuses this at load; a family not registered here
        raise CallFailed("invalid_request", f"{alias}: {exc}", request_id=request_id) from exc
    allowed = codec.input_types
    if input_type is not None and allowed is not None and input_type not in allowed:
        # A typo cost one provider request per batch, each refused (R3JE-24).
        message = f"input_type {input_type!r}: use one of {sorted(allowed)}"
        raise CallFailed("invalid_request", message, request_id=request_id)
    if limit := _max_chars(view, alias, codec):
        for index, text in enumerate(texts):
            if len(text) > limit:
                # Refused before the call: the provider refused the whole batch, and each
                # such batch spent a request of the budget (R3JE-20, Q20).
                message = f"text {index} has {len(text)} characters, above {limit} for {alias}"
                raise CallFailed("invalid_request", message, request_id=request_id)
    setting = int(view.value(alias, "embed_batch_size"))
    size = batch_size(codec, setting)
    groups = list(batches(list(texts), size))

    async def send(label: str, group: list[str]) -> tuple[list[list[float]], Any]:
        reply = await engine.call(
            alias,
            request_for(entry.provider, codec, group, input_type),
            priority=priority,
            timeout_secs=timeout_secs,
            request_id=f"{request_id}:{label}" if request_id else None,
            vectors=True,  # the worker stores the vectors packed (B97: 3x smaller)
            **call,
        )
        if reply.response is None:
            raise CallFailed(
                "provider_error", f"{alias}: no embedding body", request_id=reply.request_id
            )
        try:
            vectors = vectors_of(entry.provider, codec, reply.response)
        except (KeyError, ValueError, TypeError, IndexError, AttributeError) as exc:
            # A raw parse error is not a `CallFailed`: with `partial` it aborted every batch,
            # and the app got an exception outside `LLMQueueError` (EN-6).
            raise CallFailed(
                "provider_error", f"{alias}: unreadable embedding body", request_id=reply.request_id
            ) from exc
        if len(vectors) != len(group):
            message = f"{alias}: {len(group)} texts gave {len(vectors)} vectors"
            raise CallFailed("provider_error", message, request_id=reply.request_id)
        return vectors, reply

    async def one(label: str, start: int, group: list[str]) -> _Part:
        try:
            vectors, reply = await send(label, group)
        except CallFailed as exc:
            if exc.kind in SPLIT_KINDS and len(group) > 1:
                half = len(group) // 2  # halves in turn: a split is rare, the window stays
                part = await one(f"{label}.0", start, group[:half])
                part.add(await one(f"{label}.1", start + half, group[half:]))
                part.batches += 1
                return part
            failed = exc
            if exc.kind in SPLIT_KINDS:  # one text: say which
                failed = CallFailed(
                    exc.kind, f"text {start}: {exc}", request_id=exc.request_id or request_id
                )
            if not partial:
                raise failed from exc
            return _Part([failed] * len(group), batches=1)
        tokens = getattr(reply.meta, "tokens_in", None)
        provider_id = getattr(reply.meta, "provider_request_id", "") or ""
        return _Part(
            list(vectors), tokens, 1, [reply.request_id], [provider_id] if provider_id else []
        )

    jobs = [(str(n), n * size, group) for n, group in enumerate(groups)]
    total = _Part([])
    for part in await _windowed(jobs, one, concurrency):
        total.add(part)
    outcome = EmbedOutcome(
        [], {}, total.tokens_in, total.batches, total.request_ids, total.provider_request_ids
    )
    for index, item in enumerate(total.items):
        if isinstance(item, CallFailed):
            outcome.errors[index] = item
            outcome.vectors.append([])
        else:
            outcome.vectors.append(item)
    sizes = {len(vector) for vector in outcome.vectors if vector}
    if len(sizes) > 1:
        # A live edit of `dimensions` between two batches of one call mixed lengths (R3JE-23).
        message = f"{alias}: vectors of different lengths {sorted(sizes)} in one call"
        raise CallFailed("provider_error", message, request_id=request_id)
    return outcome


async def _windowed(
    jobs: list[tuple[str, int, list[str]]],
    one: Callable[[str, int, list[str]], Awaitable[_Part]],
    concurrency: int,
) -> list[_Part]:
    """Run `one(*job)` for every job, at most `concurrency` at once; results in job order.

    The first exception cancels the rest (they would only spend budget on a lost result).
    """
    results: list[_Part | None] = [None] * len(jobs)
    running: dict[asyncio.Future[_Part], int] = {}
    pending = iter(enumerate(jobs))
    try:
        while True:
            while len(running) < max(1, concurrency):  # a window, not every batch up front
                if (item := next(pending, None)) is None:
                    break
                running[asyncio.ensure_future(one(*item[1]))] = item[0]
            if not running:
                break
            done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                n = running.pop(task)
                results[n] = task.result()  # raises the batch's failure
    except BaseException:
        for task in running:
            task.cancel()
        await asyncio.gather(*running, return_exceptions=True)
        raise
    return [part for part in results if part is not None]
