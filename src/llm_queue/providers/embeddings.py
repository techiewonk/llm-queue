"""Embedding families: one codec per request / reply shape, and the table that resolves them.

An `EmbeddingCodec` holds everything that differs between embedding model families: how many
texts one call takes, the longest text, the request body and how the reply becomes vectors.
`codec_for` picks a model's codec (registry `family`, then provider, then the model id), and
`register_embedding_family` lets an app add a family (Cohere v4, Nova, ...) without a release.
"""

from __future__ import annotations

import base64
import re
import sys
from array import array
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Literal, TypeVar, get_args

from llm_queue.errors import ConfigError

_T = TypeVar("_T")  # PEP 695 syntax needs 3.12; the package supports 3.11

TITAN_MAX_BATCH = 1
COHERE_MAX_BATCH = 96
OPENAI_DEFAULT_CHUNK_SIZE = 2048

CohereInputType = Literal["search_document", "search_query", "classification", "clustering"]
COHERE_INPUT_TYPES = frozenset(get_args(CohereInputType))

# The same rule as a registry alias (`NAME_RE`): a family name goes into the registry document.
_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}\Z")

BuildRequest = Callable[[Sequence[str], Mapping[str, Any]], dict[str, Any]]
ParseReply = Callable[[Any], list[list[float]]]


@dataclass(frozen=True, slots=True)
class EmbeddingCodec:
    """One embedding family: its limits, its request body and how its reply is read.

    `build_request(texts, options)` returns the model's JSON body (Bedrock InvokeModel's `body`,
    or the OpenAI `embeddings.create` arguments); `options` holds the per-call `input_type` when
    one is given (registry `inference` defaults are merged later by the worker).
    `parse_reply(body)` gets the decoded reply body and returns one vector per text, in order;
    it raises `ValueError` / `TypeError` / `KeyError` on a body it cannot read.
    `patterns` are case-insensitive substrings of a model id that name this family.
    `input_types`, when set, is the closed set of `input_type` values the family takes (others
    fail before any call). `probe_options` go into the health probe's one-text request.
    """

    name: str
    build_request: BuildRequest
    parse_reply: ParseReply
    max_batch: int
    max_chars: int | None = None
    patterns: tuple[str, ...] = ()
    input_types: frozenset[str] | None = None
    probe_options: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def matches(self, model_id: str) -> bool:
        """True when one of `patterns` is in the model id, whatever its case (R3JE-21)."""
        lowered = model_id.lower()
        return any(pattern.lower() in lowered for pattern in self.patterns)


def vector(value: Any) -> list[float]:
    """One vector as floats: a JSON list of numbers, or base64 of little-endian float32.

    Anything else raises: a base64 string read as a list gave a "vector" of single
    characters (R3JE-22), and `array` refuses a list holding anything but numbers. Codecs an
    app registers can use it for the same checks.
    """
    if isinstance(value, str):
        raw = base64.b64decode(value, validate=True)
        if not raw or len(raw) % 4:
            raise ValueError("a base64 embedding is not float32")
        floats = array("f")
        floats.frombytes(raw)
        if sys.byteorder == "big":
            floats.byteswap()
        return floats.tolist()
    if not isinstance(value, list) or not value:
        raise TypeError("an embedding is not a list of numbers")
    return array("d", value).tolist()


def _bedrock_vectors(body: Any) -> list[list[float]]:
    """Vectors of a Titan (`embedding`) or Cohere (`embeddings`) reply body.

    Both Bedrock families read either shape, as before the codecs; Cohere's typed form
    (`{"embeddings": {"float": [...]}}`) is accepted too.
    """
    if not isinstance(body, Mapping):
        raise TypeError("the embedding reply is not a JSON object")
    if "embedding" in body:
        return [vector(body["embedding"])]
    vectors = body.get("embeddings")
    if isinstance(vectors, Mapping):
        vectors = vectors.get("float")
    if not isinstance(vectors, list):
        raise ValueError("response has no 'embedding' or 'embeddings'")
    return [vector(v) for v in vectors]


def _titan_request(texts: Sequence[str], options: Mapping[str, Any]) -> dict[str, Any]:
    """Titan: exactly one `inputText`; v2 also takes `dimensions` and `normalize`."""
    if len(texts) != TITAN_MAX_BATCH:
        raise ValueError(f"Titan takes exactly one text per request, got {len(texts)}")
    body: dict[str, Any] = {"inputText": texts[0]}
    for name in ("dimensions", "normalize"):
        if options.get(name) is not None:
            body[name] = options[name]
    return body


def _cohere_request(texts: Sequence[str], options: Mapping[str, Any]) -> dict[str, Any]:
    """Cohere v3: up to 96 `texts`, and `input_type`, `truncate`, `embedding_types` if given."""
    if len(texts) > COHERE_MAX_BATCH:
        raise ValueError(f"Cohere takes at most {COHERE_MAX_BATCH} texts, got {len(texts)}")
    body: dict[str, Any] = {"texts": list(texts)}
    for name in ("input_type", "truncate", "embedding_types"):
        if options.get(name):
            body[name] = options[name]
    return body


def _openai_request(texts: Sequence[str], options: Mapping[str, Any]) -> dict[str, Any]:
    """OpenAI-compatible: `input`, with the vectors asked for as base64.

    base64 float32 on the way back: the reply is about 3x smaller in the store and far
    cheaper to write and read than a JSON list of floats (R3PF-10). The SDK asks for base64
    anyway and decodes it itself; asked explicitly, it hands the string through.
    """
    return {"input": list(texts), "encoding_format": "base64"}


def _openai_reply(body: Any) -> list[list[float]]:
    """`data[i].embedding` in `index` order; the indices must be exactly 0..n-1.

    A missing or repeated index put a vector on the wrong text without an error.
    """
    data = sorted(body.get("data", []), key=lambda item: item.get("index", 0))
    if [item.get("index", 0) for item in data] != list(range(len(data))):
        raise ValueError("embedding indices are not 0..n-1")
    return [vector(item["embedding"]) for item in data]


# Longest text per family, in characters (AWS model pages, not re-checked here): Titan v2
# 50,000 (8,192 tokens), Cohere v3 2,048. OpenAI counts tokens, so the server decides. A
# model's `embed_max_chars` wins over these.
TITAN = EmbeddingCodec(
    "titan", _titan_request, _bedrock_vectors, TITAN_MAX_BATCH, 50_000, ("titan-embed",)
)
COHERE = EmbeddingCodec(
    "cohere",
    _cohere_request,
    _bedrock_vectors,
    COHERE_MAX_BATCH,
    2_048,
    ("cohere.embed",),
    input_types=COHERE_INPUT_TYPES,
    # Cohere refuses a body without `input_type`; the probe skips the registry defaults.
    probe_options=MappingProxyType({"input_type": "search_query"}),
)
OPENAI = EmbeddingCodec("openai", _openai_request, _openai_reply, OPENAI_DEFAULT_CHUNK_SIZE)

_FAMILIES: dict[str, EmbeddingCodec] = {c.name: c for c in (TITAN, COHERE, OPENAI)}


def register_embedding_family(name: str, codec: EmbeddingCodec) -> None:
    """Add (or replace) an embedding family, so a registry model can name it in `family`.

    Register before the registry is loaded, in every process that loads it (callers and
    workers). Patterns of the newest registration are tried first, so a narrower pattern
    (`cohere.embed-v4`) wins over a built-in one (`cohere.embed`).
    """
    # Checked at run time too: apps call this from untyped code, and a bad codec found here
    # fails at boot instead of on the first embed call.
    raw_name: Any = name
    if not (isinstance(raw_name, str) and _NAME_RE.match(raw_name)):
        raise ConfigError(f"embedding family name {name!r}: 1-64 lowercase letters, digits, _.-")
    raw: Any = codec
    if not isinstance(raw, EmbeddingCodec):
        raise ConfigError("register_embedding_family needs an EmbeddingCodec")
    build: Any = codec.build_request
    parse: Any = codec.parse_reply
    max_batch: Any = codec.max_batch
    max_chars: Any = codec.max_chars
    if not callable(build) or not callable(parse):
        raise ConfigError(f"embedding family {name!r}: build_request, parse_reply not callable")
    if not (isinstance(max_batch, int) and max_batch >= 1):
        raise ConfigError(f"embedding family {name!r}: max_batch must be an int of 1 or more")
    if max_chars is not None and not (isinstance(max_chars, int) and max_chars >= 0):
        raise ConfigError(f"embedding family {name!r}: max_chars must be None or an int >= 0")
    _FAMILIES.pop(name, None)  # re-registering moves it to the front of the id search
    _FAMILIES[name] = codec if codec.name == name else replace(codec, name=name)


def embedding_families() -> tuple[str, ...]:
    """The names a registry `family` may take: the built-in ones, then the registered ones."""
    return tuple(_FAMILIES)


def codec_for(provider: str, model_id: str, family: str | None = None) -> EmbeddingCodec:
    """The codec of an embedding model: its registry `family`, its provider, then its id.

    An id no codec matches is a `ConfigError`: it used to be sent Titan's body (one text per
    call) whatever the model was. Registry validation raises it at load, never mid-call.
    """
    if family is not None:
        codec = _FAMILIES.get(family)
        if codec is None:
            raise ConfigError(
                f"unknown embedding family {family!r} (one of {', '.join(_FAMILIES)})"
            )
        return codec
    if provider == "openai":
        return _FAMILIES["openai"]
    for codec in reversed(_FAMILIES.values()):
        if codec.matches(model_id):
            return codec
    raise ConfigError(
        f"cannot tell the embedding family of {model_id}; set `family` "
        f"(one of {', '.join(_FAMILIES)})"
    )


def request_body(
    provider: str, codec: EmbeddingCodec, texts: Sequence[str], options: Mapping[str, Any]
) -> dict[str, Any]:
    """The request the engine sends: the codec's body, under `body` for an InvokeModel call.

    OpenAI-compatible servers take the body as the `embeddings.create` arguments; every other
    provider (Bedrock, custom ones) as InvokeModel's `body`.
    """
    body = codec.build_request(texts, options)
    return body if provider == "openai" else {"body": body}


def batches(items: Sequence[_T], size: int) -> Iterator[list[_T]]:
    """Yield consecutive slices of at most `size` items, preserving order."""
    if size < 1:
        raise ValueError("batch size must be >= 1")
    for start in range(0, len(items), size):
        yield list(items[start : start + size])
