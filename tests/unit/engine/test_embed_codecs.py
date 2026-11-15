"""Embedding family codecs: resolution order, the unknown-id error, built-ins, custom families.

Every family's limits, request body and reply parsing sit in one `EmbeddingCodec`; these tests
pin the table and its users (`embed`, the registry check, the health probe) without a store.
"""

from __future__ import annotations

import base64
import struct
from collections.abc import Iterator, Mapping, Sequence
from types import SimpleNamespace
from typing import Any

import pytest

import llm_queue
from llm_queue.engine import embed
from llm_queue.engine.envelope import Meta, Reply
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.runtime import _probe_params
from llm_queue.errors import ConfigError
from llm_queue.providers import embeddings
from llm_queue.providers.embeddings import (
    COHERE,
    OPENAI,
    TITAN,
    EmbeddingCodec,
    codec_for,
    embedding_families,
    register_embedding_family,
)
from llm_queue.registry.schema import build

ARN = "arn:aws:bedrock:us-east-1:1:application-inference-profile/x"


@pytest.fixture(autouse=True)
def _families() -> Iterator[None]:
    """Each test's registrations are dropped afterwards (the table is process-wide)."""
    saved = dict(embeddings._FAMILIES)
    yield
    embeddings._FAMILIES.clear()
    embeddings._FAMILIES.update(saved)


def _nova_request(texts: Sequence[str], options: Mapping[str, Any]) -> dict[str, Any]:
    return {"taskType": "EMBED", "items": [{"text": t} for t in texts], **options}


def _nova_reply(body: Any) -> list[list[float]]:
    return [embeddings.vector(item["vec"]) for item in body["results"]]


NOVA = EmbeddingCodec(
    "nova", _nova_request, _nova_reply, max_batch=3, max_chars=10, patterns=("amazon.nova-embed",)
)


# ------------------------------------------------------------------------- resolution


def test_family_field_wins_over_provider_and_id() -> None:
    """The registry `family` decides first, even against an id that names another family."""
    assert codec_for("bedrock", "amazon.titan-embed-text-v2:0", "cohere") is COHERE
    assert codec_for("openai", "text-embedding", "titan") is TITAN
    assert codec_for("bedrock", ARN, "cohere") is COHERE


def test_provider_then_id_patterns() -> None:
    """OpenAI by provider; Bedrock by the first codec whose pattern is in the id (any case)."""
    assert codec_for("openai", "cohere.embed-english-v3") is OPENAI
    assert codec_for("bedrock", "amazon.titan-embed-text-v2:0") is TITAN
    assert codec_for("bedrock", "us.Amazon.Titan-Embed-Text-v1") is TITAN
    assert codec_for("bedrock", "Cohere.Embed-Multilingual-v3") is COHERE


def test_unknown_bedrock_id_is_a_config_error_not_titan() -> None:
    """The old bug: any id that was not Cohere's got Titan's body (one text, `inputText`)."""
    with pytest.raises(ConfigError, match=r"cannot tell the embedding family of arn:.*`family`"):
        codec_for("bedrock", ARN)
    with pytest.raises(ConfigError, match="unknown embedding family 'nova'"):
        codec_for("bedrock", ARN, "nova")


def _registry(model_id: str, family: str | None = None) -> dict[str, Any]:
    model: dict[str, Any] = {"type": "embedding", "provider": "bedrock", "model_id": model_id}
    if family is not None:
        model["family"] = family
    return {
        "registry_version": 1,
        "accounts": {"main": {"kind": "aws"}},
        "models": {"e": {**model, "deployments": {"main": {"confirmed": ["us-east-1"]}}}},
    }


def test_registry_refuses_an_embedding_model_it_cannot_tell() -> None:
    """At load, naming the path, the id and the fix; with `family` the same id loads."""
    _, errors, _ = build(_registry(ARN))
    assert errors == [
        f"models.e.family: cannot tell the embedding family of {ARN}; set `family` "
        "(one of titan, cohere, openai)"
    ]
    registry, errors, _ = build(_registry(ARN, "titan"))
    assert errors == []
    assert registry is not None
    assert registry.models["e"].family == "titan"


def test_registry_accepts_a_registered_family() -> None:
    """A family the app registered is a valid `family`, and its patterns resolve ids too."""
    assert build(_registry(ARN, "nova"))[1] != []
    register_embedding_family("nova", NOVA)
    assert "nova" in embedding_families()
    assert build(_registry(ARN, "nova"))[1] == []
    assert build(_registry("amazon.nova-embed-v1:0"))[1] == []


def test_a_registered_pattern_wins_over_a_built_in_one() -> None:
    """Newest first: a narrower `cohere.embed-v4` pattern is not shadowed by `cohere.embed`."""
    v4 = EmbeddingCodec("cohere-v4", _nova_request, _nova_reply, 96, patterns=("cohere.embed-v4",))
    register_embedding_family("cohere-v4", v4)
    assert codec_for("bedrock", "cohere.embed-v4:0") is v4
    assert codec_for("bedrock", "cohere.embed-english-v3") is COHERE


@pytest.mark.parametrize(
    ("name", "codec"),
    [
        ("Bad Name", NOVA),
        ("nova", "not a codec"),
        ("nova", EmbeddingCodec("nova", _nova_request, _nova_reply, max_batch=0)),
        ("nova", EmbeddingCodec("nova", _nova_request, _nova_reply, 1, max_chars=-1)),
    ],
)
def test_register_checks_its_input(name: str, codec: Any) -> None:
    """A bad name or codec fails at registration with `ConfigError`, not at the first call."""
    with pytest.raises(ConfigError):
        register_embedding_family(name, codec)


def test_register_is_public() -> None:
    """`register_embedding_family` and `EmbeddingCodec` are exported from `llm_queue`."""
    assert "register_embedding_family" in llm_queue.__all__
    assert llm_queue.register_embedding_family is register_embedding_family
    assert llm_queue.EmbeddingCodec is EmbeddingCodec


# ------------------------------------------------------------------------- built-ins


def test_titan_build_and_parse() -> None:
    """One `inputText`; v2's `dimensions` / `normalize` when given; `embedding` read back."""
    assert TITAN.build_request(["a"], {}) == {"inputText": "a"}
    assert TITAN.build_request(["a"], {"dimensions": 256, "normalize": True}) == {
        "inputText": "a",
        "dimensions": 256,
        "normalize": True,
    }
    assert TITAN.build_request(["a"], {"input_type": "search_query"}) == {"inputText": "a"}
    with pytest.raises(ValueError, match="exactly one"):
        TITAN.build_request(["a", "b"], {})
    assert TITAN.parse_reply({"embedding": [1, 2.5]}) == [[1.0, 2.5]]
    assert (TITAN.max_batch, TITAN.max_chars) == (1, 50_000)


def test_cohere_build_and_parse() -> None:
    """Up to 96 `texts` with `input_type`; plain and typed (`float`) replies; numbers only."""
    assert COHERE.build_request(["a", "b"], {}) == {"texts": ["a", "b"]}
    body = COHERE.build_request(["a"], {"input_type": "search_query", "embedding_types": ["float"]})
    assert body == {"texts": ["a"], "input_type": "search_query", "embedding_types": ["float"]}
    with pytest.raises(ValueError, match="at most 96"):
        COHERE.build_request(["a"] * 97, {})
    assert COHERE.parse_reply({"embeddings": [[1.0], [2.0]]}) == [[1.0], [2.0]]
    assert COHERE.parse_reply({"embeddings": {"float": [[3.0]]}}) == [[3.0]]
    for bad in ({"embeddings": [["x"]]}, {"nothing": 1}, b"raw"):
        with pytest.raises((ValueError, TypeError)):
            COHERE.parse_reply(bad)
    assert (COHERE.max_batch, COHERE.max_chars) == (96, 2_048)
    assert COHERE.input_types is not None
    assert "search_query" in COHERE.input_types


def test_openai_build_and_parse() -> None:
    """`input` as base64; `data[i].embedding` in index order, base64 decoded, 0..n-1 only."""
    assert OPENAI.build_request(["a", "b"], {}) == {
        "input": ["a", "b"],
        "encoding_format": "base64",
    }
    packed = base64.b64encode(struct.pack("<2f", 0.5, -1.0)).decode()
    data = [{"index": 1, "embedding": [2.0]}, {"index": 0, "embedding": packed}]
    assert OPENAI.parse_reply({"data": data}) == [[0.5, -1.0], [2.0]]
    with pytest.raises(ValueError, match="indices"):
        OPENAI.parse_reply({"data": [{"index": 1, "embedding": [1.0]}]})
    assert (OPENAI.max_batch, OPENAI.max_chars) == (2048, None)


def test_registry_defaults_follow_the_codec() -> None:
    """The worker sniffed `inputText` to tell Titan from Cohere; now the codec names the fields."""
    from llm_queue.engine import inference

    defaults = {"dimensions": 256, "input_type": "search_document"}
    nova = inference.apply("bedrock", "invoke_model", {"body": {"items": []}}, defaults, {},
                           codec=NOVA)  # fmt: skip
    assert nova["body"] == {"items": []}  # no `inference_names`: left as built
    picky = EmbeddingCodec(
        "p", _nova_request, _nova_reply, 1, inference_names=frozenset({"dimensions"})
    )
    body = {"items": [], "truncate": "END"}
    out = inference.apply("bedrock", "invoke_model", {"body": body}, defaults, {}, codec=picky)
    assert out["body"] == {"items": [], "dimensions": 256}


def test_probe_uses_the_family_codec() -> None:
    """The probe guessed from the id alone: a Cohere model behind an ARN got Titan's body."""
    assert _probe_params("bedrock", "embedding", ARN, "cohere") == {
        "body": {"texts": ["ping"], "input_type": "search_query"}
    }
    assert _probe_params("bedrock", "embedding", "amazon.titan-embed-text-v2:0") == {
        "body": {"inputText": "ping"}
    }
    assert _probe_params("openai", "embedding", "m")["input"] == ["ping"]


# ------------------------------------------------------------------------- end to end


class FakeEngine:
    """A Bedrock embedding model of the `nova` family that answers in Nova's own shape."""

    def __init__(self, *, family: str | None = "nova", model_id: str = ARN, batch: int = 8):
        entry = SimpleNamespace(provider="bedrock", model_id=model_id, family=family)
        registry = SimpleNamespace(alias_of=lambda m: m, models={"e": entry})
        values = {"embed_batch_size": batch, "embed_max_chars": None}
        view = SimpleNamespace(registry=registry, value=lambda _a, name: values[name])
        self.registry = SimpleNamespace(current=view)
        self.requests: list[dict[str, Any]] = []

    async def call(self, _alias: str, request: dict[str, Any], **_: Any) -> Reply:
        self.requests.append(request)
        items = request["body"]["items"]
        results = [{"vec": [float(len(item["text"]))]} for item in items]
        response = {"body": {"results": results}}
        return Reply(request_id="r", ok=True, response=response, meta=Meta(tokens_in=1))


@pytest.fixture
def _plain_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fake's Bedrock body is already decoded."""
    monkeypatch.setattr(embed, "decode_invoke_body", lambda response: response["body"])


@pytest.mark.usefixtures("_plain_body")
async def test_a_registered_codec_runs_through_embed() -> None:
    """Batch size, max chars, request and reply all come from the registered codec."""
    register_embedding_family("nova", NOVA)
    engine: Any = FakeEngine()
    vectors = await embed.embed(engine, "e", ["a", "bb", "ccc", "dddd"], input_type="query")
    assert vectors == [[1.0], [2.0], [3.0], [4.0]]
    assert [len(r["body"]["items"]) for r in engine.requests] == [3, 1]  # max_batch 3 < 8
    assert engine.requests[0]["body"]["input_type"] == "query"  # no closed set: passed on
    with pytest.raises(CallFailed, match="above 10"):
        await embed.embed(engine, "e", ["x" * 11])


@pytest.mark.usefixtures("_plain_body")
async def test_an_unregistered_family_fails_before_any_call() -> None:
    """A worker that never registered the app's family refuses the call, sends nothing."""
    engine: Any = FakeEngine()
    with pytest.raises(CallFailed) as info:
        await embed.embed(engine, "e", ["a"])
    assert info.value.kind == "invalid_request"
    assert engine.requests == []
