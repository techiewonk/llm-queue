"""Bug-pinning tests for embeddings from round 3 (R3JE-17..24, R3PF-10, Q19-Q22).

A fake engine answers each batch, so they run without the Docker stack or a provider.
"""

from __future__ import annotations

import base64
import struct
from types import SimpleNamespace
from typing import Any

import pytest

from llm_queue.engine import embed
from llm_queue.engine.envelope import Meta, Reply
from llm_queue.engine.errors import CallFailed
from llm_queue.registry.settings import EMBED_INFERENCE


class FakeEngine:
    """An embedding model of one provider; texts in `bad` make a batch be refused."""

    def __init__(
        self,
        *,
        provider: str = "openai",
        model_id: str = "text-embedding",
        family: str | None = None,
        batch: int = 1,
        max_chars: int | None = None,
        bad: frozenset[str] = frozenset(),
        dims: dict[str, int] | None = None,
    ) -> None:
        entry = SimpleNamespace(provider=provider, model_id=model_id, family=family)
        registry = SimpleNamespace(alias_of=lambda m: m, models={"e": entry})
        values = {"embed_batch_size": batch, "embed_max_chars": max_chars}
        view = SimpleNamespace(registry=registry, value=lambda _a, name: values[name])
        self.registry = SimpleNamespace(current=view)
        self.provider = provider
        self.bad = bad
        self.dims = dims or {}
        self.requests: list[dict[str, Any]] = []
        self.ids: list[str | None] = []

    async def call(self, _alias: str, request: dict[str, Any], **kw: Any) -> Reply:
        self.requests.append(request)
        self.ids.append(kw.get("request_id"))
        texts = (
            request.get("input")
            or request["body"].get("texts")
            or [request["body"].get("inputText")]
        )
        if self.bad & set(texts):
            raise CallFailed("invalid_request", "input rejected", request_id="r")
        vectors = [[1.0] * self.dims.get(t, 2) for t in texts]
        if self.provider == "openai":
            data = [{"index": i, "embedding": v} for i, v in enumerate(vectors)]
            response: dict[str, Any] = {"data": data}
        else:
            response = {"body": {"embeddings": vectors}}
        meta = Meta(tokens_in=len(texts))
        return Reply(request_id=f"id{len(self.ids)}", ok=True, response=response, meta=meta)


@pytest.fixture(autouse=True)
def _plain_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fake's Bedrock body is already decoded."""
    monkeypatch.setattr(embed, "decode_invoke_body", lambda response: response["body"])


@pytest.mark.parametrize("texts", ["hello world", b"abc", ["a", None]])
async def test_r3je19_texts_must_be_a_list_of_strings(texts: Any) -> None:
    """R3JE-19: one `str` was sent letter by letter; a `None` raised a bare AttributeError."""
    engine: Any = FakeEngine()
    with pytest.raises(CallFailed) as info:
        await embed.embed_partial(engine, "e", texts)
    assert info.value.kind == "invalid_request"
    assert engine.requests == []


async def test_r3je21_q22_cohere_body_from_family_or_any_case() -> None:
    """R3JE-21: an ARN or a mixed-case id got the Titan body; `family` names the shape (Q22)."""
    arn = "arn:aws:bedrock:us-east-1:1:application-inference-profile/x"
    for model_id, family in ((arn, "cohere"), ("Cohere.Embed-English-v3", None)):
        engine: Any = FakeEngine(provider="bedrock", model_id=model_id, family=family, batch=96)
        await embed.embed(engine, "e", ["a", "b"])
        assert engine.requests == [{"body": {"texts": ["a", "b"]}}]


async def test_r3je24_unknown_cohere_input_type_fails_first() -> None:
    """R3JE-24: a typo in `input_type` cost one refused provider request per batch."""
    engine: Any = FakeEngine(provider="bedrock", model_id="cohere.embed-english-v3")
    with pytest.raises(CallFailed, match="input_type"):
        await embed.embed(engine, "e", ["a"], input_type="serch_query")
    assert engine.requests == []


def test_r3je22_embedding_types_float_only() -> None:
    """R3JE-22: `["int8"]` was accepted, then every paid call failed as unreadable."""
    check = EMBED_INFERENCE["embedding_types"]
    assert check(["float"]) is None
    assert check(["int8"]) is not None
    assert check(["float", "int8"]) is not None


def test_r3je22_r3pf10_vectors_are_numbers_or_base64() -> None:
    """R3JE-22: a base64 string became a list of characters. R3PF-10: base64 is decoded."""
    packed = base64.b64encode(struct.pack("<3f", 0.5, -1.0, 2.0)).decode()
    assert embed.vectors_of("openai", {"data": [{"index": 0, "embedding": packed}]}) == [
        [0.5, -1.0, 2.0]
    ]
    for bad in ("AAA", ["a", "b"], []):
        with pytest.raises((ValueError, TypeError)):
            embed.vectors_of("openai", {"data": [{"index": 0, "embedding": bad}]})
    assert embed.request_for("openai", "m", ["a"])["encoding_format"] == "base64"


def test_openai_indices_must_be_complete() -> None:
    """A missing or repeated `index` put a vector on the wrong text with no error."""
    data = [{"index": 0, "embedding": [1.0]}, {"index": 0, "embedding": [2.0]}]
    with pytest.raises(ValueError, match="indices"):
        embed.vectors_of("openai", {"data": data})


async def test_r3je23_mixed_lengths_fail() -> None:
    """R3JE-23: vectors of different lengths in one call were returned as they came."""
    engine: Any = FakeEngine(dims={"b": 3})
    with pytest.raises(CallFailed) as info:
        await embed.embed(engine, "e", ["a", "b"])
    assert info.value.kind == "provider_error"


async def test_q19_a_refused_batch_is_split_to_the_bad_text() -> None:
    """R3JE-17 / Q19: one bad text failed its whole batch, and the caller could not tell which."""
    texts = [f"t{i}" for i in range(8)]
    engine: Any = FakeEngine(batch=8, bad=frozenset({"t5"}))
    outcome = await embed.embed_detailed(engine, "e", texts, request_id="r")
    assert list(outcome.errors) == [5]
    assert "text 5" in str(outcome.errors[5])
    assert outcome.vectors[5] == []
    assert all(outcome.vectors[i] for i in range(8) if i != 5)
    assert len(set(engine.ids)) == len(engine.ids)  # every split has its own request id
    with pytest.raises(CallFailed, match="text 5"):
        await embed.embed(engine, "e", texts)


async def test_q20_over_long_text_is_refused_first() -> None:
    """Q20: a text over the model's limit failed its batch at the provider."""
    engine: Any = FakeEngine(max_chars=3)
    with pytest.raises(CallFailed, match="text 1 has 4 characters"):
        await embed.embed(engine, "e", ["abc", "abcd"])
    assert engine.requests == []
    titan: Any = FakeEngine(provider="bedrock", model_id="amazon.titan-embed-text-v2:0")
    with pytest.raises(CallFailed, match="above 50000"):
        await embed.embed(titan, "e", ["x" * 50_001])


async def test_q21_usage_is_reported() -> None:
    """R3JE-18 / Q21: the result had no tokens, batch count or request ids."""
    engine: Any = FakeEngine(batch=2)
    outcome = await embed.embed_detailed(engine, "e", ["a", "b", "c"])
    assert (outcome.tokens_in, outcome.batches) == (3, 2)
    assert outcome.request_ids == ["id1", "id2"]


def _registry(family: object, *, kind: str = "embedding") -> dict[str, Any]:
    deployments = {"main": {"confirmed": ["us-east-1"]}}
    model = {"type": kind, "provider": "bedrock", "model_id": "arn:x", "family": family}
    return {
        "registry_version": 1,
        "accounts": {"main": {"kind": "aws"}},
        "models": {"e": {**model, "deployments": deployments}},
    }


def test_q22_family_field_is_checked() -> None:
    """Q22: a Bedrock embedding model behind an ARN had no way to name its body shape."""
    from llm_queue.registry.schema import build

    registry, errors, _ = build(_registry("cohere"))
    assert errors == []
    assert registry is not None
    assert registry.models["e"].family == "cohere"
    assert any("family" in e for e in build(_registry("nova"))[1])
    assert any("family" in e for e in build(_registry("cohere", kind="chat"))[1])
