"""Providers and error classes (0.1.0b2: C7, P1, P5, P7, P8, P10), with fakes and fakellm.

Each test names the old behaviour: signature errors ended the call, a connection reset charged
the breaker, unknown OpenAI params raised TypeError, a cut stream counted as a success, `n>1`
choices / reasoning / logprobs were dropped, cached tokens were counted twice.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any

import openai
import pytest
from aiohttp import ServerDisconnectedError
from botocore.exceptions import ClientError
from clients_testkit import StartFake, registry_doc
from openai.types.chat import ChatCompletionChunk

from llm_queue.clients import BedrockProvider, ClientPool, Providers, Timing, route_for
from llm_queue.clients.providers import _chat_body, _converse_body, _openai_usage
from llm_queue.errors import ErrorKind
from llm_queue.providers import (
    StreamTruncated,
    classify_bedrock,
    classify_openai,
    is_auth_error,
    is_connection_reset,
    request_error_kind,
)
from llm_queue.registry import load_registry
from llm_queue.tuning import BedrockOptions

CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
FAST = Timing(first_response_secs=5, idle_secs=5)


def _client_error(code: str, message: str, status: int = 400) -> ClientError:
    body: Any = {
        "Error": {"Code": code, "Message": message},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return ClientError(body, "Converse")


@pytest.mark.parametrize(
    ("code", "message", "status"),
    [
        ("InvalidSignatureException", "Signature expired: 20261114T000000Z is now earlier", 403),
        ("SignatureDoesNotMatch", "The request signature we calculated does not match", 403),
        ("ExpiredTokenException", "The security token included in the request is expired", 403),
        (
            "UnrecognizedClientException",
            "The security token included in the request is invalid",
            403,
        ),
    ],
)
def test_signature_and_token_errors_are_auth_errors(code: str, message: str, status: int) -> None:
    exc = _client_error(code, message, status)
    info = classify_bedrock(exc)
    assert info.kind is ErrorKind.FATAL
    assert is_auth_error(exc, info)  # was: a plain fatal error that ended the call
    assert request_error_kind(exc, info) is None


@pytest.mark.parametrize(
    ("code", "message", "kind"),
    [
        (
            "ValidationException",
            "Input is too long for requested model.",
            "context_length_exceeded",
        ),
        ("ValidationException", "too many input tokens", "context_length_exceeded"),
        ("ValidationException", "messages.0.content: field required", "invalid_request"),
        ("ResourceNotFoundException", "Model not found", None),
    ],
)
def test_request_error_kinds(code: str, message: str, kind: str | None) -> None:
    exc = _client_error(code, message, 404 if kind is None else 400)
    assert request_error_kind(exc, classify_bedrock(exc)) == kind


def test_a_bedrock_throttle_about_tokens_is_not_a_request_error() -> None:
    exc = _client_error("ThrottlingException", "Too many tokens, please wait", 429)
    info = classify_bedrock(exc)
    assert info.kind is ErrorKind.THROTTLE
    assert request_error_kind(exc, info) is None


def test_openai_stream_error_without_status_is_retryable() -> None:
    exc = openai.APIError("server overloaded", request=None, body=None)  # type: ignore[arg-type]
    assert classify_openai(exc).kind is ErrorKind.RETRYABLE  # was FATAL


def test_connection_resets_and_truncation() -> None:
    assert is_connection_reset(ServerDisconnectedError())
    assert is_connection_reset(ConnectionResetError())
    assert not is_connection_reset(TimeoutError())
    assert classify_bedrock(StreamTruncated("cut")).kind is ErrorKind.RETRYABLE


class _FlakyClient:
    """A Bedrock client whose first `converse` drops the connection."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    async def converse(self, **request: Any) -> dict[str, Any]:
        self.calls += 1
        if self.calls <= self.failures:
            raise ServerDisconnectedError()
        return {"output": {"message": {"role": "assistant", "content": []}}, "stopReason": "x"}


class _OnePool:
    def __init__(self, client: Any) -> None:
        self.client = client

    async def bedrock(self, route: Any) -> Any:
        return self.client


@pytest.mark.parametrize(("failures", "ok"), [(1, True), (2, False)])
async def test_one_quick_retry_on_a_connection_reset(failures: int, ok: bool) -> None:
    client = _FlakyClient(failures)
    provider = BedrockProvider(_OnePool(client))  # type: ignore[arg-type]
    route = SimpleNamespace(model_id="m")
    timing = Timing(5, 5, attempt_secs=5)
    if ok:
        result = await provider.call("converse", CHAT, route, timing)  # type: ignore[arg-type]
        assert result.response["stopReason"] == "x"
    else:
        with pytest.raises(ServerDisconnectedError):
            await provider.call("converse", CHAT, route, timing)  # type: ignore[arg-type]
    assert client.calls == min(failures, 1) + 1


def test_converse_stream_keeps_redacted_reasoning_and_citations() -> None:
    def delta(index: int, part: dict[str, Any]) -> dict[str, Any]:
        return {"contentBlockDelta": {"contentBlockIndex": index, "delta": part}}

    body = _converse_body(
        [
            {"messageStart": {"role": "assistant"}},
            delta(0, {"reasoningContent": {"redactedContent": b"ab"}}),
            delta(0, {"reasoningContent": {"redactedContent": b"cd"}}),
            delta(1, {"text": "Paris"}),
            delta(1, {"citation": {"title": "doc"}}),
            {"messageStop": {"stopReason": "end_turn"}},
        ]
    )
    content = body["output"]["message"]["content"]
    assert content[0] == {"reasoningContent": {"redactedContent": b"abcd"}}
    assert content[1]["citationsContent"]["citations"] == [{"title": "doc"}]
    with pytest.raises(StreamTruncated):  # was: a cut stream returned as a success
        _converse_body([{"messageStart": {"role": "assistant"}}, delta(0, {"text": "Par"})])


def _chunk(choices: list[dict[str, Any]], usage: dict[str, Any] | None = None) -> Any:
    return ChatCompletionChunk.model_validate(
        {
            "id": "c",
            "object": "chat.completion.chunk",
            "created": 1,
            "model": "m",
            "choices": choices,
            "usage": usage,
        }
    )


def test_chat_body_keeps_every_choice_reasoning_and_logprobs() -> None:
    lp = {"content": [{"token": "a", "logprob": -0.1, "bytes": None, "top_logprobs": []}]}
    chunks = [
        _chunk(
            [{"index": 0, "delta": {"content": "A", "reasoning_content": "think"}, "logprobs": lp}]
        ),
        _chunk([{"index": 1, "delta": {"content": "B"}}]),
        _chunk([{"index": 0, "delta": {}, "finish_reason": "stop"}]),
        _chunk([{"index": 1, "delta": {"refusal": "no"}, "finish_reason": "stop"}]),
    ]
    body = _chat_body(chunks)
    first, second = body["choices"]
    assert first["message"]["content"] == "A"
    assert first["message"]["reasoning_content"] == "think"
    assert first["logprobs"]["content"][0]["token"] == "a"
    assert (second["index"], second["message"]["content"]) == (1, "B")
    assert second["message"]["refusal"] == "no"
    with pytest.raises(StreamTruncated):
        _chat_body(chunks[:2])


def test_openai_cached_tokens_are_not_counted_twice() -> None:
    usage = _openai_usage(
        {
            "prompt_tokens": 100,
            "completion_tokens": 5,
            "prompt_tokens_details": {"cached_tokens": 60},
        }
    )
    assert (usage.input_tokens, usage.cache_read_tokens) == (40, 60)


@pytest.fixture
async def providers(start_fake: StartFake, transport: str) -> AsyncIterator[tuple[Any, Providers]]:
    url, _ = await start_fake()
    view = load_registry(registry_doc(url))
    built = Providers(
        ClientPool(
            bedrock_endpoint_url=url,
            options=BedrockOptions(transport=transport),
            credentials=lambda a: {"aws_access_key_id": a.name, "aws_secret_access_key": "x"},
            secrets={"LLMQ_ACCOUNT_VLLM_KEY": "key-v"}.get,
        )
    )
    yield view, built
    await built.aclose()


async def test_openai_unknown_params_go_to_extra_body(providers: Any) -> None:
    view, built = providers
    route = route_for(view, view.deployments("local")[0])
    params = {
        "messages": [{"role": "user", "content": "hi"}],
        "top_k": 5,
        "chat_template_kwargs": {"enable_thinking": False},
    }
    result = await built.get("openai").call("chat", params, route, FAST)  # was TypeError
    assert result.response["choices"][0]["finish_reason"]


async def test_plain_converse_when_an_attempt_timer_is_given(providers: Any) -> None:
    view, built = providers
    route = route_for(view, view.deployments("gpt120b")[0])
    timing = Timing(5, 5, attempt_secs=5)
    plain = await built.get("bedrock").call("converse", CHAT, route, timing)
    streamed = await built.get("bedrock").call(
        "converse", CHAT, route, timing, converse_api="stream"
    )
    assert plain.response["output"] == streamed.response["output"]
    assert "ResponseMetadata" not in plain.response
