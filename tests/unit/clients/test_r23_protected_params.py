"""Caller params on OpenAI-compatible routes may not touch protected fields (R3SE-1, D2).

The SDK merges `extra_body` over the JSON body and `extra_headers` over the headers, so a
caller replaced the route's model, undid the `inference_limits` cap and sent its own
`Authorization`. Such a call now fails as `invalid_request` before anything is sent.
"""

from __future__ import annotations

from typing import Any

import pytest

from llm_queue.clients import ClientPool, Providers, Timing
from llm_queue.clients import providers as providers_module
from llm_queue.providers import classify_openai, request_error_kind

FAST = Timing(5, 5)

HARMLESS = {"max_tokens": 5, "extra_body": {"top_k": 5}, "extra_headers": {"X-Trace": "1"}}
REFUSED: list[dict[str, Any]] = [
    {"extra_body": {"model": "other"}},
    {"extra_body": {"max_tokens": 999_999}},
    {"extra_body": {"max_completion_tokens": 999_999}},
    {"extra_body": {"stream": False}},
    {"extra_body": {"n": 8}},
    {"extra_headers": {"Authorization": "Bearer other"}},
    {"extra_headers": {"api-key": "k"}},
    {"store": True},
    {"api_key": "k"},
    {"extra_body": "not an object"},
]


@pytest.mark.parametrize("params", REFUSED)
def test_caller_params_cannot_touch_protected_fields(params: dict[str, Any]) -> None:
    """R3SE-1 (owner decision D2): `extra_body` replaced the model and the token cap.

    `extra_headers` replaced `Authorization`. Such params fail as `invalid_request` now.
    """
    providers_module.check_params(HARMLESS)  # harmless keys pass
    with pytest.raises(providers_module.InvalidRequestParams) as info:
        providers_module.check_params(params)
    assert request_error_kind(info.value, classify_openai(info.value)) == "invalid_request"


def test_top_level_model_and_stream_pass_because_they_are_overridden() -> None:
    """LangChain's OpenAI adapter sends `model` and `stream` on every call; the provider sets
    both after the caller's params, so they are harmless and must not fail the call."""
    providers_module.check_params({"model": "other", "stream": False, "stream_options": {}})


async def test_openai_provider_refuses_protected_params_before_sending() -> None:
    """R3SE-1: chat and embeddings refuse before any client is built (no route is read)."""
    provider = Providers(ClientPool()).get("openai")
    route: Any = None
    with pytest.raises(providers_module.InvalidRequestParams):
        await provider.call("chat", {"extra_body": {"model": "x"}}, route, FAST)
    with pytest.raises(providers_module.InvalidRequestParams):
        await provider.call("embeddings", {"extra_headers": {"authorization": "x"}}, route, FAST)
