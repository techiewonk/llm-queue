"""Request and reply conversion between Bedrock Converse and OpenAI chat (owner Q40).

A model's fallback may be a model of the other provider: its request reached that provider in
the caller's shape (refused, or misread) and the caller got the other shape back. These pin
each part both ways, the round trip, and what is refused instead of being sent half-converted.
"""

from __future__ import annotations

import base64
from typing import Any

import pytest

from llm_queue.engine import shapes

PNG = b"\x89PNG\r\n\x1a\nfake"
PNG_URL = "data:image/png;base64," + base64.b64encode(PNG).decode()
TOOL_SPEC: dict[str, Any] = {
    "name": "weather",
    "description": "the weather in a city",
    "inputSchema": {"json": {"type": "object", "properties": {"city": {"type": "string"}}}},
}
CONVERSE: dict[str, Any] = {
    "system": [{"text": "be brief"}, {"cachePoint": {"type": "default"}}],
    "messages": [
        {
            "role": "user",
            "content": [
                {"text": "what is this, and the weather?"},
                {"image": {"format": "png", "source": {"bytes": PNG}}},
            ],
        },
        {
            "role": "assistant",
            "content": [
                {"reasoningContent": {"reasoningText": {"text": "think"}}},
                {"text": "a logo; checking"},
                {"toolUse": {"toolUseId": "t1", "name": "weather", "input": {"city": "Pune"}}},
            ],
        },
        {
            "role": "user",
            "content": [
                {"toolResult": {"toolUseId": "t1", "content": [{"json": {"temp": 31}}]}},
                {"text": "and tomorrow?"},
            ],
        },
    ],
    "inferenceConfig": {"maxTokens": 300, "temperature": 0.2, "topP": 0.9, "stopSequences": ["X"]},
    "toolConfig": {"tools": [{"toolSpec": TOOL_SPEC}], "toolChoice": {"any": {}}},
    "additionalModelRequestFields": {"top_k": 5},
}
CHAT: dict[str, Any] = {
    "messages": [
        {"role": "system", "content": "be brief"},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "what is this, and the weather?"},
                {"type": "image_url", "image_url": {"url": PNG_URL}},
            ],
        },
        {
            "role": "assistant",
            "content": "a logo; checking",
            "tool_calls": [
                {
                    "id": "t1",
                    "type": "function",
                    "function": {"name": "weather", "arguments": '{"city":"Pune"}'},
                }
            ],
        },
        {"role": "tool", "tool_call_id": "t1", "content": '{"temp":31}'},
        {"role": "user", "content": "and tomorrow?"},
    ],
    "max_tokens": 300,
    "temperature": 0.2,
    "top_p": 0.9,
    "stop": ["X"],
    "tools": [
        {
            "type": "function",
            "function": {
                "name": "weather",
                "description": "the weather in a city",
                "parameters": TOOL_SPEC["inputSchema"]["json"],
            },
        }
    ],
    "tool_choice": "required",
}


def test_converse_request_to_chat() -> None:
    """Q40: system, text, inline image, tool use and result, tools and the inference config."""
    out = shapes.request(CONVERSE, "converse", "chat")
    assert out == CHAT  # reasoning, cache points and model-specific fields are dropped


def test_chat_request_to_converse() -> None:
    """Q40: the other way; the tool result and the next user words join one user turn."""
    out = shapes.request(CHAT, "chat", "converse")
    assert out["system"] == [{"text": "be brief"}]
    assert out["inferenceConfig"] == {
        "maxTokens": 300,
        "temperature": 0.2,
        "topP": 0.9,
        "stopSequences": ["X"],
    }
    assert out["toolConfig"] == {"tools": [{"toolSpec": TOOL_SPEC}], "toolChoice": {"any": {}}}
    user, assistant, results = out["messages"]
    assert user == {
        "role": "user",
        "content": [
            {"text": "what is this, and the weather?"},
            {"image": {"format": "png", "source": {"bytes": PNG}}},
        ],
    }
    assert assistant["content"] == [
        {"text": "a logo; checking"},
        {"toolUse": {"toolUseId": "t1", "name": "weather", "input": {"city": "Pune"}}},
    ]
    assert results == {
        "role": "user",
        "content": [
            {"toolResult": {"toolUseId": "t1", "content": [{"text": '{"temp":31}'}]}},
            {"text": "and tomorrow?"},
        ],
    }


def test_round_trip_keeps_what_both_shapes_have() -> None:
    """Chat -> Converse -> chat gives the same request back: nothing both shapes carry is lost."""
    back = shapes.request(shapes.request(CHAT, "chat", "converse"), "converse", "chat")
    assert back == CHAT


@pytest.mark.parametrize(
    ("choice", "converse_choice"),
    [
        ("auto", {"auto": {}}),
        ({"type": "function", "function": {"name": "weather"}}, {"tool": {"name": "weather"}}),
    ],
)
def test_tool_choices(choice: Any, converse_choice: dict[str, Any]) -> None:
    """Each tool choice maps both ways; OpenAI `none` has no counterpart and is left out."""
    request = {**CHAT, "tool_choice": choice}
    converted = shapes.request(request, "chat", "converse")
    assert converted["toolConfig"]["toolChoice"] == converse_choice
    assert shapes.request(converted, "converse", "chat")["tool_choice"] == choice
    none = shapes.request({**CHAT, "tool_choice": "none"}, "chat", "converse")
    assert "toolChoice" not in none["toolConfig"]


def test_max_completion_tokens_and_a_string_stop() -> None:
    """The newer OpenAI name wins over `max_tokens`; a single stop becomes a list."""
    request = {
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 10,
        "max_completion_tokens": 20,
        "stop": "END",
    }
    config = shapes.request(request, "chat", "converse")["inferenceConfig"]
    assert config == {"maxTokens": 20, "stopSequences": ["END"]}


def test_an_error_tool_result_says_so() -> None:
    """OpenAI tool messages have no status: a Converse error result says so in its text."""
    request = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "toolResult": {
                            "toolUseId": "t1",
                            "content": [{"text": "no such city"}],
                            "status": "error",
                        }
                    }
                ],
            }
        ]
    }
    [tool] = shapes.request(request, "converse", "chat")["messages"]
    assert tool == {"role": "tool", "tool_call_id": "t1", "content": "error: no such city"}


@pytest.mark.parametrize(
    ("request_", "source", "fragment"),
    [
        (
            {"messages": [{"role": "user", "content": [{"document": {"name": "d"}}]}]},
            "converse",
            "`document` block",
        ),
        (
            {"messages": [{"role": "user", "content": [{"video": {}}]}]},
            "converse",
            "`video` block",
        ),
        (
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [{"image": {"format": "png", "source": {"s3Location": {}}}}],
                    }
                ]
            },
            "converse",
            "inline png",
        ),
        (
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {"url": "https://example.com/a.png"},
                            }
                        ],
                    }
                ]
            },
            "chat",
            "data URL",
        ),
        (
            {"messages": [{"role": "user", "content": [{"type": "input_audio"}]}]},
            "chat",
            "`input_audio` part",
        ),
        (
            {
                "messages": [
                    {
                        "role": "assistant",
                        "tool_calls": [{"id": "1", "function": {"name": "f", "arguments": "{x"}}],
                    }
                ]
            },
            "chat",
            "not JSON",
        ),
    ],
)
def test_parts_with_no_counterpart_are_refused(
    request_: dict[str, Any], source: str, fragment: str
) -> None:
    """A part the other shape cannot carry is named, so the runtime skips that target instead
    of sending a request with the part silently missing.
    """
    target = "chat" if source == "converse" else "converse"
    reason = shapes.unconvertible(request_, source, target)
    assert reason is not None
    assert fragment in reason
    with pytest.raises(shapes.ShapeError):
        shapes.request(request_, source, target)


def test_same_shape_and_other_ops_are_left_alone() -> None:
    """No conversion within one shape, nor for embeddings or InvokeModel."""
    assert not shapes.converts("converse", "converse")
    assert not shapes.converts("embeddings", "invoke_model")
    assert shapes.unconvertible({"messages": [{"document": {}}]}, "chat", "chat") is None
    assert shapes.request(CHAT, "chat", "chat") == CHAT


def test_converse_reply_to_chat() -> None:
    """Q40: an OpenAI caller served by a Bedrock model gets a chat completion: text, tool
    calls, finish reason and usage (cached tokens too).
    """
    response = {
        "output": {
            "message": {
                "role": "assistant",
                "content": [
                    {"reasoningContent": {"reasoningText": {"text": "hm"}}},
                    {"text": "checking"},
                    {"toolUse": {"toolUseId": "t2", "name": "weather", "input": {"city": "Goa"}}},
                ],
            }
        },
        "stopReason": "tool_use",
        "usage": {
            "inputTokens": 12,
            "outputTokens": 7,
            "totalTokens": 19,
            "cacheReadInputTokens": 4,
        },
        "metrics": {"latencyMs": 80},
    }
    out = shapes.reply(response, "chat", "converse", model="small")
    assert out["object"] == "chat.completion"
    assert out["model"] == "small"
    [choice] = out["choices"]
    assert choice["finish_reason"] == "tool_calls"
    assert choice["message"] == {
        "role": "assistant",
        "content": "checking",
        "tool_calls": [
            {
                "id": "t2",
                "type": "function",
                "function": {"name": "weather", "arguments": '{"city":"Goa"}'},
            }
        ],
    }
    assert out["usage"] == {
        "prompt_tokens": 12,
        "completion_tokens": 7,
        "total_tokens": 19,
        "prompt_tokens_details": {"cached_tokens": 4},
    }


def test_chat_reply_to_converse() -> None:
    """Q40: a Converse caller served by an OpenAI-compatible model gets a Converse reply."""
    response = {
        "id": "c1",
        "object": "chat.completion",
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "t3",
                            "type": "function",
                            "function": {"name": "weather", "arguments": '{"city":"Pune"}'},
                        }
                    ],
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 9, "completion_tokens": 3, "total_tokens": 12},
    }
    out = shapes.reply(response, "converse", "chat", model="local")
    assert out == {
        "output": {
            "message": {
                "role": "assistant",
                "content": [
                    {"toolUse": {"toolUseId": "t3", "name": "weather", "input": {"city": "Pune"}}}
                ],
            }
        },
        "stopReason": "tool_use",
        "usage": {"inputTokens": 9, "outputTokens": 3, "totalTokens": 12},
    }


@pytest.mark.parametrize(
    ("finish", "stop"),
    [("stop", "end_turn"), ("length", "max_tokens"), ("content_filter", "content_filtered")],
)
def test_stop_reasons_both_ways(finish: str, stop: str) -> None:
    """Every finish reason maps to a stop reason and back."""
    chat = {"choices": [{"message": {"content": "x"}, "finish_reason": finish}], "usage": {}}
    converse = shapes.reply(chat, "converse", "chat", model="m")
    assert converse["stopReason"] == stop
    assert converse["output"]["message"]["content"] == [{"text": "x"}]
    back = shapes.reply(converse, "chat", "converse", model="m")
    assert back["choices"][0]["finish_reason"] == finish
