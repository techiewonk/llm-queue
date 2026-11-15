"""Public API typing fixes (R3AP-3, R3AP-4, owner Q38), without a store.

A TypedDict that leaves out keys the code returns makes a type checker refuse correct user
code; these compare each declared shape with what the code really builds.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, get_args, get_type_hints
from unittest.mock import MagicMock

import llm_queue
from llm_queue.core import ConcurrencyStatus, DeploymentStatus, RequestStatus
from llm_queue.engine.errors import KINDS, CallFailed, Kind
from llm_queue.engine.status import StatusReader
from llm_queue.registry import load_registry

FIXTURE = Path(__file__).parent / "registry" / "fixtures" / "smallest.json"


class _Queue:
    """The five methods `QueueClient` needs (never called here)."""

    async def converse(self, *_: Any, **__: Any) -> Any: ...

    def converse_sync(self, *_: Any, **__: Any) -> Any: ...

    async def chat(self, *_: Any, **__: Any) -> Any: ...

    async def embed(self, *_: Any, **__: Any) -> Any: ...

    def embed_sync(self, *_: Any, **__: Any) -> Any: ...


def test_r3ap3_deployment_status_declares_every_key() -> None:
    """R3AP-3: `account_enabled` and `blocked` were returned but not in `DeploymentStatus`."""
    view = load_registry(json.loads(FIXTURE.read_text(encoding="utf-8")))
    reader = StatusReader(MagicMock(), MagicMock(), MagicMock(), "")
    dep = view.deployments("gpt20b")[0]
    row = reader._deployment(view, "gpt20b", dep, [[], b"", 0, b"closed", 0], {}, 0)
    assert set(row) == set(get_type_hints(DeploymentStatus))
    assert set(row["concurrency"]) == set(get_type_hints(ConcurrencyStatus))
    for name in ("DeploymentStatus", "RateStatus", "ConcurrencyStatus", "Kind"):
        assert name in llm_queue.__all__
        assert getattr(llm_queue, name) is not None


def test_r3ap4_request_status_declares_the_state_times() -> None:
    """R3AP-4: `rs["done_at"]` was a type error although the record carries it."""
    keys = set(get_type_hints(RequestStatus))
    for state in ("in_progress", "done", "failed", "expired", "cancelled"):
        assert f"{state}_at" in keys


def test_q38_kind_is_a_literal_of_every_kind() -> None:
    """Q38: `CallFailed.kind` was a bare `str`; `KINDS` is now built from the `Kind` Literal."""
    assert frozenset(get_args(Kind)) == KINDS
    assert "request_too_large" in KINDS
    assert CallFailed("rejected", "x").kind == "rejected"


def test_q36_chat_stream_has_its_own_name_and_docs() -> None:
    """Q36 / Q37 / R3AP-15: `chat_stream` was an alias, so help() named it converse_stream."""
    from llm_queue.core import LLMQueue

    assert LLMQueue.chat_stream.__name__ == "chat_stream"
    for name in ("call", "converse", "chat", "chat_stream"):
        assert getattr(LLMQueue, name).__doc__
    assert "allow_fallback" in (LLMQueue.call.__doc__ or "")  # R3AP-10: options documented


def test_r3ap13_queued_chat_openai_stays_off_the_responses_api() -> None:
    """R3AP-13: the Responses API posted to /responses, which the queue answered with 404."""
    import pytest

    pytest.importorskip("langchain_openai")
    from pydantic import ValidationError

    from llm_queue.langchain.openai import QueuedChatOpenAI

    chat = QueuedChatOpenAI(model="m", llmq=_Queue())
    if "use_responses_api" in type(chat).model_fields:
        assert chat.use_responses_api is False
        with pytest.raises(ValidationError, match="use_responses_api"):
            QueuedChatOpenAI(model="m", llmq=_Queue(), use_responses_api=True)


def test_r3ap14_adapters_check_llmq_and_allow_fallback() -> None:
    """R3AP-14: a wrong `llmq` or `allow_fallback` was found only at the first call."""
    import pytest

    pytest.importorskip("langchain_openai")
    from pydantic import ValidationError

    from llm_queue.langchain.openai import QueuedChatOpenAI, QueuedOpenAIEmbeddings

    with pytest.raises(ValidationError, match="llmq must be an LLMQueue"):
        QueuedChatOpenAI(model="m", llmq=object())
    with pytest.raises(ValidationError, match="llmq must be an LLMQueue"):
        QueuedOpenAIEmbeddings(model="m", llmq=object())
    with pytest.raises(ValidationError, match="allow_fallback"):
        QueuedChatOpenAI(model="m", llmq=_Queue(), allow_fallback="everywhere")
