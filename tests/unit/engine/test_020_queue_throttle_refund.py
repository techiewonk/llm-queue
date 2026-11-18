"""0.2.0 queue mode, as local mode (gate BL1): a provider throttle keeps its request charged.

The worker's FINISH gave a throttled attempt its request back (`refund_r=1`); the provider
had counted the 429 in its minute, so the call was sent again in that minute and a `fixed`
window let the provider see more than its rpm. Only the tokens go back now; a call that never
reached the provider still gets everything back.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any

import pytest

from llm_queue.clients.providers import Result
from llm_queue.engine.metrics import NullSink
from llm_queue.engine.runtime import Engine, _Held, build_envelope
from llm_queue.engine.tokens import Estimate
from llm_queue.errors import ErrorInfo, ErrorKind
from llm_queue.registry.resolve import ResolvedRegistry
from llm_queue.registry.validate import check


class Refused(Exception):
    """What the fake provider raises; `kind` says how it classifies."""

    def __init__(self, kind: ErrorKind) -> None:
        super().__init__(str(kind))
        self.kind = kind


class Provider:
    name = "bedrock"

    def __init__(self, kind: ErrorKind) -> None:
        self.kind = kind

    async def call(self, *args: Any, **kwargs: Any) -> Result:
        raise Refused(self.kind)

    def classify(self, exc: BaseException) -> ErrorInfo:
        assert isinstance(exc, Refused)
        return ErrorInfo(kind=exc.kind, code="ThrottlingException")


def _view() -> ResolvedRegistry:
    doc = {
        "registry_version": 1,
        "accounts": {"aws-a": {"kind": "aws"}},
        "models": {
            "chat": {
                "type": "chat",
                "provider": "bedrock",
                "model_id": "openai.gpt-oss-20b-1:0",
                "rpm": 600,
                "rate_window": "fixed",
                "deployments": {"aws-a": {"confirmed": ["r1"]}},
            }
        },
    }
    registry, errors, _ = check(doc)
    assert registry is not None, errors
    return ResolvedRegistry(registry)


async def _finish_of(kind: ErrorKind) -> dict[str, Any]:
    """Run one queue-mode attempt that the provider refuses; return its FINISH arguments."""
    view = _view()
    finished: list[dict[str, Any]] = []

    async def finish(alias: str, owner: str, rid: str, **kwargs: Any) -> int:
        finished.append(kwargs)
        return 1

    messages = {"messages": [{"role": "user", "content": [{"text": "hi"}]}]}
    _, env = build_envelope(view, "chat", messages, caller="t", metrics=NullSink(), hash_id=str)
    now = time.time()
    held = _Held(None, env, now, int(now * 1000), "t")
    held.estimates[("chat", "o1")] = Estimate(200, 500)
    engine = SimpleNamespace(
        admission=SimpleNamespace(finish=finish),
        providers=SimpleNamespace(get=lambda name: Provider(kind)),
        _params=lambda *args: {},
        _deadline=lambda held: now + 60,
        clock=SimpleNamespace(now=lambda: now),
        metrics=NullSink(),
    )
    await Engine._attempt(engine, view, "chat", held, "aws-a/r1", "o1", 1)  # type: ignore[arg-type]
    assert len(finished) == 1, finished
    return finished[0]


async def test_a_throttle_keeps_its_request_charged_and_gives_the_tokens_back() -> None:
    finish = await _finish_of(ErrorKind.THROTTLE)
    assert finish["outcome"] == "throttle"
    assert (finish["refund_r"], finish["refund_t"]) == (0, 700)  # it was (1, 700)


@pytest.mark.parametrize("kind", [ErrorKind.FATAL])
async def test_a_call_refused_before_it_ran_still_gets_everything_back(kind: ErrorKind) -> None:
    finish = await _finish_of(kind)
    assert finish["outcome"] == "none"
    assert (finish["refund_r"], finish["refund_t"]) == (1, 700)
