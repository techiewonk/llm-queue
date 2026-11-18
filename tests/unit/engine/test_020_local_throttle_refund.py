"""0.2.0 local mode, gate BL1: a provider throttle must not hand a process its call back.

Four local-mode processes split rpm 600 (150 each) in front of a provider that admits 600
calls in any 60 s and counts every call it receives, a throttled one too, in its wall-clock
minute (fakellm's `by_minute`). At a minute boundary each process's new 150 meet the last
minute's calls still in the provider's window and are throttled; each throttle gave its
request back, the process spent it again in the same minute, and the provider saw 640 calls
in one minute (BL1 on a quiet machine: [112, 516, 639, 640, 40]).
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

import llm_queue.engine.local as local
from llm_queue.clients.providers import Result, Usage
from llm_queue.engine.local import LocalExecutor, LocalLimits, _Call, _Grant, _Wait
from llm_queue.engine.metrics import NullSink
from llm_queue.engine.runtime import build_envelope
from llm_queue.errors import ErrorInfo, ErrorKind
from llm_queue.registry.resolve import ResolvedRegistry
from llm_queue.registry.validate import check

RPM = 600
PROCESSES = 4
OFFERED = 1.5  # of the rpm, as BL1
SECS = 180.0
MAX_WAIT = 20.0  # BL1's max_wait_secs
THROTTLE_RETRIES = 6  # throttle_retry_attempts
START = 1_763_445_012.345  # not on a minute: the gate starts whenever it starts
TICK = 0.05


class Throttled(Exception):
    """The provider's 429."""


class Clock:
    """`time.time` and `time.monotonic` of the local module, moved by the test."""

    def __init__(self, now: float) -> None:
        self.now = now

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now


@dataclass
class Provider:
    """fakellm's rule: `rpm` calls admitted in any 60 s; every call counts in its minute."""

    clock: Clock
    process: int = 0
    window: collections.deque[float] = field(default_factory=collections.deque)
    by_minute: collections.Counter[int] = field(default_factory=collections.Counter)
    sent: collections.Counter[tuple[int, int]] = field(default_factory=collections.Counter)
    throttled: int = 0

    async def call(self, *args: Any, **kwargs: Any) -> Result:
        now = self.clock.now
        self.by_minute[int(now // 60)] += 1
        self.sent[(self.process, int(now // 60))] += 1
        while self.window and now - self.window[0] >= 60.0:
            self.window.popleft()
        if len(self.window) >= RPM:
            self.throttled += 1
            raise Throttled("ThrottlingException")
        self.window.append(now)
        reply = {"output": {"message": {"role": "assistant", "content": [{"text": "ok"}]}}}
        return Result(response=reply, usage=Usage(input_tokens=5, output_tokens=2))

    def classify(self, exc: BaseException) -> ErrorInfo:
        assert isinstance(exc, Throttled), exc
        return ErrorInfo(kind=ErrorKind.THROTTLE, code="ThrottlingException")


def _view(window: str) -> ResolvedRegistry:
    doc = {
        "registry_version": 1,
        "accounts": {"aws-a": {"kind": "aws"}},
        "models": {
            "chat": {
                "type": "chat",
                "provider": "bedrock",
                "model_id": "openai.gpt-oss-20b-1:0",
                "rpm": RPM,
                "rate_window": window,
                "deployments": {"aws-a": {"confirmed": ["r1"]}},
            }
        },
    }
    registry, errors, _ = check(doc)
    assert registry is not None, errors
    return ResolvedRegistry(registry)


@dataclass
class _Pending:
    call: _Call
    throttles: int = 0


async def _bl1(window: str, monkeypatch: pytest.MonkeyPatch) -> Provider:
    """BL1 on a fake clock: 4 processes, 150 % of the rpm offered for 3 minutes."""
    clock = Clock(START)
    monkeypatch.setattr(local, "time", clock)
    view = _view(window)
    provider = Provider(clock)
    providers = SimpleNamespace(get=lambda name: provider)
    executors = [
        LocalExecutor(
            providers,  # type: ignore[arg-type]
            LocalLimits(process_count=PROCESSES),
            view=lambda: view,
            params=lambda *args: {},
        )
        for _ in range(PROCESSES)
    ]
    pending: list[collections.deque[_Pending]] = [collections.deque() for _ in executors]
    retry_at = [0.0] * PROCESSES
    every = PROCESSES * 60 / (OFFERED * RPM)  # one call per process every 0.27 s
    arrived = [0] * PROCESSES
    messages = {"messages": [{"role": "user", "content": [{"text": "hi"}]}]}
    for step in range(int(SECS / TICK)):
        clock.now = START + step * TICK
        for k, executor in enumerate(executors):
            provider.process = k
            queue = pending[k]
            while arrived[k] * every <= step * TICK:
                arrived[k] += 1
                _, env = build_envelope(
                    view, "chat", messages, caller="t", metrics=NullSink(), hash_id=str
                )
                queue.append(_Pending(_Call(env, "local", clock.now, clock.now + 1e9)))
            while queue and clock.now - queue[0].call.start > MAX_WAIT:
                queue.popleft()  # rate_limited at max_wait_secs
            while queue and clock.now >= retry_at[k]:
                head = queue[0]
                pick = executor._pick(view, "chat", head.call, frozenset())
                if isinstance(pick, _Wait):
                    retry_at[k] = clock.now + max(0.01, min(pick.secs, 1.0))
                    break
                assert isinstance(pick, _Grant)
                result = await executor._attempt(view, pick, head.call)
                if result.outcome == "ok":
                    queue.popleft()
                    continue
                assert result.outcome == "throttle", result
                head.throttles += 1
                if head.throttles > THROTTLE_RETRIES:  # rate_limited
                    queue.popleft()
    return provider


@pytest.mark.parametrize("window", ["fixed", "smooth"])
async def test_a_throttled_attempt_is_not_spent_again_in_the_same_minute(
    window: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each process sends at most its 150 a wall-clock minute (+ its burst), all 600 (+2).

    It was 150 plus every throttle given back and spent again: 640 at the provider (`fixed`).
    """
    provider = await _bl1(window, monkeypatch)
    assert provider.throttled > 0  # the boundary throttles happen, as in BL1
    share = RPM // PROCESSES
    burst = 0 if window == "fixed" else share // 10  # smooth: one burst (rate_burst 0.1)
    assert max(provider.sent.values()) <= share + burst, sorted(provider.sent.items())
    if window == "fixed":
        assert max(provider.by_minute.values()) <= RPM + 2, sorted(provider.by_minute.items())
    assert sum(provider.by_minute.values()) > 0.9 * RPM * SECS / 60  # calls were served
