"""Round 3 observability: status reports and alerts (R3OB-2, -6, -17).

Each test names its finding and what used to happen. Reports are built by hand, or read through
small fakes of the store client and the queues, so nothing needs the Docker stack.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest

from llm_queue.engine.keys import Keys
from llm_queue.engine.status import AlertEvaluator, StatusReader
from llm_queue.registry.store import RegistryStore

FULL = Path(__file__).parents[1] / "registry" / "fixtures" / "full.json"


class _Pipe:
    """A pipeline whose `SET ... GET` returns the previous value from the fake's dict."""

    def __init__(self, data: dict[str, str]) -> None:
        self.data = data
        self.out: list[Any] = []

    async def __aenter__(self) -> _Pipe:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    def set(self, key: str, value: str, ex: int, get: bool) -> None:
        self.out.append(self.data.get(key))
        self.data[key] = value

    async def execute(self) -> list[Any]:
        return self.out


class _Client:
    """Just enough of a store client for `StatusReader` and `AlertEvaluator`."""

    def __init__(self, status_reply: Any = None, first_ids: dict[str, bytes] | None = None):
        self.data: dict[str, str] = {}
        self.status_reply = status_reply
        self.first_ids = first_ids or {}

    def register_script(self, _source: str) -> Any:
        async def run(keys: list[Any], args: list[Any]) -> Any:
            return self.status_reply

        return run

    def pipeline(self, transaction: bool = False) -> _Pipe:
        return _Pipe(self.data)

    async def xrange(self, key: str, count: int) -> list[tuple[bytes, dict[bytes, bytes]]]:
        first = self.first_ids.get(key)
        return [(first, {})] if first else []


class _Queues:
    """`StreamQueues.depth` with fixed numbers per queue name."""

    def __init__(self, client: _Client, depths: dict[str, tuple[int, int, int]]) -> None:
        self.client = client
        self.keys = Keys("t")
        self.depths = depths

    async def depth(self, name: str) -> tuple[int, int, int]:
        return self.depths[name]


def _view() -> Any:
    store = RegistryStore(_Client(), "t", config=json.loads(FULL.read_text("utf-8")))  # type: ignore[arg-type]  # fake
    return store.run_on_config()


def _dep(name: str, **over: Any) -> dict[str, Any]:
    dep = {
        "name": name,
        "enabled": True,
        "account_enabled": True,
        "blocked": False,
        "healthy": True,
        "outside_residency": False,
        "breaker": "closed",
        "rpm": {"limit": 100, "used": 0, "left": 100},
        "tpm": {"limit": None, "used": 0, "left": None},
        "throttles_1m": 0,
    }
    dep.update(over)
    return dep


def _report(*deps: dict[str, Any], kill_switch: bool = False, oldest: float | None = None) -> Any:
    model = {
        "model": "gpt20b",
        "dead_letters": 0,
        "oldest_waiting_secs": oldest,
        "deployments": list(deps),
    }
    return {"kill_switch": kill_switch, "models": [model]}


def _on(alerts: list[Any], name: str) -> bool:
    return any(a.on for a in alerts if a.name == name)


async def test_held_calls_are_reported_as_waiting_for_a_grant() -> None:
    """R3OB-2: calls a worker held for budget were in no number (waiting = XLEN - held)."""
    view = _view()
    deps = view.deployments("gpt20b")
    now = 1_000_000
    reply = [str(now).encode(), 0, [], *([[], b"", 0, b"closed", b"0", b"0"] for _ in deps)]
    client = _Client(reply)
    name = view.queue_name("gpt20b")
    keys = Keys("t")
    client.first_ids = {keys.queue(name, "s"): f"{(now - 90) * 1000}-0".encode()}
    queues = _Queues(client, {f"{name}-priority": (0, 0, 0), name: (10, 9, 1)})
    reader = StatusReader(client, keys, queues)  # type: ignore[arg-type]  # fakes
    model = await reader.model(view, "gpt20b")
    assert model["waiting"] == {"priority": 0, "normal": 1}
    assert model["waiting_for_grant"] == {"priority": 0, "normal": 9}
    assert model["oldest_waiting_secs"] == 90.0


async def test_throttles_read_the_last_full_minute_too() -> None:
    """R3OB-6: a sample at second 3 of the minute read near 0 throttles; now max(last, this)."""
    view = _view()
    deps = view.deployments("gpt20b")
    reply = [b"600", 0, [], *([[], b"", 0, b"closed", b"1", b"40"] for _ in deps)]
    client = _Client(reply)
    name = view.queue_name("gpt20b")
    queues = _Queues(client, {f"{name}-priority": (0, 0, 0), name: (0, 0, 0)})
    model = await StatusReader(client, Keys("t"), queues).model(view, "gpt20b")  # type: ignore[arg-type]  # fakes
    dep = model["deployments"][0]
    assert (dep["throttles_1m"], dep["throttles_this_minute"]) == (40, 1)


def test_quota_alert_does_not_flip_at_the_minute_rollover() -> None:
    """R3OB-6: at steady 95 % use, a sample just after the rollover turned the alert off."""
    view = _view()
    evaluator = AlertEvaluator(_Client(), Keys("t"))  # type: ignore[arg-type]  # fake
    busy = {"limit": 100, "used": 95, "left": 5}
    fresh = {"limit": 100, "used": 2, "left": 98}
    states = []
    for rpm in (busy, fresh, busy, fresh, fresh):
        alerts = evaluator.evaluate(view, _report(_dep("d", rpm=rpm)))
        states.append(_on(alerts, "quota_near_limit"))
    assert states == [True, True, True, True, False]  # off after two clear samples


def test_missing_alerts_are_evaluated() -> None:
    """R3OB-17: no breaker_open, kill_switch_on or queue_stalled alert existed."""
    view = _view()
    evaluator = AlertEvaluator(_Client(), Keys("t"))  # type: ignore[arg-type]  # fake
    alerts = evaluator.evaluate(
        view, _report(_dep("d", breaker="open"), kill_switch=True, oldest=900.0)
    )
    assert _on(alerts, "breaker_open")
    assert _on(alerts, "kill_switch_on")
    assert _on(alerts, "queue_stalled")  # default queue_age_secs 600
    calm = evaluator.evaluate(view, _report(_dep("d"), oldest=30.0))
    assert not any(_on(calm, n) for n in ("breaker_open", "kill_switch_on", "queue_stalled"))


async def test_an_alert_that_left_the_report_logs_off(caplog: pytest.LogCaptureFixture) -> None:
    """R3OB-17: a removed deployment's alert never logged "off"; its marker just expired."""
    view = _view()
    evaluator = AlertEvaluator(_Client(), Keys("t"))  # type: ignore[arg-type]  # fake
    await evaluator.changed(evaluator.evaluate(view, _report(_dep("gone", healthy=False))))
    with caplog.at_level(logging.INFO):
        flipped = await evaluator.changed(evaluator.evaluate(view, _report(_dep("other"))))
    off = [a for a in flipped if a.name == "deployment_disabled" and not a.on]
    assert [a.labels["deployment"] for a in off] == ["gone"]
