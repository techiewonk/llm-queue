"""Size-aware backfill (plan F1, owner Q3) without a store: settings, the flag and its cost.

The admission behaviour on a real store is in tests/integration/engine/test_020_backfill_store.py.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import orjson
import pytest

from llm_queue import ConfigError
from llm_queue.engine import _admission_lua as lua
from llm_queue.engine.admission import Admission, Ask, Candidate
from llm_queue.engine.keys import Keys
from llm_queue.engine.runtime import Engine
from llm_queue.engine.settings import InfraSettings
from llm_queue.registry import check

DEV = {
    "LLMQ_STORE_URL": "redis://localhost:6379/0",
    "LLMQ_ENVIRONMENT": "dev",
    "LLMQ_REGISTRY": '{"registry_version": 1, "models": {}}',
}
FIXTURES = Path(__file__).resolve().parents[1] / "registry" / "fixtures"


class _Script:
    """A registered script that records each call and answers `granted`."""

    def __init__(self, calls: list[dict[str, Any]]) -> None:
        self._calls = calls

    async def __call__(self, keys: list[Any], args: list[Any]) -> bytes:
        self._calls.append(orjson.loads(args[1]))
        return b'{"r": "granted", "dep": "aws-a/r1", "minute": 1}'


class _Client:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def register_script(self, source: str) -> _Script:
        return _Script(self.calls)


def _ask(**kw: Any) -> Ask:
    return Ask("m", "r1", "w1", 1, "n", 1.0, [Candidate("aws-a/r1")], cost_t=100, **kw)


async def test_off_sends_the_same_single_script_call_as_before() -> None:
    client = _Client()
    admission = Admission(client, Keys("p"))  # type: ignore[arg-type]
    await admission.pick_take(_ask(), 1)
    await admission.pick_take(_ask(backfill=True), 1)
    off, on = client.calls
    assert len(client.calls) == 2  # one round trip per ask, on or off
    assert "bf" not in off
    assert on.pop("bf") is True
    on.pop("rand")
    off.pop("rand")
    assert on == off  # the flag is the only difference


def test_backfill_lua_adds_no_store_command() -> None:
    """Off, the script runs no extra store command: the backfill chunk has none at all."""
    assert "redis.call" not in lua._BACKFILL
    assert lua._BACKFILL in lua.PICK_TAKE
    # Every other use is guarded by the ask's flag or by a reservation that carries `at`.
    body = lua.PICK_TAKE.replace(lua._BACKFILL, "")
    assert body.count("BF.") == 3
    assert body.count("req.bf") == 2


def test_env_switch_defaults_off_and_is_checked() -> None:
    assert InfraSettings.from_env(DEV).queue_backfill_enabled is False
    on = InfraSettings.from_env({**DEV, "LLMQ_QUEUE_BACKFILL_ENABLED": "on"})
    assert on.queue_backfill_enabled is True
    with pytest.raises(ConfigError, match="LLMQ_QUEUE_BACKFILL_ENABLED"):
        InfraSettings.from_env({**DEV, "LLMQ_QUEUE_BACKFILL_ENABLED": "maybe"})


def _engine(env_default: bool) -> Any:
    return SimpleNamespace(settings=SimpleNamespace(queue_backfill_enabled=env_default))


def _view(queue: dict[str, Any]) -> Any:
    return SimpleNamespace(value=lambda alias, name: queue)


@pytest.mark.parametrize(
    ("registry", "env_default", "expected"),
    [
        ({}, False, False),
        ({}, True, True),
        ({"backfill": True}, False, True),
        ({"backfill": False}, True, False),
    ],
)
def test_registry_setting_wins_over_the_env_default(
    registry: dict[str, Any], env_default: bool, expected: bool
) -> None:
    got = Engine._backfill(_engine(env_default), _view(registry), "m")
    assert got is expected


def _doc(backfill: Any) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads((FIXTURES / "smallest.json").read_text(encoding="utf-8"))
    alias = next(iter(doc["models"]))
    doc["models"][alias]["queue"] = {"backfill": backfill}
    return doc


def test_registry_accepts_a_boolean_only() -> None:
    registry, errors, _ = check(_doc(True))
    assert registry is not None, errors
    registry, errors, _ = check(_doc("yes"))
    assert registry is None
    assert any("backfill" in e for e in errors)
