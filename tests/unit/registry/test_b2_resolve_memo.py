"""Effective settings are resolved once per registry view (plan B5, 108)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from llm_queue.registry import load_registry, resolve

FIXTURES = Path(__file__).parent / "fixtures"


def _full() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((FIXTURES / "full.json").read_text(encoding="utf-8"))
    return data


def test_a_view_resolves_each_setting_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """The hot path walked the setting levels again for every one of ~40 reads per call."""
    view = load_registry(_full())
    calls = 0
    real = resolve.ResolvedRegistry._resolve

    def counted(self: Any, *args: Any) -> Any:
        nonlocal calls
        calls += 1
        return real(self, *args)

    monkeypatch.setattr(resolve.ResolvedRegistry, "_resolve", counted)
    first = view.value("gpt120b", "queue")
    assert view.value("gpt120b", "queue") is first
    assert view.effective("gpt120b", "queue").value is first
    assert calls == 1
    dep = view.deployments("gpt120b")[0].key
    view.value("gpt120b", "rpm", dep)
    view.value("gpt120b", "rpm", dep)
    assert calls == 2  # one more: another deployment level is another entry


def test_a_new_view_starts_fresh() -> None:
    """A reload builds a new view: its values come from the new document, never the memo."""
    doc = _full()
    old = load_registry(doc)
    assert old.value("gpt120b", "timeout_secs") != 77
    doc["models"]["gpt120b"]["timeout_secs"] = 77
    assert load_registry(doc).value("gpt120b", "timeout_secs") == 77
    assert old.value("gpt120b", "timeout_secs") != 77


def test_an_unknown_deployment_still_raises_every_time() -> None:
    view = load_registry(_full())
    for _ in range(2):
        with pytest.raises(KeyError):
            view.value("gpt120b", "rpm", "nope/none")
