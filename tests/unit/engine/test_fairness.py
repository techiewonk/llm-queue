"""Per-tenant fairness without a store: the setting, its checks, and the ask it builds.

`fairness` is off by default (the ask then carries nothing); the tenant is the tenant id, else
the user id; only the keyed hash leaves `fairness_for`; the registry refuses a quota with no
cap and a mode it does not know, and warns about settings the mode never reads.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from llm_queue.engine.admission import Ask, Candidate
from llm_queue.engine.fairness import NO_TENANT, Fairness, fairness_for, tenant_of
from llm_queue.registry import check, load_registry

FIXTURES = Path(__file__).parents[1] / "registry" / "fixtures"


def _hash(value: str) -> str:
    return f"h({value})"


@pytest.fixture
def small() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((FIXTURES / "smallest.json").read_text(encoding="utf-8"))
    return data


def _with(small: dict[str, Any], fairness: dict[str, Any], **model: Any) -> dict[str, Any]:
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["fairness"] = fairness
    doc["models"]["gpt20b"].update(model)
    return doc


def test_off_by_default(small: dict[str, Any]) -> None:
    """No setting: mode off, and the ask carries no fairness at all."""
    view = load_registry(small)
    setting = view.value("gpt20b", "fairness")
    assert setting["mode"] == "off"
    assert fairness_for(setting, "acme", "u1", _hash) is None


def test_tenant_then_user_then_shared() -> None:
    assert tenant_of("acme", "u1") == "acme"
    assert tenant_of(None, "u1") == "u1"
    assert tenant_of(None, None) == NO_TENANT


def test_share_weights_and_default() -> None:
    setting = {"mode": "share", "weights": {"acme": 3}, "default_weight": 2, "active_secs": 30}
    heavy = fairness_for(setting, "acme", None, _hash)
    other = fairness_for(setting, "globex", None, _hash)
    assert heavy is not None
    assert other is not None
    assert (heavy.weight, other.weight) == (3.0, 2.0)
    assert heavy.active_ms == 30_000


def test_quota_per_tenant_override() -> None:
    setting = {"mode": "quota", "rpm": 10, "tpm": 1000, "quotas": {"acme": {"rpm": 50}}}
    acme = fairness_for(setting, "acme", None, _hash, window="fixed")
    other = fairness_for(setting, None, "u1", _hash)
    assert acme is not None
    assert other is not None
    assert (acme.rpm, acme.tpm, acme.window) == (50, 1000, "fixed")
    assert (other.rpm, other.tpm) == (10, 1000)


def test_body_has_only_the_hash() -> None:
    """The script sees the keyed hash of the tenant, never the raw id."""
    fair = fairness_for({"mode": "fair_queue"}, "acme-raw", None, _hash, round_=7.0)
    assert fair is not None
    body = fair.body()
    assert body["h"] == "h(acme-raw)"
    assert "acme-raw" not in json.dumps({k: v for k, v in body.items() if k != "h"})
    assert body["fr"] == 7.0


def test_ask_without_fairness_sends_no_tf() -> None:
    """Off: the ask's body is byte for byte the one without the feature (no `tf`)."""
    ask = Ask("m", "r1", "w1", 1, "n", 1.0, [Candidate("a/r1")])
    assert ask.fairness is None
    with_it = Ask(
        "m", "r1", "w1", 1, "n", 1.0, [Candidate("a/r1")], fairness=Fairness("share", "h")
    )
    assert with_it.fairness is not None


def test_registry_accepts_each_mode(small: dict[str, Any]) -> None:
    for fairness in (
        {"mode": "share", "weights": {"acme": 2.5}},
        {"mode": "quota", "rpm": 10},
        {"mode": "quota", "quotas": {"acme": {"tpm": 50_000}}},
        {"mode": "fair_queue", "active_secs": 120},
    ):
        registry, errors, _ = check(_with(small, fairness))
        assert registry is not None, (fairness, errors)


@pytest.mark.parametrize(
    ("fairness", "text"),
    [
        ({"mode": "fifo"}, "must be one of"),
        ({"mode": "share", "weights": {"acme": 0}}, "must be above 0"),
        ({"mode": "share", "weights": {"": 1}}, "tenant ids"),
        ({"mode": "quota", "rpm": 0}, "at least 1"),
        ({"mode": "quota", "quotas": {"acme": {"rpd": 5}}}, "unknown field"),
        ({"mode": "quota", "active_secs": 0}, "at least 1"),
        ({"mode": "quota"}, "needs `rpm`, `tpm` or `quotas`"),
    ],
)
def test_registry_refuses(small: dict[str, Any], fairness: dict[str, Any], text: str) -> None:
    registry, errors, _ = check(_with(small, fairness))
    assert registry is None
    assert any(text in e for e in errors), errors


def test_registry_warnings(small: dict[str, Any]) -> None:
    """A setting the mode never reads, a cap that never binds, a tpm cap below one call."""
    doc = _with(small, {"mode": "quota", "rpm": 500, "tpm": 10, "weights": {"a": 2}}, rpm=100)
    registry, errors, warnings = check(doc)
    assert registry is not None, errors
    assert any("fairness.weights: not used in `quota` mode" in w for w in warnings)
    assert any("fairness.rpm: 500 is above the model's rpm 100" in w for w in warnings)
    assert any("fairness.tpm: 10 is below one call's output reservation" in w for w in warnings)
