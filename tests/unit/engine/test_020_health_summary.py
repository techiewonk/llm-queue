"""0.2.0 F2: the per-model health summary and its thresholds (registry `health_summary`).

Pure: model reports shaped like `StatusReader.model` against a loaded registry, no store.
The summary on a real store (status, `GET /`, CLI) is in the integration tests and gate F2.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from llm_queue.engine.status import health_report, model_health, worst_health
from llm_queue.registry import load_registry
from llm_queue.registry.validate import RegistryError

DOC: dict[str, Any] = {
    "registry_version": 1,
    "residency": {"allowed_regions": ["us-east-1", "us-west-2"]},
    "accounts": {"aws-0": {"kind": "aws"}},
    "models": {
        "chat": {
            "type": "chat",
            "provider": "bedrock",
            "model_id": "openai.gpt-oss-20b-1:0",
            "rpm": 100,
            "deployments": {"aws-0": {"confirmed": ["us-east-1", "us-west-2"]}},
            "health_summary": {"waiting_warning": 10, "waiting_critical": 50},
        }
    },
}


def _view(doc: dict[str, Any] = DOC) -> Any:
    return load_registry(doc)


def _dep(name: str, *, used: int = 0, limit: int = 100, **kw: Any) -> dict[str, Any]:
    row = {
        "name": name,
        "enabled": True,
        "account_enabled": True,
        "blocked": False,
        "healthy": True,
        "outside_residency": False,
        "breaker": "closed",
        "rpm": {"limit": limit, "used": used},
        "tpm": {"limit": None, "used": 0},
    }
    row.update(kw)
    return row


def _report(**kw: Any) -> dict[str, Any]:
    report = {
        "model": "chat",
        "enabled": True,
        "waiting": {"priority": 0, "normal": 0},
        "waiting_for_grant": {"priority": 0, "normal": 0},
        "oldest_waiting_secs": None,
        "dead_letters": 0,
        "deployments": [_dep("a"), _dep("b")],
    }
    report.update(kw)
    return report


def test_healthy_by_default() -> None:
    out = model_health(_view(), _report())
    assert out["state"] == "healthy"
    assert out["reasons"] == []
    assert out["budget_used_pct"] == 0.0
    assert out["usable_deployments"] == 2


@pytest.mark.parametrize(
    ("change", "state", "reason"),
    [
        ({"deployments": [_dep("a", used=85), _dep("b", used=85)]}, "warning", "budget_used"),
        ({"deployments": [_dep("a", used=99), _dep("b", used=99)]}, "critical", "budget_used"),
        # Per model thresholds from the registry (10 / 50), queued and held calls together.
        ({"waiting": {"priority": 5, "normal": 5}}, "warning", "waiting"),
        ({"waiting_for_grant": {"priority": 0, "normal": 50}}, "critical", "waiting"),
        ({"oldest_waiting_secs": 30.0}, "warning", "oldest_wait"),
        ({"oldest_waiting_secs": 121.0}, "critical", "oldest_wait"),
        ({"deployments": [_dep("a", breaker="open"), _dep("b")]}, "warning", "breaker_open"),
        ({"deployments": [_dep("a", healthy=False), _dep("b")]}, "warning", "deployment_disabled"),
        ({"deployments": [_dep("a", breaker="open")]}, "critical", "no_usable_deployment"),
        ({"dead_letters": 1}, "warning", "dead_letters"),
        ({"enabled": False, "deployments": []}, "warning", "model_disabled"),
    ],
)
def test_each_rule(change: dict[str, Any], state: str, reason: str) -> None:
    out = model_health(_view(), _report(**change))
    assert out["state"] == state
    assert reason in out["reasons"]


def test_budget_counts_usable_deployments_only() -> None:
    # The open-breaker deployment's full budget is not counted: 50 of 100 on the usable one.
    deps = [_dep("a", used=100, breaker="open"), _dep("b", used=50)]
    out = model_health(_view(), _report(deployments=deps))
    assert out["budget_used_pct"] == 50.0


def test_kill_switch_warns() -> None:
    doc = copy.deepcopy(DOC)
    doc["routing"] = {"kill_switch": True}
    assert "kill_switch" in model_health(_view(doc), _report())["reasons"]


def test_thresholds_are_checked() -> None:
    doc = copy.deepcopy(DOC)
    doc["models"]["chat"]["health_summary"] = {"budget_warning_pct": 101}
    with pytest.raises(RegistryError, match="health_summary"):
        load_registry(doc)


def test_report_and_worst() -> None:
    assert worst_health([]) == "healthy"
    assert worst_health(["healthy", "critical", "warning"]) == "critical"
    report = {"registry_version": 1, "models": [_report(oldest_waiting_secs=40.0)]}
    out = health_report(_view(), report)
    assert out["health"] == "warning"
    assert out["models"][0]["model"] == "chat"
