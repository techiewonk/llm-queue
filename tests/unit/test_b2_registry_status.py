"""0.1.0b2 state alerts (RESCAN O3), evaluated on hand-made status reports (no store).

0.1.0b1 called a model available while every deployment was blocked, in a disabled account or
behind an open breaker, and judged quota use by RPM alone.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from llm_queue.engine.keys import Keys
from llm_queue.engine.status import AlertEvaluator
from llm_queue.registry import load_registry

FIXTURES = Path(__file__).parent / "registry" / "fixtures"


def _row(**over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "name": "aws-main/ap-south-1",
        "enabled": True,
        "account_enabled": True,
        "blocked": False,
        "healthy": True,
        "outside_residency": False,
        "breaker": "closed",
        "rpm": {"limit": 100, "used": 10, "left": 90},
        "tpm": {"limit": 1000, "used": 10, "left": 990},
        "throttles_1m": 0,
    }
    row.update(over)
    return row


def _states(row: dict[str, Any]) -> dict[str, bool]:
    view = load_registry(json.loads((FIXTURES / "smallest.json").read_text(encoding="utf-8")))
    report = {"models": [{"model": "gpt20b", "dead_letters": 0, "deployments": [row]}]}
    evaluator = AlertEvaluator(None, Keys("t"))  # type: ignore[arg-type]  # evaluate needs no store
    return {a.name: a.on for a in evaluator.evaluate(view, report)}


@pytest.mark.parametrize(
    "over",
    [{"blocked": True}, {"account_enabled": False}, {"breaker": "open"}],
)
def test_model_unavailable_counts_blocks_accounts_and_breakers(over: dict[str, Any]) -> None:
    assert _states(_row())["model_unavailable"] is False
    assert _states(_row(**over))["model_unavailable"] is True


def test_quota_near_limit_checks_tpm() -> None:
    row = _row(tpm={"limit": 1000, "used": 950, "left": 50})
    assert _states(row)["quota_near_limit"] is True
    assert _states(_row())["quota_near_limit"] is False
    no_limits = _row(rpm={"limit": None, "used": 5, "left": None}, tpm=copy.deepcopy(row["tpm"]))
    no_limits["tpm"]["limit"] = None
    assert _states(no_limits)["quota_near_limit"] is False
