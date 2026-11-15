"""Public API typing fixes (R3AP-3, R3AP-4, owner Q38), without a store.

A TypedDict that leaves out keys the code returns makes a type checker refuse correct user
code; these compare each declared shape with what the code really builds.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import get_args, get_type_hints
from unittest.mock import MagicMock

import llm_queue
from llm_queue.core import ConcurrencyStatus, DeploymentStatus, RequestStatus
from llm_queue.engine.errors import KINDS, CallFailed, Kind
from llm_queue.engine.status import StatusReader
from llm_queue.registry import load_registry

FIXTURE = Path(__file__).parent / "registry" / "fixtures" / "smallest.json"


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
