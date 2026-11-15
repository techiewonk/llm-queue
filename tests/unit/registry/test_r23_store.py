"""Registry store decisions Q25 (stored protected fields must match the deploy config) and Q41.

Runs on the fake store client of `test_store_unit.py`; no store is needed.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import orjson
import pytest
from test_store_unit import _Client, _meta, _store

from llm_queue.registry.store import carried_routing, protected_mismatches

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def full() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((FIXTURES / "full.json").read_text(encoding="utf-8"))
    return data


class _Metrics:
    def __init__(self) -> None:
        self.counts: list[tuple[str, dict[str, str]]] = []

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        self.counts.append((name, labels))


@pytest.mark.parametrize(
    "change",
    [
        lambda d: d["accounts"]["aws-dr"].update(role_arn="arn:aws:iam::999999999999:role/x"),
        lambda d: d["models"][next(iter(d["models"]))].update(model_id="other.model-v1:0"),
        lambda d: d.update(live_edit="all"),
        lambda d: d["accounts"].update(evil={"kind": "aws"}),
    ],
)
async def test_a_stored_document_with_foreign_protected_fields_is_refused(
    full: dict[str, Any], change: Any
) -> None:
    """Q25 (R3SE-3): a stored document was accepted after a schema check only.

    Whoever could write the key could add a role in another AWS account, change a model id or
    set `live_edit: all`. A pod holding its config refuses such a document (logged, counted)
    and keeps what it had.
    """
    stored = copy.deepcopy(full)
    stored["registry_version"] = full["registry_version"] + 1
    change(stored)
    store = _store(_Client(stored, _meta(version=stored["registry_version"])), config=full)
    metrics = _Metrics()
    store.metrics = metrics  # a fake sink
    assert await store.load() is None
    assert store.stored_refused
    assert ("llmq_registry_refused_total", {"reason": "protected_mismatch"}) in metrics.counts


async def test_a_stored_document_with_live_numbers_is_accepted(full: dict[str, Any]) -> None:
    """Q25: numbers edited live (rpm) and routing blocks are not protected: still accepted."""
    stored = copy.deepcopy(full)
    alias = next(iter(stored["models"]))
    stored["models"][alias]["rpm"] = 7
    stored["routing"]["blocked_regions"] = ["ap-south-1"]
    store = _store(_Client(stored, _meta()), config=full)
    assert await store.load() is not None
    assert protected_mismatches(full, stored) == []


async def test_rollback_keeps_and_lists_the_live_blocks(full: dict[str, Any]) -> None:
    """Q41: the rollback reply did not say which live blocks and kill switch it kept."""
    live = copy.deepcopy(full)
    live["routing"] = {"kill_switch": True, "blocked_regions": ["ap-south-1"]}
    client = _Client(live, _meta(version=full["registry_version"]))
    old = copy.deepcopy(full)
    client.history[str(full["registry_version"])] = orjson.dumps(old)
    store = _store(client)
    result = await store.rollback(full["registry_version"], actor="ops@host")
    assert result.carried == ("routing.blocked_regions: ap-south-1", "routing.kill_switch")
    assert carried_routing(live, live) == ()
