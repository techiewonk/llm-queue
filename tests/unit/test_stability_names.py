"""STABILITY.md lists exactly the public names: `llm_queue.__all__` and `llm_queue.langchain`'s.

Review FB-10: the langchain list in STABILITY.md was a subset of its `__all__`.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import ModuleType

import pytest

import llm_queue
import llm_queue.langchain

STABILITY = Path(__file__).resolve().parents[2] / "STABILITY.md"


def _listed(module: str) -> set[str]:
    """The backticked names under `### `<module>`` in the Public names section."""
    text = STABILITY.read_text(encoding="utf-8")
    section = text.split("\n## Public names\n", 1)[1]
    found = re.search(rf"^### `{re.escape(module)}`\n(.*?)(?=^#|\Z)", section, re.M | re.S)
    assert found, f"STABILITY.md has no `### {module}` list under Public names"
    return set(re.findall(r"`([^`]+)`", found.group(1)))


@pytest.mark.parametrize("module", [llm_queue, llm_queue.langchain], ids=lambda m: m.__name__)
def test_stability_lists_every_public_name(module: ModuleType) -> None:
    public = set(module.__all__)
    listed = _listed(module.__name__)
    assert listed == public, {
        "missing from STABILITY.md": sorted(public - listed),
        "not in __all__": sorted(listed - public),
    }
