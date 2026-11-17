"""The test tree itself: `pytest tests/unit tests/integration` must collect.

The test directories have no `__init__.py`, so pytest imports each test file by its basename:
two files with one basename stop the whole run at collection ("import file mismatch").
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

TESTS = Path(__file__).resolve().parents[1]


def test_no_two_test_files_share_a_basename() -> None:
    seen: dict[str, list[str]] = defaultdict(list)
    for path in TESTS.rglob("test_*.py"):
        seen[path.name].append(str(path.relative_to(TESTS)))
    shared = {name: paths for name, paths in seen.items() if len(paths) > 1}
    assert not shared, f"rename one of each (pytest imports by basename): {shared}"
