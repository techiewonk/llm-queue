"""Self-tests of the run plan: check.py `all` runs each test once, scenarios' matrix (FT-1, FT-2).

`all` ran `test_runtime.py` five times and `tests/unit` six times; one harness exception ended
a serial scenario run (FT-6); a hung check held the run for ever (FT-7). No stack is needed.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

check: Any = importlib.import_module("check")
scenarios: Any = importlib.import_module("scenarios")


def test_union_keeps_each_test_path_once() -> None:
    """A file under a chosen folder, or a test id of a chosen file, is dropped."""
    paths = [
        "tests/unit",
        "tests/unit/engine",
        "tests/unit/test_tuning.py",
        "tests/integration/engine/test_runtime.py",
        "tests/integration/engine/test_runtime.py::test_x",
        "tests/integration/engine/test_runtime.py",
        "tests/integration/registry",
        "tests/integration/registry2/test_a.py",
    ]
    assert check.union(paths) == [
        "tests/unit",
        "tests/integration/engine/test_runtime.py",
        "tests/integration/registry",
        "tests/integration/registry2/test_a.py",
    ]


def test_all_runs_every_test_folder_once() -> None:
    """Every in-`all` step's tests collapse to three pytest paths; bench is not in `all`."""
    names = [n for n, s in check.STEPS.items() if s.in_all]
    tests = check.union([t for n in names for t in check.STEPS[n].tests])
    assert tests == ["tests/unit", "tests/integration/registry", "tests/integration/engine"]
    assert "bench" not in names
    assert all(check.STEPS[n].timeout_secs > 0 for n in names)


def test_extras_take_the_arguments_of_the_mode() -> None:
    """quick: `at` only; full: the default matrix; release: every load; drills excluded."""
    step = check.STEPS["scenarios"]
    assert step.extra[0][-4:] == ("--exclude", "S6", "S7", "S11")
    assert step.quick == ("--load", "at")
    assert step.full == ("--matrix", "default")
    assert step.release == ("--matrix", "full")


def test_a_check_that_hangs_fails_on_its_time_limit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """FT-7: a check past its limit is killed and fails instead of holding the run."""
    monkeypatch.setattr(check, "LOG_DIR", tmp_path)
    monkeypatch.setattr(check, "ROOT", tmp_path)
    cmd = [sys.executable, "-c", "import time; time.sleep(30)"]
    assert check._run("t", "sleeper", cmd, timeout_secs=0.5) is False
    assert "timed out" in (tmp_path / "t.sleeper.log").read_text(encoding="utf-8")


def test_default_matrix_is_at_plus_burst_where_load_matters() -> None:
    """FT-2: 35 `at` units plus 13 `burst` ones, against 105 in the full matrix."""
    ids = list(scenarios.BY_ID)
    default = scenarios.plan(ids, None, "default")
    assert {load for _, load in default} == {"at", "burst"}
    assert {sid for sid, load in default if load == "burst"} == set(scenarios.BURST_SET)
    assert len(default) == len(ids) + len(scenarios.BURST_SET)
    assert len(scenarios.plan(ids, None, "full")) == 3 * len(ids)
    assert scenarios.plan(["S4", "S6"], ["under"], "default") == [("S4", "under")]


def test_a_unit_that_raises_fails_alone(capsys: pytest.CaptureFixture[str]) -> None:
    """FT-6: a harness exception fails its unit; the caller goes on to the next one."""

    def broken() -> bool:
        raise RuntimeError("the registry edit never reached the store")

    assert scenarios.guarded("S4 at", broken) is False
    assert scenarios.guarded("S4 under", lambda: True) is True
    assert "FAIL  S4 at (harness error" in capsys.readouterr().out
