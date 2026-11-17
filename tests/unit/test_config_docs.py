"""Every setting is documented with the code's default and range (like test_log_events_doc).

Covers the registry settings and the fields of the object settings
(docs/reference/configuration.md), every `LLMQ_*` variable the process reads
(docs/reference/environment.md, and `LLMQ_JOB_*` in configuration.md) and every `Tuning` field.
A documented range is probed against the real check: both ends are tried, inside and outside.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, NamedTuple

import pytest

from llm_queue.engine.jobs import JobSettings
from llm_queue.engine.settings import InfraSettings
from llm_queue.errors import ConfigError
from llm_queue.registry import load_registry
from llm_queue.registry import settings as st
from llm_queue.registry.resolve import _complete
from llm_queue.tuning import Tuning

ROOT = Path(__file__).resolve().parents[2]
CONFIG = (ROOT / "docs" / "reference" / "configuration.md").read_text(encoding="utf-8")
ENVIRONMENT = (ROOT / "docs" / "reference" / "environment.md").read_text(encoding="utf-8")
SRC = ROOT / "src" / "llm_queue"
ROW = re.compile(r"^\| `([A-Za-z0-9_.*]+)` \|(.*)\|\s*$", re.MULTILINE)
LEVEL_TEXT = {"D": "defaults", "A": "account", "M": "model", "R": "deployment"}
DEV = {"LLMQ_STORE_URL": "redis://localhost:6379", "LLMQ_ENVIRONMENT": "dev"}


def _section(page: str, heading: str) -> str:
    """The text under `heading` up to the next heading of the same or a higher level."""
    level = heading.split(" ", 1)[0]
    start = page.index(heading + "\n")
    rest = page[start + len(heading) + 1 :]
    found = re.search(rf"^#{{1,{len(level)}}} ", rest, re.MULTILINE)
    return rest[: found.start()] if found else rest


def _rows(text: str) -> dict[str, list[str]]:
    """Table rows by their first cell (a backticked name): the remaining cells, stripped."""
    return {name: [c.strip() for c in cells.split(" | ")] for name, cells in ROW.findall(text)}


def _shown(value: Any) -> str:
    """A default as the docs write it (backticks removed)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return "empty" if value in ("", ()) else str(value)


def _first(cell: str) -> str:
    """The first word of a cell, without backticks or trailing punctuation."""
    return cell.replace("`", "").split()[0].rstrip(";,")


class Range(NamedTuple):
    """A documented range: `[above ]LO[ to HI| or more|, below HI][ (whole)]`."""

    lo: float
    lo_open: bool
    hi: float | None
    hi_open: bool
    whole: bool


_NUM = r"(-?\d+(?:\.\d+)?)"
_RANGE = re.compile(
    rf"^(above )?{_NUM}(?: to {_NUM}| or more|, below {_NUM})?( \(whole\))?$",
)


def _range(cell: str) -> Range | None:
    found = _RANGE.match(cell.replace("`", ""))
    if not found:
        return None
    above, lo, hi, below, whole = found.groups()
    top = hi if hi is not None else below
    return Range(
        float(lo), bool(above), None if top is None else float(top), below is not None, bool(whole)
    )


def _probes(r: Range) -> Iterator[tuple[float, bool]]:
    """(value, accepted?) pairs at both ends of a range, and a fraction for whole numbers."""
    step = 1 if r.whole else 0.001
    if r.lo_open:
        yield r.lo, False
        yield r.lo + step, True
    else:
        yield r.lo, True
        yield r.lo - step, False
    if r.hi is not None:
        if r.hi_open:
            yield r.hi, False
            yield r.hi - step, True
        else:
            yield r.hi, True
            yield r.hi + step, False
    if r.whole:
        fraction = r.lo + (1.5 if r.lo_open else 0.5)
        # Near 2**53 a float has no fraction left to try.
        if (r.hi is None or fraction < r.hi) and fraction % 1:
            yield fraction, False


def _plain(value: float, whole: bool) -> int | float:
    return int(value) if whole and float(value).is_integer() else value


def _check_range(name: str, cell: str, refused: Callable[[float | int], bool]) -> None:
    """Probe `cell` (when it is a numeric range) with `refused(value) -> bool`."""
    r = _range(cell)
    if r is None:
        return
    for value, ok in _probes(r):
        plain = _plain(value, r.whole)
        assert refused(plain) is not ok, f"{name}: documented range `{cell}`, value {plain}"


# Registry settings ---------------------------------------------------------------------------

REGISTRY = _section(CONFIG, "## Registry settings")
REGISTRY_ROWS = _rows(REGISTRY)


def _levels(spec: st.Spec) -> str:
    if len(spec.levels) == 4:
        return "all four"
    return ", ".join(LEVEL_TEXT[level] for level in "DAMR" if level in spec.levels)


def _sub_defaults(name: str, spec: st.Spec) -> dict[str, Any]:
    if isinstance(spec.default, dict):
        return dict(spec.default)
    filled = _complete(name, {})
    return {k: v for k, v in filled.items() if v not in ([], None)}


def _object_fields() -> Iterator[tuple[str, st.Check, Any]]:
    for name, spec in st.SETTINGS.items():
        if isinstance(spec.check, st.ObjCheck):
            defaults = _sub_defaults(name, spec)
            for field, check in spec.check.fields.items():
                yield f"{name}.{field}", check, defaults.get(field)


def _check_choices(name: str, check: st.Check, cell: str) -> None:
    if isinstance(check, st.OneOf):
        words = set(re.findall(r"`([a-z_]+)`", cell))
        assert words == set(check.values), f"{name}: documented choices {cell}"


@pytest.mark.parametrize("name", sorted(st.SETTINGS))
def test_registry_setting_is_documented(name: str) -> None:
    """Each registry setting has a row: its levels, default and range match the code."""
    spec = st.SETTINGS[name]
    assert name in REGISTRY_ROWS, f"add `{name}` to the registry settings table"
    levels, default, cell, _meaning = REGISTRY_ROWS[name]
    assert levels == _levels(spec), name
    if spec.by_type is not None:
        for kind in ("chat", "embedding"):
            assert f"{kind} {_shown(spec.by_type(kind))}" in default.replace("`", ""), name
    elif isinstance(spec.default, dict):
        assert default == "see fields", name
    elif spec.default is None:
        assert default == "none", name
    else:
        assert _first(default) == _shown(spec.default), name
    if isinstance(spec.check, st.Number):
        assert _range(cell), f"{name}: write its range as `LO to HI` (see the page)"
    _check_choices(name, spec.check, cell)
    _check_range(name, cell, lambda v: spec.check(v) is not None)


@pytest.mark.parametrize("name", sorted(n for n, _, _ in _object_fields()))
def test_object_setting_field_is_documented(name: str) -> None:
    """Each field of an object setting (queue.*, health.*, fairness.* ...) has a row."""
    check, default = {n: (c, d) for n, c, d in _object_fields()}[name]
    assert name in REGISTRY_ROWS, f"add `{name}` to the object settings' fields table"
    documented, cell, _meaning = REGISTRY_ROWS[name]
    if default is not None:
        assert _first(documented) == _shown(default), name
    if isinstance(check, st.Number):
        assert _range(cell), f"{name}: write its range as `LO to HI` (see the page)"
    _check_choices(name, check, cell)
    _check_range(name, cell, lambda v: check(v) is not None)


def test_breaker_probe_default_is_documented_as_derived() -> None:
    """D4: unset, the probe time follows a longer attempt timeout; the row says so."""
    _levels, default, _cell, _meaning = REGISTRY_ROWS["breaker_probe_secs"]
    assert default == "60, or the attempt timeout when longer"
    doc = {
        "registry_version": 1,
        "defaults": {"attempt_timeout_secs": 100, "timeout_secs": 300},
        "accounts": {"aws": {"kind": "aws"}},
        "models": {
            "m": {
                "type": "chat",
                "provider": "bedrock",
                "model_id": "openai.gpt-oss-20b-1:0",
                "deployments": {"aws": {"confirmed": ["ap-south-1"]}},
            }
        },
    }
    assert load_registry(doc).value("m", "breaker_probe_secs", "aws/ap-south-1") == 100


def test_registry_tables_list_nothing_else() -> None:
    """A removed or renamed setting must leave the tables too."""
    known = set(st.SETTINGS) | {n for n, _, _ in _object_fields()}
    assert set(REGISTRY_ROWS) - known == set()


# Process settings ----------------------------------------------------------------------------

ENV_ROWS = _rows(ENVIRONMENT)
JOB_ROWS = _rows(_section(CONFIG, "## Job settings (`LLMQ_JOB_*`)"))


def _variables_read() -> set[str]:
    """Every `LLMQ_*` variable the library reads (settings.py names, literals, job fields)."""
    settings = (SRC / "engine" / "settings.py").read_text(encoding="utf-8")
    names = {f"LLMQ_{n}" for n in re.findall(r'"([A-Z][A-Z0-9_]*[A-Z0-9])"', settings)}
    for path in SRC.rglob("*.py"):
        names |= set(re.findall(r'"(LLMQ_[A-Z0-9_]*[A-Z0-9])"', path.read_text(encoding="utf-8")))
    names |= {f"LLMQ_JOB_{f.name.upper()}" for f in dataclasses.fields(JobSettings)}
    return names


def _problems(read: Callable[[], object]) -> list[str]:
    try:
        read()
    except ConfigError as exc:
        return [line.strip() for line in str(exc).splitlines()[1:]]
    return []


def test_every_variable_is_documented() -> None:
    """A new `LLMQ_*` variable must be added to environment.md (or the job table)."""
    documented = set(ENV_ROWS) | set(JOB_ROWS)
    assert _variables_read() - documented == set()
    assert documented - _variables_read() == {"LLMQ_ACCOUNT_*"}


@pytest.mark.parametrize("variable", sorted(set(ENV_ROWS) - {"LLMQ_ACCOUNT_*"}))
def test_process_variable_default_and_range(variable: str) -> None:
    """environment.md: the default matches `InfraSettings` (dev), the range what it refuses."""
    default, cell, _meaning = ENV_ROWS[variable]
    field = variable.removeprefix("LLMQ_").lower()
    defaults = InfraSettings.from_env(DEV)
    value = getattr(defaults, field, None)
    if field == "queue_wait_scopes":
        assert _first(default) == "all"
    elif field == "environment":
        assert _first(default) == "unset"  # what `from_env({})` reads; DEV sets it
    elif field in ("store_url", "store_password", "queue_store_password"):
        pass  # required, or a secret with no default
    elif value is None:
        assert _first(default) in ("none", "the"), variable
    else:
        assert _first(default) == _shown(value), variable

    def refused(number: float | int) -> bool:
        lines = _problems(lambda: InfraSettings.from_env({**DEV, variable: str(number)}))
        return any(line.startswith(f"{variable} must") for line in lines)

    _check_range(variable, cell, refused)


@pytest.mark.parametrize("variable", sorted(JOB_ROWS))
def test_job_variable_default_and_range(variable: str) -> None:
    """configuration.md's job table: defaults of `JobSettings`, ranges it refuses."""
    default, cell, _meaning = JOB_ROWS[variable]
    field = variable.removeprefix("LLMQ_JOB_").lower()
    assert _first(default) == _shown(getattr(JobSettings(), field)), variable

    def refused(number: float | int) -> bool:
        lines = _problems(lambda: JobSettings.from_env({variable: str(number)}))
        return any(line.startswith(f"{variable} must") for line in lines)

    _check_range(variable, cell, refused)


# Tuning --------------------------------------------------------------------------------------

TUNING_ROWS = _rows(_section(CONFIG, "## `Tuning` (in code)"))
_OTHER_FIELD = re.compile(r"\b(?:store|bedrock|engine|registry|openai|warm)\.[a-z_]+")


def _tuning_fields() -> Iterator[tuple[str, str, Any]]:
    tuning = Tuning()
    for part in dataclasses.fields(tuning):
        options = getattr(tuning, part.name)
        for spec in dataclasses.fields(options):
            yield f"{part.name}.{spec.name}", spec.name, getattr(options, spec.name)


@pytest.mark.parametrize("name", sorted(n for n, _, _ in _tuning_fields()))
def test_tuning_field_is_documented(name: str) -> None:
    """Each `Tuning` field has a row with its default; its range is what `problems()` refuses."""
    assert name in TUNING_ROWS, f"add `{name}` to the Tuning tables"
    default, cell, _meaning = TUNING_ROWS[name]
    code_default = {n: d for n, _, d in _tuning_fields()}[name]
    assert _first(default) == _shown(code_default), name
    part, field = name.split(".")
    if isinstance(code_default, str):
        choices = set(re.findall(r"`([a-z_]+)`", cell))
        assert code_default in choices, name

    def refused(number: float | int) -> bool:
        options = dataclasses.replace(getattr(Tuning(), part), **{field: number})
        lines = Tuning(**{part: options}).problems()
        # Lines naming a second field are checks across fields, listed on the Tune page.
        return any(
            line.startswith(f"{name} must") and set(_OTHER_FIELD.findall(line)) == {name}
            for line in lines
        )

    _check_range(name, cell, refused)


def test_tuning_tables_list_nothing_else() -> None:
    """A removed or renamed `Tuning` field must leave the tables too."""
    assert set(TUNING_ROWS) - {n for n, _, _ in _tuning_fields()} == set()
