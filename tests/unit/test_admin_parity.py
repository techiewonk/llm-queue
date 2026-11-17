"""R3: the operations table, the CLI, the HTTP routes and `llmq.admin` agree (no store).

A new operation cannot ship on one surface only: each check compares one surface with
`OPERATIONS`, and `docs/reference/admin-api.md` must have one row per operation id.
"""

from __future__ import annotations

import argparse
import inspect
import re
import typing
from pathlib import Path
from typing import Any

import pytest
from admin_testkit import FakeAdmin, allow, client, mounted

from llm_queue.engine.admin import AdminServices
from llm_queue.engine.admin.ops import OPERATIONS, Op
from llm_queue.engine.cli import _parser
from llm_queue.engine.http import ADMIN_HEADER

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "reference" / "admin-api.md"
SYNC = {"ops.list", "tenants.hash", "registry.schema", "drain.get", "describe"}
# Python-only parameters a surface fills in itself.
PYTHON_ONLY = {"ops.index": {"routes"}}
RESERVED = {"actor", "dry_run", "yes", "admin_op", "command", "action"}


def _cli_paths(
    parser: argparse.ArgumentParser, prefix: tuple[str, ...] = ()
) -> set[tuple[str, ...]]:
    found: set[tuple[str, ...]] = set()
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for name, sub in action.choices.items():
                deeper = _cli_paths(sub, (*prefix, name))
                found |= deeper or {(*prefix, name)}
    return found


def _method(op: Op) -> Any:
    """The unbound method `op.method` names, walking `AdminServices`' annotated services."""
    assert op.method is not None
    owner: Any = AdminServices
    *path, name = op.method.split(".")
    for part in path:
        owner = typing.get_type_hints(owner)[part]
    return getattr(owner, name)


def test_ids_are_unique_and_rows_complete() -> None:
    ids = [op.id for op in OPERATIONS]
    assert len(ids) == len(set(ids))
    for op in OPERATIONS:
        assert op.summary, op.id
        for surface, present in (("cli", op.cli), ("http", op.http), ("python", op.method)):
            if present is None:
                assert op.missing.get(surface), f"{op.id}: say why it has no {surface}"
        # The surfaces add these themselves: a parameter of that name would be overwritten
        # (`audit list --op` once hid the CLI's own default).
        assert not {p.name for p in op.params} & RESERVED, op.id
        if op.writes:
            assert op.audit_op, op.id
            assert op.http is not None, op.id
            assert op.http[0] == "POST", op.id


def test_cli_equals_the_table() -> None:
    assert _cli_paths(_parser()) == {op.cli for op in OPERATIONS if op.cli}


def test_http_equals_the_table() -> None:
    http = client(
        FakeAdmin(),
        dependencies=[allow],
        allow_writes=True,
        actor=lambda r: "ann",
        drain_routes=True,
        metrics_route=True,
    )
    routes = mounted(http)
    assert routes == {op.http for op in OPERATIONS if op.http}


@pytest.mark.parametrize("op", [op for op in OPERATIONS if op.method], ids=lambda op: op.id)
def test_python_method_matches_the_params(op: Op) -> None:
    method = _method(op)
    assert inspect.iscoroutinefunction(method) or op.method in SYNC, op.id
    sig = inspect.signature(method)
    for param in op.params:
        assert param.name in sig.parameters, f"{op.id}: no parameter {param.name}"
        default = sig.parameters[param.name].default
        if param.required:
            assert default is inspect.Parameter.empty, f"{op.id}.{param.name} has a default"
        else:
            assert default == param.default, f"{op.id}.{param.name}: {default!r}"
    extra = set(sig.parameters) - {"self"} - {p.name for p in op.params}
    if op.writes:
        assert sig.parameters["actor"].default is inspect.Parameter.empty, op.id
        extra -= {"actor"}
        if op.dry_run_default is not None:
            assert sig.parameters["dry_run"].default is True, op.id
            extra -= {"dry_run"}
    assert extra == PYTHON_ONLY.get(op.id, set()), f"{op.id}: {extra}"


def test_writes_are_guarded_and_dry_run_by_default() -> None:
    admin = FakeAdmin()
    no_writes = client(admin, dependencies=[allow])
    guarded = client(admin, dependencies=[allow], allow_writes=True, actor=lambda r: "ann")
    for op in OPERATIONS:
        if not op.writes or op.mount != "writes" or op.http is None:
            continue
        path = re.sub(r"\{[a-z_]+\}", "x", op.http[1])
        assert no_writes.post(path, json={}, headers={ADMIN_HEADER: "1"}).status_code in (
            404,
            405,
        ), op.id
        assert guarded.post(path, json={}).status_code == 403, op.id  # no admin header
        assert op.dry_run_default is True, op.id
    parser = _parser()
    for op in OPERATIONS:
        if op.writes and op.cli:
            leaf = parser
            for word in op.cli:
                sub = next(a for a in leaf._actions if isinstance(a, argparse._SubParsersAction))
                leaf = sub.choices[word]
            assert any("--yes" in a.option_strings for a in leaf._actions), op.id


def test_docs_have_one_row_per_operation() -> None:
    text = DOC.read_text(encoding="utf-8")
    rows = set(re.findall(r"^\| `([a-z_]+\.[a-z_]+)` \|", text, re.MULTILINE))
    assert rows == {op.id for op in OPERATIONS}
