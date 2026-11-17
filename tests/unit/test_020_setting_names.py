"""Every documented `LLMQ_*` setting is really read, under the name the docs give (review B-01).

A rename that rewrites the docs but misses a `number("OLD", ...)` read leaves the new variable
silently ignored; this records the names `from_env` asks for instead of grepping for literals.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest

from llm_queue import secrets
from llm_queue.engine import settings as st
from llm_queue.registry.validate import check

ROOT = Path(__file__).resolve().parents[2]
ENVIRONMENT = (ROOT / "docs" / "reference" / "environment.md").read_text(encoding="utf-8")
SRC = ROOT / "src" / "llm_queue"
DOCUMENTED = set(re.findall(r"^\| `(LLMQ_[A-Z0-9_]+)` \|", ENVIRONMENT, re.MULTILINE))
REGISTRY = '{"registry_version": 1, "models": {}}'
RUNS: tuple[dict[str, str], ...] = (
    {"LLMQ_STORE_URL": "redis://localhost:6379/0", "LLMQ_ENVIRONMENT": "dev"},
    {"LLMQ_STORE_HOST": "localhost", "LLMQ_ENVIRONMENT": "dev", "LLMQ_REGISTRY": REGISTRY},
    {"LLMQ_EXECUTION_MODE": "local", "LLMQ_ENVIRONMENT": "dev", "LLMQ_REGISTRY": REGISTRY},
)


def _read_by_from_env(monkeypatch: pytest.MonkeyPatch) -> set[str]:
    """The names `InfraSettings.from_env` looks up, over runs that take each branch."""
    asked: set[str] = set()
    real = st.Layers.get

    def get(self: st.Layers, name: str, default: Any = None) -> Any:
        asked.add(name)
        return real(self, name, default)

    monkeypatch.setattr(st.Layers, "get", get)
    for environ in RUNS:
        st.InfraSettings.from_env(environ)
    return asked


def _read_elsewhere() -> set[str]:
    """Full names other modules read (store passwords, admin tokens, the secrets settings)."""
    found: set[str] = set(secrets.SETTINGS)
    for path in SRC.rglob("*.py"):
        if path.name != "settings.py":
            found |= set(re.findall(r'"(LLMQ_[A-Z0-9_]*[A-Z0-9])"', path.read_text("utf-8")))
    return found


def test_the_names_table_is_what_environment_md_documents() -> None:
    assert st.SETTING_NAMES == DOCUMENTED


def test_every_documented_setting_is_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each name in environment.md is asked for by `from_env` or read by another module."""
    read = _read_by_from_env(monkeypatch) | _read_elsewhere()
    assert DOCUMENTED - read == set()


def test_the_rename_table_is_consistent() -> None:
    """Old names are current settings, new names are not yet, and no name is listed twice."""
    old, new = set(st.PENDING_RENAMES), set(st.PENDING_RENAMES.values())
    assert old <= st.SETTING_NAMES
    assert not new & st.SETTING_NAMES
    assert not (old | new) & (set(st.RENAMED) | set(st.RENAMED.values()) | set(st.REMOVED))
    assert all(n.startswith("LLMQ_") and n.isupper() for n in old | new)


def test_no_setting_is_in_the_account_secret_namespace() -> None:
    assert not [n for n in st.SETTING_NAMES if n.startswith("LLMQ_ACCOUNT_")]


def test_a_registry_ref_naming_a_setting_is_refused() -> None:
    """Review B-03: an account's `api_key_ref` may never name a process setting."""
    doc = {
        "registry_version": 1,
        "accounts": {
            "oa": {
                "kind": "openai",
                "base_url": "https://api.openai.com/v1",
                "api_key_ref": "LLMQ_STORE_PASSWORD",
            }
        },
        "models": {},
    }
    registry, errors, _ = check(doc, allowed_hosts=("api.openai.com",))
    assert registry is None
    assert any("is a process setting" in e for e in errors)
