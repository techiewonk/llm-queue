"""The registry JSON Schema and the registry document page stay in step with the validators."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from llm_queue.engine.cli import amain
from llm_queue.registry import schema as doc
from llm_queue.registry.json_schema import check_schema, registry_schema
from llm_queue.registry.settings import SETTINGS
from llm_queue.registry.validate import check

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_FILE = ROOT / "docs" / "reference" / "registry-schema.json"
PAGE = ROOT / "docs" / "reference" / "registry.md"
FIXTURES = ROOT / "tests" / "unit" / "registry" / "fixtures"


def _example() -> dict[str, Any]:
    """The JSON block after the `registry-example` marker of the registry page."""
    text = PAGE.read_text(encoding="utf-8")
    found = re.search(r"<!-- registry-example -->\s*```json\n(.*?)\n```", text, re.DOTALL)
    assert found, "docs/reference/registry.md lost its registry-example block"
    loaded: dict[str, Any] = json.loads(found.group(1))
    return loaded


def test_checked_in_schema_is_current() -> None:
    """A setting added or changed without regenerating the published schema fails here."""
    stored = json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))
    assert stored == registry_schema(), (
        "regenerate: llm-queue registry schema > docs/reference/registry-schema.json"
    )


def test_every_setting_check_has_a_schema() -> None:
    """A new check function must be taught to `json_schema` (or it raises TypeError)."""
    for name, spec in SETTINGS.items():
        assert check_schema(spec.check), name


async def test_cli_prints_the_schema_without_settings() -> None:
    """`llm-queue registry schema` needs no LLMQ_* settings and no store."""
    assert await amain(["registry", "schema"], environ={}) == registry_schema()


def test_example_and_fixtures_pass_both_the_schema_and_the_library() -> None:
    """The page's example loads strictly, and the schema accepts what the library accepts."""
    jsonschema = pytest.importorskip("jsonschema")
    validator = jsonschema.Draft202012Validator(registry_schema())
    validator.check_schema(registry_schema())
    documents = [_example()] + [
        json.loads(path.read_text(encoding="utf-8")) for path in FIXTURES.glob("*.json")
    ]
    for document in documents:
        registry, errors, _ = check(document)
        assert registry is not None, errors
        assert [e.message for e in validator.iter_errors(document)] == []


def test_schema_refuses_what_the_library_refuses() -> None:
    """Out-of-range and unknown values are refused by the schema too (a sample of each kind)."""
    jsonschema = pytest.importorskip("jsonschema")
    validator = jsonschema.Draft202012Validator(registry_schema())
    for path, value in (
        (("defaults", "rate_burst"), 2),
        (("defaults", "queue", "retention_secs"), 10),
        (("defaults", "fairness", "mode"), "fifo"),
        (("defaults", "no_such_setting"), 1),
        (("models", "gpt120b", "timeout_secs"), 100_000),
    ):
        document = _example()
        target = document
        for key in path[:-1]:
            target = target.setdefault(key, {})
        target[path[-1]] = value
        assert list(validator.iter_errors(document)), path
        assert check(document)[0] is None, path


def test_page_lists_every_document_field() -> None:
    """Every field `registry.schema` reads appears in the registry document page."""
    page = PAGE.read_text(encoding="utf-8")
    listed = set(re.findall(r"^\| `([a-z_]+)` \|", page, re.MULTILINE))
    fields = (
        doc._TOP_KEYS
        | doc._ACCOUNT_FIELDS
        | doc._MODEL_FIELDS
        | doc._DEPLOYMENT_FIELDS
        | doc._ROUTING_FIELDS
        | doc._PROFILE_FIELDS
        | {"allowed_regions"}
    )
    assert fields - listed == set(), "add these fields to docs/reference/registry.md"
