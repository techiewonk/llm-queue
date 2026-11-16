"""The README's quickstart code, registry and links match the library and the docs.

The README is the PyPI page and the first thing a user copies from; nothing ran its code or
followed its links, so a renamed import, a registry field the validator refuses or a moved docs
page shipped unnoticed.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import orjson
import pytest

import llm_queue
from llm_queue.registry import load_registry

ROOT = Path(__file__).resolve().parents[2]
README = (ROOT / "README.md").read_text(encoding="utf-8")
DOCS_SITE = "https://techiewonk.github.io/llm-queue/"
REPO = "https://github.com/techiewonk/llm-queue/"
FENCE = re.compile(r"^```(\w+)\n(.*?)^```", re.MULTILINE | re.DOTALL)
URL = re.compile(r"https://[^\s)\]>\"'*]+")  # `*`: a bold URL ends in `**`


def blocks(lang: str) -> list[str]:
    """The README's fenced code blocks in language `lang`."""
    return [body for found, body in FENCE.findall(README) if found == lang]


def test_readme_has_python_and_registry_blocks() -> None:
    """The extraction found what the other tests check (an empty list would pass them all)."""
    assert blocks("python")
    assert any('"registry_version"' in body for body in blocks("json"))


@pytest.mark.parametrize("code", blocks("python"))
def test_python_blocks_compile(code: str) -> None:
    """Every Python block is valid Python (it is copied as is)."""
    compile(code, "README.md", "exec")


def test_imported_names_exist() -> None:
    """Every `from llm_queue import ...` name in the README is public in `llm_queue`."""
    names = {
        alias.name
        for code in blocks("python")
        for node in ast.walk(ast.parse(code))
        if isinstance(node, ast.ImportFrom) and node.module == "llm_queue"
        for alias in node.names
    }
    assert names, "no `from llm_queue import` in the README's Python blocks"
    missing = sorted(n for n in names if n not in llm_queue.__all__)
    assert missing == [], f"not in llm_queue.__all__: {missing}"


@pytest.mark.parametrize("doc", [b for b in blocks("json") if '"registry_version"' in b])
def test_registry_blocks_validate(doc: str) -> None:
    """A registry document in the README passes the library's validator (`load_registry`)."""
    load_registry(orjson.loads(doc))


def _docs_page(url: str) -> list[Path]:
    """The docs files a site URL can be built from (`<path>.md` or `<path>/index.md`)."""
    path = url.removeprefix(DOCS_SITE).split("#", 1)[0].strip("/")
    if not path:
        return [ROOT / "docs" / "index.md"]
    return [ROOT / "docs" / f"{path}.md", ROOT / "docs" / path / "index.md"]


def test_docs_links_point_at_pages() -> None:
    """Every docs-site URL in the README maps to a page in docs/ (a moved page broke it)."""
    urls = [u for u in URL.findall(README) if u.startswith(DOCS_SITE)]
    assert urls
    broken = [u for u in urls if not any(p.is_file() for p in _docs_page(u))]
    assert broken == [], f"no docs page for: {broken}"


def test_repo_links_point_at_files() -> None:
    """Every `blob/main` or `tree/main` link in the README names a file or folder in the repo."""
    paths = [
        re.sub(r"^(?:blob|tree)/main/", "", u.removeprefix(REPO)).split("#", 1)[0]
        for u in URL.findall(README)
        if u.startswith((f"{REPO}blob/main/", f"{REPO}tree/main/"))
    ]
    assert paths
    missing = [p for p in paths if not (ROOT / p).exists()]
    assert missing == [], f"not in the repo: {missing}"
