"""Self-tests of the manual tools: docs check, release watcher, install smoke test, keys page.

They run by hand (see CONTRIBUTING, "Manual tools"); these tests keep their logic honest
without network, a store or Docker.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

check_docs: Any = importlib.import_module("check_docs")
watch_release: Any = importlib.import_module("watch_release")
smoke_install: Any = importlib.import_module("smoke_install")
store_keys_page: Any = importlib.import_module("store_keys_page")


@pytest.mark.parametrize(
    ("heading", "anchor"),
    [
        ("Key expiry modes", "key-expiry-modes"),
        ("`LLMQ_KEY_EXPIRY_MODE` and the index", "llmq_key_expiry_mode-and-the-index"),
        ("Upgrading from 0.1.0b2", "upgrading-from-010b2"),
        ("[Linked](x.md) title", "linked-title"),
    ],
)
def test_slug_follows_the_docs_build(heading: str, anchor: str) -> None:
    assert check_docs.slug(heading) == anchor


def test_docs_check_finds_broken_links_anchors_and_nav(tmp_path: Path) -> None:
    """A missing page, a missing anchor and a missing nav page are each reported once."""
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "a.md").write_text(
        "# Title\n\n## Has an id {#custom}\n\n```md\n[not a link](gone.md)\n```\n"
        "[ok](b.md#second) [ok id](#custom) [web](https://example.com)\n"
        "[bad page](missing.md) [bad anchor](b.md#nope)\n",
        encoding="utf-8",
    )
    (docs / "b.md").write_text("# First\n\n## Second\n", encoding="utf-8")
    (tmp_path / "README.md").write_text("[docs](docs/a.md#title)\n", encoding="utf-8")
    (tmp_path / "zensical.toml").write_text(
        '[project]\nnav = [{"A" = "a.md"}, {"Group" = [{"Gone" = "gone.md"}]}]\n',
        encoding="utf-8",
    )
    found = check_docs.problems(tmp_path)
    assert len(found) == 3, found
    assert any("missing missing.md" in f for f in found)
    assert any("no anchor #nope" in f for f in found)
    assert any("nav: missing docs/gone.md" in f for f in found)


def test_the_repo_docs_pass() -> None:
    """The real docs have no broken link, anchor or nav entry."""
    assert check_docs.problems() == []


def test_release_watch_reads_the_github_and_pypi_shapes(monkeypatch: pytest.MonkeyPatch) -> None:
    runs = {
        "workflow_runs": [
            {"name": "ci", "head_branch": "v0.1.1"},
            {"name": "publish", "head_branch": "v0.1.0b3", "status": "completed"},
            {"name": "publish", "head_branch": "v0.1.1", "status": "in_progress"},
        ]
    }
    assert watch_release.find_run(runs, "v0.1.1")["status"] == "in_progress"
    assert watch_release.find_run(runs, "v9") is None
    jobs = {"jobs": [{"name": "verify", "conclusion": "success"}, {"name": "pypi", "status": "q"}]}
    assert [line.split() for line in watch_release.job_lines(jobs)] == [
        ["success", "verify"],
        ["q", "pypi"],
    ]
    assert watch_release.on_pypi({"releases": {"0.1.1": [{"url": "x"}]}}, "0.1.1")
    assert not watch_release.on_pypi({"releases": {"0.1.1": []}}, "0.1.1")

    def remote(url: str) -> Any:
        def run(*_: Any, **__: Any) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess([], 0, stdout=url + "\n")

        return run

    for url in ("https://github.com/owner/name.git", "git@github.com:owner/name.git"):
        monkeypatch.setattr(watch_release.subprocess, "run", remote(url))
        assert watch_release.repo_from_git() == "owner/name"


def test_smoke_install_builds_the_right_requirement(tmp_path: Path) -> None:
    assert smoke_install.requirement("0.1.1", "langchain-aws") == (
        "llm-queue[langchain-aws]==0.1.1",
        "0.1.1",
    )
    assert smoke_install.requirement("latest", "") == ("llm-queue", "")
    wheel = tmp_path / "llm_queue-0.1.1-py3-none-any.whl"
    req, version = smoke_install.requirement(str(wheel), "")
    assert req.startswith("llm-queue @ file:")
    assert version == "0.1.1"
    venv, install = smoke_install.commands(tmp_path / "v", "llm-queue==0.1.1", python="3.12",
                                           testpypi=True)  # fmt: skip
    assert venv[:3] == ["uv", "venv", "-q"]
    assert "llm-queue==0.1.1" in install
    assert "https://test.pypi.org/simple/" in install


def test_keys_page_embeds_the_rows_safely() -> None:
    """The version shows; a `</script>` in a field cannot end the page's script early."""
    rows = [{"group": "G", "key": "P:x", "schema": "a </script> b"}]
    page = store_keys_page.build(rows, "9.9.9")
    assert "llm-queue 9.9.9" in page
    assert "a </script> b" not in page
    assert r"a <\/script> b" in page
