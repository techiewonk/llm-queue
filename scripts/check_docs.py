"""Check the docs: every relative link and anchor resolves, and every nav page exists.

Scans `README.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, `STABILITY.md` and `docs/**/*.md`
(`docs/internal/` is git-ignored and skipped). Anchors follow the docs build's default slugs
(lower case, punctuation dropped, spaces to `-`) plus explicit `{#id}` attributes.

    python scripts/check_docs.py            # exit 1 and a list when something is broken
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TOP = ("README.md", "CHANGELOG.md", "CONTRIBUTING.md", "STABILITY.md")
_LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
_HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*(?:\{#([\w-]+)\})?\s*$")
_FENCE = re.compile(r"^\s*(```|~~~)")


def slug(text: str) -> str:
    """The anchor of a heading: markdown and punctuation dropped, lower case, spaces to `-`."""
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[\s]+", "-", text)


def anchors(path: Path) -> set[str]:
    """Every anchor a page has (headings outside code fences, explicit ids)."""
    out: set[str] = set()
    fenced = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if _FENCE.match(line):
            fenced = not fenced
            continue
        match = None if fenced else _HEADING.match(line)
        if match:
            out.add(match.group(3) or slug(match.group(2)))
    return out


def links(path: Path) -> list[tuple[int, str]]:
    """(line number, target) of every markdown link outside code fences."""
    out: list[tuple[int, str]] = []
    fenced = False
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if _FENCE.match(line):
            fenced = not fenced
            continue
        if not fenced:
            out += [(number, target) for target in _LINK.findall(re.sub(r"`[^`]*`", "", line))]
    return out


def problems(root: Path = ROOT) -> list[str]:
    """Broken links, anchors and nav entries under `root`."""
    pages = [root / name for name in TOP if (root / name).exists()]
    pages += [p for p in (root / "docs").rglob("*.md") if "internal" not in p.parts]
    cache: dict[Path, set[str]] = {}
    found: list[str] = []
    for page in pages:
        for number, target in links(page):
            if re.match(r"^[a-z][a-z0-9+.-]*:", target) or target.startswith("//"):
                continue  # http(s), mailto: checked by hand, not here
            path_part, _, anchor = target.partition("#")
            dest = (page.parent / path_part).resolve() if path_part else page
            where = f"{page.relative_to(root)}:{number}"
            if not dest.exists():
                found.append(f"{where}: missing {path_part}")
                continue
            if anchor and dest.suffix == ".md":
                have = cache.setdefault(dest, anchors(dest))
                if anchor not in have:
                    found.append(f"{where}: no anchor #{anchor} in {dest.relative_to(root)}")
    nav_file = root / "zensical.toml"
    if nav_file.exists():
        found += [f"zensical.toml nav: missing docs/{p}" for p in nav_pages(nav_file)
                  if not (root / "docs" / p).exists()]  # fmt: skip
    return found


def nav_pages(nav_file: Path) -> list[str]:
    """Every page path in the docs nav (`project.nav`, nested lists and tables)."""
    doc = tomllib.loads(nav_file.read_text(encoding="utf-8"))
    out: list[str] = []

    def walk(node: object) -> None:
        if isinstance(node, str) and node.endswith(".md"):
            out.append(node)
        elif isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk((doc.get("project") or {}).get("nav") or doc.get("nav") or [])
    return out


def main() -> int:
    """Print every problem; exit 1 when there is one."""
    found = problems()
    for line in found:
        print(line)
    print("docs: OK" if not found else f"docs: {len(found)} problem(s)")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
