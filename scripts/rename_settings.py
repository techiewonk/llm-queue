"""Apply the `LLMQ_*` names standard (0.2.0 FU4) from the one table in `engine/settings.py`.

`PENDING_RENAMES` there is the only list: this script reads it (no import, an AST parse), so the
code's renamed-names check and the rewrite cannot disagree. Run it from the library root once
every other branch has merged:

    python scripts/rename_settings.py            # apply
    python scripts/rename_settings.py --check    # list what is left to rename; exit 1 if any
    python scripts/rename_settings.py --tables   # only regenerate the rename tables in docs

Apply rewrites, everywhere but the history (released CHANGELOG sections, earlier upgrade and
what's-new sections, `docs/internal`):

* every full name (`LLMQ_AUTO_PURGE`) in code, tests, docs, samples, manifests and CI;
* every short name read in `src` as a whole string literal (`get("AUTO_PURGE"`,
  `number("TTL_SWEEP_BATCH", ...)`, `read_bool(layers, "ADMIN_DLQ_BODIES_ENABLED"`, the purge
  tuples, `_SECRETS_SETTINGS`), which a call-pattern regex missed (review B-01);
* the Python surfaces (owner Q2: `InfraSettings.mode` -> `role`, `.worker_id` ->
  `.instance_id`, `.default_tenant` -> `.tenant_default`, `from_env(worker_id=)`, the
  `describe()` and `GET /` keys), from `PY_RULES` below;

then moves `PENDING_RENAMES` into `RENAMED` (so an old name is reported at start) and
regenerates every `rename-table` block. Afterwards run the unit tests: a rule of `PY_RULES` that
matched nothing is printed, and tests that build `InfraSettings` by keyword may need the new
field names.
"""

from __future__ import annotations

import ast
import re
import sys
from collections.abc import Callable, Iterator
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SETTINGS = ROOT / "src" / "llm_queue" / "engine" / "settings.py"
ROOTS = (
    "src",
    "tests",
    "docs",
    "samples",
    "scripts",
    "tools",
    "docker",
    ".github",
    "README.md",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "STABILITY.md",
    "SECURITY.md",
    "Makefile",
)
SUFFIXES = {".py", ".md", ".yml", ".yaml", ".json", ".toml", ".lua", ".txt", ".env", ".sh", ""}
SKIP = ("docs/internal/", "__pycache__/", ".venv/", "scripts/rename_settings.py")
# Pages with history: only their first `## ` section (the unreleased one) is current.
HISTORY = ("CHANGELOG.md", "docs/how-to/upgrade.md", "docs/whats-new.md")
BEGIN, END = "rename-table: begin", "rename-table: end"

ADMIN = ("src/llm_queue/engine/admin/process.py", "src/llm_queue/engine/admin/local.py")
CODE = ("src/", "tests/", "samples/", "scripts/")
# (files or prefixes, pattern, replacement): applied in order, after the environment names.
PY_RULES: tuple[tuple[tuple[str, ...], str, str], ...] = (
    # InfraSettings fields and the constructor call in from_layers.
    (
        ("src/llm_queue/engine/settings.py",),
        r'(?m)^(\s+)mode: str = "both"',
        r'\1role: str = "both"',
    ),
    (
        ("src/llm_queue/engine/settings.py",),
        r'(?m)^(\s+)worker_id: str = ""',
        r'\1instance_id: str = ""',
    ),
    (("src/llm_queue/engine/settings.py",), r"(?m)^(\s+)mode=mode,$", r"\1role=mode,"),
    (
        ("src/llm_queue/engine/settings.py",),
        r"(?m)^(\s+)worker_id=worker_id,$",
        r"\1instance_id=worker_id,",
    ),
    # Reads of those fields.
    (CODE, r"\b(settings|s)\.mode\b", r"\1.role"),
    (CODE, r"\b(settings|s)\.worker_id\b", r"\1.instance_id"),
    ((*CODE, "docs/"), r"\bdefault_tenant\b", "tenant_default"),
    # LLMQueue.from_env(worker_id=) -> instance_id=.
    (
        ("src/llm_queue/core.py",),
        r"(?m)^(\s+)worker_id: str \| None = None,\n(\s+credentials:)",
        r"\1instance_id: str | None = None,\n\2",
    ),
    (
        ("src/llm_queue/core.py",),
        r"_from_env_local\(settings, layers, metrics, worker_id,",
        "_from_env_local(settings, layers, metrics, instance_id,",
    ),
    (
        ("src/llm_queue/core.py",),
        r"(never share one\.\n\s+)worker_id = worker_id or",
        r"\1worker_id = instance_id or",
    ),
    (("docs/", "README.md"), r"`worker_id=`", "`instance_id=`"),
    # describe() and GET / keys.
    (
        ("src/llm_queue/engine/admin/process.py",),
        r'"worker_id": worker_id',
        '"instance_id": worker_id',
    ),
    (("src/llm_queue/engine/admin/process.py",), r"\bmode=s\.role\b", "role=s.role"),
    (ADMIN, r'"worker": engine\.worker_id', '"instance": engine.worker_id'),
    (ADMIN, r'"mode": settings\.role', '"role": settings.role'),
)
# What --check still reports as an old Python name once the rules ran.
PY_LEFT = re.compile(r"\b(?:settings|s)\.(?:mode|worker_id|default_tenant)\b|\bdefault_tenant\b")


def tables() -> tuple[dict[str, str], dict[str, str]]:
    """`RENAMED` and `PENDING_RENAMES` from settings.py, read without importing it."""
    found: dict[str, dict[str, str]] = {}
    for node in ast.parse(SETTINGS.read_text(encoding="utf-8")).body:
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id in ("RENAMED", "PENDING_RENAMES")
            and node.value is not None
        ):
            found[node.target.id] = ast.literal_eval(node.value)
    return found["RENAMED"], found["PENDING_RENAMES"]


def files() -> Iterator[Path]:
    """Every text file the rename covers."""
    for name in ROOTS:
        base = ROOT / name
        for path in [base] if base.is_file() else sorted(base.rglob("*")):
            rel = path.relative_to(ROOT).as_posix()
            if path.is_file() and path.suffix in SUFFIXES and not any(s in rel for s in SKIP):
                yield path


def split_history(rel: str, text: str) -> tuple[str, str]:
    """(current part, history part) of a page; the whole text is current for other files."""
    if rel not in HISTORY:
        return text, ""
    heads = [m.start() for m in re.finditer(r"(?m)^## ", text)]
    return (text, "") if len(heads) < 2 else (text[: heads[1]], text[heads[1] :])


def outside_tables(text: str, change: Callable[[str], str]) -> str:
    """`change` applied to `text` except inside `rename-table` blocks."""
    out, rest = [], text
    while BEGIN in rest:
        start = rest.index(BEGIN)
        start = rest.rfind("\n", 0, start) + 1
        end = rest.index("\n", rest.index(END, start)) + 1
        out += [change(rest[:start]), rest[start:end]]
        rest = rest[end:]
    return "".join([*out, change(rest)])


def render(kind: str, renamed: dict[str, str], pending: dict[str, str]) -> str:
    """The body of one `rename-table` block."""
    if kind == "py":
        lines = ["RENAMED: dict[str, str] = {"]
        lines += [f'    "{old}": "{new}",' for old, new in renamed.items()]
        lines += ["}"]
        if pending:
            lines += ["PENDING_RENAMES: dict[str, str] = {"]
            lines += [f'    "{old}": "{new}",' for old, new in pending.items()]
            lines += ["}"]
        else:
            lines += ["PENDING_RENAMES: dict[str, str] = {}"]
        return "\n".join(lines) + "\n"
    if kind == "table":
        rows = ["| Old name | New name |", "|---|---|"]
        rows += [f"| `{old}` | `{new}` |" for old, new in renamed.items()]
        return "\n".join(rows) + "\n"
    return "".join(f"- `{old}`: renamed `{new}`.\n" for old, new in renamed.items())


def regenerate(text: str, renamed: dict[str, str], pending: dict[str, str]) -> str:
    """Every `rename-table` block rewritten from the tables (`py`, `table` or `list`)."""
    pattern = re.compile(rf"(?ms)^([^\n]*{BEGIN}[ \t]*(\w*)[^\n]*\n)(.*?)(^[^\n]*{END})")

    def block(m: re.Match[str]) -> str:
        kind = m.group(2) or "py"
        return m.group(1) + render(kind, renamed, pending) + m.group(4)

    return pattern.sub(block, text)


def main() -> int:
    """Apply, check or regenerate (see the module docstring)."""
    check, only_tables = "--check" in sys.argv, "--tables" in sys.argv
    renamed, pending = tables()
    if not pending and not check and not only_tables:
        print("nothing pending in PENDING_RENAMES")
    full = (
        re.compile(
            r"\b(" + "|".join(sorted(map(re.escape, pending), key=len, reverse=True)) + r")\b"
        )
        if pending
        else None
    )
    short_map = {old[5:]: new[5:] for old, new in pending.items()}
    short = (
        re.compile(r"""(["'])(""" + "|".join(sorted(short_map, key=len, reverse=True)) + r""")\1""")
        if short_map
        else None
    )
    merged = {old: pending.get(new, new) for old, new in renamed.items()} | pending
    used = [0] * len(PY_RULES)
    left = 0
    for path in files():
        rel = path.relative_to(ROOT).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        current, history = split_history(rel, text)

        def rename(part: str, rel: str = rel) -> str:
            if full is not None:
                part = full.sub(lambda m: pending[m.group(1)], part)
            if short is not None and rel.startswith("src/") and rel.endswith(".py"):
                part = short.sub(lambda m: m.group(1) + short_map[m.group(2)] + m.group(1), part)
            for i, (where, pattern, repl) in enumerate(PY_RULES):
                if any(rel == w or (w.endswith("/") and rel.startswith(w)) for w in where):
                    part, n = re.subn(pattern, repl, part)
                    used[i] += n
            return part

        if check:
            new = outside_tables(current, rename)
            if new != current or (rel.startswith("src/") and PY_LEFT.search(current)):
                left += 1
                print("old names in", rel)
            continue
        new = current if only_tables else outside_tables(current, rename)
        new = regenerate(
            new, merged if not only_tables else renamed, {} if not only_tables else pending
        )
        if new != current:
            path.write_text(new + history, encoding="utf-8", newline="\n")
            print("rewrote", rel)
    if check:
        print(f"files with old names: {left}")
        return 1 if left else 0
    if not only_tables:
        for i, n in enumerate(used):
            if not n:
                print("PY_RULES rule matched nothing (check by hand):", PY_RULES[i][1])
    return 0


if __name__ == "__main__":
    sys.exit(main())
