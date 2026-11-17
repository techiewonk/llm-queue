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
* the Python surfaces (owner Q2): every `InfraSettings` field follows its variable
  (`auto_purge` -> `purge_auto_enabled`, `mode` -> `role`, `worker_id` -> `instance_id`,
  `default_tenant` -> `tenant_default`, `secret_names` -> `secrets_names`, ...), with
  `from_env(worker_id=)`, the `describe()` keys and the `GET /` keys (`field_rules`, `PY_RULES`);
* lower-case forms (`llmq_key_expiry` in page anchors and slugs) and short names in a test's
  `match="..."`;

then moves `PENDING_RENAMES` into `RENAMED` (so an old name is reported at start) and
regenerates every `rename-table` block. A rule that matched nothing is printed (the code moved:
check by hand). Afterwards run `ruff format .` and `ruff check .`: the longer names push a few
comment and message lines past 100 characters (8 on the trial run of 2026-11-18, fixed by hand),
then the unit tests (the trial run passed them all) and `scripts/check_docs.py`.
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
SETTINGS_PY = ("src/llm_queue/engine/settings.py",)
# InfraSettings fields follow their variable (`LLMQ_AUTO_PURGE` -> `auto_purge`), and
# test_config_docs reads each documented variable as the field of that name, so every field is
# renamed with its variable. Most names mean only the setting and are renamed as whole words
# wherever they appear (`field_rules`); these three are common words or also a message field
# (`Meta.queue_wait_ms` is the wait a call had) and get the targeted rules below.
TARGETED_FIELDS = frozenset({"mode", "worker_id", "queue_wait_ms"})
# (files or prefixes, pattern, replacement): applied in order, after the environment names.
PY_RULES: tuple[tuple[tuple[str, ...], str, str], ...] = (
    # InfraSettings fields and the constructor call in from_layers.
    (SETTINGS_PY, r'(?m)^(\s+)mode: str = "both"', r'\1role: str = "both"'),
    (SETTINGS_PY, r'(?m)^(\s+)worker_id: str = ""', r'\1instance_id: str = ""'),
    (SETTINGS_PY, r"(?m)^(\s+)queue_wait_ms: int = 100$", r"\1replica_wait_ms: int = 100"),
    (SETTINGS_PY, r"(?m)^(\s+)mode=mode,$", r"\1role=mode,"),
    (SETTINGS_PY, r"(?m)^(\s+)worker_id=worker_id,$", r"\1instance_id=worker_id,"),
    (SETTINGS_PY, r"\bqueue_wait_ms=number\(", "replica_wait_ms=number("),
    # The engine's own settings carry the replica wait under the same name.
    (
        ("src/llm_queue/engine/runtime.py",),
        r"(?m)^(\s+)queue_wait_ms: int = 100  #",
        r"\1replica_wait_ms: int = 100  #",
    ),
    ((*CODE, "docs/"), r"\b(settings|s|set_)\.mode\b", r"\1.role"),
    ((*CODE, "docs/"), r"\b(settings|s|set_)\.worker_id\b", r"\1.instance_id"),
    ((*CODE, "docs/"), r"\bsettings\.queue_wait_ms\b", "settings.replica_wait_ms"),
    (
        CODE,
        r"\bqueue_wait_ms=settings\.replica_wait_ms",
        "replica_wait_ms=settings.replica_wait_ms",
    ),
    (("src/llm_queue/tuning.py",), r"\bqueue_wait_ms\b", "replica_wait_ms"),
    (CODE, r"\bproblems\(queue_wait_ms=", "problems(replica_wait_ms="),
    # Test doubles of the settings (`SimpleNamespace(mode="caller")`, engine settings).
    (("tests/",), r"\bSimpleNamespace\(mode=", "SimpleNamespace(role="),
    (("tests/",), r"(replica_wait_count=\d+, )queue_wait_ms=", r"\1replica_wait_ms="),
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
    (("tests/", "docs/"), r'"worker_id":', '"instance_id":'),  # describe()'s key in tests
    (ADMIN, r'"worker": engine\.worker_id', '"instance": engine.worker_id'),
    (ADMIN, r'"mode": settings\.role', '"role": settings.role'),
    # describe()'s tenant key (W5a, review B-04) follows `tenant_default`.
    ((*CODE, "docs/", "CHANGELOG.md"), r"\bdefault_tenant_hash\b", "tenant_default_hash"),
)
# What --check still reports as an old Python name once the rules ran.
PY_LEFT = re.compile(r"\b(?:settings|s)\.(?:mode|worker_id|queue_wait_ms)\b")


def field_rules(pending: dict[str, str]) -> list[tuple[tuple[str, ...], str, str]]:
    """Whole-word renames of the InfraSettings fields of `pending` (not `TARGETED_FIELDS`)."""
    rules = []
    for old, new in pending.items():
        field, target = old[len("LLMQ_") :].lower(), new[len("LLMQ_") :].lower()
        if field not in TARGETED_FIELDS:
            where = (*CODE, "docs/", "README.md", "CHANGELOG.md")  # CHANGELOG: [Unreleased] only
            rules.append((where, rf"(?<!\w){field}(?!\w)", target))
    return rules


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
    rules = [*field_rules(pending), *PY_RULES]  # fields first: the test rules expect new ones
    lower = {old.lower(): new.lower() for old, new in pending.items()}  # page anchors, slugs
    lower_re = re.compile(r"(?<!\w)(" + "|".join(sorted(lower, key=len, reverse=True)) + r")(?!\w)")
    used = [0] * len(rules)
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
            if short_map and rel.startswith("tests/"):  # `match="DEBUG_CAPTURE_FROM_CONFIG ..."`
                part = re.sub(
                    r"""(match=["'])(""" + "|".join(short_map) + r")(?!\w)",
                    lambda m: m.group(1) + short_map[m.group(2)],
                    part,
                )
            if lower:
                part = lower_re.sub(lambda m: lower[m.group(1)], part)
            for i, (where, pattern, repl) in enumerate(rules):
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
                print("rule matched nothing (check by hand):", rules[i][1])
    return 0


if __name__ == "__main__":
    sys.exit(main())
