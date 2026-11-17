"""Run the checks of one work step (plan §6) or of everything: lint, format, types, tests.

Every test run of the 0.3 work goes through this script, so a step is "done" only when
`python scripts/check.py <step>` passes. No step calls a real LLM: stack tests and samples
use fakellm (`python scripts/samples.py --live` runs the samples on real Bedrock;
`--store env` on the env file's store). `store` also runs a Docker failover drill.

    python scripts/check.py a1          # one step
    python scripts/check.py a1 a2       # several
    python scripts/check.py all --quick # dev run (~30 min): lint once, tests once, scenarios
                                        # `at` 3 at a time, gates without the slow three
    python scripts/check.py all         # before a push (~45 min): adds the burst scenarios
                                        # and every gate
    python scripts/check.py all --release  # before a release: every scenario x every load
    python scripts/check.py soak3       # soaks (not in `all`): 3 min smoke, soak10, soak20
    python scripts/check.py soak20      # before a release
    python scripts/check.py evict       # eviction drill (not in `all`, needs Docker), ~15 min
    python scripts/check.py --list      # what each step runs

`all` lints and type-checks the union of every step's paths once, runs the union of the unit
tests and of the stack tests once each (a test file named by several steps ran up to five
times), then each step's extra script (FT-1). A named step keeps its own lint, types and tests.
Every check has a time limit (`Step.timeout_secs`, `LINT_SECS`, `TEST_SECS`): a hung drill
fails its check instead of holding the run for ever (FT-7).

Every check writes its whole output, as it runs, to `reports/check-<time>/<step>.<check>.log`
(git-ignored); the console shows one PASS / FAIL line per check with its log, and
`summary.log` there keeps the console. Follow a long drill with `tail -f` on its log.
"""

from __future__ import annotations

import argparse
import datetime
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
_VENV_PY = ROOT / ".venv" / ("Scripts" if os.name == "nt" else "bin") / "python"
# The project venv, else the interpreter running this script.
_FOUND = _VENV_PY.with_suffix(".exe" if os.name == "nt" else "").exists()
PY = str(_VENV_PY) if _FOUND else sys.executable
LINT_SECS = 900.0  # one ruff or mypy run (a cold mypy over everything: ~1 min)
TEST_SECS = 2400.0  # one pytest run (the stack tests: ~7 min measured)


@dataclass(frozen=True)
class Step:
    """What one step checks: source paths (lint and types) and test paths (pytest)."""

    title: str
    sources: tuple[str, ...]
    tests: tuple[str, ...]
    needs_stack: bool = False
    extra: tuple[tuple[str, ...], ...] = field(default=())
    quick: tuple[str, ...] = ()  # arguments added to `extra` commands with --quick
    full: tuple[str, ...] = ()  # arguments added to `extra` commands without --quick
    release: tuple[str, ...] | None = None  # with --release instead of `full` (None: `full`)
    in_all: bool = True
    gating: bool = True  # False: the step reports its numbers but never fails the run
    timeout_secs: float = 1800  # per extra command; a hung drill fails instead of hanging (FT-7)


STEPS: dict[str, Step] = {
    "a0": Step(
        "Test prerequisites: fakellm accounts, TPM, speed, usage-missing, hang, streaming",
        sources=("tools/fakellm/fakellm", "scripts"),
        tests=("tests/unit/fakellm",),
    ),
    "unit": Step(
        "Every unit test (no stack): settings, validation, HTTP, front door, streaming, tuning",
        sources=("src/llm_queue",),
        tests=("tests/unit",),
    ),
    "tools": Step(
        "Test tooling: provider error table, budget properties, 200-way contention",
        sources=("tests/unit/providers", "scripts"),
        tests=(
            "tests/unit/providers",
            "tests/unit/engine/test_budget_properties.py",
            "tests/integration/engine/test_contention.py",
        ),
        needs_stack=True,
    ),
    "a1": Step(
        "Registry schema, settings, validation, resolve",
        sources=("src/llm_queue/registry",),
        tests=("tests/unit/registry",),
    ),
    "a2": Step(
        "Registry store: version rule, live edits, audit, rollback, reload",
        sources=("src/llm_queue/registry",),
        tests=("tests/integration/registry",),
        needs_stack=True,
    ),
    "a3": Step(
        "Clients per account and region; providers on routes; attempt timers",
        sources=("src/llm_queue/clients",),
        tests=("tests/unit/clients",),
    ),
    "a4": Step(
        "Engine keys (schema 3), message format v2, claim / result / status records",
        sources=("src/llm_queue/engine",),
        tests=("tests/integration/engine/test_requests.py",),
        needs_stack=True,
    ),
    "a5": Step(
        "Stream queue: order, return in place, failures, dead letters, waiting limit",
        sources=("src/llm_queue/engine",),
        tests=("tests/integration/engine/test_queue.py",),
        needs_stack=True,
    ),
    "a6": Step(
        "Admission: PICK_TAKE, FINISH, UNDO, RENEW, SWEEP (budgets, slots, waiters, strategies)",
        sources=("src/llm_queue/engine", "src/llm_queue/registry"),
        tests=("tests/integration/engine/test_admission.py",),
        needs_stack=True,
    ),
    "a7": Step(
        "Token estimate and refund rules; inference defaults and limits per provider",
        sources=(
            "src/llm_queue/engine/tokens.py",
            "src/llm_queue/engine/inference.py",
        ),
        tests=("tests/unit/engine",),
    ),
    "a8": Step(
        "Engine end to end: caller, worker loop, held calls, retries, fallback (fakellm)",
        sources=("src/llm_queue/engine",),
        tests=("tests/integration/engine/test_runtime.py",),
        needs_stack=True,
    ),
    "a9": Step(
        "Hooks (balancer, fallback policy, admission, sticky key), fallback by cause, params",
        sources=("src/llm_queue/engine",),
        tests=("tests/unit/engine", "tests/integration/engine/test_runtime.py"),
        needs_stack=True,
    ),
    "a10": Step(
        "Operations: direct path, store_down, cancel, caller gone, shutdown, store reset",
        sources=("src/llm_queue/engine", "src/llm_queue/clients"),
        tests=("tests/integration/engine",),
        needs_stack=True,
    ),
    "a11": Step(
        "Health: auto-disable with safeguards, probes, credentials per process",
        sources=("src/llm_queue/engine", "tools/fakellm/fakellm"),
        tests=("tests/integration/engine/test_runtime.py",),
        needs_stack=True,
    ),
    "a12": Step(
        "Status, health, alerts, metrics, call record, drain, HTTP handlers, CLI, settings",
        sources=("src/llm_queue/engine",),
        tests=("tests/integration/engine/test_ops.py",),
        needs_stack=True,
    ),
    "a13": Step(
        "Embeddings: batching, provider bodies, vectors in order, large results in S3",
        sources=("src/llm_queue/engine",),
        tests=("tests/integration/engine/test_embed.py",),
        needs_stack=True,
    ),
    "a14": Step(
        "Front door (LLMQueue.from_env), jobs, sync calls, LangChain adapters",
        sources=(
            "src/llm_queue/core.py",
            "src/llm_queue/langchain",
            "src/llm_queue/http_transport.py",
            "src/llm_queue/engine/jobs.py",
            "src/llm_queue/tuning.py",
        ),
        tests=("tests/integration/engine/test_front.py", "tests/unit/test_tuning.py"),
        needs_stack=True,
    ),
    "a15": Step(
        "Assume-role accounts: STS AssumeRole per account, refresh, credential failures",
        sources=("src/llm_queue/clients", "src/llm_queue/engine"),
        tests=(
            "tests/unit/clients/test_assume_role.py",
            "tests/integration/engine/test_runtime.py::test_assume_role_failure_is_a_credential_failure",
            "tests/integration/engine/test_runtime.py::test_credential_failure_is_per_process",
        ),
        needs_stack=True,
    ),
    "a16": Step(
        "Latency-aware exclusion: slow deployments become last choices",
        sources=("src/llm_queue/engine",),
        tests=("tests/integration/engine/test_runtime.py",),
        needs_stack=True,
    ),
    "a17": Step(
        "Streaming to the caller: chunk streams, flush window, cuts, worker lost, idle",
        sources=(
            "src/llm_queue/engine",
            "src/llm_queue/clients",
            "src/llm_queue/core.py",
            "tools/fakellm/fakellm",
        ),
        tests=("tests/integration/engine/test_stream.py",),
        needs_stack=True,
    ),
    "store": Step(
        "Store failover: READONLY reconnect, read-only readiness, preStop pause, HTTP router",
        sources=(
            "src/llm_queue/engine",
            "src/llm_queue/core.py",
            "scripts/failover.py",
        ),
        tests=("tests/integration/engine/test_store_and_http.py",),
        needs_stack=True,
        extra=(("scripts/failover.py",),),
    ),
    "load": Step(
        "Load drill: 2,000 calls over three workers; a burst far over the RPM budget",
        sources=("scripts/load.py",),
        tests=(),
        needs_stack=True,
        extra=(("scripts/load.py",),),
    ),
    "gates": Step(
        "Plan gates B17-B97 on worker processes: budgets, backlog, kills, store outages (~20 min)",
        sources=("scripts/gates.py", "scripts/gates_fleet.py"),
        tests=(),
        needs_stack=True,
        extra=(("scripts/gates.py",),),
        quick=("--quick",),
        timeout_secs=3600,  # ~20 min measured
    ),
    "scenarios": Step(
        "Fault scenarios (store, worker, provider, registry, caller, pairs) x loads, by calls",
        sources=("scripts/scenarios.py",),
        tests=(),
        needs_stack=True,
        # The Docker failover drills (S6 / S7 / S11) run once, in step `store` (FT-14).
        # `under` and `at` run 3 at a time (each unit is its own process, its own fakellm and
        # store proxy); `burst` runs one at a time inside scenarios.py (FT-2).
        extra=(("scripts/scenarios.py", "all", "--jobs", "3", "--exclude", "S6", "S7", "S11"),),
        quick=("--load", "at"),  # dev: every scenario at 100 % of the budget
        full=("--matrix", "default"),  # every scenario `at`, plus `burst` where load matters
        release=("--matrix", "full"),  # every scenario x every load
        timeout_secs=5400,
    ),
    "samples": Step(
        "Usage samples (samples/*.py) run as app code against fakellm",
        sources=("samples", "scripts/samples.py"),
        tests=(),
        needs_stack=True,
        extra=(("scripts/samples.py",),),
    ),
    "bench": Step(
        "Benchmark (start, first call, steady and stream overhead); numbers only, never fails",
        sources=("scripts/bench.py",),
        tests=(),
        needs_stack=True,
        extra=(("scripts/bench.py",),),
        quick=("--reps", "2", "--idle", "10"),
        in_all=False,  # numbers only (104 s); run on purpose (FT-14)
        gating=False,
    ),
    "evict": Step(
        "Eviction drill: a full evicting store (volatile-lru, noeviction) under queue traffic",
        sources=("scripts/evict_drill.py",),
        tests=(),
        extra=(("scripts/evict_drill.py",),),  # its own Valkey in Docker and its own fakellm
        quick=("--calls", "500"),
        in_all=False,  # ~15 min for both policies; run on purpose (and after expiry changes)
        timeout_secs=2700,
    ),
    **{
        f"soak{minutes}": Step(
            f"Soak {minutes} min ({use}): 8 worker processes, chaos (kill, store block, "
            "live edit, restart); leaks, drift",
            sources=("scripts/soak.py",),
            tests=(),
            needs_stack=True,
            extra=(("scripts/soak.py", "--minutes", str(minutes)),),
            in_all=False,  # long; run on purpose
            timeout_secs=minutes * 60 * 2 + 900,
        )
        for minutes, use in ((3, "smoke"), (10, "after an engine change"), (20, "release"))
    },
}

LOG_DIR = ROOT / "reports" / f"check-{datetime.datetime.now():%Y%m%d-%H%M%S}"


def say(line: str) -> None:
    """Print a console line and keep it in the run's `summary.log`."""
    print(line, flush=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with (LOG_DIR / "summary.log").open("a", encoding="utf-8") as summary:
        summary.write(line + "\n")


def stack_up() -> str | None:
    """None when the Docker stack answers (Valkey, Redis, fakellm); otherwise why not.

    Also resets a store memory limit a killed S9 drill left behind (FT-19), and says so.
    """
    import importlib
    import socket

    for name, port in (("Valkey", 6379), ("Redis", 6380), ("fakellm", 8080)):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                pass
        except OSError as exc:
            return f"{name} on 127.0.0.1:{port} is not reachable ({exc}); run `make up`"
    if str(ROOT / "scripts") not in sys.path:
        sys.path.insert(0, str(ROOT / "scripts"))
    stack: Any = importlib.import_module("stack")
    for line in [*stack.repair_memory_limit(), *stack.sweep_stale_drill_keys()]:
        say(f"  REPAIRED  {line}")
    return None


def _run(step: str, label: str, cmd: list[str], *, timeout_secs: float = LINT_SECS) -> bool:
    """Run one check with its output written live to its own log; print PASS / FAIL.

    A check still running after `timeout_secs` is killed and fails (FT-7); its worker
    processes end on their stdin closing.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = LOG_DIR / f"{step}.{re.sub(r'[^A-Za-z0-9_.-]+', '_', label).strip('_')}.log"
    env = {
        **os.environ,
        "PYTHONUNBUFFERED": "1",  # the log fills as the check runs
        "LLMQ_DRILL_LOG_DIR": str(LOG_DIR / f"{step}-processes"),  # workers' own logs
    }
    started = time.monotonic()
    timed_out = False
    with log.open("w", encoding="utf-8") as out_file:
        out_file.write(f"$ {' '.join(cmd)}\n\n")
        out_file.flush()
        try:
            proc = subprocess.run(
                cmd,
                cwd=ROOT,
                stdout=out_file,
                stderr=subprocess.STDOUT,
                env=env,
                check=False,
                timeout=timeout_secs,
            )
            ok = proc.returncode == 0
        except subprocess.TimeoutExpired:
            timed_out, ok = True, False
            out_file.write(f"\nFAIL  timed out after {timeout_secs:.0f}s (killed)\n")
    secs = time.monotonic() - started
    note = " TIMED OUT" if timed_out else ""
    say(f"  {'PASS' if ok else 'FAIL'}  {label} ({secs:.1f}s){note}  {log.relative_to(ROOT)}")
    if not ok:
        out = log.read_text(encoding="utf-8", errors="replace").rstrip()
        # Verdict lines first (a drill's log noise must not push them out of the tail).
        verdicts = [
            line
            for line in out.splitlines()
            if line.startswith(("[", "PASS", "FAIL", "ALL PASS"))
            or (line.startswith("  ") and not line.startswith("   ") and "File " not in line)
        ]
        if verdicts:
            say("\n".join(verdicts[-80:]))
        say(out[-3000:])
    return ok


def _lint(name: str, paths: list[str]) -> list[bool]:
    """Ruff, ruff format, mypy over `paths` (mypy's cache is per checkout), then the docs' links."""
    return [
        _run(name, "ruff check", [PY, "-m", "ruff", "check", *paths]),
        _run(name, "ruff format --check", [PY, "-m", "ruff", "format", "--check", *paths]),
        _run(name, "mypy", [PY, "-m", "mypy", *paths]),
        _run(name, "docs links", [PY, str(ROOT / "scripts" / "check_docs.py")]),
    ]


def _pytest(name: str, tests: list[str], *, cov: str = "") -> bool:
    """One pytest run; `cov` "new" starts a coverage data file, "append" adds to it."""
    # -rA: a line per test with its outcome, so the log shows what ran, not only failures.
    cmd = [PY, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-rA"]
    if cov:
        # Each run only collects: pytest-cov would judge `fail_under` on the unit run alone
        # (69 % of 89 %); the combined `coverage report` after both runs is the judge.
        cmd += ["--cov", "--cov-report=", "--cov-fail-under=0"]
        cmd += ["--cov-append"] if cov == "append" else []
    return _run(name, "pytest", [*cmd, *tests], timeout_secs=TEST_SECS)


def _extras(name: str, step: Step, mode: str) -> list[bool]:
    """The step's extra scripts with the arguments of `mode` (quick, full or release)."""
    added = {
        "quick": step.quick,
        "full": step.full,
        "release": step.full if step.release is None else step.release,
    }[mode]
    results = []
    for cmd in step.extra:
        full = [*cmd, *added]
        results.append(_run(name, " ".join(full), [PY, *full], timeout_secs=step.timeout_secs))
    return results


def run_step(name: str, step: Step, *, mode: str = "full") -> bool:
    """Run every check of one step; print one line per check."""
    say(f"[{name}] {step.title}")
    if step.needs_stack and (why := stack_up()):
        say(f"  FAIL  stack: {why}")
        return False
    paths = list(dict.fromkeys([*step.sources, *(t.split("::")[0] for t in step.tests)]))
    results = _lint(name, paths)
    if step.tests:
        results.append(_pytest(name, list(step.tests)))
    results += _extras(name, step, mode)
    if not step.gating and not all(results):
        say(f"  (not gating: {name} reports numbers; its failure does not fail the run)")
        return True
    return all(results)


def union(paths: list[str]) -> list[str]:
    """Each path once: a path under a chosen folder, or a test id of a chosen file, goes.

    `all` named `test_runtime.py` in five steps and `tests/unit` in six, and each ran every
    time (FT-1: 641 s of the 1,165 s of pytest was repeated work).
    """
    chosen = list(dict.fromkeys(paths))
    whole = {p for p in chosen if "::" not in p}

    def covered(path: str) -> bool:
        file = path.split("::")[0]
        if "::" in path and file in whole:
            return True
        return any(file != other and file.startswith(other.rstrip("/") + "/") for other in whole)

    return [p for p in chosen if not covered(p)]


def run_all(names: list[str], *, mode: str) -> dict[str, bool]:
    """`all`: lint and types once, unit tests once, stack tests once, then each extra script.

    Coverage is measured over both test runs and judged against `fail_under` (pyproject.toml)
    in the full and release modes, as CI does (FT-22); the dev run skips it.
    """
    steps = {n: STEPS[n] for n in names}
    results: dict[str, bool] = {}
    paths = [p for s in steps.values() for p in (*s.sources, *(t.split("::")[0] for t in s.tests))]
    say("[lint] ruff, ruff format and mypy over every step's paths, once")
    results["lint"] = all(_lint("lint", union(paths)))
    tests = union([t for s in steps.values() for t in s.tests])
    unit = [t for t in tests if not t.startswith("tests/integration")]
    stack = [t for t in tests if t.startswith("tests/integration")]
    cov = mode != "quick"
    if unit:
        say(f"[unit] pytest {' '.join(unit)}")
        results["unit"] = _pytest("unit", unit, cov="new" if cov else "")
    if stack:
        say(f"[integration] pytest {' '.join(stack)}")
        if why := stack_up():
            say(f"  FAIL  stack: {why}")
            results["integration"] = False
        else:
            started = "append" if unit else "new"
            results["integration"] = _pytest("integration", stack, cov=started if cov else "")
    if cov and unit and stack:
        say("[coverage] unit + integration against fail_under (pyproject.toml)")
        results["coverage"] = _run("coverage", "coverage report", [PY, "-m", "coverage", "report"])
    for name, step in steps.items():
        if not step.extra:
            continue
        say(f"[{name}] {step.title}")
        if step.needs_stack and (why := stack_up()):
            say(f"  FAIL  stack: {why}")
            results[name] = False
            continue
        ok = all(_extras(name, step, mode))
        if not step.gating and not ok:
            say(f"  (not gating: {name} reports numbers; its failure does not fail the run)")
            ok = True
        results[name] = ok
    return results


def main() -> int:
    """Parse the step names and run them; exit 1 if any check fails."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("steps", nargs="*", help="step names (a1, a2, ...), all, or dev")
    parser.add_argument("--list", action="store_true", help="show the steps and exit")
    parser.add_argument("--quick", action="store_true", help="dev: scenarios `at`, gates quick")
    parser.add_argument("--release", action="store_true", help="every scenario x every load")
    args = parser.parse_args()
    if args.list or not args.steps:
        for name, step in STEPS.items():
            note = "" if step.in_all else "  [not in all]"
            print(f"{name:5} {step.title}  ({', '.join(step.tests)}){note}")
        return 0
    if args.quick and args.release:
        parser.error("--quick and --release do not go together")
    mode = "quick" if args.quick else "release" if args.release else "full"
    if args.steps == ["dev"]:  # the dev run's short name
        args.steps, mode = ["all"], "quick"
    whole = args.steps == ["all"]
    names = [n for n, s in STEPS.items() if s.in_all] if whole else args.steps
    unknown = [n for n in names if n not in STEPS]
    if unknown:
        print(f"unknown step(s): {', '.join(unknown)}; see --list")
        return 2
    say(f"logs: {LOG_DIR.relative_to(ROOT)}  ({'all, ' if whole else ''}{mode})")
    started = time.monotonic()
    if whole:
        results = run_all(names, mode=mode)
    else:
        results = {n: run_step(n, STEPS[n], mode=mode) for n in names}  # all, then judge
    failed = [n for n, ok in results.items() if not ok]
    say("ALL PASS" if not failed else f"FAILED: {', '.join(failed)}")
    say(f"logs: {LOG_DIR.relative_to(ROOT)}  ({(time.monotonic() - started) / 60:.1f} min)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
