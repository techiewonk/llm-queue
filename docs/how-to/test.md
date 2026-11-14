# Test

How the library is tested, and how to run the checks yourself. Every check goes through
`scripts/check.py`. No test calls a real LLM: unit tests use fakes, stack tests and drills use
fakellm. The only path to a real model is `python scripts/samples.py --live`, run only when asked.

## Set up

You need Python 3.11+, [uv](https://docs.astral.sh/uv/) and Docker.

```bash
uv sync --all-extras          # .venv with every extra and the dev group
python scripts/stack.py up    # Valkey 8.1 (6379), Redis 7.4 (6380), fakellm (8080)
python scripts/stack.py status
```

## Run the checks

```bash
python scripts/check.py --list      # the steps and what each one runs
python scripts/check.py a6          # one step
python scripts/check.py a6 a8       # several
python scripts/check.py all         # everything
python scripts/check.py all --quick # drills shortened: gates without B17 / B91 / B92
```

Each step runs `ruff check`, `ruff format --check` and `mypy` on its source paths, then `pytest`
on its tests, then any extra script. Steps that need the stack fail at once if a service does not
answer; they never skip silently. Engine tests run on both Valkey and Redis.

| Step | Covers | Tests |
|---|---|---|
| `a0` | fakellm (accounts, TPM, pacing, hang, streaming) | `tests/unit/fakellm` |
| `tools` | test tooling: provider error table, budget properties, 200-way contention | `tests/unit/providers`, `test_budget_properties.py`, `test_contention.py` |
| `a1`, `a2` | registry schema / validation / resolve; store (version rule, edits, audit, rollback) | `tests/unit/registry`, `tests/integration/registry` |
| `a3` | clients, providers, timers | `tests/unit/clients` |
| `a4`, `a5`, `a6` | request records; stream queue; admission scripts | `tests/integration/engine/test_requests.py`, `test_queue.py`, `test_admission.py` |
| `a7` | tokens and inference | `tests/unit/engine` |
| `a8`-`a11`, `a15`, `a16` | runtime end to end, hooks, direct path, `store_down`, cancel, store reset, health, assume role, slow exclusion | `tests/integration/engine` (`test_runtime.py` and others), `tests/unit/clients/test_assume_role.py` |
| `a12`, `a13`, `a14`, `a17` | status / CLI / HTTP; embeddings; front door, jobs, sync, LangChain; streaming | `test_ops.py`, `test_embed.py`, `test_front.py`, `test_stream.py` |
| `store` | failover options, readiness, preStop, HTTP writes + `scripts/failover.py` drill | `test_store_and_http.py` |
| `load` | `scripts/load.py`: 2,000 calls over 3 workers (all succeed, none left held); 300 calls on a 600 RPM model (at most burst + rate served, the rest `rate_limited`, none past its deadline) | |
| `gates` | `scripts/gates.py`: B17, B19, B22, B23, B33, B34, B56 / B93, B91, B92, B94-B97 (~20 min) | |
| `samples` | `scripts/samples.py`: the nine samples on fakellm (`--live` real Bedrock, `--store env` a TLS store) | |
| `scenarios` | `scripts/scenarios.py`: fault scenarios x 3 loads, plus the Docker failover drills | |
| `soak3` / `soak10` / `soak20` | `scripts/soak.py --minutes N` (not in `all`) | |
| `bench` | `scripts/bench.py`, not gating: it prints numbers and never fails the run | |

pytest fails a test after 120 s (pytest-timeout) and CI requires 89 % branch coverage.
Scripts that start queues on the local stack set `LLMQ_ENVIRONMENT=test`, since an unset
environment is strict and refuses the plain-TCP store.

## Fault scenarios

`scripts/scenarios.py` runs one fault at a time (or a chosen pair) and judges it by calls, not
by time: a phase of healthy calls, the fault with calls still coming until it ends, then a phase
after it settles. Each scenario has its own pass rule.

```bash
python scripts/scenarios.py --list                   # every scenario and its rule
python scripts/scenarios.py S4 --load under          # one, while fixing it
python scripts/scenarios.py --group store            # a group
python scripts/scenarios.py all --profile prod       # production timers
python scripts/scenarios.py all --jobs 1             # one run at a time (default 4)
python scripts/check.py scenarios --quick            # every scenario, one load each
```

| Option | Values |
|---|---|
| `--load` (repeatable) | `under` (50 % of the budget), `at` (100 %), `burst` (300 %) |
| `--profile` | `fast` (default: short leases, visibility, deadlines and lifetimes; 10-40 s a scenario), `prod` (production timers) |
| `--jobs` | runs at a time (default 4; 1 runs them one by one) |
| `--group` | `worker`, `store`, `provider`, `registry`, `caller`, `pairs`, `failover` |
| `--seed` | seed of every random choice (printed at the start; the same in `gates.py` and `soak.py`), to replay a run |

| Group | Scenarios |
|---|---|
| worker | S1 killed, S2 graceful restart, S3 all down then back |
| store | S4 blocked, S5 half-open, S8 keys reset, S9 out of memory, S10 slow; S6 / S7 / S11 failover drills on Docker nodes |
| provider (fakellm) | P1 throttled, P2 5xx, P3 hangs, P4 model not in region, P5 bad credentials, P7 every region down |
| registry | R1 rpm cut live, R2 region disabled, R3 kill switch, R4 bad edit refused |
| caller | C1 cancel, C2 caller process dies, C3 duplicate request id |
| pairs | X1-X8: two faults at once (store block + worker kill, rpm cut + throttle ...) |

## Soak

`scripts/soak.py` runs 8 worker processes with chaos (kill, store block, live edit, restart) and
checks for leaks and drift. Each length scales its chaos spacing so even 3 minutes sees every
chaos event:

| Step | Calls | Chaos | Use |
|---|---|---|---|
| `soak3` | ~12,600 | every 30 s, block 10 s | smoke test while fixing |
| `soak10` | ~42,000 | every 60 s, block 20 s | after a change to the engine |
| `soak20` | ~84,000 | every 120 s, block 20 s | before a release |

`--live` runs it on real Bedrock at 10 RPM.

## Gates and the perf baseline

`scripts/gates.py` runs the plan's load and chaos gates on worker processes. Gate B34 compares
the queue's p95 and ceiling with `scripts/baselines/perf.json` (p95 within 25 % + 10 ms,
ceiling at least 80 %) and fails when no baseline exists, so record one per machine first:

```bash
python scripts/gates.py B34 --record-baseline   # store this machine's B34 numbers
python scripts/gates.py B34 --seed 7            # replay a run
```

## Benchmark

`scripts/bench.py` measures pod start, the cold first call, the first call after an idle
spell, steady and stream overhead and store round trips per call, with the public API only,
so every release is measured the same way. See [Performance](../explanation/performance.md).

```bash
python scripts/bench.py                         # everything: 5 reps, 60 s idle
python scripts/bench.py --reps 10 --idle 300 --out bench.json
python scripts/bench.py --only start steady roundtrips
```

## fakellm

fakellm's behaviour is set per model x target key in a scenario (YAML or JSON at boot,
`PUT /admin/scenario`) or patched mid-test (`PATCH /admin/models/{key}`); `POST /admin/reset`
clears the counters and `GET /admin/stats` reads them. Besides latency, RPM / TPM, concurrency,
throttle / 5xx / timeout rates and `hang`, a key can set:

| Field | Effect |
|---|---|
| `force_error` | every call returns that `ERRORS` row; the rows include the auth errors (`UnrecognizedClientException`, `InvalidSignatureException`, `InvalidSignatureClockSkew`, `ExpiredTokenException`) and the quota error (`ServiceQuotaExceededException`, OpenAI `insufficient_quota`) |
| `force_message` | replaces every error's message (to test classification by message) |
| `cut_after_chunks`, `cut_with` | a stream ends after that many text chunks, with an in-stream exception, `stall` (no more bytes, socket open) or `close` (socket dropped) |
| `tail_rate`, `tail_ms` | a heavy latency tail; `latency_per_1k_input_ms` makes latency grow with the prompt |
| `reasoning` | reasoning blocks (`text`, `redacted`, `both`), gpt-oss style |
| `cache_read_tokens`, `cache_write_tokens` | prompt-cache usage in the reply |

Per key, `/admin/stats` also counts calls per wall-clock minute (`by_minute`, to check a
fixed-window budget at the provider) and `duplicates` (a request body seen twice since the
reset: a call that ran twice).

## Property tests

Hypothesis tests check the budget math: `tests/unit/engine/test_budget_properties.py` (token
estimate, used tokens, refunds) and `tests/integration/engine/test_budget_clock.py`, which
runs the admission scripts' fixed and smooth RPM windows at chosen times and compares them
grant by grant with a Python model. `Admission(clock_ms=...)` (tests only) replaces the
store's clock for this.

## Nightly

`.github/workflows/nightly.yml` (02:17 UTC, or run by hand) runs what CI cannot run on every
push: `scenarios.py all --load under`, `gates.py --quick` and a 10-minute soak, on fakellm
and the Docker stack, and keeps `reports/` as an artifact for 7 days.

## Logs

- Every check writes its whole output, live, to `reports/check-<time>/<step>.<check>.log`
  (git-ignored). The console prints one PASS / FAIL line per check with its log path;
  `summary.log` in the same folder keeps the console. pytest runs with `-rA`, so a log lists
  every test and its outcome.
- Drill worker processes log to `reports/drill-<time>/` (or `LLMQ_DRILL_LOG_DIR`, which
  `check.py` sets per step). They log at `WARNING`; set `DRILL_LOG_LEVEL=INFO` to log every call.
- Follow a long drill with `tail -f reports/check-<time>/gates.*.log`.

## Before a release

Run `python scripts/check.py all` (without `--quick`) and `python scripts/check.py soak20`.

More on writing tests and check steps is in [Contributing](../project/contributing.md).
