"""Benchmark the queue on the local stack: pod start, first calls, steady and stream overhead.

Runs against the Docker stack of `scripts/stack.py` (Valkey 127.0.0.1:6379, fakellm on 8080 as
the Bedrock endpoint). Every run uses fresh key prefixes and deletes them at the end. The model
is a fakellm key only this script uses (`bench.llmq-fixed`), patched to a fixed latency, so the
queue's own overhead is the call time minus fakellm's direct-call time.

    python scripts/bench.py                                  # everything, 5 reps, 60 s idle
    python scripts/bench.py --reps 10 --idle 300 --out bench.json
    python scripts/bench.py --only start steady roundtrips   # some phases

Phases (each reports p50 / p95 / max in ms):
  start       `LLMQueue.from_env()` to `start()` returning with `--deployments` deployments
              (accounts x 2 regions), plus the cold first call; a fresh process per repetition
  idle        one call after start + a warm call + `--idle` seconds of quiet (one process per
              repetition, all in parallel, each on its own key prefix)
  steady      200 sequential priority calls, then 200 calls at concurrency 20, minus fakellm's
              direct-call median through an equal SDK client
  stream      time to first event and to the end, through the queue and direct, 50 runs
  roundtrips  store round trips (one per packed send) and commands for one converse and one
              converse_stream, caller and worker in this process, idle background subtracted

Uses only the public API (`from_env`, `start`, `converse`, `converse_stream`, `stop`), so the
same script measures every release; numbers are indicative when other processes load the host.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import json
import math
import os
import platform
import re
import subprocess
import sys
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

STORE_URL = "redis://:localdev@127.0.0.1:6379/0"
FAKELLM_URL = "http://127.0.0.1:8080"
MODEL_ID = "bench.llmq-fixed"  # a fakellm key no other script or test uses
REGIONS = ("us-east-1", "us-east-2", "us-west-2", "eu-west-1", "ap-south-1", "ap-southeast-1")
REQUEST = {
    "messages": [{"role": "user", "content": [{"text": "hi"}]}],
    "inferenceConfig": {"maxTokens": 32},
}
PHASES = ("start", "idle", "steady", "stream", "roundtrips")
NOTE = (
    "Indicative: other test processes may have been running on the host (machine load); "
    "compare only runs made back to back on the same machine."
)


def registry(deployments: int) -> dict[str, Any]:
    """One chat model on `deployments` deployments: accounts `bench-0..` x two regions."""
    regions = REGIONS[: min(2, deployments)]
    accounts: dict[str, list[str]] = collections.defaultdict(list)
    for i in range(deployments):
        accounts[f"bench-{i // len(regions)}"].append(regions[i % len(regions)])
    return {
        "registry_version": 1,
        "defaults": {"timeout_secs": 60, "attempt_timeout_secs": 30},
        "residency": {"allowed_regions": list(regions)},
        "accounts": {name: {"kind": "aws"} for name in accounts},
        "models": {
            "bench": {
                "type": "chat",
                "provider": "bedrock",
                "model_id": MODEL_ID,
                "rpm": 10_000_000,
                "tpm": 1_000_000_000,
                "worker_slots": 100,
                "inference": {"max_tokens": 32},
                "deployments": {name: {"confirmed": used} for name, used in accounts.items()},
            }
        },
    }


def environment(prefix: str, deployments: int) -> dict[str, str]:
    """The process environment of one queue: local stores, fakellm, the bench registry."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AWS_", "LLMQ_"))}
    env.update(
        {
            "AWS_ACCESS_KEY_ID": "bench",
            "AWS_SECRET_ACCESS_KEY": "fake",
            "AWS_DEFAULT_REGION": "us-east-1",
            "LLMQ_STORE_URL": STORE_URL,
            "LLMQ_KEY_PREFIX": prefix,
            "LLMQ_QUEUE_PREFIX": prefix.replace(":", "-") + "-",
            "LLMQ_REGISTRY": json.dumps(registry(deployments)),
            "LLMQ_METRICS_SINK": "none",
            "LLMQ_ENVIRONMENT": "test",  # local store without TLS or a salt
            "LLMQ_BEDROCK_ENDPOINT_URL": FAKELLM_URL,
            "PYTHONIOENCODING": "utf-8",
        }
    )
    return env


def account_keys(account: Any) -> dict[str, str]:
    """Static fake keys per account, so each account gets its own SDK client as in production."""
    return {"aws_access_key_id": f"bench-{account.name}", "aws_secret_access_key": "fake"}


def cleanup(prefixes: list[str]) -> None:
    """Delete every key and queue the run made (only under its own prefixes)."""
    import redis

    client = redis.Redis.from_url(STORE_URL)
    for prefix in prefixes:
        queue_prefix = prefix.replace(":", "-") + "-"
        for pattern in (f"{prefix}:*", f"{queue_prefix}*", f"*{{{queue_prefix}*"):
            keys = list(client.scan_iter(match=pattern, count=500))
            if keys:
                client.delete(*keys)
    client.close()


def summary(values: list[float]) -> dict[str, float]:
    """Count, p50, p95 (nearest rank) and max of millisecond samples, rounded to 0.1 ms."""
    if not values:
        return {"n": 0, "p50": math.nan, "p95": math.nan, "max": math.nan}
    ranked = sorted(values)

    def rank(p: float) -> float:
        return round(ranked[max(0, math.ceil(p * len(ranked)) - 1)], 1)

    return {"n": len(ranked), "p50": rank(0.5), "p95": rank(0.95), "max": round(ranked[-1], 1)}


def ms_since(started: float) -> float:
    """Milliseconds since a `time.perf_counter()` reading."""
    return (time.perf_counter() - started) * 1000


# ---------------------------------------------------------------- child processes


async def child(phase: str, idle: float) -> dict[str, float]:
    """One fresh process: cold start and first call, or a call after `idle` seconds of quiet."""
    started = time.perf_counter()
    from llm_queue import LLMQueue

    out = {"import_ms": ms_since(started)}
    started = time.perf_counter()
    queue = LLMQueue.from_env(credentials=account_keys)
    await queue.start()
    out["start_ms"] = ms_since(started)
    try:
        started = time.perf_counter()
        await queue.converse("bench", REQUEST, priority=True, timeout_secs=30)
        out["first_call_ms"] = ms_since(started)
        if phase == "idle":
            await asyncio.sleep(idle)
            started = time.perf_counter()
            await queue.converse("bench", REQUEST, priority=True, timeout_secs=30)
            out["after_idle_ms"] = ms_since(started)
    finally:
        await queue.stop(drain_secs=1)
    return out


def spawn(phase: str, env: dict[str, str], idle: float = 0) -> subprocess.Popen[str]:
    """Start this script as a child process for one repetition."""
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", phase, "--idle", str(idle)]
    return subprocess.Popen(
        cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8"
    )


def collect(proc: subprocess.Popen[str], timeout: float) -> dict[str, float]:
    """The JSON a child printed on its last line; raise with its stderr when it failed."""
    out, err = proc.communicate(timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"child failed ({proc.returncode}):\n{err[-3000:]}")
    result: dict[str, float] = json.loads(out.strip().splitlines()[-1])
    return result


def phase_start(reps: int, deployments: int, prefixes: list[str]) -> dict[str, Any]:
    """Pod start and cold first call: one priming process (dropped), then `reps` fresh ones."""
    prefix = f"llmqbench:{uuid.uuid4().hex[:8]}"
    prefixes.append(prefix)
    env = environment(prefix, deployments)
    collect(spawn("start", env), timeout=120)  # publishes the registry, as an earlier pod did
    runs = []
    for i in range(reps):
        runs.append(collect(spawn("start", env), timeout=120))
        print(f"  start rep {i + 1}/{reps}: {runs[-1]['start_ms']:.0f} ms", flush=True)
    return {
        "import": summary([r["import_ms"] for r in runs]),
        "pod_start": summary([r["start_ms"] for r in runs]),
        "cold_first_call": summary([r["first_call_ms"] for r in runs]),
    }


def phase_idle(reps: int, deployments: int, idle: float, prefixes: list[str]) -> dict[str, Any]:
    """One call after `idle` seconds: `reps` processes in parallel, each on its own prefix."""
    procs = []
    for _ in range(reps):
        prefix = f"llmqbench:{uuid.uuid4().hex[:8]}"
        prefixes.append(prefix)
        procs.append(spawn("idle", environment(prefix, deployments), idle))
        time.sleep(0.5)  # stagger, so the calls after the idle wait do not all land at once
    print(f"  {reps} processes idle for {idle:.0f} s ...", flush=True)
    runs = [collect(proc, timeout=idle + 180) for proc in procs]
    return {f"first_call_after_idle_{idle:.0f}s": summary([r["after_idle_ms"] for r in runs])}


# ---------------------------------------------------------------- in-process phases


class Counter:
    """Counts store round trips (packed sends) and commands made by this process's async clients."""

    COMMAND = re.compile(rb"\*\d+\r\n\$\d+\r\n([A-Za-z]+)\r\n")

    def __init__(self) -> None:
        """Patch redis-py's async connection send; counting is on only inside `window()`."""
        from redis.asyncio.connection import AbstractConnection

        self.on = False
        self.sends = 0
        self.commands: collections.Counter[str] = collections.Counter()
        original = AbstractConnection.send_packed_command
        counter = self

        async def send(conn: Any, command: Any, check_health: bool = True) -> None:
            if counter.on:
                counter.sends += 1
                raw = command if isinstance(command, bytes) else b"".join(command)
                for name in Counter.COMMAND.findall(raw):
                    counter.commands[name.decode().upper()] += 1
            await original(conn, command, check_health)

        AbstractConnection.send_packed_command = send  # type: ignore[method-assign,assignment]

    @contextmanager
    def window(self) -> Iterator[None]:
        """Count from zero while the block runs."""
        self.sends, self.commands, self.on = 0, collections.Counter(), True
        try:
            yield
        finally:
            self.on = False


async def timed(fn: Callable[[], Awaitable[Any]], runs: int, concurrency: int = 1) -> list[float]:
    """Milliseconds of `runs` calls of `fn`, `concurrency` at a time."""
    gate = asyncio.Semaphore(concurrency)
    out: list[float] = []

    async def one() -> None:
        async with gate:
            started = time.perf_counter()
            await fn()
            out.append(ms_since(started))

    await asyncio.gather(*(one() for _ in range(runs)))
    return out


async def stream_times(events: Callable[[], Any]) -> tuple[float, float]:
    """Milliseconds to the first event and to the end of one stream."""
    started = time.perf_counter()
    first = math.nan
    async for _ in events():
        if math.isnan(first):
            first = ms_since(started)
    return first, ms_since(started)


async def in_process(args: argparse.Namespace, prefixes: list[str]) -> dict[str, Any]:
    """Steady, stream and round-trip phases on one started queue and a direct SDK client."""
    from aiobotocore.session import get_session

    from llm_queue import LLMQueue

    counter = Counter()
    prefix = f"llmqbench:{uuid.uuid4().hex[:8]}"
    prefixes.append(prefix)
    queue = LLMQueue.from_env(environment(prefix, args.deployments), credentials=account_keys)
    await queue.start()
    results: dict[str, Any] = {}
    direct_args: dict[str, Any] = {"modelId": MODEL_ID, **REQUEST}
    session = get_session()
    async with session.create_client(
        "bedrock-runtime",
        region_name="us-east-1",
        endpoint_url=FAKELLM_URL,
        aws_access_key_id="bench-direct",
        aws_secret_access_key="fake",
    ) as direct:
        try:
            meta: list[Any] = []

            async def via_queue(priority: bool) -> None:
                reply = await queue.converse("bench", REQUEST, priority=priority, timeout_secs=30)
                meta.append(reply.meta)

            async def direct_call() -> None:
                await direct.converse(**direct_args)

            async def direct_stream() -> Any:
                response = await direct.converse_stream(**direct_args)
                async for event in response["stream"]:
                    yield event

            def queue_stream() -> Any:
                return queue.converse_stream("bench", REQUEST, priority=True, timeout_secs=30)

            await timed(direct_call, 10)  # warm both paths and fakellm
            await timed(lambda: via_queue(True), 10)
            if "steady" in args.only:
                direct_seq = await timed(direct_call, 200)
                direct_conc = await timed(direct_call, 200, concurrency=20)
                base_seq = summary(direct_seq)["p50"]
                base_conc = summary(direct_conc)["p50"]
                meta.clear()
                seq = await timed(lambda: via_queue(True), 200)
                seq_meta, meta[:] = list(meta), []
                conc = await timed(lambda: via_queue(False), 200, concurrency=20)
                results.update(
                    {
                        "direct_sequential": summary(direct_seq),
                        "direct_concurrent20": summary(direct_conc),
                        "queue_sequential_priority": summary(seq),
                        "queue_concurrent20": summary(conc),
                        "overhead_sequential_priority": summary([t - base_seq for t in seq]),
                        "overhead_concurrent20": summary([t - base_conc for t in conc]),
                        "meta_queue_wait_sequential": summary(
                            [float(m.queue_wait_ms) for m in seq_meta]
                        ),
                        "meta_queue_wait_concurrent20": summary(
                            [float(m.queue_wait_ms) for m in meta]
                        ),
                    }
                )
                print("  steady done", flush=True)
            if "stream" in args.only:
                direct_runs = [await stream_times(direct_stream) for _ in range(50)]
                queue_runs = [await stream_times(queue_stream) for _ in range(50)]
                first_base = summary([r[0] for r in direct_runs])["p50"]
                end_base = summary([r[1] for r in direct_runs])["p50"]
                results.update(
                    {
                        "stream_direct_first_event": summary([r[0] for r in direct_runs]),
                        "stream_direct_end": summary([r[1] for r in direct_runs]),
                        "stream_queue_first_event": summary([r[0] for r in queue_runs]),
                        "stream_queue_end": summary([r[1] for r in queue_runs]),
                        "stream_overhead_first_event": summary(
                            [r[0] - first_base for r in queue_runs]
                        ),
                        "stream_overhead_end": summary([r[1] - end_base for r in queue_runs]),
                    }
                )
                print("  stream done", flush=True)
            if "roundtrips" in args.only:
                results["store_round_trips"] = await round_trips(
                    counter,
                    lambda: via_queue(True),
                    lambda: stream_times(queue_stream),
                )
                print("  roundtrips done", flush=True)
        finally:
            await queue.stop(drain_secs=1)
    return results


async def round_trips(
    counter: Counter,
    call: Callable[[], Awaitable[Any]],
    stream: Callable[[], Awaitable[Any]],
    runs: int = 10,
    settle: float = 0.3,
) -> dict[str, Any]:
    """Median sends and commands per call over `runs`, idle background rate subtracted."""
    await asyncio.sleep(1)
    with counter.window():
        started = time.perf_counter()
        await asyncio.sleep(5)
        idle_secs = time.perf_counter() - started
    idle_rate = counter.sends / idle_secs
    idle_commands = dict(counter.commands)
    out: dict[str, Any] = {
        "idle_background_sends_per_s": round(idle_rate, 2),
        "idle_background_commands_5s": idle_commands,
        "settle_secs": settle,
    }
    for name, fn in (("converse", call), ("converse_stream", stream)):
        sends: list[float] = []
        commands: list[collections.Counter[str]] = []
        for _ in range(runs):
            await asyncio.sleep(0.2)
            with counter.window():
                started = time.perf_counter()
                await fn()
                await asyncio.sleep(settle)  # the worker's ack and status after the reply
                secs = time.perf_counter() - started
            sends.append(counter.sends - idle_rate * secs)
            commands.append(counter.commands)
        sends.sort()
        median = commands[len(commands) // 2]
        out[name] = {
            "sends_median": round(sends[len(sends) // 2], 1),
            "sends_min": round(sends[0], 1),
            "sends_max": round(sends[-1], 1),
            "commands_one_run_raw": dict(sorted(median.items())),
        }
    return out


# ---------------------------------------------------------------- main


def fakellm_fixed(latency_ms: float) -> None:
    """Patch the bench key in fakellm: fixed latency, instant output, no throttling."""
    behaviour = {
        "latency_ms": {"p50": latency_ms, "p95": latency_ms},
        "max_concurrency": 10_000,
        "tokens_per_s": 0,
        "output_tokens": 16,
        "throttle_rate": 0,
        "error_rate_5xx": 0,
    }
    response = httpx.patch(f"{FAKELLM_URL}/admin/models/{MODEL_ID}", json=behaviour, timeout=10)
    response.raise_for_status()


def git_sha() -> str:
    """The checkout's commit (and `+dirty` with local changes), or `unknown`."""
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--", "src"], cwd=ROOT, capture_output=True, text=True
        ).stdout.strip()
    except OSError:
        return "unknown"
    return f"{sha}+dirty" if dirty else sha or "unknown"


def table(results: Mapping[str, Any]) -> None:
    """Print the timing rows and the round-trip counts."""
    print(f"\n{'metric (ms)':<38}{'n':>5}{'p50':>10}{'p95':>10}{'max':>10}")
    for name, row in results.items():
        if isinstance(row, dict) and "p50" in row:
            print(
                f"{name:<38}{row['n']:>5}{row['p50']:>10.1f}{row['p95']:>10.1f}{row['max']:>10.1f}"
            )
    trips = results.get("store_round_trips")
    if trips:
        print(f"\nstore round trips per call (idle {trips['idle_background_sends_per_s']}/s off)")
        for name in ("converse", "converse_stream"):
            row = trips[name]
            print(
                f"  {name:<16} sends median {row['sends_median']} "
                f"(min {row['sends_min']}, max {row['sends_max']}); "
                f"raw commands {row['commands_one_run_raw']}"
            )


def main() -> int:
    """Run the chosen phases, print the table and write `--out`."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reps", type=int, default=5, help="repetitions of start and idle")
    parser.add_argument("--idle", type=float, default=60, help="idle seconds (60, 300, 900)")
    parser.add_argument("--deployments", type=int, default=6, help="deployments in the registry")
    parser.add_argument("--latency-ms", type=float, default=5, help="fakellm's fixed latency")
    parser.add_argument("--only", nargs="+", choices=PHASES, default=list(PHASES))
    parser.add_argument("--out", type=Path, help="write the results as JSON here")
    parser.add_argument("--note", default=NOTE, help="metadata note stored in the JSON")
    parser.add_argument("--child", choices=("start", "idle"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        print(json.dumps(asyncio.run(child(args.child, args.idle))))
        return 0

    import llm_queue

    fakellm_fixed(args.latency_ms)
    prefixes: list[str] = []
    results: dict[str, Any] = {}
    started = time.monotonic()
    try:
        if "start" in args.only:
            print(f"[start] {args.reps} reps, {args.deployments} deployments", flush=True)
            results.update(phase_start(args.reps, args.deployments, prefixes))
        if "idle" in args.only:
            print(f"[idle] {args.reps} reps, {args.idle:.0f} s", flush=True)
            results.update(phase_idle(args.reps, args.deployments, args.idle, prefixes))
        if {"steady", "stream", "roundtrips"} & set(args.only):
            print("[in-process] steady / stream / roundtrips", flush=True)
            results.update(asyncio.run(in_process(args, prefixes)))
    finally:
        cleanup(prefixes)
    table(results)
    if args.out:
        doc = {
            "meta": {
                "llm_queue_version": llm_queue.__version__,
                "git": git_sha(),
                "date": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "python": platform.python_version(),
                "platform": platform.platform(),
                "args": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
                "fakellm_latency_ms": args.latency_ms,
                "duration_s": round(time.monotonic() - started, 1),
                "note": args.note,
            },
            "results": results,
        }
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
