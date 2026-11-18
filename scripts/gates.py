"""The plan's load and chaos gates (§11: B17 ... B97; 0.2.0: BL1, G-T1, G-T2) on fakellm.

Workers are real processes (pods) against one store, driven from here. Rates and durations are
scaled to run on one machine in minutes; each gate prints its numbers and the plan's pass mark
it applies. Nothing calls a real LLM.

    python scripts/gates.py                # every gate (about 20 minutes)
    python scripts/gates.py B17 B95        # some
    python scripts/gates.py --quick        # all but the slow calibration gates (about 14 minutes)
    python scripts/gates.py --list
    python scripts/gates.py B34 --record-baseline   # store this machine's B34 numbers

`--quick` leaves out B17 (fixed windows over whole minutes), B91 (20 models x 30 deployments),
B92 (3 minutes idle), BL1 (3 minutes of local mode) and G-T2 (10,000 waiting calls); run every
gate before a release. `--short` scales BL1 (40 s), G-T1 (400 calls a side) and G-T2 (1,000
waiting) down to prove the harness; their pass marks are for the full size.

    python scripts/gates.py BL1 G-T1 G-T2           # the 0.2.0 gates (about 6 minutes)
    python scripts/gates.py BL1 G-T1 G-T2 --short   # a harness smoke (about 2 minutes)
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import functools
import itertools
import json
import math
import random
import statistics
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from gates_fleet import (  # needs the sys.path entry
    CHAT_ID,
    EMBED_ID,
    PY,
    REGIONS,
    TITAN_ID,
    FakeLLM,
    FaultProxy,
    Store,
    Worker,
    account_keys,
    drill_tuning,
    environment,
    kill_and_reap,
    log_dir,
    log_file,
    new_prefix,
    registry,
    setup_logging,
    stop_all,
    workers,
)

from llm_queue import CallFailed, LLMQueue

FAST = {"defaults": {"latency_ms": {"p50": 150, "p95": 250}}}
BASELINE = Path(__file__).resolve().parent / "baselines" / "perf.json"
RECORD_BASELINE = "--record-baseline" in sys.argv  # B34 writes BASELINE instead of reading it


def chat(text: str = "hi", *, tools: bool = False) -> dict[str, Any]:
    """A Converse request."""
    request: dict[str, Any] = {
        "messages": [{"role": "user", "content": [{"text": text}]}],
        "inferenceConfig": {"maxTokens": 50},
    }
    if tools:
        spec = {"type": "object", "properties": {"symbol": {"type": "string"}}}
        request["toolConfig"] = {
            "tools": [{"toolSpec": {"name": "price", "inputSchema": {"json": spec}}}]
        }
    return request


@dataclass(slots=True)
class Result:
    """One call's outcome (slots, interned strings: a soak keeps ~80,000 of them)."""

    sent: float
    ok: bool
    kind: str = ""
    wait_secs: float = 0.0
    total_secs: float = 0.0
    deployment: str = ""
    model: str = ""
    worker: str = ""
    seen_secs: float = 0.0  # until a worker first received it
    priority: bool = False
    order: int = 0

    @property
    def started(self) -> float:
        """When its provider call began (the reply's arrival minus the provider's time)."""
        return self.sent + self.wait_secs


@dataclass
class Run:
    """The calls of one gate."""

    t0: float = field(default_factory=time.monotonic)
    results: list[Result] = field(default_factory=list)
    messages: dict[str, str] = field(default_factory=dict)  # first message per failure kind

    async def call(
        self, llmq: LLMQueue, model: str, request: dict[str, Any], *, order: int = 0, **kw: Any
    ) -> Result:
        """One call, timed and recorded."""
        sent = time.monotonic() - self.t0
        try:
            reply = await llmq.converse(model, request, **kw)
            done = time.monotonic() - self.t0
            # Wait = until the provider call began (granted and started), not until a worker
            # first saw it (`meta.queue_wait_ms`), which hides the time spent held.
            began = done - reply.meta.provider_ms / 1000
            result = Result(
                sent,
                True,
                wait_secs=max(0.0, began - sent),
                total_secs=done - sent,
                deployment=sys.intern(reply.meta.deployment or ""),
                model=sys.intern(reply.meta.served_model or ""),
                worker=sys.intern(reply.meta.worker or ""),
                seen_secs=reply.meta.queue_wait_ms / 1000,
                priority=bool(kw.get("priority")),
                order=order,
            )
        except CallFailed as exc:
            done = time.monotonic() - self.t0
            result = Result(sent, False, exc.kind, total_secs=done - sent, order=order)
            self.messages.setdefault(exc.kind, str(exc)[:160])
        self.results.append(result)
        return result

    def kinds(self) -> dict[str, Any]:
        """Failures by kind, with the first message of each."""
        counts = collections.Counter(r.kind for r in self.results if not r.ok)
        return {k: f"{n} ({self.messages.get(k, '')})" for k, n in counts.items()}


def p95(values: list[float]) -> float:
    """95th percentile (0 for no values)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))]


async def callers(env: dict[str, str], count: int = 2) -> list[LLMQueue]:
    """Caller-only queues in this process."""
    queues = []
    for _ in range(count):
        queue = LLMQueue.from_env(
            {**env, "LLMQ_ROLE_MODE": "caller"}, credentials=account_keys, tuning=drill_tuning(env)
        )
        await queue.start()
        queues.append(queue)
    return queues


async def paced(rate: float, secs: float, fn: Callable[[int], Coroutine[Any, Any, Any]]) -> None:
    """Start `fn(i)` `rate` times a second for `secs`, then wait for all of them."""
    tasks = []
    t0 = time.monotonic()
    i = 0
    while time.monotonic() - t0 < secs:
        tasks.append(asyncio.create_task(fn(i)))
        i += 1
        await asyncio.sleep(max(0.0, t0 + i / rate - time.monotonic()))
    await asyncio.gather(*tasks)


class Gate:
    """One gate's setup and teardown (prefix, fakellm, workers, callers)."""

    def __init__(self, doc: dict[str, Any], scenario: dict[str, Any] | None = None, **env: str):
        """Prepare; `async with` starts fakellm, callers and the workers added later."""
        self.prefix = new_prefix()
        self.env = environment(self.prefix, doc, **env)
        self.fake = FakeLLM(scenario or FAST)
        self.store = Store()
        self.fleet: list[Worker] = []
        self.queues: list[LLMQueue] = []

    async def __aenter__(self) -> Gate:
        try:
            await asyncio.to_thread(self.fake.start)
        except BaseException:
            self.store.close()
            raise
        return self

    async def workers(self, count: int) -> list[Worker]:
        """Start `count` more worker processes."""
        more: list[Worker] = await workers(self.env, count)
        self.fleet += more
        return more

    async def callers(self, count: int = 2) -> list[LLMQueue]:
        """Caller queues in this process."""
        more = await callers(self.env, count)
        self.queues += more
        return more

    async def __aexit__(self, *exc: object) -> None:
        # Every step runs even when one before it raised: a stuck worker used to leave the
        # workers, fakellm (its port still bound) and every key of the run behind (CO-3).
        try:
            for queue in self.queues:
                with contextlib.suppress(Exception):
                    await queue.stop(drain_secs=1)
        finally:
            try:
                await stop_all(self.fleet)
            finally:
                try:
                    await asyncio.to_thread(self.fake.stop)  # its wait off the loop (N7)
                finally:
                    try:
                        self.store.cleanup(self.prefix)
                    finally:
                        self.store.close()


def verdict(name: str, checks: dict[str, bool], lines: list[str]) -> bool:
    """Print a gate's numbers and checks; True when all pass."""
    for line in lines:
        print(f"  {line}")
    for check, ok in checks.items():
        print(f"  {'ok  ' if ok else 'FAIL'} {check}")
    passed = all(checks.values())
    print(f"{'PASS' if passed else 'FAIL'}  {name}")
    return passed


# ------------------------------------------------------------------------------------------ gates


async def b17() -> bool:
    """10K mixed calls on 3 accounts x 3 regions (scaled: ~5,000 over 2.5 min).

    Pass: no deployment over its RPM in any minute, by the store's counters and by the
    provider's (fakellm counts each deployment's calls per wall-clock minute); served >= 0.9 x
    the quota in the full minute; priority wait p95 <= 2 s.
    """
    rpm = 200  # per deployment, fixed minute: 9 x 200 = 1,800 a minute
    deployments = [f"{CHAT_ID}@aws-{a}/{r}" for a in range(3) for r in REGIONS[:3]]
    scenario = {**FAST, "models": {key: {} for key in deployments}}  # one fakellm key each
    doc = registry(
        {
            "chat": {"rpm": rpm, "rate_window": "fixed", "balance": "headroom"},
            "titan": {"type": "embedding", "model_id": TITAN_ID, "rpm": 100_000},
        }
    )
    async with Gate(doc, scenario) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        run = Run()
        sizes = [500, 2000, 8000, 20000]

        async def one(i: int) -> None:
            llmq = queues[i % 2]
            if random.random() < 0.4:
                await run.call(llmq, "titan", {"body": {"inputText": "text " * 50}})
                return
            text = "x" * (4 * random.choice(sizes))
            await run.call(
                llmq,
                "chat",
                chat(text, tools=random.random() < 0.2),
                priority=random.random() < 0.3,
                order=i,
            )

        used: dict[tuple[int, str], int] = {}  # (minute, deployment) -> the store's own count
        stop = asyncio.Event()

        async def sample() -> None:
            while not stop.is_set():
                status = await queues[0].status("chat")
                minute = int(time.time() // 60)
                for dep in status["deployments"]:
                    key = (minute, dep["name"])
                    used[key] = max(used.get(key, 0), dep["rpm"]["used"])
                await asyncio.sleep(0.25)

        sampler = asyncio.create_task(sample())
        # 95 % of the quota offered (28.5 chat/s of 30/s): at 100 % the priority calls above
        # the 20 % reserve wait for the next fixed minute whenever normal calls spend theirs first.
        await paced(47.5, 150, one)
        stop.set()
        await sampler
        provider = await asyncio.to_thread(gate.fake.stats)
    # A call granted in the last instant of a minute may reach the provider in the next one:
    # two calls of slack per deployment-minute for that, no more.
    provider_over = max(
        (n for key in deployments for n in (provider.get(key) or {}).get("by_minute", {}).values()),
        default=0,
    )
    chats = [r for r in run.results if r.model == "chat"]
    minutes = sorted({m for m, _ in used})
    full = minutes[1] if len(minutes) > 2 else minutes[0]  # a minute the run covered whole
    served_full = sum(n for (m, _), n in used.items() if m == full)
    chat_served = sum(1 for r in chats if r.ok)
    priority_wait = p95([r.wait_secs for r in run.results if r.ok and r.priority])
    over = max(used.values(), default=0)
    lines = [
        f"{len(run.results)} calls ({chat_served} chat served); failures {run.kinds() or 0}",
        f"served in the full minute: {served_full} of {9 * rpm}; max per deployment-minute "
        f"{over} (limit {rpm}); priority wait p95 {priority_wait:.2f}s",
        f"provider: max calls per deployment-minute {provider_over}",
    ]
    return verdict(
        "B17",
        {
            "no deployment over its RPM in a minute": over <= rpm,
            "provider: no deployment over its RPM in a minute (+2 at the boundary)": provider_over
            <= rpm + 2,
            "throughput >= 0.9 x quota": served_full >= 0.9 * 9 * rpm,
            "priority wait p95 <= 2 s": priority_wait <= 2.0,
        },
        lines,
    )


async def b19() -> bool:
    """Burst of 2,000 in 10 s.

    Pass: admission refusals per call <= 0.05; store commands per
    call recorded (0.2.1 had 6 state-store calls per worker call).
    """
    doc = registry({"chat": {"rpm": 1_000_000, "worker_slots": 100}}, accounts=1, regions=1)
    async with Gate(doc) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        run = Run()
        before_cmd, before_scr = gate.store.commands(), gate.store.script_calls()
        await paced(200, 10, lambda i: run.call(queues[i % 2], "chat", chat()))
        await asyncio.sleep(2)
        cmds = gate.store.commands() - before_cmd
        scripts = gate.store.script_calls() - before_scr
        metrics = gate.store.metrics(gate.prefix)
    refusals = sum(v for k, v in metrics.items() if k.startswith("llmq_admission_refusals"))
    # FT-24: a refusal count of 0 also comes from a metrics flusher that died; the call
    # counter the workers must have written proves the metrics reached the store.
    counted = sum(v for k, v in metrics.items() if k.startswith("llmq_calls_total"))
    calls = len(run.results)
    lines = [
        f"{calls} calls, {sum(r.ok for r in run.results)} ok; failures {run.kinds() or 0}",
        f"admission refusals {refusals:.0f} ({refusals / calls:.3f} per call); "
        f"store scripts per call {scripts / calls:.1f} "
        f"(commands incl. inside them {cmds / calls:.0f})",
        f"calls counted by the workers' metrics: {counted:.0f}",
    ]
    return verdict(
        "B19",
        {
            "every call served": all(r.ok for r in run.results),
            "the workers' metrics reached the store (calls counted >= 90 %)": counted
            >= 0.9 * calls,
            "refusals per call <= 0.05": refusals / calls <= 0.05,
        },
        lines,
    )


async def _shares(processes: int) -> tuple[dict[str, float], float]:
    """Deployment shares and store commands per call for one load on `processes` workers."""
    doc = registry({"chat": {"rpm": 1_000_000, "balance": "round_robin"}})
    async with Gate(doc) as gate:
        await gate.workers(processes)
        queues = await gate.callers(2)
        await asyncio.sleep(3)
        idle_t0, idle0 = time.monotonic(), gate.store.commands()
        await asyncio.sleep(5)
        idle_rate = (gate.store.commands() - idle0) / (time.monotonic() - idle_t0)
        run = Run()
        t0, c0 = time.monotonic(), gate.store.commands()
        await paced(100, 15, lambda i: run.call(queues[i % 2], "chat", chat()))
        secs, cmds = time.monotonic() - t0, gate.store.commands() - c0
    served = collections.Counter(r.deployment for r in run.results if r.ok)
    total = sum(served.values())
    shares = {d: n / total for d, n in served.items()}
    per_call = (cmds - idle_rate * secs) / max(1, len(run.results))
    return shares, per_call


async def b22() -> bool:
    """The same load on 2 and on 8 worker processes (plan: 20).

    Pass: deployment shares and
    store commands per call (idle polling subtracted) equal within 5 %.
    """
    two, two_cmds = await _shares(2)
    eight, eight_cmds = await _shares(8)
    worst = max(abs(two.get(d, 0) - eight.get(d, 0)) / max(two.get(d, 1e-9), 1e-9) for d in two)
    cmd_diff = abs(two_cmds - eight_cmds) / two_cmds
    lines = [
        f"shares on 2: {sorted(round(v, 3) for v in two.values())}",
        f"shares on 8: {sorted(round(v, 3) for v in eight.values())}",
        f"store commands per call: {two_cmds:.1f} on 2, {eight_cmds:.1f} on 8",
    ]
    return verdict(
        "B22",
        {
            "shares equal within 5 %": worst <= 0.05,
            "store commands per call equal within 5 %": cmd_diff <= 0.05,
        },
        lines,
    )


async def b56_b93() -> bool:
    """Backlog: 2,400 normal calls waiting on rpm 1,200 (plan: 5,000 on 500 / 4,000), 8 processes.

    Pass: starts per minute >= 0.95 x the normal-class quota; after warm-up, start
    order = send order (1 s of cross-process timing allowed); priority calls start within 2 s;
    store calls per second the same with 2,000 waiting as with a few hundred.
    """
    rpm = 1200
    doc = registry(
        {"chat": {"rpm": rpm, "worker_slots": 50}},
        accounts=1,
        regions=1,
        defaults={"timeout_secs": 300, "timeout_max_secs": 300, "max_wait_secs": 300},
    )
    async with Gate(doc) as gate:
        await gate.workers(8)
        queues = await gate.callers(2)
        run = Run()
        samples: list[tuple[int, float]] = []  # (calls not yet started, store scripts/s)
        done = asyncio.Event()

        async def sample() -> None:
            while not done.is_set():
                s0, t0 = gate.store.script_calls(), time.monotonic()
                await asyncio.sleep(10)
                rate = (gate.store.script_calls() - s0) / (time.monotonic() - t0)
                samples.append((2400 - sum(1 for r in run.results if r.ok), rate))

        tasks = []
        for i in range(2400):  # 5 ms apart: a well-defined send order
            tasks.append(asyncio.create_task(run.call(queues[i % 2], "chat", chat(), order=i)))
            await asyncio.sleep(0.005)
        await asyncio.sleep(3)
        sampler = asyncio.create_task(sample())  # after the sends: only the waiting is measured
        await asyncio.sleep(17)
        prio = [
            asyncio.create_task(run.call(queues[i % 2], "chat", chat(), priority=True))
            for i in range(20)
        ]
        await asyncio.gather(*prio)
        await asyncio.gather(*tasks)
        done.set()
        await sampler
    normal = sorted((r for r in run.results if r.ok and not r.priority), key=lambda r: r.order)
    starts = sorted(r.started for r in normal)
    window = [s for s in starts if starts[0] + 10 <= s < starts[0] + 70]
    quota = int(rpm * 0.8)  # normal calls may use 1 - priority_reserve (plan E79: 80 %)
    steady = [r for r in normal if r.sent >= 15]  # after the first burst and cold pools
    behind = [a.started - b.started for a, b in itertools.pairwise(steady)]
    late = sum(1 for d in behind if d > 1.0)
    prio_wait = max((r.wait_secs for r in run.results if r.priority and r.ok), default=99)
    big = [rate for left, rate in samples if left > 1500]
    small = [rate for left, rate in samples if 100 < left < 600]
    load_big = statistics.mean(big) if big else 0.0
    load_small = statistics.mean(small) if small else 0.0
    lines = [
        f"{len(normal)} normal served; failures {run.kinds() or 0}",
        f"starts in a full minute {len(window)} (normal-class quota {quota} of {rpm})",
        f"after warm-up, older-after-newer starts: worst {max(behind, default=0):.2f}s, "
        f"{late} over 1 s; priority wait max {prio_wait:.2f}s",
        f"store scripts/s: {load_big:.0f} with > 1,500 waiting, {load_small:.0f} with 100-600",
    ]
    return verdict(
        "B56/B93",
        {
            "starts per minute >= 0.95 x quota": len(window) >= 0.95 * quota,
            "start order = send order": late == 0,
            "priority calls start within 2 s": prio_wait <= 2.0,
            "store calls do not grow with the backlog": bool(big and small)
            and load_big <= 1.25 * load_small,
            "every call served": all(r.ok for r in run.results),
        },
        lines,
    )


async def b94() -> bool:
    """The pod holding waiters killed with others waiting, both window modes.

    Pass: the next
    waiter is granted within 2 s (the dead pod's waiters do not block the line).
    """
    ok = True
    for window, rpm in (("smooth", 60), ("fixed", 30)):
        doc = registry({"chat": {"rpm": rpm, "rate_window": window}}, accounts=1, regions=1)
        async with Gate(doc) as gate:
            fleet = await gate.workers(4)
            queues = await gate.callers(2)
            run = Run()
            tasks = [
                asyncio.create_task(run.call(queues[i % 2], "chat", chat(), order=i))
                for i in range(60)
            ]
            await asyncio.sleep(8)
            killed_at = time.monotonic() - run.t0
            await fleet[0].kill()
            await asyncio.gather(*tasks)
        grants = sorted(r.started for r in run.results if r.ok and r.started > killed_at)
        interval = 60 / rpm if window == "smooth" else 0.5
        if window == "fixed":  # the next grants come at the next minute
            clock = time.time() - time.monotonic()  # fixed windows turn on wall-clock minutes
            minute = (int((clock + run.t0 + killed_at) // 60) + 1) * 60 - clock - run.t0
            first = min((g for g in grants if g >= minute), default=1e9) - minute
            gap = first
        else:
            gap = max((b - a for a, b in itertools.pairwise(grants)), default=0)
        ok &= verdict(
            f"B94 ({window})",
            {f"next waiter granted within 2 s (+{interval:.1f} s of rate)": gap <= 2 + interval,
             "every call served": all(r.ok for r in run.results)},
            [f"{sum(r.ok for r in run.results)}/60 served; failures {run.kinds() or 0}; "
             f"worst gap after the kill {gap:.2f}s"],
        )  # fmt: skip
    return ok


async def b95() -> bool:
    """State store blocked 45 s with 500 calls waiting.

    Pass: none dropped as `caller_gone`;
    all served.
    """
    doc = registry({"chat": {"rpm": 600}}, accounts=1, regions=1)
    proxy = FaultProxy()  # in front of the stack's Valkey, its own process (CO-2)
    await asyncio.to_thread(proxy.start)
    try:
        async with Gate(doc, LLMQ_STORE_URL=f"redis://:localdev@127.0.0.1:{proxy.port}/0") as gate:
            await gate.workers(4)
            queues = await gate.callers(2)
            run = Run()
            tasks = [
                asyncio.create_task(
                    run.call(queues[i % 2], "chat", chat(), timeout_secs=300, max_wait_secs=300)
                )
                for i in range(500)
            ]
            await asyncio.sleep(5)
            await proxy.set(refuse=True)
            await proxy.drop_all()
            try:
                await asyncio.sleep(45)
            finally:
                await proxy.set(refuse=False)
            await asyncio.gather(*tasks)
    finally:
        await asyncio.to_thread(proxy.stop)
    kinds = run.kinds()
    return verdict(
        "B95",
        {
            "0 calls dropped as caller_gone": "caller_gone" not in kinds,
            "all served": all(r.ok for r in run.results),
        },
        [f"{sum(r.ok for r in run.results)}/500 served; failures {kinds or 0}"],
    )


async def b96() -> bool:
    """Kill switch on, 2,000 direct calls waiting in 4 caller processes (plan: 5,000 on 20).

    Pass: every caller started and is still waiting (alive, no outcome yet) through the
    measurement, the store saw some admission traffic, and scripts <= 20 per process per second.
    FT-3: a crashed or idle caller used to pass (rate 0); FT-4: callers are always reaped;
    FT-30: each caller logs to its own file instead of DEVNULL.
    """
    doc = registry({"chat": {"rpm": 600}}, accounts=1, regions=1, routing={"kill_switch": True})
    procs: list[subprocess.Popen[bytes]] = []
    ready = alive = 0
    FLEET = Path(__file__).parent / "gates_fleet.py"  # a path, not a variable
    rate = 0.0
    async with Gate(doc) as gate:
        try:
            for _ in range(4):
                log = log_file("b96-caller")
                try:
                    procs.append(
                        subprocess.Popen(  # noqa: ASYNC220  # started once, before measuring
                            [PY, str(FLEET), "caller", "500", "chat"],
                            env={**gate.env, "LLMQ_ROLE_MODE": "caller"},
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=log,
                        )
                    )
                finally:
                    log.close()  # the child keeps its own handle
            for proc in procs:
                assert proc.stdout is not None
                try:
                    line = await asyncio.wait_for(asyncio.to_thread(proc.stdout.readline), 60)
                except TimeoutError:
                    line = b""
                ready += line.strip() == b"ready"
            if ready == len(procs):
                for proc in procs:
                    assert proc.stdin is not None
                    proc.stdin.write(b"go\n")
                    proc.stdin.flush()
                await asyncio.sleep(10)
                s0, t0 = gate.store.script_calls(), time.monotonic()
                await asyncio.sleep(20)
                calls = gate.store.script_calls() - s0
                rate = calls / (time.monotonic() - t0) / len(procs)
                alive = sum(proc.poll() is None for proc in procs)
        finally:
            await asyncio.gather(*(kill_and_reap(p) for p in procs), return_exceptions=True)
            for proc in procs:
                for pipe in (proc.stdin, proc.stdout):
                    if pipe is not None:
                        with contextlib.suppress(OSError):
                            pipe.close()
    return verdict(
        "B96",
        {
            "every caller started": ready == len(procs),
            "every caller still waiting during the measurement": alive == len(procs),
            "store scripts > 0 (the callers really ask)": rate > 0,
            "store scripts <= 20 per process per second": rate <= 20,
        },
        [
            f"ready {ready}/{len(procs)}, alive {alive}/{len(procs)}; store scripts per caller "
            f"process per second while 2,000 direct calls wait: {rate:.1f}"
        ],
    )


async def b91() -> bool:
    """20 models, 10 regions x 3 accounts.

    Pass: script p99 <= 2 ms server time; status of
    everything <= 500 KB and <= 200 ms.
    """
    models = {f"m{i:02d}": {"rpm": 100_000, "model_id": f"openai.m{i}-1:0"} for i in range(20)}
    doc = registry(models, accounts=3, regions=10)
    async with Gate(doc) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        await asyncio.sleep(2)
        gate.store.reset_latency()
        run = Run()
        await paced(100, 15, lambda i: run.call(queues[i % 2], f"m{i % 20:02d}", chat()))
        p99 = gate.store.script_p99_ms()
        times = []
        for _ in range(6):
            t0 = time.monotonic()
            status = await queues[0].status()
            times.append((time.monotonic() - t0) * 1000)
        cold_ms, status_ms = times[0], statistics.median(times[1:])
        size_kb = len(json.dumps(status, default=str)) / 1024
    return verdict(
        "B91",
        {
            "every call served": all(r.ok for r in run.results),
            "script p99 <= 2 ms": p99 <= 2.0,
            "status <= 500 KB": size_kb <= 500,
            "status <= 200 ms": status_ms <= 200,
        },
        [f"{len(run.results)} calls over 20 models x 30 deployments; script p99 {p99:.2f} ms "
         f"(exact); status {size_kb:.0f} KB in {status_ms:.0f} ms (first call {cold_ms:.0f} ms)"],
    )  # fmt: skip


async def b92() -> bool:
    """Idle (plan: 30 min; here 3 min), then 3,000 calls in 60 s across 3 regions.

    Pass: 0
    connect errors; first-call overhead <= 500 ms; priority wait p95 <= 2 s.
    """
    doc = registry({"chat": {"rpm": 100_000}}, accounts=1, regions=3)
    async with Gate(doc) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        await asyncio.sleep(180)
        run = Run()
        first = await run.call(queues[0], "chat", chat(), priority=True)
        await paced(50, 60, lambda i: run.call(queues[i % 2], "chat", chat(), priority=i % 3 == 0))
    overhead = first.total_secs - 0.25  # minus fakellm's p95 latency
    wait = p95([r.wait_secs for r in run.results if r.ok and r.priority])
    return verdict(
        "B92",
        {
            "0 errors": all(r.ok for r in run.results),
            "first-call overhead <= 500 ms": overhead <= 0.5,
            "priority wait p95 <= 2 s": wait <= 2.0,
        },
        [f"{len(run.results)} calls; first call {first.total_secs * 1000:.0f} ms; "
         f"priority wait p95 {wait:.2f}s; failures {run.kinds() or 0}"],
    )  # fmt: skip


async def b97() -> bool:
    """Bulk embedding on cohere (96 texts per call) at its full rpm.

    Pass: store memory stays
    under its budget (here 64 MB above the start) for the whole run.
    """
    doc = registry(
        {"cohere": {"type": "embedding", "model_id": EMBED_ID, "rpm": 600,
                    "embed_batch_size": 96}},
        accounts=1, regions=1,
    )  # fmt: skip
    async with Gate(doc) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        base = gate.store.memory_mb()
        peak = base
        done = asyncio.Event()

        async def sample() -> None:
            nonlocal peak
            while not done.is_set():
                peak = max(peak, gate.store.memory_mb())
                await asyncio.sleep(0.5)

        sampler = asyncio.create_task(sample())
        texts = ["a sentence about markets and earnings " * 8] * (96 * 300)
        failures: collections.Counter[str] = collections.Counter()
        try:
            await queues[0].embed("cohere", texts, input_type="search_document")
        except CallFailed as exc:
            failures[exc.kind] += 1
        done.set()
        await sampler
    return verdict(
        "B97",
        {"every batch served": not failures, "store memory <= +64 MB": peak - base <= 64},
        [f"300 calls of 96 texts; store memory {base:.0f} -> peak {peak:.0f} MB"],
    )


async def b33() -> bool:
    """Rolling restart of 4 workers under load (plan: 0.3.x -> 0.3.y at 10K / min).

    Here 3K / min and one version.

    Pass: 0 lost (priority calls included); with a live edit at each restart, the
    registry change counter each caller sees never goes back and ends at the last edit
    (a live edit keeps `version` and bumps the counter).
    """
    doc = registry({"chat": {"rpm": 1_000_000}}, accounts=1, regions=1)
    # A caller sees another process's live edit at its next reload: every 30 s by default, so
    # the last edit (~33 s in, after four restarts) came after the edit-free caller's first
    # reload whenever a restart took a little longer, and the 2 s wait below could not catch it.
    # Reload every second, as the registry scenarios do.
    async with Gate(doc, LLMQ_REGISTRY_RELOAD_SECS="1") as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        run = Run()
        seen: list[list[int]] = [[], []]  # change counters each caller saw, sampled
        done = asyncio.Event()
        edits = 0

        async def restarts() -> None:
            nonlocal edits
            try:
                for n, worker in enumerate(list(gate.fleet)):
                    await asyncio.sleep(6)
                    await gate.workers(1)
                    edit = queues[0].admin.registry.set(
                        {"rpm": 1_000_000 - n - 1}, model="chat", actor="b33", dry_run=False
                    )
                    await asyncio.gather(worker.stop(), edit)  # an edit mid-restart
                    edits += 1
            finally:
                done.set()

        async def sample() -> None:
            while not done.is_set():
                for k, queue in enumerate(queues):
                    seen[k].append(queue.admin.registry.changes)
                await asyncio.sleep(0.2)

        load = paced(50, 40, lambda i: run.call(queues[i % 2], "chat", chat(), priority=i % 4 == 0))
        await asyncio.gather(load, restarts(), sample())
        await asyncio.sleep(2)  # LLMQ_REGISTRY_RELOAD_SECS 1: every caller has the last edit
        final = [q.admin.registry.changes for q in queues]
    start = seen[0][0] if seen[0] else 0
    return verdict(
        "B33",
        {
            "0 lost": all(r.ok for r in run.results),
            "registry changes never go back": all(v == sorted(v) for v in seen),
            "every caller ends on the last edit": final == [start + edits] * 2,
        },
        [
            f"{len(run.results)} calls during 4 restarts and {edits} live edits; "
            f"failures {run.kinds() or 0}; changes seen {[sorted(set(v)) for v in seen]}"
        ],
    )


def _ramp_call(run: Run, queues: list[LLMQueue], i: int) -> Coroutine[Any, Any, Result]:
    """One ramp call (a named function: a lambda's default arguments are untyped)."""
    return run.call(queues[i % 2], "chat", chat())


async def b34() -> bool:
    """Ramp until something saturates; queue overhead.

    Pass: with no backlog the queue path adds <= 20 ms at p95 over a direct call; against
    the stored baseline (`scripts/baselines/perf.json`, `--record-baseline` writes it), the
    queued p95 is within 25 % + 10 ms and the ceiling at least 80 %. The ceiling is the median
    of three ramps (owner decision, 2026-11-15): on a machine whose store sits behind Docker
    Desktop's port forwarding one ramp swung between 200 and 490 calls/s.
    """
    fixed = {"defaults": {"latency_ms": {"p50": 100, "p95": 100}}}
    doc = registry({"chat": {"rpm": 10_000_000, "worker_slots": 200}}, accounts=1, regions=1)
    async with Gate(doc, fixed) as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        warm = Run()
        await paced(10, 5, lambda i: warm.call(queues[i % 2], "chat", chat()))
        for _ in range(20):
            await warm.call(queues[0], "chat", chat(), force_direct=True)
        queued, direct = Run(), Run()  # alternated, both warm: same conditions for each

        async def pair(i: int) -> None:
            await queued.call(queues[i % 2], "chat", chat())
            await direct.call(queues[i % 2], "chat", chat(), force_direct=True)

        await paced(5, 30, pair)
        ceilings: list[float] = []
        for _ in range(3):
            ceiling = 0.0
            for rate in (50, 100, 150, 200, 250, 300, 350, 400, 500, 600, 800):
                ramp = Run()
                await paced(rate, 8, functools.partial(_ramp_call, ramp, queues))
                ok = [r for r in ramp.results if r.ok]
                if len(ok) < len(ramp.results) or p95([r.wait_secs for r in ok]) > 1.0:
                    break
                ceiling = len(ok) / 8
            ceilings.append(ceiling)
            await asyncio.sleep(5)  # the last ramp's backlog drains before the next
        ceiling = statistics.median(ceilings)
        memory = gate.store.memory_mb()
    queued_ms = p95([r.total_secs for r in queued.results]) * 1000
    added = queued_ms / 1000 - p95([r.total_secs for r in direct.results])
    numbers = {"queued_p95_ms": round(queued_ms, 1), "ceiling_per_s": ceiling}
    checks = {"queue path adds <= 20 ms at p95": added <= 0.020}
    if RECORD_BASELINE:
        BASELINE.parent.mkdir(exist_ok=True)
        BASELINE.write_text(json.dumps(numbers, indent=2) + "\n", encoding="utf-8")
        print(f"  baseline recorded: {numbers}")
    elif BASELINE.exists():
        base = json.loads(BASELINE.read_text(encoding="utf-8"))
        checks["queued p95 within 25 % + 10 ms of the baseline"] = (
            queued_ms <= base["queued_p95_ms"] * 1.25 + 10
        )
        checks["ceiling >= 0.8 x the baseline"] = ceiling >= 0.8 * base["ceiling_per_s"]
        print(f"  baseline: {base}")
    else:
        checks["a baseline exists (record one with --record-baseline)"] = False
    return verdict(
        "B34",
        checks,
        [f"queued p95 {p95([r.total_secs for r in queued.results]) * 1000:.0f} ms, direct p95 "
         f"{p95([r.total_secs for r in direct.results]) * 1000:.0f} ms (+{added * 1000:.0f} ms)",
         f"ceiling on this machine: {ceiling:.0f} calls/s (median of {ceilings}) with queue "
         f"wait p95 <= 1 s "
         f"(4 workers); store memory {memory:.0f} MB"],
    )  # fmt: skip


async def b23() -> bool:
    """Queue store primary killed mid-run, `replica_wait_count` off and on.

    Pass: lost sends
    and duplicates (a call the provider ran twice: every body is unique) recorded; with it on,
    losses and duplicates not higher than with it off.
    """
    import failover

    losses: dict[str, int] = {}
    dups: dict[str, int] = {}
    failed: dict[str, int] = {}
    calls: dict[str, int] = {}
    promoted: dict[str, bool] = {}
    for wait in (0, 1):
        proxy = failover.Proxy()  # its own process (CO-2)
        doc = registry({"chat": {"rpm": 1_000_000}}, accounts=1, regions=1)
        try:  # the nodes and the proxy go even if starting them fails (CO-17)
            failover.nodes_up()
            await asyncio.to_thread(proxy.start)
            async with Gate(
                doc,
                LLMQ_QUEUE_STORE_URL=f"redis://127.0.0.1:{failover.PROXY_PORT}/0",
                LLMQ_REPLICA_WAIT_COUNT=str(wait),
                LLMQ_REPLICA_WAIT_MS="200",
                LLMQ_QUEUE_STORE_FAILOVER_SECS="3",
            ) as gate:
                await gate.workers(2)
                queues = await gate.callers(2)
                run = Run()

                async def chaos() -> None:
                    await asyncio.sleep(5)
                    # In a thread: a blocking docker call stalled this driver's own callers
                    # (their misses counted as lost sends under load).
                    await asyncio.to_thread(failover.docker, "kill", failover.NODES["a"][0])
                    await asyncio.to_thread(
                        failover.docker,
                        "exec",
                        failover.NODES["b"][0],
                        "valkey-cli",
                        "REPLICAOF",
                        "NO",
                        "ONE",
                    )
                    await proxy.move_to("b")  # noqa: B023  # awaited within this iteration

                load = paced(
                    100,
                    15,
                    lambda i: run.call(  # noqa: B023  # awaited within this iteration
                        queues[i % 2],  # noqa: B023
                        "chat",
                        chat(f"b23 {wait} {i}"),  # noqa: B023  # unique: fakellm counts repeats
                        timeout_secs=20,
                    ),
                )
                await asyncio.gather(load, chaos())
                provider = await asyncio.to_thread(gate.fake.stats)
                # FT-12: prove the fault happened (b took over); 0 <= 0 passed when it did not.
                role = await asyncio.to_thread(failover.role, "b")
                promoted["on" if wait else "off"] = role == "master"
        finally:
            try:
                await asyncio.to_thread(proxy.stop)
            finally:
                failover.nodes_down()
        lost = sum(1 for r in run.results if not r.ok and r.kind == "deadline_exceeded")
        losses["on" if wait else "off"] = lost
        dups["on" if wait else "off"] = sum(int(m.get("duplicates", 0)) for m in provider.values())
        side = "on" if wait else "off"
        failed[side] = sum(1 for r in run.results if not r.ok)
        calls[side] = len(run.results)
        print(f"  replica_wait_count={wait}: {len(run.results)} calls, lost {lost}, "
              f"duplicates {dups[side]}, failures {run.kinds() or 0}")  # fmt: skip
    return verdict(
        "B23",
        {
            # FT-12: the kill really happened on both sides, and absolute bounds as well as the
            # on-vs-off comparison (one noisy run used to decide it; 2 calls of slack each).
            "the replica took over in both runs": all(promoted.values()) and len(promoted) == 2,
            "losses with the wait on <= with it off (+2)": losses["on"] <= losses["off"] + 2,
            "duplicates with the wait on <= with it off (+2)": dups["on"] <= dups["off"] + 2,
            "every failure with the wait on <= 5 % of its calls": failed["on"]
            <= 0.05 * max(1, calls["on"]),
        },
        [
            f"lost sends: off {losses['off']}, on {losses['on']}; "
            f"duplicates: off {dups['off']}, on {dups['on']}; "
            f"all failures: off {failed['off']}/{calls['off']}, on {failed['on']}/{calls['on']}"
        ],
    )


async def f2() -> bool:
    """Health summary (plan 0.2.0 F2): its states follow a real backlog, and it is cheap.

    Pass: an idle model is `healthy`; with a backlog over its `health_summary` marks it turns
    `critical` (reasons `waiting` and `oldest_wait`) within 20 s; once the backlog is cancelled
    it is `healthy` again within 15 s; a summary read takes <= 200 ms at p95.
    """
    marks = {
        "waiting_warning": 10,
        "waiting_critical": 40,
        "wait_warning_secs": 3,
        "wait_critical_secs": 8,
        "budget_warning_pct": 100,  # budget use is not what this gate drives
        "budget_critical_pct": 100,
    }
    doc = registry(
        {"chat": {"rpm": 30, "worker_slots": 10, "health_summary": marks}},
        accounts=1,
        regions=1,
        defaults={"timeout_secs": 300, "timeout_max_secs": 300, "max_wait_secs": 300},
    )
    states: list[tuple[float, str, list[str]]] = []
    reads: list[float] = []
    idle = critical_at = healthy_after = None
    async with Gate(doc) as gate:
        await gate.workers(2)
        (queue,) = await gate.callers(1)
        admin = queue.admin
        run = Run()

        async def summary() -> dict[str, Any]:
            t0 = time.monotonic()
            row: dict[str, Any] = (await admin.status.health("chat"))["models"][0]
            reads.append(time.monotonic() - t0)
            states.append((time.monotonic() - run.t0, row["state"], row["reasons"]))
            return row

        idle = (await summary())["state"]
        for _ in range(19):
            await summary()
        tasks = [asyncio.create_task(run.call(queue, "chat", chat())) for _ in range(80)]
        sent = time.monotonic()
        while time.monotonic() - sent < 20:
            row = await summary()
            if row["state"] == "critical" and {"waiting", "oldest_wait"} <= set(row["reasons"]):
                critical_at = time.monotonic() - sent
                break
            await asyncio.sleep(0.5)
        await admin.requests.cancel(model="chat", state="waiting", actor="gate", dry_run=False)
        cancelled = time.monotonic()
        while time.monotonic() - cancelled < 15:
            if (await summary())["state"] == "healthy":
                healthy_after = time.monotonic() - cancelled
                break
            await asyncio.sleep(0.5)
        await asyncio.gather(*tasks, return_exceptions=True)
    read_p95 = p95(reads) * 1000
    seen = sorted({state for _, state, _ in states}, key=("healthy", "warning", "critical").index)
    return verdict(
        "F2",
        {
            "idle model is healthy": idle == "healthy",
            "backlog turns critical within 20 s": critical_at is not None,
            "healthy again within 15 s of the cancel": healthy_after is not None,
            "summary read p95 <= 200 ms": read_p95 <= 200,
        },
        [
            f"states seen {seen}; critical after {critical_at or 0:.1f}s, healthy "
            f"{healthy_after or 0:.1f}s after the cancel; read p95 {read_p95:.0f} ms over "
            f"{len(reads)} reads; calls {len(run.results)} (failures {run.kinds() or 0})"
        ],
    )


async def _local_pods(env: dict[str, str], specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Client processes (`gates_fleet.py client`), started, then all sent `go`; their reports."""
    procs = []
    try:
        for spec in specs:
            log = log_file("client")
            procs.append(
                subprocess.Popen(  # noqa: ASYNC220  # started once each, before the run
                    [PY, str(Path(__file__).parent / "gates_fleet.py"), "client", json.dumps(spec)],
                    env=env,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=log,
                )
            )
            log.close()

        async def line(proc: Any) -> dict[str, Any]:
            while True:
                raw = await asyncio.wait_for(asyncio.to_thread(proc.stdout.readline), 600)
                if not raw:
                    raise RuntimeError(f"client process ended (exit {proc.poll()})")
                if raw.startswith(b"{"):
                    return dict(json.loads(raw))

        for proc in procs:
            await line(proc)  # ready
        for proc in procs:
            assert proc.stdin is not None
            proc.stdin.write(b"go\n")
            proc.stdin.flush()
        return list(await asyncio.gather(*(line(proc) for proc in procs)))
    finally:
        await asyncio.gather(*(kill_and_reap(p) for p in procs), return_exceptions=True)


BL1_RPM = 600  # the registry's rpm and fakellm's own limit (one deployment)
BL1_PROCESSES = 4
BL1_OFFERED = 1.5  # of the rpm
BL1_TIMEOUT_SECS = 30


async def bl1() -> bool:
    """Local mode at fleet level: 4 processes share one provider quota (gap G9, LM-15).

    `LLMQ_EXECUTION_MODE=local`, `LLMQ_LOCAL_PROCESS_COUNT=4` (each process takes a quarter of
    the rpm), fakellm limited to the registry's rpm, 150 % offered for 3 minutes (`--short`:
    40 s). The model uses `fixed` minute windows: a smooth bucket lets its burst through once
    per process on top of the minute. Pass: fakellm never saw more than its rpm in a wall-clock
    minute (+2 at the boundary); every call answered or `rate_limited`, none after its deadline.
    """
    secs = 40.0 if SHORT else 180.0
    key = f"{CHAT_ID}@aws-0/{REGIONS[0]}"
    scenario = {**FAST, "models": {key: {"rpm": BL1_RPM}}}
    doc = registry(
        {
            "chat": {
                "rpm": BL1_RPM,
                "rate_window": "fixed",
                "timeout_secs": BL1_TIMEOUT_SECS,
                "attempt_timeout_secs": 10,
                "max_wait_secs": 20,
            }
        },
        accounts=1,
        regions=1,
    )
    rate = BL1_OFFERED * BL1_RPM / 60 / BL1_PROCESSES  # per process
    async with Gate(
        doc,
        scenario,
        LLMQ_EXECUTION_MODE="local",
        LLMQ_LOCAL_PROCESS_COUNT=str(BL1_PROCESSES),
    ) as gate:
        specs = [
            {"op": "converse", "model": "chat", "calls": round(rate * secs), "rate": rate,
             "tag": f"bl1-{k}", "priority_every": 0}
            for k in range(BL1_PROCESSES)
        ]  # fmt: skip
        reports = await _local_pods(gate.env, specs)
        provider = (await asyncio.to_thread(gate.fake.stats)).get(key) or {}
    calls = [r for report in reports for r in report["results"]]
    kinds = collections.Counter(r.get("kind", "ok") for r in calls)
    bad = {k: n for k, n in kinds.items() if k not in ("ok", "rate_limited")}
    late = [r["secs"] for r in calls if r["secs"] > BL1_TIMEOUT_SECS + 5 + 1]
    by_minute = {int(m): int(n) for m, n in (provider.get("by_minute") or {}).items()}
    worst = max(by_minute.values(), default=0)
    return verdict(
        "BL1",
        {
            f"provider: never over {BL1_RPM} in a minute (+2)": worst <= BL1_RPM + 2,
            "every call answered or rate_limited": not bad,
            "no answer after its deadline": not late,
            "calls were served": kinds["ok"] > 0,
        },
        [
            f"{len(calls)} calls from {BL1_PROCESSES} local-mode processes over {secs:.0f}s "
            f"({BL1_OFFERED:.0%} of rpm {BL1_RPM}); outcomes {dict(kinds)}",
            f"provider calls per wall-clock minute {sorted(by_minute.items())}; throttled "
            f"{provider.get('throttled', 0)}; slowest answer "
            f"{max((r['secs'] for r in calls), default=0):.1f}s",
        ],
    )


def _sha(prefix: str, body: str) -> str:
    """The EVALSHA id of one request script under `prefix` (requests.py registers it so)."""
    import hashlib

    from llm_queue.engine.expiry import prelude

    return hashlib.sha1((prelude(prefix) + body).encode()).hexdigest()  # Redis' script id


async def _index_cost(index: bool, calls: int) -> tuple[float, dict[str, float], list[str]]:
    """B19's burst with the request index on or off: scripts per call, usec per script call.

    Server times come from the store's slow log at a 0 us threshold (raised for the run, then
    put back): every EVALSHA with its duration, matched to REGISTER_SEND / SETTLE_DONE by sha.
    """
    from llm_queue.engine.requests import REGISTER_SEND, SETTLE_DONE

    doc = registry({"chat": {"rpm": 1_000_000, "worker_slots": 100}}, accounts=1, regions=1)
    async with Gate(doc, LLMQ_REQUESTS_INDEX_ENABLED="true" if index else "false") as gate:
        await gate.workers(4)
        queues = await gate.callers(2)
        warm = Run()
        await paced(20, 1, lambda i: warm.call(queues[i % 2], "chat", chat()))  # scripts loaded
        client = gate.store.client
        keep = {k: client.config_get(k)[k] for k in ("slowlog-log-slower-than", "slowlog-max-len")}
        run = Run()
        try:
            client.config_set("slowlog-max-len", 1_000_000)
            client.config_set("slowlog-log-slower-than", 0)
            client.slowlog_reset()
            # Idle polling (and anything else on the shared store) subtracted, as B22 does:
            # 400 calls in 2 s moved 4.45 -> 5.07 scripts a call between two identical runs.
            idle_t0, idle0 = time.monotonic(), gate.store.script_calls()
            await asyncio.sleep(3)
            idle_rate = (gate.store.script_calls() - idle0) / (time.monotonic() - idle_t0)
            t0, before = time.monotonic(), gate.store.script_calls()
            await paced(200, calls / 200, lambda i: run.call(queues[i % 2], "chat", chat()))
            await asyncio.sleep(2)
            secs = time.monotonic() - t0
            scripts = gate.store.script_calls() - before - idle_rate * secs
            log = client.slowlog_get(1_000_000)
        finally:
            for name, value in keep.items():
                client.config_set(name, value)
            client.slowlog_reset()
        names = {
            _sha(gate.prefix, REGISTER_SEND): "REGISTER_SEND",
            _sha(gate.prefix, SETTLE_DONE): "SETTLE_DONE",
        }
    durations: dict[str, list[int]] = collections.defaultdict(list)
    for entry in log:
        parts = bytes(entry["command"]).split()
        if len(parts) > 1 and parts[0].upper() == b"EVALSHA" and parts[1].decode() in names:
            durations[names[parts[1].decode()]].append(int(entry["duration"]))
    # The median: on Docker Desktop the mean of one script moved by 200-300 us run to run.
    usec = {name: float(statistics.median(v)) for name, v in durations.items()}
    failed = [r.kind for r in run.results if not r.ok]
    return scripts / max(1, len(run.results)), usec, failed


async def gt1() -> bool:
    """Request index cost (0.2.0 design G-T1): B19's burst with the index off, then on.

    Pass: store scripts per call unchanged (the index rides in existing scripts) and the store
    time of REGISTER_SEND + SETTLE_DONE (median usec per script call) within +50 us of the
    index-off run. `--short`: 400 calls a side instead of 2,000.
    """
    calls = 400 if SHORT else 2000
    off_scripts, off_usec, off_failed = await _index_cost(False, calls)
    on_scripts, on_usec, on_failed = await _index_cost(True, calls)
    pair = ("REGISTER_SEND", "SETTLE_DONE")
    off_sum = sum(off_usec.get(n, math.nan) for n in pair)
    on_sum = sum(on_usec.get(n, math.nan) for n in pair)
    return verdict(
        "G-T1",
        {
            "every call served (both runs)": not off_failed and not on_failed,
            "both scripts seen in the slow log (both runs)": all(
                n in u for n in pair for u in (off_usec, on_usec)
            ),
            # B19 prints one decimal (11.6): a tenth of a script of noise from idle polling.
            "store scripts per call unchanged (+0.1)": on_scripts <= off_scripts + 0.1,
            "REGISTER_SEND + SETTLE_DONE within +50 us of the index off": on_sum <= off_sum + 50,
        },
        [
            f"{calls} calls a run; failures off {off_failed[:3] or 0}, on {on_failed[:3] or 0}",
            f"store scripts per call: off {off_scripts:.2f}, on {on_scripts:.2f}",
            "median usec per script call: off "
            + ", ".join(f"{k} {v:.0f}" for k, v in sorted(off_usec.items()))
            + "; on "
            + ", ".join(f"{k} {v:.0f}" for k, v in sorted(on_usec.items()))
            + f"; sum off {off_sum:.0f}, on {on_sum:.0f} (+{on_sum - off_sum:.0f})",
        ],
    )


GT2_TENANTS = 20


async def _timed(fn: Callable[[], Awaitable[Any]], times: int = 1) -> tuple[float, Any]:
    """Median milliseconds of `times` runs of `fn`, and its last result.

    `perf_counter`: Windows' `monotonic` ticks every 15.6 ms, so a 20 ms mark read 0, 16 or 31.
    """
    spent, out = [], None
    for _ in range(times):
        t0 = time.perf_counter()
        out = await fn()
        spent.append((time.perf_counter() - t0) * 1000)
    return statistics.median(spent), out


async def gt2() -> bool:
    """Admin latency at scale (0.2.0 design G-T2): 10,000 waiting calls of 20 tenants.

    No worker runs, so every call waits in its queue. Pass: `list` 100 rows <= 20 ms, `count`
    of one tenant <= 100 ms, reprioritize 1,000 <= 2 s, cancel all 10,000 <= 3 s (local stack;
    a machine note as B91), and every cancelled caller gets `cancelled`. `--short`: 1,000
    waiting, reprioritize 100 (same marks).
    """
    waiting = 1_000 if SHORT else 10_000
    moves = waiting // 10
    doc = registry(
        {"chat": {"rpm": 1_000_000, "timeout_secs": 300, "max_wait_secs": 300}},
        accounts=1,
        regions=1,
    )
    async with Gate(doc) as gate:
        queues = await gate.callers(2)
        llmq = queues[0]
        engine = llmq._engine
        assert hasattr(engine, "requests_admin"), "G-T2 needs queue mode"
        admin = engine.requests_admin
        outcomes: collections.Counter[str] = collections.Counter()

        async def one(i: int) -> None:
            try:
                await queues[i % 2].converse(
                    "chat", chat(f"g-t2 {i}"), tenant_id=f"tenant-{i % GT2_TENANTS}"
                )
                outcomes["ok"] += 1
            except CallFailed as exc:
                outcomes[exc.kind] += 1

        tasks = [asyncio.create_task(one(i)) for i in range(waiting)]
        indexed = 0
        t0 = time.monotonic()
        while time.monotonic() - t0 < 120:
            indexed = (await admin.count("chat")).total
            if indexed >= waiting:
                break
            await asyncio.sleep(0.5)
        fill_secs = time.monotonic() - t0
        # Each caller reads its result once `poll_secs` (5 s) after its send, the safety net.
        # These callers share this process's loop with the admin: measured during the last
        # 5 s of those reads, a list of 100 took 0.5 s of other callers' work. Wait them out.
        await asyncio.sleep(engine.settings.poll_secs + 1)
        list_ms, page = await _timed(lambda: admin.list("chat", limit=100), 5)
        count_ms, counts = await _timed(lambda: admin.count("chat", tenant="tenant-0"), 3)
        move_ms, moved = await _timed(
            lambda: admin.reprioritize(
                "chat", priority=True, limit=moves, actor="g-t2", dry_run=False
            )
        )
        cancel_ms, cancelled = await _timed(
            lambda: admin.cancel("chat", state="waiting", actor="g-t2", dry_run=False)
        )
        _, pending = await asyncio.wait(tasks, timeout=30)
        for task in pending:
            task.cancel()
    return verdict(
        "G-T2",
        {
            f"{waiting} calls indexed as waiting": indexed >= waiting,
            "list 100 rows <= 20 ms": len(page.items) == 100 and list_ms <= 20,
            "count of one tenant <= 100 ms": count_ms <= 100,
            f"reprioritize {moves} <= 2 s": moved.changed == moves and move_ms <= 2000,
            f"cancel {waiting} <= 3 s": cancelled.changed == waiting and cancel_ms <= 3000,
            "every caller answered cancelled within 30 s": not pending
            and outcomes["cancelled"] == waiting,
        },
        [
            f"{waiting} waiting of {GT2_TENANTS} tenants, indexed in {fill_secs:.1f}s "
            f"(index total {indexed})",
            f"list 100: {list_ms:.1f} ms; count one tenant: {count_ms:.1f} ms "
            f"({counts.total} calls)",
            f"reprioritize: {moved.changed}/{moves} moved in {move_ms:.0f} ms "
            f"(skipped {moved.skipped or 0})",
            f"cancel: {cancelled.changed}/{waiting} in {cancel_ms:.0f} ms "
            f"(skipped {cancelled.skipped or 0}); callers {dict(outcomes)}, "
            f"{len(pending)} unanswered",
        ],
    )


# ------------------------------------------------------------------------- F1 backfill (0.2.0)


async def _f1_run(backfill: bool) -> dict[str, Any]:
    """One offered load through the real PICK_TAKE on a simulated store clock.

    Callers re-ask as a held call does (every min(wait, 1 s)); a grant is finished at once with
    its tokens kept, so only the budget limits the flow. Returns waits by kind and per-minute use.
    """
    import heapq

    from gates_fleet import STORE  # here, so the shared import block stays as it was

    from llm_queue.engine.admission import Admission, Ask, Candidate, owner_token
    from llm_queue.engine.keys import Keys
    from llm_queue.engine.store_client import store_client
    from llm_queue.registry.store import RegistryStore

    tpm, minute_ms = 60_000, 60_000
    start = 28_333_400 * minute_ms  # a clock minute's start, far from the real one
    doc = {
        "registry_version": 1,
        "accounts": {"aws-a": {"kind": "aws"}},
        "models": {
            "m": {
                "type": "chat",
                "provider": "bedrock",
                "model_id": "x.model-1:0",
                "rate_window": "fixed",
                "rate_limits": "tpm",
                "tpm": tpm,
                "max_concurrency": 10_000,
                "queue": {"backfill": backfill},
                "deployments": {"aws-a": {"confirmed": ["r1"]}},
            }
        },
    }
    client = store_client(STORE)
    prefix = new_prefix()
    clock = [start]
    try:
        reg = RegistryStore(client, prefix, config=doc)
        await reg.boot()
        admission = Admission(client, Keys(prefix), clock_ms=lambda: clock[0])
        # Offered per minute: large normal calls (36,000, one near the minute's end), small
        # normal calls (18,000) and priority calls (6,000): the whole 60,000 tokens, normal
        # calls above their 48,000 class share, so a large call often waits for the next minute.
        calls: list[tuple[int, str, str, int]] = []  # (arrival ms, kind, class, tokens)
        for m in range(5):
            base = start + m * minute_ms
            calls += [(base + at, "large", "n", 12_000) for at in (20_000, 40_000, 55_000)]
            calls += [(base + 500 + i * 1_000, "small", "n", 300) for i in range(60)]
            calls += [(base + 3_000 + i * 10_000, "priority", "p", 1_000) for i in range(6)]
        calls.sort()
        events = [(t, i) for i, (t, *_rest) in enumerate(calls)]
        heapq.heapify(events)
        waits: dict[str, list[float]] = {"large": [], "small": [], "priority": []}
        used: dict[int, int] = collections.Counter()
        while events:
            t, i = heapq.heappop(events)
            clock[0] = t
            arrived, kind, cls, tokens = calls[i]
            rid = f"c{i}"
            ask = Ask(
                "m",
                rid,
                "w1",
                1,
                "p" if cls == "p" else "n",
                float(i + 1),
                [Candidate("aws-a/r1")],
                cost_t=tokens,
                backfill=backfill,
            )
            decision = await admission.pick_take(ask, reg.changes)
            if decision.kind == "granted":
                waits[kind].append((t - arrived) / 1000)
                used[t // minute_ms] += tokens
                await admission.finish(
                    "m", owner_token(rid, "w1", 1), rid, refund_r=0, refund_t=0, outcome="ok"
                )
            elif decision.kind == "wait":
                step = int(min(max(decision.wait_secs, 0.01), 1.0) * 1000)
                heapq.heappush(events, (t + step, i))
            else:
                raise RuntimeError(f"F1: unexpected {decision.kind} for {kind}")
        return {"waits": waits, "used": used, "end": (clock[0] - start) / 1000}
    finally:
        keys = [k async for k in client.scan_iter(match=f"{prefix}*", count=500)]
        if keys:
            await client.delete(*keys)
        await client.aclose()


async def f1() -> bool:
    """Size-aware backfill (plan F1): the same offered load with backfill off and on.

    Pass: small-call p95 lower with backfill on; large and priority call p95 not above off's
    by more than 10 % (+0.1 s); every call done sooner or as soon (throughput); no clock minute
    over the model's tpm in either run.
    """
    off, on = await _f1_run(False), await _f1_run(True)
    p = {
        run: {k: p95(v) for k, v in res["waits"].items()} for run, res in (("off", off), ("on", on))
    }

    def within(kind: str) -> bool:
        return p["on"][kind] <= p["off"][kind] * 1.1 + 0.1

    return verdict(
        "F1",
        {
            "small-call p95 lower with backfill": p["on"]["small"] < p["off"]["small"],
            "large-call p95 within 10 % of off": within("large"),
            "priority p95 within 10 % of off": within("priority"),
            "all calls done no later with backfill": on["end"] <= off["end"],
            "no minute over tpm 60,000": max([*off["used"].values(), *on["used"].values()])
            <= 60_000,
        },
        [
            f"p95 wait (s) off: {p['off']}",
            f"p95 wait (s) on:  {p['on']}",
            f"last grant (s): off {off['end']:.1f}, on {on['end']:.1f}",
            f"most tokens in a minute: off {max(off['used'].values())}, "
            f"on {max(on['used'].values())}",
        ],
    )


GATES: dict[str, Callable[[], Coroutine[Any, Any, bool]]] = {
    "B17": b17,
    "B19": b19,
    "B22": b22,
    "B23": b23,
    "B33": b33,
    "B34": b34,
    "B56": b56_b93,
    "B91": b91,
    "B92": b92,
    "B94": b94,
    "B95": b95,
    "B96": b96,
    "B97": b97,
    "F2": f2,
    "BL1": bl1,
    "G-T1": gt1,
    "G-T2": gt2,
    "F1": f1,
}


SLOW_GATES = frozenset({"B17", "B91", "B92", "BL1", "G-T2"})  # left out by --quick
SHORT = "--short" in sys.argv  # BL1 / G-T1 / G-T2 scaled down to prove the harness


def main() -> int:
    """Run the chosen gates; exit 1 if any fails."""
    setup_logging()
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("gates", nargs="*", help=f"gate names (default: all): {', '.join(GATES)}")
    parser.add_argument("--quick", action="store_true", help="leave out the slow gates")
    parser.add_argument("--list", action="store_true", help="list the gates and exit")
    parser.add_argument("--seed", type=int, help="replay the random choices of a run")
    parser.add_argument("--record-baseline", action="store_true", help="kept for check.py")
    parser.add_argument(
        "--short", action="store_true", help="BL1, G-T1, G-T2 scaled down (a harness smoke)"
    )
    # FT-27: argparse, so `--quick B19` works and a typo is an error, not a KeyError.
    opts = parser.parse_args()
    seed = opts.seed if opts.seed is not None else random.randrange(2**31)
    random.seed(seed)
    print(f"  seed {seed} (replay with --seed {seed})", flush=True)
    if opts.list:  # before any output folder is needed
        for name, fn in GATES.items():
            print(f"{name:4} {(fn.__doc__ or '').splitlines()[0]}")
        return 0
    unknown = [g for g in opts.gates if g not in GATES]
    if unknown:
        parser.error(f"unknown gate(s) {unknown}; have {', '.join(GATES)}")
    chosen = opts.gates or list(GATES)
    if opts.quick:
        chosen = [name for name in chosen if name not in SLOW_GATES]
    print(f"  logs of worker processes: {log_dir()}", flush=True)
    results: dict[str, bool] = {}
    for name in chosen:
        print(f"[{name}]", flush=True)
        started = time.monotonic()
        results[name] = asyncio.run(GATES[name]())
        print(f"  ({time.monotonic() - started:.0f}s)", flush=True)
    failed = [n for n, ok in results.items() if not ok]
    print("ALL PASS" if not failed else f"FAILED: {', '.join(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
