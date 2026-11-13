"""Scenarios: one fault (or a chosen pair) at a time, measured by calls, each with a pass rule.

Every scenario runs three phases of calls against worker processes and fakellm, never real
Bedrock: `before` (healthy), `during` (the fault is on, and calls keep coming until it ends),
`after` (the fault is gone and the system has settled). A phase is a number of calls, not a
time, so a scenario takes as long as its fault: 10-40 s with the fast profile.

Each run is one scenario x one load (`under` 50 % of the budget, `at` 100 %, `burst` 300 %).
Common pass rule: `before` and `after` calls all succeed (a burst may refuse calls on time,
as `rate_limited` or `deadline_exceeded`, in any phase); `during` fails only with the kinds
the scenario allows; no answer arrives after its deadline; nothing is left in progress at the
end. Some scenarios add checks (the provider never called again, one run for two callers ...).

    python scripts/scenarios.py --list
    python scripts/scenarios.py S4 P1            # these, under every load
    python scripts/scenarios.py S4 --load at     # one load
    python scripts/scenarios.py --group store    # store worker provider registry caller pairs
    python scripts/scenarios.py all              # every scenario and pair, every load
    python scripts/scenarios.py S4 --profile prod  # production timers (slower)
    python scripts/scenarios.py all --jobs 4     # 4 runs at a time (default; 1: one by one)

In parallel, each run is its own process with its own fakellm and store-proxy ports and its
own log (`reports/scenarios-<time>/<id>-<load>.log`, worker logs beside it). Runs that change
something shared run alone, after the others: S9 (the store's memory limit) and the Docker
failover drills (fixed ports).

Profiles: `fast` (default) shortens the timers so faults resolve in seconds (lease 3 s,
visibility 2 s, caller TTL 5 s, deadline 10 s, attempt 3 s, wait 8 s); `prod` keeps the
production values. Each run's worker and fakellm logs go to `reports/` (see gates_fleet).
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import dataclasses
import json
import os
import random
import subprocess
import sys
import time
import urllib.request
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import failover  # needs the sys.path entry
from gates import Gate, Result, Run, chat, registry
from gates_fleet import (
    CHAT_ID,
    EMBED_ID,
    FAKE_PORT,
    PY,
    ROOT,
    log_dir,
    log_file,
    setup_logging,
)

from llm_queue import CallFailed, LLMQueue, StoreUnavailable

PROXY_PORT = int(os.environ.get("LLMQ_SCENARIO_PROXY_PORT", "6389"))  # one per parallel run
QUEUE_PROXY_PORT = PROXY_PORT + 100  # a separate queue store (LLMQ_QUEUE_STORE_URL), L3
REGIONS = ("us-east-1", "us-east-2")  # gates_fleet.registry's first two
RPM = 600  # per deployment: 2 deployments, 20 chat calls a second of budget
BURST = int(RPM * 0.1)  # rate_burst's default: the smooth bucket holds a tenth of the minute
STREAM_ID = "openai.gpt-oss-120b-1:0"  # the streaming model (its own fakellm keys), L2
PHASE_CALLS = {"before": 30, "during": 60, "after": 30}
LOADS = {"under": 0.5, "at": 1.0, "burst": 3.0}


@dataclasses.dataclass(frozen=True)
class Profile:
    """Timers for one profile: process settings (env) and registry defaults."""

    env: dict[str, str]
    defaults: dict[str, Any]
    fault_secs: float  # how long a timed fault stays on
    deadline_secs: float  # timeout_secs, for the "no answer after its deadline" check


PROFILES = {
    "fast": Profile(
        env={
            "LLMQ_LEASE_SECS": "3",
            "LLMQ_CALLER_TTL_SECS": "10",  # 5 s: one stray caller_gone at 300 % (busy driver?)
            "LLMQ_STATUS_TTL_SECS": "30",
            "LLMQ_CFG_RELOAD_SECS": "1",
            "LLMQ_DRAIN_SECS": "5",
        },
        defaults={
            "timeout_secs": 10,
            "attempt_timeout_secs": 3,
            "max_wait_secs": 8,
            "queue": {"visibility_secs": 2},
            # Recovery in seconds, not minutes: an open breaker or a disabled deployment is
            # tried again soon after the fault ends (prod: 15 s and 15 min).
            "breaker_cooldown_secs": 3,
            "health": {"probe_after_secs": 5, "disable_window_secs": 30},
        },
        fault_secs=5,
        deadline_secs=10,
    ),
    "prod": Profile(
        env={},
        defaults={"timeout_secs": 30, "attempt_timeout_secs": 10, "max_wait_secs": 20},
        fault_secs=20,
        deadline_secs=30,
    ),
}
CALLER_MARGIN_SECS = 5.0  # EngineSettings.caller_margin_secs


# ------------------------------------------------------------------------------ the store


class FaultProxy(failover.Proxy):  # type: ignore[misc]  # failover is a script module
    """The store as the processes see it, in front of the stack's Valkey.

    It can refuse new connections (`refuse`), drop open ones (`drop_all`), swallow traffic
    without closing anything (`freeze`: a half-open connection) or add latency (`delay_secs`).
    """

    def __init__(self) -> None:
        """Forwarding normally."""
        super().__init__()
        self.freeze = False
        self.delay_secs = 0.0

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(65536):
                if self.freeze:
                    continue  # swallowed: the sender waits for a reply that never comes
                if self.delay_secs:
                    await asyncio.sleep(self.delay_secs)
                writer.write(data)
                await writer.drain()
        except OSError:
            pass
        finally:
            writer.close()

    async def handle(self, client_r: Any, client_w: Any) -> None:
        """Forward one connection to the stack's Valkey (or refuse it)."""
        if self.refuse:
            client_w.close()
            return
        try:
            node_r, node_w = await asyncio.open_connection("127.0.0.1", 6379)
        except OSError:
            client_w.close()
            return
        self.open += [client_w, node_w]
        await asyncio.gather(self._pipe(client_r, node_w), self._pipe(node_r, client_w))


async def serve(proxy: FaultProxy, port: int = PROXY_PORT) -> None:
    """Keep the proxy listening, re-opening it if asyncio closed it.

    On Windows a failed accept (WinError 64: a client gave up mid-accept while the store was
    blocked) makes asyncio close the listening socket for good, and the store then looked
    down forever (scenario X1, a harness fault).
    """
    server = await asyncio.start_server(proxy.handle, "127.0.0.1", port)
    try:
        while True:
            await asyncio.sleep(0.2)
            if all(sock.fileno() == -1 for sock in server.sockets):
                server.close()
                server = await asyncio.start_server(proxy.handle, "127.0.0.1", port)
    finally:
        server.close()


# ------------------------------------------------------------------------------ one run


@dataclasses.dataclass
class Ctx:
    """What a scenario's fault can touch: the processes, the store proxy, fakellm, callers."""

    gate: Gate
    proxy: FaultProxy
    queues: list[LLMQueue]
    profile: Profile
    run: Run
    rate: float = 0.0  # calls a second of this run's load
    limit: float = 0.0  # seconds: an answer later than this is late
    queue_proxy: FaultProxy | None = None  # the separate queue store's proxy (L3)
    notes: list[str] = dataclasses.field(default_factory=list)
    checks: dict[str, bool] = dataclasses.field(default_factory=dict)

    def fake(self, method: str, path: str, body: Any = None) -> Any:
        """Call fakellm's admin API (blocking: run it in a thread)."""
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{FAKE_PORT}{path}", data=data, method=method
        )
        request.add_header("content-type", "application/json")
        with urllib.request.urlopen(request, timeout=10) as reply:
            return json.loads(reply.read() or b"null")

    async def provider(self, region: str, **behaviour: Any) -> None:
        """Change the fake provider's behaviour in one region (`{}` keys reset it)."""
        await asyncio.to_thread(self.fake, "PATCH", f"/admin/models/{CHAT_ID}@{region}", behaviour)

    async def provider_calls(self, region: str) -> int:
        """Calls fakellm received in one region so far."""
        stats = await asyncio.to_thread(self.fake, "GET", "/admin/stats")
        entry = stats.get("models", {}).get(f"{CHAT_ID}@{region}") or {}
        return int(entry.get("calls", 0) or 0)

    async def edit(self, values: dict[str, Any], *, model: str | None = "chat") -> None:
        """A live registry edit."""
        # During a store outage the edit fails with StoreUnavailable; an operator would retry.
        for _ in range(60):
            try:
                await self.queues[0].registry.set(values, model=model, actor="scenario")
                return
            except StoreUnavailable:
                await asyncio.sleep(0.5)
        raise RuntimeError("the registry edit never reached the store")


@dataclasses.dataclass(frozen=True)
class Scenario:
    """One fault: what it does, which failures `during` may show, and its own checks."""

    sid: str
    group: str
    title: str
    rule: str
    fault: Callable[[Ctx], Awaitable[None]]  # turns the fault on, waits, turns it off
    allowed: frozenset[str] = frozenset()  # failure kinds `during` may show
    settle_secs: float = 0.0  # after the fault, before `after` starts
    extra: Callable[[Ctx], Awaitable[None]] | None = None  # scenario checks, after `after`
    env: dict[str, str] = dataclasses.field(default_factory=dict)  # more process settings
    models: dict[str, dict[str, Any]] = dataclasses.field(default_factory=dict)  # registry
    fake: dict[str, dict[str, Any]] = dataclasses.field(default_factory=dict)  # by model id
    share: float = 1.0  # of the load sent by `during` here (client processes send the rest)
    queue_proxy: bool = False  # LLMQ_QUEUE_STORE_URL through its own proxy


async def phase(
    ctx: Ctx, name: str, rate: float, calls: int, until: asyncio.Event | None
) -> list[Result]:
    """Send `calls` calls at `rate` a second (and more while `until` is not set)."""
    tasks: list[asyncio.Task[Result]] = []
    t0 = time.monotonic()
    i = 0
    while i < calls or (until is not None and not until.is_set()):
        llmq = ctx.queues[i % len(ctx.queues)]
        tasks.append(asyncio.create_task(ctx.run.call(llmq, "chat", chat(), priority=i % 5 == 0)))
        i += 1
        await asyncio.sleep(max(0.0, t0 + i / rate - time.monotonic()))
    return list(await asyncio.gather(*tasks))


async def run_one(scenario: Scenario, load: str, profile_name: str) -> bool:
    """Run one scenario under one load; print its numbers and verdict."""
    profile = PROFILES[profile_name]
    doc = registry(
        {"chat": {"rpm": RPM, "worker_slots": 50}, **scenario.models},
        accounts=1,
        regions=len(REGIONS),
        defaults=profile.defaults,
    )
    proxy = FaultProxy()
    serving = [asyncio.create_task(serve(proxy))]
    scenario_doc = {
        "defaults": {"latency_ms": {"p50": 150, "p95": 250}},
        "models": {f"{CHAT_ID}@{r}": {} for r in REGIONS}
        | {f"{mid}@{r}": dict(b) for mid, b in scenario.fake.items() for r in REGIONS},
    }
    rate = LOADS[load] * RPM / 60 * len(REGIONS)
    limit = profile.deadline_secs + CALLER_MARGIN_SECS + 1
    started = time.monotonic()
    phases: dict[str, list[Result]] = {}
    env = {**profile.env, **scenario.env}
    queue_proxy = None
    if scenario.queue_proxy:
        queue_proxy = FaultProxy()
        serving.append(asyncio.create_task(serve(queue_proxy, QUEUE_PROXY_PORT)))
        env["LLMQ_QUEUE_STORE_URL"] = f"redis://:localdev@127.0.0.1:{QUEUE_PROXY_PORT}/0"
    try:
        store = f"redis://:localdev@127.0.0.1:{PROXY_PORT}/0"
        async with Gate(doc, scenario_doc, LLMQ_STORE_URL=store, **env) as gate:
            await gate.workers(3)
            queues = await gate.callers(2)
            ctx = Ctx(gate, proxy, queues, profile, Run(), rate, limit, queue_proxy)
            phases["before"] = await phase(ctx, "before", rate, PHASE_CALLS["before"], None)
            ended = asyncio.Event()

            async def fault() -> None:
                try:
                    await scenario.fault(ctx)
                finally:
                    ended.set()

            during_calls = max(10, round(PHASE_CALLS["during"] * scenario.share))
            during = asyncio.create_task(
                phase(ctx, "during", rate * scenario.share, during_calls, ended)
            )
            await fault()
            phases["during"] = await during
            await asyncio.sleep(scenario.settle_secs)
            phases["after"] = await phase(ctx, "after", rate, PHASE_CALLS["after"], None)
            if scenario.extra is not None:
                await scenario.extra(ctx)
            # Nothing left in progress: a killed worker's lease ends and the sweep clears it.
            keys = [f"{gate.prefix}:{{m:{alias}}}:inprog" for alias in doc["models"]]
            for _ in range(60):
                inprog = sum(gate.store.client.zcard(key) for key in keys)
                if not inprog:
                    break
                await asyncio.sleep(0.5)
    finally:
        for task in serving:
            task.cancel()
    secs = time.monotonic() - started
    # Three times the budget: refusing on time (no budget, or no time left) is the right answer.
    allow_everywhere = {"rate_limited", "deadline_exceeded"} if load == "burst" else set()
    bad = {
        name: collections.Counter(
            r.kind
            for r in results
            if not r.ok
            and r.kind not in allow_everywhere
            and not (name == "during" and r.kind in scenario.allowed)
        )
        for name, results in phases.items()
    }
    late = [r for results in phases.values() for r in results if r.total_secs > limit]
    checks = {
        "before: all served": not bad["before"],
        f"during: failures only {sorted(scenario.allowed | allow_everywhere) or 'none'}": not bad[
            "during"
        ],
        "after: all served": not bad["after"],
        "no answer after its deadline": not late,
        "nothing in progress at the end": not inprog,
        **ctx.checks,
    }
    counts = {
        name: f"{sum(r.ok for r in results)}/{len(results)} ok"
        + (
            f" {dict(collections.Counter(r.kind for r in results if not r.ok))}"
            if any(not r.ok for r in results)
            else ""
        )
        for name, results in phases.items()
    }
    passed = all(checks.values())
    print(
        f"[{scenario.sid} {load}] {scenario.title} ({secs:.0f}s): "
        + "; ".join(f"{k} {v}" for k, v in counts.items()),
        flush=True,
    )
    for note in ctx.notes:
        print(f"    {note}")
    for name, ok in checks.items():
        if not ok:
            print(
                f"    FAIL {name}"
                + (f" {dict(bad[name.split(':')[0]])}" if name.split(":")[0] in bad else "")
            )
    if late:
        print(f"    late: {[round(r.total_secs, 1) for r in late[:5]]}")
    for name, results in phases.items():  # when each unexpected failure was sent, and its time
        shown = [r for r in results if not r.ok and r.kind in bad[name]][:5]
        for r in shown:
            print(f"    {name}: {r.kind} sent at {r.sent:.1f}s, answered after {r.total_secs:.1f}s")
    print(f"{'PASS' if passed else 'FAIL'}  {scenario.sid} {load}", flush=True)
    return passed


# ------------------------------------------------------------------------------ faults


async def _hold(ctx: Ctx) -> None:
    await asyncio.sleep(ctx.profile.fault_secs)


def _live(ctx: Ctx) -> list[Any]:
    return [w for w in ctx.gate.fleet if w.proc.poll() is None]


async def worker_kill(ctx: Ctx) -> None:
    """Kill -9 one worker mid-traffic; start a replacement."""
    await asyncio.sleep(1)
    random.choice(_live(ctx)).kill()
    await ctx.gate.workers(1)
    await _hold(ctx)


async def worker_restart(ctx: Ctx) -> None:
    """Graceful: a new worker first, then the old one drains and stops."""
    await asyncio.sleep(1)
    victim = random.choice(_live(ctx))
    await ctx.gate.workers(1)
    await victim.stop()


async def workers_all_down(ctx: Ctx) -> None:
    """Every worker killed; new ones only after the fault time."""
    await asyncio.sleep(1)
    for worker in _live(ctx):
        worker.kill()
    await _hold(ctx)
    await ctx.gate.workers(3)


async def store_block(ctx: Ctx) -> None:
    """Every store connection dropped, new ones refused."""
    ctx.proxy.refuse = True
    ctx.proxy.drop_all()
    await _hold(ctx)
    ctx.proxy.refuse = False


async def store_half_open(ctx: Ctx) -> None:
    """Connections stay open but nothing comes back (a blackholed node)."""
    ctx.proxy.freeze = True
    await _hold(ctx)
    ctx.proxy.freeze = False


async def store_slow(ctx: Ctx) -> None:
    """Every store packet 50 ms late."""
    ctx.proxy.delay_secs = 0.05
    await _hold(ctx)
    ctx.proxy.delay_secs = 0.0


async def store_reset(ctx: Ctx) -> None:
    """Every key of this run deleted (a flushed or replaced store)."""
    await asyncio.sleep(1)
    await asyncio.to_thread(ctx.gate.store.cleanup, ctx.gate.prefix)
    await _hold(ctx)


async def store_oom(ctx: Ctx) -> None:
    """The store refuses writes (maxmemory reached, noeviction)."""
    client = ctx.gate.store.client
    before = client.config_get("maxmemory")["maxmemory"]
    policy = client.config_get("maxmemory-policy")["maxmemory-policy"]
    used = int(client.info("memory")["used_memory"])
    client.config_set("maxmemory-policy", "noeviction")
    client.config_set("maxmemory", str(max(1, used - 1_000_000)))
    try:
        await _hold(ctx)
    finally:
        client.config_set("maxmemory", before)
        client.config_set("maxmemory-policy", policy)


def _provider_fault(**behaviour: Any) -> Callable[[Ctx], Awaitable[None]]:
    async def fault(ctx: Ctx) -> None:
        await ctx.provider(REGIONS[0], **behaviour)
        await _hold(ctx)
        reset = dict.fromkeys(behaviour)
        await ctx.provider(REGIONS[0], **reset)

    return fault


async def provider_both_down(ctx: Ctx) -> None:
    """Every region of the model failing (5xx)."""
    for region in REGIONS:
        await ctx.provider(region, force_error="InternalServerException")
    await _hold(ctx)
    for region in REGIONS:
        await ctx.provider(region, force_error=None)


async def rpm_cut(ctx: Ctx) -> None:
    """The budget cut to a quarter, live, then put back."""
    await ctx.edit({"rpm": RPM // 4})
    await _hold(ctx)
    await ctx.edit({"rpm": RPM})


async def deployment_off(ctx: Ctx) -> None:
    """One region disabled live, then enabled."""
    path = f"deployments.aws-0.overrides.{REGIONS[0]}.enabled"
    await ctx.edit({path: False})
    await _hold(ctx)
    await ctx.edit({path: True})


async def kill_switch(ctx: Ctx) -> None:
    """The kill switch: every call runs direct (no queue), same budgets."""
    await ctx.edit({"routing.kill_switch": True}, model=None)
    await _hold(ctx)
    await ctx.edit({"routing.kill_switch": False}, model=None)


async def bad_edit(ctx: Ctx) -> None:
    """An invalid live edit is refused; the version in use does not change."""
    version = ctx.queues[0].registry.current.registry.version
    try:
        await ctx.edit({"rpm": -5})
        ctx.checks["bad edit refused"] = False
    except Exception:  # the refusal
        ctx.checks["bad edit refused"] = True
    await asyncio.sleep(1)
    ctx.checks["version unchanged"] = ctx.queues[0].registry.current.registry.version == version
    await _hold(ctx)


async def caller_cancel(ctx: Ctx) -> None:
    """Half of 40 slow calls cancelled by their caller; the rest served."""
    await ctx.provider(REGIONS[0], latency_ms={"p50": 2000, "p95": 2000})
    await ctx.provider(REGIONS[1], latency_ms={"p50": 2000, "p95": 2000})
    tasks = [asyncio.create_task(ctx.queues[0].converse("chat", chat())) for _ in range(40)]
    await asyncio.sleep(0.5)
    for task in tasks[:20]:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # Cancelling some must not cancel the others: each kept call is served, or (under a
    # burst, competing with three times the budget) refused on time, never `cancelled`.
    on_time = {"rate_limited", "deadline_exceeded"}
    kept_ok = all(
        not isinstance(r, BaseException) or (isinstance(r, CallFailed) and r.kind in on_time)
        for r in results[20:]
    )
    ctx.checks["uncancelled calls served or refused on time"] = kept_ok
    for region in REGIONS:
        await ctx.provider(region, latency_ms=None)


async def caller_dies(ctx: Ctx) -> None:
    """A caller process with 100 calls waiting is killed; its calls must not stay held."""
    proc = subprocess.Popen(  # noqa: ASYNC220  # one process, started once
        [PY, str(Path(__file__).parent / "gates_fleet.py"), "caller", "100", "chat"],
        env={**ctx.gate.env, "LLMQ_MODE": "caller"},
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    assert proc.stdin is not None
    assert proc.stdout is not None
    await asyncio.to_thread(proc.stdout.readline)
    proc.stdin.write(b"go\n")
    proc.stdin.flush()
    await asyncio.sleep(1)
    proc.kill()
    await _hold(ctx)


async def duplicate_id(ctx: Ctx) -> None:
    """Two callers, one request id, at once: one provider call, the same reply for both."""
    before = sum([await ctx.provider_calls(r) for r in REGIONS])
    rid = f"dup-{random.randrange(10**9)}"
    a, b = await asyncio.gather(
        ctx.queues[0].converse("chat", chat(), request_id=rid),
        ctx.queues[1].converse("chat", chat(), request_id=rid),
        return_exceptions=True,
    )
    after = sum([await ctx.provider_calls(r) for r in REGIONS])
    same = (
        not isinstance(a, BaseException)
        and not isinstance(b, BaseException)
        and a.response == b.response
    )
    ctx.checks["both callers got the same reply"] = same
    ctx.notes.append(f"provider calls for the shared id: {after - before} (other traffic included)")
    await _hold(ctx)


def pair(
    first: Callable[[Ctx], Awaitable[None]], second: Callable[[Ctx], Awaitable[None]]
) -> Callable[[Ctx], Awaitable[None]]:
    """Two faults at once."""

    async def both(ctx: Ctx) -> None:
        await asyncio.gather(first(ctx), second(ctx))

    return both


# ------------------------------------------------------------------------------ client pods


class Client:
    """A client pod: its own process (`gates_fleet.py client`), its own engine and calls."""

    def __init__(self, ctx: Ctx, spec: dict[str, Any], **env: str) -> None:
        """Start it (`LLMQ_MODE=caller` unless `env` says otherwise)."""
        log = log_file("client")
        self.spec = spec
        self.proc = subprocess.Popen(
            [PY, str(Path(__file__).parent / "gates_fleet.py"), "client", json.dumps(spec)],
            env={**ctx.gate.env, "LLMQ_MODE": "caller", **env},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
        )
        log.close()
        self.ready: dict[str, Any] = {}
        self.report: dict[str, Any] = {}

    async def _line(self) -> dict[str, Any]:
        assert self.proc.stdout is not None
        while True:
            line = await asyncio.to_thread(self.proc.stdout.readline)
            if not line:
                raise RuntimeError(f"client {self.spec['tag']} ended (exit {self.proc.poll()})")
            if line.startswith(b"{"):
                return dict(json.loads(line))

    async def start(self) -> None:
        """Until it printed its ready line (engine started, registry booted)."""
        self.ready = await self._line()

    def go(self) -> None:
        """Start its calls."""
        assert self.proc.stdin is not None
        self.proc.stdin.write(b"go\n")
        self.proc.stdin.flush()

    async def done(self) -> dict[str, Any]:
        """Its report, once every call has its answer."""
        self.report = await self._line()
        await asyncio.to_thread(self.proc.wait, 30)
        return self.report

    def kill(self) -> None:
        """Gone at once (cleanup)."""
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(10)


async def clients(ctx: Ctx, pods: list[Client], *, during: Awaitable[None] | None = None) -> None:
    """Start the pods one after another, start their calls together, wait for every answer.

    `during` (a fault) runs while the calls go on. The pods are killed if anything fails.
    """
    try:
        for pod in pods:
            if not pod.ready:  # some are started earlier (M2: the newer one first)
                await pod.start()
        for pod in pods:
            pod.go()
        waits = [pod.done() for pod in pods]
        if during is not None:
            await asyncio.gather(during, *waits)
        else:
            await asyncio.gather(*waits)
    finally:
        for pod in pods:
            pod.kill()


def judge_clients(
    ctx: Ctx, pods: list[Client], allowed: set[str], *, partial_ok: bool = False
) -> list[dict[str, Any]]:
    """The checks every client scenario shares; returns every call of every pod."""
    on_time = {"rate_limited", "deadline_exceeded"} if ctx.rate > 25 else set()
    calls = [r for pod in pods for r in pod.report["results"]]
    unexpected = collections.Counter(
        r["kind"]
        for r in calls
        if not r["ok"]
        and r["kind"] not in on_time | allowed
        and not (partial_ok and r.get("partial"))
    )
    mixed = [r["marker"] for r in calls if r["ok"] and not r.get("own_reply")]
    late = [r["secs"] for r in calls if r["secs"] > ctx.limit]
    ids = [r["request_id"] for r in calls if r.get("request_id")]
    ctx.checks["clients: every call answered (or refused on time)"] = not unexpected
    ctx.checks["clients: each call got its own reply"] = not mixed and len(ids) == len(set(ids))
    ctx.checks["clients: no answer after its deadline"] = not late
    for pod in pods:
        rs = pod.report["results"]
        kinds = dict(collections.Counter(r["kind"] for r in rs if not r["ok"]))
        ctx.notes.append(
            f"client {pod.spec['tag']}: {sum(r['ok'] for r in rs)}/{len(rs)} ok"
            + (f" {kinds}" if kinds else "")
            + f"; registry v{pod.report['version']}"
        )
    if unexpected:
        ctx.notes.append(f"clients: unexpected failures {dict(unexpected)}")
    if mixed:
        ctx.notes.append(f"clients: {len(mixed)} replies not their own, e.g. {mixed[:3]}")
    if late:
        ctx.notes.append(f"clients: late answers {sorted(late)[-5:]}")
    return calls


def _client_spec(ctx: Ctx, tag: str, share: float, secs: float = 10, **more: Any) -> dict[str, Any]:
    """Converse calls at `share` of the run's load for about `secs`."""
    rate = max(0.5, ctx.rate * share)
    calls = max(5, round(rate * secs))
    spec = {"op": "converse", "model": "chat", "calls": calls, "rate": rate, "tag": tag}
    return {**spec, "priority_every": 5, **more}


async def _budget_samples(ctx: Ctx, stop: asyncio.Event) -> list[tuple[float, list[int]]]:
    """The call counts fakellm saw per region of the chat model, every half second until `stop`."""
    samples: list[tuple[float, list[int]]] = []
    while True:
        stats = await asyncio.to_thread(ctx.fake, "GET", "/admin/stats")
        models = stats.get("models", {})
        counts = [int((models.get(f"{CHAT_ID}@{r}") or {}).get("calls", 0)) for r in REGIONS]
        samples.append((time.monotonic(), counts))
        if stop.is_set():
            return samples
        await asyncio.sleep(0.5)


def _over_budget(samples: list[tuple[float, list[int]]]) -> float:
    """The largest excess over the budget of any stretch between two samples (<= 0: none).

    The smooth window is a bucket: `BURST` calls at once, then RPM / 60 a second, per
    deployment. A half second of sampling slack is allowed at the rate.
    """
    worst = float("-inf")
    for i, (t1, c1) in enumerate(samples):
        for t2, c2 in samples[i + 1 :]:
            allowed = BURST + RPM / 60 * (t2 - t1 + 0.5)
            worst = max(worst, *(b - a - allowed for a, b in zip(c1, c2, strict=True)))
    return worst


async def many_clients(ctx: Ctx) -> None:
    """Four client pods (four services) on one model queue at once, with the usual workers."""
    pods = [Client(ctx, _client_spec(ctx, f"svc{k}", 0.2)) for k in range(4)]
    stop = asyncio.Event()
    sampler = asyncio.create_task(_budget_samples(ctx, stop))
    try:
        await clients(ctx, pods)
    finally:
        stop.set()
        samples = await sampler
    judge_clients(ctx, pods, set())
    excess = _over_budget(samples)
    ctx.checks["provider calls never above the budget"] = excess <= 0
    ctx.notes.append(
        f"budget: worst stretch {excess:+.1f} calls against the bucket "
        f"({BURST} + {RPM // 60}/s per region); provider calls {samples[-1][1]}"
    )


async def registry_versions(ctx: Ctx) -> None:
    """Two services whose documents differ in registry_version (and rpm): the newer is live."""
    doc = json.loads(ctx.gate.env["LLMQ_REGISTRY"])
    newer = json.loads(json.dumps(doc))
    newer["registry_version"] = int(doc["registry_version"]) + 1
    newer["models"]["chat"]["rpm"] = RPM * 2  # more budget: the old one would not hurt
    new_pod = Client(ctx, _client_spec(ctx, "new", 0.2), LLMQ_REGISTRY=json.dumps(newer))
    try:
        await new_pod.start()  # publishes v2 first
    except BaseException:
        new_pod.kill()
        raise
    old_pod = Client(ctx, _client_spec(ctx, "old", 0.2))  # the run's v1 document
    await clients(ctx, [new_pod, old_pod])
    judge_clients(ctx, [new_pod, old_pod], set())
    want = newer["registry_version"]
    ctx.checks["newer version live for both services"] = (
        new_pod.report["version"] == want
        and old_pod.ready["version"] == want
        and old_pod.report["version"] == want
        and new_pod.report["rpm"] == old_pod.report["rpm"] == RPM * 2
    )
    ctx.checks["older service logged registry_older_ignored"] = (
        "registry_older_ignored" in old_pod.report["events"]
    )
    await asyncio.sleep(2)  # LLMQ_CFG_RELOAD_SECS 1: the driver's callers reload
    seen = int(ctx.queues[0].registry.current.registry.version)
    ctx.checks["the other callers moved to the newer version"] = seen == want
    ctx.notes.append(
        f"versions: new pod {new_pod.report['version']}, old pod {old_pod.ready['version']} at "
        f"start, {old_pod.report['version']} at end; driver {seen}; "
        f"old pod events {old_pod.report['events']}"
    )


async def split_pods(ctx: Ctx) -> None:
    """Caller-only pods and worker-only pods (no `both`): every call served by a worker pod."""
    await ctx.gate.workers(2)  # two more worker-only pods
    pods = [Client(ctx, _client_spec(ctx, f"api{k}", 0.4)) for k in range(2)]
    await clients(ctx, pods)
    calls = judge_clients(ctx, pods, set())
    worker_ids = {w.worker_id for w in ctx.gate.fleet}
    caller_ids = {pod.ready["worker_id"] for pod in pods} | {q.engine.worker_id for q in ctx.queues}
    served = [r["worker"] for r in calls if r["ok"]] + [r.worker for r in ctx.run.results if r.ok]
    ctx.checks["every call served by a worker pod"] = all(w in worker_ids for w in served)
    ctx.checks["no caller pod served a call"] = not set(served) & caller_ids
    ctx.notes.append(
        f"served by {len(set(served))} worker ids of {len(worker_ids)} worker pods; "
        f"{len(set(served) - worker_ids)} other"
    )


STREAM_TOKENS = 120  # 2 s at 60 tokens a second, plus the first event's latency


async def streams_faulted(ctx: Ctx) -> None:
    """Streams running while the store is blocked and a worker is killed."""
    spec = {"op": "stream", "model": "stream", "rate": 2.0, "calls": 16, "priority_every": 0}
    pods = [
        Client(ctx, {**spec, "tag": f"stream{k}", "output_tokens": STREAM_TOKENS}) for k in range(2)
    ]

    async def faults() -> None:
        await asyncio.sleep(1.5)  # streams under way
        await pair(store_block, worker_kill)(ctx)

    await clients(ctx, pods, during=faults())
    allowed = {"worker_lost", "deadline_exceeded", "queue_unavailable"}
    judge_clients(ctx, pods, allowed, partial_ok=True)


async def queue_store_block(ctx: Ctx) -> None:
    """Only the separate queue store blocked; the state store keeps working."""
    assert ctx.queue_proxy is not None
    sent = len(ctx.run.results)
    ctx.queue_proxy.refuse = True
    ctx.queue_proxy.drop_all()
    await _hold(ctx)
    ctx.queue_proxy.refuse = False
    await asyncio.sleep(2)  # the calls sent while blocked come back
    caller_ids = {q.engine.worker_id for q in ctx.queues}
    direct = [r for r in ctx.run.results[sent:] if r.ok and r.worker in caller_ids]
    ctx.checks["calls ran direct while the queue store was down"] = bool(direct)
    ctx.notes.append(f"{len(direct)} calls ran direct (in the caller) during the block")


EMBED_BATCHES = 3  # Cohere: 96 texts a call


async def embeds_faulted(ctx: Ctx) -> None:
    """Embedding calls (3 batches of 96 each) while a worker is killed and the store blocked."""
    sys.path.insert(0, str(ROOT / "tools" / "fakellm"))
    from fakellm.synth import embedding

    outcomes: list[tuple[str, float, bool]] = []  # kind ("" ok), secs, vectors in order
    done = asyncio.Event()

    async def one(i: int) -> None:
        texts = [f"doc {i}-{j} {random.randrange(10**9)}" for j in range(96 * EMBED_BATCHES)]
        t0 = time.monotonic()
        try:
            result = await ctx.queues[i % 2].embed("cohere", texts, input_type="search_document")
            ordered = len(result.vectors) == len(texts) and all(
                abs(v[0] - embedding(t, 1024)[0]) < 1e-6
                and abs(v[1] - embedding(t, 1024)[1]) < 1e-6
                for v, t in zip(result.vectors, texts, strict=False)
            )
            outcomes.append(("", time.monotonic() - t0, ordered))
        except CallFailed as exc:
            outcomes.append((exc.kind, time.monotonic() - t0, True))

    async def load() -> None:
        tasks = []
        i = 0
        while not done.is_set() or i < 6:
            tasks.append(asyncio.create_task(one(i)))
            i += 1
            await asyncio.sleep(1)
        await asyncio.gather(*tasks)

    async def faults() -> None:
        await asyncio.sleep(1.5)
        try:
            await pair(store_block, worker_kill)(ctx)
        finally:
            done.set()

    await asyncio.gather(load(), faults())
    bad = collections.Counter(k for k, _, _ in outcomes if k and k not in ON_TIME)
    ctx.checks["embeddings: every call answered or refused on time"] = not bad and all(
        secs <= ctx.limit for _, secs, _ in outcomes
    )
    ctx.checks["embeddings: vectors in order"] = all(ordered for _, _, ordered in outcomes)
    kinds = dict(collections.Counter(k for k, _, _ in outcomes if k))
    ctx.notes.append(
        f"embeddings: {sum(1 for k, _, _ in outcomes if not k)}/{len(outcomes)} calls ok "
        f"({EMBED_BATCHES} batches each)"
        + (f" {kinds}" if kinds else "")
        + f"; slowest {max(secs for _, secs, _ in outcomes):.1f}s"
    )


def _s(
    sid: str,
    group: str,
    title: str,
    rule: str,
    fault: Callable[[Ctx], Awaitable[None]],
    *allowed: str,
    settle: float = 0.0,
    extra: Any = None,
    **kw: Any,
) -> Scenario:
    return Scenario(sid, group, title, rule, fault, frozenset(allowed), settle, extra, **kw)


ON_TIME = ("deadline_exceeded", "queue_unavailable", "rate_limited")
SCENARIOS = [
    _s(
        "S1",
        "worker",
        "worker killed (kill -9)",
        "its calls finish elsewhere; nothing held",
        worker_kill,
    ),
    _s("S2", "worker", "worker restarted (graceful)", "nothing lost", worker_restart),
    _s(
        "S3",
        "worker",
        "every worker down, then back",
        "calls wait, then served or refused on time",
        workers_all_down,
        *ON_TIME,
        settle=2,
    ),
    _s(
        "S4",
        "store",
        "store blocked",
        "refused on time while down; served after",
        store_block,
        *ON_TIME,
        settle=2,
    ),
    _s(
        "S5",
        "store",
        "store half-open (no replies)",
        "fails within the read timeout; nothing stuck",
        store_half_open,
        *ON_TIME,
        settle=17,
    ),
    _s(
        "S8",
        "store",
        "store reset (keys gone)",
        "running calls re-registered; nothing stuck",
        store_reset,
        *ON_TIME,
        settle=3,
    ),
    _s(
        "S9",
        "store",
        "store out of memory",
        "refused as queue_unavailable; no crash",
        store_oom,
        *ON_TIME,
        settle=2,
    ),
    _s("S10", "store", "store slow (+50 ms a packet)", "all served", store_slow),
    _s(
        "P1",
        "provider",
        "region throttled (429)",
        "other region serves",
        _provider_fault(force_error="ThrottlingException"),
    ),
    _s(
        "P2",
        "provider",
        "region failing (5xx)",
        "other region serves; breaker opens and recovers",
        _provider_fault(force_error="InternalServerException"),
    ),
    _s(
        "P3", "provider", "region hangs", "attempt timeout, next region", _provider_fault(hang=True)
    ),
    _s(
        "P4",
        "provider",
        "model not in region",
        "target error: other region serves",
        _provider_fault(force_error="ResourceNotFoundException"),
    ),
    _s(
        "P5",
        "provider",
        "bad credentials in a region",
        "other region serves",
        _provider_fault(force_error="UnrecognizedClientException"),
    ),
    _s(
        "P7",
        "provider",
        "every region failing (5xx)",
        "refused on time; served after",
        provider_both_down,
        "provider_error",
        "model_unavailable",
        "deadline_exceeded",
        "rate_limited",
        "retry_budget_exhausted",  # every region failing: the budget stops a retry storm
        # Breaker cooldown 3 s, health probe 5 s; a probe made while the fault was still on
        # backs off to 10 s, so recovery takes up to ~15 s (fast profile).
        settle=16,
    ),
    _s(
        "R1",
        "registry",
        "rpm cut to a quarter, live",
        "over-budget calls rate_limited",
        rpm_cut,
        "rate_limited",
        "deadline_exceeded",
    ),
    _s("R2", "registry", "region disabled, live", "other region serves", deployment_off),
    _s("R3", "registry", "kill switch on", "direct path, same budgets", kill_switch),
    _s("R4", "registry", "bad live edit", "refused; version unchanged", bad_edit),
    _s("C1", "caller", "callers cancel", "cancelled stop; others served", caller_cancel),
    _s("C2", "caller", "caller process dies", "its calls dropped; nothing held", caller_dies),
    _s("C3", "caller", "duplicate request id", "one run; same reply for both", duplicate_id),
    _s(
        "X1",
        "pairs",
        "store blocked + worker killed",
        "as S4",
        pair(store_block, worker_kill),
        *ON_TIME,
        settle=3,
    ),
    _s(
        "X2",
        "pairs",
        "store blocked + rpm cut",
        "as S4 and R1",
        pair(store_block, rpm_cut),
        *ON_TIME,
        settle=3,
    ),
    _s(
        "X3",
        "pairs",
        "worker killed + region throttled",
        "as S1 and P1",
        pair(worker_kill, _provider_fault(force_error="ThrottlingException")),
    ),
    _s(
        "X4",
        "pairs",
        "store slow + region failing",
        "as S10 and P2",
        pair(store_slow, _provider_fault(force_error="InternalServerException")),
    ),
    _s(
        "X5",
        "pairs",
        "store half-open + worker killed",
        "as S5 and S1",
        pair(store_half_open, worker_kill),
        *ON_TIME,
        settle=17,
    ),
    _s(
        "X6",
        "pairs",
        "store reset + worker killed",
        "as S8 and S1",
        pair(store_reset, worker_kill),
        *ON_TIME,
        settle=3,
    ),
    _s(
        "X7",
        "pairs",
        "rpm cut + region throttled",
        "as R1 and P1",
        pair(rpm_cut, _provider_fault(force_error="ThrottlingException")),
        "rate_limited",
        "deadline_exceeded",
    ),
    _s(
        "X8",
        "pairs",
        "region disabled + other region hangs",
        "refused on time; served after",
        pair(deployment_off, _provider_fault(hang=True)),
        *ON_TIME,
        "model_unavailable",
        "provider_error",
        settle=3,
    ),
    _s(
        "M1",
        "clients",
        "many client pods, one model",
        "4 caller processes: all served (burst: refused on time); own replies; within budget",
        many_clients,
        share=0.2,
    ),
    _s(
        "M2",
        "clients",
        "services with different registry versions",
        "the newer is live for both; the older logs registry_older_ignored; all served",
        registry_versions,
        share=0.6,
    ),
    _s(
        "M3",
        "clients",
        "caller-only pods and worker-only pods",
        "all served, every call by a worker pod",
        split_pods,
        share=0.2,
    ),
    _s(
        "L2",
        "store",
        "streams under store block + worker kill",
        "each stream completes intact or ends with an on-time error; nothing hangs",
        streams_faulted,
        *ON_TIME,
        settle=3,
        models={"stream": {"model_id": STREAM_ID, "rpm": RPM, "worker_slots": 50}},
        fake={STREAM_ID: {"output_tokens": STREAM_TOKENS, "tokens_per_s": 60}},
    ),
    _s(
        "L3",
        "store",
        "separate queue store blocked",
        "calls run direct after LLMQ_QUEUE_FAILOVER_SECS; all served on time",
        queue_store_block,
        env={"LLMQ_QUEUE_FAILOVER_SECS": "2"},
        queue_proxy=True,
    ),
    _s(
        "L4",
        "store",
        "embeddings under worker kill + store block",
        "every batch answered or refused on time; vectors in order",
        embeds_faulted,
        *ON_TIME,
        settle=3,
        models={
            "cohere": {
                "type": "embedding",
                "model_id": EMBED_ID,
                "rpm": RPM,
                "embed_batch_size": 96,
                "worker_slots": 50,
            }
        },
    ),
]
BY_ID = {s.sid: s for s in SCENARIOS}
ALONE = {"S9"}  # changes the shared store's memory limit: never next to another run
# Store failover on two Docker nodes: run by failover.py's drill (its own pass marks).
FAILOVER = {"S6": "failover", "S7": "slowdns", "S11": "restart"}


async def parallel(units: list[tuple[str, str]], profile: str, jobs: int) -> dict[str, bool]:
    """Run each (scenario, load) as its own process, `jobs` at a time, on its own ports."""
    folder = ROOT / "reports" / f"scenarios-{time.strftime('%Y%m%d-%H%M%S')}"
    folder.mkdir(parents=True, exist_ok=True)
    print(f"  logs: {folder}", flush=True)
    slots: asyncio.Queue[int] = asyncio.Queue()
    for slot in range(jobs):
        slots.put_nowait(slot)
    results: dict[str, bool] = {}

    async def one(sid: str, load: str) -> None:
        slot = await slots.get()
        name = f"{sid}-{load}"
        env = {
            **os.environ,
            "PYTHONUNBUFFERED": "1",
            "LLMQ_FAKE_PORT": str(8100 + slot),
            "LLMQ_SCENARIO_PROXY_PORT": str(6400 + slot),
            "LLMQ_DRILL_LOG_DIR": str(folder / f"{name}-processes"),
        }
        cmd = [PY, __file__, sid, "--load", load, "--profile", profile, "--jobs", "1"]
        try:
            with (folder / f"{name}.log").open("w", encoding="utf-8") as log:
                proc = await asyncio.create_subprocess_exec(*cmd, env=env, stdout=log, stderr=log)
                await proc.wait()
            results[f"{sid} {load}"] = proc.returncode == 0
            text = (folder / f"{name}.log").read_text(encoding="utf-8", errors="replace")
            lines = [
                line
                for line in text.splitlines()
                if line.startswith(("[", "PASS", "FAIL", "    "))
                and not line.startswith("    logs")
            ]
            print("\n".join(lines), flush=True)
        finally:
            slots.put_nowait(slot)

    await asyncio.gather(*(one(sid, load) for sid, load in units))
    return results


def main() -> int:
    """Run the chosen scenarios under the chosen loads; exit 1 if any fails."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("ids", nargs="*", help="scenario ids (S4 P1 ...) or all")
    parser.add_argument("--group", choices=sorted({s.group for s in SCENARIOS} | {"failover"}))
    parser.add_argument("--load", choices=sorted(LOADS), action="append")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="fast")
    parser.add_argument("--jobs", type=int, default=4, help="runs at a time (1: one by one)")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    if args.list:
        for s in SCENARIOS:
            print(f"{s.sid:4} {s.group:9} {s.title}: {s.rule}")
        for sid, name in FAILOVER.items():
            print(f"{sid:4} failover  Docker failover drill `{name}` (failover.py)")
        return 0
    setup_logging()
    ids = list(BY_ID) + list(FAILOVER) if args.ids == ["all"] else list(args.ids)
    if args.group == "failover":
        ids += list(FAILOVER)
    elif args.group:
        ids += [s.sid for s in SCENARIOS if s.group == args.group]
    unknown = [i for i in ids if i not in BY_ID and i not in FAILOVER]
    if unknown or not ids:
        parser.error(f"unknown or no scenario: {unknown}; see --list")
    ids = list(dict.fromkeys(ids))
    loads = args.load or list(LOADS)
    started = time.monotonic()
    results: dict[str, bool] = {}
    shared = [(sid, load) for sid in ids if sid in BY_ID and sid not in ALONE for load in loads]
    alone = [(sid, load) for sid in ids if sid in ALONE for load in loads]
    if args.jobs > 1 and len(shared) > 1:
        results.update(asyncio.run(parallel(shared, args.profile, args.jobs)))
    else:
        alone = shared + alone
    if alone:
        print(f"  logs of worker processes: {log_dir()}", flush=True)
    for sid, load in alone:
        results[f"{sid} {load}"] = asyncio.run(run_one(BY_ID[sid], load, args.profile))
    for sid in [i for i in ids if i in FAILOVER]:

        async def drill(name: str = FAILOVER[sid]) -> bool:
            return bool(await failover.drill(name))

        failover.nodes_up()
        try:
            results[sid] = asyncio.run(drill())
        finally:
            failover.nodes_down()
    failed = [k for k, ok in results.items() if not ok]
    print(
        f"{len(results) - len(failed)}/{len(results)} passed in {time.monotonic() - started:.0f}s"
    )
    print("ALL PASS" if not failed else f"FAILED: {', '.join(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
