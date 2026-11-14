"""Store failover drill: queue traffic on fakellm while a Valkey primary fails over.

Starts a throwaway primary and replica in Docker and a TCP proxy that plays the store's DNS
name (free ports picked per run), its own fakellm (which counts calls it saw twice), then sends
10 calls a second for 30 s and at 8 s:

- `failover`: FAILOVER (the primary becomes a replica); the name moves to the new primary.
- `slowdns`: FAILOVER, but the name keeps pointing at the old primary for 6 s (READONLY).
- `restart`: every connection drops and the node refuses new ones for 6 s.

Pass mark: `failover` and `restart` lose no call. In `slowdns` only calls already in flight
when the store stopped taking writes may fail (`deadline_exceeded`), at most 5 %. In every
scenario no call runs twice at the provider (each request body is unique).

    python scripts/failover.py                  # all three
    python scripts/failover.py slowdns          # one
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from uuid_utils import uuid7

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from gates_fleet import FakeLLM, FaultProxy, free_port  # noqa: E402  # needs the sys.path entry

from llm_queue import CallFailed, LLMQueue  # noqa: E402

NODES = {"a": ("llmqfo-a", free_port()), "b": ("llmqfo-b", free_port())}
NETWORK = "llmqfo"
IMAGE = "valkey/valkey:8.1-alpine"
PROXY_PORT = free_port()
RUN_SECS = 30
EVENT_AT_SECS = 8
OUTAGE_SECS = 6
REQUEST = {
    "messages": [{"role": "user", "content": [{"text": "hi"}]}],
    "inferenceConfig": {"maxTokens": 20},
}


def docker(*args: str, check: bool = True) -> str:
    """Run one docker command; its stdout."""
    proc = subprocess.run(["docker", *args], capture_output=True, text=True, check=check)
    return proc.stdout.strip()


def role(node: str) -> str:
    """`master` or `slave`."""
    return docker("exec", NODES[node][0], "valkey-cli", "role").splitlines()[0]


def nodes_up() -> None:
    """A primary (a) and its replica (b), replication link up."""
    nodes_down()
    docker("network", "create", NETWORK)
    name_a, port_a = NODES["a"]
    name_b, port_b = NODES["b"]
    docker("run", "-d", "--name", name_a, "--network", NETWORK, "-p", f"{port_a}:6379", IMAGE)
    docker(
        "run", "-d", "--name", name_b, "--network", NETWORK, "-p", f"{port_b}:6379", IMAGE,
        "valkey-server", "--replicaof", name_a, "6379",
    )  # fmt: skip
    wait_link()


def wait_link() -> None:
    """Until the replica's link to the primary is up (20 s at most)."""
    for _ in range(40):
        replica = "b" if role("a") == "master" else "a"
        info = docker("exec", NODES[replica][0], "valkey-cli", "info", "replication", check=False)
        if "master_link_status:up" in info:
            return
        time.sleep(0.5)
    raise SystemExit("replication link did not come up")


def nodes_down() -> None:
    """Remove the drill's containers and network."""
    for name, _ in NODES.values():
        docker("rm", "-f", name, check=False)
    docker("network", "rm", NETWORK, check=False)


class Proxy(FaultProxy):  # type: ignore[misc]  # gates_fleet is a script module
    """The store's DNS name: new connections go to `target`; old ones stay where they are.

    A `FaultProxy` in its own process: the drill's queue is caller and worker here, and on the
    proxy's old shared loop its own stalls looked like store latency (CO-2).
    """

    def __init__(self, port: int = PROXY_PORT) -> None:
        """On node a, taking connections, once `start()` is called."""
        self._target = "a"
        super().__init__(port, upstream=NODES["a"][1])

    @property
    def target(self) -> str:
        """The node new connections go to (`a` or `b`)."""
        return self._target

    async def move_to(self, node: str) -> None:
        """Point new connections at `node` (async: the proxy's answer is read in a thread)."""
        await self.set(upstream=NODES[node][1])
        self._target = node


def environment(prefix: str, fake_url: str) -> dict[str, str]:
    """`LLMQ_*` for the drill: the proxy as store, fakellm, quotas far above the load."""
    doc = json.loads((ROOT / "samples" / "registry.json").read_text(encoding="utf-8"))
    for model in doc["models"].values():
        model["rpm"], model["tpm"] = 100_000, 100_000_000
    return {
        "LLMQ_STORE_URL": f"redis://127.0.0.1:{PROXY_PORT}/0",
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_QUEUE_PREFIX": prefix.replace(":", "-") + "-",
        "LLMQ_REGISTRY": json.dumps(doc),
        "LLMQ_METRICS_SINK": "none",
        "LLMQ_ENVIRONMENT": "test",  # local store without TLS or a salt
        "LLMQ_BEDROCK_ENDPOINT_URL": fake_url,
    }


async def drill(scenario: str) -> bool:
    """Run one scenario; print per-second results; True when it meets the pass mark."""
    if role("a") != "master":  # the previous scenario failed over: swap back
        docker("exec", NODES["b"][0], "valkey-cli", "FAILOVER")
        await asyncio.sleep(2)
    await asyncio.to_thread(wait_link)
    # Each resource is undone on its own, in reverse, even if a later start or an earlier
    # undo fails: the proxy and fakellm leaked when llmq.start() raised (CO-17).
    async with contextlib.AsyncExitStack() as stack:
        return await _drill(scenario, stack)
    raise AssertionError("unreachable: the exit stack swallows no exception")


async def _drill(scenario: str, stack: contextlib.AsyncExitStack) -> bool:
    proxy = Proxy()
    stack.push_async_callback(asyncio.to_thread, proxy.stop)
    await asyncio.to_thread(proxy.start)
    fake = FakeLLM({"defaults": {"latency_ms": {"p50": 150, "p95": 250}}})
    stack.push_async_callback(asyncio.to_thread, fake.stop)  # its wait off the loop (N7)
    await asyncio.to_thread(fake.start)
    llmq = LLMQueue.from_env(environment(f"llmqfo:{uuid7().hex[-6:]}", fake.url))
    stack.push_async_callback(llmq.stop, drain_secs=1)
    await llmq.start()
    started = time.monotonic()
    ok: collections.Counter[int] = collections.Counter()
    failed: collections.Counter[int] = collections.Counter()
    errors: collections.Counter[str] = collections.Counter()
    latencies: list[float] = []

    async def one(i: int) -> None:
        second = int(time.monotonic() - started)
        sent = time.monotonic()
        request = {**REQUEST, "messages": [{"role": "user", "content": [{"text": f"fo {i}"}]}]}
        try:
            await asyncio.wait_for(llmq.converse("gpt20b", request), 150)
            ok[second] += 1
            latencies.append(time.monotonic() - sent)
        except CallFailed as exc:
            failed[second] += 1
            errors[exc.kind] += 1
        except Exception as exc:  # the drill reports every kind of failure
            failed[second] += 1
            errors[type(exc).__name__] += 1

    async def traffic() -> None:
        calls: list[asyncio.Task[None]] = []
        while time.monotonic() - started < RUN_SECS:
            calls.append(asyncio.create_task(one(len(calls))))
            await asyncio.sleep(0.1)
        await asyncio.gather(*calls)

    async def event() -> None:
        await asyncio.sleep(EVENT_AT_SECS)
        at = f"[{time.monotonic() - started:.1f}s]"
        if scenario == "restart":
            await proxy.set(refuse=True)
            try:
                await proxy.drop_all()
                print(f"  {at} node down: every connection dropped")
                await asyncio.sleep(OUTAGE_SECS)
            finally:
                await proxy.set(refuse=False)
            print(f"  [{time.monotonic() - started:.1f}s] node back")
            return
        # In a thread: a blocking call here stalled the drill's own callers mid-traffic.
        await asyncio.to_thread(docker, "exec", NODES["a"][0], "valkey-cli", "FAILOVER")
        print(f"  {at} FAILOVER: a is now a replica")
        if scenario == "slowdns":
            await asyncio.sleep(OUTAGE_SECS)
        await proxy.move_to("b")
        print(f"  [{time.monotonic() - started:.1f}s] the name now points at b")

    await asyncio.gather(traffic(), event())
    health = await llmq.health()
    await asyncio.sleep(1)  # the last attempts have ended
    provider = await asyncio.to_thread(fake.stats)
    await stack.aclose()  # stopped before the report, as before
    twice = sum(int(m.get("duplicates", 0)) for m in provider.values())
    provider_calls = sum(int(m.get("calls", 0)) for m in provider.values())
    total_ok, total_failed = sum(ok.values()), sum(failed.values())
    latencies.sort()
    timing = (
        f"p50 {latencies[len(latencies) // 2]:.2f}s, max {latencies[-1]:.1f}s"
        if latencies
        else "no call succeeded"
    )
    print("  per second ok/failed:", " ".join(f"{s}:{ok[s]}/{failed[s]}" for s in sorted(ok)))
    print(
        f"  {total_ok} ok, {total_failed} failed {dict(errors) or ''}; {timing}; "
        f"stores after: {health['stores']}"
    )
    print(f"  provider: {provider_calls} calls, {twice} ran twice")
    allowed = 0 if scenario != "slowdns" else (total_ok + total_failed) // 20
    passed = (
        total_failed <= allowed
        and health["stores"] == {"state": "ok", "queue": "ok"}
        and twice == 0
    )
    print(f"{'PASS' if passed else 'FAIL'}  {scenario}")
    return passed


def main() -> int:
    """Run the chosen scenarios on fresh containers; exit 1 if any fails."""
    known = ["failover", "slowdns", "restart"]
    chosen = sys.argv[1:] or known
    if set(chosen) - set(known):  # `--help` once started the Docker nodes and a drill
        print(__doc__)
        return 2
    os.environ.update({"AWS_ACCESS_KEY_ID": "aws-main", "AWS_SECRET_ACCESS_KEY": "fake"})
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    try:  # a failed nodes_up still removes what it started (CO-17)
        nodes_up()
        results = []
        for scenario in chosen:
            print(f"[{scenario}]")
            results.append(asyncio.run(drill(scenario)))
    finally:
        nodes_down()
    print("ALL PASS" if all(results) else "FAILED")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
