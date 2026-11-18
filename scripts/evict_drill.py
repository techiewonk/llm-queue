"""Eviction drill: queue traffic on a full store that evicts, as a shared cache does (0.1.0b3).

Starts a throwaway Valkey in Docker with a small `maxmemory`, its own fakellm, and an "app
cache" filler that keeps writing keys with a TTL until the store is full and beyond. Meanwhile
it sends `--calls` calls through the queue (`LLMQ_KEY_EXPIRY_MODE=auto`).

- `volatile-lru`: the queue runs `swept`. Pass: every call answers, the store evicted app keys
  (`evicted_keys` > 0) and not one key of the queue's prefix (keyspace `evicted` events).
- `noeviction`: the queue runs `native`. Pass: nothing is evicted and every call ends by itself
  (an answer, or a `CallFailed` once the store refuses writes when full: the model's
  `store_down` rule waits for the store until the call's deadline, `--call-timeout`); none is
  left hanging past it.

Every run prints its numbers and saves them in `reports/evict-<time>.json`.

    python scripts/evict_drill.py                       # both policies
    python scripts/evict_drill.py volatile-lru --calls 500
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import contextlib
import datetime
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import redis.asyncio
from uuid_utils import uuid7

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from gates_fleet import FakeLLM, free_port  # noqa: E402  # needs the sys.path entry

from llm_queue import CallFailed, LLMQueue  # noqa: E402

IMAGE = "valkey/valkey:8.1-alpine"
POLICIES = ("volatile-lru", "noeviction")
FILL_VALUE = b"x" * 8192  # one app cache entry
FILL_BATCH = 100  # entries per pipelined round
# Keys kept on native TTLs on purpose (engine/expiry.py): losing one is harmless.
NATIVE_OK = (":lock:", ":health:", ":debug")
HUNG = "TimeoutError"  # the drill gave up on the call: it never ended by itself


def docker(*args: str, check: bool = True) -> str:
    """Run one docker command; its stdout."""
    proc = subprocess.run(["docker", *args], capture_output=True, text=True, check=check)
    return proc.stdout.strip()


def environment(port: int, prefix: str, fake_url: str) -> dict[str, str]:
    """`LLMQ_*` for the drill: the throwaway store, fakellm, quotas far above the load."""
    doc = json.loads((ROOT / "samples" / "registry.json").read_text(encoding="utf-8"))
    for model in doc["models"].values():
        model["rpm"], model["tpm"] = 1_000_000, 1_000_000_000
    return {
        "LLMQ_STORE_URL": f"redis://127.0.0.1:{port}/0",
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_QUEUE_PREFIX": prefix.replace(":", "-") + "-",
        "LLMQ_REGISTRY": json.dumps(doc),
        "LLMQ_METRICS_SINK_MODE": "none",
        "LLMQ_ENVIRONMENT": "test",  # local store without TLS or a salt
        "LLMQ_BEDROCK_ENDPOINT_URL": fake_url,
        "LLMQ_KEY_EXPIRY_MODE": "auto",
    }


async def run(
    policy: str, calls: int, concurrency: int, maxmemory: str, call_timeout: float
) -> dict[str, Any]:
    """One policy: start the store, fill it, send the calls; the numbers and the verdict."""
    name, port = f"llmqev-{uuid7().hex[-6:]}", free_port()
    docker(
        "run", "-d", "--name", name, "-p", f"{port}:6379", IMAGE, "valkey-server",
        "--save", "", "--appendonly", "no", "--maxmemory", maxmemory,
        "--maxmemory-policy", policy, "--notify-keyspace-events", "Ee",
    )  # fmt: skip
    async with contextlib.AsyncExitStack() as stack:
        stack.callback(docker, "rm", "-f", name, check=False)
        admin = redis.asyncio.Redis(port=port)
        stack.push_async_callback(admin.aclose)
        for _ in range(50):
            with contextlib.suppress(Exception):
                if await admin.ping():
                    break
            await asyncio.sleep(0.2)
        fake = FakeLLM({"defaults": {"latency_ms": {"p50": 20, "p95": 40}}})
        stack.push_async_callback(asyncio.to_thread, fake.stop)
        await asyncio.to_thread(fake.start)
        prefix = f"llmqev:{uuid7().hex[-6:]}"
        llmq = LLMQueue.from_env(environment(port, prefix, fake.url))
        await llmq.start()
        stack.push_async_callback(llmq.stop, drain_secs=1)
        mode = (await llmq.health())["key_expiry_mode"]

        evicted_ours: list[str] = []
        evicted_harmless: list[str] = []
        evicted_all = 0
        pubsub = admin.pubsub()
        await pubsub.subscribe("__keyevent@0__:evicted")
        stack.push_async_callback(pubsub.aclose)

        async def watch() -> None:
            nonlocal evicted_all
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                evicted_all += 1
                key = message["data"].decode()
                if key.startswith((prefix, prefix.replace(":", "-"))):
                    harmless = any(part in key for part in NATIVE_OK)
                    (evicted_harmless if harmless else evicted_ours).append(key)

        fill = {"written": 0, "refused": 0}
        stop_fill = asyncio.Event()

        async def filler() -> None:
            n = 0
            while not stop_fill.is_set():
                try:
                    async with admin.pipeline(transaction=False) as pipe:
                        for _ in range(FILL_BATCH):
                            pipe.set(f"appcache:{n}", FILL_VALUE, ex=3600)
                            n += 1
                        await pipe.execute()
                    fill["written"] += FILL_BATCH
                except redis.exceptions.ResponseError:  # OOM: noeviction refuses the app too
                    fill["refused"] += FILL_BATCH
                    await asyncio.sleep(0.05)
                await asyncio.sleep(0)

        watcher = asyncio.create_task(watch())
        filling = asyncio.create_task(filler())
        await asyncio.sleep(2)  # the store fills before the calls start
        gate = asyncio.Semaphore(concurrency)
        outcomes: collections.Counter[str] = collections.Counter()
        request = {"inferenceConfig": {"maxTokens": 20}}

        async def one(i: int) -> None:
            async with gate:
                messages = [{"role": "user", "content": [{"text": f"evict drill {i}"}]}]
                try:
                    await asyncio.wait_for(
                        llmq.converse(
                            "gpt20b",
                            {**request, "messages": messages},
                            timeout_secs=call_timeout,
                        ),
                        call_timeout + 30,
                    )
                    outcomes["ok"] += 1
                except CallFailed as exc:
                    outcomes[exc.kind] += 1
                except Exception as exc:  # every kind is reported
                    outcomes[type(exc).__name__] += 1

        started = time.monotonic()
        await asyncio.gather(*(one(i) for i in range(calls)))
        secs = time.monotonic() - started
        stop_fill.set()
        await filling
        await asyncio.sleep(0.5)  # the last eviction events
        watcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher
        info = await admin.info("stats")
        memory = await admin.info("memory")
        ours_with_ttl = 0
        async for key in admin.scan_iter(match=f"{prefix}*", count=1000):
            text_key = key.decode()
            if any(part in text_key for part in NATIVE_OK):
                continue
            if await admin.pttl(key) > 0:
                ours_with_ttl += 1
        if policy == "noeviction":
            passed = int(info["evicted_keys"]) == 0 and not outcomes[HUNG] and mode == "native"
        else:
            passed = (
                outcomes["ok"] == calls
                and int(info["evicted_keys"]) > 0
                and not evicted_ours
                and ours_with_ttl == 0
                and mode == "swept"
            )
        return {
            "policy": policy,
            "key_expiry_mode": mode,
            "maxmemory": maxmemory,
            "calls": calls,
            "secs": round(secs, 1),
            "outcomes": dict(outcomes),
            "evicted_keys": int(info["evicted_keys"]),
            "evicted_events": evicted_all,
            "evicted_queue_keys": evicted_ours[:20],
            "evicted_native_on_purpose": sorted({k.rsplit(":", 1)[0] for k in evicted_harmless}),
            "queue_keys_with_native_ttl": ours_with_ttl,
            "app_keys_written": fill["written"],
            "app_writes_refused": fill["refused"],
            "used_memory_mb": round(int(memory["used_memory"]) / 2**20, 1),
            "pass": passed,
        }


async def main_async(args: argparse.Namespace) -> int:
    """Run the chosen policies one after the other; print and save the results."""
    results = []
    for policy in args.policies or POLICIES:
        result = await run(policy, args.calls, args.concurrency, args.maxmemory, args.call_timeout)
        results.append(result)
        verdict = "PASS" if result["pass"] else "FAIL"
        print(f"{verdict}  {policy}: {json.dumps(result)}")
    out = ROOT / "reports" / f"evict-{datetime.datetime.now():%Y%m%d-%H%M%S}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"results saved in {out}")
    return 0 if all(r["pass"] for r in results) else 1


def main() -> int:
    """Parse the arguments and run the drill."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("policies", nargs="*", choices=POLICIES)
    parser.add_argument("--calls", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--maxmemory", default="64mb")
    parser.add_argument("--call-timeout", type=float, default=20.0, help="each call's deadline")
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    return asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
