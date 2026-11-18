"""The fleet `scripts/gates.py` drives: fakellm, worker / caller processes, store measurements.

Workers are separate Python processes (`python scripts/gates_fleet.py worker`), like pods: each
builds `LLMQueue.from_env()` with `LLMQ_ROLE_MODE=worker`, counts its metrics into the store, and
stops cleanly on a `stop` line (or dies at once on `kill()`). Callers can be processes too
(`caller` mode, for the kill-switch gate) or queues in the driver itself. The store proxy the
drills fault (`FaultProxy`) is a process too (`proxy` mode).
"""

from __future__ import annotations

import asyncio
import collections
import concurrent.futures
import contextlib
import json
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from uuid_utils import uuid7

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

PY = sys.executable
STORE = "redis://:localdev@127.0.0.1:6379/0"


def free_port() -> int:
    """A TCP port nothing listens on now, so parallel runs never share fixed ports."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


# A fresh port per run unless the caller fixes one (workers read the URL from their env).
FAKE_PORT = int(os.environ.get("LLMQ_FAKE_PORT") or free_port())
CHAT_ID = "openai.gpt-oss-20b-1:0"
EMBED_ID = "cohere.embed-english-v3"
TITAN_ID = "amazon.titan-embed-text-v2:0"
REGIONS = (
    "us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1",
    "eu-west-2", "eu-central-1", "ap-south-1", "ap-southeast-1", "ap-northeast-1",
)  # fmt: skip


def registry(
    models: dict[str, dict[str, Any]],
    *,
    accounts: int = 3,
    regions: int = 3,
    defaults: dict[str, Any] | None = None,
    routing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A registry: every model on `accounts` x `regions` deployments (all confirmed)."""
    names = [f"aws-{i}" for i in range(accounts)]
    used = list(REGIONS[:regions])
    doc: dict[str, Any] = {
        "registry_version": 1,
        "defaults": {"timeout_secs": 120, "max_wait_secs": 120, **(defaults or {})},
        "residency": {"allowed_regions": used},
        "accounts": {n: {"kind": "aws"} for n in names},
        "models": {},
    }
    if routing:
        doc["routing"] = routing
    for alias, spec in models.items():
        entry = {"type": "chat", "provider": "bedrock", "model_id": CHAT_ID, **spec}
        entry["deployments"] = {n: {"confirmed": used} for n in names}
        doc["models"][alias] = entry
    return doc


def environment(prefix: str, doc: dict[str, Any], **extra: str) -> dict[str, str]:
    """A process environment for one gate run (fake AWS keys, the gate's fakellm)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("LLMQ_", "AWS_"))}
    env.update(
        {
            "LLMQ_STORE_URL": STORE,
            "LLMQ_KEY_PREFIX": prefix,
            "LLMQ_QUEUE_PREFIX": prefix.replace(":", "-") + "-",
            "LLMQ_REGISTRY": json.dumps(doc),
            "LLMQ_METRICS_SINK_MODE": "none",
            "LLMQ_ENVIRONMENT": "test",  # local store without TLS or a salt
            "LLMQ_BEDROCK_ENDPOINT_URL": f"http://127.0.0.1:{FAKE_PORT}",
            "AWS_ACCESS_KEY_ID": "gates",
            "AWS_SECRET_ACCESS_KEY": "fake",
            "AWS_DEFAULT_REGION": "us-east-1",
            "PYTHONIOENCODING": "utf-8",
            **extra,
        }
    )
    return env


def account_keys(account: Any) -> dict[str, str]:
    """Fake AWS keys named after the registry account, so fakellm keys calls per deployment.

    fakellm reads the account from the SigV4 access key id: `<model>@<account>/<region>`
    scenario keys then count each deployment on its own (B17).
    """
    return {"aws_access_key_id": str(account.name), "aws_secret_access_key": "fake"}


def drill_tuning(env: dict[str, str] | None = None) -> Any:
    """`Tuning` for a drill process, or None (the library's defaults).

    `LLMQ_DRILL_STORE_READ_TIMEOUT_SECS` sets the store read timeout: scenario S5's fast
    profile, so a half-open store is detected within its fault (CO-14).
    """
    raw = (os.environ if env is None else env).get("LLMQ_DRILL_STORE_READ_TIMEOUT_SECS")
    if not raw:
        return None
    from llm_queue import Tuning
    from llm_queue.tuning import StoreOptions

    return Tuning(store=StoreOptions(read_timeout_secs=float(raw)))


def process_stats() -> dict[str, float]:
    """This process: RSS, open asyncio tasks and threads (the soak's leak samples)."""
    import psutil

    return {
        "pid": os.getpid(),
        "rss_mb": psutil.Process().memory_info().rss / 1e6,
        "tasks": len(asyncio.all_tasks()),
        "threads": threading.active_count(),
    }


LOG_FORMAT = "%(asctime)s %(process)d %(levelname)s %(name)s: %(message)s"


def log_dir() -> Path:
    """Where drill processes write their logs, made on first use.

    `LLMQ_DRILL_LOG_DIR` (check.py sets it per step) or `reports/drill-<time>` (git-ignored).
    """
    raw = os.environ.get("LLMQ_DRILL_LOG_DIR")
    if not raw:
        raw = str(ROOT / "reports" / f"drill-{time.strftime('%Y%m%d-%H%M%S')}")
        os.environ["LLMQ_DRILL_LOG_DIR"] = raw  # one folder for the whole run
    path = Path(raw)
    path.mkdir(parents=True, exist_ok=True)
    return path


def log_file(kind: str) -> Any:
    """A new, numbered log file for one drill process (`worker-3.log`, `fakellm-1.log`)."""
    folder = log_dir()
    n = 1 + sum(1 for _ in folder.glob(f"{kind}-*.log"))
    return (folder / f"{kind}-{n}.log").open("ab")


def setup_logging() -> None:
    """Library warnings with timestamps on stderr (the drill's own log file)."""
    import logging

    level = os.environ.get("DRILL_LOG_LEVEL", "WARNING").upper()  # INFO: every call
    logging.basicConfig(level=level, format=LOG_FORMAT, force=True)


def new_prefix() -> str:
    """A fresh key prefix (deleted by `cleanup`)."""
    return f"llmqgate:{uuid7().hex[-6:]}"


class FakeLLM:
    """A fakellm process on FAKE_PORT with a gate's own scenario (latency, limits)."""

    def __init__(self, scenario: dict[str, Any]) -> None:
        """Write the scenario; `start()` runs it."""
        self.file = Path(tempfile.mkdtemp()) / "scenario.json"
        self.file.write_text(json.dumps({"seed": 7, "strict_models": False, **scenario}))
        self.proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        """Start and wait until it answers."""
        # FT-17: a listener already on the port (the stack's fakellm, or another drill's) used
        # to count as "started", and the drill then measured someone else's counters.
        with (
            contextlib.suppress(OSError),
            socket.create_connection(("127.0.0.1", FAKE_PORT), timeout=0.2),
        ):
            raise RuntimeError(f"port {FAKE_PORT} is already in use; fakellm cannot start there")
        env = dict(os.environ, PYTHONPATH=str(ROOT / "tools" / "fakellm"))
        cmd = [PY, "-m", "fakellm", "--host", "127.0.0.1", "--port", str(FAKE_PORT)]
        log = log_file("fakellm")
        # A stdin pipe it watches: it ends when this process dies, even killed hard with no
        # teardown (a scenario child killed by `parallel`); it was orphaned on its port (CO-15).
        self.proc = subprocess.Popen(
            [*cmd, "--scenario", str(self.file), "--exit-on-stdin-eof"],
            env=env,
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=log,
        )
        log.close()  # the process keeps its own handle
        for _ in range(100):
            if self.proc.poll() is not None:  # it died (for example, the port was taken)
                break
            try:
                with socket.create_connection(("127.0.0.1", FAKE_PORT), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.1)
        self.stop()  # a process that never listened must not keep running (CO-3)
        raise RuntimeError("fakellm did not start")

    @property
    def url(self) -> str:
        """Its base URL (`LLMQ_BEDROCK_ENDPOINT_URL`)."""
        return f"http://127.0.0.1:{FAKE_PORT}"

    def stats(self) -> dict[str, dict[str, Any]]:
        """Per-key counters (`GET /admin/stats`): calls, ok, by_minute, duplicates ..."""
        import urllib.request

        url = f"{self.url}/admin/stats"
        with urllib.request.urlopen(url, timeout=10) as reply:
            return dict(json.loads(reply.read())["models"])

    def stop(self) -> None:
        """Stop the process and remove its scenario folder (a temp folder per run leaked).

        Blocking (up to 10 s): async code calls it in a thread (N7).
        """
        try:
            if self.proc is not None and self.proc.poll() is None:
                self.proc.kill()
                self.proc.wait(10)
        finally:
            if self.proc is not None and self.proc.stdin is not None:
                with contextlib.suppress(OSError):
                    self.proc.stdin.close()
            shutil.rmtree(self.file.parent, ignore_errors=True)


class Worker:
    """One worker process (a pod)."""

    def __init__(self, env: dict[str, str]) -> None:
        """Start it (`LLMQ_ROLE_MODE=worker`); `ready()` waits for its start."""
        log = log_file("worker")  # its warnings and errors, with timestamps
        self.proc = subprocess.Popen(
            [PY, str(Path(__file__)), "worker"],
            env={**env, "LLMQ_ROLE_MODE": "worker"},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
        )
        log.close()
        self.worker_id = ""  # the engine's id (`meta.worker` of the calls it serves)
        self._lock = asyncio.Lock()  # one stdin request at a time

    async def ready(self) -> None:
        """Until the worker printed `ready <worker id>`."""
        assert self.proc.stdout is not None
        try:  # FT-7: a worker stuck at start used to hang the whole run for ever
            line = await asyncio.wait_for(asyncio.to_thread(self.proc.stdout.readline), 60)
        except TimeoutError:
            await self.kill()
            raise RuntimeError("worker did not print ready within 60 s; killed") from None
        if not line.startswith(b"ready"):
            raise RuntimeError(f"worker did not start: {line!r}")
        self.worker_id = line.decode().split()[1] if len(line.split()) > 1 else ""

    async def stats(self) -> dict[str, float] | None:
        """Its `process_stats()` (the debug hook of this harness), or None once it is gone."""
        async with self._lock:
            if self.proc.poll() is not None or self.proc.stdin is None or not self.proc.stdout:
                return None
            try:
                self.proc.stdin.write(b"stats\n")
                self.proc.stdin.flush()
                line = await asyncio.wait_for(asyncio.to_thread(self.proc.stdout.readline), 10)
            except (OSError, TimeoutError):
                return None
            return dict(json.loads(line)) if line.startswith(b"{") else None

    async def drain_status(self) -> dict[str, Any] | None:
        """Its `LLMQueue.drain_status()` (draining, held, running), or None once it is gone."""
        async with self._lock:
            if self.proc.poll() is not None or self.proc.stdin is None or not self.proc.stdout:
                return None
            try:
                self.proc.stdin.write(b"drain\n")
                self.proc.stdin.flush()
                line = await asyncio.wait_for(asyncio.to_thread(self.proc.stdout.readline), 10)
            except (OSError, TimeoutError):
                return None
            return dict(json.loads(line)) if line.startswith(b"{") else None

    def terminate(self) -> None:
        """A pod's SIGTERM: the signal on POSIX, a `stop` line on Windows (no SIGTERM there).

        Either way the worker pauses and stops within `LLMQ_DRILL_STOP_DRAIN_SECS` (default 30).
        """
        if self.proc.poll() is not None:
            return
        if sys.platform == "win32":
            if self.proc.stdin is not None:
                with contextlib.suppress(OSError):
                    self.proc.stdin.write(b"stop\n")
                    self.proc.stdin.flush()
        else:
            self.proc.send_signal(signal.SIGTERM)

    async def stop(self) -> None:
        """Graceful: pause, drain, stop; killed if it has not stopped within 60 s.

        A stuck worker used to raise `TimeoutExpired` here and stay alive, and the gate's
        teardown skipped fakellm and the key cleanup after it (CO-3).
        """
        async with self._lock:
            if self.proc.poll() is None and self.proc.stdin is not None:
                try:
                    self.proc.stdin.write(b"stop\n")
                    self.proc.stdin.flush()
                    await asyncio.to_thread(self.proc.wait, 60)
                except (OSError, subprocess.TimeoutExpired):  # gone already, or stuck
                    await self.kill()

    async def kill(self) -> None:
        """At once, no cleanup (a crashed pod); reaped in a thread, off the loop (N7)."""
        await kill_and_reap(self.proc)


async def kill_and_reap(proc: Any, wait_secs: float = 10) -> None:
    """Kill `proc` and wait for it in a thread: `proc.wait` on the loop froze the drill (N7).

    A process the OS has not reaped within `wait_secs` is reported and left: raising here
    skipped the rest of a teardown (`stop_all`'s finally).
    """
    if proc.poll() is None:
        with contextlib.suppress(OSError):  # gone in between
            proc.kill()
    try:
        await asyncio.to_thread(proc.wait, wait_secs)
    except subprocess.TimeoutExpired:
        print(f"process {getattr(proc, 'pid', '?')} not reaped {wait_secs:g} s after kill")


async def workers(env: dict[str, str], count: int) -> list[Worker]:
    """`count` started worker processes (all killed if one fails to start)."""
    fleet = [Worker(env) for _ in range(count)]
    try:
        await asyncio.gather(*(w.ready() for w in fleet))
    except BaseException:  # the caller never sees this fleet, so nobody else would stop it
        await asyncio.gather(*(w.kill() for w in fleet), return_exceptions=True)
        raise
    return fleet


async def stop_all(fleet: list[Worker]) -> None:
    """Stop every worker still alive; any left (a stop that raised) is killed."""
    try:
        await asyncio.gather(
            *(w.stop() for w in fleet if w.proc.poll() is None), return_exceptions=True
        )
    finally:
        await asyncio.gather(
            *(w.kill() for w in fleet if w.proc.poll() is None), return_exceptions=True
        )


class Store:
    """Measurements on the local Valkey: commands, script latency, memory, gate metrics."""

    def __init__(self) -> None:
        """A client on the stack's Valkey."""
        import redis

        self.client = redis.Redis.from_url(STORE)

    def commands(self) -> int:
        """Commands processed so far."""
        return int(self.client.info("stats")["total_commands_processed"])

    def script_calls(self) -> int:
        """EVALSHA calls so far (every admission, receive, finish ... is one)."""
        stats = self.client.info("commandstats")
        return int(stats.get("cmdstat_evalsha", {}).get("calls", 0))

    def reset_latency(self) -> None:
        """Clear the per-command latency histograms."""
        self.client.config_resetstat()

    def script_p99_ms(self) -> float:
        """p99 server time of EVALSHA since the last reset (INFO latencystats, exact)."""
        stats = self.client.info("latencystats").get("latency_percentiles_usec_evalsha", {})
        return float(stats.get("p99", 0.0)) / 1000.0

    def connections(self, name: str) -> int:
        """Open store connections whose client name is `name` (CLIENT LIST)."""
        return sum(1 for c in self.client.client_list() if c.get("name") == name)

    def memory_mb(self) -> float:
        """Used memory."""
        return int(self.client.info("memory")["used_memory"]) / 1e6

    def metrics(self, prefix: str) -> dict[str, float]:
        """What the workers counted (`MetricsToStore`)."""
        raw = self.client.hgetall(f"{prefix}:gates:metrics")
        return {(k.decode() if isinstance(k, bytes) else k): float(v) for k, v in raw.items()}

    def cleanup(self, prefix: str) -> None:
        """Delete every key and queue of a run."""
        queue_prefix = prefix.replace(":", "-") + "-"
        for pattern in (f"{prefix}:*", f"{queue_prefix}*", f"*{{{queue_prefix}*"):
            keys = list(self.client.scan_iter(match=pattern, count=1000))
            for i in range(0, len(keys), 500):
                self.client.delete(*keys[i : i + 500])

    def close(self) -> None:
        """Close the client's connections (it was never closed)."""
        self.client.close()


STACK_STORE_PORT = 6379  # the stack's Valkey, behind STORE


class _ProxyLoop:
    """The proxy itself, on its own thread and event loop (inside a `FaultProxy` process).

    The switches apply on the proxy's loop (`call_soon_threadsafe`) and return once applied.
    """

    def __init__(self, port: int, upstream: int) -> None:
        """Ready to `start()` on `port`, forwarding to `upstream`."""
        self.port = port
        self._upstream = upstream
        self._refuse = False
        self._freeze = False
        self._thaw = asyncio.Event()  # set while not frozen (bound to the proxy's loop on use)
        self._thaw.set()
        self._delay_secs = 0.0
        # Both writers of every live connection; each connection removes its own when it ends,
        # so a long soak no longer keeps thousands of dead ones (CO-21).
        self._open: set[asyncio.StreamWriter] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._stop: asyncio.Event | None = None
        self._error: BaseException | None = None
        self.lag_max_secs = 0.0  # the proxy loop's worst lateness: should stay a few ms

    @property
    def refuse(self) -> bool:
        """New connections are closed at once (a store that is down)."""
        return self._refuse

    @refuse.setter
    def refuse(self, value: bool) -> None:
        self._apply(lambda: setattr(self, "_refuse", value))

    @property
    def freeze(self) -> bool:
        """Half-open: bytes are held (not read on, not dropped) until the thaw.

        A real half-open link delays bytes or dies; it never loses some and carries on. Not
        reading models TCP backpressure; a chunk read as the freeze began waits too (CO-14).
        """
        return self._freeze

    @freeze.setter
    def freeze(self, value: bool) -> None:
        def change() -> None:
            self._freeze = value
            if value:
                self._thaw.clear()
            else:
                self._thaw.set()

        self._apply(change)

    @property
    def delay_secs(self) -> float:
        """Added before each forwarded chunk (a slow store or network)."""
        return self._delay_secs

    @delay_secs.setter
    def delay_secs(self, value: float) -> None:
        self._apply(lambda: setattr(self, "_delay_secs", value))

    @property
    def upstream(self) -> int:
        """The store port new connections go to; open ones stay where they are."""
        return self._upstream

    @upstream.setter
    def upstream(self, port: int) -> None:
        self._apply(lambda: setattr(self, "_upstream", port))

    def drop_all(self) -> None:
        """Close every open connection (a node restart)."""
        self._apply(self._close_open)

    def _close_open(self) -> None:
        for writer in list(self._open):
            writer.close()
        self._open.clear()

    def _apply(self, change: Callable[[], object]) -> None:
        """Run `change` on the proxy's loop and wait until it ran (at once if not running)."""
        loop = self._loop
        if loop is None or not loop.is_running() or threading.current_thread() is self._thread:
            change()
            return
        done: concurrent.futures.Future[None] = concurrent.futures.Future()

        def run() -> None:
            try:
                change()
                done.set_result(None)
            except BaseException as exc:  # handed back to the calling thread
                done.set_exception(exc)

        try:
            loop.call_soon_threadsafe(run)
        except RuntimeError:  # the loop closed in between: nothing runs on it any more
            change()
            return
        done.result(timeout=10)

    def start(self) -> _ProxyLoop:
        """Listen on `port` in a new thread; return once listening (or raise why not)."""
        self._thread = threading.Thread(
            target=self._run, name=f"fault-proxy-{self.port}", daemon=True
        )
        self._thread.start()
        if not self._ready.wait(10):
            raise RuntimeError(f"the store proxy on port {self.port} did not start")
        if self._error is not None:
            raise RuntimeError(f"the store proxy on port {self.port} failed") from self._error
        return self

    def stop(self) -> None:
        """Close the listener and every connection; end the thread (safe to call twice)."""
        loop, stop = self._loop, self._stop
        if loop is not None and stop is not None and loop.is_running():
            with contextlib.suppress(RuntimeError):  # the loop closed in between
                loop.call_soon_threadsafe(stop.set)
        if self._thread is not None:
            self._thread.join(10)

    def _run(self) -> None:
        try:
            asyncio.run(self._serve())
        except BaseException as exc:  # start() reports it
            self._error = exc
        finally:
            self._ready.set()

    async def _serve(self) -> None:
        """Keep the proxy listening, re-opening it if asyncio closed it.

        On Windows a failed accept (WinError 64: a client gave up mid-accept while the store
        was blocked) makes asyncio close the listening socket for good, and the store then
        looked down forever (scenario X1, a harness fault).
        """
        self._loop = asyncio.get_running_loop()
        self._stop = asyncio.Event()
        server = await asyncio.start_server(self._handle, "127.0.0.1", self.port)
        ticker = asyncio.create_task(self._measure_lag())
        self._ready.set()
        try:
            while not self._stop.is_set():
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), 0.2)
                if not self._stop.is_set() and all(s.fileno() == -1 for s in server.sockets):
                    server.close()
                    server = await asyncio.start_server(self._handle, "127.0.0.1", self.port)
        finally:
            ticker.cancel()
            server.close()
            self._close_open()

    async def _measure_lag(self) -> None:
        """How late this loop wakes up: the forwarding delay a busy driver used to add."""
        # perf_counter: monotonic ticks in 15.6 ms steps on Windows. The loop's own timer has
        # that granularity too, so a few tens of ms here are the OS, not a stall.
        step = 0.05
        while True:
            t0 = time.perf_counter()
            await asyncio.sleep(step)
            self.lag_max_secs = max(self.lag_max_secs, time.perf_counter() - t0 - step)

    async def _handle(self, client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
        """Forward one connection to the upstream store (or refuse it)."""
        if self._refuse:
            client_w.close()
            return
        try:
            node_r, node_w = await asyncio.open_connection("127.0.0.1", self._upstream)
        except OSError:
            client_w.close()
            return
        self._open |= {client_w, node_w}
        try:
            await asyncio.gather(self._pipe(client_r, node_w), self._pipe(node_r, client_w))
        finally:
            self._open -= {client_w, node_w}

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                await self._thaw.wait()  # frozen: read nothing more (TCP backpressure)
                data = await reader.read(65536)
                if not data:
                    break
                await self._thaw.wait()  # read as the freeze began: held, not dropped
                if self._delay_secs:
                    await asyncio.sleep(self._delay_secs)
                writer.write(data)
                await writer.drain()
        except OSError:
            pass
        finally:
            writer.close()


def _proxy_main(port: int, upstream: int) -> None:
    """A store proxy process: forward on its own loop, take switches from stdin, one per line.

    `set refuse|freeze 0|1`, `set delay_secs S`, `set upstream PORT`, `drop`, `lag` (answered
    `ok [value]`) and `stop`. It ends on `stop` or when the driver is gone (stdin closed), so
    it never outlives the driver; Ctrl-C is the driver's to handle (it stops this process).
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    proxy = _ProxyLoop(port, upstream).start()
    print(f"ready {port}", flush=True)
    try:
        for line in sys.stdin:
            words = line.split()
            if not words or words[0] == "stop":
                break
            reply = "ok"
            if words[0] == "drop":
                proxy.drop_all()
            elif words[0] == "lag":
                reply = f"ok {proxy.lag_max_secs}"
            elif words[:2] == ["set", "upstream"]:
                proxy.upstream = int(words[2])
            elif words[:2] == ["set", "delay_secs"]:
                proxy.delay_secs = float(words[2])
            elif words[0] == "set" and words[1] in ("refuse", "freeze"):
                setattr(proxy, words[1], words[2] == "1")
            else:
                reply = f"unknown command {line.strip()!r}"
            print(reply, flush=True)
    finally:
        proxy.stop()


class FaultProxy:
    """A TCP proxy in front of a store that the drills fault: refuse, drop, freeze, delay.

    One class for every drill (scenarios, soak, failover, gates B23 / B95), run as its own
    process (`gates_fleet.py proxy`). It used to share the driver's event loop with the
    in-process callers under test, so a busy driver (thousands of call tasks at 300 % load)
    delayed every forwarded store packet, workers' included, and the library looked slow for
    a reason it did not cause (audit CO-2). A thread was not enough: with the driver busy in
    Python for 2 s, round trips from another process through a proxy thread had p99 79-83 ms
    (max 93-326 ms); through this process p99 1.4-3 ms (median 0.5 ms either way; Python
    3.12, Windows, a loaded machine, measured 2026-11-15). Each switch is applied before
    `set` returns.
    """

    ANSWER_SECS = 15.0  # the proxy applies a switch within 10 s (`_ProxyLoop._apply`)

    def __init__(self, port: int | None = None, upstream: int = STACK_STORE_PORT) -> None:
        """Ready to `start()` on `port` (a free one by default), forwarding to `upstream`."""
        self.port = port or free_port()
        self._upstream = upstream
        self._refuse = False
        self._freeze = False
        self._delay_secs = 0.0
        self._lag_secs = 0.0  # the last reading, kept once the process is gone
        self._proc: subprocess.Popen[bytes] | None = None
        self._lock = threading.Lock()  # one command and its answer at a time

    def start(self) -> FaultProxy:
        """Start the proxy process; return once it listens (or raise why not)."""
        log = log_file("proxy")
        self._proc = subprocess.Popen(
            [PY, str(Path(__file__)), "proxy", str(self.port), str(self._upstream)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
        )
        log.close()
        assert self._proc.stdout is not None
        line = self._proc.stdout.readline()
        if not line.startswith(b"ready"):
            self.stop()
            raise RuntimeError(f"the store proxy on port {self.port} did not start: {line!r}")
        for name, value in (("refuse", self._refuse), ("freeze", self._freeze)):
            if value:  # set before start
                self._send(f"set {name} 1")
        if self._delay_secs:
            self._send(f"set delay_secs {self._delay_secs}")
        return self

    def stop(self) -> None:
        """End the process (killed if it does not stop at once); safe to call twice."""
        proc = self._proc
        if proc is None:
            return
        try:
            if proc.poll() is None:
                with contextlib.suppress(OSError, RuntimeError, ValueError):
                    self._lag_secs = float(self._send("lag") or 0.0)
                with contextlib.suppress(OSError):
                    assert proc.stdin is not None
                    proc.stdin.write(b"stop\n")
                    proc.stdin.flush()
                try:
                    proc.wait(5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(10)
        finally:
            for pipe in (proc.stdin, proc.stdout):
                if pipe is not None:
                    with contextlib.suppress(OSError):
                        pipe.close()

    def _send(self, command: str) -> str:
        """One command; its answer's value (`ok <value>`). Blocking: async code uses `_ask`.

        The answer is awaited at most `ANSWER_SECS`; a proxy that gives none is killed (its
        pipe then ends) and this raises, instead of hanging the driver for ever (N5).
        """
        with self._lock:
            proc = self._proc
            if proc is None or proc.poll() is not None:
                raise RuntimeError(f"the store proxy on port {self.port} is not running")
            stdin, stdout = proc.stdin, proc.stdout
            assert stdin is not None
            assert stdout is not None
            stdin.write(command.encode() + b"\n")
            stdin.flush()
            answer: list[bytes] = []
            reader = threading.Thread(target=lambda: answer.append(stdout.readline()), daemon=True)
            reader.start()
            reader.join(self.ANSWER_SECS)
            if reader.is_alive():
                proc.kill()  # its stdout ends, so the reader thread returns
                reader.join(5)
                raise RuntimeError(
                    f"store proxy: no answer to {command!r} in {self.ANSWER_SECS:g} s; killed"
                )
            reply = answer[0].decode().strip() if answer else ""
        if not reply.startswith("ok"):
            raise RuntimeError(f"store proxy: {command!r} answered {reply!r}")
        return reply[2:].strip()

    async def _ask(self, command: str) -> None:
        """`_send` in a thread, if the process runs (pipe reads on the driver's loop, N5)."""
        if self._running():
            await asyncio.to_thread(self._send, command)

    def _running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    async def set(
        self,
        *,
        refuse: bool | None = None,
        freeze: bool | None = None,
        delay_secs: float | None = None,
        upstream: int | None = None,
    ) -> None:
        """Switch faults, each applied before this returns (kept for `start` if not running).

        Async, not property setters: the proxy's answer is read in a thread (N5).
        """
        if refuse is not None:
            await self._ask(f"set refuse {int(refuse)}")
            self._refuse = refuse
        if freeze is not None:
            await self._ask(f"set freeze {int(freeze)}")
            self._freeze = freeze
        if delay_secs is not None:
            await self._ask(f"set delay_secs {delay_secs}")
            self._delay_secs = delay_secs
        if upstream is not None:
            await self._ask(f"set upstream {upstream}")
            self._upstream = upstream

    @property
    def refuse(self) -> bool:
        """New connections are closed at once (a store that is down)."""
        return self._refuse

    @property
    def freeze(self) -> bool:
        """Half-open: bytes are held, not dropped, until the thaw (see `_ProxyLoop.freeze`)."""
        return self._freeze

    @property
    def delay_secs(self) -> float:
        """Added before each forwarded chunk (a slow store or network)."""
        return self._delay_secs

    @property
    def upstream(self) -> int:
        """The store port new connections go to; open ones stay (a DNS name moving)."""
        return self._upstream

    async def drop_all(self) -> None:
        """Close every open connection (a node restart)."""
        await self._ask("drop")

    @property
    def lag_max_secs(self) -> float:
        """The proxy loop's worst lateness so far: the forwarding delay it added."""
        if self._running():
            with contextlib.suppress(OSError, RuntimeError, ValueError):
                self._lag_secs = float(self._send("lag") or 0.0)
        return self._lag_secs


def lag_note(*proxies: FaultProxy | None) -> str:
    """One line for a drill's report: the worst forwarding lateness of its store proxies."""
    worst = max((p.lag_max_secs for p in proxies if p is not None), default=0.0)
    return f"store proxy (own process): worst loop lag {worst * 1000:.0f} ms"


class MetricsToStore:
    """A metrics sink that adds counts into one store hash every second (per worker process)."""

    def __init__(self, prefix: str) -> None:
        """Counts kept in memory until flushed."""
        self.key = f"{prefix}:gates:metrics"
        self.counts: collections.Counter[str] = collections.Counter()  # float values

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        """A count, by name and reason / outcome."""
        tag = labels.get("reason") or labels.get("outcome") or ""
        key = f"{name}:{tag}" if tag else name
        self.counts[key] = self.counts[key] + int(value)

    def observe(self, name: str, value: float, **labels: str) -> None:
        """Not needed by the gates."""

    def gauge(self, name: str, value: float, **labels: str) -> None:
        """Not needed by the gates."""

    async def send(self, client: Any) -> None:
        """Add and clear what was counted.

        Not named `flush`: the engine calls a sink's `flush()` (no arguments) when it stops,
        which logged `stop_metrics_flush_failed` on every drill worker.
        """
        counts, self.counts = self.counts, collections.Counter()
        if counts:
            async with client.pipeline(transaction=False) as pipe:
                for name, value in counts.items():
                    pipe.hincrbyfloat(self.key, name, value)
                await pipe.execute()


async def _worker_main() -> None:
    """A worker process: start, count metrics into the store, stop on `stop`."""
    import redis.asyncio

    from llm_queue import LLMQueue

    setup_logging()
    sink = MetricsToStore(os.environ["LLMQ_KEY_PREFIX"])
    llmq = LLMQueue.from_env(metrics=sink, credentials=account_keys, tuning=drill_tuning())
    await llmq.start()
    client = redis.asyncio.Redis.from_url(STORE)

    async def flusher() -> None:
        while True:
            await asyncio.sleep(1)
            await sink.send(client)

    task = asyncio.create_task(flusher())
    loop = asyncio.get_running_loop()
    lines: asyncio.Queue[str] = asyncio.Queue()

    def read() -> None:  # a daemon thread: a SIGTERM must not wait for a stdin line
        for line in iter(sys.stdin.readline, ""):
            loop.call_soon_threadsafe(lines.put_nowait, line.strip())
        loop.call_soon_threadsafe(lines.put_nowait, "eof")

    threading.Thread(target=read, name="stdin", daemon=True).start()
    if sys.platform != "win32":  # a pod's SIGTERM (scenario D1); Windows sends "stop"
        loop.add_signal_handler(signal.SIGTERM, lines.put_nowait, "sigterm")
    print(f"ready {llmq._engine.worker_id}", flush=True)
    while True:  # "stats" (the soak's debug hook), "drain" (D1) until "stop", SIGTERM or EOF
        command = await lines.get()
        if command == "stats":
            print(json.dumps(process_stats()), flush=True)
        elif command == "drain":
            print(json.dumps(llmq.drain_status(), default=str), flush=True)
        else:
            break
    llmq.pause_receiving()
    await llmq.stop(drain_secs=float(os.environ.get("LLMQ_DRILL_STOP_DRAIN_SECS") or 30))
    task.cancel()
    await sink.send(client)
    await client.aclose()


async def _caller_main(calls: int, model: str) -> None:
    """A caller process: run `calls` calls at once, print the outcome counts as JSON."""
    from llm_queue import CallFailed, LLMQueue

    llmq = LLMQueue.from_env(credentials=account_keys, tuning=drill_tuning())
    await llmq.start()
    print("ready", flush=True)
    await asyncio.to_thread(sys.stdin.readline)  # "go"
    outcomes: collections.Counter[str] = collections.Counter()
    request = {"messages": [{"role": "user", "content": [{"text": "hi"}]}]}

    async def one() -> None:
        try:
            await llmq.converse(model, request)
            outcomes["ok"] += 1
        except CallFailed as exc:
            outcomes[exc.kind] += 1

    await asyncio.gather(*(one() for _ in range(calls)))
    print(json.dumps(outcomes), flush=True)
    await llmq.stop(drain_secs=1)


class _Events(logging.Handler):
    """The library's log events seen in this process (`registry_older_ignored` ...)."""

    def __init__(self) -> None:
        """No events yet."""
        super().__init__(logging.INFO)
        self.seen: set[str] = set()

    def emit(self, record: logging.LogRecord) -> None:
        """Keep the event name."""
        fields = getattr(record, "llmq", None)
        if isinstance(fields, dict) and fields.get("event"):
            self.seen.add(str(fields["event"]))


def expected_text(messages: list[Any], output_tokens: int | None = None) -> str:
    """The reply text fakellm gives these messages (a pure function of the request)."""
    sys.path.insert(0, str(ROOT / "tools" / "fakellm"))
    from fakellm.synth import reply_text

    text = str(reply_text({"messages": messages, "system": None, "toolConfig": None}))
    if output_tokens:  # padded to the fixed length, as fakellm's `_reply` does
        text = ((text + " ") * (output_tokens * 4 // (len(text) + 1) + 1))[: output_tokens * 4]
    return text


async def _client_main(spec: dict[str, Any]) -> None:
    """A client pod: paced calls, each with its own marker; per-call results as JSON.

    `spec`: op (`converse` / `stream`), model, calls, rate (a second), tag (in each marker),
    priority_every (every n-th call priority, 0: none), output_tokens (streams' fixed length).
    Prints `{"ready": ...}` once started, waits for `go`, then prints one `{"results": ...}`.
    """
    from llm_queue import CallFailed, LLMQueue

    setup_logging()
    events = _Events()
    logging.getLogger("llm_queue").addHandler(events)
    logging.getLogger("llm_queue").setLevel(logging.INFO)  # the registry events are INFO
    llmq = LLMQueue.from_env(credentials=account_keys, tuning=drill_tuning())
    await llmq.start()
    view = llmq.admin.registry.current
    model = str(spec["model"])
    print(json.dumps({"ready": True, "worker_id": llmq._engine.worker_id,
                      "version": int(view.registry.version)}), flush=True)  # fmt: skip
    await asyncio.to_thread(sys.stdin.readline)  # "go"
    t0 = time.monotonic()
    every = int(spec.get("priority_every", 0))

    async def one(i: int) -> dict[str, Any]:
        marker = f"{spec['tag']}-{i}-{uuid7().hex[-12:]}"
        messages = [{"role": "user", "content": [{"text": f"marker {marker}"}]}]
        request = {"messages": messages, "inferenceConfig": {"maxTokens": 400}}
        want = expected_text(messages, spec.get("output_tokens"))
        sent = time.monotonic()
        out: dict[str, Any] = {"i": i, "marker": marker, "sent": round(sent - t0, 3)}
        text = ""
        try:
            if spec["op"] == "stream":
                async for event in llmq.converse_stream(model, request, priority=bool(
                        every and i % every == 0)):  # fmt: skip
                    text += ((event.get("contentBlockDelta") or {}).get("delta") or {}).get(
                        "text", ""
                    )
                out.update(ok=True, worker="")
            else:
                reply = await llmq.converse(model, request, priority=bool(every and i % every == 0))
                response = reply.response or {}
                text = response["output"]["message"]["content"][0]["text"]
                out.update(ok=True, worker=reply.meta.worker, request_id=reply.request_id)
            out["own_reply"] = text == want
        except CallFailed as exc:
            out.update(ok=False, kind=exc.kind, partial=bool(getattr(exc, "partial_text", None)))
        except Exception as exc:  # not the library's CallFailed: a bug, recorded as its own kind
            out.update(ok=False, kind=f"raised {type(exc).__module__}.{type(exc).__name__}")
        out["secs"] = round(time.monotonic() - sent, 3)
        return out

    tasks = []
    rate = float(spec["rate"])
    for i in range(int(spec["calls"])):
        tasks.append(asyncio.create_task(one(i)))
        await asyncio.sleep(max(0.0, t0 + (i + 1) / rate - time.monotonic()))
    results = await asyncio.gather(*tasks)
    view = llmq.admin.registry.current
    report = {
        "results": results,
        "events": sorted(events.seen),
        "version": int(view.registry.version),
        "rpm": view.value(model, "rpm"),
    }
    print(json.dumps(report), flush=True)
    await llmq.stop(drain_secs=1)


if __name__ == "__main__":
    if sys.argv[1] == "worker":
        asyncio.run(_worker_main())
    elif sys.argv[1] == "caller":
        asyncio.run(_caller_main(int(sys.argv[2]), sys.argv[3]))
    elif sys.argv[1] == "client":
        asyncio.run(_client_main(json.loads(sys.argv[2])))
    elif sys.argv[1] == "proxy":
        _proxy_main(int(sys.argv[2]), int(sys.argv[3]))
