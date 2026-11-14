"""Scenario C1 ("callers cancel"): a cancelled call's provider attempt must stop, and be seen to.

Each test failed on the code before its fix.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
import redis.asyncio
from aiohttp import web
from engine_testkit import CHAT, SMALL, FakeEngine, Make, Scenario, create_app, registry, stats

from llm_queue.clients import ClientPool, Providers
from llm_queue.engine.admission import Decision
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.runtime import Engine, EngineSettings
from llm_queue.registry.store import RegistryStore

pytestmark = pytest.mark.integration
SLOW = {"latency_ms": {"p50": 2000, "p95": 2000}}
LONG = {"small": {"attempt_timeout_secs": 8}}  # 2 s answers are not attempt timeouts


@pytest.fixture
async def served_like_cli(
    store: redis.asyncio.Redis, prefix: str
) -> AsyncIterator[tuple[Engine, FakeEngine]]:
    """An engine on a fakellm served as `python -m fakellm` serves it (a plain AppRunner).

    aiohttp's TestServer turns handler cancellation on; `web.run_app` and AppRunner do not.
    """
    fake = FakeEngine(Scenario.from_mapping({"models": {f"{SMALL}@r1": SLOW}}))
    runner = web.AppRunner(create_app(engine=fake))
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    pool = ClientPool(
        bedrock_endpoint_url=f"http://127.0.0.1:{port}",
        credentials=lambda account: {
            "aws_access_key_id": account.name,
            "aws_secret_access_key": "x",
        },
    )
    engine = Engine(
        store,
        RegistryStore(store, prefix, config=registry(LONG)),
        Providers(pool),
        settings=EngineSettings(lease_secs=3, poll_secs=0.2, sweep_secs=1),
    )
    await engine.start()
    yield engine, fake
    await engine.stop(drain_secs=1)
    await engine.providers.aclose()
    await runner.cleanup()


async def test_cancelled_attempt_is_not_counted_answered(
    served_like_cli: tuple[Engine, FakeEngine],
) -> None:
    """C1 failed at every load: fakellm counted `ok` for a call whose client had hung up.

    `python -m fakellm` left the handler running after the engine closed the connection, so
    each cancelled attempt slept its latency out and counted as answered.
    """
    engine, fake = served_like_cli
    task = asyncio.create_task(engine.call("small", CHAT, request_id="c1"))
    await asyncio.sleep(0.5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(2.5)  # past the provider latency
    counters = stats(fake, f"{SMALL}@r1")
    assert counters["calls"] == 1
    assert counters["ok"] == 0
    assert counters["concurrent"] == 0


async def test_cancel_read_before_the_run_started_stops_it(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancel the reader got between the grant and the run task's start was dropped.

    The call then ran to its end, unless the heartbeat's key check (a third of the lease;
    10 s in production) caught it. The check is off here, so only the channel can stop it.
    """
    engine, fake = await make(LONG, {"models": {f"{SMALL}@r1": SLOW}})

    async def no_check() -> None:
        return None

    monkeypatch.setattr(engine, "_check_cancels", no_check)
    real = engine.admission.pick_take

    async def grant_then_cancel(*args: Any, **kwargs: Any) -> Decision:
        decision: Decision = await real(*args, **kwargs)
        if decision.kind == "granted":
            await engine.cancel("early")
            await asyncio.sleep(0.3)  # the reader has the message; the run task is not made
        return decision

    monkeypatch.setattr(engine.admission, "pick_take", grant_then_cancel)
    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(CallFailed) as info:
        await engine.call("small", CHAT, request_id="early")
    assert info.value.kind == "cancelled"
    assert loop.time() - started < 1.5  # stopped, not waited out (2 s)
    assert stats(fake, f"{SMALL}@r1").get("calls", 0) == 0
