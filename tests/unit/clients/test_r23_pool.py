"""Client pool and hub fixes (FC-5 to FC-18, R3SE-11, R3SE-13, R3PF-9, Q43).

Each test names its finding. fakellm (in process) or fake clients; moto as STS where a role is
assumed. No network, no real model.
"""

from __future__ import annotations

import asyncio
import collections.abc
import dataclasses
import socket
import sys
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from clients_testkit import StartFake, pool_limit, registry_doc
from moto.server import ThreadedMotoServer

from llm_queue.clients import (
    ClientHub,
    ClientPool,
    CredentialsUnavailable,
    Providers,
    Route,
    Timing,
    WarmOptions,
    bedrock_http,
    route_for,
)
from llm_queue.clients import pool as pool_module
from llm_queue.clients.hub import Lease
from llm_queue.registry import ResolvedRegistry, load_registry

CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
PLAIN = Timing(5, 5, attempt_secs=5)
NO_WARM = WarmOptions(connections=0)


def _keys(account: Any) -> dict[str, Any]:
    return {"aws_access_key_id": account.name, "aws_secret_access_key": "x"}


def _route(view: ResolvedRegistry, model: str = "gpt120b", account: str = "aws-a") -> Route:
    return route_for(view, next(d for d in view.deployments(model) if d.account == account))


def test_plain_text_blocks_skip_the_blob_walk(monkeypatch: pytest.MonkeyPatch) -> None:
    """FC-5: every text block ran ~8 `Mapping` ABC checks: 243 us per 40-message request.

    A block with none of the blob keys is skipped first; measured 629 -> 60 us at 40
    messages, 6.9 -> 0.5 us per streamed delta (Python 3.12.13, 2026-11-15).
    """
    checks = 0

    class Counting(type):
        def __instancecheck__(cls, obj: object) -> bool:
            nonlocal checks
            checks += 1
            return isinstance(obj, collections.abc.Mapping)

    class CountingMapping(metaclass=Counting):
        pass

    monkeypatch.setattr(bedrock_http, "Mapping", CountingMapping)
    request = {"messages": [{"role": "user", "content": [{"text": "hi"}]} for _ in range(40)]}
    assert not bedrock_http._text_blobs(request)
    assert checks <= 2 * 40  # the message and block checks of `_text_blobs` only
    image = {"image": {"format": "png", "source": {"bytes": "aGk="}}}
    assert bedrock_http._blob_paths(image)  # a blob block is still found


class _Client:
    def __init__(self) -> None:
        self.closed = 0

    async def close(self) -> None:
        self.closed += 1


async def test_two_concurrent_acloses_release_each_client_once() -> None:
    """FC-7: both acloses captured the same lists and released every lease twice.

    `refs` went below zero, and for a client shared with a second pool the extra release
    closed it under that pool.
    """
    hub = ClientHub()
    pool = ClientPool(hub=hub, warm=NO_WARM)
    live, retired = _Client(), _Client()
    held = hub.acquire_now(("a",), lambda: (live, live.close))
    old = hub.acquire_now(("b",), lambda: (retired, retired.close))
    pool._bedrock[("x", "r")] = held
    pool._retire([old])
    await asyncio.gather(pool.aclose(), pool.aclose())
    assert (live.closed, retired.closed) == (1, 1)
    assert (held.refs, old.refs) == (0, 0)


async def test_a_hung_credentials_callable_fails_in_time(
    start_fake: StartFake, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FC-8: a `credentials=` callable that never returned held the build lock for good.

    The lookup is bounded now: `CredentialsUnavailable`, and the next call asks again.
    """
    monkeypatch.setattr(pool_module, "LOOKUP_TIMEOUT_SECS", 0.2)
    gate = threading.Event()
    asked: list[int] = []

    def credentials(account: Any) -> dict[str, Any]:
        asked.append(1)
        if len(asked) == 1:
            gate.wait(5)
        return _keys(account)

    url, _ = await start_fake()
    pool = ClientPool(bedrock_endpoint_url=url, credentials=credentials, warm=NO_WARM)
    route = _route(load_registry(registry_doc(url)))
    try:
        with pytest.raises(CredentialsUnavailable, match="lookup took more than"):
            await pool.bedrock(route)
        assert await pool.bedrock(route) is not None
        assert len(asked) == 2
    finally:
        gate.set()
        await pool.aclose()


def test_the_default_lookups_need_no_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    """FC-18: the pod chain and `os.environ.get` each took a thread hop per build."""

    async def no_thread(*_: Any) -> Any:
        raise AssertionError("a thread hop")

    monkeypatch.setattr(asyncio, "to_thread", no_thread)
    monkeypatch.setenv("LLMQ_ACCOUNT_X", "v")
    pool = ClientPool()
    assert asyncio.run(pool._lookup(pool._secrets, "LLMQ_ACCOUNT_X", "x")) == "v"
    assert asyncio.run(pool._lookup(pool._credentials, None, "x")) is None


async def test_an_openai_account_edit_retires_the_old_client(
    start_fake: StartFake, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FC-10: after a `base_url` edit the old client and its connections stayed until aclose."""
    monkeypatch.setattr(pool_module, "RETIRE_GRACE_SECS", 0)
    url, _ = await start_fake()
    route = _route(load_registry(registry_doc(url)), "local", "vllm")
    edited = dataclasses.replace(
        route, account=dataclasses.replace(route.account, base_url=f"{url}/v2")
    )
    pool = ClientPool(secrets={"LLMQ_ACCOUNT_VLLM_KEY": "k"}.get, warm=NO_WARM)
    try:
        first = await pool.openai(route)
        closed: list[bool] = []
        original = first.close

        async def close() -> None:
            closed.append(True)
            await original()

        first.close = close  # type: ignore[method-assign]
        pool._openai[pool_module._identity(route.account)].close = close
        await pool.openai(edited)
        for _ in range(100):
            if closed:
                break
            await asyncio.sleep(0.01)
        assert closed == [True]
        assert len(pool._openai) == 1
    finally:
        await pool.aclose()


async def test_refreshed_keys_already_expired_fail_over(start_fake: StartFake) -> None:
    """FC-11: a refresh returning a past expiry raised aiobotocore's RuntimeError (FATAL).

    The call failed `fatal` instead of trying the next account; it is `CredentialsUnavailable`.
    """
    asked: list[int] = []

    def credentials(account: Any) -> dict[str, Any]:
        asked.append(1)
        when = timedelta(seconds=60) if len(asked) == 1 else timedelta(seconds=-60)
        return {**_keys(account), "aws_session_token": "t", "expiry_time": datetime.now(UTC) + when}

    url, _ = await start_fake()
    providers = Providers(
        ClientPool(bedrock_endpoint_url=url, credentials=credentials, warm=NO_WARM)
    )
    route = _route(load_registry(registry_doc(url)))
    try:
        with pytest.raises(CredentialsUnavailable, match="already expired"):
            await providers.get("bedrock").call("converse", CHAT, route, PLAIN)
    finally:
        await providers.aclose()


def test_a_dead_loops_lock_is_dropped_with_its_entry() -> None:
    """FC-14: `_live` dropped a dead loop's entry but kept its lock for a reused `id(loop)`."""
    hub = ClientHub()
    loop = asyncio.new_event_loop()
    loop.close()
    key = (id(loop), "k")
    hub._entries[key] = Lease(key, object(), loop, _Client().close)
    hub._locks[key] = asyncio.Lock()
    assert hub._live(key) is None
    assert key not in hub._locks


async def test_a_failing_close_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    """FC-15: an exception from a client's close was swallowed without a log line."""
    import logging

    async def broken() -> None:
        raise OSError("close failed")

    hub = ClientHub()
    lease = hub.acquire_now(("k",), lambda: (object(), broken))
    with caplog.at_level(logging.DEBUG):
        await hub.release(lease)
    assert any("client_close_failed" in r.getMessage() for r in caplog.records)


async def test_borrows_are_capped_at_the_pool_size() -> None:
    """R3PF-9: caller-only engines had no cap; the 101st call waited for a socket in its timer.

    Borrows beyond the client's pool size now wait before the provider's timers start.
    """
    hub = ClientHub()
    pool = ClientPool(hub=hub, warm=NO_WARM)
    lease = hub.acquire_now(("k",), lambda: (object(), _Client().close))
    lease.cap(2)
    running = peak = 0

    async def borrow() -> None:
        nonlocal running, peak
        async with pool_module._capped(lease):
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0.01)
            running -= 1

    await asyncio.gather(*(borrow() for _ in range(10)))
    assert peak == 2
    await pool.aclose()


async def test_a_new_plan_resizes_built_clients(start_fake: StartFake) -> None:
    """Q43: a client kept its pool size until a restart; a new plan rebuilds it when idle."""
    url, _ = await start_fake()
    route = _route(load_registry(registry_doc(url)))
    pool = ClientPool(bedrock_endpoint_url=url, credentials=_keys, warm=NO_WARM)
    try:
        pool.plan({("aws-a", route.region): 12})
        first = await pool.bedrock(route)
        assert pool_limit(first) == 12
        pool.plan({("aws-a", route.region): 40})
        second = await pool.bedrock(route)
        assert second is not first
        assert pool_limit(second) == 40
    finally:
        await pool.aclose()


async def test_the_openai_pool_is_sized_by_its_slots(start_fake: StartFake) -> None:
    """R3PF-9: OpenAI pools were always 100 connections, whatever the slots routed to them."""
    url, _ = await start_fake()
    route = _route(load_registry(registry_doc(url)), "local", "vllm")
    pool = ClientPool(secrets={"LLMQ_ACCOUNT_VLLM_KEY": "k"}.get, warm=NO_WARM)
    try:
        pool.plan({("vllm", route.region): 300})
        await pool.openai(route)
        assert pool._openai[pool_module._identity(route.account)].size == 300
    finally:
        await pool.aclose()


async def test_a_rotated_api_key_is_used_after_an_auth_failure(start_fake: StartFake) -> None:
    """R3SE-11: an OpenAI key was read once per account and kept for the process's life.

    An auth failure makes the worker `forget` the route; the next build reads the key again.
    """
    url, _ = await start_fake()
    route = _route(load_registry(registry_doc(url)), "local", "vllm")
    secrets = {"LLMQ_ACCOUNT_VLLM_KEY": "old"}
    pool = ClientPool(secrets=secrets.get, warm=NO_WARM)
    try:
        assert (await pool.openai(route)).api_key == "old"
        secrets["LLMQ_ACCOUNT_VLLM_KEY"] = "new"
        pool.forget(route)
        assert (await pool.openai(route)).api_key == "new"
    finally:
        await pool.aclose()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="module")
def sts_url() -> Iterator[str]:
    port = _free_port()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    yield f"http://127.0.0.1:{port}"
    server.stop()


async def test_the_external_id_is_read_on_every_assume_role(
    start_fake: StartFake, sts_url: str
) -> None:
    """R3SE-11: the external id was read once and captured; a rotated one failed every refresh."""
    url, _ = await start_fake()
    doc = registry_doc(url)
    doc["accounts"]["aws-b"] = {
        "kind": "aws",
        "role_arn": "arn:aws:iam::123456789012:role/llmq-bedrock",
        "external_id_ref": "LLMQ_ACCOUNT_B_EXTERNAL_ID",
    }
    route = _route(load_registry(doc), account="aws-b")
    read: list[str] = []

    def secrets(name: str) -> str | None:
        read.append(name)
        return "ext-1"

    pool = ClientPool(
        bedrock_endpoint_url=url,
        credentials=lambda a: {"aws_access_key_id": "base", "aws_secret_access_key": "x"},
        secrets=secrets,
        sts_endpoint_url=sts_url,
        warm=NO_WARM,
    )
    try:
        session = await pool._role_session(route)
        before = len(read)
        await session._credentials._refresh_using()  # type: ignore[attr-defined]
        assert len(read) == before + 1
    finally:
        await pool.aclose()


async def test_a_missing_private_openai_module_is_not_a_missing_extra(
    start_fake: StartFake, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R3SE-13: `openai._constants` failing to import was reported as `MissingExtra("openai")`."""
    url, _ = await start_fake()
    route = _route(load_registry(registry_doc(url)), "local", "vllm")
    monkeypatch.setitem(sys.modules, "openai._constants", None)  # an import now fails
    pool = ClientPool(secrets={"LLMQ_ACCOUNT_VLLM_KEY": "k"}.get, warm=NO_WARM)
    try:
        assert await pool.openai(route) is not None
    finally:
        await pool.aclose()
