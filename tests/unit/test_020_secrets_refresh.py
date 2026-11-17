"""Secret refresh after a failure is jittered, and a refused store login re-reads (review S-2)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from redis.exceptions import AuthenticationError

from llm_queue import secrets
from llm_queue.engine.settings import Layers
from llm_queue.engine.store_client import BusyPool, FreshPassword, secret_store_client
from llm_queue.tuning import StoreOptions


class _Source:
    """A source whose value changes and whose reads can fail."""

    kind = "fake"
    label = "fake:test"

    def __init__(self) -> None:
        self.value = "one"
        self.reads = 0
        self.fail = False

    def load(self) -> Mapping[str, str]:
        self.reads += 1
        if self.fail:
            raise RuntimeError("down")
        return {"LLMQ_STORE_PASSWORD": self.value}


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_a_failed_refresh_is_retried_after_a_jittered_wait() -> None:
    clock = _Clock()
    seen = set()
    for _ in range(20):
        source = _Source()
        reader = secrets.SecretReader([source], cache_secs=300, clock=clock)
        reader.check()
        clock.now += 301
        source.fail = True
        assert reader("LLMQ_STORE_PASSWORD") == "one"  # last values kept
        cached = reader._sources[0]
        retry = cached.read_at - clock.now + 300
        assert 30 * 0.8 <= retry <= 30 * 1.2
        seen.add(round(retry, 6))
    assert len(seen) > 1  # spread, not one fixed second


def test_invalidate_rereads_but_not_more_than_every_few_seconds() -> None:
    clock = _Clock()
    source = _Source()
    reader = secrets.SecretReader([source], cache_secs=300, clock=clock)
    reader.check()
    source.value = "two"
    reader.invalidate()  # read just now: kept, a wrong password cannot hammer the source
    assert reader("LLMQ_STORE_PASSWORD") == "one"
    clock.now += secrets.INVALIDATE_MIN_SECS
    reader.invalidate()
    assert reader("LLMQ_STORE_PASSWORD") == "two"
    assert source.reads == 2


def test_fresh_password_invalidates_the_layers_reader() -> None:
    clock = _Clock()
    source = _Source()
    reader = secrets.SecretReader([source], cache_secs=300, clock=clock)
    layers = Layers({}, reader)
    provider = FreshPassword(layers.get, "LLMQ_STORE_PASSWORD")
    assert provider.get_credentials() == ("one",)
    source.value = "two"
    clock.now += 10
    provider.invalidate()
    assert provider.get_credentials() == ("two",)


async def test_a_refused_login_calls_the_pool_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    client = secret_store_client(
        "redis://localhost:6379/0", lambda _: "pw", "LLMQ_STORE_PASSWORD", 4, StoreOptions()
    )
    pool = client.connection_pool
    assert isinstance(pool, BusyPool)
    pool.auth_failed = lambda: calls.append("invalidated")

    async def refuse(self: Any, *args: Any, **kwargs: Any) -> Any:
        raise AuthenticationError("WRONGPASS")

    monkeypatch.setattr(BusyPool.__mro__[1], "get_connection", refuse)
    with pytest.raises(AuthenticationError):
        await pool.get_connection()
    assert calls == ["invalidated"]
    await client.aclose()
