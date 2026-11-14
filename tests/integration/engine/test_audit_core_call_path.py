"""Registry failures on the call path end in `CallFailed`, never a raw error (G7, CO-9).

A call whose model's admission numbers are gone (`no_config`) runs `registry.refresh()`
inside the call; when the stored registry is gone too, that refresh republishes the
process's own config. These pin what the caller sees when that restore fails. Run on both
engines (Valkey and Redis), like every engine test.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from engine_testkit import CHAT, Make

from llm_queue.engine.errors import CallFailed
from llm_queue.engine.runtime import Engine
from llm_queue.registry.store import RegistryConflict

pytestmark = pytest.mark.integration


async def _lose_the_registry(engine: Engine, monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Flush the registry and the model's numbers; every publish (the restore) then fails."""
    reg = engine.registry
    await engine.state.delete(reg.doc_key, reg.meta_key, reg.numbers_key("small"))
    tries: list[int] = []

    async def conflict(*args: Any, **kwargs: Any) -> Any:
        tries.append(1)
        raise RegistryConflict("the registry kept changing (3 tries); try again")

    monkeypatch.setattr(reg, "publish", conflict)
    return tries


async def test_direct_call_with_a_failed_restore_is_call_failed(
    make: Make, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """G7: a failed registry restore inside a direct call reached the caller as RegistryConflict.

    CO-9 made `_restore` log the failure instead; the call now ends in `CallFailed`. Pinned on
    the current runtime, which calls `registry.refresh()` unguarded (the runtime guard of CO-9
    is still open): the store's own catch is what keeps the error in the family. Today the
    direct attempt's `queue_unavailable` is followed by the call's deadline, so the kind is
    either one.
    """
    engine, _ = await make()
    tries = await _lose_the_registry(engine, monkeypatch)
    with caplog.at_level(logging.ERROR), pytest.raises(CallFailed) as info:
        await engine.call(
            "small", CHAT, request_id="r-g7-direct", force_direct=True, timeout_secs=3
        )
    assert info.value.kind in ("queue_unavailable", "deadline_exceeded")
    assert tries, "the call never tried to restore the registry"
    assert "registry_restore_failed" in caplog.text


async def test_queued_call_with_a_failed_restore_is_call_failed(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G7: the same through the queue: the worker keeps the call, the caller gets `CallFailed`.

    The worker releases the held call for a later retry, so the caller ends at its deadline;
    no raw `RegistryConflict` reaches either side.
    """
    engine, _ = await make()
    await _lose_the_registry(engine, monkeypatch)
    with pytest.raises(CallFailed):
        await engine.call("small", CHAT, request_id="r-g7-queued", timeout_secs=3)
