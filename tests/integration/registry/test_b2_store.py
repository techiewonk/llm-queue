"""0.1.0b2 registry store fixes against real Valkey (RESCAN R1, R2, R5, R6, R10, X3, X4, C5).

Each test is a sequence that went wrong on 0.1.0b1: a pod that crashed or looped on a stored
document it could not use, an edit typo that did nothing, a deploy that lifted an emergency
block, and live-edit and audit gaps.
"""

from __future__ import annotations

import asyncio
import copy
from typing import Any

import orjson
import pytest
import redis.asyncio

from llm_queue.errors import ConfigError
from llm_queue.registry import RegistryError
from llm_queue.registry.store import LiveEditRefused, RegistryStore

pytestmark = pytest.mark.integration


def _store(
    client: redis.asyncio.Redis, prefix: str, doc: dict[str, Any], mode: str = "safe"
) -> RegistryStore:
    return RegistryStore(client, prefix, config=doc, live_edit=mode)


async def _plant(
    client: redis.asyncio.Redis, store: RegistryStore, doc: Any, version: int, changes: int = 7
) -> None:
    """Write a stored document directly, as another (newer or broken) writer would."""
    body = doc if isinstance(doc, bytes) else orjson.dumps(doc)
    await client.set(store.doc_key, body)
    await client.hset(store.meta_key, mapping={"version": version, "changes": changes})


async def test_boot_on_invalid_newer_registry_runs_on_own_config(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    """R1: a new pod crashed at start when the stored registry was invalid and newer."""
    store = _store(valkey, prefix, full)
    await _plant(valkey, store, {"registry_version": 5, "models": "broken"}, version=5)
    view = await store.boot()
    assert view.registry.version == 1
    assert (store.on_own_config, store.stored_refused, store.lib_too_old) == (True, True, False)
    assert store.changes == 7  # the stored numbers' counter: admission does not loop
    await store.refresh()  # stored is newer: left alone
    assert orjson.loads(await valkey.get(store.doc_key) or b"{}")["models"] == "broken"


@pytest.mark.parametrize("stored_version", [1, 0])
async def test_invalid_registry_not_newer_is_replaced(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any], stored_version: int
) -> None:
    """R1: a running pod looped on `stale_config` while an invalid document stayed stored."""
    store = _store(valkey, prefix, full)
    await store.boot()
    await _plant(valkey, store, b"{broken", version=stored_version, changes=store.changes)
    await store.load()
    refused = store.stored_refused
    await store.refresh()
    assert (refused, store.stored_refused) == (True, False)
    stored = orjson.loads(await valkey.get(store.doc_key) or b"{}")
    assert stored["models"]["gpt20b"]["rpm"] == 1000
    entries = await store.log(1)
    assert entries[0]["actor"].startswith("restore@")


async def test_registry_needing_newer_library_is_never_overwritten(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    """C5: an older pod must stay up and never publish over a newer library's registry."""
    newer = copy.deepcopy(full)
    newer["min_lib_version"] = "99.0"
    store = _store(valkey, prefix, full)
    await _plant(valkey, store, newer, version=1)
    view = await store.boot()
    assert view.registry.version == 1
    assert (store.lib_too_old, store.on_own_config) == (True, True)
    mine = copy.deepcopy(full)
    mine["registry_version"] = 9
    assert (await store.publish(mine, actor="deploy")).action == "lib_too_old"
    await store.refresh()
    assert orjson.loads(await valkey.get(store.doc_key) or b"{}")["min_lib_version"] == "99.0"
    with pytest.raises(ConfigError, match=r"needs llm-queue 99\.0"):
        await store.set({"rpm": 5}, model="gpt20b", actor="op")


async def test_edit_typo_is_refused_but_stored_unknowns_are_kept(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    """R2: `rmp` was accepted by a live edit (lenient check) and did nothing."""
    store = _store(valkey, prefix, full)
    await store.boot()
    with pytest.raises(RegistryError, match="rmp"):
        await store.set({"rmp": 5}, model="gpt20b", actor="op")
    newer = copy.deepcopy(full)
    newer["models"]["gpt20b"]["future_setting"] = 1  # written by a newer library
    await valkey.set(store.doc_key, orjson.dumps(newer))
    await store.set({"rpm": 5}, model="gpt20b", actor="op")  # other paths still editable
    assert store.current.value("gpt20b", "rpm") == 5


async def test_blocks_and_kill_switch_survive_a_publish(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    """R5: a redeploy (higher version) lifted emergency blocks and the kill switch."""
    store = _store(valkey, prefix, full)
    await store.boot()
    await store.block(region="us-east-1", on=True, actor="op")
    await store.set({"routing.kill_switch": True}, actor="op")
    v2 = copy.deepcopy(full)
    v2["registry_version"] = 2
    assert (await store.publish(v2, actor="deploy")).action == "published"
    routing = store.current.registry.routing
    assert routing.blocked_regions == ("us-east-1",)
    assert routing.kill_switch is True
    # The same deploy again is `kept`: the carried routing does not count as new content.
    assert (await store.publish(v2, actor="deploy")).action == "kept"
    await store.block(region="us-east-1", on=False, actor="op")
    assert store.current.registry.routing.blocked_regions == ()


async def test_live_edit_mode_comes_from_the_document(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    """R6: the mode came from each process, so one pod with `all` bypassed the others' `safe`."""
    doc = copy.deepcopy(full)
    doc["live_edit"] = "off"
    store = _store(valkey, prefix, doc, mode="all")
    await store.boot()
    with pytest.raises(LiveEditRefused, match="live edits are off"):
        await store.set({"rpm": 5}, model="gpt20b", actor="op")
    doc["live_edit"] = "safe"
    other = _store(valkey, prefix + "x", doc, mode="all")
    await other.boot()
    with pytest.raises(LiveEditRefused, match="deploy"):
        await other.set({"live_edit": "all"}, actor="op")
    with pytest.raises(LiveEditRefused, match="region lists"):
        await other.set(
            {"deployments.aws-main.confirmed": ["ap-south-1"]}, model="gpt20b", actor="o"
        )
    for key in await valkey.keys(f"{prefix}x:*"):
        await valkey.delete(key)


@pytest.mark.parametrize(
    "values",
    [
        {"model_fallback.models": ["gpt20b"]},
        {"inference.extra.reasoning_effort": "high"},
    ],
)
async def test_routing_and_provider_fields_protected_in_safe_mode(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any], values: dict[str, Any]
) -> None:
    """X3 / X4: safe mode let an edit reroute calls or add provider fields."""
    store = _store(valkey, prefix, full)
    await store.boot()
    with pytest.raises(LiveEditRefused, match="only with a deploy"):
        await store.set(values, model="gpt120b", actor="op")


@pytest.mark.parametrize("actor", ["op\nfake entry", "op\x1b[2J", "", "x" * 201])
async def test_audit_actor_refuses_control_characters(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any], actor: str
) -> None:
    store = _store(valkey, prefix, full)
    await store.boot()
    with pytest.raises(ConfigError, match="actor"):
        await store.set({"rpm": 5}, model="gpt20b", actor=actor)


async def test_boot_actor_names_the_process_and_loads_share_one_read(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """R10: boot was audited as plain `boot`; a `stale_config` burst read the store per call."""
    store = _store(valkey, prefix, full)
    await store.boot()
    assert (await store.log(1))[0]["actor"].startswith("boot@")
    reads = 0
    real = store._read

    async def counted() -> Any:
        nonlocal reads
        reads += 1
        await asyncio.sleep(0.05)
        return await real()

    monkeypatch.setattr(store, "_read", counted)
    await asyncio.gather(*(store.load() for _ in range(20)))
    assert reads == 1
