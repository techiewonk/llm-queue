"""0.1.0b2 command line (RESCAN X10, plan D) against real Valkey.

0.1.0b1 purged dead letters, redrove, drained and garbage-collected on a bare command (one typo
in shell history away), left a removed model's queues (and the prompts in their dead letters)
behind, and ignored `LLMQ_STORE_PASSWORD`, which the queue itself reads.
"""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest
import redis.asyncio

from llm_queue.engine.cli import amain
from llm_queue.registry.store import RegistryStore

pytestmark = pytest.mark.integration

HOST = "127.0.0.1"


def _env(prefix: str, doc: dict[str, Any], port: int = 6379, **extra: str) -> dict[str, str]:
    return {
        "LLMQ_STORE_URL": f"redis://{HOST}:{port}/0",
        "LLMQ_STORE_PASSWORD": "localdev",
        "LLMQ_ENVIRONMENT": "test",
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_REGISTRY": json.dumps(doc),
        **extra,
    }


async def test_password_variable_and_dry_run_by_default(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    env = _env(prefix, full, valkey.connection_pool.connection_kwargs["port"])
    # The URL has no password: the CLI reads LLMQ_STORE_PASSWORD as the queue does.
    plan = await amain(["registry", "publish"], env)
    assert (plan["action"], plan["dry_run"]) == ("published", True)
    assert (await amain(["registry", "publish", "--yes"], env))["action"] == "published"
    # Without --yes every change is a dry run (D3): nothing is written.
    for argv in (
        ["dlq", "purge", "--model", "gpt20b"],
        ["dlq", "redrive", "--model", "gpt20b"],
        ["models", "drain", "gpt20b"],
        ["purge", "removed"],
    ):
        assert (await amain(argv, env))["dry_run"] is True, argv
    assert (await amain(["dlq", "purge", "--model", "gpt20b"], env))["deleted"] == 0
    drain = await amain(["models", "drain", "gpt20b"], env)
    assert (drain["enabled"], drain["diff"][0]["after"]) == (False, False)
    got = await amain(["registry", "get", "--model", "gpt20b"], env)
    assert got["registry"].get("enabled") is None  # the dry run changed nothing
    with pytest.raises(SystemExit):  # `--dry-run` is gone: a dry run is the default
        await amain(["purge", "removed", "--dry-run"], env)


async def test_gc_removes_queues_of_removed_models(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    env = _env(prefix, full, valkey.connection_pool.connection_kwargs["port"])
    await amain(["registry", "publish", "--yes"], env)
    keep = [f"{prefix}:{{q:llm-gpt20b}}:s", f"{prefix}:{{q:jobs-report}}:s"]
    drop = [
        f"{prefix}:{{q:llm-embed-titan}}:dlq",  # queue.name of a removed model (from history)
        f"{prefix}:{{q:llm-embed-cohere-priority}}:s",
        f"{prefix}:bell:llm-embed-cohere",
    ]
    for key in keep + drop:  # real types: gc reads a queue's depth (0.1.0b3)
        if "bell:" in key:
            await valkey.rpush(key, b"1")
        else:
            await valkey.xadd(key, {"body": b"x"})
    v2 = copy.deepcopy(full)
    v2["registry_version"] = 2
    for alias in ("embed-titan", "embed-cohere"):
        del v2["models"][alias]
    await RegistryStore(valkey, prefix).publish(v2, actor="deploy")
    # The publish itself deletes the removed models' numbers keys (CO-20); gc does the rest.
    assert not [k async for k in valkey.scan_iter(match=f"{prefix}:{{m:embed-*")]
    # A queue still holding messages is kept unless --force (0.1.0b3 purge service).
    kept = await amain(["purge", "removed"], env)
    assert "llm-embed-cohere-priority" in [row["thing"] for row in kept["skipped"]]
    dry = await amain(["purge", "removed", "--force"], env)
    assert "llm-embed-titan" in dry["queues"]
    assert dry["deleted_keys"] >= len(drop)
    assert dry["dry_run"] is True
    assert all([await valkey.exists(key) for key in drop])
    done = await amain(["purge", "removed", "--yes", "--force"], env)
    assert done["deleted_keys"] == dry["deleted_keys"]
    assert [await valkey.exists(key) for key in drop] == [0, 0, 0]
    assert [await valkey.exists(key) for key in keep] == [1, 1]
    assert not [k async for k in valkey.scan_iter(match=f"{prefix}:{{m:embed-*")]


async def test_registry_rollback_plans_then_publishes(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    """Q11: publish / rollback are writes with a dry-run diff first."""
    env = _env(prefix, full, valkey.connection_pool.connection_kwargs["port"])
    await amain(["registry", "publish", "--yes"], env)
    await amain(["--actor", "ann", "registry", "set", "--model", "gpt20b", "rpm=7", "--yes"], env)
    v2 = copy.deepcopy(full)
    v2["registry_version"] = 2
    await RegistryStore(valkey, prefix).publish(v2, actor="deploy")
    versions = await amain(["registry", "versions"], env)
    assert versions["current"] == 2
    plan = await amain(["registry", "rollback", "1"], env)
    assert (plan["action"], plan["version"], plan["dry_run"]) == ("published", 3, True)
    assert (await amain(["registry", "versions"], env))["current"] == 2
    done = await amain(["--actor", "ann", "registry", "rollback", "1", "--yes"], env)
    assert (done["action"], done["version"]) == ("published", 3)
    entry = (await amain(["audit", "list", "--count", "1", "--op", "publish"], env))["entries"][0]
    assert entry["actor"].startswith("ann (rollback to 1)")
