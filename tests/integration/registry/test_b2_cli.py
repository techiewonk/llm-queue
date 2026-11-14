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
from llm_queue.errors import ConfigError
from llm_queue.registry.store import RegistryStore

pytestmark = pytest.mark.integration

HOST = "127.0.0.1"


def _env(prefix: str, doc: dict[str, Any], **extra: str) -> dict[str, str]:
    return {
        "LLMQ_STORE_URL": f"redis://{HOST}:6379/0",
        "LLMQ_STORE_PASSWORD": "localdev",
        "LLMQ_ENVIRONMENT": "test",
        "LLMQ_KEY_PREFIX": prefix,
        "LLMQ_REGISTRY": json.dumps(doc),
        **extra,
    }


async def test_password_variable_and_confirmation(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    env = _env(prefix, full)
    # The URL has no password: the CLI reads LLMQ_STORE_PASSWORD as the queue does.
    assert (await amain(["registry", "publish"], env))["action"] == "published"
    for argv in (
        ["dlq", "purge", "--model", "gpt20b"],
        ["dlq", "redrive", "--model", "gpt20b"],
        ["drain", "gpt20b"],
        ["registry", "gc"],
    ):
        with pytest.raises(ConfigError, match="--yes"):
            await amain(argv, env)
    dry = await amain(["dlq", "purge", "--model", "gpt20b", "--dry-run"], env)
    assert dry["would_purge"] == 0
    drain = await amain(["drain", "gpt20b", "--dry-run"], env)
    assert drain["would_disable"] is True
    assert (await amain(["registry", "get", "--model", "gpt20b"], env)).get("enabled") is None
    with pytest.raises(SystemExit):  # argparse: both flags at once
        await amain(["registry", "gc", "--yes", "--dry-run"], env)


async def test_gc_removes_queues_of_removed_models(
    valkey: redis.asyncio.Redis, prefix: str, full: dict[str, Any]
) -> None:
    env = _env(prefix, full)
    await amain(["registry", "publish"], env)
    keep = [f"{prefix}:{{q:llm-gpt20b}}:s", f"{prefix}:{{q:jobs-report}}:s"]
    drop = [
        f"{prefix}:{{q:llm-embed-titan}}:dlq",  # queue.name of a removed model (from history)
        f"{prefix}:{{q:llm-embed-cohere-priority}}:s",
        f"{prefix}:bell:llm-embed-cohere",
    ]
    for key in keep + drop:
        await valkey.set(key, b"x")
    v2 = copy.deepcopy(full)
    v2["registry_version"] = 2
    for alias in ("embed-titan", "embed-cohere"):
        del v2["models"][alias]
    await RegistryStore(valkey, prefix).publish(v2, actor="deploy")
    dry = await amain(["registry", "gc", "--dry-run"], env)
    assert "llm-embed-titan" in dry["queues"]
    assert dry["would_delete"] >= len(drop) + 2  # plus the two models' numbers keys
    assert all([await valkey.exists(key) for key in drop])
    done = await amain(["registry", "gc", "--yes"], env)
    assert done["deleted_keys"] == dry["would_delete"]
    assert [await valkey.exists(key) for key in drop] == [0, 0, 0]
    assert [await valkey.exists(key) for key in keep] == [1, 1]
    assert not [k async for k in valkey.scan_iter(match=f"{prefix}:{{m:embed-*")]
