"""0.2.0 secrets end to end: the store password only in a `file:` source (a Kubernetes-style
secret volume), never in the URL or the environment; a queue starts, logs in and serves.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
import redis.asyncio
from aiohttp.test_utils import TestServer
from engine_testkit import FakeEngine, Scenario, create_app, registry

from llm_queue import LLMQueue

pytestmark = pytest.mark.integration


async def test_store_password_from_a_file_source_logs_in(
    store: redis.asyncio.Redis,
    prefix: str,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    kwargs = store.connection_pool.connection_kwargs
    password, port = str(kwargs["password"]), int(kwargs["port"])
    volume = tmp_path / "llmq-secrets"
    volume.mkdir()
    (volume / "valkey_key").write_bytes(password.encode() + b"\n")  # as a volume mounts it
    fake = FakeEngine(Scenario.from_mapping({}))
    server = TestServer(create_app(engine=fake), host="127.0.0.1")
    await server.start_server()
    key_prefix = prefix.replace(":", "-")
    env = {
        "LLMQ_ENVIRONMENT": "test",
        "LLMQ_STORE_URL": f"redis://127.0.0.1:{port}/0",  # no password here
        "LLMQ_KEY_PREFIX": key_prefix,
        "LLMQ_REGISTRY": json.dumps(registry()),
        "LLMQ_BEDROCK_ENDPOINT_URL": str(server.make_url("")).rstrip("/"),
        "LLMQ_METRICS_SINK": "none",
        "LLMQ_SECRETS_SOURCE": f"file:{volume}",
        "LLMQ_SECRET_NAMES": json.dumps({"LLMQ_STORE_PASSWORD": "valkey_key"}),
    }
    llmq = LLMQueue.from_env(
        env,
        credentials=lambda account: {
            "aws_access_key_id": account.name,
            "aws_secret_access_key": "x",
        },
    )
    try:
        with caplog.at_level(logging.DEBUG, logger="llm_queue"):
            await llmq.start()
            vector = await llmq.embed_one("titan", "hello", input_type="search_query")
        assert vector
        assert llmq.describe()["secrets"]["sources"] == [f"file:{volume}"]
        assert password not in caplog.text
        assert password not in repr(llmq.describe())
    finally:
        await llmq.stop(drain_secs=1)
        await server.close()
        keys = [k async for k in store.scan_iter(match=f"{key_prefix}:*", count=500)]
        if keys:
            await store.delete(*keys)
