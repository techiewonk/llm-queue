"""0.2.0 FU1 beside a real store: `LLMQ_ENABLED=false` never touches it.

Every queue setting points at the test store, yet a switched-off queue started under
`install()` writes no key and serves its probes; the factories return None.
"""

from __future__ import annotations

import json

import pytest
import redis.asyncio
from engine_testkit import registry as registry_doc
from fastapi import FastAPI
from fastapi.testclient import TestClient

from llm_queue import LLMQueue
from llm_queue.fastapi import install
from llm_queue.langchain import chat_model

pytestmark = pytest.mark.integration


async def test_disabled_queue_leaves_the_store_alone(
    store: redis.asyncio.Redis, prefix: str
) -> None:
    port = store.connection_pool.connection_kwargs["port"]
    key_prefix = prefix.replace(":", "-")
    llmq = LLMQueue.from_env(
        {
            "LLMQ_ENABLED": "false",
            "LLMQ_STORE_URL": f"redis://:localdev@127.0.0.1:{port}/0",
            "LLMQ_ENVIRONMENT": "test",
            "LLMQ_KEY_PREFIX": key_prefix,
            "LLMQ_REGISTRY": json.dumps(registry_doc()),
        }
    )
    assert chat_model("small", registry=llmq) is None
    app = FastAPI()
    install(app, llmq)
    with TestClient(app) as client:
        assert client.get("/internal/llm-queue/health/ready").json()["enabled"] is False
    assert [k async for k in store.scan_iter(match=f"{key_prefix}*", count=500)] == []
