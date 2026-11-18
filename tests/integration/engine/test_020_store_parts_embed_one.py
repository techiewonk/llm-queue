"""0.2.0 D3, D7, E1, H8 end to end on a real store and fakellm: a queue built from store parts
or with the password only in the environment, `embed_one` (async and sync) and a factory-made
chat model.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
import redis.asyncio
from aiohttp.test_utils import TestServer
from engine_testkit import FakeEngine, Scenario, create_app, registry

from llm_queue import LLMQueue
from llm_queue.engine.settings import STORE_POOL_MARGIN
from llm_queue.langchain import QueuedChatBedrockConverse, chat_model

pytestmark = pytest.mark.integration


@pytest.fixture
async def build(
    store: redis.asyncio.Redis, prefix: str
) -> AsyncIterator[Callable[[dict[str, str]], Any]]:
    """Start an `LLMQueue.from_env` on fakellm with the store given by `store_env`."""
    fake = FakeEngine(Scenario.from_mapping({}))
    server = TestServer(create_app(engine=fake), host="127.0.0.1")
    await server.start_server()
    url = str(server.make_url("")).rstrip("/")
    key_prefix = prefix.replace(":", "-")
    queues: list[LLMQueue] = []

    async def make(store_env: dict[str, str]) -> LLMQueue:
        env = {
            "LLMQ_ENVIRONMENT": "test",
            "LLMQ_KEY_PREFIX": key_prefix,
            "LLMQ_REGISTRY": json.dumps(registry()),
            "LLMQ_BEDROCK_ENDPOINT_URL": url,
            "LLMQ_METRICS_SINK_MODE": "none",
            **store_env,
        }
        queue = LLMQueue.from_env(
            env,  # no `secrets=`: the password comes from the environment only
            credentials=lambda account: {
                "aws_access_key_id": account.name,
                "aws_secret_access_key": "x",
            },
        )
        await queue.start()
        queues.append(queue)
        return queue

    yield make
    for queue in queues:
        await queue.stop(drain_secs=1)
    await server.close()
    keys = [k async for k in store.scan_iter(match=f"{key_prefix}:*", count=500)]
    if keys:
        await store.delete(*keys)


def _password(store: redis.asyncio.Redis) -> str:
    return str(store.connection_pool.connection_kwargs["password"])


def _port(store: redis.asyncio.Redis) -> int:
    return int(store.connection_pool.connection_kwargs["port"])


async def test_password_only_in_the_environment_and_embed_one(
    build: Callable[[dict[str, str]], Any], store: redis.asyncio.Redis
) -> None:
    """0.1.1 ignored LLMQ_STORE_PASSWORD without `secrets=`: the start failed to log in."""
    llmq = await build(
        {
            "LLMQ_STORE_URL": f"redis://127.0.0.1:{_port(store)}/0",
            "LLMQ_STORE_PASSWORD": _password(store),
            "LLMQ_STORE_MAX_CONNECTIONS": "auto",
        }
    )
    assert llmq.settings is not None
    # registry(): four models at the default 20 worker slots each.
    assert llmq.settings.store_max_connections == 4 * 20 + STORE_POOL_MARGIN
    vector = await llmq.embed_one("titan", "hello", input_type="search_query")
    assert isinstance(vector, list)
    assert vector
    assert all(isinstance(x, float) for x in vector)
    same = await asyncio.to_thread(llmq.embed_one_sync, "titan", "hello")
    assert len(same) == len(vector)


async def test_store_from_parts_and_a_factory_model(
    build: Callable[[dict[str, str]], Any], store: redis.asyncio.Redis
) -> None:
    llmq = await build(
        {
            "LLMQ_STORE_HOST": "127.0.0.1",
            "LLMQ_STORE_PORT": str(_port(store)),
            "LLMQ_STORE_DB": "0",
            "LLMQ_STORE_TLS_ENABLED": "false",
            "LLMQ_STORE_PASSWORD": _password(store),
        }
    )
    assert (await llmq.health())["ready"]
    chat = chat_model("big", registry=llmq, max_tokens=20)
    assert isinstance(chat, QueuedChatBedrockConverse)
    message = await chat.ainvoke("hi")
    assert str(message.content).startswith("fakellm")
    assert chat_model("not-in-registry", registry=llmq) is None
