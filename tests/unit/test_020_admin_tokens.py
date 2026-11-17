"""0.2.0 admin token length in strict mode (review B-21, owner Q12): at least 32 characters,
checked at mount on the resolved value (a token may be a callable), and per request after.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from llm_queue import ConfigError, LLMQueue

REGISTRY = '{"registry_version": 1, "models": {}}'
STRICT = {
    "LLMQ_STORE_URL": "rediss://store.example.com:6380/0",
    "LLMQ_ENVIRONMENT": "prod",
    "LLMQ_REGISTRY": REGISTRY,
    "LLMQ_ID_HASH_SALT": "s" * 16,
    "LLMQ_KEY_PREFIX": "app-prod",
}
DEV = {"LLMQ_STORE_URL": "redis://localhost:6379/0", "LLMQ_ENVIRONMENT": "dev",
       "LLMQ_REGISTRY": REGISTRY}  # fmt: skip
LONG = "t" * 32
SHORT = "s3cret-but-short"


@pytest.mark.parametrize(
    "options",
    [
        {"admin_token": SHORT},
        {"admin_read_token": SHORT},
        {"admin_token": lambda: SHORT},  # a callable is resolved to check it
        {"admin_token": LONG, "admin_read_token": lambda: SHORT},
    ],
)
def test_strict_mode_refuses_a_short_token_at_mount(options: dict[str, Any]) -> None:
    llmq = LLMQueue.from_env(STRICT)
    with pytest.raises(ConfigError, match="at least 32 characters") as info:
        llmq.http_router(**options)
    assert SHORT not in str(info.value)  # the name, never the value


def test_strict_mode_refuses_a_short_token_from_the_environment() -> None:
    llmq = LLMQueue.from_env({**STRICT, "LLMQ_ADMIN_TOKEN": SHORT})
    with pytest.raises(ConfigError, match="admin_token"):
        llmq.http_router()


def test_strict_mode_mounts_long_tokens_and_dev_mounts_short_ones() -> None:
    LLMQueue.from_env(STRICT).http_router(admin_token=LONG, admin_read_token=LONG + "r")
    LLMQueue.from_env(DEV).http_router(admin_token="x")


def test_a_token_rotated_to_a_short_one_is_refused_per_request() -> None:
    current = {"token": LONG}
    llmq = LLMQueue.from_env(STRICT)
    app = FastAPI()
    app.include_router(llmq.http_router(admin_token=lambda: current["token"]), prefix="/q")
    client = TestClient(app)
    assert client.get("/q/describe", headers={"X-LLMQ-Token": LONG}).status_code == 200
    current["token"] = SHORT
    assert client.get("/q/describe", headers={"X-LLMQ-Token": SHORT}).status_code == 403
