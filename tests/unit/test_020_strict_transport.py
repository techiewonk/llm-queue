"""0.2.0 strict mode refuses the botocore transport with an endpoint override (review B-29):
botocore follows redirects, re-sending the signed request and session token to another host.
"""

from __future__ import annotations

import pytest

from llm_queue import ConfigError, LLMQueue
from llm_queue.tuning import BedrockOptions, Tuning

REGISTRY = '{"registry_version": 1, "models": {}}'
STRICT = {
    "LLMQ_STORE_URL": "rediss://store.example.com:6380/0",
    "LLMQ_ENVIRONMENT": "prod",
    "LLMQ_REGISTRY": REGISTRY,
    "LLMQ_ID_HASH_SALT": "s" * 16,
    "LLMQ_KEY_PREFIX": "app-prod",
}
ENDPOINT = {"LLMQ_BEDROCK_ENDPOINT_URL": "https://bedrock.internal.example.com"}
BOTOCORE = Tuning(bedrock=BedrockOptions(transport="botocore"))


@pytest.mark.parametrize("local", [False, True])
def test_strict_refuses_botocore_with_an_endpoint_override(local: bool) -> None:
    env = {
        **STRICT,
        **ENDPOINT,
        "LLMQ_CONFIG_ERROR_MODE": "raise",
        **({"LLMQ_EXECUTION_MODE": "local"} if local else {}),
    }
    with pytest.raises(ConfigError, match="transport=botocore is refused outside dev"):
        LLMQueue.from_env(env, tuning=BOTOCORE)


def test_aiohttp_no_override_or_dev_are_allowed() -> None:
    LLMQueue.from_env({**STRICT, **ENDPOINT})  # the default transport, aiohttp
    LLMQueue.from_env(STRICT, tuning=BOTOCORE)  # AWS's own endpoints
    dev = {
        "LLMQ_STORE_URL": "redis://localhost:6379/0",
        "LLMQ_ENVIRONMENT": "dev",
        "LLMQ_REGISTRY": REGISTRY,
        "LLMQ_BEDROCK_ENDPOINT_URL": "http://127.0.0.1:8080",
    }
    LLMQueue.from_env(dev, tuning=BOTOCORE)  # a local fake
