"""One admin builder from `Layers`, with `from_env`'s full signature (0.2.0 review B-23).

`AdminServices`, `RequestsAdmin` and `PurgeService` each wired the stores and the registry from
`LLMQ_*` on their own and took neither `defaults=` nor `secret_names=`: the CLI and admin
workers could not reproduce an app that configures through `defaults=`. All three now build
on `AdminContext.from_layers` and take `LLMQueue.from_env`'s arguments.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from llm_queue.engine.admin import AdminServices
from llm_queue.engine.purge import PurgeService
from llm_queue.engine.requests_admin import RequestsAdmin
from llm_queue.ids import keyed_hash

DEV = {"LLMQ_ENVIRONMENT": "dev", "LLMQ_STORE_URL": "redis://localhost:6379/0"}
SECRETS = {"app_prefix": "from-a-secret", "app_salt": "s" * 16}.get


def _prefix(service: Any) -> str:
    if isinstance(service, AdminServices):
        return service.context.keys.prefix
    if isinstance(service, RequestsAdmin):
        return service.keys.prefix
    return str(service.keys.prefix)


BUILDERS: dict[str, Callable[..., Any]] = {
    "AdminServices": AdminServices.from_env,
    "RequestsAdmin": RequestsAdmin.from_env,
    "PurgeService": PurgeService.from_env,
}


@pytest.mark.parametrize("name", sorted(BUILDERS))
async def test_builders_take_defaults_and_secret_names(name: str) -> None:
    built = BUILDERS[name](DEV, defaults={"LLMQ_KEY_PREFIX": "from-defaults"})
    try:
        assert _prefix(built) == "from-defaults"
    finally:
        await built.aclose()
    names = {"LLMQ_KEY_PREFIX": "app_prefix", "LLMQ_ID_HASH_SALT": "app_salt"}
    built = BUILDERS[name](DEV, secrets=SECRETS, secret_names=names)
    try:
        assert _prefix(built) == "from-a-secret"
        if isinstance(built, RequestsAdmin):
            assert built.hash_id("acme") == keyed_hash("s" * 16, "acme")
    finally:
        await built.aclose()
