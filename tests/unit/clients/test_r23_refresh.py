"""A slow credentials refresh cut by the attempt timer (FC-2, owner decision).

The account is still backed off, but its client and the shared refresh are kept, one refresh
takes one back-off step however many calls it cut, and a cut while the keys are still valid
(the advisory window) is a plain retryable timeout.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from clients_testkit import TRANSPORTS, StartFake, registry_doc

from llm_queue.clients import (
    AttemptTimeout,
    ClientPool,
    CredentialsRefreshSlow,
    Providers,
    Timing,
    WarmOptions,
    route_for,
)
from llm_queue.registry import load_registry
from llm_queue.tuning import BedrockOptions

CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
TIGHT = Timing(0.3, 0.3, attempt_secs=0.3)


def _credentials(first_expiry_secs: float, asked: list[int]) -> Any:
    def credentials(account: Any) -> dict[str, Any]:
        asked.append(1)
        keys = {"aws_access_key_id": account.name, "aws_secret_access_key": "x"}
        if len(asked) == 1:  # the build
            soon = datetime.now(UTC) + timedelta(seconds=first_expiry_secs)
            return {**keys, "aws_session_token": "t", "expiry_time": soon}
        time.sleep(0.8)  # a slow refresh (runs in a thread)
        return {
            **keys,
            "aws_session_token": "t",
            "expiry_time": datetime.now(UTC) + timedelta(hours=1),
        }

    return credentials


async def _setup(start_fake: StartFake, transport: str, expiry_secs: float) -> Any:
    url, _ = await start_fake()
    view = load_registry(registry_doc(url))
    route = route_for(view, next(d for d in view.deployments("gpt120b") if d.account == "aws-a"))
    asked: list[int] = []
    pool = ClientPool(
        bedrock_endpoint_url=url,
        credentials=_credentials(expiry_secs, asked),
        options=BedrockOptions(transport=transport),
        warm=WarmOptions(connections=0),
    )
    providers = Providers(pool)
    await pool.bedrock(route)  # built outside the tight timer
    return providers, route, asked


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_one_refresh_takes_one_back_off_step(start_fake: StartFake, transport: str) -> None:
    """FC-2: every call cut by one slow refresh doubled the back-off (5 s -> 60 s at once).

    Each cut call is `CredentialsRefreshSlow`; exactly one of them is marked `first`, and the
    refresh lands for the next call without a second lookup.
    """
    providers, route, asked = await _setup(start_fake, transport, 60)
    bedrock = providers.get("bedrock")
    try:
        results = await asyncio.gather(
            *(bedrock.call("converse", CHAT, route, TIGHT) for _ in range(5)),
            return_exceptions=True,
        )
        slow = [r for r in results if isinstance(r, CredentialsRefreshSlow)]
        assert slow, results
        assert sum(r.first for r in slow) == 1
        for _ in range(30):  # the shared refresh lands; the client was never dropped
            try:
                await bedrock.call("converse", CHAT, route, TIGHT)
                break
            except CredentialsRefreshSlow:
                await asyncio.sleep(0.1)
        else:
            pytest.fail("the refresh never landed")
        assert len(asked) == 2  # one build, one refresh
    finally:
        await providers.aclose()


@pytest.mark.parametrize("transport", TRANSPORTS)
async def test_a_cut_in_the_advisory_window_is_a_timeout(
    start_fake: StartFake, transport: str
) -> None:
    """FC-2: keys good for 4 more minutes were reported as `CredentialsUnavailable`.

    The worker then dropped a healthy client and backed the account off. It is a plain
    `AttemptTimeout` (retryable) now, and the client stays in the pool.
    """
    providers, route, _ = await _setup(start_fake, transport, 240)
    bedrock = providers.get("bedrock")
    try:
        with pytest.raises(AttemptTimeout) as info:
            await bedrock.call("converse", CHAT, route, TIGHT)
        assert not isinstance(info.value, CredentialsRefreshSlow)
        assert providers.pool._bedrock  # the client was not dropped
    finally:
        await providers.aclose()
