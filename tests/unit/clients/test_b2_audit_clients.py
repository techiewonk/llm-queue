"""Fixes from the clients audit (docs/internal/audit/audit-clients.md, CL-*), pinned per bug.

Each test names its finding: redirects, credentials, client retirement, quick retries, stream
release and the parity of the two Bedrock transports. Runs fakellm or small aiohttp test
servers in process; no network, no real model.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from clients_testkit import registry_doc

from llm_queue.clients import ClientPool, Providers, Route, Timing, WarmOptions, route_for
from llm_queue.errors import ErrorKind
from llm_queue.registry import ResolvedRegistry, load_registry
from llm_queue.tuning import BedrockOptions

CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
FAST = Timing(first_response_secs=5, idle_secs=5)
PLAIN = Timing(5, 5, attempt_secs=5)
NO_WARM = WarmOptions(connections=0)


def _keys(account: Any) -> dict[str, Any]:
    return {"aws_access_key_id": account.name, "aws_secret_access_key": "x"}


def _route(view: ResolvedRegistry, model: str = "gpt120b") -> Route:
    return route_for(view, next(d for d in view.deployments(model) if d.account == "aws-a"))


async def _server(app: web.Application) -> TestServer:
    server = TestServer(app, host="127.0.0.1")
    await server.start_server()
    return server


@pytest.fixture
async def servers() -> AsyncIterator[Any]:
    """`start(app)` -> base URL of a test server; every server is closed at teardown."""
    started: list[TestServer] = []

    async def start(app: web.Application) -> str:
        server = await _server(app)
        started.append(server)
        return str(server.make_url("")).rstrip("/")

    yield start
    for server in started:
        await server.close()


def _providers(url: str, transport: str, **kw: Any) -> Providers:
    kw.setdefault("credentials", _keys)
    kw.setdefault("warm", NO_WARM)
    return Providers(
        ClientPool(bedrock_endpoint_url=url, options=BedrockOptions(transport=transport), **kw)
    )


_BOTOCORE_FOLLOWS = pytest.mark.xfail(
    reason="aiobotocore sends with aiohttp's allow_redirects=True and has no setting for it",
    strict=True,
)


@pytest.mark.parametrize("status", [301, 307, 308])
@pytest.mark.parametrize(
    "transport", ["aiohttp", pytest.param("botocore", marks=_BOTOCORE_FOLLOWS)]
)
async def test_a_redirect_is_never_followed(servers: Any, status: int, transport: str) -> None:
    """CL-4: aiohttp followed a 3xx and re-sent the signed body and session token elsewhere.

    The aiohttp transport now sends with `allow_redirects=False`: a 3xx is a FATAL error
    (code = the status) and the other host gets nothing. The botocore transport still follows
    (aiobotocore's default), pinned by the strict xfail.
    """
    seen: list[str] = []

    async def elsewhere(request: web.Request) -> web.Response:
        seen.append(request.path)
        return web.json_response({})

    other = web.Application()
    other.router.add_route("*", "/{tail:.*}", elsewhere)
    target = await servers(other)

    async def moved(request: web.Request) -> web.Response:
        return web.Response(status=status, headers={"Location": f"{target}{request.path}"})

    app = web.Application()
    app.router.add_post("/model/{model_id:.+}/{action}", moved)
    url = await servers(app)
    view = load_registry(registry_doc(url))
    providers = _providers(url, transport)
    try:
        bedrock = providers.get("bedrock")
        with pytest.raises(Exception) as info:  # noqa: PT011  # any error: classified below
            await bedrock.call("converse", CHAT, _route(view), PLAIN)
        found = bedrock.classify(info.value)
    finally:
        await providers.aclose()
    assert not seen  # the other host never got the signed request
    assert (found.kind, found.code) == (ErrorKind.FATAL, str(status))
