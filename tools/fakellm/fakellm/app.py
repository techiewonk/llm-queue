"""Application factory wiring the Bedrock, OpenAI and admin routes around one `Engine`."""

from __future__ import annotations

import uuid
from collections import deque

from aiohttp import web

from fakellm import admin, bedrock, openai_api
from fakellm.behaviour import Engine
from fakellm.scenario import Scenario
from fakellm.state import ENGINE, REQUEST_IDS

# Embedding batches (Cohere 96 texts, OpenAI chunks) easily exceed aiohttp's 1 MiB default.
_MAX_BODY_BYTES = 32 * 1024 * 1024


async def _request_id(request: web.Request, response: web.StreamResponse) -> None:
    """Give every response a request id, as Bedrock (`x-amzn-RequestId`) and OpenAI do.

    One id per response under both names, kept in `REQUEST_IDS`, so a test can match the id a
    client reports with the one fakellm sent. A handler that set its own keeps it.
    """
    rid = response.headers.get("x-amzn-RequestId") or str(uuid.uuid4())
    response.headers.setdefault("x-amzn-RequestId", rid)
    response.headers.setdefault("x-request-id", rid)
    request.app[REQUEST_IDS].append(rid)


def create_app(
    scenario: Scenario | None = None, *, engine: Engine | None = None
) -> web.Application:
    """Build the fakellm aiohttp app from a scenario (or a prepared engine for tests)."""
    # A client that hangs up cancels its handler (aiohttp's default leaves it running), so a
    # call aborted mid-latency never counts as `ok`: `python -m fakellm` behaves like TestServer.
    app = web.Application(
        client_max_size=_MAX_BODY_BYTES, handler_args={"handler_cancellation": True}
    )
    app[ENGINE] = engine or Engine(scenario or Scenario())
    app[REQUEST_IDS] = deque(maxlen=1000)
    app.on_response_prepare.append(_request_id)
    bedrock.add_routes(app)
    openai_api.add_routes(app)
    admin.add_routes(app)
    return app
