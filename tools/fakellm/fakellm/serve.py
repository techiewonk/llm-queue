"""Serve the app and keep it listening: re-open the listening socket if asyncio closed it.

On Windows a failed accept (WinError 64: a client gave up mid-accept, e.g. a worker killed
while connecting) makes asyncio close the listening socket for good. fakellm then refused
every new connection, and scenario L4 at 100 % crashed on its next admin request (HX-1).
The drills' store proxy has the same guard (`scripts/gates_fleet.py`).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from aiohttp import web

logger = logging.getLogger("fakellm")


def closed(server: asyncio.Server) -> bool:
    """True when the server no longer accepts: closed, or every socket gone (fileno -1)."""
    return not server.is_serving() or all(s.fileno() == -1 for s in server.sockets)


async def keep_listening(
    listen: Callable[[], Awaitable[asyncio.Server]], check_secs: float = 0.2
) -> None:
    """Listen with `listen()`, and listen again whenever the server stops accepting.

    Runs until cancelled; the last server is closed on the way out.
    """
    server = await listen()
    try:
        while True:
            await asyncio.sleep(check_secs)
            if closed(server):
                logger.warning("fakellm: listening socket closed by asyncio; listening again")
                server.close()
                server = await listen()
    finally:
        server.close()


async def serve(app: web.Application, host: str, port: int) -> None:
    """Serve `app` on host:port until cancelled (what `web.run_app` did, plus the guard)."""
    runner = web.AppRunner(app)
    await runner.setup()
    handler = runner.server
    assert handler is not None  # set by setup()
    loop = asyncio.get_running_loop()
    try:
        await keep_listening(lambda: loop.create_server(handler, host, port))
    finally:
        await runner.cleanup()
