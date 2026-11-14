"""Serve the app and keep it listening: re-open the listening socket if asyncio closed it.

On Windows a failed accept (WinError 64: a client gave up mid-accept, e.g. a worker killed
while connecting) makes asyncio close the listening socket for good. fakellm then refused
every new connection, and scenario L4 at 100 % crashed on its next admin request (HX-1).
The drills' store proxy has the same guard (`scripts/gates_fleet.py`).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
from collections.abc import Awaitable, Callable
from typing import BinaryIO

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


def watch_eof(stream: BinaryIO) -> asyncio.Event:
    """An event set once `stream` ends: the parent closed the pipe, or died (CO-15).

    Read in a daemon thread (not `to_thread`): `asyncio.run` would wait for a pool thread
    still blocked on the read at exit.
    """
    loop = asyncio.get_running_loop()
    gone = asyncio.Event()

    def read() -> None:
        with contextlib.suppress(OSError, ValueError):
            while stream.read(1024):
                pass
        with contextlib.suppress(RuntimeError):  # the loop already closed
            loop.call_soon_threadsafe(gone.set)

    threading.Thread(target=read, name="fakellm-parent-watch", daemon=True).start()
    return gone


async def serve(
    app: web.Application, host: str, port: int, *, parent: BinaryIO | None = None
) -> None:
    """Serve `app` on host:port until cancelled (what `web.run_app` did, plus the guard).

    With `parent` (fakellm's stdin, a pipe from the process that started it), serving also
    ends when that pipe closes: a drill killed hard no longer leaves fakellm on its port.
    """
    runner = web.AppRunner(app)
    await runner.setup()
    handler = runner.server
    assert handler is not None  # set by setup()
    loop = asyncio.get_running_loop()
    listening = keep_listening(lambda: loop.create_server(handler, host, port))
    try:
        if parent is None:
            await listening
            return
        gone = watch_eof(parent)
        async with asyncio.TaskGroup() as group:
            listener = group.create_task(listening)
            await gone.wait()
            logger.info("fakellm: stdin closed (the parent is gone); stopping")
            listener.cancel()
    finally:
        await runner.cleanup()
