"""fakellm keeps listening when asyncio closes its listening socket (HX-1).

On Windows a failed accept closed fakellm's listening socket for good; the drill's next admin
request was refused and scenario L4 at 100 % crashed. `keep_listening` opens it again.
"""

from __future__ import annotations

import asyncio
import os
import socket

import fakellm_testkit  # noqa: F401  # puts tools/fakellm on sys.path
from aiohttp import web
from fakellm.serve import keep_listening, serve


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _answers(port: int) -> bool:
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), 2)
    except OSError:
        return False
    writer.write(b"ping")
    data = await asyncio.wait_for(reader.read(4), 2)
    writer.close()
    return data == b"ping"


async def test_listens_again_after_the_socket_is_closed() -> None:
    """The bug: once asyncio closed the listener, every new connection was refused for good."""
    port = _free_port()
    servers: list[asyncio.Server] = []

    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(await reader.read(4))
        await writer.drain()
        writer.close()

    async def listen() -> asyncio.Server:
        server = await asyncio.start_server(echo, "127.0.0.1", port)
        servers.append(server)
        return server

    task = asyncio.create_task(keep_listening(listen, check_secs=0.02))
    try:
        for _ in range(100):  # until it listens
            if servers:
                break
            await asyncio.sleep(0.01)
        assert await _answers(port)
        servers[0].close()  # what the failed accept did to the listener
        for _ in range(100):
            if len(servers) > 1:
                break
            await asyncio.sleep(0.02)
        assert len(servers) == 2, "the listener was not opened again"
        assert await _answers(port)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert not servers[-1].is_serving()  # the last one is closed on cancel


async def test_serving_ends_when_the_parent_pipe_closes() -> None:
    """CO-15: fakellm outlived a drill killed hard and kept its port (no parent watch).

    With `parent` (its stdin pipe in a drill), serving ends once the pipe closes, which is
    what the OS does to a dead parent's end; the port is free again.
    """
    port = _free_port()
    app = web.Application()
    app.router.add_get("/ping", _pong)
    read_fd, write_fd = os.pipe()
    parent = os.fdopen(read_fd, "rb")
    task = asyncio.create_task(serve(app, "127.0.0.1", port, parent=parent))
    try:
        for _ in range(200):  # until it listens
            if await _accepts(port):
                break
            await asyncio.sleep(0.01)
        assert await _accepts(port)
        os.close(write_fd)  # the parent is gone
        await asyncio.wait_for(task, 5)
        assert not await _accepts(port)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        parent.close()


async def _pong(request: web.Request) -> web.Response:
    return web.Response(text="pong")


async def _accepts(port: int) -> bool:
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), 2)
    except OSError:
        return False
    writer.close()
    return True
