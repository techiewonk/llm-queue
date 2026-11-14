"""Public-API contracts that held before the 0.3 cut-over and were dropped with its old tests.

Audit G9: commit 5547ffd removed `tests/unit/test_contracts.py`; only
`test_public_exports_resolve` came back. These are the checks that still apply to this API:
no heavy imports or threads on import, one error root, and the request context's reach.
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import os
import subprocess
import sys
import threading

import llm_queue
from llm_queue import LLMQueueError, current_context, request_context


def test_import_has_no_heavy_side_effects() -> None:
    """Importing the package loads no store or AWS clients and starts no threads."""
    code = (
        "import sys, threading; import llm_queue; "
        "heavy = [m for m in ('aiobotocore', 'botocore', 'redis', 'aiohttp', 'httpx', 'openai') "
        "if m in sys.modules]; print(heavy, threading.active_count())"
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}  # this checkout's src
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True, env=env
    ).stdout.strip()
    assert out == "[] 1"


def test_every_exported_exception_has_one_root() -> None:
    """Callers catch `LLMQueueError` for everything the package raises."""
    exported = [getattr(llm_queue, name) for name in llm_queue.__all__]
    errors = [e for e in exported if inspect.isclass(e) and issubclass(e, BaseException)]
    assert errors
    assert [e.__name__ for e in errors if not issubclass(e, LLMQueueError)] == []


async def test_context_flows_into_tasks_and_nests() -> None:
    """The context reaches created tasks; a nested block overrides only what it sets."""
    seen: list[tuple[bool, str | None]] = []

    async def worker() -> None:
        ctx = current_context()
        seen.append((ctx.priority, ctx.session_id))

    with request_context(priority=True, session_id="s-1", user_id="u-1"):
        await asyncio.create_task(worker())
        with request_context(priority=False):
            await asyncio.create_task(worker())
    await asyncio.create_task(worker())
    assert seen == [(True, "s-1"), (False, "s-1"), (False, None)]


def test_context_reaches_threads_only_with_copy_context() -> None:
    """Raw threads lose the context unless started under `contextvars.copy_context().run`."""
    seen: dict[str, str | None] = {}

    def record(name: str) -> None:
        seen[name] = current_context().session_id

    with request_context(session_id="s-9"):
        plain = threading.Thread(target=record, args=("plain",))
        copied = threading.Thread(target=contextvars.copy_context().run, args=(record, "copied"))
        for thread in (plain, copied):
            thread.start()
            thread.join()
    assert seen == {"plain": None, "copied": "s-9"}
