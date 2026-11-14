"""Attempt timers: time to the first response, then an idle limit between chunks (plan E117).

A healthy long answer keeps sending chunks and is never cut; a region that accepts the
connection and never answers is given up after `first_response_secs`. Raised errors are
`AttemptTimeout`, a `TimeoutError`, so they classify as retryable.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass, replace
from typing import TypeVar

_T = TypeVar("_T")  # PEP 695 syntax needs 3.12; the package supports 3.11


class AttemptTimeout(TimeoutError):
    """An attempt got no first response, or went idle mid-answer, for too long."""


@dataclass(frozen=True, slots=True)
class Timing:
    """Limits of one attempt: until the first response, between later chunks, and in total.

    `attempt_secs` bounds a non-streamed call (plain Converse), whose first response is the
    whole answer; while it is None, Bedrock chat streams underneath (first-response timer).
    """

    first_response_secs: float = 60.0
    idle_secs: float = 30.0
    attempt_secs: float | None = None


async def first(awaitable: Awaitable[_T], secs: float, what: str) -> _T:
    """Await within `secs`, else raise `AttemptTimeout` naming `what`."""
    try:
        return await asyncio.wait_for(awaitable, secs)
    except TimeoutError as exc:
        raise AttemptTimeout(f"no {what} within {secs:g}s") from exc


def opened(timing: Timing, started: float) -> Timing:
    """`timing` with what is left of the first-response budget since `started` (monotonic).

    One budget covers opening the call and its first event (K7): a slow open no longer gets
    `first_response_secs` once more for the first event.
    """
    left = timing.first_response_secs - (time.monotonic() - started)
    return replace(timing, first_response_secs=max(0.0, left))


async def paced(stream: AsyncIterator[_T], timing: Timing, *, started: bool) -> AsyncIterator[_T]:
    """Yield a stream's items, each within the idle limit.

    The first item gets the first-response limit unless `started` (the response already came).
    """
    iterator = stream.__aiter__()
    limit = timing.idle_secs if started else timing.first_response_secs
    what = "chunk" if started else "first response"
    while True:
        try:
            item = await first(iterator.__anext__(), limit, what)
        except StopAsyncIteration:
            return
        yield item
        limit, what = timing.idle_secs, "chunk"
