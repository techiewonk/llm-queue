"""Structured loggers that always go through stdlib `logging` (no global structlog config).

Unconfigured structlog prints to stdout and ignores the app's levels and handlers, so every
module logs via `get_logger(__name__)`: structlog's key-value API on top of a stdlib logger.
Apps keep full control (levels, handlers, loguru's InterceptHandler, structlog's
ProcessorFormatter); the fields ride on `record.llmq` as a dict.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable, Mapping, MutableMapping
from typing import Any

import structlog

_PASSTHROUGH = ("exc_info", "stack_info")
# C0 and C1 control characters. One compiled search instead of a Python loop over every
# character (R3PF-5, measured 2026-11-15, Python 3.12): 13 fields of `llmq_call_finished`
# 20.2 -> 5.6 us, a 2,000-character error 362 -> 19 us; same output.
_CONTROL = re.compile("[\\x00-\\x1f\\x7f-\\x9f]")


def _to_stdlib(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> tuple[tuple[str], dict[str, Any]]:
    # Message is readable on its own ("event k=v ..."); the structured copy lives on one
    # `llmq` attribute so field names can never clash with LogRecord's reserved attributes.
    event = str(event_dict.pop("event", ""))
    kwargs: dict[str, Any] = {k: event_dict.pop(k) for k in _PASSTHROUGH if k in event_dict}
    fields: Mapping[str, Any] = dict(event_dict)
    message = " ".join([event, *(f"{k}={_value(v)}" for k, v in fields.items())])
    kwargs["extra"] = {"llmq": {"event": event, **fields}}
    kwargs["stacklevel"] = 4  # point %(funcName)s/%(lineno)d at the library call site
    return (message,), kwargs


def _value(value: Any) -> str:
    """A field value for the message line; `repr()` when it holds a control character.

    `error=str(exc)` with a newline in it forged a second log line (CO-22), the risk
    `check_actor` guards against for the audit actor.
    """
    text = str(value)
    return repr(text) if _CONTROL.search(text) else text


class LogLimiter:
    """At most one line per event and key every `every_secs`, the rest counted (R3OB-7).

    In a store outage every model loop, heartbeat step and held call logged its own failure:
    about 1,000 lines a second on 50 pods, burying the one line that mattered. The first line
    of a key goes out at once; later ones within the interval are counted, and the next line
    carries `suppressed=<n>`. Keys must be bounded (model, step), never a request id.
    """

    MAX_KEYS = 4096  # a wrong key (a request id) cannot grow this without bound

    def __init__(
        self, every_secs: float = 10.0, clock: Callable[[], float] = time.monotonic
    ) -> None:
        """`every_secs` between lines of one key; `clock` for tests."""
        self.every_secs = every_secs
        self._clock = clock
        self._seen: dict[tuple[str, str], list[float]] = {}  # key -> [last line at, suppressed]

    def __call__(self, log: Callable[..., Any], event: str, key: str = "", **fields: Any) -> bool:
        """Log `event` through `log` unless this key logged within the interval; True if logged."""
        now = self._clock()
        slot = self._seen.get((event, key))
        if slot is not None and now - slot[0] < self.every_secs:
            slot[1] += 1
            return False
        if slot is None and len(self._seen) >= self.MAX_KEYS:
            self._seen.clear()
        suppressed = int(slot[1]) if slot is not None else 0
        self._seen[(event, key)] = [now, 0]
        if suppressed:
            fields["suppressed"] = suppressed
        log(event, **fields)
        return True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Key-value logger bound to `logging.getLogger(name)`; used by every library module."""
    return structlog.wrap_logger(  # type: ignore[no-any-return]
        logging.getLogger(name),
        # The level check first: a disabled call built its whole message before the stdlib
        # logger dropped it (CO-22).
        processors=[structlog.stdlib.filter_by_level, _to_stdlib],
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
