"""Typed aiohttp app keys: the shared `Engine`, and the request ids fakellm last sent."""

from __future__ import annotations

from collections import deque

from aiohttp import web

from fakellm.behaviour import Engine

ENGINE = web.AppKey("fakellm_engine", Engine)
# The request ids of the latest responses, newest last (bounded: tests read the recent ones).
REQUEST_IDS = web.AppKey("fakellm_request_ids", deque[str])
