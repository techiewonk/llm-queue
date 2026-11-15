"""Clients and providers per account and region (plan step A3, E1-E7).

`ClientHub` holds the process's SDK clients, shared by every `ClientPool` on the same loop;
`ClientPool` hands them out per (account, region); `Providers` runs one attempt of an operation
on a `Route` with the attempt timers.
"""

from __future__ import annotations

from llm_queue.clients.hub import (
    ClientHub,
    client_hub,
    pool_size_for,
    run,
)
from llm_queue.clients.pool import (
    ClientPool,
    CredentialsRefreshSlow,
    CredentialsUnavailable,
    Route,
    route_for,
)
from llm_queue.clients.providers import (
    BedrockProvider,
    OpenAIProvider,
    Provider,
    Providers,
    Result,
    Usage,
    decode_invoke_body,
    register_provider,
    split_params,
)
from llm_queue.clients.timing import AttemptTimeout, Timing
from llm_queue.tuning import OpenAIOptions, WarmOptions

__all__ = [
    "AttemptTimeout",
    "BedrockProvider",
    "ClientHub",
    "ClientPool",
    "CredentialsRefreshSlow",
    "CredentialsUnavailable",
    "OpenAIOptions",
    "OpenAIProvider",
    "Provider",
    "Providers",
    "Result",
    "Route",
    "Timing",
    "Usage",
    "WarmOptions",
    "client_hub",
    "decode_invoke_body",
    "pool_size_for",
    "register_provider",
    "route_for",
    "run",
    "split_params",
]
