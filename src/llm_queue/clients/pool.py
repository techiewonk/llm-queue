"""Clients per (account, region) for one engine, borrowed from the process's `ClientHub`.

A `Route` says where one attempt goes: the account, the region the client calls and the model
id (an inference profile's id for a profile). `ClientPool` hands out Bedrock and
OpenAI-compatible clients for routes; the clients themselves live in the hub, shared with every
other pool of the process on the same event loop, and are closed when the last pool lets go.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import astuple, dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from llm_queue._logging import get_logger
from llm_queue.clients.hub import (
    ClientHub,
    Lease,
    WarmOptions,
    client_hub,
    identity_hash,
    pool_size_for,
)
from llm_queue.errors import ConfigError, LLMQueueError, MissingExtra
from llm_queue.tuning import BedrockOptions

if TYPE_CHECKING:
    from aiobotocore.session import AioSession
    from openai import AsyncOpenAI
    from types_aiobotocore_bedrock_runtime import BedrockRuntimeClient

    from llm_queue.registry import Account, Deployment, ResolvedRegistry

logger = get_logger(__name__)

# Unverified starting values. The SDK read timeout only guards a dead socket: the library's own
# first-response and idle timers decide when an attempt is given up.

# Extra `create_client` arguments for an AWS account (None = the pod's own credential chain).
# A mapping with `expiry_time` (datetime or ISO 8601) is refreshed: the callable is asked again
# ~5 minutes before that time (`REFRESH_BEFORE_SECS`), in a thread.
CredentialSource = Callable[["Account"], Mapping[str, Any] | None]
# The value of a secret by name (`api_key_ref`, `external_id_ref`); default: the environment.
SecretSource = Callable[[str], str | None]

# Refreshable credentials (assumed roles, `credentials=` with an expiry) are renewed from this
# long before they expire (botocore's default 15 min refreshed 15-minute sessions on every
# call); inside the last 2 minutes every caller waits for the renewal.
REFRESH_BEFORE_SECS = 300
REFRESH_MANDATORY_SECS = 120


@dataclass(frozen=True, slots=True)
class OpenAIOptions:
    """The OpenAI-compatible clients' timeouts and connection pool (one client per account)."""

    read_timeout_secs: float = 300.0  # a long generation; each attempt has its own timer too
    connect_timeout_secs: float = 10.0
    max_connections: int = 100
    max_keepalive_connections: int = 20
    keepalive_secs: float = 15.0


@dataclass(frozen=True, slots=True)
class Route:
    """Where one attempt goes: account, region to call, model id, and the deployment's name."""

    account: Account
    region: str
    model_id: str
    deployment: str


def route_for(view: ResolvedRegistry, deployment: Deployment) -> Route:
    """The route of a deployment: its account and region, its profile id or the model id."""
    model = view.registry.models[deployment.model]
    return Route(
        account=view.registry.accounts[deployment.account],
        region=deployment.region,
        model_id=deployment.profile_id or model.model_id,
        deployment=deployment.key,
    )


def _pod_chain(account: Account) -> Mapping[str, Any] | None:
    return None


def _identity(account: Account) -> tuple[str | None, ...]:
    """The fields that decide an account's credentials: a registry edit to any is a new client."""
    return (
        account.name,
        account.kind,
        account.role_arn,
        account.external_id_ref,
        account.base_url,
        account.api_key_ref,
    )


# AWS RoleSessionName allows only [\w+=,.@-]; a worker id like "pod:1a2b" would fail AssumeRole.
_SESSION_UNSAFE = re.compile(r"[^\w+=,.@-]")


class CredentialsUnavailable(LLMQueueError):
    """An account's credentials could not be obtained (STS refused or unreachable)."""


def _metadata(extra: Mapping[str, Any], expiry: Any) -> dict[str, Any]:
    """The refreshable-credential metadata of `create_client` style arguments."""
    when = expiry.isoformat() if isinstance(expiry, datetime) else str(expiry)
    return {
        "access_key": extra.get("aws_access_key_id"),
        "secret_key": extra.get("aws_secret_access_key"),
        "token": extra.get("aws_session_token"),
        "expiry_time": when,
    }


def _refreshing(metadata: dict[str, Any], refresh: Any, method: str) -> AioSession:
    """A session whose credentials renew themselves `REFRESH_BEFORE_SECS` before expiry."""
    from aiobotocore.credentials import AioRefreshableCredentials
    from aiobotocore.session import AioSession

    credentials = AioRefreshableCredentials.create_from_metadata(
        metadata=metadata, refresh_using=refresh, method=method
    )
    # Set after creation: older botocore has no advisory/mandatory arguments here.
    credentials._advisory_refresh_timeout = REFRESH_BEFORE_SECS  # type: ignore[attr-defined]  # botocore class attribute, not in the stubs
    credentials._mandatory_refresh_timeout = REFRESH_MANDATORY_SECS  # type: ignore[attr-defined]  # as above
    session = AioSession()
    # aiobotocore has no public setter for refreshable credentials.
    session._credentials = credentials  # type: ignore[attr-defined]
    return session


async def _keep_warm(client: Any, warm: WarmOptions) -> None:
    """Open `warm.connections` connections to the client's endpoint, then keep them open (E4).

    Unsigned HEAD requests through the client's own aiohttp session (so the connections land in
    the pool its calls use): no model call, nothing billable. Skipped while the client has calls
    in flight (its connections are warm then) or when the HTTP backend is not aiohttp.
    """
    import aiohttp

    http = getattr(getattr(client, "_endpoint", None), "http_session", None)
    get_session = getattr(http, "_get_session", None)
    url = client.meta.endpoint_url
    if get_session is None or warm.connections <= 0:
        return
    timeout = aiohttp.ClientTimeout(total=15)

    async def head(session: Any, proxy: str | None) -> None:
        async with session.head(url, allow_redirects=False, proxy=proxy, timeout=timeout) as r:
            await r.read()

    while True:
        try:
            proxy = http._proxy_config.proxy_url_for(url)  # type: ignore[union-attr]
            session = await get_session(proxy)
            if not getattr(session.connector, "_acquired", None):
                await asyncio.gather(*(head(session, proxy) for _ in range(warm.connections)))
        except Exception as exc:  # warm-up is best effort: a call meets real errors itself
            logger.debug("client_warm_failed", endpoint=url, error=type(exc).__name__)
        if warm.refresh_secs <= 0:
            return
        await asyncio.sleep(warm.refresh_secs)


class ClientPool:
    """Clients per (account, region) for Bedrock and per account for OpenAI-compatible servers.

    `credentials` gives an AWS account's client arguments (tests pass static keys per account);
    `secrets` reads `api_key_ref` values. Clients come from the process hub (shared with other
    pools on this loop) and are given back by `aclose()`.
    """

    def __init__(
        self,
        *,
        bedrock_endpoint_url: str | None = None,
        credentials: CredentialSource = _pod_chain,
        secrets: SecretSource = os.environ.get,
        session: AioSession | None = None,
        options: BedrockOptions | None = None,
        openai_options: OpenAIOptions | None = None,
        warm: WarmOptions | None = None,
        sts_endpoint_url: str | None = None,
        session_name: str = "llmq",
        hub: ClientHub | None = None,
    ) -> None:
        """Keep settings only; no SDK import and no client until the first call.

        `session_name` names assumed-role sessions (`llmq-<env>-<pod>` in CloudTrail).
        """
        self._endpoint = bedrock_endpoint_url
        self._credentials = credentials
        self._secrets = secrets
        self._session = session
        self._own_session = session is not None
        self._options = options or BedrockOptions()  # timeouts, pool size: `Tuning.bedrock`
        self._openai_options = openai_options or OpenAIOptions()
        self._warm = warm or WarmOptions()
        self._read = self._options.read_timeout_secs
        self._connect = self._options.connect_timeout_secs
        self._sts_endpoint = sts_endpoint_url
        self._session_name = session_name
        self._hub = hub if hub is not None else client_hub()  # an empty hub is falsy
        self._bedrock: dict[tuple[Any, str], Lease] = {}
        self._openai: dict[Any, Lease] = {}
        self._roles: dict[tuple[Any, str], AioSession] = {}
        self._locks: dict[tuple[Any, ...], asyncio.Lock] = {}
        self._sizes: dict[tuple[str, str], int] = {}
        self._retiring: dict[asyncio.Task[None], list[Lease]] = {}

    def _lock(self, key: tuple[Any, ...]) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = self._locks[key] = asyncio.Lock()
        return lock

    async def bedrock(self, route: Route) -> BedrockRuntimeClient:
        """The Bedrock runtime client of the route's account and region."""
        key = (_identity(route.account), route.region)
        if (lease := self._bedrock.get(key)) is not None:
            return lease.client  # type: ignore[no-any-return]
        async with self._lock(("bedrock", *key)):  # per key: other accounts build meanwhile
            if (lease := self._bedrock.get(key)) is not None:
                return lease.client  # type: ignore[no-any-return]
            lease = await self._acquire_bedrock(route)
            stale = [
                k for k in self._bedrock if k[1] == route.region and k[0][0] == route.account.name
            ]
            self._bedrock[key] = lease
            # The registry changed this account's fields: the old client goes after a grace.
            old = [self._bedrock.pop(k) for k in stale]
            for lease_ in old:
                self._hub.evict(lease_)
            self._retire(old)
            return lease.client  # type: ignore[no-any-return]

    async def _acquire_bedrock(self, route: Route) -> Lease:
        from aiobotocore.config import AioConfig
        from aiobotocore.session import get_session

        account = route.account
        if self._session is None:
            self._session = get_session()
        extra = dict(self._credentials(account) or {})
        expiry = extra.pop("expiry_time", None)
        who: tuple[Any, ...] = (
            _identity(account),
            sorted(extra.items()),
            id(self._session) if self._own_session else None,
        )
        if account.role_arn:
            role = (self._sts_endpoint, self._session_name, self._options.role_session_secs)
            who = (*who, "role", role)
        size = self._sizes.get((account.name, route.region), self._options.max_pool_connections)
        options = (self._endpoint, self._read, self._connect, size, self._warm.keepalive_secs)
        hub_key = ("bedrock", identity_hash(*who), route.region, options)

        async def build() -> tuple[Any, Callable[[], Awaitable[Any]]]:
            config = AioConfig(
                read_timeout=self._read,
                connect_timeout=self._connect,
                max_pool_connections=size,
                # One attempt per call: throttles reach the library, not botocore's retries.
                retries={"total_max_attempts": 1, "mode": "standard"},
                # botocore re-walks every message block before sending: 8.5 ms vs 3.5 ms CPU
                # per attempt at 218 KB (measured). Bedrock validates anyway; a bad request
                # comes back as ValidationException, which is classified fatal.
                parameter_validation=False,
                # Idle pooled connections live this long (aiobotocore's default is 12 s).
                connector_args={"keepalive_timeout": self._warm.keepalive_secs},
            )
            session, kwargs = self._session, extra
            if account.role_arn:  # AssumeRole only when the hub has no client for it yet
                session, kwargs = await self._role_session(route), {}
            elif expiry is not None:
                session, kwargs = self._refreshing_session(account, extra, expiry), {}
            assert session is not None
            context = session.create_client(
                "bedrock-runtime",
                region_name=route.region,
                endpoint_url=self._endpoint,
                config=config,
                **kwargs,
            )
            client = await context.__aenter__()
            return client, lambda: context.__aexit__(None, None, None)

        lease = await self._hub.acquire(hub_key, build)
        if not lease.tasks and lease.refs == 1 and self._warm.connections > 0:
            self._hub.spawn(lease, _keep_warm(lease.client, self._warm))
        return lease

    def _refreshing_session(self, account: Account, extra: dict[str, Any], expiry: Any) -> Any:
        """A session for `credentials=` values with an expiry: asked again before it passes."""

        async def refresh() -> dict[str, Any]:
            given = await asyncio.to_thread(self._credentials, account)
            fresh = dict(given) if given is not None else {}
            when = fresh.pop("expiry_time", None)
            if when is None:
                raise CredentialsUnavailable(f"account {account.name}: no expiry_time on refresh")
            return _metadata(fresh, when)

        return _refreshing(_metadata(extra, expiry), refresh, "llmq-credentials")

    async def _role_session(self, route: Route) -> AioSession:
        """A session whose credentials come from STS AssumeRole, per (account, STS region)."""
        account = route.account
        key = (_identity(account), route.region)
        if key in self._roles:
            return self._roles[key]
        from aiobotocore.session import get_session

        base = self._session or get_session()
        base_kwargs = dict(self._credentials(account) or {})
        base_kwargs.pop("expiry_time", None)
        external_id = self._secrets(account.external_id_ref) if account.external_id_ref else None
        if account.external_id_ref and not external_id:
            raise CredentialsUnavailable(
                f"account {account.name}: secret {account.external_id_ref} is not set"
            )

        async def assume() -> dict[str, Any]:
            request: dict[str, Any] = {
                "RoleArn": account.role_arn,
                "RoleSessionName": _SESSION_UNSAFE.sub("-", self._session_name)[:64],
                "DurationSeconds": self._options.role_session_secs,
            }
            if external_id:
                request["ExternalId"] = external_id
            try:
                async with base.create_client(
                    "sts", region_name=route.region, endpoint_url=self._sts_endpoint, **base_kwargs
                ) as sts:
                    reply = await sts.assume_role(**request)
            except Exception as exc:
                # The code only: STS text can carry role ARNs and account ids, and this message
                # reaches replies, status and logs (the cause keeps the detail for debugging).
                code = (
                    getattr(exc, "response", {}).get("Error", {}).get("Code") or type(exc).__name__
                )
                raise CredentialsUnavailable(
                    f"account {account.name}: AssumeRole failed ({code})"
                ) from exc
            creds = reply["Credentials"]
            return {
                "access_key": creds["AccessKeyId"],
                "secret_key": creds["SecretAccessKey"],
                "token": creds["SessionToken"],
                "expiry_time": creds["Expiration"].isoformat(),
            }

        session = _refreshing(await assume(), assume, "sts-assume-role")
        self._roles[key] = session
        return session

    def openai(self, route: Route) -> AsyncOpenAI:
        """The OpenAI-compatible client of the route's account (its `base_url` and key)."""
        account = route.account
        ident = _identity(account)
        if (lease := self._openai.get(ident)) is not None:
            return lease.client  # type: ignore[no-any-return]
        try:
            import openai
            from openai import AsyncOpenAI, Timeout
            from openai._constants import DEFAULT_CONNECTION_LIMITS
        except ImportError as exc:
            raise MissingExtra("an openai account", "openai") from exc
        key = self._secrets(account.api_key_ref) if account.api_key_ref else None
        if account.api_key_ref and not key:
            raise ConfigError(f"account {account.name}: secret {account.api_key_ref} is not set")
        opts = self._openai_options

        def build() -> tuple[Any, Callable[[], Awaitable[Any]]]:
            # The SDK's own HTTP types: openai 1.x uses httpx, 3.x its httpx2 fork.
            limits = type(DEFAULT_CONNECTION_LIMITS)(
                max_connections=opts.max_connections,
                max_keepalive_connections=opts.max_keepalive_connections,
                keepalive_expiry=opts.keepalive_secs,
            )
            timeout = Timeout(opts.read_timeout_secs, connect=opts.connect_timeout_secs)
            client = AsyncOpenAI(
                api_key=key or "unused",
                base_url=account.base_url,
                max_retries=0,
                timeout=timeout,
                http_client=openai.DefaultAsyncHttpxClient(limits=limits, timeout=timeout),
            )
            return client, client.close

        hub_key = ("openai", identity_hash(ident, key), astuple(opts))
        lease = self._hub.acquire_now(hub_key, build)
        self._openai[ident] = lease
        return lease.client  # type: ignore[no-any-return]

    def plan(self, slots: Mapping[tuple[str, str], int]) -> None:
        """Size future Bedrock clients: concurrency slots per (account name, region) (E5).

        Each client gets `pool_size_for(slots)` connections (at least 10, at most 200); a
        client already built keeps its size until it is built again.
        """
        self._sizes = {key: pool_size_for(n) for key, n in slots.items()}

    async def warm(
        self, routes: Iterable[Route], slots: Mapping[tuple[str, str], int] | None = None
    ) -> None:
        """Build the Bedrock clients of `routes` concurrently (E3); errors are left for calls.

        One build per (account, region), all at once: start takes about one client's build
        time instead of one per deployment. `slots` sizes the pools first (`plan`).
        """
        if slots is not None:
            self.plan(slots)
        unique = {(_identity(r.account), r.region): r for r in routes if r.account.kind == "aws"}
        results = await asyncio.gather(
            *(self.bedrock(r) for r in unique.values()), return_exceptions=True
        )
        for route, result in zip(unique.values(), results, strict=True):
            if isinstance(result, BaseException):
                logger.warning(
                    "client_build_failed",
                    account=route.account.name,
                    region=route.region,
                    error=type(result).__name__,
                )

    def forget(self, route: Route) -> None:
        """Drop the route's account clients in its region so the next call reads credentials again.

        Only this (account, region): another region of the account keeps its client. The
        dropped clients are given back after a grace period (calls already using them finish
        first); before, each auth failure leaked a client and its connection pool.
        """
        ident = _identity(route.account)
        leases = []
        if (lease := self._bedrock.pop((ident, route.region), None)) is not None:
            leases.append(lease)
        self._roles.pop((ident, route.region), None)
        if (lease := self._openai.pop(ident, None)) is not None:
            leases.append(lease)
        for lease in leases:
            self._hub.evict(lease)  # the next pool to ask builds a fresh one
        self._retire(leases)

    def _retire(self, leases: list[Lease]) -> None:
        if not leases:
            return
        task = asyncio.get_running_loop().create_task(self._release_later(leases))
        self._retiring[task] = leases
        task.add_done_callback(lambda done: self._retiring.pop(done, None))

    async def _release_later(self, leases: list[Lease]) -> None:
        await asyncio.sleep(self._read + self._connect)  # the longest a running call can need
        for lease in leases:
            await self._hub.release(lease)

    async def aclose(self) -> None:
        """Give every client back to the hub (the last holder closes it); safe to repeat."""
        retired = [lease for leases in self._retiring.values() for lease in leases]
        for task in list(self._retiring):  # give retired clients back now, not after the grace
            task.cancel()
        self._retiring.clear()
        leases = [*self._bedrock.values(), *self._openai.values(), *retired]
        self._bedrock.clear()
        self._openai.clear()
        self._roles.clear()
        for lease in leases:
            await self._hub.release(lease)
