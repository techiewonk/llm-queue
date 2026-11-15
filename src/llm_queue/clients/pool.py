"""Clients per (account, region) for one engine, borrowed from the process's `ClientHub`.

A `Route` says where one attempt goes: the account, the region the client calls and the model
id (an inference profile's id for a profile). `ClientPool` hands out Bedrock and
OpenAI-compatible clients for routes; the clients themselves live in the hub, shared with every
other pool of the process on the same event loop, and are closed when the last pool lets go.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import os
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import astuple, dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, cast

from llm_queue._logging import get_logger
from llm_queue.clients.hub import (
    ClientHub,
    Lease,
    client_hub,
    identity_hash,
    pool_size_for,
)
from llm_queue.clients.timing import AttemptTimeout
from llm_queue.errors import LLMQueueError, MissingExtra
from llm_queue.tuning import BedrockOptions, OpenAIOptions, WarmOptions

if TYPE_CHECKING:
    from aiobotocore.session import AioSession
    from openai import AsyncOpenAI

    from llm_queue.clients.bedrock_http import BedrockHTTP
    from llm_queue.registry import Account, Deployment, ResolvedRegistry

logger = get_logger(__name__)


class BedrockClient(Protocol):
    """The Bedrock calls `BedrockProvider` makes, on either transport (`BedrockOptions`).

    `BedrockHTTP` (aiohttp) and the aiobotocore client both fit: an adapter per transport
    behind one interface, so strict mypy checks the provider against what actually runs.
    """

    async def converse(self, **request: Any) -> dict[str, Any]:
        """Bedrock Converse: the parsed reply with `ResponseMetadata`."""
        ...

    async def converse_stream(self, **request: Any) -> dict[str, Any]:
        """Bedrock ConverseStream: `{"stream": ..., "ResponseMetadata": ...}`."""
        ...

    async def invoke_model(self, **request: Any) -> dict[str, Any]:
        """Bedrock InvokeModel: `{"body": ..., "contentType": ..., "ResponseMetadata": ...}`."""
        ...


if TYPE_CHECKING:

    def _fits(client: BedrockHTTP) -> BedrockClient:
        """Type check only: a signature drift in `BedrockHTTP` fails mypy here (CL-12)."""
        return client


# A retired client (forget, account edit) is given back once no call uses it; the grace lets
# a call that just looked it up start borrowing it, the cap bounds a call stuck forever.
RETIRE_GRACE_SECS = 1.0
RETIRE_MAX_SECS = 3600.0

# Extra `create_client` arguments for an AWS account (None = the pod's own credential chain).
# A mapping with `expiry_time` (datetime or ISO 8601) is refreshed: the callable is asked again
# ~5 minutes before that time (`REFRESH_BEFORE_SECS`). Called in a thread (CL-8).
CredentialSource = Callable[["Account"], Mapping[str, Any] | None]
# The value of a secret by name (`api_key_ref`, `external_id_ref`); default: the environment.
# Called in a thread on the async paths (a Secrets Manager lookup must not block the loop).
SecretSource = Callable[[str], str | None]

# Refreshable credentials (assumed roles, `credentials=` with an expiry) are renewed from this
# long before they expire (botocore's default 15 min refreshed 15-minute sessions on every
# call); inside the last 2 minutes every caller waits for the renewal.
REFRESH_BEFORE_SECS = 300
REFRESH_MANDATORY_SECS = 120
# STS answers in well under a second; botocore's defaults (60 s connect and read, legacy
# retries) let one slow AssumeRole eat a caller's whole attempt (CL-8). Unverified values.
STS_READ_TIMEOUT_SECS = 10.0
STS_MAX_ATTEMPTS = 2
# `credentials=` keys the aiohttp transport understands; any other key (verify, use_ssl,
# api_version ...) is a `create_client` argument, so that account uses the botocore transport.
_AWS_KEYS = frozenset({"aws_access_key_id", "aws_secret_access_key", "aws_session_token"})


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


class CredentialsRefreshSlow(CredentialsUnavailable):
    """The attempt timer fired while expiring credentials were being refreshed (FC-2).

    The worker backs the account off but keeps its client and the shared refresh task (the
    next call finds the refresh done). `first` is True for the first call cut by one refresh:
    only that one takes a back-off step, so five calls cut together do not double it five times.
    """

    def __init__(self, message: str, *, first: bool) -> None:
        """Keep `first` (see the class)."""
        super().__init__(message)
        self.first = first


def _metadata(extra: Mapping[str, Any], expiry: Any) -> dict[str, Any]:
    """The refreshable-credential metadata of `create_client` style arguments."""
    when = expiry.isoformat() if isinstance(expiry, datetime) else str(expiry)
    return {
        "access_key": extra.get("aws_access_key_id"),
        "secret_key": extra.get("aws_secret_access_key"),
        "token": extra.get("aws_session_token"),
        "expiry_time": when,
    }


# Set by `borrow_bedrock` for one attempt: `_single_flight` adds (mandatory, first) when the
# attempt was cancelled (its timer fired) while waiting for a credentials refresh (CL-8, FC-2).
_refresh_cut: contextvars.ContextVar[list[tuple[bool, bool]] | None] = contextvars.ContextVar(
    "llmq_refresh_cut", default=None
)


def _single_flight(
    fetch: Callable[[], Awaitable[dict[str, Any]]],
    mandatory: Callable[[], bool] = lambda: True,
) -> Callable[[], Awaitable[dict[str, Any]]]:
    """`fetch` run as one shared task: a caller cancelled while waiting does not abort it.

    aiobotocore refreshes inside the caller's attempt timer; when the timer fired it cancelled
    the refresh, so inside the mandatory window a slow STS was never waited out and every call
    failed (CL-8). The next caller now awaits the same task. A cut records whether the keys
    were inside the mandatory window (`mandatory()`) and whether it is the refresh's first.
    """
    running: asyncio.Task[dict[str, Any]] | None = None
    counted: asyncio.Task[dict[str, Any]] | None = None  # the refresh whose cut was counted

    async def refresh() -> dict[str, Any]:
        nonlocal running, counted
        if running is None or running.done():
            running = asyncio.ensure_future(fetch())
            # Read the outcome even when every waiter was cancelled (no "never retrieved").
            running.add_done_callback(lambda t: t.cancelled() or t.exception())
        task = running
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # The attempt ran out waiting for credentials, not for the provider (CL-8).
            if (cut := _refresh_cut.get()) is not None:
                urgent = mandatory()
                first = urgent and counted is not task
                if first:
                    counted = task
                cut.append((urgent, first))
            raise

    return refresh


def _refreshing(metadata: dict[str, Any], refresh: Any, method: str) -> AioSession:
    """A session whose credentials renew themselves `REFRESH_BEFORE_SECS` before expiry."""
    from aiobotocore.credentials import AioRefreshableCredentials
    from aiobotocore.session import AioSession

    box: list[Any] = []

    def mandatory() -> bool:
        """True when the current keys expire within `REFRESH_MANDATORY_SECS` (FC-2).

        aiobotocore also refreshes inline in the advisory window, while the keys are still good
        for minutes: a timer cut there is a plain timeout, not a credentials failure.
        """
        try:
            return bool(box[0]._seconds_remaining() < REFRESH_MANDATORY_SECS)
        except Exception:  # an older botocore without the helper: the old behaviour
            return True

    credentials = AioRefreshableCredentials.create_from_metadata(
        metadata=metadata, refresh_using=_single_flight(refresh, mandatory), method=method
    )
    box.append(credentials)
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

    from llm_queue.clients.bedrock_http import BedrockHTTP

    own = client if isinstance(client, BedrockHTTP) else None
    http: Any = getattr(getattr(client, "_endpoint", None), "http_session", None)
    get_session: Any = getattr(http, "_get_session", None)
    # The aiohttp transport has its own session and proxy.
    url = own.endpoint_url if own is not None else client.meta.endpoint_url
    if (own is None and get_session is None) or warm.connections <= 0:
        return
    timeout = aiohttp.ClientTimeout(total=15)

    async def head(session: Any, proxy: str | None) -> None:
        async with session.head(url, allow_redirects=False, proxy=proxy, timeout=timeout) as r:
            await r.read()

    while True:
        try:
            if own is not None:
                proxy, session = own.proxy, own.session
            else:
                proxy = http._proxy_config.proxy_url_for(url)
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
        self._read_secs = self._options.read_timeout_secs
        self._connect_secs = self._options.connect_timeout_secs
        self._sts_endpoint = sts_endpoint_url
        self._session_name = session_name
        self._hub = hub if hub is not None else client_hub()  # an empty hub is falsy
        self._bedrock: dict[tuple[Any, str], Lease] = {}
        self._openai: dict[Any, Lease] = {}
        self._roles: dict[tuple[Any, str], AioSession] = {}
        self._locks: dict[tuple[Any, ...], asyncio.Lock] = {}
        self._sizes: dict[tuple[str, str], int] = {}
        self._retiring: dict[asyncio.Task[None], list[Lease]] = {}
        self._releasing: set[asyncio.Future[None]] = set()  # shielded releases running (N1)
        self._warned: set[tuple[str, str, str]] = set()

    def _warn_once(self, event: str, account: str, what: str = "", **fields: Any) -> None:
        """Log a config problem once per (event, account, what): it would repeat on every build.

        `what` tells apart problems of one account, such as its two secrets (`problems`).
        """
        if (event, account, what) not in self._warned:
            self._warned.add((event, account, what))
            logger.warning(event, account=account, **fields)

    async def bedrock(self, route: Route) -> BedrockClient:
        """The Bedrock runtime client of the route's account and region."""
        return cast("BedrockClient", (await self._bedrock_lease(route)).client)

    @contextlib.asynccontextmanager
    async def borrow_bedrock(self, route: Route) -> AsyncIterator[BedrockClient]:
        """The route's Bedrock client, counted as in use until the block ends (CL-3).

        An attempt timer that fired while the call waited for a refresh of expiring keys (a
        slow STS or `credentials=` lookup) raises `CredentialsRefreshSlow`, not
        `AttemptTimeout`: the region did nothing wrong (CL-8). While the keys are still valid
        (aiobotocore's advisory window) the cut stays an `AttemptTimeout` (FC-2). A call still
        queued on botocore's refresh lock (not yet in the refresh) reads as `AttemptTimeout`.
        """
        lease = await self._bedrock_lease(route)
        lease.enter()
        cut: list[tuple[bool, bool]] = []
        token = _refresh_cut.set(cut)
        try:
            yield cast("BedrockClient", lease.client)
        except AttemptTimeout as exc:
            if not any(urgent for urgent, _ in cut):
                raise
            raise CredentialsRefreshSlow(
                f"account {route.account.name}: credentials refresh not done in time ({exc})",
                first=any(first for _, first in cut),
            ) from exc
        finally:
            _refresh_cut.reset(token)
            lease.leave()

    @contextlib.asynccontextmanager
    async def borrow_openai(self, route: Route) -> AsyncIterator[AsyncOpenAI]:
        """The route's OpenAI-compatible client, counted as in use until the block ends."""
        await self.openai(route)
        lease = self._openai[_identity(route.account)]
        lease.enter()
        try:
            yield cast("AsyncOpenAI", lease.client)
        finally:
            lease.leave()

    async def _bedrock_lease(self, route: Route) -> Lease:
        key = (_identity(route.account), route.region)
        if (lease := self._bedrock.get(key)) is not None:
            return lease
        # Per key: other accounts build meanwhile.
        async with self._locks.setdefault(("bedrock", *key), asyncio.Lock()):
            if (lease := self._bedrock.get(key)) is not None:
                return lease
            lease = await self._acquire_bedrock(route)
            stale = [
                k for k in self._bedrock if k[1] == route.region and k[0][0] == route.account.name
            ]
            self._bedrock[key] = lease
            # The registry changed this account's fields: the old client goes when idle.
            old = [self._bedrock.pop(k) for k in stale]
            for lease_ in old:
                self._hub.evict(lease_)
            self._retire(old)
            return lease

    async def _acquire_bedrock(self, route: Route) -> Lease:
        from aiobotocore.config import AioConfig
        from aiobotocore.session import get_session

        account = route.account
        if self._session is None:
            self._session = get_session()
        given: Mapping[str, Any] | None = await asyncio.to_thread(self._credentials, account)
        extra = dict(given or {})
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
        transport = self._options.transport
        unknown = sorted(set(extra) - _AWS_KEYS)
        if extra.get("verify") is False or extra.get("use_ssl") is False:
            # Owner decision Q31 (R3SE-12): allowed everywhere, never silent.
            self._warn_once(
                "bedrock_tls_verify_off",
                account.name,
                hint="credentials= turned TLS verification off; set AWS_CA_BUNDLE or "
                "REQUESTS_CA_BUNDLE to trust a private CA instead",
            )
        if transport == "aiohttp" and unknown and not account.role_arn and expiry is None:
            # Never raise into a call for this (CL-6): botocore takes these keys. botocore
            # follows redirects (CL-4), re-sending the signed request and session token to
            # the new host, so the warning says so (N4).
            self._warn_once(
                "bedrock_transport_fallback",
                account.name,
                keys=unknown,
                transport="botocore",
                caveat="the botocore transport follows HTTP redirects (aiohttp does not)",
            )
            transport = "botocore"
        options = (
            self._endpoint,
            self._read_secs,
            self._connect_secs,
            size,
            self._warm.keepalive_secs,
            transport,
        )
        hub_key = ("bedrock", identity_hash(*who), route.region, options)

        async def build() -> tuple[Any, Callable[[], Awaitable[Any]]]:
            if transport == "aiohttp":
                return await self._build_http(route, extra, expiry, size)
            config = AioConfig(
                read_timeout=self._read_secs,
                connect_timeout=self._connect_secs,
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
            if not kwargs and session is self._session:
                await self._chain_credentials(account, session)
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

    async def _build_http(
        self, route: Route, extra: dict[str, Any], expiry: Any, size: int
    ) -> tuple[Any, Callable[[], Awaitable[Any]]]:
        """A `BedrockHTTP` client (transport "aiohttp") with the credentials botocore would use.

        The same sources as the botocore clients: an assumed role or `credentials=` with an
        expiry (both refreshable), static keys, else the pod's credential chain.
        """
        from botocore.credentials import Credentials

        from llm_queue.clients.bedrock_http import BedrockHTTP

        account = route.account
        credentials: Any
        if account.role_arn:
            credentials = await (await self._role_session(route)).get_credentials()
        elif expiry is not None:
            credentials = await self._refreshing_session(account, extra, expiry).get_credentials()
        elif extra:
            credentials = Credentials(
                str(extra.get("aws_access_key_id") or ""),
                str(extra.get("aws_secret_access_key") or ""),
                extra.get("aws_session_token"),
            )
        else:
            assert self._session is not None
            credentials = await self._chain_credentials(account, self._session)
        client = await BedrockHTTP.open(
            region=route.region,
            credentials=credentials,
            endpoint_url=self._endpoint,
            read_timeout_secs=self._read_secs,
            connect_timeout_secs=self._connect_secs,
            pool_size=size,
            keepalive_secs=self._warm.keepalive_secs,
        )
        return client, client.close

    @staticmethod
    async def _chain_credentials(account: Account, session: AioSession) -> Any:
        """The pod chain's credentials; `CredentialsUnavailable` when it resolves none (CL-7).

        Raised before a client exists, so no client holding `None` is cached: the worker
        backs off and the next build reads the chain again (IRSA / IMDS keys that appear later).
        """
        credentials = await session.get_credentials()
        if credentials is None:
            raise CredentialsUnavailable(f"account {account.name}: no AWS credentials found")
        return credentials

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
        given: Mapping[str, Any] | None = await asyncio.to_thread(self._credentials, account)
        base_kwargs = dict(given or {})
        base_kwargs.pop("expiry_time", None)
        external_id = None
        if account.external_id_ref:
            external_id = await asyncio.to_thread(self._secrets, account.external_id_ref)
        if account.external_id_ref and not external_id:
            self._warn_once(
                "account_secret_missing",
                account.name,
                account.external_id_ref,
                secret=account.external_id_ref,
            )
            raise CredentialsUnavailable(
                f"account {account.name}: secret {account.external_id_ref} is not set"
            )

        from aiobotocore.config import AioConfig

        sts_config = AioConfig(
            connect_timeout=self._connect_secs,
            read_timeout=STS_READ_TIMEOUT_SECS,
            retries={"total_max_attempts": STS_MAX_ATTEMPTS, "mode": "standard"},
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
                    "sts",
                    region_name=route.region,
                    endpoint_url=self._sts_endpoint,
                    config=sts_config,
                    **base_kwargs,
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

    async def openai(self, route: Route) -> AsyncOpenAI:
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
        # In a thread: a Secrets Manager lookup must not block the loop (CL-8).
        key = None
        if account.api_key_ref:
            key = await asyncio.to_thread(self._secrets, account.api_key_ref)
        if (lease := self._openai.get(ident)) is not None:  # built while the lookup ran
            return lease.client  # type: ignore[no-any-return]
        if account.api_key_ref and not key:
            # Not ConfigError (FATAL, `provider_error`): the worker forgets the route, backs off
            # and tries the next deployment, as for the Bedrock twin (CL-6).
            self._warn_once(
                "account_secret_missing",
                account.name,
                account.api_key_ref,
                secret=account.api_key_ref,
            )
            raise CredentialsUnavailable(
                f"account {account.name}: secret {account.api_key_ref} is not set"
            )
        opts = self._openai_options

        def build() -> tuple[Any, Callable[[], Awaitable[Any]]]:
            # The SDK's own HTTP types: openai 1.x uses httpx, 3.x its httpx2 fork.
            limits = type(DEFAULT_CONNECTION_LIMITS)(
                max_connections=opts.max_connections,
                max_keepalive_connections=opts.max_keepalive_connections,
                keepalive_expiry=opts.keepalive_secs,
            )
            timeout = Timeout(opts.read_timeout_secs, connect=opts.connect_timeout_secs)
            http: Any = None
            if opts.http == "aiohttp":
                # openai >= 1.9x; 1.x / 2.x need `httpx-aiohttp`, 3.x vendors it. A missing
                # piece raises RuntimeError on construction: fall back to httpx.
                aio = getattr(openai, "DefaultAioHttpClient", None)
                with contextlib.suppress(RuntimeError):
                    http = aio(limits=limits, timeout=timeout) if aio is not None else None
                if http is None:
                    logger.warning("openai_aiohttp_unavailable", account=account.name)
            if http is None:
                http = openai.DefaultAsyncHttpxClient(limits=limits, timeout=timeout)
            client = AsyncOpenAI(
                api_key=key or "unused",
                base_url=account.base_url,
                max_retries=0,
                timeout=timeout,
                http_client=http,
            )
            return client, client.close

        hub_key = ("openai", identity_hash(ident, key), astuple(opts))
        lease = self._hub.acquire_now(hub_key, build)
        self._openai[ident] = lease
        return lease.client  # type: ignore[no-any-return]

    def plan(self, slots: Mapping[tuple[str, str], int]) -> None:
        """Size future Bedrock clients: concurrency slots per (account name, region) (E5).

        Each client gets `pool_size_for(slots)` connections (at least 10); a
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
        routes = list(routes)
        for problem in await asyncio.to_thread(self.problems, routes):
            logger.warning("client_config_problem", problem=problem)
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

    def problems(self, routes: Iterable[Route]) -> list[str]:
        """Every secret the routes' accounts name but `secrets` does not have, listed at once.

        `warm` logs them at start, so a missing key shows before the first call; a call on such
        an account fails over with `CredentialsUnavailable` instead of raising config errors.
        """
        out: list[str] = []
        seen: set[str] = set()
        for route in routes:
            account = route.account
            for ref in (account.api_key_ref, account.external_id_ref):
                warned = ("account_secret_missing", account.name, ref or "")  # at a call (N2)
                if ref and warned not in self._warned and ref not in seen:
                    seen.add(ref)
                    if not self._secrets(ref):
                        out.append(f"account {account.name}: secret {ref} is not set")
        return out

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
        """Give retired leases back once no call uses them (at most `RETIRE_MAX_SECS`).

        A fixed `read + connect` grace closed the client under a healthy answer longer than
        that (the stream timer is idle-based), which read as a provider fault (CL-3). Each
        lease leaves `leases` as it is released, so `aclose` never releases one twice (CL-2).
        """
        await asyncio.sleep(RETIRE_GRACE_SECS)  # a call that just got the client borrows it
        await asyncio.gather(*(lease.wait_idle(RETIRE_MAX_SECS) for lease in leases))
        while leases:
            lease = leases.pop(0)
            # Shielded: `aclose` cancelling this task must not cut a close half way; `aclose`
            # awaits it through `_releasing`, so it returns once the client is closed (N1).
            release = asyncio.ensure_future(self._hub.release(lease))
            self._releasing.add(release)
            release.add_done_callback(self._releasing.discard)
            await asyncio.shield(release)

    async def aclose(self) -> None:
        """Give every client back to the hub (the last holder closes it); safe to repeat."""
        tasks, lists = list(self._retiring), list(self._retiring.values())
        for task in tasks:  # give retired clients back now, not after the calls on them end
            task.cancel()
        # Wait for the tasks first: one may be releasing a lease right now (CL-2), and for the
        # shielded release it started, which the cancel does not stop (N1).
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*self._releasing, return_exceptions=True)
        self._retiring.clear()
        retired = [lease for leases in lists for lease in leases]  # those not released yet
        leases = [*self._bedrock.values(), *self._openai.values(), *retired]
        self._bedrock.clear()
        self._openai.clear()
        self._roles.clear()
        for lease in leases:
            await self._hub.release(lease)
