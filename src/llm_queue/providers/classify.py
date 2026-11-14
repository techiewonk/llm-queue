"""Map Bedrock and OpenAI SDK exceptions to `ErrorInfo` (throttle / retryable / fatal).

Decisions use error *families*, not exact class names or codes, because real services and
fakes spell the same failure differently (`ThrottlingException`, `Throttling`, `throttled`).
Used by `BedrockProvider.classify` and `OpenAIProvider.classify`; never imports an SDK.
`is_auth_error`, `request_error_kind` and `is_connection_reset` refine a FATAL / transport
error for the worker (try elsewhere, which `CallFailed` kind, one quick retry).
"""

from __future__ import annotations

import re
import sys
import time
from collections.abc import Mapping
from email.utils import parsedate_to_datetime
from typing import Any

from llm_queue.errors import ErrorInfo, ErrorKind, LLMQueueError

# Prefixes of normalised codes (lower-case letters only, "exception"/"error" suffix dropped).
_THROTTLE_PREFIXES = (
    "throttl",
    "toomanyrequests",
    "modelnotready",
    "servicequotaexceeded",
    "requestlimitexceeded",
    "provisionedthroughputexceeded",
    "ratelimit",
    "slowdown",
)
_RETRYABLE_PREFIXES = (
    "serviceunavailable",
    "internal",
    "modeltimeout",
    "modelerror",
    "modelstreamerror",
    "requesttimeout",
    "timeout",
    "serviceerror",
    "unavailable",
)
_FATAL_PREFIXES = (
    "validation",
    "accessdenied",
    "resourcenotfound",
    "unrecognizedclient",
    "notauthorized",
    "unauthorized",
    "expiredtoken",
    "invalid",
    "badrequest",
)
# Fatal families that name one target (model, profile, region, endpoint or the key for that
# URL), not the request: the next target may serve it. Credentials errors are not here.
_TARGET_FAMILIES = (
    "resourcenotfound",
    "accessdenied",
    "permissiondenied",
    "modelnotfound",
    "notfound",
    "endpointnotfound",
    "unknownoperation",
)
# A validation / bad-request family counts as target-specific only when its message says so.
_TARGET_MESSAGE = re.compile(
    r"model identifier is invalid|invalid model identifier|on-demand throughput"
    r"|inference profile|model_not_found|unknown model|no such model|end of (?:its )?life"
    r"|not (?:available|supported|enabled|accessible) (?:in|for) (?:this|the|your) region"
    r"|\b(?:model|endpoint)\b[^.]{0,80}?\b(?:not found|does not exist|doesn't exist"
    r"|not available|unavailable|is not supported|isn't supported)",
    re.IGNORECASE,
)
# Request-fatal however the code is spelled: a different target would refuse it the same way.
_REQUEST_MESSAGE = re.compile(
    r"too long|too large|too many (?:input )?tokens|context (?:length|window)"
    r"|maximum (?:input|context|token)|malformed",
    re.IGNORECASE,
)
# Credentials refused, expired or signed with a skewed clock: the account (or this pod's clock)
# is the problem, not the request or the model, so the worker drops the client and tries the
# next target (plan C7). `invalidsignature` / `signaturedoesnotmatch` include "Signature
# expired" (clock skew over 5 minutes).
_AUTH_FAMILIES = (
    "unrecognizedclient",
    "expiredtoken",
    "invalidclienttokenid",
    "invalidsignature",
    "signaturedoesnotmatch",
    "incompletesignature",
    "missingauthenticationtoken",
    "authfailure",
    "authentication",
    "invalidapikey",
    "requestexpired",
    # botocore found no (or half of the) credentials: the client is rebuilt so keys that
    # appear later (IRSA, IMDS, a rotated secret) are read; before, it kept failing (CL-7).
    "nocredentials",
    "partialcredentials",
)
_AUTH_MESSAGE = re.compile(
    r"signature expired|security token included in the request is (?:invalid|expired)"
    r"|request signature we calculated does not match|incorrect api key|invalid api key",
    re.IGNORECASE,
)
# Request-fatal: the input does not fit the model's context window, on any target.
_CONTEXT_MESSAGE = re.compile(
    r"too many (?:input )?tokens|input is too long|prompt is too long|context.length"
    r"|context window|maximum context|maximum input",
    re.IGNORECASE,
)
# Connection dropped by the peer (keep-alive socket closed by the server, NAT reset): one quick
# retry on a new connection, without charging the breaker (plan C7). Timeouts are not here.
_RESET_TYPES: dict[str, tuple[str, ...]] = {
    # ClientPayloadError: a Bedrock stream whose body was cut mid-way (the peer closed it).
    "aiohttp": (
        "ServerDisconnectedError",
        "ClientConnectionResetError",
        "ClientOSError",
        "ClientPayloadError",
    ),
    "botocore.exceptions": ("ConnectionClosedError",),
    # A garbled status line: aiobotocore maps it to ConnectionClosedError, while the aiohttp
    # transport raises `ClientResponseError` from it, so look for the cause (CL-5).
    "aiohttp.http_exceptions": ("BadStatusLine",),
    "httpx": ("RemoteProtocolError", "ReadError"),
    "httpx2": ("RemoteProtocolError", "ReadError"),
}
# No connection was made (refused, DNS): not a reset, though aiohttp's is a `ClientOSError`.
_CONNECT_FAILED: dict[str, tuple[str, ...]] = {"aiohttp": ("ClientConnectorError",)}


class StreamTruncated(LLMQueueError):
    """A provider stream ended without its end marker (messageStop, finish_reason): retryable."""


# Built lazily from whichever modules are already imported; an SDK that was never imported
# cannot have raised the exception being classified.
_TRANSPORT_TYPES: dict[str, tuple[str, ...]] = {
    "botocore.exceptions": (
        "ReadTimeoutError",
        "ConnectTimeoutError",
        "EndpointConnectionError",
        "ConnectionClosedError",
        "HTTPClientError",
    ),
    # A stream frame that fails its CRC or length checks: corrupt on the wire, so another
    # attempt can succeed (was FATAL on both transports, CL-13).
    "botocore.eventstream": ("ParserError",),
    "aiohttp": ("ClientError",),
    "httpx": ("TransportError",),
    "httpx2": ("TransportError",),
    "openai": ("APITimeoutError", "APIConnectionError"),
}


def error_family(code: str) -> str:
    """Normalise a provider error code: letters only, lower case, no exception suffix.

    `ThrottlingException`, `throttling_exception` and `Throttling` all become `throttling`.
    """
    family = re.sub(r"[^a-z]", "", code.lower())
    for suffix in ("exception", "error", "fault"):
        if family.endswith(suffix) and len(family) > len(suffix):
            return family[: -len(suffix)]
    return family


def kind_for_code(code: str, status: int | None = None) -> ErrorKind | None:
    """Classify a provider error code by family, falling back to the HTTP status.

    Returns None when neither the code nor the status says anything.
    """
    family = error_family(code)
    if family:
        for prefixes, kind in (
            (_THROTTLE_PREFIXES, ErrorKind.THROTTLE),
            (_RETRYABLE_PREFIXES, ErrorKind.RETRYABLE),
            (_FATAL_PREFIXES, ErrorKind.FATAL),
        ):
            if family.startswith(prefixes):
                return kind
    return kind_for_status(status)


def kind_for_status(status: int | None) -> ErrorKind | None:
    """Classify by HTTP status alone: 429 throttle, 408/409/5xx retryable, other 4xx fatal."""
    if status is None:
        return None
    if status == 429:
        return ErrorKind.THROTTLE
    if status in (408, 409) or status >= 500:
        return ErrorKind.RETRYABLE
    if 400 <= status < 500:
        return ErrorKind.FATAL
    return None


def parse_retry_after(
    headers: Mapping[str, Any] | None, *, now: float | None = None
) -> float | None:
    """Seconds to wait from `retry-after-ms`, else `Retry-After` (seconds or an HTTP date).

    Header names are matched case-blind; unparsable values are ignored and past dates give 0.
    """
    if not headers:
        return None
    lowered = {str(k).lower(): str(v).strip() for k, v in headers.items()}
    if (raw_ms := lowered.get("retry-after-ms")) is not None:
        try:
            return max(float(raw_ms) / 1000.0, 0.0)
        except ValueError:
            pass
    raw = lowered.get("retry-after")
    if raw is None:
        return None
    try:
        return max(float(raw), 0.0)
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        return None
    current = time.time() if now is None else now
    return max(when.timestamp() - current, 0.0)


# Transport failures no retry fixes: bad TLS (CA bundle, certificate), a refused proxy, a bad
# URL. aiobotocore maps the first two to botocore `SSLError` / `ProxyConnectionError`, which are
# not transport errors (FATAL); the aiohttp transport raises the aiohttp types, which were
# RETRYABLE and charged breakers on every attempt (CL-5). Checked before `_TRANSPORT_TYPES`.
_FATAL_TRANSPORT: dict[str, tuple[str, ...]] = {
    "aiohttp": (
        "ClientSSLError",
        "ClientProxyConnectionError",
        "ClientHttpProxyError",
        "InvalidURL",
        "TooManyRedirects",
    ),
}


def is_transport_error(exc: BaseException) -> bool:
    """True for timeouts and connection failures from asyncio, botocore, aiohttp, httpx, openai.

    False for TLS, proxy and URL failures (`_FATAL_TRANSPORT`): the same on every attempt.
    """
    if _is_any(exc, _FATAL_TRANSPORT):
        return False
    if isinstance(exc, TimeoutError | ConnectionError | StreamTruncated):
        return True
    return _is_any(exc, _TRANSPORT_TYPES)


def _is_any(exc: BaseException, table: Mapping[str, tuple[str, ...]]) -> bool:
    for module_name, names in table.items():
        module = sys.modules.get(module_name)
        if module is None:
            continue
        types = tuple(t for n in names if isinstance(t := getattr(module, n, None), type))
        if types and isinstance(exc, types):
            return True
    return False


def is_connection_reset(exc: BaseException) -> bool:
    """True when the peer dropped the connection (not a timeout): worth one immediate retry.

    Looks down the cause / context chain, since SDKs wrap the socket error (openai's
    `APIConnectionError` around httpx's `RemoteProtocolError`; with the aiohttp transport,
    `APITimeoutError` around `ConnectTimeout` around `ServerDisconnectedError`).
    """
    for err in _chain(exc):
        if isinstance(err, TimeoutError) or _is_any(err, _CONNECT_FAILED):
            continue
        if isinstance(err, ConnectionResetError | BrokenPipeError) or _is_any(err, _RESET_TYPES):
            return True
    return False


def _chain(exc: BaseException) -> list[BaseException]:
    """The error and its causes (`__cause__`, else `__context__`), at most 8 deep."""
    out: list[BaseException] = []
    err: BaseException | None = exc
    while err is not None and len(out) < 8 and err not in out:
        out.append(err)
        err = err.__cause__ or err.__context__
    return out


def is_auth_error(exc: BaseException, info: ErrorInfo) -> bool:
    """Credentials refused or expired, or a signature off by clock skew (plan C7).

    The worker drops the account's client (credentials are read again) and tries the next
    target instead of failing the call; an OpenAI-compatible 401 counts too.
    """
    if info.kind is not ErrorKind.FATAL:
        return False
    if error_family(info.code).startswith(_AUTH_FAMILIES) or _error_status(exc) == 401:
        return True
    return bool(_AUTH_MESSAGE.search(_error_message(exc)))


def request_error_kind(exc: BaseException, info: ErrorInfo) -> str | None:
    """The `CallFailed` kind of a FATAL error about the request itself, else None.

    `context_length_exceeded` when the input does not fit the model (Bedrock "too many tokens"
    / "Input is too long", OpenAI `context_length_exceeded`); `invalid_request` for any other
    validation / bad-request error. Target and credentials errors return None.
    """
    if info.kind is not ErrorKind.FATAL or is_auth_error(exc, info):
        return None
    if info.code == "context_length_exceeded" or _CONTEXT_MESSAGE.search(_error_message(exc)):
        return "context_length_exceeded"
    if is_target_error(exc, info):
        return None
    family = error_family(info.code)
    if family.startswith(("validation", "badrequest", "invalidrequest", "unprocessable")):
        return "invalid_request"
    return "invalid_request" if _error_status(exc) in (400, 413, 422) else None


def is_target_error(exc: BaseException, info: ErrorInfo) -> bool:
    """True when a FATAL error is about the target, so `run_call` tries the next fallback step.

    Model or profile not found / not in this region, access denied for the model, unknown
    endpoint (404). Bad payloads, too-long input and credentials errors end the call.
    """
    if info.kind is not ErrorKind.FATAL:
        return False
    message = _error_message(exc)
    if _REQUEST_MESSAGE.search(message):
        return False
    family = error_family(info.code)
    if family.startswith(_TARGET_FAMILIES) or _error_status(exc) == 404:
        return True
    request_family = family.startswith(("validation", "badrequest", "invalid"))
    return (request_family or _error_status(exc) == 400) and bool(_TARGET_MESSAGE.search(message))


def _error_message(exc: BaseException) -> str:
    """A botocore `Error.Message`, else the exception text (openai puts the body there)."""
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping) and isinstance(response.get("Error"), Mapping):
        return str(response["Error"].get("Message") or "")
    return str(exc)


def _error_status(exc: BaseException) -> int | None:
    """HTTP status of a botocore `ClientError` or an openai `APIStatusError`, if any."""
    status = getattr(exc, "status_code", None)
    response = getattr(exc, "response", None)
    if status is None and isinstance(response, Mapping):
        status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
    return status if isinstance(status, int) else None


def classify_bedrock(exc: BaseException) -> ErrorInfo:
    """Classify a botocore `ClientError` (or transport error) from a Bedrock runtime call.

    Unknown codes fall back to the HTTP status; anything else that is not a transport error is
    fatal, so a bug in the request never loops through retries.
    """
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping) and isinstance(response.get("Error"), Mapping):
        code = str(response["Error"].get("Code") or "")
        meta = response.get("ResponseMetadata") or {}
        status = meta.get("HTTPStatusCode")
        kind = kind_for_code(code, status if isinstance(status, int) else None)
        retry_after = parse_retry_after(meta.get("HTTPHeaders"))
        return ErrorInfo(kind=kind or ErrorKind.FATAL, code=code, retry_after_secs=retry_after)
    if is_transport_error(exc):
        return ErrorInfo(kind=ErrorKind.RETRYABLE, code=type(exc).__name__)
    return ErrorInfo(kind=ErrorKind.FATAL, code=type(exc).__name__)


def classify_openai(exc: BaseException) -> ErrorInfo:
    """Classify an openai SDK exception (or transport error) from an OpenAI-compatible server.

    429 is a throttle carrying the server's advised wait, except `insufficient_quota`, which
    no wait fixes. 408/409/5xx, timeouts and connection errors are retryable; other 4xx fatal.
    """
    if is_transport_error(exc):
        code = type(exc).__name__
        # The aiohttp transport maps a dropped or refused connection to `ConnectTimeout`
        # (no timer fired anywhere in the chain): name it as the httpx transport does.
        chain = _chain(exc)
        timed_out = any(isinstance(err, TimeoutError) for err in chain)
        if code == "APITimeoutError" and len(chain) > 1 and not timed_out:
            code = "APIConnectionError"
        return ErrorInfo(kind=ErrorKind.RETRYABLE, code=code)
    status = getattr(exc, "status_code", None)
    if not isinstance(status, int):
        openai = sys.modules.get("openai")
        if openai is not None and isinstance(exc, openai.APIError):
            # An `error` event inside a stream (no HTTP status): the server failed mid-answer.
            # Retryable; the worker never retries once chunks reached the caller (plan P5).
            code = str(getattr(exc, "code", None) or "stream_error")
            kind = ErrorKind.FATAL if code == "context_length_exceeded" else ErrorKind.RETRYABLE
            return ErrorInfo(kind=kind, code=code)
        return ErrorInfo(kind=ErrorKind.FATAL, code=type(exc).__name__)
    code = str(getattr(exc, "code", None) or getattr(exc, "type", None) or type(exc).__name__)
    response = getattr(exc, "response", None)
    retry_after = parse_retry_after(getattr(response, "headers", None))
    kind = kind_for_status(status) or ErrorKind.FATAL
    if kind is ErrorKind.THROTTLE and code == "insufficient_quota":
        kind = ErrorKind.FATAL
    return ErrorInfo(kind=kind, code=code, retry_after_secs=retry_after)
