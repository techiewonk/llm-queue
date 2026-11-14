"""Bedrock runtime over aiohttp: botocore only resolves credentials, signs and frames streams.

`BedrockHTTP` makes the three calls `BedrockProvider` makes (`converse`, `converse_stream`,
`invoke_model`) with aiobotocore's argument and reply shapes, so the provider runs on either
transport (`BedrockOptions.transport`). The JSON body is built with orjson, signed with
botocore's `SigV4Auth`, POSTed on the client's own aiohttp session and parsed with orjson;
stream frames go through botocore's `EventStreamBuffer` (CRC-checked). Errors are botocore
`ClientError` / `EventStreamError` with the response dict botocore builds, so
`classify_bedrock` reads both transports the same way.
"""

from __future__ import annotations

import asyncio
import base64
import http.client
import inspect
import os
import ssl
from collections.abc import AsyncIterator, Mapping
from copy import deepcopy
from typing import Any
from urllib.parse import quote

import aiohttp
import orjson
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.eventstream import EventStreamBuffer, EventStreamMessage
from botocore.exceptions import (
    ClientError,
    EventStreamError,
    NoCredentialsError,
    ParamValidationError,
)
from botocore.httpsession import ProxyConfiguration, create_urllib3_context, get_cert_path
from yarl import URL

from llm_queue._version import __version__
from llm_queue.providers.classify import StreamTruncated

SIGNING_NAME = "bedrock"  # bedrock-runtime signs as "bedrock"
_USER_AGENT = f"llm-queue/{__version__} aiohttp/{aiohttp.__version__}"
# ConverseStream events of the service model; botocore drops any other event type too.
_EVENTS = frozenset(
    {
        "messageStart",
        "contentBlockStart",
        "contentBlockDelta",
        "contentBlockStop",
        "messageStop",
        "metadata",
    }
)
# InvokeModel members sent as headers (botocore's service model, bedrock-runtime 2023-09-30).
_INVOKE_HEADERS = {
    "contentType": "Content-Type",
    "accept": "Accept",
    "trace": "X-Amzn-Bedrock-Trace",
    "guardrailIdentifier": "X-Amzn-Bedrock-GuardrailIdentifier",
    "guardrailVersion": "X-Amzn-Bedrock-GuardrailVersion",
    "performanceConfigLatency": "X-Amzn-Bedrock-PerformanceConfig-Latency",
    "serviceTier": "X-Amzn-Bedrock-Service-Tier",
    "requestMetadata": "X-Amzn-Bedrock-Request-Metadata",
}
_INVOKE_REPLY_HEADERS = {
    "performanceConfigLatency": "x-amzn-bedrock-performanceconfig-latency",
    "serviceTier": "x-amzn-bedrock-service-tier",
}
_MEDIA = ("image", "document", "video", "audio")


def default_endpoint(region: str) -> str:
    """The public Bedrock runtime endpoint of `region` (China regions use `.com.cn`)."""
    suffix = "amazonaws.com.cn" if region.startswith("cn-") else "amazonaws.com"
    return f"https://bedrock-runtime.{region}.{suffix}"


def _blob(value: Any) -> str:
    """The orjson `default`: bytes become base64 text, as botocore sends blob members."""
    if isinstance(value, bytes | bytearray | memoryview):
        return base64.b64encode(value).decode("ascii")
    raise TypeError(f"cannot send {type(value).__name__} to Bedrock")


def _blob_paths(block: Mapping[str, Any]) -> list[tuple[Mapping[str, Any], str]]:
    """(holder, key) of every blob member of one Converse content block or stream delta.

    The service model's blob members: media `source.bytes`, the same inside `toolResult`
    content and `guardContent.image`, and `reasoningContent.redactedContent`.
    """
    found: list[tuple[Mapping[str, Any], str]] = []
    for media in _MEDIA:
        source = (block.get(media) or {}).get("source")
        if isinstance(source, Mapping) and "bytes" in source:
            found.append((source, "bytes"))
    guard = ((block.get("guardContent") or {}).get("image") or {}).get("source")
    if isinstance(guard, Mapping) and "bytes" in guard:
        found.append((guard, "bytes"))
    reasoning = block.get("reasoningContent")
    if isinstance(reasoning, Mapping) and "redactedContent" in reasoning:
        found.append((reasoning, "redactedContent"))
    for inner in (block.get("toolResult") or {}).get("content") or ():
        if isinstance(inner, Mapping):
            found += _blob_paths(inner)
    return found


def _decode_blobs(blocks: Any) -> None:
    """Turn the base64 text of every blob member of `blocks` into bytes, in place."""
    for block in blocks or ():
        if isinstance(block, dict):
            for holder, key in _blob_paths(block):
                if isinstance(holder[key], str):
                    holder[key] = base64.b64decode(holder[key])  # type: ignore[index]


def _text_blobs(request: Mapping[str, Any]) -> bool:
    """True when a blob member holds text: botocore base64-encodes its UTF-8 bytes."""
    for message in request.get("messages") or ():
        for block in (message.get("content") if isinstance(message, Mapping) else None) or ():
            if isinstance(block, Mapping) and any(
                isinstance(h[k], str) for h, k in _blob_paths(block)
            ):
                return True
    return False


def _with_text_blobs_encoded(request: dict[str, Any]) -> dict[str, Any]:
    """A copy of `request` whose text blob members are bytes (sent base64-encoded)."""
    copy = deepcopy(request)
    for message in copy.get("messages") or ():
        content = message.get("content") if isinstance(message, dict) else None
        for block in content or ():
            if isinstance(block, dict):
                for holder, key in _blob_paths(block):
                    if isinstance(holder[key], str):
                        holder[key] = holder[key].encode()  # type: ignore[index]
    return copy


def _headers(response: aiohttp.ClientResponse) -> dict[str, str]:
    """Response headers with lower-case names, as botocore's `HTTPHeaders`."""
    return {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in response.raw_headers}


def _metadata(status: int, headers: dict[str, str]) -> dict[str, Any]:
    return {
        "RequestId": headers.get("x-amzn-requestid", ""),
        "HTTPStatusCode": status,
        "HTTPHeaders": headers,
        "RetryAttempts": 0,
    }


def client_error(status: int, headers: dict[str, str], body: bytes, operation: str) -> ClientError:
    """The `ClientError` botocore's rest-json parser builds for an error response.

    Code from `x-amzn-ErrorType`, else the body's `code` / `__type`, else the status; message
    from the body's `message` / `Message`; a 5xx with an empty or HTML body is generic.
    """
    stripped = body.strip()
    if status >= 500 and (not stripped or stripped.startswith(b"<html>")):
        generic: Any = {  # botocore's TypedDict stubs do not take a plain dict
            "Error": {"Code": str(status), "Message": http.client.responses.get(status, "")},
            "ResponseMetadata": {"HTTPStatusCode": status, "HTTPHeaders": headers},
        }
        return ClientError(generic, operation)
    try:
        parsed = orjson.loads(body) if body else {}
    except orjson.JSONDecodeError:
        parsed = {"message": body.decode("utf-8", "replace")}
    if not isinstance(parsed, dict):
        parsed = {}
    code: Any = parsed.get("__type", str(status))
    if "x-amzn-errortype" in headers:
        code = headers["x-amzn-errortype"]
    elif "code" in parsed or "Code" in parsed:
        code = parsed.get("code", parsed.get("Code", ""))
    if isinstance(code, str):
        code = code.split(":", 1)[0].rsplit("#", 1)[-1]
    error = {"Code": code, "Message": parsed.get("message", parsed.get("Message", ""))}
    response: Any = {"Error": error, "ResponseMetadata": _metadata(status, headers)}
    return ClientError(response, operation)


def _loads(data: bytes, what: str) -> Any:
    """Parse JSON from the wire; garbage (a gateway's HTML page, a cut frame) is retryable.

    `StreamTruncated` instead of `orjson.JSONDecodeError`, which classified FATAL and was never
    retried (CL-13); the cause keeps the parser's detail.
    """
    try:
        return orjson.loads(data) if data else {}
    except orjson.JSONDecodeError as exc:
        raise StreamTruncated(f"the Bedrock {what} is not JSON") from exc


def _event(message: EventStreamMessage) -> dict[str, Any] | None:
    """One ConverseStream frame as botocore yields it: `{event_type: payload}`.

    None for an event type the service model does not know; an exception or error frame
    raises `EventStreamError` with botocore's error dict.
    """
    headers: dict[str, Any] = message.headers  # type: ignore[assignment]  # stubs say int
    kind = headers.get(":message-type")
    if kind == "event":
        event_type = headers.get(":event-type")
        if event_type not in _EVENTS:
            return None
        payload = _loads(message.payload, "stream event")
        if event_type == "contentBlockDelta":
            _decode_blobs((payload.get("delta"),))
        return {event_type: payload}
    if kind == "exception":
        body = _loads(message.payload, "stream exception")
        error = {
            "Code": headers.get(":exception-type", ""),
            "Message": body.get("Message", body.get("message", "")),
        }
    else:
        error = {
            "Code": headers.get(":error-code", ""),
            "Message": headers.get(":error-message", ""),
        }
    response: Any = {"Error": error}
    raise EventStreamError(response, "ConverseStream")


class EventStream:
    """The `stream` of a ConverseStream reply; `close()` drops its connection."""

    def __init__(self, response: aiohttp.ClientResponse) -> None:
        """Read frames from `response`."""
        self._response = response

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self._events()

    async def _events(self) -> AsyncIterator[dict[str, Any]]:
        buffer = EventStreamBuffer()
        async for chunk in self._response.content.iter_any():
            buffer.add_data(chunk)
            for message in buffer:
                event = _event(message)
                if event is not None:
                    yield event

    def close(self) -> None:
        """Close the response: its connection is dropped unless the body was fully read."""
        self._response.close()


class Body:
    """The `body` of an InvokeModel reply: `async with body as b: await b.read()`."""

    def __init__(self, response: aiohttp.ClientResponse) -> None:
        """Read from `response`."""
        self._response = response

    async def __aenter__(self) -> Body:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._response.close()  # a no-op once read: the connection went back to the pool

    async def read(self) -> bytes:
        """The whole body."""
        return await self._response.read()


class BedrockHTTP:
    """A Bedrock runtime client for one (credentials, region) on its own aiohttp session.

    `credentials` is a botocore / aiobotocore credentials object (refreshable or not; its
    frozen copy is read before every signature, so a refresh re-signs the next call).
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        credentials: Any,
        region: str,
        endpoint_url: str,
        proxy: str | None = None,
    ) -> None:
        """Use `session`; build with `open()`."""
        self.session = session
        self.credentials = credentials
        self.region = region
        self.endpoint_url = endpoint_url.rstrip("/")
        self.proxy = proxy
        self._paths: dict[tuple[str, str], tuple[str, URL]] = {}

    @classmethod
    async def open(
        cls,
        *,
        region: str,
        credentials: Any,
        endpoint_url: str | None = None,
        read_timeout: float,
        connect_timeout: float,
        pool_size: int,
        keepalive_secs: float,
    ) -> BedrockHTTP:
        """A client with a pool of `pool_size` connections and botocore's TLS and proxy setup."""
        url = (endpoint_url or default_endpoint(region)).rstrip("/")
        proxy = ProxyConfiguration().proxy_url_for(url)
        tls: ssl.SSLContext | bool = False
        if url.startswith("https:"):
            tls = await asyncio.to_thread(_tls_context)  # loads the CA bundle from disk
        connector = aiohttp.TCPConnector(limit=pool_size, keepalive_timeout=keepalive_secs, ssl=tls)
        session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(sock_connect=connect_timeout, sock_read=read_timeout),
            auto_decompress=False,
            headers={"User-Agent": _USER_AGENT, "Accept-Encoding": "identity"},
        )
        return cls(session, credentials, region, url, proxy)

    async def close(self) -> None:
        """Close the session and its connections."""
        await self.session.close()

    def _url(self, model_id: str, action: str) -> tuple[str, URL]:
        """The (signing, sending) URL of `/model/{modelId}/{action}`, cached per model."""
        key = (model_id, action)
        found = self._paths.get(key)
        if found is None:
            # botocore percent-encodes the label (":" and "/" too); yarl must not re-encode it.
            raw = f"{self.endpoint_url}/model/{quote(model_id, safe='')}/{action}"
            found = self._paths[key] = (raw, URL(raw, encoded=True))
        return found

    async def _send(
        self, model_id: str, action: str, body: bytes, headers: dict[str, str]
    ) -> aiohttp.ClientResponse:
        """Sign and POST; the response is open (the caller reads and releases it)."""
        credentials = self.credentials
        if credentials is None:
            raise NoCredentialsError()
        frozen = credentials.get_frozen_credentials()
        if inspect.isawaitable(frozen):  # aiobotocore's refreshable credentials
            frozen = await frozen
        raw, url = self._url(model_id, action)
        request = AWSRequest(method="POST", url=raw, data=body, headers=headers)
        SigV4Auth(frozen, SIGNING_NAME, self.region).add_auth(request)
        # Never follow a redirect (botocore does not): aiohttp would re-send the signed body,
        # `X-Amz-Date` and `X-Amz-Security-Token` to the new host (CL-4). A 3xx is an error.
        return await self.session.post(
            url,
            data=body,
            headers=dict(request.headers.items()),
            proxy=self.proxy,
            allow_redirects=False,
        )

    async def _fail(self, response: aiohttp.ClientResponse, operation: str) -> ClientError:
        """Read an error response, release its connection, and build its `ClientError`."""
        try:
            body = await response.read()
        finally:
            response.release()
        return client_error(response.status, _headers(response), body, operation)

    @staticmethod
    def _json(request: dict[str, Any]) -> bytes:
        if _text_blobs(request):
            request = _with_text_blobs_encoded(request)
        return orjson.dumps(request, default=_blob)

    async def converse(self, **request: Any) -> dict[str, Any]:
        """Bedrock Converse: the parsed reply with `ResponseMetadata`, as botocore returns it."""
        model_id = request.pop("modelId")
        body = self._json(request)
        response = await self._send(
            model_id, "converse", body, {"Content-Type": "application/json"}
        )
        if response.status >= 300:
            raise await self._fail(response, "Converse")
        try:
            data = await response.read()
        except BaseException:
            response.close()
            raise
        reply: dict[str, Any] = _loads(data, "Converse reply")
        _decode_blobs(((reply.get("output") or {}).get("message") or {}).get("content"))
        reply["ResponseMetadata"] = _metadata(response.status, _headers(response))
        return reply

    async def converse_stream(self, **request: Any) -> dict[str, Any]:
        """Bedrock ConverseStream: `{"stream": EventStream, "ResponseMetadata": ...}`."""
        model_id = request.pop("modelId")
        body = self._json(request)
        response = await self._send(
            model_id, "converse-stream", body, {"Content-Type": "application/json"}
        )
        if response.status >= 300:
            raise await self._fail(response, "ConverseStream")
        meta = _metadata(response.status, _headers(response))
        return {"stream": EventStream(response), "ResponseMetadata": meta}

    async def invoke_model(self, **request: Any) -> dict[str, Any]:
        """Bedrock InvokeModel: `{"body": Body, "contentType": ..., "ResponseMetadata": ...}`."""
        model_id = request.pop("modelId")
        body = request.pop("body", b"")
        if isinstance(body, str):
            body = body.encode()
        headers = {}
        for name, value in request.items():
            header = _INVOKE_HEADERS.get(name)
            if header is None:
                raise ParamValidationError(report=f"Unknown parameter in input: {name!r}")
            headers[header] = str(value)
        response = await self._send(model_id, "invoke", bytes(body), headers)
        if response.status >= 300:
            raise await self._fail(response, "InvokeModel")
        replied = _headers(response)
        reply: dict[str, Any] = {
            "ResponseMetadata": _metadata(response.status, replied),
            "contentType": replied.get("content-type", ""),
            "body": Body(response),
        }
        for name, header in _INVOKE_REPLY_HEADERS.items():
            if header in replied:
                reply[name] = replied[header]
        return reply


def _tls_context() -> ssl.SSLContext:
    """The verifying TLS context of botocore, with `AWS_CA_BUNDLE` or certifi's CA bundle."""
    context: ssl.SSLContext = create_urllib3_context()
    context.check_hostname = True
    context.load_verify_locations(os.environ.get("AWS_CA_BUNDLE") or get_cert_path(True))
    return context
