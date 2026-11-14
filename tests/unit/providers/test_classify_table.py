"""Table of real provider errors -> kind / retry-after / target flag, as classify.py sees them.

Bedrock rows are botocore `ClientError` / `EventStreamError` shapes with the service's own
message text; OpenAI rows are openai SDK exceptions built on real httpx responses. Rows where
today's answer is wrong (RESCAN C7, P5) are strict xfails: the fix flips them to passing.
"""

from __future__ import annotations

from typing import Any

import aiohttp
import httpx
import openai
import pytest
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    EndpointConnectionError,
    EventStreamError,
    ReadTimeoutError,
)

from llm_queue.errors import ErrorKind
from llm_queue.providers.classify import classify_bedrock, classify_openai, is_target_error

T, R, F = ErrorKind.THROTTLE, ErrorKind.RETRYABLE, ErrorKind.FATAL
URL = "https://bedrock-runtime.ap-south-1.amazonaws.com/model/x/converse"


def _aws(code: str, status: int, message: str, headers: dict[str, str] | None = None) -> Any:
    response: Any = {
        "Error": {"Code": code, "Message": message},
        "ResponseMetadata": {"HTTPStatusCode": status, "HTTPHeaders": headers or {}},
    }
    return ClientError(response, "ConverseStream")


def _in_stream(code: str, message: str) -> Any:
    """An exception event inside ConverseStream: botocore gives no HTTP metadata."""
    return EventStreamError({"Error": {"Code": code, "Message": message}}, "ConverseStream")


def _fix(reason: str) -> Any:
    return pytest.mark.xfail(strict=True, reason=f"fixed by {reason}")


BEDROCK: list[Any] = [
    # (error, kind, target, retry_after)
    pytest.param(
        _aws("ThrottlingException", 429, "Too many requests, please wait before trying again."),
        T,
        False,
        None,
        id="throttling",
    ),
    pytest.param(
        _aws("ThrottlingException", 429, "Too many tokens, please wait before trying again."),
        T,
        False,
        None,
        id="throttling-tokens",
    ),
    pytest.param(
        _aws(
            "ServiceQuotaExceededException",
            400,
            "Your request exceeds the service quota for "
            "your account. You can resubmit your request later.",
        ),
        T,
        False,
        None,
        id="service-quota",
    ),
    pytest.param(
        _aws("ModelNotReadyException", 429, "Model is not ready to serve inference requests."),
        T,
        False,
        None,
        id="model-not-ready",
    ),
    pytest.param(
        _aws("ThrottlingException", 429, "Rate exceeded", {"Retry-After": "3"}),
        T,
        False,
        3.0,
        id="throttling-retry-after",
    ),
    pytest.param(
        _aws("ServiceUnavailableException", 503, "Service is temporarily unavailable."),
        R,
        False,
        None,
        id="unavailable",
    ),
    pytest.param(
        _aws("InternalServerException", 500, "An internal server error occurred."),
        R,
        False,
        None,
        id="internal",
    ),
    pytest.param(
        _aws("ModelTimeoutException", 408, "Model has timed out in processing the request."),
        R,
        False,
        None,
        id="model-timeout",
    ),
    pytest.param(
        _aws("ModelErrorException", 424, "The model returned an error."),
        R,
        False,
        None,
        id="model-error",
    ),
    pytest.param(
        _aws("ValidationException", 400, "The provided model identifier is invalid."),
        F,
        True,
        None,
        id="bad-model-id",
    ),
    pytest.param(
        _aws(
            "ValidationException",
            400,
            "Invocation of model ID openai.gpt-oss-120b-1:0 with "
            "on-demand throughput isn't supported. Retry your request with the ID or ARN of "
            "an inference profile that contains this model.",
        ),
        F,
        True,
        None,
        id="needs-profile",
    ),
    pytest.param(
        _aws("ValidationException", 400, "This model version has reached the end of its life."),
        F,
        True,
        None,
        id="end-of-life",
    ),
    pytest.param(
        _aws("ValidationException", 400, "Input is too long for requested model."),
        F,
        False,
        None,
        id="input-too-long",
    ),
    pytest.param(
        _aws("ValidationException", 400, "A conversation must start with a user message."),
        F,
        False,
        None,
        id="bad-request",
    ),
    pytest.param(
        _aws(
            "AccessDeniedException",
            403,
            "You don't have access to the model with the specified model ID.",
        ),
        F,
        True,
        None,
        id="access-denied",
    ),
    pytest.param(
        _aws("ResourceNotFoundException", 404, "Model not found."),
        F,
        True,
        None,
        id="not-found",
    ),
    pytest.param(
        _aws(
            "UnrecognizedClientException",
            403,
            "The security token included in the request is invalid.",
        ),
        F,
        False,
        None,
        id="unrecognized-client",
    ),
    pytest.param(
        _aws("ExpiredTokenException", 403, "The security token included in the request is expired"),
        F,
        False,
        None,
        id="expired-token",
    ),
    pytest.param(
        _aws(
            "InvalidSignatureException",
            403,
            "The request signature we calculated does not "
            "match the signature you provided. Check your AWS Secret Access Key and signing "
            "method.",
        ),
        F,
        True,
        None,
        id="signature-mismatch",
        marks=_fix("C7"),
    ),
    pytest.param(
        _aws(
            "InvalidSignatureException",
            403,
            "Signature expired: 20261114T100000Z is now "
            "earlier than 20261114T100600Z (20261114T101100Z - 5 min.)",
        ),
        F,
        True,
        None,
        id="clock-skew",
        marks=_fix("C7"),
    ),
    pytest.param(
        _in_stream("throttlingException", "Too many tokens, please wait."),
        T,
        False,
        None,
        id="stream-throttling",
    ),
    pytest.param(
        _in_stream("modelStreamErrorException", "The model stream was interrupted."),
        R,
        False,
        None,
        id="stream-model-error",
    ),
    pytest.param(
        _in_stream("internalServerException", "An internal server error occurred."),
        R,
        False,
        None,
        id="stream-internal",
    ),
    pytest.param(
        _in_stream("serviceUnavailableException", "Service is temporarily unavailable."),
        R,
        False,
        None,
        id="stream-unavailable",
    ),
    pytest.param(
        _in_stream("validationException", "Input is too long for requested model."),
        F,
        False,
        None,
        id="stream-validation",
    ),
    pytest.param(ReadTimeoutError(endpoint_url=URL), R, False, None, id="read-timeout"),
    pytest.param(EndpointConnectionError(endpoint_url=URL), R, False, None, id="no-endpoint"),
    pytest.param(ConnectionClosedError(endpoint_url=URL), R, False, None, id="connection-closed"),
    pytest.param(ConnectionResetError(104, "reset"), R, False, None, id="connection-reset"),
    pytest.param(
        aiohttp.SocketTimeoutError("Timeout on reading data from socket"),
        R,
        False,
        None,
        id="stream-stall",
    ),
    pytest.param(
        aiohttp.ClientPayloadError("Response payload is not completed"),
        R,
        False,
        None,
        id="stream-closed",
    ),
    pytest.param(ValueError("bug in the request builder"), F, False, None, id="plain-bug"),
]


@pytest.mark.parametrize(("exc", "kind", "target", "retry_after"), BEDROCK)
def test_bedrock_errors(
    exc: BaseException, kind: ErrorKind, target: bool, retry_after: float | None
) -> None:
    info = classify_bedrock(exc)
    assert info.kind is kind
    assert is_target_error(exc, info) is target
    assert info.retry_after_secs == retry_after


# Any: openai 3 types its errors on httpx2; the SDK only reads attributes, so httpx works.
_REQUEST: Any = httpx.Request("POST", "http://vllm.local/v1/chat/completions")


def _oa(cls: type[openai.APIStatusError], status: int, body: Any, **headers: str) -> Any:
    response: Any = httpx.Response(status, headers=headers, json={"error": body}, request=_REQUEST)
    return cls(f"Error code: {status} - {body}", response=response, body=body)


def _body(message: str, kind: str, code: str | None) -> dict[str, Any]:
    return {"message": message, "type": kind, "code": code, "param": None}


OPENAI: list[Any] = [
    pytest.param(
        _oa(
            openai.RateLimitError,
            429,
            _body("Rate limit reached", "requests", "rate_limit_exceeded"),
            **{"retry-after-ms": "1500"},
        ),
        T,
        False,
        1.5,
        id="rate-limit",
    ),
    pytest.param(
        _oa(
            openai.RateLimitError,
            429,
            _body(
                "You exceeded your current quota, please check your plan and billing details.",
                "insufficient_quota",
                "insufficient_quota",
            ),
        ),
        F,
        False,
        None,
        id="insufficient-quota",
    ),
    pytest.param(
        _oa(openai.InternalServerError, 500, _body("internal", "server_error", None)),
        R,
        False,
        None,
        id="500",
    ),
    pytest.param(
        _oa(openai.InternalServerError, 502, _body("bad gateway", "server_error", None)),
        R,
        False,
        None,
        id="502",
    ),
    pytest.param(
        _oa(
            openai.InternalServerError,
            503,
            _body("overloaded", "server_error", None),
            **{"retry-after": "2"},
        ),
        R,
        False,
        2.0,
        id="503-retry-after",
    ),
    pytest.param(
        _oa(openai.APIStatusError, 408, _body("timeout", "timeout", None)),
        R,
        False,
        None,
        id="408",
    ),
    pytest.param(
        _oa(openai.ConflictError, 409, _body("conflict", "invalid_request_error", None)),
        R,
        False,
        None,
        id="409",
    ),
    pytest.param(
        _oa(
            openai.BadRequestError,
            400,
            _body(
                "This model's maximum context length is 8192 tokens. However, you requested 9000 "
                "tokens.",
                "invalid_request_error",
                "context_length_exceeded",
            ),
        ),
        F,
        False,
        None,
        id="context-length",
    ),
    pytest.param(
        _oa(
            openai.NotFoundError,
            404,
            _body("The model `gpt-x` does not exist", "invalid_request_error", "model_not_found"),
        ),
        F,
        True,
        None,
        id="model-not-found",
    ),
    pytest.param(
        _oa(
            openai.AuthenticationError,
            401,
            _body("Incorrect API key provided", "invalid_request_error", "invalid_api_key"),
        ),
        F,
        False,
        None,
        id="bad-key",
    ),
    pytest.param(
        _oa(
            openai.UnprocessableEntityError, 422, _body("bad field", "invalid_request_error", None)
        ),
        F,
        False,
        None,
        id="422",
    ),
    pytest.param(openai.APIConnectionError(request=_REQUEST), R, False, None, id="connection"),
    pytest.param(openai.APITimeoutError(request=_REQUEST), R, False, None, id="timeout"),
    pytest.param(
        httpx.RemoteProtocolError("peer closed connection without sending complete message"),
        R,
        False,
        None,
        id="stream-closed",
    ),
    pytest.param(httpx.ReadTimeout("read timed out"), R, False, None, id="stream-stall"),
    pytest.param(
        # A `data: {"error": ...}` event mid-stream: APIError with no status code.
        openai.APIError(
            "An internal server error occurred.",
            _REQUEST,
            body=_body("An internal server error occurred.", "server_error", "internal_error"),
        ),
        R,
        False,
        None,
        id="mid-stream-error",
        marks=_fix("P5"),
    ),
]


@pytest.mark.parametrize(("exc", "kind", "target", "retry_after"), OPENAI)
def test_openai_errors(
    exc: BaseException, kind: ErrorKind, target: bool, retry_after: float | None
) -> None:
    info = classify_openai(exc)
    assert info.kind is kind
    assert is_target_error(exc, info) is target
    assert info.retry_after_secs == retry_after
