"""Error table shared by the Bedrock and OpenAI-compatible fronts of fakellm.

One row per Bedrock error name (TESTING.md "Error fidelity"), with the HTTP status and
the OpenAI-shaped equivalent, so a scenario names an error once and both fronts agree.
Messages are the real services' texts, since classification reads some of them.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ErrorSpec:
    """How one named error is rendered by the Bedrock and the OpenAI fronts.

    `code` is the wire error type when it differs from the row name (two rows can share one
    AWS code with different messages, e.g. signature mismatch and clock skew);
    `openai_message` is the OpenAI front's text when it differs from the Bedrock one.
    """

    status: int
    message: str
    openai_status: int
    openai_type: str
    openai_code: str
    code: str | None = None
    openai_message: str | None = None

    def wire_code(self, name: str) -> str:
        """The `x-amzn-ErrorType` sent for the row called `name`."""
        return self.code or name


ERRORS: dict[str, ErrorSpec] = {
    "ThrottlingException": ErrorSpec(
        429,
        "Too many requests, please wait before trying again.",
        429,
        "requests",
        "rate_limit_exceeded",
    ),
    "ModelNotReadyException": ErrorSpec(
        429,
        "Model is not ready to serve inference requests. Retry later.",
        429,
        "requests",
        "model_not_ready",
    ),
    "ServiceUnavailableException": ErrorSpec(
        503,
        "Service is temporarily unavailable. Try again later.",
        503,
        "server_error",
        "service_unavailable",
    ),
    "InternalServerException": ErrorSpec(
        500,
        "An internal server error occurred. Retry your request.",
        500,
        "server_error",
        "internal_error",
    ),
    "ModelTimeoutException": ErrorSpec(
        408,
        "Model has timed out in processing the request. Try your request again.",
        408,
        "timeout",
        "timeout",
    ),
    "ModelErrorException": ErrorSpec(
        424,
        "The model returned an error while processing the request.",
        500,
        "server_error",
        "model_error",
    ),
    "ValidationException": ErrorSpec(
        400,
        "The request failed validation.",
        400,
        "invalid_request_error",
        "invalid_request",
    ),
    "AccessDeniedException": ErrorSpec(
        403,
        "You don't have access to the model with the specified model ID.",
        403,
        "invalid_request_error",
        "permission_denied",
    ),
    "ResourceNotFoundException": ErrorSpec(
        404,
        "The specified model was not found.",
        404,
        "invalid_request_error",
        "model_not_found",
    ),
    "UnrecognizedClientException": ErrorSpec(
        403,
        "The security token included in the request is invalid.",
        401,
        "invalid_request_error",
        "invalid_api_key",
    ),
    "InvalidSignatureException": ErrorSpec(
        403,
        "The request signature we calculated does not match the signature you provided. "
        "Check your AWS Secret Access Key and signing method. "
        "Consult the service documentation for details.",
        401,
        "invalid_request_error",
        "invalid_api_key",
    ),
    "InvalidSignatureClockSkew": ErrorSpec(
        403,
        "Signature expired: 20261114T100000Z is now earlier than 20261114T100600Z "
        "(20261114T101100Z - 5 min.)",
        401,
        "invalid_request_error",
        "invalid_api_key",
        code="InvalidSignatureException",
    ),
    "ExpiredTokenException": ErrorSpec(
        403,
        "The security token included in the request is expired",
        401,
        "invalid_request_error",
        "invalid_api_key",
    ),
    "ServiceQuotaExceededException": ErrorSpec(
        400,
        "Your request exceeds the service quota for your account. You can view your quotas at "
        "https://docs.aws.amazon.com/servicequotas/latest/userguide/gs-request-quota.html. "
        "You can resubmit your request later.",
        429,
        "insufficient_quota",
        "insufficient_quota",
        openai_message="You exceeded your current quota, please check your plan and billing "
        "details. For more information on this error, read the docs: "
        "https://platform.openai.com/docs/guides/error-codes/api-errors.",
    ),
}

# In-stream exception names (ConverseStream) -> the error row used for their message and for
# the OpenAI front's mid-stream `{"error": ...}` event.
STREAM_ERROR_ROWS = {
    "modelStreamErrorException": "ModelErrorException",
    "throttlingException": "ThrottlingException",
    "internalServerException": "InternalServerException",
    "serviceUnavailableException": "ServiceUnavailableException",
    "validationException": "ValidationException",
}

# Errors counted as "throttled" in the stats rather than "errors".
THROTTLE_ERRORS = frozenset({"ThrottlingException", "ModelNotReadyException"})
