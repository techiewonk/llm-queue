"""The engine of one process: caller and worker over the registry, queues and admission.

`Engine.call` registers a call (duplicate ids attach to the first run), sends it to its model's
queue and waits for the result. The worker side reads each model's priority queue first, admits
calls with one store call each, holds a refused call (and stops reading its class) until it is
granted or expires, runs granted calls with the attempt timers, retries by the refund rules and
falls back to fallback regions and other models by the registry's rules (plan §3.3 to §3.18).
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import hashlib
import hmac
import inspect
import math
import random
import time
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Iterator,
    Mapping,
    Sequence,
)
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from redis.exceptions import BusyLoadingError, OutOfMemoryError, RedisError, ResponseError
from redis.exceptions import ConnectionError as StoreConnectionError
from redis.exceptions import ReadOnlyError as StoreReadOnlyError
from redis.exceptions import TimeoutError as StoreTimeoutError

from llm_queue._logging import LogLimiter, get_logger
from llm_queue._version import __version__
from llm_queue.clients.pool import CredentialsRefreshSlow, CredentialsUnavailable, route_for
from llm_queue.clients.providers import Providers
from llm_queue.clients.timing import AttemptTimeout, Timing
from llm_queue.engine import embed as embed_codec
from llm_queue.engine import hooks, inference, shapes, tokens
from llm_queue.engine._codec import text
from llm_queue.engine.admission import (
    Admission,
    Ask,
    Candidate,
    Decision,
    owner_token,
    position_of,
)
from llm_queue.engine.envelope import (
    Envelope,
    Meta,
    Reply,
    encode_params,
    params_hash,
    parse,
)
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.expiry import TTL_SWEEP, Expiry, decide
from llm_queue.engine.fairness import Fairness, fairness_for, tenant_hash_of
from llm_queue.engine.health import Health
from llm_queue.engine.keys import Keys
from llm_queue.engine.metrics import GaugeSet, Metrics, NullSink, TenantLabels
from llm_queue.engine.offload import ResultOffload
from llm_queue.engine.purge import PurgeService, PurgeSettings
from llm_queue.engine.queue import (
    Message,
    NotReplicated,
    QueueFull,
    QueueSettings,
    StreamQueues,
)
from llm_queue.engine.requests import IndexEntry, RequestStore, Settled, extend_notice
from llm_queue.engine.requests_admin import AdminSettings, RequestsAdmin
from llm_queue.engine.status import AlertEvaluator, StatusReader
from llm_queue.engine.store_checks import UNKNOWN as UNKNOWN_POLICY
from llm_queue.engine.store_checks import check_store, eviction_policy, eviction_warning
from llm_queue.engine.store_client import StorePoolBusy
from llm_queue.engine.store_guard import WAIT_SCOPES, ReplicaWait
from llm_queue.engine.streaming import (
    ChunkReader,
    ChunkWriter,
    StreamTooLarge,
    replay_events,
    text_of,
)
from llm_queue.errors import ConfigError, ErrorKind, LLMQueueError, UnsupportedEnvelopeError
from llm_queue.ids import new_id
from llm_queue.providers.classify import (
    error_family,
    is_auth_error,
    is_target_error,
    request_error_kind,
)
from llm_queue.providers.embeddings import codec_for, request_body
from llm_queue.registry.validate import tpm_refuses
from llm_queue.tuning import EngineTimings

if TYPE_CHECKING:
    import redis.asyncio

    from llm_queue.registry import Deployment, ResolvedRegistry
    from llm_queue.registry.store import RegistryStore

logger = get_logger(__name__)
OPS = {("bedrock", "chat"): "converse", ("bedrock", "embedding"): "invoke_model"}
HEALTH_PROBE_EVERY_SECS = 5.0  # the background store probe behind readiness (RESCAN O1)
NEWER_MESSAGE_DELAY_SECS = 15.0  # a message from a newer library waits this long for a newer pod
# Store retries (send, result read, settling writes): full jitter from 0.1 s doubling to 1 s.
STORE_RETRY_BASE_SECS = 0.1
STORE_RETRY_CAP_SECS = 1.0
PROBE_EVERY_SECS = 5.0  # how often one process looks for disabled deployments due a probe
PROBE_TIMEOUT_SECS = 30.0  # one probe call's budget (first response, idle and whole call)
ALERT_EVERY_SECS = 30.0
ALERT_LOCK_SECS = 45  # longer than a tick: the holder keeps it (R3OB-1)
POLICY_EVERY_ROUNDS = 12  # the stores' eviction policy is read again each minute (R3FM-8)
HAND_BACK_FLOOR_SECS = 2.0  # `stop()` returns waiting calls within at least this (FE-6)
CLOCK_MAX_RTT_SECS = 0.25  # a TIME read slower than this keeps the old offset (R3RC-7)
CLOCK_JUMP_SECS = 2.0  # the store clock moving this much against ours is a jump (R3FM-10)
PUBSUB_PING_SECS = 5.0  # a subscriber pings this often; silent for two pings is reset (R3RC-6)
PUBSUB_SWEEP_MISSES = 3  # sweeps in a row finding unannounced results reset the reader (R3FM-12)
START_PROBE_SECS = 10.0  # each store step of `start()`: a dead store never holds the pod's start
# Request fields a fallback model may not understand (another family's knobs): dropped from the
# caller's params when the call is served by another model (RESCAN P6). The target's own
# `inference` defaults are applied after, so its own extra fields still go in.
_MODEL_SPECIFIC = ("additionalModelRequestFields",)
_TOOL_FIELDS = ("toolConfig", "tools", "tool_choice")


def _probe_params(
    provider: str, kind: str, model_id: str, family: str | None = None
) -> dict[str, Any]:
    """The default probe request: one short word, one output token."""
    if kind == "embedding":
        # The calls' own codec: the probe guessed from the id alone and sent Titan's body to
        # a Cohere model behind an ARN (registry `family`), which refused every probe.
        codec = codec_for(provider, model_id, family)
        return request_body(provider, codec, ["ping"], codec.probe_options)
    if provider == "openai":
        return {"messages": [{"role": "user", "content": "ping"}], "max_tokens": 1}
    return {
        "messages": [{"role": "user", "content": [{"text": "ping"}]}],
        "inferenceConfig": {"maxTokens": 1},
    }


def _cls(env: Envelope) -> Literal["p", "n"]:
    """A call's admission class: `p` (priority) or `n` (normal)."""
    return "p" if env.priority else "n"


def _op_of(view: ResolvedRegistry, alias: str) -> str:
    """The provider operation of a model's calls (Converse, InvokeModel, chat, embeddings)."""
    entry = view.registry.models[alias]
    return OPS.get((entry.provider, entry.type), "chat" if entry.type == "chat" else "embeddings")


def _served_op(view: ResolvedRegistry, alias: str, env: Envelope) -> str:
    """The provider operation for `alias` serving `env`: its own shape when it differs (Q40).

    `env.op` is the caller's model's shape (Converse or OpenAI chat); a fallback model of the
    other provider is sent its own, converted (`shapes`).
    """
    op = _op_of(view, alias)
    return op if shapes.converts(env.op, op) else env.op


def _packed(view: ResolvedRegistry, alias: str, response: Any) -> Any:
    """An `embed()` reply as its packed vectors (`pack_reply`, gate B97), else unchanged.

    Read with the codec of the caller's model `alias`: the reply is in its shape. Anything
    that is not a readable embedding reply is stored as it came.
    """
    entry = view.registry.models.get(alias)
    if entry is None or entry.type != "embedding" or not isinstance(response, dict):
        return response
    try:
        codec = codec_for(entry.provider, entry.model_id, entry.family)
    except ConfigError:
        return response
    return embed_codec.pack_reply(entry.provider, codec, response) or response


def _by_deployment(view: ResolvedRegistry, alias: str, name: str) -> float:
    """A setting at its most specific level, the largest over the model's deployments.

    Owner Q39: `timeout_secs` and `default_output_tokens` set on an account (or deployment)
    were never read, as both were looked up for the model alone. The deployment is not known
    when they are needed (the call's deadline, the estimate charged at admission), so the
    largest of the deployments' values is used: no deployment gets less than its own.
    """
    values = [float(view.value(alias, name, d.key)) for d in view.deployments(alias)]
    return max(values) if values else float(view.value(alias, name))


def _call_timeout_secs(view: ResolvedRegistry, alias: str, asked: float | None) -> float:
    """A call's timeout: the one asked for (else its levels'), capped at `timeout_max_secs`."""
    timeout = float(asked or _by_deployment(view, alias, "timeout_secs"))
    return min(timeout, float(view.value(alias, "timeout_max_secs")))


def _takes_calls(view: ResolvedRegistry, alias: str) -> bool:
    """True when a model is enabled with at least one enabled deployment on an enabled account."""
    if not view.value(alias, "enabled"):
        return False
    accounts = view.registry.accounts
    return any(
        view.value(alias, "enabled", d.key) and accounts[d.account].enabled
        for d in view.deployments(alias)
    )


def _dep_takes_calls(view: ResolvedRegistry, alias: str, dep: Deployment) -> bool:
    """True when a deployment may take calls: enabled, account enabled, unblocked, in residency.

    Admission's own filter (PICK_TAKE `evaluate`), less health and breakers.
    """
    routing = view.registry.routing
    return bool(
        view.value(alias, "enabled", dep.key)
        and view.registry.accounts[dep.account].enabled
        and dep.region not in routing.blocked_regions
        and dep.account not in routing.blocked_accounts
        and not view.outside_residency(dep)
    )


def shape_ok(
    view: ResolvedRegistry,
    target: str,
    op: str,
    params: Mapping[str, Any],
    *,
    stream: bool,
) -> bool:
    """Whether a fallback model can serve a call of shape `op` (Q40).

    Same shape: always. The other shape: not for a stream (its events are not converted),
    and only when the request converts (`shapes.unconvertible`); either is logged.
    """
    target_op = _op_of(view, target)
    if not shapes.converts(op, target_op):
        return True
    if stream:
        logger.info("fallback_shape_skipped", model=target, reason="stream")
        return False
    reason = shapes.unconvertible(params, op, target_op)
    if reason is not None:
        logger.info("fallback_shape_skipped", model=target, reason=reason)
        return False
    return True


def may_spill(
    view: ResolvedRegistry,
    alias: str,
    env: Envelope,
    age: float,
    cause: str,
    rule_name: str,
) -> bool:
    """Whether a call may use fallback regions / models now: rule or `fallback_policy`."""
    allowed = ("all", "regions") if rule_name == "fallback" else ("all",)
    rule = view.value(alias, rule_name)
    if env.allow_fallback not in allowed or not rule:
        return False
    policy_name = view.value(alias, "fallback_policy")
    policy = hooks.get("fallback_policy", policy_name)
    if policy is not None:
        ctx = hooks.CallContext(
            alias, env.request_id, env.priority, env.session_id, env.user_id, env.tenant_id, age
        )
        default = cause in rule["on"] and age >= float(rule["after_secs"])
        return bool(
            hooks.call_safely(
                "fallback_policy", policy_name, policy, cause, age, rule, ctx, default=default
            )
        )
    return cause in rule["on"] and age >= float(rule["after_secs"])


def request_params(
    view: ResolvedRegistry, alias: str, env: Envelope, dep: str | None
) -> dict[str, Any]:
    """The provider request for `alias` (and deployment `dep`): defaults, limits, params.

    Served by a fallback model (`alias` is not the call's model, RESCAN P6), the caller's
    model-specific fields are dropped (`additionalModelRequestFields`; tool fields too
    when the target's `supports_tools` is false) and an output above the target's limit is
    clamped, never rejected: the caller asked another model for it.
    """
    entry = view.registry.models[alias]
    params: Mapping[str, Any] = env.params or {}
    limits = view.value(alias, "inference_limits", dep) or {}
    if alias != env.model:
        dropped: tuple[str, ...] = _MODEL_SPECIFIC
        if not view.value(alias, "supports_tools"):
            dropped = (*dropped, *_TOOL_FIELDS)
        params = {k: v for k, v in params.items() if k not in dropped}
        limits = {**limits, "on_exceed": "clamp"}
    op = _served_op(view, alias, env)
    if op != env.op:
        # A fallback model of the other provider (Q40): its own request shape, its own
        # defaults and limits applied after.
        try:
            params = shapes.request(params, env.op, op)
        except shapes.ShapeError as exc:
            raise inference.InferenceRejected(f"{alias}: {exc}") from exc
    embedding = entry.type == "embedding"
    request = inference.apply(
        entry.provider,
        op,
        params,
        view.value(alias, "inference", dep) or {},
        limits,
        codec=codec_for(entry.provider, entry.model_id, entry.family) if embedding else None,
    )
    extra = view.value(alias, "params", dep) if dep else None
    return inference.merge_params(request, extra or {})


def estimate_of(
    view: ResolvedRegistry, alias: str, env: Envelope, params: Mapping[str, Any]
) -> tokens.Estimate:
    """The token estimate admission charges for `params` on model `alias`."""
    entry = view.registry.models[alias]
    return tokens.estimate(
        env.op,
        params,
        embedding=entry.type == "embedding",
        default_output_tokens=int(_by_deployment(view, alias, "default_output_tokens")),
        output_weight=float(view.value(alias, "tpm_output_weight")),
        estimator=view.value(alias, "token_estimator"),
    )


def log_outcome(
    metrics: Metrics,
    worker_id: str,
    env: Envelope,
    alias: str,
    account: str,
    region: str,
    outcome: str,
    meta: Meta | None,
    path: str,
    *,
    tenant_label: str,
    **fields: Any,
) -> None:
    """Count one finished call and log `llmq_call_finished` (every path: R3OB-5, R3OB-8).

    `path`: queue, direct, static_cap, local or dead_letter (a label of
    `llmq_calls_total`). Static-cap and dead-lettered calls were in no counter and no record.
    `tenant_label`: the call's `tenant` label (`TenantLabels`, 0.2.0 T6).
    """
    labels = {"model": alias, "account": account or "-", "region": region or "-"}
    metrics.count(
        "llmq_calls_total", 1.0, outcome=outcome, path=path, tenant=tenant_label, **labels
    )
    if meta is not None:
        metrics.count("llmq_tokens_input_total", float(meta.tokens_in or 0), **labels)
        metrics.count("llmq_tokens_output_total", float(meta.tokens_out or 0), **labels)
        if meta.fallback:
            metrics.count("llmq_fallback_total", kind=meta.fallback, model=env.model)
    fields.setdefault("deployment", meta.deployment if meta else None)
    fields.setdefault("attempts", meta.attempts if meta else None)
    logger.info(
        "llmq_call_finished",
        request_id=env.request_id,
        model=env.model,
        served_model=alias,
        outcome=outcome,
        path=path,
        priority=env.priority,
        account=account or None,
        region=region or None,
        worker=worker_id,
        fallback=meta.fallback if meta else None,
        tokens_in=meta.tokens_in if meta else None,
        tokens_out=meta.tokens_out if meta else None,
        provider_ms=meta.provider_ms if meta else None,
        user=env.user_id,  # keyed hashes already (D1; RESCAN X10)
        tenant=env.tenant_id,
        **fields,
    )


def build_envelope(
    view: ResolvedRegistry,
    model: str,
    params: Mapping[str, Any],
    *,
    caller: str,
    metrics: Metrics,
    priority: bool = False,
    timeout_secs: float | None = None,
    max_wait_secs: float | None = None,
    request_id: str | None = None,
    session_id: str | None = None,
    user_id: str | None = None,
    tenant_id: str | None = None,
    hash_id: Callable[[str], str],
    allow_fallback: str = "all",
    allowed_regions: Sequence[str] | None = None,
    allowed_accounts: Sequence[str] | None = None,
    stream: bool = False,
    stream_id: str | None = None,
    vectors: bool = False,
) -> tuple[str, Envelope]:
    """A call's model (a disabled one's fallback) and envelope, checked as every path needs.

    One copy for the queue engine and the local executor (`LLMQ_EXECUTION_MODE=local`): a
    disabled model's fallback, `max_message_bytes`, the call's timeout and the `admission`
    hook. Raises `CallFailed` for a call that cannot run. The envelope carries only keyed
    hashes (`hash_id`) of `user_id` and `tenant_id` (owner D1, 0.2.0): the message, its dead
    letter and the worker never hold a raw id; the admission hook still sees the raw ones.
    """
    alias = view.registry.alias_of(model)
    rid = request_id or stream_id or new_id()
    # The caller's shape (its model's provider), kept when a fallback model of the other
    # provider serves it: the request and reply are converted on the worker (Q40).
    op = _op_of(view, alias)
    if view.effective(alias, "enabled").level == "model" and not view.value(alias, "enabled"):
        rule = view.value(alias, "model_fallback")
        if not rule or allow_fallback != "all" or not rule["models"]:
            raise CallFailed("model_disabled", f"{alias} is disabled", request_id=rid)
        # The first target that can take calls: one disabled itself (model, every
        # deployment or every account) got them all and failed each (RESCAN R8). A target
        # of another shape needs a request that converts, and never takes a stream.
        targets = [item["model"] for item in rule["models"]]
        usable = [
            t
            for t in targets
            if t in view.registry.models
            and _takes_calls(view, t)
            and shape_ok(view, t, op, params, stream=stream)
        ]
        if not usable:
            raise CallFailed(
                "model_unavailable",
                f"{alias} is disabled and no fallback model is enabled (or none takes this "
                "request's shape)",
                request_id=rid,
            )
        alias = usable[0]
    # Here, not in the front door, so every path meets it: async and sync calls, streams,
    # embeddings, jobs and the LangChain adapters (plan C1). One orjson pass over the params,
    # kept for the message itself (FO-21: it was encoded a second time by `dumps`).
    encoded = encode_params(params)
    size, limit = len(encoded[0]), int(view.value(alias, "max_message_bytes"))
    if size > limit:
        raise CallFailed(  # one kind for every too-big call (owner Q38)
            "request_too_large",
            f"the message is {size} bytes, above max_message_bytes {limit} (raise it on the "
            "model, or send large files by reference)",
            request_id=rid,
        )
    timeout = _call_timeout_secs(view, alias, timeout_secs)
    envelope = Envelope(
        request_id=rid,
        op=op,
        model=alias,
        params=dict(params),
        priority=priority,
        caller=caller,
        timeout_secs=timeout,
        max_wait_secs=max_wait_secs or view.value(alias, "max_wait_secs"),
        allow_fallback=allow_fallback,
        allowed_regions=tuple(allowed_regions) if allowed_regions else None,
        allowed_accounts=tuple(allowed_accounts) if allowed_accounts else None,
        session_id=session_id,
        user_id=hash_id(user_id) if user_id else None,
        tenant_id=hash_id(tenant_id) if tenant_id else None,
        app_request_id=request_id is not None,
        stream=stream,
        vectors=vectors,
    )
    envelope.keep_params_json(encoded)
    envelope.keep_tenant_name(tenant_id)
    hook_name = view.value(alias, "admission")
    hook = hooks.get("admission", hook_name)
    if hook is not None:
        ctx = hooks.CallContext(alias, rid, priority, session_id, user_id, tenant_id)
        verdict = hooks.call_safely(
            "admission", hook_name, hook, ctx, default=None, metrics=metrics
        )
        if isinstance(verdict, hooks.Reject):
            metrics.count("llmq_caller_failures_total", model=alias, outcome="rejected")
            raise CallFailed("rejected", verdict.reason, request_id=rid)
    return alias, envelope


@dataclass(frozen=True, slots=True)
class EngineSettings:
    """Process-level settings (plan §3.18 infrastructure table)."""

    queue_prefix: str = ""
    lease_secs: float = 60.0
    waiter_secs: float = 2.0
    result_ttl_secs: int = 300
    result_dedupe_secs: int = 0
    status_ttl_secs: int = 1800
    caller_ttl_secs: int = 30
    poll_secs: float = 5.0  # safety-net result check (the wake-up is published with the result)
    sweep_secs: float = 10.0
    receive_batch: int = 10
    caller_margin_secs: float = 5.0
    # The local executor (engine/local.py): `local` mode and `store_down: static_cap`. Budgets
    # are each deployment's rpm / tpm divided by `local_process_count`; at most
    # `local_concurrency` calls per model and process run at once.
    local_process_count: int = 1
    local_concurrency: int = 10
    drain_secs: float = 25.0  # k8s grace 30 s: calls still running then are given back (C3)
    # LLMQ_DRAIN_HANDBACK_ENABLED (0.2.0 A1, owner Q2): a pause returns held calls at once.
    drain_handback_enabled: bool = True
    # LLMQ_ID_HASH_SALT: the key of every user / session id hash; never in repr() (X10).
    id_hash_salt: str = field(default="", repr=False)
    cfg_reload_secs: float = 30.0  # LLMQ_CFG_RELOAD_SECS: how often the registry is re-read
    queue_failover_secs: float = 5.0  # queue-store errors this long before calls run direct
    queue_wait_replicas: int = 0  # a send waits for this many replicas (0: off)
    queue_wait_ms: int = 100  # how long it waits for them
    # What the replica wait covers (owner Q8): calls, jobs, registry; all by default.
    queue_wait_scopes: frozenset[str] = frozenset(WAIT_SCOPES)
    # Strict mode (outside dev / test): an evicting store fails readiness (owner Q10).
    strict: bool = False
    debug_capture_allowed: bool = False  # LLMQ_DEBUG_CAPTURE_FROM_CONFIG (never in prod)
    key_expiry: str = "auto"  # LLMQ_KEY_EXPIRY: auto | native | swept (engine/expiry.py)
    ttl_sweep_batch: int = 500  # LLMQ_TTL_SWEEP_BATCH
    ttl_sweep_rounds: int = 100  # LLMQ_TTL_SWEEP_ROUNDS
    # A store outage seen by a call is remembered this long (owner Q11, R3FM-3): `fail_fast` and
    # `static_cap` then answer at once instead of re-trying the store (1.5-21.5 s per call).
    outage_memo_secs: float = 3.0
    timings: EngineTimings = field(default_factory=EngineTimings)  # `Tuning.engine`
    purge: PurgeSettings = field(default_factory=PurgeSettings)  # LLMQ_*PURGE*
    # The request index and admin operations (LLMQ_REQUEST_INDEX_ENABLED, LLMQ_ADMIN_*, ...).
    admin: AdminSettings = field(default_factory=AdminSettings)


# ARGV: key prefix, request ids... Reply: {the 0-based indexes of the ids with a stored result,
# those whose record is gone too (the store lost it: R3FM-4, they are sent again)}.
_RESULTS_EXIST = """
local out = {}
local gone = {}
for i = 2, #ARGV do
  local base = ARGV[1] .. ':{r:' .. ARGV[i] .. '}:'
  if redis.call('EXISTS', base .. 'res') == 1 then
    table.insert(out, i - 2)
  elseif redis.call('EXISTS', base .. 'job') == 0 then
    table.insert(gone, i - 2)
  end
end
return {out, gone}
"""


async def _close_all(steps: Sequence[tuple[str, Callable[[], Awaitable[object]]]]) -> None:
    """Run every close step, each whatever the others did; log each failure, raise the first."""
    first: BaseException | None = None
    for name, close in steps:
        try:
            await close()
        except Exception as exc:
            logger.warning(
                "close_step_failed", step=name, error=str(exc), error_type=type(exc).__name__
            )
            first = first or exc
    if first is not None:
        raise first


_ACCEPTS_API: dict[type, bool] = {}


def _api_kwargs(provider: Any, view: ResolvedRegistry, alias: str) -> dict[str, Any]:
    """`converse_api` for a provider whose `call` accepts it; nothing for one that does not.

    Owner Q33: a custom provider written for 0.1.0b1 has no such parameter, and passing it
    raised `TypeError` on every call. Checked once per provider class.
    """
    kind = type(provider)
    accepts = _ACCEPTS_API.get(kind)
    if accepts is None:
        try:
            params = inspect.signature(provider.call).parameters.values()
        except (TypeError, ValueError):
            accepts = True
        else:
            accepts = any(
                p.name == "converse_api" or p.kind is inspect.Parameter.VAR_KEYWORD for p in params
            )
        _ACCEPTS_API[kind] = accepts
    return {"converse_api": str(view.value(alias, "converse_api"))} if accepts else {}


# The marker `stream()` puts on its chunk queue when the call task ends (R3PF-11).
_CALL_DONE: dict[str, Any] = {"t": "call_done"}
# How long `stream()` waits for a failed call's error entry once the call task ended (R3ST-3).
_STREAM_GRACE_SECS = 1.0


def _call_failed(task: asyncio.Task[Reply]) -> bool:
    """True when a finished call task raised."""
    return task.done() and not task.cancelled() and task.exception() is not None


def _raise_call_failure(task: asyncio.Task[Reply], partial_text: str | None) -> None:
    """Raise a finished call task's failure, with the stream's text so far; else return."""
    if not _call_failed(task):
        return
    exc = task.exception()
    if isinstance(exc, CallFailed):
        message = str(exc).removeprefix(exc.kind).removeprefix(": ")
        raise CallFailed(
            exc.kind,
            message,
            retry_after_secs=exc.retry_after_secs,
            partial_text=exc.partial_text or partial_text,
            request_id=exc.request_id,
        ) from exc
    assert exc is not None
    raise exc


def _meta_event(meta: Mapping[str, Any], usage: Mapping[str, Any] | None) -> dict[str, Any]:
    """The library's last stream event (`meta_event=True`): usage and how it was served."""
    if usage is None:
        usage = {"inputTokens": meta.get("tokens_in"), "outputTokens": meta.get("tokens_out")}
    return {"llmq": {"usage": dict(usage), "meta": dict(meta)}}


def _seen(task: asyncio.Future[Any]) -> None:
    """Mark a finished task's exception as read (no "exception was never retrieved")."""
    if not task.cancelled():
        task.exception()


def _refusal(exc: BaseException) -> str:
    """A caller-facing message for a store that answered with a refusal."""
    if isinstance(exc, OutOfMemoryError) or "OOM" in str(exc):
        return "the state store is out of memory"  # redis-py strips the "OOM" prefix
    return f"the state store refused the call: {exc}"


# Refusals a store answers while it cannot take writes for a while: out of memory, a failed
# save (MISCONF), too few replicas under `min-replicas-to-write` (NOREPLICAS), a long script
# (BUSY), loading, no primary. Treated as the store being down (owner Q12, R3FM-7): they reached
# callers as `queue_unavailable` and threw away answers in hand.
_REFUSAL_CODES = frozenset(
    {"OOM", "MISCONF", "NOREPLICAS", "BUSY", "LOADING", "MASTERDOWN", "TRYAGAIN"}
)


def _store_refused(exc: BaseException) -> bool:
    """True for a store answer that refuses writes for a while (counted as the store down)."""
    if isinstance(exc, (OutOfMemoryError, BusyLoadingError)):
        return True
    if not isinstance(exc, ResponseError):
        return False
    words = str(exc).split(maxsplit=1)
    return bool(words) and words[0].upper() in _REFUSAL_CODES


# READONLY: a failover left this connection on the old primary (now a replica). `OSError` takes
# in the socket errors redis-py lets through and also the built-in `TimeoutError` (what an
# `asyncio.timeout` raises since 3.11): a `try` with a timer inside reads its expiry as store
# trouble, which is meant where it is used (start-up, the cancel batch). The result offload no
# longer raises into these paths (EN-3: it falls back inline), so an offload I/O error is not
# read as `store_down` (audit RT-26).
STORE_ERRORS = (
    StoreConnectionError,
    StoreTimeoutError,
    StoreReadOnlyError,
    ConnectionError,
    OSError,
)


@dataclass(slots=True)
class _Held:
    """A call this process holds (waiting for budget, or running).

    `message` is None on the direct path (kill switch, `queue_pct`, queue-store failover); its
    position is then the store's time at registration.
    """

    message: Message | None
    envelope: Envelope
    first_seen_secs: float  # the store's clock when this process took it
    position_ms: int
    tag: str
    grants: set[tuple[str, str]] = field(default_factory=set)
    meta: dict[str, dict[str, Any]] = field(default_factory=dict)
    broken: bool = False
    cancelled: bool = False
    target_failed: list[tuple[str, str]] = field(default_factory=list)
    run: asyncio.Task[None] | None = None
    hold: asyncio.Task[None] | None = None  # its `_hold` task while it waits for budget
    # Model-level request and estimate, built once per registry view (a deep copy of the
    # prompt each time: ~0.7 ms for 200 KB, and it was rebuilt in _ask, _decide and _attempt).
    charged: tuple[Any, str, tokens.Estimate] | None = None
    # The estimate each grant was charged with, by (alias, owner): refunds settle that one, not
    # one rebuilt from a registry reloaded meanwhile (RESCAN A4).
    estimates: dict[tuple[str, str], tokens.Estimate] = field(default_factory=dict)
    writer: ChunkWriter | None = None  # the running attempt's chunk writer (streams)
    shutdown: bool = False  # the drain ran out: give it back, do not fail it (plan C3)
    refused: bool = False  # an admission refusal was counted for this call (RESCAN O4)
    model_retried: bool = False  # its one free retry after a model error was used (P9)
    check_callers: bool = False  # its first admission also checks its callers are alive
    # For the per-call record of a failed call (R3OB-4): its last deployment tried and the
    # attempt number; and the store time of its first grant (R3OB-2: admission wait).
    last_dep: str = ""
    attempts: int = 0
    granted_secs: float = 0.0
    # Tenant fairness (fair_queue): the call's round per model, so a re-ask keeps its place.
    fair_rounds: dict[str, float] = field(default_factory=dict)
    tenant_waited: bool = False  # a share wait was counted for this call
    # An admin extend (0.2.0 T2): the call's new deadline, store ms (0: none seen).
    extended_ms: int = 0
    answered: bool = False  # its success is stored: a later step's error never fails it (A-1)

    @property
    def position(self) -> float:
        """Queue position as a number (direct calls: their registration time)."""
        if self.message is not None:
            return position_of(self.message.place)  # a moved call keeps its place (Q9)
        return float(self.position_ms * 1000)


@dataclass(frozen=True, slots=True)
class _AttemptResult:
    """How one provider attempt ended, for `_attempts` (audit RT-30: a stringly tuple before).

    `outcome` ok, retry, throttle, fatal, invalid or stream_failed; `kind` the error kind it
    names; `info` its detail (retry-after for a throttle, the partial text of a cut stream).
    """

    outcome: Literal["ok", "retry", "throttle", "fatal", "invalid", "stream_failed"]
    kind: str = ""
    info: Any = None
    exc: BaseException | None = None


@dataclass(frozen=True, slots=True)
class _Backoff:
    """Credentials that failed for one account and region: unused until `until_mono`.

    `until_mono` is monotonic seconds; `backoff_secs` doubles per failure, up to 60 s (RT-29:
    an unnamed `(until, backoff)` tuple before).
    """

    until_mono: float
    backoff_secs: float


class _Slots:
    """A model's worker slots: a semaphore whose free count is kept here, readable.

    `asyncio.Semaphore._value` is private and its meaning is not promised across Python
    versions (audit RT-22). `size` is fixed at start: `worker_slots` needs a restart (RT-23).
    """

    def __init__(self, size: int) -> None:
        """`size` slots, all free."""
        self.size = size
        self.free = size
        self.warned = size  # the last configured size a restart warning was logged for
        self._sem = asyncio.Semaphore(size)

    async def __aenter__(self) -> None:
        """Take a slot (waits for one)."""
        await self._sem.acquire()
        self.free -= 1

    async def __aexit__(self, *exc: object) -> None:
        """Give the slot back."""
        self.free += 1
        self._sem.release()


@dataclass(slots=True)
class _Alive:
    """When a subscriber last delivered anything (a message or a pong) and last pinged."""

    name: str
    seen_mono: float = field(default_factory=time.monotonic)
    pinged_mono: float = field(default_factory=time.monotonic)

    def reset(self) -> None:
        """A fresh subscription: alive now."""
        self.seen_mono = self.pinged_mono = time.monotonic()

    def seen(self) -> None:
        """It delivered something."""
        self.seen_mono = time.monotonic()


class _StaleSubscription(Exception):
    """A subscriber stopped delivering: reset it and subscribe again."""


class _QueueDown(Exception):
    """The queue store refused a send while the state store works: run the call direct."""


class StoreClock:
    """The state store's clock, from one `TIME` read and the local monotonic clock."""

    def __init__(self) -> None:
        """No offset until `sync`."""
        self._offset = 0.0
        self._synced = False

    async def sync(self, client: redis.asyncio.Redis) -> str | None:
        """Measure the offset between the store's clock and this process.

        Returns None, `slow` (the read took too long: the old offset is kept) or `jump` (the
        store's clock moved against the local one). The midpoint of a read that took 6-15 s
        (a failover's retries, a loop stall) put the clock seconds ahead for a whole beat:
        calls were settled `rate_limited` before their deadline (R3RC-7, R3FM-10).
        """
        before = time.monotonic()
        secs, micros = await client.time()
        after = time.monotonic()
        offset = (secs + micros / 1e6) - (before + after) / 2
        if not self._synced:
            self._offset, self._synced = offset, True
            return None
        if after - before > CLOCK_MAX_RTT_SECS:
            return "slow"
        moved = abs(offset - self._offset) > CLOCK_JUMP_SECS
        self._offset = offset
        return "jump" if moved else None

    def now(self) -> float:
        """Seconds on the store's clock."""
        return time.monotonic() + self._offset


# `llmq_cancels_total` sources; any other value is counted as `other` (bounded labels).
CANCEL_SOURCES = frozenset({"api", "caller_task", "fail_fast", "admin", "cli", "http"})


def _where(view: ResolvedRegistry, alias: str, dep_key: str) -> tuple[str, str]:
    """A deployment's (account, region) for metric labels; ("-", "-") when unknown."""
    if dep_key and alias in view.registry.models:
        for dep in view.deployments(alias):
            if dep.key == dep_key:
                return dep.account, dep.region
    return "-", "-"


class Engine:
    """Caller and worker of one process (plan E72: workers run in the app pods)."""

    def __init__(
        self,
        state: redis.asyncio.Redis,
        registry: RegistryStore,
        providers: Providers,
        *,
        queue_store: redis.asyncio.Redis | None = None,
        settings: EngineSettings | None = None,
        worker_id: str | None = None,
        metrics: Metrics | None = None,
        offload: ResultOffload | None = None,
        offload_bytes: int | None = None,
    ) -> None:
        """Wire the stores; nothing runs until `start`."""
        self.settings = settings or EngineSettings()
        # Two heartbeats at least (owner Q4): one late or lost renewal no longer makes a live
        # caller look gone (R3FM-1, R3RC-3). Send and attach refresh it too (REGISTER).
        self.caller_ttl_secs = max(
            self.settings.caller_ttl_secs, math.ceil(2 * self.settings.lease_secs / 3) + 5
        )
        self.keys = Keys(registry.prefix)
        self.state = state
        self.registry = registry
        self.providers = providers
        self._queue_store = queue_store
        self.metrics: Metrics = metrics or NullSink()
        registry.metrics = self.metrics  # reload failures are counted too (CO-4)
        self.queues = StreamQueues(queue_store or state, self.keys)
        self.replica_wait = ReplicaWait(
            replicas=self.settings.queue_wait_replicas,
            wait_ms=self.settings.queue_wait_ms,
            scopes=self.settings.queue_wait_scopes,
        )
        self._planned: dict[tuple[str, str], int] | None = None  # Bedrock pool sizes (Q43)
        self._replid: str | None = None  # the state store's `master_replid` last seen (R3FM-8)
        self.requests = RequestStore(
            state,
            self.keys,
            result_ttl_secs=self.settings.result_ttl_secs,
            result_dedupe_secs=self.settings.result_dedupe_secs,
            status_ttl_secs=self.settings.status_ttl_secs,
            caller_ttl_secs=self.caller_ttl_secs,
            lease_secs=self.settings.lease_secs,
            offload=offload,
            offload_bytes=offload_bytes,
        )
        self.admission = Admission(
            state,
            self.keys,
            lease_secs=self.settings.lease_secs,
            waiter_secs=self.settings.waiter_secs,
            timings=self.settings.timings,
            metrics=self.metrics,
        )
        # An app's id is a prefix plus a random part (owner Q6): two processes started with one
        # id (prefork workers given the pod name) shared every per-process key, and a second
        # delivery to the sibling looked like this process's own (R3RC-9).
        tail = new_id()[-14:]  # the random tail: the head is time
        self.worker_id = f"{worker_id}-{tail[-6:]}" if worker_id else tail
        self.clock = StoreClock()
        self.purge = PurgeService(  # `LLMQueue.purge`; auto purge runs in the sweep loop
            state,
            registry,
            queue_client=queue_store,
            queue_prefix=self.settings.queue_prefix,
            settings=self.settings.purge,
            hash_id=self.hash_id,
            metrics=self.metrics,
            status_ttl_secs=self.settings.status_ttl_secs,
            owner=self.worker_id,
        )
        # The admin side of requests (0.2.0 T2): list, count, get, cancel, reprioritize, extend
        # by model and tenant; `llmq.admin.requests` wraps it.
        self.requests_admin = RequestsAdmin(
            state,
            queue_client=queue_store,
            keys=self.keys,
            registry=registry,
            queue_prefix=self.settings.queue_prefix,
            hash_id=self.hash_id,
            settings=self.settings.admin,
            status_ttl_secs=self.settings.status_ttl_secs,
            caller_margin_secs=self.settings.caller_margin_secs,
            metrics=self.metrics,
            owner=self.worker_id,
        )
        # The `tenant` label of llmq_calls_total / llmq_wait_seconds (0.2.0 T6, owner Q10).
        self._tenant_label = TenantLabels(self.settings.admin.metrics_tenants, self.hash_id)
        self._tasks: list[asyncio.Task[None]] = []
        self._running: set[asyncio.Task[None]] = set()
        self._held: dict[str, _Held] = {}
        self._held_class: set[tuple[str, str]] = set()
        self._slots: dict[str, _Slots] = {}
        self._waiters: dict[str, asyncio.Future[None]] = {}
        # Waited-on ids whose record the store lost (R3FM-4, R3RC-2): their callers send again.
        self._gone: set[str] = set()
        self._sweep_now = asyncio.Event()  # wakes the result sweep (an epoch change)
        # Readers to reset: their subscription stopped delivering (R3RC-6, R3FM-12).
        self._stale_readers: set[str] = set()
        self._sweep_misses = 0  # sweeps in a row that found results pub/sub did not announce
        self._cancel_misses = 0  # cancel checks in a row that found cancels never announced
        self._pubsub: Any = None
        self._cancels: Any = None
        self._stopping = False
        self._closed = False  # `stop()` cancelled the background tasks: start no new one
        self._paused = False
        self._draining_since: float | None = None  # wall time of the pause (0.2.0 A3)
        self._handing_back: asyncio.Task[None] | None = None  # a pause's hand-back (A1)
        self._own_loop: asyncio.AbstractEventLoop | None = None  # the loop `start()` ran on
        self._started = False
        self._local_callers: dict[str, int] = {}  # request id -> coroutines waiting on it here
        # Admin extends of calls waited on here (0.2.0 T2): request id -> the new deadline as
        # this loop's time, and the timers of the waiting calls to reschedule.
        self._extended_until: dict[str, float] = {}
        self._call_timeouts: dict[str, set[asyncio.Timeout]] = {}
        self._wake: dict[str, asyncio.Event] = {}  # model -> wakes its receive loop
        self._beat_ok_mono = time.monotonic()  # last heartbeat that reached the store
        self._store_trouble_mono = -1e9  # monotonic time this process last saw the store fail
        self._outage_until_mono = -1e9  # a call saw the store down: skip it until then (Q11)
        # A separate queue store a send found down: calls run direct until then (R3FM-3).
        self._queue_down_until_mono = -1e9
        self._store_ping: asyncio.Task[bool] | None = None  # pings for paused calls
        self._store_waiting = 0  # calls paused on the store_down path, waiting on the probe
        # Calls whose awaiting task was cancelled, for the one canceller (`_cancel_later`).
        self._cancel_queue: list[tuple[str, asyncio.Future[None]]] = []
        self._canceller: asyncio.Task[None] | None = None
        self._settling: set[asyncio.Task[None]] = set()  # shutdown give-backs (`_run`)
        # Per model: the claim (key, nonce) of a receive whose reply was lost (S11).
        self._unclaimed: dict[str, tuple[str, str]] = {}
        self._results_exist = state.register_script(_RESULTS_EXIST)
        # Key expiry of each store (owner K4): native TTLs or the index, set at start.
        # Until a publish succeeds: what the setting asks for, native for `auto` (a process
        # reported `swept` and ready while forced native on a volatile store; rescan B3).
        first = self.settings.key_expiry if self.settings.key_expiry != "auto" else "native"
        self.expiry = Expiry(self.keys.prefix, self.clock.now, first)
        self.queue_expiry = self.expiry
        if queue_store is not None:
            self.queue_expiry = Expiry(self.keys.prefix, self.clock.now, first)
        self._ttl_sweeps = [(self.expiry, state.register_script(TTL_SWEEP))]
        if queue_store is not None:
            self._ttl_sweeps.append((self.queue_expiry, queue_store.register_script(TTL_SWEEP)))
        self.ready = False
        self._direct_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self.health = Health(state, self.keys)
        self.status_reader = StatusReader(
            state,
            self.keys,
            self.queues,
            self.settings.queue_prefix,
            requests=self.requests_admin,
            request_index=self.settings.admin.request_index,
            scan_max=self.settings.admin.scan_max,
        )
        self.alerts = AlertEvaluator(state, self.keys)
        # Each part writes expiries in its store's mode (owner K4; set again at start).
        self.requests.expiry = self.alerts.expiry = self.purge.expiry = self.expiry
        self.queues.expiry = self.queue_expiry
        # Gauges this process sets for the cluster while it holds the alert lock; cleared when
        # another process holds it, so no pod's /metrics keeps a stale value (R3OB-1).
        self._cluster = GaugeSet(self.metrics)
        self._limited = LogLimiter()  # outage log lines: one per event and key a while (R3OB-7)
        self._loop_died: set[str] = set()  # background loops already logged as died (R3OB-20)
        # Credentials that failed in this process, per account and region: (account, region)
        # -> (until, backoff). Per region: a token can be refused in one region only (an
        # opt-in region such as ap-south-2 refuses v1 STS tokens), and keying by account took
        # every region of it down (scenario P5).
        self._bad_accounts: dict[tuple[str, str], _Backoff] = {}
        self._idle = asyncio.Event()
        from llm_queue.engine.local import LocalExecutor, LocalLimits  # imports this module

        # `store_down: static_cap` runs here, on the executor of local mode (0.2.0).
        self.local = LocalExecutor(
            providers,
            LocalLimits(
                process_count=self.settings.local_process_count, timings=self.settings.timings
            ),
            view=lambda: self.registry.current,
            concurrency=self.settings.local_concurrency,
            worker_id=self.worker_id,
            metrics=self.metrics,
            hash_id=self.hash_id,
            tenant_label=self._tenant_label,
            # Read at each call, so the engine's own `_params` (and a test's stand-in) serves.
            params=lambda view, alias, env, dep: self._params(view, alias, env, dep),
        )
        self._rng = random.Random()
        self.chunks = ChunkReader(
            state,
            block_ms=self.settings.timings.stream_read_block_ms,
            wake_key=f"{self.keys.prefix}:chunkwake:{self.worker_id}",
            expiry=self.expiry,
        )
        # Call tasks of streams already read to their `end`: they finish here (result read,
        # cleanup) while the caller has moved on.
        self._tails: set[asyncio.Task[Reply]] = set()
        self._epoch: str | None = None  # the store's epoch this process last saw (RESCAN A2)
        self._worker = False
        self._model_tasks: dict[str, asyncio.Task[None]] = {}  # receive loop per model alias
        self._reload: asyncio.Task[Any] | None = None  # the one registry reload in flight (R10)
        self._stores: tuple[float, dict[str, str]] | None = None  # cached store probe (O1)
        self._beat_ran_mono = time.monotonic()  # the heartbeat loop last made progress (O2)
        # Each store's `maxmemory-policy`, read at start (plan C4): in `health_check`.
        self._eviction: dict[str, str] = {}
        self._newer_warned_mono = -1e9  # last warning about a message from a newer library (C5)

    # ================================================================ lifecycle

    async def start(self, *, worker: bool = True, caller: bool = True) -> None:
        """Boot the registry, then start the worker loops and the result reader.

        Each store step at start is bounded (`START_PROBE_SECS`): a blackholed store held
        `start()` for connect timeouts times retries (41 s per command) before the pod could
        report ready, and a refusal (out of memory) raised redis-py's own error (audit RT-13).
        """
        try:
            async with asyncio.timeout(START_PROBE_SECS):
                await self.registry.boot()  # runs on its own config when the store is down
        except TimeoutError:
            logger.error("registry_store_unreachable_at_start", error="timeout")
            self.registry.run_on_config()
        try:
            async with asyncio.timeout(START_PROBE_SECS):
                await self.clock.sync(self.state)
                # Cluster mode is refused (`ConfigError`); an evicting store is a warning (C4).
                setting = self.settings.key_expiry
                self._eviction["state"] = await check_store(self.state, "state", setting)
                await self.state.set(self.keys.epoch, str(self.clock.now()), nx=True)
                self._epoch = text(await self.state.get(self.keys.epoch))
                await self.requests.check_offload()  # `ConfigError` without it (Q24)
        except (*STORE_ERRORS, RedisError) as exc:  # the timeout is a TimeoutError (OSError)
            # Start anyway (plan §3.18): calls follow each model's `store_down` rule and
            # the background loops keep retrying until the store answers.
            logger.error(
                "state_store_unreachable_at_start", error=str(exc), error_type=type(exc).__name__
            )
        if self._queue_store is not None:
            # After the state steps, with its own time limit: a queue store that hung at start
            # skipped the epoch and offload checks above (review B3-2, rescan).
            try:
                async with asyncio.timeout(START_PROBE_SECS):
                    self._eviction["queue"] = await check_store(
                        self._queue_store, "queue", self.settings.key_expiry
                    )
            except (*STORE_ERRORS, RedisError) as exc:
                logger.error(
                    "queue_store_unreachable_at_start",
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
        # Last and never raising: the minute re-read publishes the mode when this one fails.
        await self._publish_expiry_safely()
        # Every role: a worker-only process still makes calls (a job that embeds, a tool), and
        # with no reader each waited for the `poll_secs` (5 s) safety net (docs review 0.1.1).
        self._pubsub = self.state.pubsub()
        self._loop(self._result_reader(), "result_reader")
        self._loop(self._result_sweep(), "result_sweep")
        if worker:
            self._worker = True
            self._sync_model_loops()
            self._loop(self._sweeper(), "sweeper")
            self._loop(self._prober(), "prober")
            self._loop(self._alert_loop(), "alerts")
            self._cancels = self.state.pubsub()
            self._loop(self._cancel_reader(), "cancel_reader")
            self._loop(self._warm_clients(), "warm_clients")
        self._loop(self._heartbeat(), "heartbeat")
        # Every process, its own lock: callers write indexed keys too, and sharing the worker
        # sweep's lock let callers starve the dead-owner and lease sweep (rescan B3-R2).
        self._loop(self._ttl_sweeper(), "ttl_sweeper")
        self._loop(self._store_prober(), "store_prober")
        self._loop(self.requests.check_lifecycle(), "offload_lifecycle")  # Q23
        self._loop(self.registry.run(self.settings.cfg_reload_secs), "registry_reload")
        self._own_loop = asyncio.get_running_loop()
        self._started = True
        self.ready = not self._paused  # a pause before the start holds

    def _loop(self, work: Coroutine[Any, Any, None], name: str) -> None:
        """Start a background loop, named: `stop_task_failed` logged `Task-123` (FE-19)."""
        task = asyncio.create_task(work, name=f"llmq:{name}")
        task.add_done_callback(functools.partial(self._loop_ended, name))
        self._tasks.append(task)

    def _loop_ended(self, name: str, task: asyncio.Task[None]) -> None:
        """A background loop ended: an error is logged and counted now (R3OB-20).

        Liveness turned false with no line saying why: only model loops were logged, and the
        others only at `stop()`.
        """
        if self._stopping or task.cancelled() or task.exception() is None:
            return
        self._loop_died.add(name)
        self.metrics.count("llmq_loop_died_total", loop=name)
        logger.error("background_loop_died", loop=name, error=repr(task.exception()))

    def _sync_model_loops(self) -> None:
        """One receive loop per model of the current registry (RESCAN R3).

        Called at start and on every heartbeat: a model a newer registry added gets its loop
        (before, loops started only at start); a removed model's loop ends by itself.
        """
        if not self._worker or self._stopping:
            return
        try:
            self._replan_pools()
        except Exception as exc:  # the pools keep their size; calls go on
            logger.warning("pool_replan_failed", error=str(exc), error_type=type(exc).__name__)
        view = self.registry.current
        for alias in view.registry.models:
            wanted = int(view.value(alias, "worker_slots"))
            slots = self._slots.get(alias)
            if slots is None:
                self._slots[alias] = _Slots(wanted)
            elif slots.size != wanted and slots.warned != wanted:
                # Set once per process: a live edit changed nothing, and nothing said so (RT-23).
                slots.warned = wanted
                logger.warning(
                    "worker_slots_changed_needs_restart",
                    model=alias,
                    running=slots.size,
                    configured=wanted,
                )
            task = self._model_tasks.get(alias)
            if task is not None and not task.done():
                continue
            if task is not None and not task.cancelled() and task.exception() is not None:
                logger.error("model_loop_died", model=alias, error=repr(task.exception()))
                self.metrics.count("llmq_loop_died_total", loop="model")
            self._model_tasks[alias] = asyncio.create_task(
                self._model_loop(alias), name=f"llmq:model:{alias}"
            )

    def pause_receiving(self) -> None:
        """Not ready, and take no new calls from the queues; running calls go on.

        The first step of a shutdown, for a preStop hook: the load balancer stops sending
        requests while this pod finishes its work. With `drain_handback_enabled` (default on,
        `LLMQ_DRAIN_HANDBACK_ENABLED`) calls held here waiting for budget go back to the queue
        at once, for another pod (0.2.0 A1). Sync and callable from any thread: the hand-back
        is scheduled on the engine's loop, and `stop()` waits for it.
        """
        self.ready = False
        if self._draining_since is None:
            self._draining_since = time.time()
        self._paused = True
        loop = self._own_loop
        if not (self._worker and self.settings.drain_handback_enabled) or loop is None:
            return  # a caller holds no queued calls; an engine never started has none
        with contextlib.suppress(RuntimeError):  # the loop is closed: nothing to hand back
            loop.call_soon_threadsafe(self._start_hand_back)

    def _start_hand_back(self) -> None:
        """On the engine's loop: return the held calls in one tracked task (`stop()` awaits it)."""
        if self._stopping or self._closed or not self._paused:
            return  # stopping hands them back itself; a resume came first
        if self._handing_back is not None and not self._handing_back.done():
            return
        until = asyncio.get_running_loop().time() + HAND_BACK_FLOOR_SECS
        self._handing_back = asyncio.create_task(
            self._hand_back_held(until), name="llmq:drain_hand_back"
        )

    def resume_receiving(self) -> None:
        """Undo `pause_receiving`: receive again, and ready when started (0.2.0 A2).

        Callable from any thread, as the pause is (A-8). `LLMQueueError` once `stop()` began:
        a stopped engine cannot take calls again.
        """
        if self._stopping or self._closed:
            raise LLMQueueError("this engine is stopped; it cannot resume receiving")
        self._paused = False
        self._draining_since = None
        self.ready = self._started
        loop = self._own_loop
        if loop is None:
            return  # never started: no receive loop waits
        # The loops' wake-up events belong to the engine's loop: set from another thread, the
        # wake-up was lost (asyncio debug raised).
        with contextlib.suppress(RuntimeError):  # the loop is closed
            loop.call_soon_threadsafe(self._wake_receivers)

    def _wake_receivers(self) -> None:
        """On the engine's loop: wake the receive loops waiting out a pause."""
        for wake in self._wake.values():
            wake.set()

    @property
    def draining(self) -> bool:
        """True between `pause_receiving()` and `resume_receiving()`."""
        return self._paused

    @property
    def draining_since(self) -> float | None:
        """Wall time (Unix seconds) of the pause, None when not draining."""
        return self._draining_since

    def drain_status(self) -> dict[str, Any]:
        """What `GET /drain` answers: draining, since, held (waiting for budget) and running."""
        held = list(self._held.values())
        return {
            "draining": self._paused,
            "since": self._draining_since,
            "held": sum(1 for h in held if h.run is None),
            "running": sum(1 for h in held if h.run is not None and not h.run.done()),
        }

    async def wait_running(self, timeout_secs: float) -> bool:
        """Wait up to `timeout_secs` for the hand-back and every running call; True when idle."""
        loop = asyncio.get_running_loop()
        until = loop.time() + max(0.0, timeout_secs)
        handing = self._handing_back
        if handing is not None and not handing.done():
            await asyncio.wait({handing}, timeout=max(0.0, until - loop.time()))
        if self._running:
            await asyncio.wait(set(self._running), timeout=max(0.0, until - loop.time()))
        return not self._running and (handing is None or handing.done())

    async def stop(self, drain_secs: float | None = None) -> None:
        """Shut down in order (plan §3.18): not ready, stop receiving, finish running calls.

        Held calls go back to the queue; results keep being read for this pod's own callers
        until they finish or the drain ends; then everything stops.
        """
        drain = self.settings.drain_secs if drain_secs is None else drain_secs
        loop = asyncio.get_running_loop()
        until = loop.time() + drain
        self.ready = False
        self._stopping = True
        handing = self._handing_back
        if handing is not None and not handing.done():
            # A pause's hand-back first (A1); it runs under its own floor deadline.
            await asyncio.gather(handing, return_exceptions=True)
        await self._hand_back_held(until)
        if self._running:
            await asyncio.wait(set(self._running), timeout=max(0.0, until - loop.time()))
        # The drain ran out (plan C3): calls still running are given back, not failed: grants
        # refunded, message returned at once (another pod runs it); a stream that already sent
        # chunks ends with `worker_shutdown` (it cannot be resumed elsewhere).
        late = [h.run for h in list(self._held.values()) if h.run is not None and not h.run.done()]
        for held in list(self._held.values()):
            if held.run is not None and not held.run.done():
                held.shutdown = True
                held.run.cancel()
        if late:
            await asyncio.wait(set(late), timeout=5.0)
        if self._settling:  # the give-backs of those runs, before the clients close (RT-6)
            await asyncio.wait(set(self._settling), timeout=5.0)
        if self._tails:  # streams already delivered: their call tasks read the result
            await asyncio.wait(set(self._tails), timeout=max(0.0, until - loop.time()))
        if self._waiters:
            self._idle.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._idle.wait(), max(0.0, until - loop.time()))
        tasks: list[asyncio.Task[Any]] = [
            *self._tasks,
            *self._model_tasks.values(),
            *self._running,
            *self._tails,
            *self._settling,
        ]
        for extra in (self._store_ping, self._reload):
            if extra is not None:
                tasks.append(extra)
        for task in tasks:
            task.cancel()
        # `gather` lets a cancel of `stop()` itself through (a k8s hook timing out) and keeps
        # each result: a loop that died with an error is logged, not swallowed (audit RT-7).
        self._closed = True
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for task, result in zip(tasks, results, strict=True):
            if isinstance(result, Exception):
                logger.warning(
                    "stop_task_failed",
                    task=task.get_name(),
                    error=str(result),
                    error_type=type(result).__name__,
                )
        await self._reclaim_lost()
        self._flush_metrics()
        closes: list[tuple[str, Callable[[], Awaitable[object]]]] = [
            ("chunk_reader", self.chunks.aclose)
        ]
        for name, pubsub in (("result_pubsub", self._pubsub), ("cancel_pubsub", self._cancels)):
            if pubsub is not None:
                closes.append((name, pubsub.aclose))
        await _close_all(closes)  # each on its own, the first failure raised at the end (CO-1)

    def _flush_metrics(self) -> None:
        """At stop, write the points a buffering sink still holds (`EmfSink` adds up a second).

        Only the default stdout writer flushes at exit; a sink given its own `write` kept the
        last second of points until the next point, which never came after a stop.
        """
        flush = getattr(self.metrics, "flush", None)
        if not callable(flush):
            return
        try:
            flush()
        except Exception as exc:  # a failing sink never fails the stop
            logger.warning("stop_metrics_flush_failed", error=str(exc))

    async def _reclaim_lost(self) -> None:
        """At stop, put back what receives with a lost reply took (FE-8, FL-3): time-bound.

        Left alone, those calls stayed hidden for the visibility time and were charged a
        failure, as the next round of the receive loop never came.
        """
        lost, self._unclaimed = list(self._unclaimed.values()), {}
        if not lost:
            return
        try:
            async with asyncio.timeout(self.settings.timings.health_probe_secs):
                await asyncio.gather(*(self.queues.reclaim(*claim) for claim in lost))
        except Exception as exc:  # their visibility timeout brings them back
            logger.warning("stop_reclaim_failed", claims=len(lost), error=str(exc))

    async def _hand_back_held(self, until: float) -> None:
        """Return every call held while waiting for budget, at once and by the drain's end.

        Its hold task is stopped first: it ran one more round, re-admitted the returned call
        and ran it at the provider while the message was already back (audit RT-5). The returns
        run together under the drain's deadline: one at a time, a down store held `stop()` for
        connect timeouts and retries per message, far past `drain_secs` (audit RT-7).
        """
        handed = [
            h
            for h in list(self._held.values())
            if not h.grants and h.run is None and h.message is not None
        ]
        for held in handed:
            # Dropped, not cancelled: its hold sees that within a second and ends. A cancel
            # inside its admission lost a grant the store had made (FE-4, RV-N1).
            self._drop(held)  # else the heartbeat hides it again for the whole drain
        messages = [h.message for h in handed if h.message is not None]
        if not messages:
            return
        # At least a short floor: with `drain_secs=0` (or a grace already spent) the deadline
        # had passed, nothing was returned and every waiting call was hidden for its whole
        # visibility time and charged a failure (FE-6).
        loop = asyncio.get_running_loop()
        try:
            async with asyncio.timeout_at(max(until, loop.time() + HAND_BACK_FLOOR_SECS)):
                await self._release_claims(handed)
                results = await asyncio.gather(
                    *(self.queues.return_(m) for m in messages), return_exceptions=True
                )
        except TimeoutError:
            logger.warning("stop_return_timeout", calls=len(messages))
            return
        failed = [r for r in results if isinstance(r, BaseException)]
        if failed:  # their visibility timeout brings them back
            logger.warning("stop_return_failed", calls=len(failed), error=str(failed[0]))

    async def aclose(self) -> None:
        """Close the provider clients and both store clients (after `stop()`).

        Every step runs whatever the one before did: a provider close that raised left both
        store clients open (CO-1). Each failure is logged and the first is raised at the end.
        """
        steps: list[tuple[str, Callable[[], Awaitable[object]]]] = [
            ("providers", self.providers.aclose)
        ]
        if self._queue_store is not None:
            steps.append(("queue_store", self._queue_store.aclose))
        steps.append(("state_store", self.state.aclose))
        await _close_all(steps)

    # ================================================================ caller

    def _queue_name(self, view: ResolvedRegistry, alias: str, priority: bool) -> str:
        name = self.settings.queue_prefix + view.queue_name(alias)
        return f"{name}-priority" if priority else name

    async def call(
        self,
        model: str,
        params: Mapping[str, Any],
        *,
        priority: bool = False,
        timeout_secs: float | None = None,
        max_wait_secs: float | None = None,
        request_id: str | None = None,
        session_id: str | None = None,
        user_id: str | None = None,
        tenant_id: str | None = None,
        allow_fallback: str = "all",
        allowed_regions: Sequence[str] | None = None,
        allowed_accounts: Sequence[str] | None = None,
        force_direct: bool = False,
        stream: bool = False,
        stream_id: str | None = None,
        vectors: bool = False,
    ) -> Reply:
        """Run one call through the queue and return its reply; `CallFailed` on failure.

        `force_direct` runs it in this process (sync calls), with the same admission.
        `stream` has the worker write events to the chunk stream of `stream_id` (`stream`).
        `vectors` (from `embed()`) lets an embedding reply come back as its packed vectors.
        """
        view = self.registry.current
        alias, envelope = build_envelope(
            view,
            model,
            params,
            caller=self.worker_id,
            metrics=self.metrics,
            priority=priority,
            timeout_secs=timeout_secs,
            max_wait_secs=max_wait_secs,
            request_id=request_id,
            session_id=session_id,
            user_id=user_id,
            tenant_id=tenant_id,
            hash_id=self.hash_id,
            allow_fallback=allow_fallback,
            allowed_regions=allowed_regions,
            allowed_accounts=allowed_accounts,
            stream=stream,
            stream_id=stream_id,
            vectors=vectors,
        )
        rid, timeout = envelope.request_id, envelope.timeout_secs
        direct = force_direct or self._direct_route(view, alias, rid, user_id)
        try:
            with self._caller_ref(rid):
                return await self._call_in(
                    view, alias, envelope, params, priority, timeout, request_id, direct, stream
                )
        except CallFailed as exc:
            # Every failure a caller sees, whether a worker took the call or not: queue_full,
            # queue_unavailable and caller deadlines were in no counter, so llmq_calls_total
            # fell during an outage and read as "traffic fell" (R3OB-5).
            self.metrics.count("llmq_caller_failures_total", model=alias, outcome=str(exc.kind))
            raise

    def _shape_ok(
        self,
        view: ResolvedRegistry,
        target: str,
        op: str,
        params: Mapping[str, Any],
        *,
        stream: bool,
    ) -> bool:
        """`shape_ok`: whether a fallback model can serve a call of shape `op`."""
        return shape_ok(view, target, op, params, stream=stream)

    @contextlib.contextmanager
    def _caller_ref(self, rid: str) -> Iterator[None]:
        """Count a coroutine waiting on `rid` here; the last one out drops the shared wake-up.

        One place for the pair: the count was kept in `call()` and the wake-up dropped in
        `_call_once`'s `finally`, which relied on `call()` not having decremented yet (RT-32).
        Other coroutines here may wait on the same id and share its wake-up: removing it left
        them asleep until their deadline's last read (one waiter cancelled).
        """
        self._local_callers[rid] = self._local_callers.get(rid, 0) + 1
        try:
            yield
        finally:
            left = self._local_callers.get(rid, 1) - 1
            if left:
                self._local_callers[rid] = left
            else:
                self._local_callers.pop(rid, None)
                self._waiters.pop(rid, None)
                self._extended_until.pop(rid, None)
            if not self._waiters:
                self._idle.set()

    async def _call_in(
        self,
        view: ResolvedRegistry,
        alias: str,
        envelope: Envelope,
        params: Mapping[str, Any],
        priority: bool,
        call_secs: float,
        request_id: str | None,
        direct: bool,
        stream: bool,
    ) -> Reply:
        """`call()` from its envelope on: one deadline, the store_down rule, cancel cleanup."""
        rid = envelope.request_id
        # One deadline for the whole call, whatever path it takes: a store outage sent calls
        # down the store_down path with a fresh timeout, and the soak saw 135 answers come
        # after their deadline (20 s store block, 30 s calls).
        deadline = asyncio.get_running_loop().time() + call_secs + self.settings.caller_margin_secs
        timer: asyncio.Timeout | None = None
        try:
            async with asyncio.timeout_at(deadline) as timer:
                # An admin extend moves this timer (`_extend_caller`, 0.2.0 T2).
                self._call_timeouts.setdefault(rid, set()).add(timer)
                self._apply_extend(rid, timer)
                if (
                    self._store_waiting
                    and not stream
                    and view.value(alias, "store_down") == "pause"
                ):
                    # Calls here already wait for the store to come back: this one joins them
                    # instead of trying first. Each try opened up to four connections to a
                    # store that refused them (redis-py's retries), the rest of the storm.
                    return await self._store_down(
                        view, alias, envelope, None, deadline, direct=direct
                    )
                mode = view.value(alias, "store_down")
                if not stream and mode != "pause" and self._outage_remembered():
                    # A store outage seen moments ago (owner Q11): `fail_fast` and `static_cap`
                    # answer now; each call re-discovered it at the client's full retry budget.
                    return await self._store_down(
                        view, alias, envelope, None, deadline, direct=direct
                    )
                try:
                    return await self._call_routed(
                        view, alias, envelope, params, priority, call_secs, request_id, direct
                    )
                except StorePoolBusy as exc:
                    # Every pooled connection was in use: the process is busy, the store is
                    # up. Read as store_down it paused or ran calls outside their budgets and
                    # set the outage memo for everyone (R3PF-6).
                    self.metrics.count("llmq_store_pool_busy_total", model=alias)
                    raise CallFailed(
                        "rate_limited", str(exc), retry_after_secs=1.0, request_id=rid
                    ) from exc
                except STORE_ERRORS as exc:
                    self._remember_outage()
                    if stream:  # chunks travel through the state store
                        message = "the state store is unreachable"
                        raise CallFailed("queue_unavailable", message, request_id=rid) from exc
                    return await self._store_down(
                        view, alias, envelope, exc, deadline, direct=direct
                    )
                except ResponseError as exc:
                    if _store_refused(exc) and not stream:
                        # NOREPLICAS, OOM, MISCONF, BUSY: the store is down for writes (Q12).
                        self._remember_outage()
                        return await self._store_down(
                            view, alias, envelope, exc, deadline, direct=direct
                        )
                    # The store answered but refused (a script error): still one error family
                    # for callers. A refused registration raised redis-py's error straight to
                    # the app (scenario S9).
                    raise CallFailed("queue_unavailable", _refusal(exc), request_id=rid) from exc
        except TimeoutError as exc:
            raise CallFailed("deadline_exceeded", "no reply in time", request_id=rid) from exc
        except asyncio.CancelledError:
            # The awaiting task was cancelled: cancel the call for every caller of its id, here
            # and in other processes (owner Q3: cancel wins).
            held = self._held.get(rid)
            if held is not None and held.message is None:
                # Its direct run is here: stopped now. In a caller-only process nothing reads
                # the cancel channel, and it ran on to its own deadline (FE-17).
                held.cancelled = True
                task = held.run or held.hold
                if task is not None and not task.done():
                    task.cancel()
            # Waited for until the deadline at most, then it goes on behind: a slow store
            # (its client's retries) held the cancellation up to 1.9 s past the deadline
            # (scenario L4: a deadline cancelling the call retried on the store_down path).
            cleanup = self._cancel_later(rid)
            remaining = deadline - asyncio.get_running_loop().time()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait({cleanup}, timeout=max(0.0, remaining))
            raise
        finally:
            timers = self._call_timeouts.get(rid)
            if timers is not None and timer is not None:
                timers.discard(timer)
                if not timers:
                    self._call_timeouts.pop(rid, None)

    def _apply_extend(self, rid: str, timer: asyncio.Timeout) -> None:
        """Move a waiting call's timer to its extended deadline, when one was announced."""
        until = self._extended_until.get(rid)
        when = timer.when()
        if until is not None and when is not None and until > when:
            timer.reschedule(until)

    def _extend_caller(self, rid: str, deadline_ms: int) -> None:
        """An admin extended a call waited on here: move its timers, wake it to re-read.

        `deadline_ms` is the worker's new deadline (store ms); the caller waits its margin
        more, as it did for the first deadline.
        """
        if rid not in self._local_callers:
            return
        loop = asyncio.get_running_loop()
        until = loop.time() + deadline_ms / 1000 - self.clock.now()
        until += self.settings.caller_margin_secs
        if until <= self._extended_until.get(rid, 0.0):
            return
        self._extended_until[rid] = until
        for timer in self._call_timeouts.get(rid, ()):
            self._apply_extend(rid, timer)
        waiter = self._waiters.get(rid)
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def _caller_until(self, rid: str, until: float) -> float:
        """A waiting call's deadline (this loop's time), an admin extend included."""
        return max(until, self._extended_until.get(rid, until))

    async def stream(
        self, model: str, params: Mapping[str, Any], *, meta_event: bool = False, **call: Any
    ) -> AsyncIterator[dict[str, Any]]:
        """Run a chat call and yield the provider's events as they arrive (plan §3.13, F6).

        Takes `call`'s options. The events are ConverseStream events (Bedrock) or chat
        completion chunks (OpenAI-compatible). A failure after events were yielded raises
        `CallFailed` with `partial_text`; no switch to another deployment happens then.
        `meta_event=True` ends a complete stream with one library event, `{"llmq": {"usage":
        ..., "meta": ...}}`: tokens, served model, deployment, fallback, attempts (R3ST-9).
        """
        view = self.registry.current
        alias = view.registry.alias_of(model)
        kind = view.registry.models[alias].type
        if kind != "chat":  # it returned nothing at all before (RESCAN S11)
            # A failed call, not bad config: `ConfigError` is for start-up and refused registry
            # edits (CONTRIBUTING, audit RT-28).
            raise CallFailed(
                "invalid_request",
                f"{alias} is an {kind} model; stream() needs a chat model",
                request_id=call.get("request_id"),
            )
        idle = float(view.value(alias, "stream_idle_secs"))
        flush_secs = int(view.value(alias, "stream_flush_ms")) / 1000
        # The worker gives up a stalled provider after `idle` and then writes its error entry
        # (2+ round trips): waiting the same `idle` here raced it, and the caller got a bare
        # `deadline_exceeded` with no `partial_text` about half the time (R3ST-1).
        idle_wait = idle + flush_secs + self.settings.caller_margin_secs
        app_id = call.pop("request_id", None)  # an app id keeps its app-id behaviour (S11)
        rid = app_id or new_id()
        key = self.keys.request(rid, "chunks")
        try:
            queue = self.chunks.open(key)
        except ValueError:  # both starved before (R3ST-8)
            raise CallFailed(
                "invalid_request",
                "a stream with this request id is already being read in this process",
                request_id=rid,
            ) from None
        # The call's own deadline, as `call` computes it: the idle wait between chunks
        # (stream_idle_secs, 30) once let a stream whose worker died answer after it (scenario
        # L2 at 300 %: 38.8 s for a 15 s deadline).
        timeout = _call_timeout_secs(view, alias, call.get("timeout_secs"))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout + self.settings.caller_margin_secs
        task = asyncio.create_task(
            self.call(model, params, stream=True, stream_id=rid, request_id=app_id, **call)
        )
        # The call's end is a marker on the queue: one plain `get` per entry, where a Task and
        # an `asyncio.wait` per entry cost 42 us each (R3PF-11).
        task.add_done_callback(lambda _: queue.put_nowait(_CALL_DONE))
        expected, attempt_id, ended, finished = 1, "", False, False
        texts: list[str] = []  # what was yielded: `partial_text` of the failures raised here

        def failed(kind: str, message: str) -> CallFailed:
            return CallFailed(kind, message, partial_text="".join(texts) or None, request_id=rid)

        try:
            while True:
                try:
                    entry = queue.get_nowait()  # already read: a slow consumer gets it (S2)
                except asyncio.QueueEmpty:
                    left = deadline - loop.time()
                    if left <= 0:
                        raise failed("deadline_exceeded", "no reply in time") from None
                    if finished and _call_failed(task):
                        # Its error entry, if any, is on the way: a short grace, not the idle
                        # time (a cancel waited 30 s, R3ST-3).
                        wait = _STREAM_GRACE_SECS + flush_secs
                    elif finished or expected > 1:
                        wait = idle_wait
                    else:
                        wait = left
                    try:
                        async with asyncio.timeout(min(wait, left)):
                            entry = await queue.get()
                    except TimeoutError:
                        if deadline - loop.time() <= 0:
                            raise failed("deadline_exceeded", "no reply in time") from None
                        if finished:
                            _raise_call_failure(task, "".join(texts) or None)
                            raise failed(
                                "worker_lost", "the stream ended without its end entry"
                            ) from None
                        raise failed(
                            "deadline_exceeded", f"no chunk for {idle:g} s (stream_idle_secs)"
                        ) from None
                if entry is _CALL_DONE:
                    finished = True
                    if expected == 1:  # nothing read yet
                        # The store may be down too: then nothing can be replayed, and the
                        # call's own failure is the answer (scenario L2: this read raised
                        # redis-py's ConnectionError to the app).
                        try:
                            pending = await self.state.xlen(key)
                        except STORE_ERRORS:
                            task.result()  # the call's failure, if it failed
                            raise CallFailed(
                                "queue_unavailable",
                                "the store is unreachable; the stream cannot be read",
                                request_id=rid,
                            ) from None
                        if not pending:
                            reply = task.result()  # raises the call's failure (nothing streamed)
                            # A finished call whose chunks expired: an app id streamed again
                            # after `END_TTL_SECS` ended with no event and no error (R3ST-6).
                            events = replay_events(reply.response)
                            if events is None:
                                raise CallFailed(
                                    "result_expired",
                                    "the stream's chunks expired",
                                    request_id=rid,
                                )
                            ended = True
                            for event in events:
                                yield event
                            if meta_event:
                                yield _meta_event(reply.meta.model_dump(), None)
                            return
                    continue  # its last entries are still on the way
                if entry["seq"] < expected and entry["att"] == attempt_id:
                    continue  # the same entry read twice (a re-read): not a gap (S11)
                if entry["seq"] != expected:
                    raise failed("worker_lost", f"gap in the stream at {expected}")
                expected += 1
                attempt_id = entry["att"]
                if entry["t"] == "chunk":
                    for event in entry["d"]:
                        if text := text_of(event):
                            texts.append(text)
                        yield event
                elif entry["t"] == "end":
                    # Delivered in full: the caller goes on now; the call task (its result read
                    # and cleanup, ~80 ms more) finishes in the background, and a failure after
                    # `end` (the re-run found the stream taken) is logged, never raised (S11).
                    ended = True
                    if meta_event:
                        data = entry["d"]
                        yield _meta_event(data.get("meta") or {}, data.get("usage"))
                    return
                else:
                    data = entry["d"]
                    raise CallFailed(
                        data.get("kind", "provider_error"),
                        data.get("message", ""),
                        partial_text=data.get("partial") or "".join(texts) or None,
                        request_id=rid,
                    )
        finally:
            self.chunks.close(key)
            if ended:
                self._tails.add(task)
                task.add_done_callback(self._tail_done)
            else:
                if not task.done():
                    task.cancel()  # the call's own handler cancels the request
                # The call's failure is this stream's to see, never raised here; `asyncio.wait`
                # lets the consumer's own cancel through. `suppress(BaseException)` swallowed
                # it, and the wait (the call's cancel cleanup, up to the deadline) could not be
                # cut short (audit RT-4).
                task.add_done_callback(_seen)
                await asyncio.wait({task})

    def _tail_done(self, task: asyncio.Task[Reply]) -> None:
        """A stream's call task ended after its `end` was read: log a failure, never raise it."""
        self._tails.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.warning(
                "stream_call_failed_after_end",
                request_id=getattr(exc, "request_id", None),
                error=str(exc),
                error_type=type(exc).__name__,
            )

    def hash_id(self, value: str) -> str:
        """Keyed hash (HMAC-SHA256, `LLMQ_ID_HASH_SALT`) of a user or session id, 16 hex chars.

        Every id outside the message itself is stored and logged only as this (plan §3.17):
        status records, sticky keys, logs and `routing.allow_users`. Unkeyed, a low-entropy id
        (a client code, a phone number) is brute-forced back from the hash.
        """
        key = self.settings.id_hash_salt.encode()
        return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:16]

    def _direct_route(
        self, view: ResolvedRegistry, alias: str, rid: str, user_id: str | None
    ) -> bool:
        """True when this call runs direct: kill switch, or outside `queue_pct` (canary)."""
        routing = view.registry.routing
        if user_id and self.hash_id(user_id) in routing.allow_users:
            return False
        if routing.kill_switch:
            return True
        pct = int(view.value(alias, "queue_pct"))
        return pct < 100 and int(hashlib.sha256(rid.encode()).hexdigest()[:8], 16) % 100 >= pct

    async def _call_routed(
        self,
        view: ResolvedRegistry,
        alias: str,
        envelope: Envelope,
        params: Mapping[str, Any],
        priority: bool,
        call_secs: float,
        request_id: str | None,
        direct: bool,
    ) -> Reply:
        """`_call_once`, run direct when the separate queue store refuses the send.

        Its own `try`, so the callers' store handlers also see the direct run's errors: run
        inside an `except _QueueDown` handler, a store error there reached the app as a raw
        redis-py error and skipped the model's `store_down` rule (audit RT-2).
        """
        if not direct and not self.queues_shared and self._queue_down_remembered():
            # The separate queue store was found down moments ago (R3FM-3): run direct now.
            # Each call re-discovered it at the client's full retry budget (about 21 s when
            # the store is blackholed) before it took the direct path.
            direct = True
        try:
            return await self._call_once(
                view, alias, envelope, params, priority, call_secs, request_id, direct
            )
        except _QueueDown:
            return await self._call_once(
                view, alias, envelope, params, priority, call_secs, request_id, True
            )

    async def _call_once(
        self,
        view: ResolvedRegistry,
        alias: str,
        envelope: Envelope,
        params: Mapping[str, Any],
        priority: bool,
        call_secs: float,
        request_id: str | None,
        direct: bool,
    ) -> Reply:
        rid = envelope.request_id
        user_hash, session_id = envelope.user_id, envelope.session_id  # the user id is hashed
        record: dict[str, Any] = {
            "caller": self.worker_id,
            "user_hash": user_hash or self.hash_id(session_id or ""),
            "params_hash": params_hash({"m": alias, "p": params}),
            "model": alias,
            "priority": priority,
            "app_id": request_id is not None,
        }
        # Queues in the state store: register, send and confirm are one script (3 round trips
        # before). Not with `queue_wait_replicas`: that send needs its own WAIT.
        combined = not direct and self.queues_shared and not self.replica_wait.replicas_for("calls")
        index = self._index_entry(envelope, alias, priority, call_secs)

        async def register() -> tuple[str, bool]:
            """The registration's outcome, and whether the call was sent with it.

            `conflict` and `expired` raise here, for the first registration and a later one
            alike: after a re-registration only `conflict` was checked, so an `expired` id
            waited for a result that was gone until its deadline (audit RT-17).
            """
            outcome, sent = await registered()
            if outcome == "conflict":
                raise CallFailed(
                    "request_id_conflict", "the id was used with other params", request_id=rid
                )
            if outcome == "expired":
                raise CallFailed("result_expired", "the id finished earlier", request_id=rid)
            if outcome == "cancelled":  # cancel wins for every caller of the id (owner Q3)
                raise CallFailed("cancelled", "the call was cancelled", request_id=rid)
            return outcome, sent

        async def registered() -> tuple[str, bool]:
            if not combined:
                return (await self.requests.register(rid, **record)).outcome, False
            name = self._queue_name(view, alias, priority)
            settings = self._queue_settings(view, alias)  # its own retention (Q27)
            try:
                found = await self.requests.register_send(
                    rid,
                    **record,
                    queue=name,
                    queue_keys=self.queues.queue_keys(name),
                    bell=self.queues.bell(name),
                    body=envelope.dumps(),
                    max_waiting=settings.max_waiting,
                    retention_secs=settings.retention_secs,
                    keep_secs=call_secs,
                    index=index,
                )
            except QueueFull as exc:
                raise CallFailed(
                    "queue_full", str(exc), retry_after_secs=5, request_id=rid
                ) from exc
            return found.outcome, found.outcome in ("new", "takeover")

        outcome, sent = await register()
        waiter = self._waiters.setdefault(rid, asyncio.get_running_loop().create_future())
        loop = asyncio.get_running_loop()
        until = loop.time() + call_secs + self.settings.caller_margin_secs
        while True:
            run: asyncio.Task[None] | None = None  # the direct run, watched while waiting
            if outcome in ("new", "takeover") and not sent:
                if direct:
                    await self.requests.confirm(rid, keep_secs=call_secs, direct=True, index=index)
                    run = self._start_direct(envelope)
                else:
                    await self._send(rid, envelope, view, alias, priority, call_secs, index)
            reply = await self._wait_result(
                rid,
                waiter,
                self._caller_until(rid, until) - loop.time(),
                recheck=outcome == "attached",
                watch=run,
                # A stored result is read at once: `done` waited for a wake-up nobody sent, up
                # to the sweep's `poll_secs` (R3RC-5).
                read_first=outcome in ("attached", "done"),
            )
            if reply is not None:
                return reply
            # The send this caller attached to never happened: register again.
            outcome, sent = await register()

    def _index_entry(
        self, envelope: Envelope, alias: str, priority: bool, call_secs: float
    ) -> IndexEntry | None:
        """Where the call goes in the request index of its model and tenant (T1); None when off.

        Written by the script that confirms the call (no round trip of its own), removed by
        the one that writes its final state. The tenant is the call's tenant only (never the
        user id, unlike fairness): `-` for none.
        """
        admin = self.settings.admin
        if not admin.request_index:
            return None
        tenant = self._index_tenant_hash(envelope)
        keys: tuple[str, ...] = (
            self.keys.request_index(alias, tenant),
            self.keys.tenant_index(alias),
        )
        name = ""
        if admin.tenant_names and tenant != "-" and envelope.tenant_name:
            # Q5: the name stays out of the message; only this opt-in key holds it.
            name = envelope.tenant_name
            keys += (self.keys.tenant_names(alias),)
        return IndexEntry(
            keys=keys,
            tenant_hash=tenant,
            keep_ms=int((call_secs + self.settings.caller_margin_secs) * 1000),
            cls="p" if priority else "n",
            name=name,
        )

    def _index_tenant_hash(self, envelope: Envelope) -> str:
        """The call's tenant as the index names it: its keyed hash, or `-` for none."""
        return envelope.tenant_id or "-"  # already the keyed hash (D1)

    async def embed(
        self,
        model: str,
        texts: Sequence[str],
        *,
        priority: bool = False,
        timeout_secs: float | None = None,
        input_type: str | None = None,
        **call: Any,
    ) -> list[list[float]]:
        """One vector per text, in order (batched by the model's `embed_batch_size`)."""
        from llm_queue.engine.embed import embed

        return await embed(
            self,
            model,
            texts,
            priority=priority,
            timeout_secs=timeout_secs,
            input_type=input_type,
            **call,
        )

    async def _send(
        self,
        rid: str,
        envelope: Envelope,
        view: ResolvedRegistry,
        alias: str,
        priority: bool,
        keep_secs: float,
        index: IndexEntry | None = None,
    ) -> None:
        """Put the message on its queue and confirm the record; undo the record on failure.

        A separate queue store that keeps failing for `queue_failover_secs` sends the call
        down the direct path; shorter blips are retried. With `queue_wait_replicas`, a send
        that too few replicas acknowledged in time stays sent (owner Q9): warned at most once
        a minute and counted. It ran direct in the caller before, its copy bouncing on the
        queue as a duplicate every 30 s.
        """
        settings = self._queue_settings(view, alias)  # its own retention_secs (Q27)
        loop = asyncio.get_running_loop()
        give_up = loop.time() + self.settings.queue_failover_secs
        body = envelope.dumps()
        tries = 0
        # Each try against a separate queue store is bounded by what is left of
        # `queue_failover_secs` (with 0, one store probe's time): one send to a blackholed
        # store took the client's whole budget, about 21 s, before the failover (R3FM-3). A
        # shared store keeps the client's own timeouts (the call has no other path).
        floor_secs = self.settings.timings.health_probe_secs
        message_id = ""  # unknown when too few replicas acknowledged (the send stands)
        while True:
            try:
                try:
                    bound = None
                    if not self.queues_shared:
                        left = give_up - loop.time()
                        bound = left if left > 0 else floor_secs
                    async with asyncio.timeout(bound):
                        message_id = await self.queues.send(
                            self._queue_name(view, alias, priority),
                            body,
                            settings,
                            wait_replicas=self.replica_wait.replicas_for("calls"),
                            wait_ms=self.replica_wait.wait_ms,
                        )
                    break
                except STORE_ERRORS:  # a try past its bound too (TimeoutError is an OSError)
                    if not self.queues_shared and loop.time() < give_up:
                        tries += 1
                        await asyncio.sleep(self._store_retry_secs(tries))
                        continue
                    raise
            except NotReplicated as exc:
                # On the primary, not on enough replicas: sent anyway (owner Q9).
                self.replica_wait.short("calls", self.metrics, str(exc))
                break
            except BaseException as exc:
                # One cleanup for every failure, cancels included: four handlers awaited the
                # abandon bare, so its own store error replaced the original (a cancel turned
                # into a store error) and a cancel waited out the client's retries (RT-9).
                await self._abandon(rid)
                if isinstance(exc, QueueFull):
                    raise CallFailed(
                        "queue_full", str(exc), retry_after_secs=5, request_id=rid
                    ) from exc
                if isinstance(exc, ResponseError) and (
                    isinstance(exc, OutOfMemoryError) or "OOM" in str(exc)
                ):
                    raise CallFailed("queue_unavailable", _refusal(exc), request_id=rid) from exc
                if isinstance(exc, STORE_ERRORS) and not self.queues_shared:
                    self._queue_down_until_mono = time.monotonic() + self.settings.outage_memo_secs
                    logger.warning(
                        "queue_store_down", error=str(exc), error_type=type(exc).__name__
                    )
                    raise _QueueDown from exc
                raise
        try:
            await self.requests.confirm(
                rid, keep_secs=keep_secs, index=index, message_id=message_id
            )
        except STORE_ERRORS as exc:  # the message is on the queue: never run it a second time
            logger.warning("confirm_failed", request_id=rid, error=str(exc))

    @property
    def queues_shared(self) -> bool:
        """True when queues live in the state store (one store for both)."""
        return self._queue_store is None

    async def cancel(self, request_id: str, *, source: str = "api") -> None:
        """Cancel a call for every caller of its id: a waiting one never starts, running ones end.

        Anyone may cancel any id (owner Q5); each cancel is logged with its `source`. One
        pipelined round trip (two before, FE-21).
        """
        await self._cancel_batch([request_id], source=source)

    def _cancel_later(self, rid: str) -> asyncio.Future[None]:
        """Queue the cancel of a call its awaiting task gave up on; resolved once tried.

        One canceller per process serves every such call, in batches: each cancelled caller
        started its own cleanup task (up to three store calls, no timeout, failures hidden), so
        apps cancelling many calls during a store outage made one connection storm per call
        (audit RT-3).
        """
        done = asyncio.get_running_loop().create_future()
        if self._closed:
            # After `stop()`'s last gather: a new canceller would write to closing clients,
            # tracked by nobody (RV-N3). The worker settles the call by its deadline.
            logger.warning("cancel_after_stop", request_id=rid)
            done.set_result(None)
            return done
        self._cancel_queue.append((rid, done))
        task = self._canceller
        if task is None or task.done():
            task = self._canceller = asyncio.create_task(self._cancel_worker())
            self._running.add(task)
            task.add_done_callback(self._running.discard)
        return done

    async def _cancel_worker(self) -> None:
        """Cancel the queued calls, a batch at a time, each batch bounded by the probe timer.

        A batch that fails or runs out of time is logged and dropped: the worker settles each
        of its calls by the call's deadline anyway.
        """
        while self._cancel_queue:
            batch, self._cancel_queue = self._cancel_queue, []
            try:
                async with asyncio.timeout(self.settings.timings.health_probe_secs):
                    await self._cancel_batch([rid for rid, _ in batch])
            except Exception as exc:
                logger.warning(
                    "cancel_cleanup_failed",
                    calls=len(batch),
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
            finally:
                for _, done in batch:
                    if not done.done():
                        done.set_result(None)

    async def _cancel_batch(self, rids: Sequence[str], *, source: str = "caller_task") -> None:
        """Cancel each call for every caller of its id: one pipelined round trip per 500.

        Cancel wins (owner Q3). It cancelled only when no other caller was registered, read in
        one round trip and acted on in a second: two pods waiting on one id never cancelled it,
        and an attach between the two round trips was cancelled unasked (R3RC-1). A pending
        cancel now also refuses an attach (REGISTER answers `cancelled`).
        """
        for rid in rids:
            logger.info("call_cancel", request_id=rid, source=source, by=self.worker_id)
        # The label is bounded: `source` is an argument of the public `cancel()` (owner Q5).
        label = source if source in CANCEL_SOURCES else "other"
        self.metrics.count("llmq_cancels_total", float(len(rids)), source=label)
        for start in range(0, len(rids), 500):
            await self.requests.cancel(
                rids[start : start + 500],
                ttl_secs=self.settings.status_ttl_secs,
                channel=self.keys.cancel_channel,
            )

    async def _cancel_reader(self) -> None:
        """Stop running attempts whose call was cancelled (by any process)."""
        subscribed = False
        alive = _Alive("cancel_reader")
        while True:
            try:
                if not subscribed:
                    await self._cancels.subscribe(self.keys.cancel_channel)
                    subscribed = True
                    alive.reset()
                    # Cancels published while unsubscribed were missed: read the keys (C9).
                    await self._check_cancels()
                await self._keep_alive(self._cancels, alive)
                message = await self._cancels.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
            except Exception as exc:
                logger.warning("cancel_reader_error", error=str(exc))
                subscribed = False
                with contextlib.suppress(Exception):  # silent: the line above said why
                    await self._cancels.reset()
                await asyncio.sleep(1)
                continue
            if not message:
                continue
            alive.seen()
            if message.get("type") == "pong":
                continue
            data = message["data"]
            rid = data.decode() if isinstance(data, bytes) else str(data)
            extended = extend_notice(rid)
            if extended is not None:  # an admin extend (0.2.0 T2), not a cancel
                held = self._held.get(extended[0])
                if held is not None:
                    held.extended_ms = max(held.extended_ms, extended[1])
                continue
            held = self._held.get(rid)
            if held is None:
                continue
            # Flagged even before its run task started: a cancel read between the grant and
            # the task's first step was dropped, and the attempt ran to its end unless the
            # heartbeat's key check (a third of the lease) caught it first. Cancelled once: a
            # second cancel landed in `_run`'s cleanup and killed it before the reply was written.
            already = held.cancelled
            held.cancelled = True
            if not already and held.run is not None and not held.run.done():
                held.run.cancel()

    async def _check_cancels(self) -> None:
        """Stop running calls whose cancel key is set: one MGET for all (plan C9).

        The safety net behind the cancel channel: a cancel published while the subscriber was
        reconnecting was lost, and its call ran to the end.
        """
        running = [h for h in list(self._held.values()) if h.run is not None and not h.run.done()]
        if not running:
            return
        keys = [self.keys.request(h.envelope.request_id, "cancel") for h in running]
        flags = await self.state.mget(keys)
        missed = False
        for held, flag in zip(running, flags, strict=True):
            if flag is None or held.cancelled:  # not cancelled, or cancelled already (once)
                continue
            if held.run is not None and not held.run.done():
                missed = True  # the channel never announced it
                held.cancelled = True
                held.run.cancel()
        self._cancel_misses = self._cancel_misses + 1 if missed else 0
        if self._cancel_misses >= PUBSUB_SWEEP_MISSES:
            self._cancel_misses = 0
            self._reader_stale("cancel_reader")

    async def _store_down(
        self,
        view: ResolvedRegistry,
        alias: str,
        envelope: Envelope,
        exc: BaseException | None,
        deadline: float,
        *,
        direct: bool = False,
    ) -> Reply:
        """The state store is unreachable: the model's `store_down` rule (plan §3.11).

        `deadline` is the call's own (loop time, margin included): `pause` waits only until
        then, never a fresh timeout from the moment the store failed. `direct` is the call's
        own route, kept for the retry. `exc` None: a `pause` call that joined calls already
        waiting for the store, without trying it (not logged: it saw no error).
        """
        mode = view.value(alias, "store_down")
        if exc is not None:
            self._limited(
                logger.warning, "store_down", alias, model=alias, mode=mode, error=str(exc)
            )
        if mode == "static_cap":
            reply = await self._static_cap(view, alias, envelope)
            if exc is not None:
                # The failed register / send may have reached the store (a read timeout): its
                # queued copy then meets this answer (`done`) instead of running a second time
                # outside every budget (R3FM-2). Written behind, through the outage.
                self._spawn(
                    self._through_outage(
                        lambda: self.requests.put_result(reply, final_state="done")
                    ),
                    "static_cap_result",
                )
            return reply
        if mode == "pause":
            loop = asyncio.get_running_loop()
            until = deadline - self.settings.caller_margin_secs
            app_id = envelope.request_id if envelope.app_request_id else None
            while loop.time() < until:
                if not await self._store_back(until - loop.time()):
                    break
                # Spread the paused calls over up to half a second: all resumed in the tick the
                # probe saw the store back, a herd on a store just recovering (audit RT-14).
                await asyncio.sleep(self._rng.uniform(0.0, min(0.5, until - loop.time())))
                left = until - loop.time()
                if left <= 0:
                    break
                # The same request id and every option of the call, with only the time left (at
                # least 1 s, as before, ran the retry past the deadline). A new id ran the call
                # twice when the failed send had reached the store and only its reply was cut
                # (restart drill S11); with the same id the retry attaches to that send. A
                # fresh `call()` also dropped allowed_regions / accounts, fallback and direct.
                retry = envelope.model_copy(update={"timeout_secs": left})
                try:
                    return await self._call_routed(
                        view, alias, retry, retry.params, retry.priority, left, app_id, direct
                    )
                except STORE_ERRORS as again:  # out of reach again: pause again
                    exc = again
                    self._limited(
                        logger.warning, "store_down", alias, model=alias, mode=mode, error=str(exc)
                    )
                except ResponseError as refused:  # as `call()`: one error family (S9)
                    if not _store_refused(refused):
                        raise CallFailed(
                            "queue_unavailable", _refusal(refused), request_id=retry.request_id
                        ) from refused
                    exc = refused  # down for writes (Q12): pause again
                    self._limited(
                        logger.warning, "store_down", alias, model=alias, mode=mode, error=str(exc)
                    )
        elif exc is not None:
            # `fail_fast` after a failure that may have queued the call (a read timeout): it is
            # cancelled behind, so the app's retry does not make a second run (R3FM-2).
            rid = envelope.request_id
            self._spawn(
                self._through_outage(lambda: self._cancel_batch([rid], source="fail_fast")),
                "fail_fast_cancel",
            )
        # A refusal says what it was (out of memory...): the app's fix differs (S9, Q12).
        message = _refusal(exc) if exc is not None and _store_refused(exc) else None
        raise CallFailed(
            "queue_unavailable",
            message or "the state store is unreachable",
            request_id=envelope.request_id,
        ) from exc

    def _outage_remembered(self) -> bool:
        """True while a store outage a call saw is remembered (owner Q11)."""
        return time.monotonic() < self._outage_until_mono

    def _queue_down_remembered(self) -> bool:
        """True while a separate queue store a send found down is remembered (R3FM-3)."""
        return time.monotonic() < self._queue_down_until_mono

    def _remember_outage(self) -> None:
        """Remember a store outage for `outage_memo_secs`; a probe that succeeds ends it."""
        if not self._outage_remembered():
            self.metrics.count("llmq_outage_memo_set_total")
        self._outage_until_mono = time.monotonic() + self.settings.outage_memo_secs

    async def _store_back(self, secs: float) -> bool:
        """True once the state store answers again; False when `secs` pass first.

        Every call paused on the `store_down` path waits on one probe of this process (a ping
        a second). A ping per paused call made N pings a second, each a new connection while
        the store refused them: at 300 % load (scenario X1) about 600 connects a second through
        the store proxy, half of them these pings, and the pool's connections went to them.
        `asyncio.wait`: a probe stopped with the engine is "not back", never a cancel raised in
        a caller's task.
        """
        probe = self._store_ping
        if probe is None or probe.done():
            probe = self._store_ping = asyncio.create_task(self._ping_until_back())
        self._store_waiting += 1
        try:
            await asyncio.wait({probe}, timeout=max(0.0, secs))
        finally:
            self._store_waiting -= 1
        return probe.done() and not probe.cancelled() and probe.result()

    async def _ping_until_back(self) -> bool:
        """Probe every second until the store takes writes (True) or no call waits (False).

        The bounded write probe of readiness, not a PING: a read-only replica answers PING, so
        during a failover every paused call was released on a store that still refused writes
        and paused again each second (FE-7). An error answer (loading, auth) is not back either.
        """
        while True:
            await asyncio.sleep(1.0)
            if not self._store_waiting:
                return False
            if (await self._probe_stores())["state"] == "ok":
                return True

    async def _static_cap(self, view: ResolvedRegistry, alias: str, envelope: Envelope) -> Reply:
        """Run without the store, on the local executor (engine/local.py, path `static_cap`).

        The same per-process budgets, concurrency cap, retries and fallbacks as
        `LLMQ_EXECUTION_MODE=local`: each rpm / tpm divided by `LLMQ_LOCAL_PROCESS_COUNT`, at
        most `LLMQ_LOCAL_CONCURRENCY` calls per model in this process.
        """
        return await self.local.run(view, alias, envelope, path="static_cap")

    def _start_direct(self, env: Envelope) -> asyncio.Task[None]:
        """Start a direct run; its caller watches the task (`_wait_result`, `watch`)."""
        task = asyncio.create_task(self._direct(env))
        self._running.add(task)
        task.add_done_callback(self._direct_done)
        return task

    def _direct_done(self, task: asyncio.Task[None]) -> None:
        """A direct run ended: log a failure (its caller raises it; read here, never lost)."""
        self._running.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.warning("direct_run_failed", error=str(exc), error_type=type(exc).__name__)

    @staticmethod
    def _direct_failure(task: asyncio.Task[None], rid: str) -> CallFailed | None:
        """The failure a finished direct run leaves its caller, as one `CallFailed`; None if none.

        A direct run that raised wrote no reply: its caller waited to its deadline and got
        `deadline_exceeded`, never the real error (audit RT-1).
        """
        if not task.done() or task.cancelled() or (exc := task.exception()) is None:
            return None
        return Engine._as_failure(exc, rid)

    @staticmethod
    def _as_failure(exc: BaseException, rid: str) -> CallFailed:
        """A direct run's error as the one `CallFailed` its callers get."""
        if isinstance(exc, CallFailed):
            exc.request_id = exc.request_id or rid
            return exc
        if isinstance(exc, ResponseError):
            message = _refusal(exc)
        elif isinstance(exc, (*STORE_ERRORS, RedisError)):
            message = "the state store is unreachable"
        else:
            message = f"the direct run failed ({type(exc).__name__})"
        failure = CallFailed("queue_unavailable", message, request_id=rid)
        failure.__cause__ = exc
        return failure

    async def _direct(self, env: Envelope) -> None:
        """Run a call in this process (no queue), with the same admission and waiter order.

        One waiter per process, model and class registers in the store; the others wait here.
        """
        held = _Held(
            message=None,
            envelope=env,
            first_seen_secs=self.clock.now(),
            position_ms=int(self.clock.now() * 1000),
            tag=f"d{new_id()[-10:]}",  # random tail (a v7 id starts with the time)
        )
        held.hold = asyncio.current_task()  # stopped by its caller's cancel (FE-17)
        self._held[env.request_id] = held
        alias = env.model
        cls = _cls(env)
        lock = self._direct_locks.setdefault((alias, cls), asyncio.Lock())
        try:
            async with lock:
                decision = await self._direct_wait(held)
            if decision is not None:
                await self._run(held, decision, 1, alias)
        except asyncio.CancelledError:
            if held.run is None and held.grants:
                # Cancelled while its admission was in flight (A-4): the grant goes back now,
                # not a lease later (its slot) or never (its budget).
                self._spawn(self._release(held), "cancel_give_back")
            raise
        except Exception as exc:
            if held.answered:
                # A step after the stored success raised (A-1): the caller reads the success;
                # a failure reply here told it the call failed.
                logger.warning(
                    "direct_run_after_result_failed",
                    request_id=env.request_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                return
            # The failure is also stored, best effort and time-bound: it wrote no reply, so
            # callers attached from other processes waited to their deadline, and a retry of the
            # id attached to the dead record for 30 min (FE-5).
            failure = self._as_failure(exc, env.request_id)
            reply = self._spawn(
                self._reply(env, failure.kind, str(failure).removeprefix(f"{failure.kind}: ")),
                "direct_failure_reply",
            )
            await asyncio.wait({reply}, timeout=self.settings.timings.health_probe_secs)
            raise
        finally:
            if not held.grants:
                self._drop(held)

    async def _direct_wait(self, held: _Held) -> Decision | None:
        """Admission for a direct call until granted (the decision) or settled (None)."""
        env = held.envelope
        alias = env.model
        while True:
            view = self.registry.current
            now = self.clock.now()
            if await self._expire_if_due(held, now):
                return None
            waited = now - held.position_ms / 1000.0
            decision = await self._decide(view, alias, held, 1, frozenset())
            if decision is None:
                return None
            if decision.kind == "granted":
                return decision
            if await self._model_fallback(view, alias, held, 1, waited):
                return None
            await asyncio.sleep(min(max(decision.wait_secs, 0.01), 1.0))

    def _rearm(self, rid: str, done: asyncio.Future[None]) -> asyncio.Future[None]:
        """The id's next wake-up after `done` fired: made by the first waiter to see it, shared.

        Each waiter making its own replaced the others', so all but the last slept until a
        recheck (two coroutines waiting on one id in one process).
        """
        current = self._waiters.get(rid)
        if current is None or current is done:
            current = asyncio.get_running_loop().create_future()
            self._waiters[rid] = current
        return current

    async def _wake_or_fail(
        self,
        rid: str,
        waiter: asyncio.Future[None],
        secs: float,
        watch: asyncio.Task[None] | None,
    ) -> asyncio.Task[None] | None:
        """Wait up to `secs` for the wake-up or the end of the watched direct run.

        Raises the direct run's failure (audit RT-1); returns the run still to watch (None once
        it ended well: its reply is in the store). `asyncio.wait` never cancels the shared
        waiter, as `wait_for(shield(...))` did not.
        """
        pending = {waiter} if watch is None else {waiter, watch}
        await asyncio.wait(pending, timeout=max(0.0, secs), return_when=asyncio.FIRST_COMPLETED)
        if watch is None or not watch.done():
            return watch
        failure = self._direct_failure(watch, rid)
        if failure is not None:
            raise failure
        return None

    async def _wait_result(
        self,
        rid: str,
        waiter: asyncio.Future[None],
        secs: float,
        *,
        recheck: bool = False,
        watch: asyncio.Task[None] | None = None,
        read_first: bool = False,
    ) -> Reply | None:
        """The reply; None when the call's record disappeared (its caller sends it again).

        `watch`: this call's direct run; its failure is raised at once (it writes no reply).
        `recheck` (attached): the record is checked on each wake-up, the first time after the
        takeover grace. Any caller's record found gone by the sweep returns None too: at least
        once (owner D1), a store that lost its data no longer leaves callers to their deadline.
        """
        loop = asyncio.get_running_loop()
        until = loop.time() + secs
        first_check = loop.time() + self.requests.takeover_grace_secs + 1.0
        if not read_first:
            # Just sent: the reply cannot be there yet. Wait for the wake-up (or one poll)
            # before the first read; reading at once cost a round trip on every call.
            watch = await self._wake_or_fail(rid, waiter, min(secs, self.settings.poll_secs), watch)
            if waiter.done():
                waiter = self._rearm(rid, waiter)
        tries = 0
        while True:
            until = self._caller_until(rid, until)  # an admin extend moves it (0.2.0 T2)
            try:
                reply = await self.requests.take_result(rid)
                gone = reply is None and (recheck or rid in self._gone) and await self._lost(rid)
            except (*STORE_ERRORS, ResponseError) as exc:
                if isinstance(exc, ResponseError) and not _store_refused(exc):
                    raise
                # A refusal too (NOREPLICAS, OOM...): the caller keeps waiting (Q12, R3FM-7).
                # The call is already sent or running: wait for the store, never re-run it
                # on the store_down path (that ran it twice, once outside the budget).
                if until - loop.time() <= 0:
                    raise CallFailed(
                        "deadline_exceeded", "the store did not answer in time", request_id=rid
                    ) from exc
                tries += 1
                pause = min(self._store_retry_secs(tries), max(0.0, until - loop.time()))
                watch = await self._wake_or_fail(rid, waiter, pause, watch)
                if waiter.done():
                    waiter = self._rearm(rid, waiter)
                continue
            if gone:
                return None
            if reply is not None:
                if reply.ok:
                    return reply
                raise CallFailed(
                    reply.error or "provider_error",
                    reply.message,
                    retry_after_secs=reply.retry_after_secs,
                    partial_text=reply.partial_text,
                    request_id=rid,
                )
            left = self._caller_until(rid, until) - loop.time()
            if left <= 0:
                raise CallFailed("deadline_exceeded", "no reply in time", request_id=rid)
            # The wake-up comes by pub/sub (published with the result), or from this process's
            # one result sweep (`_result_sweep`); a read per waiting caller made store load
            # grow with the backlog (B56). The timeout is only a last-resort recheck. An
            # attached caller checks first after the takeover grace: the send it attached to
            # may have failed or its sender died, noticed only after 60 s (R3RC-4).
            tries = 0
            pace = self.settings.timings.result_recheck_secs
            if recheck and loop.time() < first_check:
                pace = first_check - loop.time()
            watch = await self._wake_or_fail(rid, waiter, min(left, pace), watch)
            if waiter.done():
                waiter = self._rearm(rid, waiter)

    async def _lost(self, rid: str) -> bool:
        """True when a waited-on call must be registered again: its record is gone or stale.

        Stale: still unconfirmed past the takeover grace (its send failed or its sender died),
        so a new registration takes it over (R3RC-4).
        """
        self._gone.discard(rid)
        job = await self.requests.job(rid)
        if not job:
            return True
        if job.get("confirmed") != "0":
            return await self._direct_runner_dead(job)
        created = float(job.get("created") or 0)
        return self.clock.now() - created >= self.requests.takeover_grace_secs

    async def _direct_runner_dead(self, job: Mapping[str, str]) -> bool:
        """True when a confirmed record's direct run lives in a process that died (AMEND FE-5).

        REGISTER's rule, read here so an attached caller registers again (and takes the record
        over) instead of waiting to its deadline: the runner's liveness key is gone and its
        claim had no heartbeat for a lease, or it never started running.
        """
        runner = job.get("caller")
        if job.get("direct") != "1" or not runner or runner == self.worker_id:
            return False
        if await self.state.exists(self.keys.caller(runner)):
            return False
        if job.get("state") != "running":
            return True
        return self.clock.now() * 1000 - float(job.get("hb") or 0) >= self.requests.lease_ms

    async def _keep_alive(self, pubsub: Any, alive: _Alive) -> None:
        """Ping a subscriber; raise (reset and resubscribe) when it went silent.

        redis-py's health check writes a PING and never needs the answer, so a half-open
        socket went unnoticed for about 15 min: results came only by the 5 s sweep, cancels by
        the 20 s heartbeat (R3RC-6). An old primary cut off by a partition answers the PING but
        delivers nothing: the sweep and the cancel check mark that reader stale (R3FM-12).
        """
        now = time.monotonic()
        if alive.name in self._stale_readers:
            self._stale_readers.discard(alive.name)
            raise _StaleSubscription(f"{alive.name}: results arrive only by the sweep")
        if now - alive.seen_mono > 2 * PUBSUB_PING_SECS + 1:
            raise _StaleSubscription(f"{alive.name}: no answer to its pings")
        if now - alive.pinged_mono >= PUBSUB_PING_SECS:
            alive.pinged_mono = now
            await pubsub.ping("llmq-alive")

    def _reader_stale(self, name: str) -> None:
        """Reset a subscriber that stopped delivering (counted)."""
        logger.warning("pubsub_stale", reader=name)
        self.metrics.count("llmq_pubsub_resubscribes_total", reader=name)
        self._stale_readers.add(name)

    async def _warm_clients(self) -> None:
        """Create every Bedrock deployment's client now, not on its first call.

        A client takes about a second to build (measured: the first call of a fresh worker took
        1,032 ms against ~140 ms after; B92 asks for <= 500 ms of first-call overhead).
        All at once, each pool sized by the worker slots of the models routed to its account
        and region (E3, E5). Failures are logged by the pool; a call meets them again. The
        OpenAI-compatible routes go too (nothing is built for them): their missing secrets are
        warned about at start, not first met by a call (CL-6).
        """
        view = self.registry.current
        routes = [
            route_for(view, deployment)
            for alias in view.registry.models
            for deployment in view.deployments(alias)
        ]
        slots = self._pool_slots(view)
        self._planned = slots
        try:
            await self.providers.pool.warm(routes, slots=slots)
        except Exception as exc:  # a call meets the problem again; start goes on (RT-24)
            logger.warning("warm_clients_failed", error=str(exc), error_type=type(exc).__name__)

    @staticmethod
    def _pool_slots(view: ResolvedRegistry) -> dict[tuple[str, str], int]:
        """Concurrent Bedrock calls this process may make per (account, region).

        Per model, its worker slots, or a deployment's `max_concurrency` when that is lower
        (no more connections than calls the deployment admits).
        """
        slots: dict[tuple[str, str], int] = {}
        for alias, entry in view.registry.models.items():
            if entry.provider != "bedrock":
                continue
            workers = int(view.value(alias, "worker_slots"))
            places: dict[tuple[str, str], int] = {}
            for deployment in view.deployments(alias):
                cap = view.value(alias, "max_concurrency", deployment.key)
                want = min(workers, int(cap)) if cap else workers
                place = (deployment.account, deployment.region)
                places[place] = max(places.get(place, 0), want)
            for place, want in places.items():
                slots[place] = slots.get(place, 0) + want
        return slots

    def _replan_pools(self) -> None:
        """Resize the Bedrock pools when a reload changed their sizes (owner Q43).

        A live `max_concurrency` or `worker_slots` edit changed nothing until a restart: the
        pool retires a client whose size changed (closed once idle) and builds the new one.
        """
        if not self._worker or self._planned is None:
            return  # not warmed yet: `_warm_clients` plans the first sizes
        slots = self._pool_slots(self.registry.current)
        if slots != self._planned:
            self._planned = slots
            self.providers.pool.plan(slots)

    async def _result_sweep(self) -> None:
        """Every `poll_secs`, one store call finds results of this process's waiting callers.

        The safety net behind the pub/sub wake-up: one script per process per period, however
        many calls wait (a read per waiting caller grew with the backlog, plan B56).
        """
        while True:
            with contextlib.suppress(TimeoutError):  # an epoch change wakes it at once
                await asyncio.wait_for(self._sweep_now.wait(), self.settings.poll_secs)
            self._sweep_now.clear()
            waiting = [rid for rid, w in self._waiters.items() if not w.done()]
            for start in range(0, len(waiting), 500):
                chunk = waiting[start : start + 500]
                try:
                    found, gone = await self._results_exist([], [self.keys.prefix, *chunk])
                except Exception as exc:  # the sweep must survive store hiccups
                    logger.warning("result_sweep_failed", error=str(exc))
                    break
                misses = [
                    i
                    for i in found
                    if (w := self._waiters.get(chunk[int(i)])) is not None and not w.done()
                ]
                self._sweep_misses = self._sweep_misses + 1 if misses else 0
                if self._sweep_misses >= PUBSUB_SWEEP_MISSES:
                    self._sweep_misses = 0
                    self._reader_stale("result_reader")
                if gone:
                    # The store lost these records (restart without persistence, flush, a
                    # failover that lost writes): their callers send them again (R3FM-4).
                    logger.warning("waiting_calls_lost", count=len(gone))
                    self._gone.update(chunk[int(index)] for index in gone)
                for index in [*found, *gone]:
                    waiter = self._waiters.get(chunk[int(index)])
                    if waiter is not None and not waiter.done():
                        waiter.set_result(None)

    async def _result_reader(self) -> None:
        """Wake waiting callers when their result is published (re-subscribes after errors)."""
        subscribed = False
        alive = _Alive("result_reader")
        while True:
            try:
                if not subscribed:
                    await self._pubsub.subscribe(self.keys.done_channel(self.worker_id))
                    subscribed = True
                    alive.reset()
                    # Results published while unsubscribed were missed: every waiter re-reads.
                    for pending in list(self._waiters.values()):
                        if not pending.done():
                            pending.set_result(None)
                await self._keep_alive(self._pubsub, alive)
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
            except Exception as exc:  # the reader must survive store hiccups
                logger.warning("result_reader_error", error=str(exc))
                subscribed = False
                with contextlib.suppress(Exception):  # silent: the line above said why
                    await self._pubsub.reset()
                await asyncio.sleep(1)
                continue
            if not message:
                continue
            alive.seen()
            if message.get("type") == "pong":
                continue
            rid = (
                message["data"].decode()
                if isinstance(message["data"], bytes)
                else str(message["data"])
            )
            extended = extend_notice(rid)
            if extended is not None:  # an admin extend (0.2.0 T2): the call waits longer
                self._extend_caller(*extended)
                continue
            waiter = self._waiters.get(rid)
            if waiter is not None and not waiter.done():
                waiter.set_result(None)

    # ================================================================ worker: receiving

    async def _model_loop(self, alias: str) -> None:
        """Receive this model's calls (priority first) and admit them in order.

        Idle, it blocks on its queues' doorbell (a send wakes one worker at once) for 50 ms at a
        time, then 1 s after a second with nothing: polling idle loops made ~96 receive
        scripts/s per pod with 4 models (measured); a busy loop never waits. With no free worker
        slot it waits for one (`_run` wakes it), not on the doorbell; a token it took but cannot
        serve (no slot, or that class held) goes back, so another pod's worker wakes for it.
        """
        empty = 0
        wake = self._wake.setdefault(alias, asyncio.Event())
        rung: str | None = None  # the queue whose doorbell token this loop took last
        while not self._stopping:
            if alias not in self.registry.current.registry.models:
                return  # removed by a reload; `_sync_model_loops` starts it again if it returns
            if self._paused:
                if rung is not None:  # the doorbell token it took: another pod's to answer
                    with contextlib.suppress(*STORE_ERRORS, RedisError):
                        await self.queues.ring_again(rung)
                    rung = None
                with contextlib.suppress(TimeoutError):  # a resume wakes it at once
                    await asyncio.wait_for(wake.wait(), 0.2)
                wake.clear()
                continue
            try:
                got = await self._receive_once(alias)
            except Exception as exc:  # a store blip must not end the loop
                self._limited(logger.warning, "receive_failed", alias, model=alias, error=str(exc))
                if isinstance(exc, STORE_ERRORS):
                    # Not only the heartbeat (every lease/3): a block between two beats went
                    # unseen, and callers whose keys lapsed in it were dropped as gone (soak).
                    self._store_trouble("receive")
                await asyncio.sleep(1)
                continue
            view = self.registry.current
            names = {self._queue_name(view, alias, p): "p" if p else "n" for p in (True, False)}
            if (
                rung is not None
                and not got
                and (got is None or (alias, names.get(rung)) in self._held_class)
            ):
                # A plain error reply too (`UNBLOCKED` in a failover): only connection errors
                # were caught, and it ended the loop until the next heartbeat (FE-20).
                with contextlib.suppress(*STORE_ERRORS, RedisError):
                    await self.queues.ring_again(rung)
            rung = None
            held = (alias, "p") in self._held_class or (alias, "n") in self._held_class
            # Idle only when the queues were empty, not when a class waits on its held call.
            empty = 0 if got or held else empty + 1
            if not got:
                pace = self.settings.timings
                pause = (
                    pace.idle_poll_secs if empty < pace.idle_after_empty else pace.idle_slow_secs
                )
                if held or got is None:  # a held call ending, or a slot freed, wakes it at once
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(wake.wait(), pause)
                else:  # a send rings the doorbell: one idle worker wakes per call
                    try:
                        rung = await self.queues.wait_bell(list(names), pause)
                    except STORE_ERRORS:
                        self._store_trouble("bell")  # (was swallowed unseen)
                    except RedisError as exc:
                        # `UNBLOCKED ... (master -> replica?)`: a failover ends the blocked
                        # BLPOP with a plain error reply. It ended this loop, so the model took
                        # no calls until the heartbeat restarted it (failover drills).
                        logger.warning("bell_failed", model=alias, error=str(exc))
                        self._store_trouble("bell")
                        await asyncio.sleep(pause)
                wake.clear()

    async def _receive_once(self, alias: str) -> bool | None:
        """Receive and admit this model's next calls: True if any came, None with no free slot.

        One script reads the priority queue, then the normal one (classes waiting on a held
        call are skipped): two round trips per poll before.
        """
        view = self.registry.current
        if alias not in view.registry.models:
            await asyncio.sleep(1)
            return False
        lost = self._unclaimed.pop(alias, None)
        if lost is not None:
            # The last receive's reply was lost (scenario S11): what it took is ours and hidden
            # for the visibility timeout (60 s). It goes back to the queue now, receipt-checked.
            # Before the slot check: a full worker left the lost batch hidden (FE-8).
            try:
                back = await self.queues.reclaim(*lost)
            except BaseException:
                self._unclaimed[alias] = lost
                raise
            if back:
                logger.warning("receive_reclaimed", model=alias, count=back)
                self.metrics.count("llmq_reclaimed_total", float(back), model=alias)
        free = self._slots[alias].free
        if free <= 0:
            return None
        settings = self._queue_settings(view, alias)
        names = [
            self._queue_name(view, alias, priority)
            for priority in (True, False)
            if (alias, "p" if priority else "n") not in self._held_class
        ]
        key = self.queues.claim_key(f"{self.worker_id}:{alias}")
        claim = (key, new_id())
        try:
            messages, dead = await self.queues.receive_first(
                names, min(free, self.settings.receive_batch), settings, claim=claim
            )
        except BaseException:
            # It may have run: reclaim it on the next round, or at `stop()`. A cancel too (a
            # loop restarted, shutdown): only store errors were recorded (FL-3, FE-8).
            self._unclaimed[alias] = claim
            raise
        rest = list(messages)  # not admitted yet: back to the queue whatever happens
        try:
            for letter in dead:
                # One failed reply skips only itself: it skipped every received message,
                # neither admitted nor returned (hidden 60 s) (audit RT-8).
                try:
                    await self._dead_lettered(alias, letter.body)
                except Exception as exc:
                    logger.warning("dead_letter_reply_failed", model=alias, error=str(exc))
            # A pause during the receive: what it took goes back unadmitted (0.2.0 A1).
            while rest and not self._hand_back_on_pause():
                if await self._admit_new(alias, rest.pop(0)):
                    break
        finally:
            if rest:
                await self._return_all(rest)
        return bool(messages)

    def _hand_back_on_pause(self) -> bool:
        """Paused, with held calls handed back (`drain_handback_enabled`): admit nothing new."""
        return self._paused and self.settings.drain_handback_enabled

    async def _return_all(self, messages: Sequence[Message]) -> None:
        """Return messages to their queue together; a failure is logged and skips only itself.

        One at a time, the first failed return stranded the rest (audit RT-8); their
        visibility timeout brings back the ones that failed.
        """
        results = await asyncio.gather(
            *(self.queues.return_(m) for m in messages), return_exceptions=True
        )
        failed = [r for r in results if isinstance(r, BaseException)]
        if failed:
            logger.warning("return_failed", count=len(failed), error=str(failed[0]))

    def _queue_settings(self, view: ResolvedRegistry, alias: str) -> QueueSettings:
        queue = view.value(alias, "queue")
        return QueueSettings(
            visibility_secs=float(queue["visibility_secs"]),
            max_receives=int(queue["max_receives"]),
            retention_secs=int(queue["retention_secs"]),
            dlq_retention_secs=int(queue["dlq_retention_secs"]),
            max_waiting=int(view.value(alias, "max_waiting")),
        )

    async def _dead_lettered(self, alias: str, body: bytes) -> None:
        """Tell a dead letter's caller (`dead_lettered`); an unreadable one is counted, dropped."""
        try:
            env = parse(body)
        except Exception as exc:  # nobody to tell: counted, it was dropped silently (RT-24)
            logger.warning("bad_dead_letter", model=alias, error_type=type(exc).__name__)
            self.metrics.count("llmq_bad_messages_total", model=alias)
            return
        await self._reply(env, "dead_lettered", "the call failed too many times", final="failed")
        self._outcome(env, alias, "", "", "dead_lettered", None, "dead_letter")

    async def _admit_new(self, alias: str, message: Message) -> bool:
        """Admit a newly received message; True when it is now held (its class stops)."""
        try:
            env = parse(message.body)
        except UnsupportedEnvelopeError as exc:
            # Sent by a newer library (a rollout in progress, plan C5): passed on in its place
            # with no failure counted, so a newer pod runs it. Every 5 s this pod took it again
            # and logged each time; now 15 s, a metric, and one warning a minute.
            self.metrics.count("llmq_newer_messages_total", model=alias)
            now = time.monotonic()
            if now - self._newer_warned_mono >= 60:
                self._newer_warned_mono = now
                logger.warning("message_needs_newer_library", model=alias, error=str(exc))
            await self._pass_on(
                message, self.queues.return_(message, delay_secs=NEWER_MESSAGE_DELAY_SECS)
            )
            return False
        except Exception as exc:
            # The type only: the validation text can quote the prompt (RESCAN X10). The model's
            # own queue settings decide when it is dead-lettered (A12).
            logger.warning("bad_message", queue=message.queue, error_type=type(exc).__name__)
            self.metrics.count("llmq_bad_messages_total", model=alias)
            await self._pass_on(
                message,
                self.queues.fail(message, self._queue_settings(self.registry.current, alias)),
            )
            return False
        try:
            priority = message.queue == self._queue_name(self.registry.current, alias, True)
        except KeyError:  # removed by a reload meanwhile: the name's suffix says the class
            priority = message.queue.endswith("-priority")
        if env.priority != priority:
            # The class is the queue's, not the body's: a reprioritize moves the message and
            # never rewrites its body (0.2.0 T2).
            env = env.model_copy(update={"priority": priority})
        current = self._held.get(env.request_id)
        if current is not None:
            # Already held here (RESCAN A1): its hold lapsed and the same message came back, or
            # a second copy arrived. A new record overwrote the running one (heartbeat lost,
            # slot leaked, call run twice): the running record keeps the new receipt instead.
            if current.message is not None and current.message.id == message.id:
                current.message = message
                logger.warning("redelivered_while_held", request_id=env.request_id)
            else:
                await self.queues.return_(message, delay_secs=self.settings.lease_secs / 2)
            return False
        held = _Held(
            message,
            env,
            # The store's time at receive: the wait is measured on one clock (the local view
            # of the store clock drifted between syncs).
            first_seen_secs=message.received_ms / 1000 if message.received_ms else self.clock.now(),
            position_ms=message.position_ms,
            tag=message.id,
        )
        self._held[env.request_id] = held
        # Its first admission checks that a caller still waits (plan §3.18 R24), in the same
        # script: dropped as `caller_gone` when none does.
        held.check_callers = self._callers_judgeable()
        try:
            return await self._try(held)
        except Exception:
            await self._release(held, unsure=True)
            raise

    async def _pass_on(self, message: Message, step: Awaitable[object]) -> None:
        """Run the store step that passes an unadmitted message on; on failure return it.

        The message was already out of the receive's list: a store blip in that step left it
        hidden for its visibility time and charged a failure (FE-9).
        """
        try:
            await step
        except Exception:
            await self._return_all([message])
            raise

    def _callers_judgeable(self) -> bool:
        """False while lapsed caller keys prove nothing: the store was out of reach lately.

        After a store outage the callers' liveness keys may have expired only because nobody
        could renew them: for a caller TTL and a heartbeat after this process last saw the
        store fail, callers count as alive (B95: a 45 s block dropped 429 of 500 calls).

        Judged from this process's own heartbeat too: a half-open store hung every command for
        the read timeout, the doorbell wait swallowed its error, and calls were dropped the
        moment the store came back (scenario S5). A heartbeat that is late or just recovered
        from a gap means the store was out of reach here as well.
        """
        beat = self.settings.lease_secs / 3
        grace = self.caller_ttl_secs + beat + 5
        now = time.monotonic()
        return not (
            now - self._store_trouble_mono < grace or now - self._beat_ok_mono > 2 * beat + 1
        )

    def _store_trouble(self, where: str) -> None:
        """Note a store failure seen here: callers count as alive for a while; metric."""
        self._store_trouble_mono = time.monotonic()
        self.metrics.count("llmq_store_errors_total", where=where)

    # ================================================================ worker: admission

    def _deadline(self, held: _Held) -> float:
        """The call's deadline, store secs: its send time + its timeout, or an admin extend."""
        return max(held.position_ms / 1000.0 + held.envelope.timeout_secs, held.extended_ms / 1000)

    async def _past_deadline(self, held: _Held, now: float) -> bool:
        """True when the call is past its deadline, an admin extend read from the store first.

        A worker that took the call after the extend's notice never saw it: the job's `dl` is
        read here, once, only when the deadline it knows is reached (no read per call).
        """
        if now < self._deadline(held):
            return False
        try:
            raw = await self.state.hget(self.keys.request(held.envelope.request_id, "job"), "dl")
        except (*STORE_ERRORS, ResponseError):
            return True
        if raw is not None:
            held.extended_ms = max(held.extended_ms, int(float(text(raw))))
        return now >= self._deadline(held)

    def _candidates(
        self,
        view: ResolvedRegistry,
        alias: str,
        env: Envelope,
        age: float,
        failed: frozenset[str],
        cause: str,
    ) -> list[Candidate]:
        deps: list[Deployment] = list(view.deployments(alias))
        if env.allowed_regions:
            deps = [d for d in deps if d.region in env.allowed_regions]
        if env.allowed_accounts:
            deps = [d for d in deps if d.account in env.allowed_accounts]
        fallback_ok = self._may_spill(view, alias, env, age, cause, "fallback")
        now = time.monotonic()
        deps = [
            d
            for d in deps
            if (bad := self._bad_accounts.get((d.account, d.region))) is None
            or bad.until_mono <= now
        ]
        out = []
        for dep in deps:
            if dep.group == "fallback" and not fallback_ok:
                continue
            out.append(Candidate(dep.key, dep.group, last=dep.key in failed))  # type: ignore[arg-type]
        return out

    def _ctx(self, alias: str, held: _Held, waited: float) -> hooks.CallContext:
        env = held.envelope
        return hooks.CallContext(
            alias, env.request_id, env.priority, env.session_id, env.user_id, env.tenant_id, waited
        )

    def _may_spill(
        self,
        view: ResolvedRegistry,
        alias: str,
        env: Envelope,
        age: float,
        cause: str,
        rule_name: str,
    ) -> bool:
        """`may_spill`: whether a call may use fallback regions / models now."""
        return may_spill(view, alias, env, age, cause, rule_name)

    def _ask(
        self,
        view: ResolvedRegistry,
        alias: str,
        held: _Held,
        attempt: int,
        failed: frozenset[str],
        *,
        foreign_from: str | None = None,
        require_pct: int = 0,
    ) -> Ask | None:
        env = held.envelope
        age = self.clock.now() - held.position_ms / 1000.0
        cause = "error" if failed else ("broken" if held.broken else "exhausted")
        candidates = self._candidates(view, alias, env, age, failed, cause)
        if not candidates:
            return None
        entry = view.registry.models[alias]
        estimate = self._charged(view, alias, held)
        strategy = view.value(alias, "balance")
        if env.priority and view.value(alias, "balance_priority"):
            strategy = view.value(alias, "balance_priority")
        if str(strategy).startswith("custom:"):
            candidates = self._custom_order(view, alias, held, candidates, str(strategy)[7:], age)
            strategy = "ordered"
        sticky = view.value(alias, "sticky")
        sticky_hash = None
        if sticky and entry.type == "chat":
            key: str | None = env.session_id
            if str(sticky["key"]).startswith("custom:"):
                name = str(sticky["key"])[7:]
                fn = hooks.get("sticky_key", name)
                ctx = self._ctx(alias, held, age)
                key = hooks.call_safely("sticky_key", name, fn, ctx, default=None) if fn else None
            if key:
                sticky_hash = self.hash_id(str(key))
        ask = Ask(
            alias=alias,
            request_id=env.request_id,
            worker=self.worker_id,
            attempt=attempt,
            cls=_cls(env),
            position=held.position,
            candidates=candidates,
            strategy=str(strategy),
            pick=str((view.value(alias, "balance_opts") or {}).get("pick", "percent")),
            cost_r=1,
            cost_t=estimate.total,
            claim=foreign_from is None,
            sticky_hash=sticky_hash,
            sticky_ttl_secs=float(sticky["idle_ttl_secs"]) if sticky else 1800.0,
            sticky_move_secs=float(view.value(alias, "sticky_move_after_secs")),
            foreign_from=foreign_from,
            require_pct=require_pct,
            tag=held.tag,
            callers_ttl=self.caller_ttl_secs if held.check_callers else None,
            count_first=int(self.clock.now() // 60) if attempt == 1 else None,
            chunks=env.stream and env.op in ("converse", "chat"),
            fairness=self._fairness(view, alias, held),
        )
        held.estimates[(alias, ask.owner)] = estimate
        return ask

    def _fairness(self, view: ResolvedRegistry, alias: str, held: _Held) -> Fairness | None:
        """The call's tenant fairness on model `alias` (None when its `fairness` is off).

        The tenant is the call's tenant id, else its user id: the keyed hashes its message
        carries (D1), looked up in the registry's hashed `weights` / `quotas` (D2).
        """
        setting = view.value(alias, "fairness")
        if setting.get("mode", "off") == "off":
            return None
        env = held.envelope
        return fairness_for(
            setting,
            tenant_hash_of(env.tenant_id, env.user_id, self.hash_id),
            burst=float(view.value(alias, "rate_burst")),
            window=str(view.value(alias, "rate_window")),
            round_=held.fair_rounds.get(alias),
        )

    def _custom_order(
        self,
        view: ResolvedRegistry,
        alias: str,
        held: _Held,
        candidates: list[Candidate],
        name: str,
        age: float,
    ) -> list[Candidate]:
        """Candidates in the order a custom balancer prefers (unknown keys ignored)."""
        fn = hooks.get("balancer", name)
        if fn is None:
            return candidates
        deps = {d.key: d for d in view.deployments(alias)}
        views = [
            hooks.DeploymentView(
                key=c.key,
                account=deps[c.key].account,
                region=deps[c.key].region,
                group=c.tier,
                rpm=view.value(alias, "rpm", c.key),
                tpm=view.value(alias, "tpm", c.key),
                weight=float(view.value(alias, "weight", c.key)),
                tags=view.value(alias, "tags", c.key) or {},
            )
            for c in candidates
        ]
        ctx = self._ctx(alias, held, age)
        order = hooks.call_safely("balancer", name, fn, views, ctx, default=None)
        if not order:
            return candidates
        by_key = {c.key: c for c in candidates}
        ranked = [by_key[k] for k in order if k in by_key]
        return ranked + [c for c in candidates if c.key not in set(order)]

    def _owner(self, held: _Held, attempt: int) -> str:
        """Owner token of an attempt; the message id tells two deliveries of one id apart."""
        worker = f"{self.worker_id}@{held.tag}"
        return owner_token(held.envelope.request_id, worker, attempt)

    def _params(
        self, view: ResolvedRegistry, alias: str, env: Envelope, dep: str | None
    ) -> dict[str, Any]:
        """`request_params`: the provider request for `alias` (and deployment `dep`)."""
        return request_params(view, alias, env, dep)

    def _charged(self, view: ResolvedRegistry, alias: str, held: _Held) -> tokens.Estimate:
        """The token estimate admission charges: from model-level params, cached per view."""
        cached = held.charged
        if cached is not None and cached[0] is view and cached[1] == alias:
            return cached[2]
        params = self._params(view, alias, held.envelope, None)
        estimate = self._estimate(view, alias, held.envelope, params)
        held.charged = (view, alias, estimate)
        return estimate

    def _estimate(
        self, view: ResolvedRegistry, alias: str, env: Envelope, params: Mapping[str, Any]
    ) -> tokens.Estimate:
        return estimate_of(view, alias, env, params)

    async def _try(self, held: _Held) -> bool:
        """Admit a newly received call; start it when granted. True when it stays held.

        Only `_admit_new` calls it, always as a first attempt: the second `max_wait` check (same
        clock reading, same condition) and the retry recursion for later attempts never ran
        and are gone (audit RT-20).
        """
        view = self.registry.current
        env = held.envelope
        alias = env.model
        now = self.clock.now()
        # A call that already waited its limit in the queue, unreceived, is refused now rather
        # than started with no time left (it ran past its deadline and the caller gave up).
        if await self._expire_if_due(held, now, retry_after=1.0):
            return False
        decision = await self._decide(view, alias, held, 1, frozenset())
        if decision is None:
            return False
        if decision.kind == "granted":
            self._start_run(held, decision, 1)
            return False
        if decision.kind != "wait":
            return False
        if await self._model_fallback(view, alias, held, 1, now - held.position_ms / 1000.0):
            return False
        if self._stopping or self._hand_back_on_pause():
            # Received while `stop()` ran, after it handed the held calls back: a hold here
            # ended at once without returning the message, hidden for the drain plus its
            # visibility time and charged a failure (FE-2). The same for a receive in flight
            # at a pause, after its hand-back (0.2.0 A1): another pod runs it.
            await self._release(held)
            return False
        key = (alias, _cls(env))
        self._held_class.add(key)
        task = held.hold = asyncio.create_task(self._hold(held, decision.wait_secs))
        self._running.add(task)
        task.add_done_callback(self._running.discard)
        task.add_done_callback(lambda done: self._hold_cancelled(done, key))
        return True

    def _hold_cancelled(self, task: asyncio.Task[None], key: tuple[str, str]) -> None:
        """Free a hold's class when the hold was cancelled before its first step (RV-N2).

        Its `finally` never ran then, so the class stayed held and this process never
        received that class of the model again.
        """
        if not task.cancelled():
            return
        others = (
            h.hold
            for h in self._held.values()
            if h.hold is not None
            and h.hold is not task
            and (h.envelope.model, _cls(h.envelope)) == key
        )
        if not any(not other.done() for other in others):
            self._held_class.discard(key)

    async def _expire_if_due(
        self, held: _Held, now: float, *, retry_after: float | None = None
    ) -> bool:
        """Settle a call past its deadline or its `max_wait_secs` as expired; True if it was.

        One copy of the two checks `_try`, `_hold` and `_direct_wait` each wrote (RT-21).
        """
        if await self._past_deadline(held, now):
            await self._finish_call(
                held, "rate_limited", "no budget before the deadline", final="expired"
            )
            return True
        max_wait = held.envelope.max_wait_secs
        if held.extended_ms:  # an extended call waits up to its new deadline (0.2.0 T2)
            max_wait = None
        if max_wait is not None and now - held.position_ms / 1000.0 >= max_wait:
            await self._finish_call(
                held,
                "rate_limited",
                "max_wait_secs passed",
                final="expired",
                retry_after=retry_after,
            )
            return True
        return False

    async def _decide(
        self,
        view: ResolvedRegistry,
        alias: str,
        held: _Held,
        attempt: int,
        failed: frozenset[str],
    ) -> Decision | None:
        """One admission call and the terminal outcomes; None when the call was settled."""
        try:
            ask = self._ask(view, alias, held, attempt, failed)
        except inference.InferenceRejected as exc:
            # `on_exceed: reject`: the request itself is refused. It raised out of the worker
            # and the message looped until dead-lettered (RESCAN P4).
            await self._finish_call(held, "invalid_request", str(exc))
            return None
        except Exception as exc:
            # Any other failure to build the request (a param the estimate or the hooks cannot
            # read) fails the same way every time: final, not handed back by `_release` to run
            # again until dead-lettered (EN-2). The type only: the text can quote the prompt.
            rid = held.envelope.request_id
            logger.error("request_unusable", request_id=rid, error_type=type(exc).__name__)
            self.metrics.count("llmq_bad_messages_total", model=alias)
            await self._finish_call(
                held, "invalid_request", f"the request could not be prepared ({type(exc).__name__})"
            )
            return None
        if ask is None:
            if self._bad_accounts and any(
                (d.account, d.region) in self._bad_accounts for d in view.deployments(alias)
            ):
                await self._finish_call(
                    held,
                    "credentials_unavailable",
                    "no account with working credentials",
                    retry_after=60,
                )
            else:
                await self._finish_call(
                    held, "no_deployment", "the call's filter leaves no deployment"
                )
            return None
        estimate = self._charged(view, alias, held)
        # The whole reservation (input and output), where TPM is enforced: a call above every
        # candidate's TPM is never admitted; it waited until its deadline (RESCAN R4).
        if tpm_refuses(view, alias, (c.key for c in ask.candidates), estimate.total):
            await self._finish_call(held, "request_too_large", "the call is above the token budget")
            return None
        decision = await self._pick_take(held, ask)
        if decision.kind == "wait" and not held.refused:  # once per call, not per poll (O4)
            held.refused = True
            self.metrics.count("llmq_admission_refusals_total", model=alias, reason=decision.reason)
        if decision.kind == "stale_config" and await self._registry_step(
            self._reload_registry(decision.changes), "reload"
        ):
            decision = await self._pick_take(held, ask)
        # The store lost the numbers: restore, then once more.
        if decision.kind == "no_config" and await self._registry_step(
            self.registry.refresh(), "refresh"
        ):
            decision = await self._pick_take(held, ask)
        held.check_callers = False  # checked once, on its first admission
        if ask.fairness is not None and await self._tenant_step(alias, held, ask, decision):
            return None
        if decision.kind == "gone":
            # A warning: a dropped call is worth seeing (the soak and scenario S5 found calls
            # dropped wrongly, and INFO logs alone did not show it).
            logger.warning("caller_gone", request_id=held.envelope.request_id, model=alias)
            await self._finish_call(held, "cancelled", "caller_gone", final="cancelled")
            return None
        if decision.kind in ("no_config", "stale_config"):
            # Still no usable numbers. Before, the call stayed held and hidden for good.
            logger.error("admission_config_missing", model=alias, kind=decision.kind)
            if held.message is None:
                await self._release(held)
                raise CallFailed(
                    "queue_unavailable",
                    f"{alias}: admission numbers are missing",
                    request_id=held.envelope.request_id,
                )
            await self._release(held, delay_secs=5.0)
            return None
        if decision.kind in ("done", "cancelled"):
            if decision.kind == "cancelled":
                await self._finish_call(held, "cancelled", "cancelled", final="cancelled")
            else:
                await self._ack(held)
            return None
        if decision.kind == "duplicate":
            self._drop(held)
            if held.message is not None:
                # When the claim lapses, if the script says (`ms`); else half a lease. The fixed
                # half lease made a lower `visibility_secs` take over a dead pod's call later
                # (R3RC-8). PICK_TAKE's `duplicate` carries `ms`; the half lease is a fallback.
                delay = (
                    decision.wait_secs if decision.wait_secs > 0 else self.settings.lease_secs / 2
                )
                await self.queues.return_(held.message, delay_secs=delay)
            return None
        if decision.kind == "none":
            return await self._none_usable(view, alias, held, attempt, failed)
        return decision

    async def _pick_take(self, held: _Held, ask: Ask) -> Decision:
        """PICK_TAKE for a call; a cancel landing while it is in flight keeps its maybe-grant.

        The script may have granted before the cancel (A-4): the owner joins `held.grants`, so
        the cancel's give-back sends an owner-checked UNDO (a no-op when nothing was granted).
        """
        try:
            return await self.admission.pick_take(ask, self.registry.changes)
        except asyncio.CancelledError:
            held.grants.add((ask.alias, ask.owner))
            raise

    async def _tenant_step(self, alias: str, held: _Held, ask: Ask, decision: Decision) -> bool:
        """Tenant fairness after an admission: keep the fair round, count, settle a quota refusal.

        True when the call was settled: its tenant's quota is used up, so it fails at once as
        `rate_limited` with `retry_after_secs` (a hard cap, no wait). Labels are the model and
        the mode only, never a tenant.
        """
        fairness = ask.fairness
        assert fairness is not None
        if (fair_round := decision.extra.get("fr")) is not None:
            held.fair_rounds[alias] = float(fair_round)
        if decision.kind == "tenant_limited":
            self.metrics.count("llmq_tenant_refusals_total", model=alias, mode=fairness.mode)
            await self._finish_call(
                held,
                "rate_limited",
                "the tenant's quota is used up",
                retry_after=max(decision.wait_secs, 0.001),
            )
            return True
        if decision.kind == "wait" and decision.reason == "tenant" and not held.tenant_waited:
            held.tenant_waited = True  # once per call, like the refusals counter
            self.metrics.count("llmq_tenant_refusals_total", model=alias, mode=fairness.mode)
        return False

    async def _none_usable(
        self,
        view: ResolvedRegistry,
        alias: str,
        held: _Held,
        attempt: int,
        failed: frozenset[str],
    ) -> Decision | None:
        """No deployment is usable: the fallback group, then fallback models, else unavailable.

        `model_unavailable` says why per deployment. Its own method: `_decide` mixed it with
        admission and registry repair in 125 lines (FE-22).
        """
        has_fallback = any(d.group == "fallback" for d in view.deployments(alias))
        rule = view.value(alias, "fallback")
        if not held.broken and has_fallback and rule and "broken" in rule["on"]:
            held.broken = True
            age = self.clock.now() - held.position_ms / 1000.0
            if age < float(rule["after_secs"]):
                return Decision("wait", wait_secs=float(rule["after_secs"]) - age, reason="broken")
            return await self._decide(view, alias, held, attempt, failed)
        fell = await self._model_fallback(view, alias, held, attempt, 1e9, broken=True)
        if not fell:
            due = await self.health.disabled(alias)
            wait = max(1.0, min(due.values()) - self.clock.now()) if due else 60.0
            # Why no deployment was usable, per deployment: an operator needs to tell an
            # open breaker from a disabled deployment (the bare line said neither).
            # The breakers in one pipelined round trip: one HGETALL per deployment, one
            # after another, on a path that already failed (audit RT-31).
            why: dict[str, str] = {}
            deps = list(view.deployments(alias))
            with contextlib.suppress(*STORE_ERRORS):  # the line goes out without them
                async with self.state.pipeline(transaction=False) as pipe:
                    for dep in deps:
                        pipe.hget(self.keys.breaker(alias, dep.key), "state")
                    states = await pipe.execute()
                for dep, raw in zip(deps, states, strict=True):
                    disabled = " (disabled)" if dep.key in due else ""
                    why[dep.key] = f"breaker {text(raw or b'closed')}{disabled}"
            self._limited(
                logger.error,
                "model_unavailable",
                alias,
                model=alias,
                request_id=held.envelope.request_id,
                deployments=why,
            )
            await self._finish_call(
                held, "model_unavailable", "no usable deployment", retry_after=wait
            )
        return None

    async def _registry_step(self, step: Awaitable[object], name: str) -> bool:
        """Run a registry reload or refresh for a call; False (logged) when its data is bad.

        A corrupt stored value (a change counter that is not a number: ValueError) raised
        into the live call (CO-9); the call then goes on with the numbers it has. Store
        errors still raise: the call's own store handling applies.
        """
        try:
            await step
        except (ValueError, TypeError, KeyError) as exc:
            logger.error("registry_step_failed", step=name, error_type=type(exc).__name__)
            return False
        return True

    async def _hold(self, held: _Held, wait_secs: float) -> None:
        """Keep a refused call: retry admission until granted, settled or expired."""
        alias = held.envelope.model
        cls = _cls(held.envelope)
        try:
            while not self._stopping:
                await asyncio.sleep(min(max(wait_secs, 0.01), 1.0))
                # Handed back by `stop()` or let go by `_free_stuck` meanwhile: one more round
                # re-admitted a call whose message was already back (audit RT-5).
                if not self._owns(held):  # `stop()` drops what it hands back
                    return
                view = self.registry.current
                message = held.message
                if message is not None:
                    kept = await self.queues.extend(
                        message, float(view.value(alias, "queue")["visibility_secs"])
                    )
                    if not kept and held.message is message:
                        # A stale receipt: the message is another worker's now (its hold
                        # lapsed) or back on the queue. Admitting it here ran it twice.
                        logger.warning(
                            "hold_lost",
                            request_id=held.envelope.request_id,
                            model=held.envelope.model,
                        )
                        self._drop(held)
                        return
                now = self.clock.now()
                if await self._expire_if_due(held, now, retry_after=wait_secs):
                    return
                waited = now - held.position_ms / 1000.0
                if await self._model_fallback(view, alias, held, 1, waited):
                    return
                decision = await self._decide(view, alias, held, 1, frozenset())
                if decision is None:
                    return
                if decision.kind == "granted":
                    self._start_run(held, decision, 1)
                    return
                wait_secs = decision.wait_secs
            if self._owns(held) and held.run is None and not held.grants:
                # `stop()` began after this call was received and handed back the holds it
                # saw: return this one too, or it stayed hidden for the drain (FE-2).
                await self._release(held)
        except Exception as exc:  # never leave the call held with nobody working on it
            if isinstance(exc, STORE_ERRORS):
                self._store_trouble("hold")
            self._limited(
                logger.warning,
                "hold_failed",
                held.envelope.model,
                request_id=held.envelope.request_id,
                model=held.envelope.model,
                error=str(exc),
            )
            await self._release(held, unsure=True)
        finally:
            self._held_class.discard((alias, cls))
            if (event := self._wake.get(alias)) is not None:
                event.set()  # the class is free: receive its next call now

    async def _model_fallback(
        self,
        view: ResolvedRegistry,
        alias: str,
        held: _Held,
        attempt: int,
        waited: float,
        *,
        broken: bool = False,
    ) -> bool:
        """Try the model's fallback models when its rule allows; True when one granted."""
        rule = view.value(alias, "model_fallback")
        env = held.envelope
        cause = "broken" if broken else "exhausted"
        if not self._may_spill(view, alias, env, waited, cause, "model_fallback"):
            return False
        for item in rule["models"]:
            other = item["model"]
            if other not in view.registry.models or not _takes_calls(view, other):
                continue
            if not self._shape_ok(view, other, env.op, env.params, stream=env.stream):
                continue
            pct = int(view.value(other, "require_headroom_pct")) if item["require_headroom"] else 0
            ask = self._ask(
                view, other, held, attempt, frozenset(), foreign_from=alias, require_pct=pct
            )
            if ask is None:
                continue
            decision = await self._pick_take(held, ask)
            if decision.kind == "granted":
                # The run (and its grant in `held.grants`) first; the status is informational,
                # written behind. A failed status write before it left the grant neither run
                # nor given back, holding a slot until its lease ended (audit RT-10).
                self._start_run(held, decision, attempt, served=other)
                self._spawn(
                    self.requests.set_status(env.request_id, "in_progress", served_model=other),
                    "fallback_status",
                )
                return True
        return False

    # ================================================================ worker: running

    def _owns(self, held: _Held) -> bool:
        """True while this process still holds the call (not dropped, handed back or freed)."""
        return self._held.get(held.envelope.request_id) is held

    def _start_run(
        self, held: _Held, decision: Decision, attempt: int, served: str | None = None
    ) -> None:
        alias = served or held.envelope.model
        owner = self._owner(held, attempt)
        if not self._owns(held):
            # Let go while its admission was in flight (`_free_stuck`, `stop`, a lost hold):
            # a run would hold a grant the heartbeat never renews, swept only after its lease
            # (scenario L4 left leases behind). The grant goes straight back.
            logger.warning("grant_for_released_call", request_id=held.envelope.request_id)
            self._spawn(self.admission.undo(alias, owner, held.envelope.request_id))
            return
        held.grants.add((alias, owner))
        task = asyncio.create_task(self._run(held, decision, attempt, alias))
        self._running.add(task)
        task.add_done_callback(self._running.discard)

    def _spawn(self, work: Awaitable[Any], what: str = "background_write") -> asyncio.Task[None]:
        """Run a best-effort store write behind, tracked (stopped by `stop`), errors logged."""

        async def run() -> None:
            try:
                await work
            except Exception as exc:
                logger.warning(f"{what}_failed", error=str(exc), error_type=type(exc).__name__)

        task = asyncio.create_task(run())
        self._running.add(task)
        task.add_done_callback(self._running.discard)
        return task

    async def _abandon(self, rid: str) -> None:
        """Drop this caller's unconfirmed record after a failed send: best effort, bounded.

        Its failure is logged, never raised over the send's own error; a cancelled caller
        waits for it at most the probe timer (it goes on behind).
        """
        task = self._spawn(self.requests.abandon(rid, caller=self.worker_id), "abandon")
        await asyncio.wait({task}, timeout=self.settings.timings.health_probe_secs)

    async def _run(self, held: _Held, decision: Decision, attempt: int, alias: str) -> None:
        slots = self._slots.get(held.envelope.model)
        held.run = asyncio.current_task()
        try:
            if held.cancelled:  # its cancel arrived before this task ran
                raise asyncio.CancelledError
            if slots is None:
                await self._attempts(held, decision, attempt, alias)
            else:
                async with slots:
                    await self._attempts(held, decision, attempt, alias)
        except asyncio.CancelledError:
            if held.shutdown:  # the drain ran out (`stop`): give it back, never fail it
                # A kept task: `stop()` waits for it before the clients close. Shielded as a
                # bare coroutine, it was an orphan still writing to closing clients (RT-6).
                settle = asyncio.create_task(self._shutdown_call(held))
                self._settling.add(settle)
                settle.add_done_callback(self._settling.discard)
                await asyncio.shield(settle)
                return
            if not held.cancelled:
                raise
            # Cancelled by a caller: give the grant back, reply `cancelled`. A stream that sent
            # chunks ends with an error entry first: its caller waited out its idle timer with
            # no `partial_text`, and the flush timer wrote one more batch (R3ST-3). Shielded: a
            # second cancel of this task (the channel and the key check both saw the cancel)
            # killed the cleanup, and the call stayed `running` until its caller's deadline.
            settle = asyncio.create_task(self._settle_cancelled(held))
            self._settling.add(settle)
            settle.add_done_callback(self._settling.discard)
            while not settle.done():
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.shield(settle)
        except Exception as exc:
            # A store outage, a store refusal (OOM) or a bug mid-call. Never leave the call held
            # with nobody working on it (the heartbeat would hide its message for good): give
            # the grants back and return the message, so it runs again (at least once; a stored
            # result is reused). A direct call has no message: its caller gets the error.
            if isinstance(exc, (*STORE_ERRORS, RedisError)):
                self._store_trouble("run")
            self._limited(
                logger.warning,
                "run_failed",
                held.envelope.model,
                request_id=held.envelope.request_id,
                model=held.envelope.model,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            await self._release(held)
            if held.message is None:
                raise
        finally:
            if slots is not None and (event := self._wake.get(held.envelope.model)) is not None:
                event.set()  # a worker slot is free: the model's loop receives at once

    async def _settle_cancelled(self, held: _Held) -> None:
        """End a call cancelled while it ran: grant back, `cancelled` to every caller."""
        try:
            partial = await self._end_stream(held, "cancelled", "the call was cancelled")
            await self._give_back(held)
            await self._finish_call(
                held, "cancelled", "cancelled", final="cancelled", partial=partial
            )
        except Exception as exc:
            logger.warning("cancel_failed", request_id=held.envelope.request_id, error=str(exc))
            await self._release(held)

    async def _attempts(self, held: _Held, decision: Decision, attempt: int, alias: str) -> None:
        throttles = 0
        tries = 0
        failed: set[str] = set()
        retry_secs = 0.0
        while True:
            view = self.registry.current
            dep_key = decision.deployment or ""
            owner = self._owner(held, attempt)
            held.grants.add((alias, owner))
            result = await self._attempt(
                view, alias, held, dep_key, owner, attempt, decision.extra.get("streamed")
            )
            held.grants.discard((alias, owner))
            outcome, kind, info, exc = result.outcome, result.kind, result.info, result.exc
            # Which deployment failed, and how: failures carried no account or region (R3OB-4).
            account, region = _where(view, alias, dep_key)
            self.metrics.count(
                "llmq_attempts_total",
                model=alias,
                account=account,
                region=region,
                result="ok" if outcome == "ok" else (kind or outcome),
            )
            if outcome == "ok":
                return
            if outcome in ("stream_failed", "invalid"):  # never switched once chunks were sent
                await self._finish_call(held, kind, str(exc), partial=info)
                return
            # A move to a deployment this call has not tried is free: it is failover, not a
            # retry storm (bounded by the deployments and max_attempts). Counting it failed
            # calls as retry_budget_exhausted while another region was healthy (scenario P3:
            # 20 of 60 calls with one region hanging).
            tried = frozenset(failed | {dep_key})
            age = self.clock.now() - held.position_ms / 1000.0
            untried = any(
                not c.last
                for c in self._candidates(view, alias, held.envelope, age, tried, "error")
            )
            if outcome == "throttle":
                # The throttled deployment is out for every pod until its window ends (P2, in
                # FINISH): another one is tried at once; with none left the call waits for it.
                # Throttles count in the retry budget (always counted, enforced like a 5xx's).
                retry_secs = 0.0
                throttles += 1
                limit = int(view.value(alias, "throttle_retry_attempts", dep_key))
                budget_ok = await self._retry_allowed(alias, view)
                if throttles > limit or (not budget_ok and not untried):
                    await self._finish_call(
                        held, "rate_limited", "the provider kept throttling", retry_after=info
                    )
                    return
            else:
                tries += 1
                if outcome == "fatal" or tries >= int(view.value(alias, "max_attempts")):
                    known = kind in ("invalid_request", "context_length_exceeded")
                    await self._finish_call(held, kind if known else "provider_error", str(exc))
                    return
                if kind == "target" and not untried:
                    # The model is missing on the only deployment left: it refuses again however
                    # long the backoff (RESCAN P10).
                    await self._finish_call(held, "provider_error", str(exc))
                    return
                if not untried and not await self._retry_allowed(alias, view):
                    await self._finish_call(held, "retry_budget_exhausted", str(exc))
                    return
                # A target or model error moves to another deployment at once: the backoff is
                # for a struggling deployment, and this one is not tried again (P9, P10).
                quick = kind in ("target", "model_error") and untried
                retry_secs = 0.0 if quick else self._backoff(view, alias, dep_key, tries)
            self.metrics.count("llmq_retries_total", model=alias, reason=str(kind))
            failed.add(dep_key)
            attempt += 1
            if retry_secs:  # never past the deadline: the caller is told in time
                left = self._deadline(held) - self.clock.now()
                await asyncio.sleep(max(0.0, min(retry_secs, left)))
            while True:
                if await self._past_deadline(held, self.clock.now()):
                    await self._finish_call(held, "deadline_exceeded", f"last error: {kind}")
                    return
                # The view now: one read before the failed attempt still offered a deployment
                # a reload had removed (FE-13).
                view = self.registry.current
                next_decision = await self._decide(view, alias, held, attempt, frozenset(failed))
                if next_decision is None:
                    return
                if next_decision.kind == "granted":
                    decision = next_decision
                    break
                if next_decision.kind == "busy":
                    await self._finish_call(held, "rate_limited", "fallback model is busy")
                    return
                await asyncio.sleep(min(next_decision.wait_secs, 1.0))

    def _backoff(self, view: ResolvedRegistry, alias: str, dep: str, tries: int) -> float:
        base = float(view.value(alias, "retry_base_secs", dep))
        factor = float(view.value(alias, "retry_multiplier", dep))
        spread = float(view.value(alias, "retry_spread", dep))
        low = base * factor ** (tries - 1)
        return min(
            low * (1 + (spread - 1) * self._rng.random()),
            float(view.value(alias, "retry_max_secs", dep)),
        )

    async def _retry_allowed(self, alias: str, view: ResolvedRegistry) -> bool:
        """Count one retry; True when it fits the model's retry budget over a sliding minute.

        The previous minute's counts weigh by the part of it still inside the last 60 s: a
        fixed minute let a burst of retries through at every minute turn (RESCAN P10).
        """
        now = self.clock.now()
        minute = int(now // 60)
        weight = 1.0 - (now % 60) / 60
        key = self.keys.model(alias, f"retries:{minute}")
        async with self.state.pipeline(transaction=False) as pipe:
            pipe.hincrby(key, "retries", 1)
            pipe.hget(key, "first")
            pipe.hmget(self.keys.model(alias, f"retries:{minute - 1}"), ["retries", "first"])
            self.expiry.expire(pipe, key, 180)  # last: its replies are not read
            retries, first, (old_retries, old_first) = (await pipe.execute())[:3]
        used = int(retries) + int(old_retries or 0) * weight
        first_n = int(first or 0) + int(old_first or 0) * weight
        allowed = max(
            float(view.value(alias, "retry_budget_min")),
            first_n * int(view.value(alias, "retry_budget_pct")) / 100,
        )
        return used <= allowed

    async def _attempt(
        self,
        view: ResolvedRegistry,
        alias: str,
        held: _Held,
        dep_key: str,
        owner: str,
        attempt: int,
        streamed: int | None = None,
    ) -> _AttemptResult:
        """One provider attempt and its settlement (see `_AttemptResult`).

        `streamed`: the chunk stream's length its grant reported (read here when None). A first
        attempt was counted for the retry budget by its grant (PICK_TAKE, `count_first`).
        """
        env = held.envelope
        # This grant's FINISH: alias, owner and request id were repeated at nine sites (FE-22).
        settle = functools.partial(self.admission.finish, alias, owner, env.request_id)
        entry = view.registry.models.get(alias)
        deployment = (
            next((d for d in view.deployments(alias) if d.key == dep_key), None) if entry else None
        )
        if entry is None or deployment is None:
            # A reload removed the granted deployment (or model) since the grant: give the grant
            # back in full and try another. `next()` without a default raised StopIteration,
            # logged as a bare `run_failed` and the call was run again (audit RT-16).
            charged = held.estimates.get((alias, owner))
            await settle(
                refund_r=1,
                refund_t=charged.total if charged else 0,
                outcome="none",
            )
            gone = RuntimeError(f"{alias} {dep_key} was removed by a registry reload")
            logger.warning("granted_deployment_removed", model=alias, deployment=dep_key)
            return _AttemptResult("retry", "target", None, gone)
        route = route_for(view, deployment)
        # Refunds settle what admission charged: the estimate kept for this grant (`_ask`, from
        # model-level params). One from this deployment's params (a larger account max_tokens)
        # or from a registry reloaded since refunded more than was taken (0.3.0 review, A4).
        estimate = held.estimates.get((alias, owner)) or self._charged(view, alias, held)
        try:
            params = self._params(view, alias, env, dep_key)
        except inference.InferenceRejected as exc:  # this deployment's own limit (RESCAN P4)
            await settle(refund_r=1, refund_t=estimate.total, outcome="none")
            return _AttemptResult("invalid", "invalid_request", None, exc)
        remaining = max(0.1, self._deadline(held) - self.clock.now())
        attempt_budget = float(view.value(alias, "attempt_timeout_secs", dep_key))
        # One first-response budget, never shortened below `attempt_timeout_secs` but by the
        # deadline itself: a timer halved for the deadline was not the provider's failure, so a
        # hung region never tripped its breaker (RESCAN P3). `attempt_secs` bounds a plain
        # Converse answer (the model's `converse_api`, plan P7); a stream to the caller always
        # uses ConverseStream.
        budget = min(attempt_budget, remaining)
        timing = Timing(
            first_response_secs=budget,
            idle_secs=float(view.value(alias, "stream_idle_secs")),
            attempt_secs=budget,
        )
        provider = self.providers.get(entry.provider)
        held.meta[owner] = {
            "alias": alias,
            "owner": owner,
            "request_id": env.request_id,
            "dep": dep_key,
            "cls": _cls(env),
            "cost_r": 1,
            "cost_t": estimate.total,
        }
        served_op = _served_op(view, alias, env)
        writer = held.writer = None
        if env.stream and env.op in ("converse", "chat"):
            writer = held.writer = ChunkWriter(
                self.state,
                self.keys.request(env.request_id, "chunks"),
                owner,
                flush_ms=int(view.value(alias, "stream_flush_ms", dep_key)),
                max_bytes=int(view.value(alias, "stream_max_bytes")),
                ttl_secs=self.settings.result_ttl_secs + int(env.timeout_secs),
                expiry=self.expiry,
            )
            if streamed is None:
                streamed = await self.state.xlen(writer.key)
            if streamed:
                # Another worker already streamed part of this answer (plan R19).
                await settle(
                    refund_r=1,
                    refund_t=estimate.total,
                    outcome="none",
                    latency_ms=-1,
                )
                lost = "the worker serving this stream was lost"
                await writer.error("worker_lost", lost)
                return _AttemptResult("stream_failed", "worker_lost", None, RuntimeError(lost))
        held.last_dep, held.attempts = dep_key, attempt
        if not held.granted_secs:
            held.granted_secs = self.clock.now()
        labels = {"model": alias, "account": deployment.account, "region": deployment.region}
        started = time.monotonic()
        sink = None
        if writer is not None:
            add = writer.add
            first = [True]

            async def sink(event: Any) -> None:
                # Time to first token (R3OB-3): one check per chunk.
                if first[0]:
                    first[0] = False
                    elapsed = time.monotonic() - started
                    self.metrics.observe("llmq_first_token_seconds", elapsed, **labels)
                await add(event)

        outer = asyncio.timeout(remaining)  # the call's own deadline
        try:
            async with outer:
                result = await provider.call(
                    served_op,
                    params,
                    route,
                    timing,
                    sink=sink,
                    **_api_kwargs(provider, view, alias),
                )
            if writer is not None:
                # Usage and how the call was served, for `stream(meta_event=True)`: a stream
                # caller never sees the call's `Reply` (R3ST-9).
                await writer.end(
                    {
                        "inputTokens": result.usage.input_tokens,
                        "outputTokens": result.usage.output_tokens,
                        "cacheReadInputTokens": result.usage.cache_read_tokens,
                        "cacheWriteInputTokens": result.usage.cache_write_tokens,
                    },
                    {
                        "served_model": alias,
                        "deployment": dep_key,
                        "fallback": "model"
                        if alias != env.model
                        else ("region" if deployment.group == "fallback" else None),
                        "attempts": attempt,
                        "tokens_in": result.usage.input_tokens,
                        "tokens_out": result.usage.output_tokens,
                        "worker": self.worker_id,
                        "provider_request_id": result.request_id,
                    },
                )
        except Exception as exc:
            latency = (time.monotonic() - started) * 1000
            self.metrics.observe("llmq_provider_seconds", latency / 1000, **labels)  # (R3OB-3)
            info = provider.classify(exc)
            # Not the provider's doing (RESCAN P9): the stream's size cap, or the store under
            # the chunk writer. Counted as failures they opened healthy deployments' breakers.
            ours = isinstance(exc, StreamTooLarge | RedisError)
            if writer is not None and writer.sent:
                # Chunks reached the caller: end the stream with the partial text (plan §3.13).
                # The call's own deadline is not the deployment's fault: it charged the breaker
                # and reached the caller as `provider_error` with an empty message (R3ST-2).
                late = outer.expired()
                # Input plus the output written so far stay charged; the rest of the output
                # reservation goes back. `cut` kept all of it, for our own failures too (R3ST-13).
                written = math.ceil(
                    tokens.input_tokens("converse", {"t": writer.partial_text})
                    * float(view.value(alias, "tpm_output_weight", dep_key))
                )
                await settle(
                    refund_r=0,
                    refund_t=max(0, estimate.output - written),
                    outcome="none" if ours or late else "fail",
                    latency_ms=-1 if late else latency,
                )
                # The store under the chunk writer is the store's kind, as a caller meets it:
                # `provider_error` here failed scenario L2 (a store block cut streams).
                if late:
                    kind = "deadline_exceeded"
                elif isinstance(exc, RedisError):
                    kind = "queue_unavailable"
                elif info.kind is ErrorKind.THROTTLE:
                    kind = "rate_limited"
                else:
                    kind = "provider_error"
                if late:
                    message = "the call's deadline passed mid-stream"
                elif isinstance(exc, StreamTooLarge):
                    message = "stream above stream_max_bytes"
                else:
                    message = str(exc)
                try:
                    await writer.error(kind, message)
                except Exception as end_exc:  # the caller's idle timer ends its stream (RT-24)
                    logger.warning(
                        "stream_end_failed", request_id=env.request_id, error=str(end_exc)
                    )
                return _AttemptResult("stream_failed", kind, writer.partial_text, exc)
            if ours:  # nothing sent: the call is given back and runs again (`_run`)
                await settle(
                    refund_r=1,
                    refund_t=estimate.total,
                    outcome="none",
                )
                held.grants.discard((alias, owner))
                raise
            cut = isinstance(exc, AttemptTimeout) and "chunk" in str(exc)
            # The call's own deadline (the outer timer) cut this attempt: not the provider's
            # fault. Counted as a failure, late grants under a rate limit opened the breaker
            # (scripts/load.py). Only that timer: an attempt's own first-response or idle
            # timeout is the provider's, so a hung region trips its breaker (RESCAN P3).
            if outer.expired():
                await settle(
                    refund_r=0,
                    refund_t=0,
                    outcome="none",
                    latency_ms=-1,
                )
                return _AttemptResult("retry", "retryable", None, exc)
            if info.kind is ErrorKind.THROTTLE:
                refund = tokens.refund(estimate, "throttle")
                await settle(
                    refund_r=refund.requests,
                    refund_t=refund.tokens,
                    outcome="throttle",
                    latency_ms=-1,
                    retry_after_secs=info.retry_after_secs,
                )
                return _AttemptResult("throttle", "throttled", info.retry_after_secs, exc)
            model_error = error_family(info.code) in ("modelerror", "modeltimeout")
            if info.kind is ErrorKind.RETRYABLE and model_error and not held.model_retried:
                # ModelErrorException / ModelTimeoutException: the model's answer failed, not the
                # region; one retry on another deployment, without charging the breaker (P9).
                held.model_retried = True
                refund = tokens.refund(estimate, "fail")
                await settle(
                    refund_r=refund.requests,
                    refund_t=refund.tokens,
                    outcome="none",
                )
                return _AttemptResult("retry", "model_error", None, exc)
            if info.kind is ErrorKind.RETRYABLE or isinstance(exc, TimeoutError):
                refund = tokens.refund(estimate, "cut" if cut else "fail")
                await settle(
                    refund_r=refund.requests,
                    refund_t=refund.tokens,
                    outcome="fail",
                    latency_ms=latency,
                )
                return _AttemptResult("retry", "retryable", None, exc)
            refund = tokens.refund(estimate, "throttle")  # refused before it ran: all back
            await settle(
                refund_r=refund.requests,
                refund_t=refund.tokens,
                outcome="none",
                latency_ms=-1,
            )
            if is_target_error(exc, info):
                return await self._target_error(view, alias, held, dep_key, exc)
            if isinstance(exc, CredentialsUnavailable) or is_auth_error(exc, info):
                # FC-2: a slow refresh keeps its client and shared refresh task, and only its
                # first cut call takes a back-off step (five cut together doubled it five times).
                slow = exc if isinstance(exc, CredentialsRefreshSlow) else None
                if slow is None:
                    self.providers.pool.forget(route)
                where = (deployment.account, deployment.region)
                last = self._bad_accounts.get(where)
                if slow is None or slow.first or last is None:
                    backoff = min((last.backoff_secs if last else 2.5) * 2, 60.0)
                    self._bad_accounts[where] = _Backoff(time.monotonic() + backoff, backoff)
                # Expired token, AccessDenied and a missing secret looked the same (R3OB-8).
                logger.error(
                    "account_credentials_failed",
                    account=deployment.account,
                    region=deployment.region,
                    model=alias,
                    request_id=env.request_id,
                    error_type=type(exc).__name__,
                    error=str(exc)[:300],
                )
                self.metrics.count(
                    "llmq_credentials_failed_total",
                    account=deployment.account,
                    region=deployment.region,
                )
                return _AttemptResult("retry", "auth", None, exc)
            # The request itself was refused: its own kind, not `provider_error` (plan P8).
            return _AttemptResult("fatal", request_error_kind(exc, info) or "fatal", None, exc)
        latency = (time.monotonic() - started) * 1000
        self.metrics.observe("llmq_provider_seconds", latency / 1000, **labels)  # (R3OB-3)
        used = tokens.used_tokens(
            result.usage,
            output_weight=float(view.value(alias, "tpm_output_weight", dep_key)),
            count_cached=bool(view.value(alias, "tpm_count_cached", dep_key)),
        )
        refund = tokens.refund(estimate, "ok", used)
        meta = Meta(
            served_model=alias,
            deployment=dep_key,
            fallback="model"
            if alias != env.model
            else ("region" if deployment.group == "fallback" else None),
            attempts=attempt,
            queue_wait_ms=int(max(0.0, held.first_seen_secs - held.position_ms / 1000.0) * 1000),
            provider_ms=int(latency),
            tokens_in=result.usage.input_tokens,
            tokens_out=result.usage.output_tokens,
            worker=self.worker_id,
            provider_request_id=result.request_id,
        )
        # In the caller's shape: a fallback model of the other provider answered (Q40).
        response = shapes.reply(result.response, env.op, served_op, model=alias)
        if env.vectors:
            response = _packed(view, env.model, response)
        reply = Reply(request_id=env.request_id, ok=True, response=response, meta=meta)
        self._bad_accounts.pop((deployment.account, deployment.region), None)  # (plan A4)
        self._record(held, alias, deployment.account, deployment.region, "ok", meta)
        if self.settings.debug_capture_allowed and view.value(alias, "debug_capture"):
            # Before the result (so it is there when the caller reads it), and never failing
            # the call: a store error here released a call that had succeeded, and it ran
            # again at the provider (audit RT-12).
            try:
                await self._debug(env.request_id, params, result.response)  # (never in prod)
            except Exception as exc:
                logger.warning("debug_capture_failed", request_id=env.request_id, error=str(exc))
        status = {
            "deployment": dep_key,
            "served_model": alias,
            "attempts": attempt,
            "tokens_in": result.usage.input_tokens,
            "tokens_out": result.usage.output_tokens,
        }
        if self.queues_shared and held.message is not None:
            await self._settle_done(held, alias, owner, reply, status, refund, latency)
            await self._count_target_errors(held, succeeded=True)
            return _AttemptResult("ok")
        # The caller first: its result, with the final status in the same script. Then the
        # grant and the message are settled together (four round trips one after another
        # before the caller was woken, before).
        # Result and ack through a store outage: the answer is in hand, and a failed write
        # released the message, which ran a second time once it showed again (failover drills:
        # 1 call run twice, 61 s). Held meanwhile, so the heartbeat keeps its grant and message.
        await self._through_outage(
            lambda: self.requests.put_result(reply, final_state="done", status=status)
        )
        held.answered = True
        # Settled from here: a failure below must neither renew the grant nor refund the whole
        # estimate of a call that succeeded (`_release`). `return_exceptions`: a FINISH refused
        # (out of memory) raised out of the gather while the ack ran on as an orphan, and the
        # grant was still listed, so the call was refunded in full and run again (RT-11).
        held.grants.discard((alias, owner))
        finished, acked = await asyncio.gather(
            self.admission.finish(  # skipped on a store error: SWEEP frees its slot
                alias,
                owner,
                env.request_id,
                refund_r=refund.requests,
                refund_t=refund.tokens,
                outcome="ok",
                latency_ms=latency,
            ),
            self._through_outage(lambda: self._ack(held), held),
            return_exceptions=True,
        )
        if isinstance(finished, BaseException):  # SWEEP frees the slot after its lease
            logger.warning(
                "finish_failed",
                model=alias,
                request_id=env.request_id,
                error=str(finished),
                error_type=type(finished).__name__,
            )
        if isinstance(acked, BaseException):
            raise acked
        await self._count_target_errors(held, succeeded=True)
        return _AttemptResult("ok")

    async def _settle_done(
        self,
        held: _Held,
        alias: str,
        owner: str,
        reply: Reply,
        status: Mapping[str, Any],
        refund: tokens.Refund,
        latency: float,
    ) -> None:
        """Result, ack and FINISH of a call that succeeded, in one round trip (FL-22).

        Queues in the state store only. It was RESULT_PUT, then FINISH and ACK together: two
        round trips, three scripts, and a window where the result was stored while the slot
        waited. Through a store outage as the result and ack were (the answer is in hand).
        """
        message = held.message
        assert message is not None  # the caller checks: a direct run has no message
        env = held.envelope
        finish = self.admission.finish_args(
            alias,
            owner,
            env.request_id,
            refund_r=refund.requests,
            refund_t=refund.tokens,
            outcome="ok",
            latency_ms=latency,
        )
        out: list[Settled] = []

        async def write() -> None:
            out.append(
                await self.requests.put_result_settled(
                    reply,
                    final_state="done",
                    status=status,
                    queue_keys=self.queues.queue_keys(message.queue),
                    message_id=message.id,
                    generation=message.generation,
                    finish=finish,
                )
            )

        await self._through_outage(write, held)
        held.answered = True
        held.grants.discard((alias, owner))  # settled: never renewed or refunded again
        _, acked, settled, replayed = out[-1]
        if replayed:
            # The script ran before and its reply was lost (a retried connection): its ack
            # and settle happened then. Not a double run (A-7).
            logger.info("settle_replayed", request_id=env.request_id, model=env.model)
        elif settled == 0:
            self.admission.grant_lost(alias, env.request_id)
        if not acked and not replayed:
            logger.warning("ack_stale_receipt", request_id=env.request_id, model=env.model)
            self.metrics.count("llmq_stale_acks_total", model=env.model)
        self._drop(held)

    async def _target_error(
        self, view: ResolvedRegistry, alias: str, held: _Held, dep_key: str, exc: BaseException
    ) -> _AttemptResult:
        """Note a target error; a request failing this way on two deployments is the cause.

        Nothing is counted yet: `_count_target_errors` records them once the call's outcome
        shows whether the deployment or the request was at fault (plan §3.18 R16).
        """
        held.target_failed.append((alias, dep_key))
        if len({d for _, d in held.target_failed}) >= 2:
            return _AttemptResult("fatal", "request", None, exc)
        return _AttemptResult("retry", "target", None, exc)

    async def _count_target_errors(self, held: _Held, *, succeeded: bool) -> None:
        """Count noted target errors: after a success elsewhere, or a failure on one deployment."""
        noted = held.target_failed
        held.target_failed = []
        if not noted or (not succeeded and len({d for _, d in noted}) >= 2):
            return
        view = self.registry.current
        for alias, dep_key in noted:
            settings = view.value(alias, "health", dep_key)
            # Only deployments that take calls count as usable (A-3, owner Q-3): a disabled,
            # blocked or out-of-residency one let the real last deployment be disabled, and
            # the model was `model_unavailable` until a probe recovered it.
            others = [
                d.key
                for d in view.deployments(alias)
                if d.key != dep_key and _dep_takes_calls(view, alias, d)
            ]
            outcome = await self.health.record_target_error(
                alias,
                dep_key,
                held.envelope.request_id,
                others=others,
                window_secs=float(settings["disable_window_secs"]),
                needed=int(settings["disable_errors"]),
                probe_after_secs=float(settings["probe_after_secs"]),
            )
            if outcome == "disabled":
                logger.error("deployment_disabled", model=alias, deployment=dep_key)
            elif outcome == "last":
                logger.error("deployment_failing_last_usable", model=alias, deployment=dep_key)

    async def _prober(self) -> None:
        """One process per model probes disabled deployments when they are due."""
        key = f"{self.keys.prefix}:lock:probe"
        while True:
            await asyncio.sleep(PROBE_EVERY_SECS)
            try:
                # One lock for every model (one SET per model per 5 s from every pod before).
                lock = await self.state.set(key, self.worker_id, nx=True, ex=int(PROBE_EVERY_SECS))
            except Exception as exc:
                logger.warning("prober_failed", error=str(exc))
                continue
            if not lock:
                continue
            for alias in self.registry.current.registry.models:
                try:
                    await self.probe_due(alias, lock_key=key)
                except Exception as exc:  # one model's failure does not skip the others
                    logger.warning("prober_failed", model=alias, error=str(exc))
            # Back to one pass per period: a probe stretched the lock to 35 s, and no pod
            # (this one included) probed again for that long after the last probe (RV-N6).
            try:
                await self.state.expire(key, int(PROBE_EVERY_SECS))
            except Exception as exc:
                logger.warning("prober_failed", step="lock", error=str(exc))

    async def probe_due(self, alias: str, *, lock_key: str | None = None) -> dict[str, str]:
        """Probe every disabled deployment of a model that is due; their outcomes.

        `lock_key`: the prober's lock, held on for each probe's whole budget. A pass longer
        than the lock's 5 s let another pod take it and probe the same deployments at the
        same time (audit RT-27).
        """
        view = self.registry.current
        now = self.clock.now()
        out: dict[str, str] = {}
        for dep_key, due in (await self.health.disabled(alias)).items():
            if due > now + 0.5:  # the local view of the store clock may lag slightly
                continue
            if lock_key is not None:
                await self.state.expire(lock_key, int(PROBE_TIMEOUT_SECS + PROBE_EVERY_SECS))
            ok = await self._probe(view, alias, dep_key)
            settings = view.value(alias, "health", dep_key)
            out[dep_key] = await self.health.probed(
                alias,
                dep_key,
                ok=ok,
                probe_max_secs=float(settings["probe_max_secs"]),
                probe_after_secs=float(settings["probe_after_secs"]),  # shortest wait (EN-23)
            )
            if out[dep_key] == "recovered":
                logger.info("deployment_recovered", model=alias, deployment=dep_key)
        return out

    async def _probe(self, view: ResolvedRegistry, alias: str, dep_key: str) -> bool:
        """One small real call on a deployment: reachable and allowed (a throttle counts)."""
        entry = view.registry.models[alias]
        deployment = next((d for d in view.deployments(alias) if d.key == dep_key), None)
        if deployment is None:
            return True
        op = _op_of(view, alias)
        name = view.value(alias, "probe")
        hook = hooks.get("probe", name)
        params = (
            hooks.call_safely("probe", name, hook, alias, op, default=None) if hook else None
        ) or _probe_params(entry.provider, entry.type, entry.model_id, entry.family)
        provider = self.providers.get(entry.provider)
        try:
            await provider.call(
                op,
                params,
                route_for(view, deployment),
                Timing(PROBE_TIMEOUT_SECS, PROBE_TIMEOUT_SECS, PROBE_TIMEOUT_SECS),
                **_api_kwargs(provider, view, alias),  # the calls' own API (IAM)
            )
        except Exception as exc:
            return provider.classify(exc).kind is ErrorKind.THROTTLE
        return True

    # ================================================================ worker: settling

    async def _reply(
        self,
        env: Envelope,
        kind: str,
        message: str,
        *,
        final: str = "failed",
        retry_after: float | None = None,
        partial: str | None = None,
    ) -> None:
        reply = Reply(
            request_id=env.request_id,
            ok=False,
            error=kind,
            message=message,
            retry_after_secs=retry_after,
            partial_text=partial or None,
            meta=Meta(served_model=env.model, worker=self.worker_id),
        )
        await self.requests.put_result(reply, final_state=final, status={"error": kind})

    async def _finish_call(
        self,
        held: _Held,
        kind: str,
        message: str,
        *,
        final: str = "failed",
        retry_after: float | None = None,
        partial: str | None = None,
    ) -> None:
        await self._reply(
            held.envelope, kind, message, final=final, retry_after=retry_after, partial=partial
        )
        await self._count_target_errors(held, succeeded=False)
        # The last deployment tried, so a failing region or account shows (R3OB-4).
        view = self.registry.current
        alias = held.envelope.model
        account, region = _where(view, alias, held.last_dep)
        self._record(held, alias, account, region, kind, None, error=message)
        await self._ack(held)

    def _record(
        self,
        held: _Held,
        alias: str,
        account: str,
        region: str,
        outcome: str,
        meta: Meta | None,
        *,
        error: str | None = None,
    ) -> None:
        """Metrics and the durable per-call record (`llmq_call_finished`, no text; E134).

        Waits and durations (R3OB-2, R3OB-3): `llmq_wait_seconds` until a worker took the
        call, `llmq_admission_wait_seconds` until its first grant (the budget hold included),
        `llmq_call_seconds` until now, by outcome.
        """
        env = held.envelope
        priority = "1" if env.priority else "0"
        start_secs = held.position_ms / 1000.0
        wait = max(0.0, held.first_seen_secs - start_secs)
        now = self.clock.now()
        tenant = self._tenant_label(env.tenant_id)  # LLMQ_METRICS_TENANTS (0.2.0 T6)
        self.metrics.observe(
            "llmq_wait_seconds", wait, model=env.model, priority=priority, tenant=tenant
        )
        admit = max(0.0, held.granted_secs - start_secs) if held.granted_secs else None
        if admit is not None:
            self.metrics.observe(
                "llmq_admission_wait_seconds", admit, model=env.model, priority=priority
            )
        total = max(0.0, now - start_secs)
        self.metrics.observe("llmq_call_seconds", total, model=env.model, outcome=outcome)
        self._outcome(
            env,
            alias,
            account,
            region,
            outcome,
            meta,
            "queue" if held.message is not None else "direct",
            deployment=(meta.deployment if meta else None) or held.last_dep or None,
            attempts=(meta.attempts if meta else None) or held.attempts or None,
            queue_wait_ms=int(wait * 1000),
            admit_wait_ms=None if admit is None else int(admit * 1000),
            duration_ms=int(total * 1000),
            error=error[:300] if error else None,
        )

    def _outcome(
        self,
        env: Envelope,
        alias: str,
        account: str,
        region: str,
        outcome: str,
        meta: Meta | None,
        path: str,
        **fields: Any,
    ) -> None:
        """`log_outcome` with this engine's sink, id and hash."""
        log_outcome(
            self.metrics,
            self.worker_id,
            env,
            alias,
            account,
            region,
            outcome,
            meta,
            path,
            tenant_label=self._tenant_label(env.tenant_id),
            **fields,
        )

    async def _debug(
        self, request_id: str, params: Mapping[str, Any], response: Mapping[str, Any]
    ) -> None:
        """`debug_capture`: truncated text for at most 60 minutes (plan §3.12)."""
        import orjson

        body = orjson.dumps(
            {
                "request": orjson.dumps(params).decode()[:2000],
                "response": orjson.dumps(response).decode()[:2000],
            }
        )
        await self.state.set(self.keys.request(request_id, "debug"), body, ex=3600)

    async def status(
        self,
        model: str | None = None,
        *,
        tenant: str | None = None,
        tenant_hash: str | None = None,
    ) -> dict[str, Any]:
        """Status of one model, or of everything (plan §3.12).

        With `tenant` (a raw name, hashed here) or `tenant_hash` (`-`: no tenant), the model's
        report adds that tenant's block (0.2.0 T4); a tenant needs a model (`ConfigError`).
        """
        view = self.registry.current
        if tenant is not None and tenant_hash is not None:
            raise ConfigError("give tenant or tenant_hash, not both")
        h = self.hash_id(tenant) if tenant is not None else tenant_hash
        if model is not None:
            alias = view.registry.alias_of(model)
            return await self.status_reader.model(view, alias, tenant_hash=h)
        if h is not None:
            raise ConfigError("a tenant's status needs a model")
        return await self.status_reader.report(view)

    async def request_status(self, request_id: str) -> dict[str, Any]:
        """Status of one call (empty when unknown or expired)."""
        return await self.status_reader.request(request_id)

    async def health_check(self, *, fresh: bool = False) -> dict[str, Any]:
        """Liveness and readiness (plan E124), answered from cached state at once.

        Ready = registry loaded and not draining; the stores' state is reported but never
        makes the process unready. It comes from `_store_prober` every few seconds (`fresh`:
        probe now), so a slow store never makes a readiness probe slow (RESCAN O1). Live is
        false when a background loop died or the heartbeat stopped making progress (O2).
        """
        cached = self._stores
        stale = cached is None or time.monotonic() - cached[0] > 3 * HEALTH_PROBE_EVERY_SECS
        if fresh or stale or cached is None:
            stores = await self._probe_stores()
        else:
            stores = cached[1]
        loaded = self.registry._current is not None
        out: dict[str, Any] = {
            "live": self._live(),
            "ready": bool(self.ready and loaded),
            "mode": "queue",
        }
        out["draining"] = self._paused
        out["draining_since"] = self._draining_since
        if self._paused:
            # The operator's action is the first reason (0.2.0 A3); it was a bare 503.
            out["not_ready_reason"] = "draining"
        evicting = self._evicting()
        if evicting:
            # Strict mode and a store that may evict queue keys, results and caller keys
            # (owner Q10): not ready, with the reason. It was a warning at start only.
            out["ready"] = False
            out.setdefault("not_ready_reason", evicting)
        if not out["live"]:
            out["live_reason"] = self._not_live_reason()
        out["worker"] = self.worker_id
        out["version"] = __version__
        current = self.registry._current
        out["registry_version"] = current.registry.version if current is not None else None
        out["held"] = len(self._held)
        out["heartbeat_age_secs"] = round(time.monotonic() - self._beat_ran_mono, 1)
        out["stores"] = stores
        out["eviction_policy"] = self._eviction.get("state", UNKNOWN_POLICY)
        out["key_expiry"] = self.expiry.mode
        if "queue" in self._eviction:
            out["queue_eviction_policy"] = self._eviction["queue"]
        return out

    async def _probe_stores(self) -> dict[str, str]:
        """Each store's state from a short-lived write (a replica answers PING, refuses writes)."""
        probe = f"{self.keys.prefix}:health:{self.worker_id}"

        async def check(client: Any) -> str:
            # Bounded: a blackholed store took 41 s (connect timeout x retries), past any probe.
            try:
                await asyncio.wait_for(
                    client.set(probe, b"1", px=5000), self.settings.timings.health_probe_secs
                )
            except StoreReadOnlyError:
                return "read_only"
            except (*STORE_ERRORS, ResponseError, TimeoutError):
                return "down"  # a refusal too (out of memory): it raised and readiness gave 500
            return "ok"

        if self._queue_store is None:
            state = queue = await check(self.state)  # one store: probe it once
        else:
            state, queue = await asyncio.gather(check(self.state), check(self._queue_store))
        stores = {"state": state, "queue": queue}
        self._stores = (time.monotonic(), stores)
        if state == "ok":
            self._outage_until_mono = -1e9  # the store is back: calls try it again (Q11)
        if queue == "ok":
            self._queue_down_until_mono = -1e9  # the queue store is back: send again (R3FM-3)
        return stores

    def _evicting(self) -> str | None:
        """In strict mode, why a store's eviction policy makes this process not ready."""
        if not self.settings.strict:
            return None
        for name, policy in self._eviction.items():
            warning = eviction_warning(policy, self._expiry_of(name).mode)
            if warning:
                return f"{name} store: {warning}"
        return None

    async def _store_prober(self) -> None:
        """Keep the stores' state for `health_check` fresh (one probe per few seconds)."""
        rounds = 0
        while True:
            try:
                await self._probe_stores()
            except Exception as exc:  # store trouble reads "down" there: this is a bug (RT-24)
                logger.warning("store_probe_failed", error=str(exc), error_type=type(exc).__name__)
            if rounds % POLICY_EVERY_ROUNDS == 0 and rounds:
                # Re-read: a parameter-group change or a failover to a node of another group
                # went unseen; it was read once at start (R3FM-8, owner Q10).
                await self._read_policies()
            rounds += 1
            await asyncio.sleep(HEALTH_PROBE_EVERY_SECS)

    async def _read_policies(self) -> None:
        """Read each store's eviction policy again (an unreadable one keeps its old value)."""
        stores = [("state", self.state)]
        if self._queue_store is not None:
            stores.append(("queue", self._queue_store))
        for name, client in stores:
            try:
                async with asyncio.timeout(self.settings.timings.health_probe_secs):
                    policy = await eviction_policy(client)
            except Exception as exc:
                logger.debug("policy_read_failed", store=name, error=str(exc))
                continue
            if policy != UNKNOWN_POLICY:
                self._eviction[name] = policy
        await self._publish_expiry_safely()  # a policy change, or a reset (R3FM-8, owner K4)

    async def _publish_expiry_safely(self) -> None:
        """`_apply_expiry`, logged instead of raised: retried by the minute policy re-read."""
        try:
            await self._apply_expiry()
        except Exception as exc:
            logger.warning("key_expiry_publish_failed", error=str(exc))

    async def _apply_expiry(self) -> None:
        """Set each store's key expiry mode from `LLMQ_KEY_EXPIRY` and its eviction policy."""
        stores = [("state", self.state, self.expiry)]
        if self._queue_store is not None:
            stores.append(("queue", self._queue_store, self.queue_expiry))
        for name, client, expiry in stores:
            policy = self._eviction.get(name, UNKNOWN_POLICY)
            mode = decide(self.settings.key_expiry, policy)
            replaced = await expiry.publish_mode(client, mode)
            if replaced:
                logger.warning(
                    "key_expiry_changed", store=name, policy=policy, before=replaced, mode=mode
                )

    async def _ttl_sweeper(self) -> None:
        """The expiry index sweep: one process at a time (`P:lock:ttlsweep`), every sweep."""
        while True:
            await asyncio.sleep(self.settings.sweep_secs)
            try:
                lock = await self.state.set(
                    f"{self.keys.prefix}:lock:ttlsweep",
                    self.worker_id,
                    nx=True,
                    px=max(1, int(self.settings.sweep_secs * 1000)),
                )
            except Exception as exc:
                logger.warning("ttl_sweep_failed", error=str(exc))
                continue
            if lock:
                await self._sweep_ttl_index()

    def _expiry_of(self, store: str) -> Expiry:
        """The key expiry of the `state` or `queue` store."""
        return self.queue_expiry if store == "queue" else self.expiry

    async def _sweep_ttl_index(self) -> None:
        """Delete the keys whose expiry in `P:ttl` is due, on each store.

        In any mode: keys indexed while `swept` still go after a switch back to `native`.
        """
        for expiry, script in self._ttl_sweeps:
            try:
                deleted = await expiry.sweep(
                    script,
                    batch=self.settings.ttl_sweep_batch,
                    rounds=self.settings.ttl_sweep_rounds,
                )
            except Exception as exc:
                logger.warning("ttl_sweep_failed", error=str(exc))
                continue
            if deleted:
                self.metrics.count("llmq_ttl_swept_total", float(deleted))

    def _not_live_reason(self) -> str:
        """Why `_live()` is false: the loops that died, or the stalled heartbeat (R3OB-20)."""
        dead = [
            task.get_name().removeprefix("llmq:")
            for task in [*self._tasks, *self._model_tasks.values()]
            if task.done() and (task.cancelled() or task.exception() is not None)
        ]
        if dead:
            return "loop died: " + ", ".join(sorted(dead))
        return f"heartbeat stalled {time.monotonic() - self._beat_ran_mono:.0f} s"

    def _live(self) -> bool:
        """False when a background loop died, or the heartbeat made no progress for 3 beats."""
        if self._stopping:
            return True
        for task in [*self._tasks, *self._model_tasks.values()]:
            if task.done() and (task.cancelled() or task.exception() is not None):
                return False
        return time.monotonic() - self._beat_ran_mono <= self.settings.lease_secs  # 3 x lease/3

    def _report_gauges(self, report: Mapping[str, Any]) -> None:
        """Gauges of every model from one status report (RESCAN O4: 6 metrics were not enough).

        Through `_cluster`, so series of removed models or deployments go away (R3OB-1). Limits
        next to use, usability and routing switches (R3OB-16); held calls and the oldest call's
        age (R3OB-2, R3OB-17); dead jobs (owner Q15).
        """
        gauge = self._cluster.gauge
        routing_blocked = set(report.get("blocked_regions", ()))
        regions: set[str] = set(routing_blocked)
        view = self.registry.current
        gauge("llmq_kill_switch", 1.0 if report.get("kill_switch") else 0.0)
        for model in report["models"]:
            alias = str(model["model"])
            for cls, count in model["waiting"].items():
                gauge("llmq_queue_waiting", float(count), model=alias, priority=str(cls))
            for cls, count in model.get("waiting_for_grant", {}).items():
                gauge("llmq_held_waiting", float(count), model=alias, priority=str(cls))
            gauge("llmq_in_progress", float(model["in_progress"]), model=alias)
            gauge("llmq_dead_letters", float(model["dead_letters"]), model=alias)
            oldest = model.get("oldest_waiting_secs")
            gauge("llmq_oldest_waiting_seconds", float(oldest or 0.0), model=alias)
            region_of = {d.key: d.region for d in view.deployments(alias)}
            for dep in model["deployments"]:
                labels = {"model": alias, "deployment": str(dep["name"])}
                if (region := region_of.get(str(dep["name"]))) is not None:
                    regions.add(region)
                usable = (
                    dep["enabled"]
                    and dep["account_enabled"]
                    and not dep["blocked"]
                    and dep["healthy"]
                    and dep["breaker"] != "open"
                )
                gauge("llmq_breaker_open", 1.0 if dep["breaker"] == "open" else 0.0, **labels)
                gauge("llmq_deployment_usable", 1.0 if usable else 0.0, **labels)
                gauge("llmq_rpm_used", float(dep["rpm"]["used"]), **labels)
                gauge("llmq_tpm_used", float(dep["tpm"]["used"]), **labels)
                for budget in ("rpm", "tpm"):
                    if dep[budget]["limit"] is not None:
                        gauge(f"llmq_{budget}_limit", float(dep[budget]["limit"]), **labels)
                gauge("llmq_concurrency_in_use", float(dep["concurrency"]["in_use"]), **labels)
                gauge("llmq_concurrency_limit", float(dep["concurrency"]["limit"]), **labels)
                gauge("llmq_throttles_1m", float(dep["throttles_1m"]), **labels)
                if dep["latency_ms"] is not None:  # base units (R3OB-14): was _ms
                    seconds = float(dep["latency_ms"]) / 1000
                    gauge("llmq_provider_latency_seconds", seconds, **labels)
        for region in sorted(regions):
            gauge("llmq_region_blocked", 1.0 if region in routing_blocked else 0.0, region=region)
        for job in report.get("jobs", ()):
            gauge("llmq_job_dead_letters", float(job["dead_letters"]), job=str(job["job"]))
            gauge("llmq_job_waiting", float(job["waiting"]), job=str(job["job"]))

    def _registry_gauges(self) -> None:
        """This pod's view of the stored registry (R1, C5): every pod, not only the alerter.

        A refused stored document (invalid, or needing a newer library) leaves the pod on its
        own config; only a log line said so.
        """
        store = self.registry
        gauge = self.metrics.gauge
        gauge("llmq_registry_invalid_reads", float(store.invalid_count))
        gauge("llmq_registry_stored_refused", 1.0 if store.stored_refused else 0.0)
        gauge("llmq_registry_lib_too_old", 1.0 if store.lib_too_old else 0.0)
        gauge("llmq_registry_on_own_config", 1.0 if store.on_own_config else 0.0)

    def _pod_gauges(self) -> None:
        """This process: version, stores, held calls, heartbeat, readiness (R3OB-14, -15).

        Also the outage memo (owner Q11) and the eviction policy that fails readiness in
        strict mode (owner Q10). Every pod, every alert tick.
        """
        gauge = self.metrics.gauge
        gauge("llmq_build_info", 1.0, version=__version__)
        stores = self._stores[1] if self._stores is not None else {}
        for name, state in stores.items():
            gauge("llmq_store_up", 1.0 if state == "ok" else 0.0, store=name)
        gauge("llmq_held_calls", float(len(self._held)))
        gauge("llmq_heartbeat_age_seconds", time.monotonic() - self._beat_ran_mono)
        gauge("llmq_outage_memo_active", 1.0 if self._outage_remembered() else 0.0)
        evicting = self._evicting()
        for name, policy in self._eviction.items():
            expiry = self._expiry_of(name)
            unsafe = 1.0 if eviction_warning(policy, expiry.mode) else 0.0
            gauge("llmq_store_eviction_unsafe", unsafe, store=name)
            gauge("llmq_key_expiry_swept", 1.0 if expiry.swept else 0.0, store=name)
        loaded = self.registry._current is not None
        ready = self.ready and loaded and not evicting
        gauge("llmq_ready", 1.0 if ready else 0.0)

    async def _alert_lock(self) -> bool:
        """True when this process evaluates alerts this tick: it takes or keeps the lock.

        The lock lived 25 s against a 30 s tick, so the evaluator rotated across pods every tick
        and each pod's gauges held its last win (R3OB-1). The holder now renews it (45 s); a
        holder that stops lets another take over within one tick or two.
        """
        key = f"{self.keys.prefix}:lock:alerts"
        if await self.state.set(key, self.worker_id, nx=True, ex=ALERT_LOCK_SECS):
            return True
        if text(await self.state.get(key) or b"") != self.worker_id:
            return False
        await self.state.expire(key, ALERT_LOCK_SECS)
        return True

    async def _alert_loop(self) -> None:
        """Every 30 s one process evaluates alerts: gauges every time, a log line per change."""
        while True:
            await asyncio.sleep(ALERT_EVERY_SECS)
            await self._alert_tick()

    async def _alert_tick(self) -> None:
        """One evaluation: pod gauges here; cluster gauges and alerts on the lock holder only."""
        self._registry_gauges()
        self._pod_gauges()
        try:
            if not await self._alert_lock():
                self._cluster.clear()  # another process evaluates: drop this one's (R3OB-1)
                return
            view = self.registry.current
            report = await self.status_reader.report(view)
            self._report_gauges(report)
            alerts = self.alerts.evaluate(view, report)
            for alert in alerts:
                self._cluster.gauge(
                    f"llmq_alert_{alert.name}", 1.0 if alert.on else 0.0, **alert.labels
                )
            self._cluster.finish()
            await self.alerts.changed(alerts)
            # Alarm on this not moving: evaluation stopped, which nothing showed (R3OB-17).
            self.metrics.gauge("llmq_alerts_evaluated_timestamp_seconds", time.time())
        except Exception as exc:
            logger.warning("alerts_failed", error=str(exc), error_type=type(exc).__name__)

    async def _ack(self, held: _Held) -> None:
        """Ack a call's message and forget the call; a stale receipt is logged and counted.

        A stale receipt means another worker took the message over (this one stalled past its
        visibility time): the call may have run twice. The ack's answer was dropped (FE-14).
        """
        if held.message is not None and not await self.queues.ack(held.message):
            logger.warning(
                "ack_stale_receipt", request_id=held.envelope.request_id, model=held.envelope.model
            )
            self.metrics.count("llmq_stale_acks_total", model=held.envelope.model)
        self._drop(held)

    def _drop(self, held: _Held) -> None:
        """Forget a call, but only this one: a newer delivery may hold the same request id."""
        if self._held.get(held.envelope.request_id) is held:
            del self._held[held.envelope.request_id]

    async def _give_back(self, held: _Held) -> None:
        """Give every grant of a call back in full, the estimate it took (nothing learned).

        It refunded nothing before (RESCAN A11): a cancelled or released call kept its whole
        reservation charged.
        """
        for served, owner in list(held.grants):
            # `undo` skips (and logs) store errors itself (EN-16); a refusal is logged here and
            # skips only this grant, which SWEEP frees after its lease (RT-24).
            try:
                await self.admission.undo(served, owner, held.envelope.request_id)
            except RedisError as exc:
                logger.warning(
                    "give_back_failed", request_id=held.envelope.request_id, error=str(exc)
                )
        held.grants.clear()

    async def _end_stream(self, held: _Held, kind: str, message: str) -> str | None:
        """End a stream that sent chunks with an `error` entry; its partial text, else None.

        Best effort and bounded by the probe timer: the call's reply tells the caller too.
        """
        writer = held.writer
        if writer is None or not writer.sent:
            return None
        try:
            async with asyncio.timeout(self.settings.timings.health_probe_secs):
                await writer.error(kind, message)
        except Exception as exc:
            logger.warning("stream_end_failed", request_id=held.envelope.request_id, error=str(exc))
        return writer.partial_text or None

    async def _shutdown_call(self, held: _Held) -> None:
        """The drain ran out mid-call (plan C3): give it back; no failure is counted.

        Grants are refunded and the message returned at once, so another pod runs it. A stream
        that already sent chunks cannot move: it ends with `worker_shutdown`.
        """
        try:
            await self._give_back(held)
            writer = held.writer
            if writer is not None and writer.sent:
                message = "the worker shut down mid-stream"
                with contextlib.suppress(Exception):  # the reply below tells the caller too
                    await writer.error("worker_shutdown", message)
                await self._finish_call(
                    held, "worker_shutdown", message, partial=writer.partial_text
                )
            elif held.message is not None:
                self._drop(held)
                await self._release_claims([held])
                await self.queues.return_(held.message)
            else:  # a direct call: its caller is in this process, which is stopping
                await self._finish_call(held, "worker_shutdown", "the worker shut down")
        except Exception as exc:
            logger.warning(
                "shutdown_give_back_failed", request_id=held.envelope.request_id, error=str(exc)
            )

    async def _release(self, held: _Held, delay_secs: float = 0.0, *, unsure: bool = False) -> None:
        """Let go of a call after an unexpected error: grants back, not held, message returned.

        The return waits out a store outage: one that failed left the message hidden for its
        whole visibility timeout (60 s), and its caller waited that long. `unsure`: its first
        admission failed without an answer (a read timeout): the script may have granted, so
        that grant is undone too (owner-checked, a no-op when none was made; AMEND FE-4).
        """
        if unsure and held.run is None and not held.grants and self._owns(held):
            alias, rid = held.envelope.model, held.envelope.request_id
            try:
                await self.admission.undo(alias, self._owner(held, 1), rid)
            except RedisError as exc:
                logger.warning("give_back_failed", request_id=rid, error=str(exc))
        await self._give_back(held)
        self._drop(held)
        message = held.message
        if message is not None:
            await self._release_claims([held])
            try:
                await self._through_outage(
                    lambda: self.queues.return_(message, delay_secs=delay_secs)
                )
            except Exception as exc:
                # The outage outlasted a lease: hidden until its visibility timeout. Said, not
                # silent: nobody saw why the call came back so late (audit RT-24).
                logger.warning(
                    "release_return_failed",
                    request_id=held.envelope.request_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

    async def _release_claims(self, helds: Sequence[_Held]) -> None:
        """Let go of the claims of calls this process gives back: best effort, time-bound.

        Without it a given-back call ran elsewhere only once its claim was a lease old (FE-1):
        a 30 s call missed its deadline on every graceful restart, drain timeout or failed run.
        """
        jobs = [
            (held.envelope.request_id, f"{self.worker_id}@{held.tag}")
            for held in helds
            if held.message is not None
        ]
        if not jobs:
            return
        try:
            async with asyncio.timeout(self.settings.timings.health_probe_secs):
                await self.admission.release_claims(jobs)  # owner-checked (FL-1)
        except Exception as exc:  # the claim then ages out after a lease, as before
            logger.warning(
                "release_claims_failed",
                calls=len(jobs),
                error=str(exc),
                error_type=type(exc).__name__,
            )

    async def _through_outage(
        self, write: Callable[[], Awaitable[object]], held: _Held | None = None
    ) -> None:
        """Run a store write that must land, again while the store is out of reach or refuses.

        Gives up (raises) once the outage outlasts a lease, or with `held` the call's deadline
        when that is later (owner Q7: an answer in hand is kept while its caller still waits;
        it was dropped after one lease and the call ran again). A refusal (NOREPLICAS, OOM,
        MISCONF, BUSY) counts as the store down (Q12, R3FM-7). Only for writes that are safe
        to run twice (each checks its own receipt or key).
        """
        wait_secs = self.settings.lease_secs
        if held is not None:
            wait_secs = max(wait_secs, self._deadline(held) - self.clock.now())
        give_up = time.monotonic() + wait_secs
        tries = 0
        while True:
            try:
                await write()
                return
            except (*STORE_ERRORS, ResponseError) as exc:
                if isinstance(exc, ResponseError) and not _store_refused(exc):
                    raise
                self._store_trouble("settle")
                if time.monotonic() >= give_up:
                    raise
                tries += 1
                await asyncio.sleep(self._store_retry_secs(tries))

    def _store_retry_secs(self, tries: int) -> float:
        """The pause before store retry `tries` (from 1): exponential with full jitter, capped.

        The fixed 0.25 s (`_send`) and 0.5 s (`_wait_result`, `_through_outage`) pauses had
        every call that failed in one outage retry in lockstep against the recovering store
        (audit RT-15); the X1 and S11 drills show recoveries are where the storms happen. The
        effect on the gates' connects per second is unverified (not measured yet).
        """
        ceiling = min(STORE_RETRY_CAP_SECS, STORE_RETRY_BASE_SECS * 2 ** max(0, tries - 1))
        return max(0.01, self._rng.uniform(0.0, ceiling))

    # ================================================================ background

    def _free_stuck(self) -> None:
        """Let go of calls held a lease past their deadline; nothing should still hold them.

        Every path settles a call by its deadline, so one still held a lease later is a bug or
        a hung store call. The heartbeat renewed its grants for good (one-hour soak: two calls
        in progress for 50 minutes after a store block). Now it is logged, its task cancelled,
        its grants given back and its message returned (the next worker finds it expired).
        """
        late = self.clock.now() - self.settings.lease_secs - self.settings.caller_margin_secs
        for held in list(self._held.values()):
            if self._deadline(held) >= late:
                continue
            running = held.run is not None and not held.run.done()
            self.metrics.count("llmq_stuck_calls_total", model=held.envelope.model)
            logger.error(
                "held_call_stuck",
                request_id=held.envelope.request_id,
                model=held.envelope.model,
                grants=sorted(held.grants),
                running=running,
                past_deadline_secs=round(self.clock.now() - self._deadline(held), 1),
            )
            self._drop(held)  # never renewed again, even if the release below fails
            if running:
                assert held.run is not None
                held.run.cancel()
            # A hold is not cancelled: dropped, it ends at its next round, and a grant its
            # admission got meanwhile goes back in `_start_run` (FE-4, RV-N1).
            task = asyncio.create_task(self._release(held))
            self._running.add(task)
            task.add_done_callback(self._running.discard)

    async def _heartbeat(self) -> None:
        """Renew leases and holds; keep the caller liveness key; resync the clock.

        It beats at least three times per lease and per held message's visibility timeout, so
        a short `visibility_secs` never lets a running call's message reappear. Each step fails
        on its own: one bad message (e.g. of a model removed by a reload) skips only itself.
        """
        while True:
            beat = self.settings.lease_secs / 3
            self._beat_ran_mono = time.monotonic()
            try:  # the only step outside a `try` before: an error here ended the heartbeat (FE-18)
                self._free_stuck()
            except Exception as exc:
                self._limited(
                    logger.warning,
                    "heartbeat_failed",
                    "free_stuck",
                    step="free_stuck",
                    error=str(exc),
                )
            try:
                self._sync_model_loops()
            except Exception as exc:
                self._limited(
                    logger.warning, "heartbeat_failed", "loops", step="loops", error=str(exc)
                )
            renewed = False
            try:
                held_now = list(self._held.values())
                items = [
                    (alias, owner, held.envelope.request_id)
                    for held in held_now
                    for alias, owner in list(held.grants)
                ]
                # Calls held between grants (waiting, or between retries) keep their claim
                # fresh, so a redelivered copy elsewhere is a duplicate, not a second run (A1).
                jobs = [
                    (held.envelope.request_id, f"{self.worker_id}@{held.tag}")
                    for held in held_now
                    if not held.grants
                ]
                # The caller key and the epoch ride on the same script (FL-21: two round trips).
                _, lost = await self.admission.renew_all(
                    items, jobs, caller=(self.keys.caller(self.worker_id), self.caller_ttl_secs)
                )
                now = time.monotonic()
                if now - self._beat_ok_mono > 2 * beat + 1:  # a gap: the store was out of reach
                    self._store_trouble_mono = now
                self._beat_ok_mono = now
                self._beat_ran_mono = now
                renewed = True
            except Exception as exc:  # the heartbeat must survive store hiccups
                self._limited(
                    logger.warning, "heartbeat_failed", "renew", step="renew", error=str(exc)
                )
                self._store_trouble("heartbeat")
                beat = 1.0  # renew the caller key as soon as the store is back
            if renewed:
                # Each step on its own: a failure here was logged as `renew`, counted as store
                # trouble and sped the beat up although the renew had worked (audit RT-33).
                if lost:  # swept, or lost with the store: still running here (A2, A6)
                    try:
                        await self._reregister(set(lost))
                    except Exception as exc:
                        self._limited(
                            logger.warning,
                            "heartbeat_failed",
                            "reregister",
                            step="reregister",
                            error=str(exc),
                        )
                try:
                    await self._check_cancels()
                except Exception as exc:
                    self._limited(
                        logger.warning,
                        "heartbeat_failed",
                        "cancels",
                        step="cancels",
                        error=str(exc),
                    )
            view = self.registry.current
            holds: list[tuple[Message, float]] = []
            holders: list[_Held] = []
            for held in list(self._held.values()):
                if held.message is None:
                    continue
                try:
                    visibility = float(view.value(held.envelope.model, "queue")["visibility_secs"])
                except Exception as exc:  # e.g. a model removed by a reload: skip only this one
                    self._limited(
                        logger.warning,
                        "heartbeat_failed",
                        "extend",
                        step="extend",
                        request_id=held.envelope.request_id,
                        error=str(exc),
                    )
                    continue
                beat = min(beat, visibility / 3)
                holds.append((held.message, visibility))
                holders.append(held)
            self._beat_ran_mono = time.monotonic()  # progress, even on a slow store (O2)
            try:
                kept = await self.queues.extend_many(holds)  # every hold in one call
            except Exception as exc:
                self._limited(
                    logger.warning, "heartbeat_failed", "extend", step="extend", error=str(exc)
                )
            else:
                self._holds_lost(holds, holders, kept)
            try:
                clock = await self.clock.sync(self.state)
                if clock == "jump":
                    # A promoted replica with another wall clock, a VM resume: TTLs and holds
                    # moved with it; callers count as alive for a while (R3FM-10).
                    logger.error("store_clock_jump")
                    self._store_trouble("clock")
                await self._check_epoch(read=not renewed)
            except Exception as exc:
                self._limited(
                    logger.warning, "heartbeat_failed", "clock", step="clock", error=str(exc)
                )
            try:
                await self._check_replication()
            except Exception as exc:
                self._limited(
                    logger.warning,
                    "heartbeat_failed",
                    "replication",
                    step="replication",
                    error=str(exc),
                )
            await asyncio.sleep(max(0.2, beat))

    def _holds_lost(
        self, holds: Sequence[tuple[Message, float]], holders: Sequence[_Held], kept: Sequence[bool]
    ) -> None:
        """Let go of waiting calls whose receipt went stale (their flags were ignored, RT-5).

        A stale receipt means the message is another worker's now (its hold lapsed in a store
        outage) or back on the queue. A waiting call (no run, no grant) is dropped and its hold
        stopped, so it is never admitted here as well; a running call goes on (its ack reports
        the stale receipt). A message replaced meanwhile (redelivered here) is not judged.
        """
        for (message, _), held, ok in zip(holds, holders, kept, strict=True):
            if ok or held.message is not message or held.run is not None or held.grants:
                continue
            if not self._owns(held):
                continue
            logger.warning(
                "hold_lost", request_id=held.envelope.request_id, model=held.envelope.model
            )
            self._drop(held)  # its hold ends at its next round (not cancelled: FE-4)

    async def _check_epoch(self, *, read: bool = True) -> None:
        """The store lost its data (restart, failover): re-register running calls (T10).

        Each process remembers the epoch it saw and re-registers when it is missing or changed
        (RESCAN A2): only the first process to notice re-registered before; the others found
        the new epoch and did nothing. `read` False: this beat's RENEW read it (FL-21).
        """
        value = await self.state.get(self.keys.epoch) if read else self.admission.last_epoch
        missing = value is None
        if missing:
            logger.error("state_store_reset_detected")
            self.metrics.count("llmq_store_resets_total")
            await self.state.set(self.keys.epoch, str(self.clock.now()), nx=True)
            await self._publish_expiry_safely()  # before any write that sets an expiry
            await self.expiry.set(
                self.state, f"{self.keys.prefix}:reset_minute", int(self.clock.now() // 60), 120
            )
            value = await self.state.get(self.keys.epoch)
        seen, self._epoch = self._epoch, None if value is None else text(value)
        if missing or (seen is not None and seen != self._epoch):
            if not missing:
                logger.error("state_store_reset_detected", by="epoch_changed")
                self.metrics.count("llmq_store_resets_total")
            self._sweep_now.set()  # waiting callers check their records now (R3RC-2)
            # The reset lost `P:expiry` too: publish it before the grants are written again
            # (else they get native TTLs), and never raising: a failed write here skipped the
            # re-registration for good (the epoch is already the new one; review B3-1).
            await self._publish_expiry_safely()
            try:
                await self._reregister()
            except Exception:
                self._epoch = seen  # still "changed" at the next beat: re-registered then
                raise

    async def _check_replication(self) -> None:
        """Read the state store's replication: replicas connected, and a promotion.

        A promotion that lost writes leaves the old epoch key in place, so `_check_epoch` saw
        nothing; a changed `master_replid` now re-registers running grants and has waiting
        callers check their records (R3FM-8). The replica count lets the replica wait skip a
        WAIT that cannot succeed (R3FM-11). One INFO per beat.
        """
        info = await self.state.info("replication")
        connected = info.get("connected_slaves")
        if connected is not None:
            self.replica_wait.connected = int(connected)
        replid = info.get("master_replid")
        if replid is None:
            return
        replid, seen = text(replid), self._replid
        self._replid = replid
        if seen is not None and seen != replid:
            logger.error("state_store_promoted", by="master_replid")
            self._sweep_now.set()
            await self._reregister()

    async def _reregister(self, owners: set[str] | None = None) -> None:
        """Put back the slots and entries of this process's running grants (all, or `owners`)."""
        items = [
            meta
            for held in list(self._held.values())
            for owner, meta in held.meta.items()
            if any(owner == o for _, o in held.grants) and (owners is None or owner in owners)
        ]
        if items:
            done = await self.admission.reregister(items)
            logger.warning("grants_reregistered", count=done, asked=len(items))

    async def _reload_registry(self, wanted: int | None) -> None:
        """Reload the registry once for every call that met `stale_config` (RESCAN R10).

        Each such call reloaded on its own: one publish under load was a reload storm. A call
        joins the reload in flight, and starts one more only if that one missed `wanted`.
        """
        task = self._reload
        if task is None or task.done():
            task = self._reload = asyncio.ensure_future(self.registry.load())
            await asyncio.shield(task)
            return
        await asyncio.shield(task)  # it may have started before the publish
        if wanted is not None and self.registry.changes != wanted:
            await self._reload_registry(None)

    async def _purge_tick(self) -> None:
        """Auto purge (owner K1): schedule what left the registry, purge what is due."""
        try:
            await self.purge.tick(self.registry.current)
        except Exception as exc:
            logger.warning("purge_failed", target="auto", error=repr(exc))

    async def _sweeper(self) -> None:
        """One process at a time removes in-progress entries of dead owners and old dead letters."""
        while True:
            await asyncio.sleep(self.settings.sweep_secs)
            await self._purge_tick()
            try:
                lock = await self.state.set(
                    f"{self.keys.prefix}:lock:sweep",
                    self.worker_id,
                    nx=True,
                    px=max(1, int(self.settings.sweep_secs * 1000)),  # ex=0 for < 1 s (A13)
                )
            except Exception as exc:
                logger.warning("sweep_failed", error=str(exc))
                continue
            if not lock:
                continue
            view = self.registry.current
            for alias in view.registry.models:
                # One model that fails skips only itself: one `try` around the loop stopped the
                # sweep (L4 lease pruning too) for every model after it, each round (FE-15).
                try:
                    swept = await self.admission.sweep(
                        alias, [d.key for d in view.deployments(alias)]
                    )
                    if swept:  # the count was dropped: a lease leak showed nowhere (R3OB-15)
                        self.metrics.count("llmq_swept_total", float(swept), model=alias)
                    # Dead letters age out on a timer too: only the next dead letter trimmed
                    # them, so a quiet queue kept its prompts for good (RESCAN X10).
                    keep = int(view.value(alias, "queue")["dlq_retention_secs"])
                    names = [self._queue_name(view, alias, p) for p in (True, False)]
                    await self.queues.trim_dead_many(names, keep)  # one round trip (FL-20)
                except Exception as exc:
                    logger.warning("sweep_failed", model=alias, error=str(exc))
