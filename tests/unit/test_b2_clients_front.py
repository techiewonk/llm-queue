"""Front door and message layer (0.1.0b2: A1, A3, A5, C1, C2, D1, O5, O6, P8, S10, X9).

Old behaviour each test pins: bytes broke the envelope; images were estimated by their base64
size; `embed(request_id=X)` reused X for every batch and made every task up front; a second
PrometheusSink raised; provider_error was HTTP 400; a forked child or another event loop hung
or failed obscurely; sync calls built a second engine although the queue was started.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from moto.server import ThreadedMotoServer
from prometheus_client import CollectorRegistry

import llm_queue
from llm_queue import CallFailed, LLMQueue, LLMQueueError
from llm_queue.core import _after_fork_in_child
from llm_queue.engine.embed import embed, embed_partial
from llm_queue.engine.envelope import (
    MAX_MESSAGE_BYTES,
    Envelope,
    Reply,
    message_size_bytes,
    params_hash,
    parse,
)
from llm_queue.engine.errors import KINDS
from llm_queue.engine.metrics import PrometheusSink, sink_for
from llm_queue.engine.offload import S3Offload
from llm_queue.engine.tokens import BINARY_BLOCK_TOKENS, input_tokens
from llm_queue.http_transport import _STATUS, _error_response
from llm_queue.langchain import get_default_queue, set_default_queue

IMAGE = os.urandom(300_000)
CHAT = {
    "messages": [
        {
            "role": "user",
            "content": [
                {"text": "what is it"},
                {"image": {"format": "png", "source": {"bytes": IMAGE}}},
            ],
        }
    ]
}

# ------------------------------------------------------------------------- envelope / tokens


def test_bytes_travel_in_the_envelope_and_come_back_as_bytes() -> None:
    env = Envelope(
        request_id="r", op="converse", model="m", params=CHAT, caller="c", timeout_secs=5
    )
    back = parse(env.dumps())  # was: orjson TypeError on bytes
    assert back.params["messages"][0]["content"][1]["image"]["source"]["bytes"] == IMAGE
    assert params_hash(CHAT) == params_hash(back.params)
    reply = Reply(request_id="r", ok=True, response={"x": {"redactedContent": b"\x00\x01"}})
    assert Reply.model_validate_json(reply.dumps()).response == {
        "x": {"redactedContent": b"\x00\x01"}
    }
    plain = Reply(request_id="r", ok=True, response={"x": 1})
    assert b"b64" not in plain.dumps()  # no flag, no walk, when nothing was binary


def test_old_message_version_text_has_no_stale_release() -> None:
    with pytest.raises(ValueError, match="this library reads 2"):
        parse(b'{"v": 1}')


def test_binary_blocks_are_not_counted_as_text() -> None:
    encoded = {
        "messages": [
            {"content": [{"image": {"source": {"bytes": base64.b64encode(IMAGE).decode()}}}]}
        ]
    }
    assert input_tokens("converse", encoded) == BINARY_BLOCK_TOKENS  # was ~100,000
    openai_style = {
        "messages": [{"content": [{"type": "image_url", "image_url": {"url": "data:x" * 9999}}]}]
    }
    assert input_tokens("chat", openai_style) < BINARY_BLOCK_TOKENS + 10


def test_message_size_counts_encoded_bytes() -> None:
    size = message_size_bytes(CHAT)
    assert len(IMAGE) * 4 // 3 < size < len(IMAGE) * 4 // 3 + 500
    assert MAX_MESSAGE_BYTES == 4 * 1024 * 1024


# ------------------------------------------------------------------------- embed


class _EmbedEngine:
    """An engine stub for `embed`: Cohere batches, optional failing batches, peak concurrency."""

    def __init__(self, fail: set[int] | None = None) -> None:
        entry = SimpleNamespace(provider="openai", model_id="e")
        registry = SimpleNamespace(alias_of=lambda m: m, models={"e": entry})
        self.registry = SimpleNamespace(
            current=SimpleNamespace(registry=registry, value=lambda alias, name: 2)
        )
        self.fail = fail or set()
        self.ids: list[str | None] = []
        self.live = self.peak = 0

    async def call(self, alias: str, request: dict[str, Any], **kw: Any) -> Reply:
        self.ids.append(kw.get("request_id"))
        self.live += 1
        self.peak = max(self.peak, self.live)
        await asyncio.sleep(0.01)
        self.live -= 1
        batch = len(self.ids) - 1
        if batch in self.fail:
            raise CallFailed("provider_error", f"batch {batch}")
        data = [{"index": i, "embedding": [float(i)]} for i in range(len(request["input"]))]
        return Reply(request_id="x", ok=True, response={"data": data})


async def test_embed_derives_one_request_id_per_batch() -> None:
    engine = _EmbedEngine()
    vectors = await embed(engine, "e", ["a", "b", "c", "d", "e"], request_id="job-1")  # type: ignore[arg-type]
    assert len(vectors) == 5
    assert sorted(str(i) for i in engine.ids) == [
        "job-1:0",
        "job-1:1",
        "job-1:2",
    ]  # was "job-1" three times


async def test_embed_rejects_empty_texts_before_sending() -> None:
    engine = _EmbedEngine()
    with pytest.raises(CallFailed) as caught:
        await embed(engine, "e", ["a", "  "])  # type: ignore[arg-type]
    assert caught.value.kind == "invalid_request"
    assert engine.ids == []


async def test_embed_runs_a_window_of_batches() -> None:
    engine = _EmbedEngine()
    await embed(engine, "e", [f"t{i}" for i in range(200)], concurrency=4)  # type: ignore[arg-type]
    assert engine.peak <= 4
    assert len(engine.ids) == 100


async def test_embed_partial_keeps_the_good_batches() -> None:
    engine = _EmbedEngine(fail={1})
    vectors, errors = await embed_partial(engine, "e", ["a", "b", "c", "d"], concurrency=1)  # type: ignore[arg-type]
    assert [bool(v) for v in vectors] == [True, True, False, False]
    assert set(errors) == {2, 3}
    with pytest.raises(CallFailed):
        await embed(_EmbedEngine(fail={1}), "e", ["a", "b", "c", "d"], concurrency=1)  # type: ignore[arg-type]


# ------------------------------------------------------------------------- offload


@pytest.fixture(scope="module")
def s3_kwargs() -> Iterator[dict[str, Any]]:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    yield {
        "endpoint_url": f"http://{host}:{port}",
        "region_name": "us-east-1",
        "aws_access_key_id": "x",
        "aws_secret_access_key": "x",
    }
    server.stop()


async def test_offload_keys_are_hashed_checked_and_deleted(s3_kwargs: dict[str, Any]) -> None:
    offload = S3Offload("b2-results", key_prefix="app", client_kwargs=s3_kwargs)
    try:
        clients = await asyncio.gather(*(offload._s3() for _ in range(5)))
        assert len({id(c) for c in clients}) == 1  # was: one client per concurrent first call
        await clients[0].create_bucket(Bucket="b2-results")
        ref = await offload.put("tenant/../secret id", b"data")
        assert ref.startswith("llmq-results/app/")
        assert "secret" not in ref
        assert ref != await offload.put("tenant/../secret id", b"data")  # unique names
        with pytest.raises(LLMQueueError):
            await offload.get("other-prefix/x")
        assert await offload.get(ref) == b"data"
        assert await offload.get(ref) is None  # deleted after the read
    finally:
        await offload.aclose()


# ------------------------------------------------------------------------- metrics / http


def test_prometheus_sinks_share_collectors() -> None:
    registry = CollectorRegistry()
    first, second = PrometheusSink(registry), PrometheusSink(registry)
    first.count("llmq_b2_calls_total", model="m")
    second.count("llmq_b2_calls_total", model="m")  # was: ValueError Duplicated timeseries
    assert registry.get_sample_value("llmq_b2_calls_total", {"model": "m"}) == 2
    assert sink_for("none") is sink_for("none")


def test_every_call_failed_kind_has_an_http_status() -> None:
    assert {"invalid_request", "context_length_exceeded", "worker_shutdown"} <= KINDS
    assert set(KINDS) <= set(_STATUS)
    request = httpx.Request("POST", "http://llmq.invalid/v1/chat/completions")
    assert _error_response(request, CallFailed("provider_error")).status_code == 502  # was 400
    assert _error_response(request, CallFailed("context_length_exceeded")).status_code == 400
    spent = _error_response(request, CallFailed("retry_budget_exhausted", retry_after_secs=3))
    assert (spent.status_code, spent.headers["retry-after"]) == (503, "3")


def test_public_exports_resolve() -> None:
    for name in llm_queue.__all__:
        assert getattr(llm_queue, name) is not None, name


# ------------------------------------------------------------------------- LLMQueue guards


class _FakeEngine:
    """Just enough engine for `LLMQueue`: records which loop and thread ran each call."""

    def __init__(self) -> None:
        current = SimpleNamespace(
            registry=SimpleNamespace(alias_of=lambda m: m),
            value=lambda alias, name, dep=None: MAX_MESSAGE_BYTES,
        )
        self.registry = SimpleNamespace(current=current)
        self.queues = None
        self.metrics = None
        self.settings = SimpleNamespace(queue_prefix="", drain_secs=0)
        self.loops: list[asyncio.AbstractEventLoop] = []

    async def start(self, **kw: Any) -> None:
        return None

    async def stop(self, **kw: Any) -> None:
        return None

    async def aclose(self) -> None:
        return None

    async def call(self, model: str, params: Any, **kw: Any) -> Reply:
        self.loops.append(asyncio.get_running_loop())
        return Reply(request_id="r", ok=True, response={})


def _queue() -> LLMQueue:
    return LLMQueue(_FakeEngine(), settings=SimpleNamespace(mode="caller"))  # type: ignore[arg-type]


@pytest.fixture
def no_default() -> Iterator[None]:
    set_default_queue(None)
    yield
    set_default_queue(None)


async def test_second_queue_warns_and_the_first_stays_default(
    no_default: None, caplog: pytest.LogCaptureFixture
) -> None:
    first, second = _queue(), _queue()
    await first.start()
    with caplog.at_level(logging.WARNING):
        await second.start()
    assert "second_llm_queue_started" in caplog.text
    assert get_default_queue() is first  # was: the last started queue
    await second.stop()
    assert get_default_queue() is first  # stopping the other never clears it
    await first.stop()


async def test_forked_child_and_other_loops_get_a_clear_error(no_default: None) -> None:
    queue = _queue()
    await queue.start()
    try:
        assert (await queue.converse("m", {"messages": []})).ok

        def other_loop() -> None:
            asyncio.run(queue.converse("m", {"messages": []}))

        with pytest.raises(LLMQueueError, match="another event loop"):
            await asyncio.to_thread(other_loop)
        _after_fork_in_child()  # what os.register_at_fork runs in a child
        with pytest.raises(LLMQueueError, match="fork"):
            await queue.converse("m", {"messages": []})
    finally:
        queue._inherited = False
        await queue.stop()


async def test_sync_calls_run_on_the_started_queue_loop(no_default: None) -> None:
    queue = _queue()
    await queue.start()
    try:
        main = asyncio.get_running_loop()
        reply = await asyncio.to_thread(queue.converse_sync, "m", {"messages": []})
        assert reply.ok
        assert queue.engine.loops[-1] is main  # type: ignore[attr-defined]
        assert queue._sync is None  # no private engine was built
        with pytest.raises(LLMQueueError, match="deadlock"):
            queue.converse_sync("m", {"messages": []})  # on the loop's own thread
    finally:
        await queue.stop()
