"""Bug-pinning tests for the engine audit (EN-*): each names its finding and the old behaviour.

They use small in-memory fakes of the store client, so they run without the Docker stack.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import orjson
import pytest
from redis.exceptions import ConnectionError as StoreConnectionError

from llm_queue.engine.admission import Admission
from llm_queue.engine.keys import Keys


def events(caplog: pytest.LogCaptureFixture, name: str) -> list[logging.LogRecord]:
    """The captured records of one structured event (`record.llmq["event"]`)."""
    return [r for r in caplog.records if getattr(r, "llmq", {}).get("event") == name]


class FakeScript:
    """A registered script whose reply comes from `run(keys, args)`."""

    def __init__(self, run: Callable[[list[Any], list[Any]], Any]) -> None:
        self.run = run
        self.calls = 0

    async def __call__(self, keys: list[Any], args: list[Any]) -> Any:
        self.calls += 1
        return self.run(keys, args)


class ScriptClient:
    """A store client whose scripts, in registration order, run the given functions."""

    def __init__(self, *runs: Callable[[list[Any], list[Any]], Any]) -> None:
        self.scripts = [FakeScript(run) for run in runs]
        self._next = 0

    def register_script(self, _source: str) -> FakeScript:
        script = self.scripts[self._next] if self._next < len(self.scripts) else FakeScript(_none)
        self._next += 1
        return script


def _none(_keys: list[Any], _args: list[Any]) -> Any:
    return None


def _down(_keys: list[Any], _args: list[Any]) -> Any:
    raise StoreConnectionError("connection reset")


# Admission registers PICK_TAKE, FINISH, UNDO, RENEW, SWEEP, REREGISTER in that order.
def _admission(**runs: Callable[[list[Any], list[Any]], Any]) -> Admission:
    order = ("pick", "finish", "undo", "renew", "sweep", "reregister")
    client = ScriptClient(*(runs.get(name, _none) for name in order))
    return Admission(client, Keys("t"), lease_secs=1.0)  # type: ignore[arg-type]  # fake


class RecordingSink:
    """A metrics sink that keeps every count point."""

    def __init__(self) -> None:
        self.counts: list[tuple[str, float, dict[str, str]]] = []

    def count(self, name: str, value: float = 1.0, **labels: str) -> None:
        self.counts.append((name, value, labels))

    def observe(self, name: str, value: float, **labels: str) -> None:
        """Ignore."""

    def gauge(self, name: str, value: float, **labels: str) -> None:
        """Ignore."""


def test_en1_prometheus_sink_drops_a_point_with_other_labels(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """EN-1: a second label set for one metric raised `ValueError` into the call; now dropped."""
    from prometheus_client import CollectorRegistry

    from llm_queue.engine.metrics import PrometheusSink

    sink = PrometheusSink(CollectorRegistry())
    sink.count("llmq_test_lost_total", step="renew")
    with caplog.at_level(logging.WARNING):
        sink.count("llmq_test_lost_total", model="m", step="finish")
        sink.count("llmq_test_lost_total", model="m", step="finish")
        sink.observe("llmq_test_seconds", 1.0, model="m")
        sink.observe("llmq_test_seconds", 1.0)
        sink.gauge("llmq_test_depth", 1.0, model="m")
        sink.gauge("llmq_test_depth", 1.0, other="x")
    assert len(events(caplog, "metric_dropped")) == 3  # once per name


async def test_en1_renew_counts_lost_grants_with_the_finish_labels() -> None:
    """EN-1: `renew_all` counted `llmq_grants_lost_total` by step only, `finish` by model too."""
    reply = orjson.dumps({"n": 0, "lost": ["o1", "o2", "o3"]})
    admission = _admission(renew=lambda _k, _a: reply, finish=lambda _k, _a: 0)
    sink = RecordingSink()
    admission.metrics = sink
    items = [("a", "o1", "r1"), ("a", "o2", "r2"), ("b", "o3", "r3")]
    assert (await admission.renew_all(items))[1] == ["o1", "o2", "o3"]
    await admission.finish("a", "o9", "r9", refund_r=0, refund_t=0, outcome="ok")
    labels = {tuple(sorted(lb)) for name, _, lb in sink.counts if name == "llmq_grants_lost_total"}
    assert labels == {("model", "step")}
    per_model = {lb["model"]: v for _, v, lb in sink.counts if lb["step"] == "renew"}
    assert per_model == {"a": 2.0, "b": 1.0}


async def test_en16_undo_skips_on_store_error(caplog: pytest.LogCaptureFixture) -> None:
    """EN-16: `undo` let store errors escape (the runtime suppressed every exception unlogged)."""
    admission = _admission(undo=_down)
    with caplog.at_level(logging.WARNING):
        assert await admission.undo("m", "owner", "rid") is False
    assert len(events(caplog, "undo_skipped")) == 1


def test_en17_resettled_owner_does_not_block_pruning(monkeypatch: pytest.MonkeyPatch) -> None:
    """EN-17: re-settling an owner kept its old place, so pruning stopped at it."""
    import time

    now = [100.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    admission = _admission()  # lease 1 s: owners are kept 2 s
    admission._settle("A")
    now[0] += 0.5
    admission._settle("B")
    now[0] += 4.5
    admission._settle("A")  # recent again: it sat first and stopped B's pruning
    now[0] += 1.0
    admission._settle("C")
    assert list(admission._settled) == ["A", "C"]


def test_en2_raising_estimator_falls_back_to_chars(caplog: pytest.LogCaptureFixture) -> None:
    """EN-2: a custom estimator that raised escaped into the call loop (hot requeue)."""
    from llm_queue.engine import tokens

    tokens.register_token_estimator("en2_broken", lambda *_: 1 // 0)
    params = {"messages": [{"role": "user", "content": [{"text": "x" * 400}]}]}
    with caplog.at_level(logging.WARNING):
        first = tokens.input_tokens("converse", params, "en2_broken")
        second = tokens.input_tokens("converse", params, "en2_broken")
    assert first == second == tokens.input_tokens("converse", params)
    assert len(events(caplog, "hook_failed")) == 1


def test_en2_inference_config_none_is_filled() -> None:
    """EN-2: `"inferenceConfig": None` raised `TypeError` ('NoneType' item assignment)."""
    from llm_queue.engine import inference, tokens

    params: dict[str, Any] = {"inferenceConfig": None, "additionalModelRequestFields": None}
    out = inference.apply(
        "bedrock", "converse", params, {"max_tokens": 1, "extra": {"k": 1}, "top_k": 5}, {}
    )
    assert out["inferenceConfig"] == {"maxTokens": 1}
    assert out["additionalModelRequestFields"] == {"top_k": 5, "k": 1}
    assert params["inferenceConfig"] is None  # the caller's request is not changed
    assert tokens.max_tokens_of("converse", {"inferenceConfig": None}) is None


def test_en15_unknown_estimator_warns_once(caplog: pytest.LogCaptureFixture) -> None:
    """EN-15: an estimator name nobody registered fell back to characters / 4 with no log."""
    from llm_queue.engine import tokens

    with caplog.at_level(logging.WARNING):
        tokens.input_tokens("converse", {"x": "abcd"}, "en15_nope")
        tokens.input_tokens("converse", {"x": "abcd"}, "en15_nope")
    found = events(caplog, "hook_not_registered")
    assert len(found) == 1
    assert found[0].llmq["kind"] == "token_estimator"  # type: ignore[attr-defined]  # extra


def test_en28_tokens_does_not_import_the_provider_stack() -> None:
    """EN-28: `engine.tokens` imported `clients.providers` for one annotation (~60 ms)."""
    import subprocess
    import sys

    code = (
        "import sys, llm_queue.engine.tokens\nprint('llm_queue.clients.providers' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert out == "False"


class FakeEmbedEngine:
    """Just enough of `Engine` for `embed_partial`: OpenAI model, batch size 1."""

    def __init__(self, bad: set[str]) -> None:
        from types import SimpleNamespace

        entry = SimpleNamespace(provider="openai", model_id="text-embedding")
        registry = SimpleNamespace(alias_of=lambda m: m, models={"e": entry})
        view = SimpleNamespace(registry=registry, value=lambda _a, _n: 1)
        self.registry = SimpleNamespace(current=view)
        self.bad = bad

    async def call(self, _alias: str, request: dict[str, Any], **_: Any) -> Any:
        from llm_queue.engine.envelope import Reply

        text = request["input"][0]
        item: dict[str, Any] = (
            {"index": 0} if text in self.bad else {"index": 0, "embedding": [1.0]}
        )
        return Reply(request_id=text, ok=True, response={"data": [item]})


async def test_en6_unreadable_embedding_body_is_a_call_failure() -> None:
    """EN-6: a body without vectors raised a raw KeyError and aborted every other batch."""
    from llm_queue.engine import embed
    from llm_queue.engine.errors import CallFailed

    engine: Any = FakeEmbedEngine({"b"})
    vectors, errors = await embed.embed_partial(engine, "e", ["a", "b", "c"])
    assert vectors == [[1.0], [], [1.0]]
    assert errors[1].kind == "provider_error"
    with pytest.raises(CallFailed):
        await embed.embed(engine, "e", ["a", "b"])


class CountingOffload:
    """An offload that counts writes; `fail_put` makes every write raise."""

    def __init__(self, *, fail_put: bool = False, delete_on_get: bool = True) -> None:
        self.puts = 0
        self.fail_put = fail_put
        self.delete_on_get = delete_on_get
        self.data: dict[str, bytes] = {}

    async def put(self, key: str, data: bytes) -> str:
        self.puts += 1
        if self.fail_put:
            raise RuntimeError("botocore said no")
        ref = f"{key}-{self.puts}"
        self.data[ref] = data
        return ref

    async def get(self, ref: str) -> bytes | None:
        return self.data.pop(ref, None) if self.delete_on_get else self.data.get(ref)


# RequestStore registers REGISTER, REGISTER_SEND, CONFIRM, ABANDON, RESULT_PUT, RESULT_TAKE,
# RESULT_DROP, STATUS_SET in that order.
def _requests(offload: Any, **runs: Callable[[list[Any], list[Any]], Any]) -> Any:
    from llm_queue.engine.requests import RequestStore

    order = ("register", "register_send", "confirm", "abandon", "put", "take", "drop", "status")
    client = ScriptClient(*(runs.get(name, _none) for name in order))
    store = RequestStore(client, Keys("t"), offload=offload, offload_bytes=10)  # type: ignore[arg-type]  # fake
    return store, client


async def test_en3_result_is_offloaded_once_through_a_store_outage() -> None:
    """EN-3: each retry of `put_result` during an outage uploaded the result again."""
    from llm_queue.engine.envelope import Reply

    tries = [0]
    stored: list[bytes] = []

    def put(_keys: list[Any], args: list[Any]) -> int:
        tries[0] += 1
        if tries[0] <= 2:
            raise StoreConnectionError("reset")
        stored.append(args[0])
        return 1

    offload = CountingOffload()
    requests, _ = _requests(offload, put=put)
    reply = Reply(request_id="r", ok=True, response={"x": "y" * 50})
    for _ in range(2):
        with pytest.raises(StoreConnectionError):
            await requests.put_result(reply, final_state="done")
    assert await requests.put_result(reply, final_state="done") is True
    assert offload.puts == 1
    assert stored[0].startswith(b"llmq-offload:")
    assert requests._unsent == {}


async def test_en3_offload_failure_keeps_the_result_inline(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """EN-3: a botocore error from the offload escaped `put_result` and the call ran again."""
    from llm_queue.engine.envelope import Reply

    stored: list[bytes] = []

    def put(_keys: list[Any], args: list[Any]) -> int:
        stored.append(args[0])
        return 1

    requests, _ = _requests(CountingOffload(fail_put=True), put=put)
    reply = Reply(request_id="r", ok=True, response={"x": "y" * 50})
    with caplog.at_level(logging.WARNING):
        assert await requests.put_result(reply, final_state="done") is True
    assert Reply.model_validate(orjson.loads(stored[0])).response == {"x": "y" * 50}
    assert len(events(caplog, "offload_put_failed")) == 1


async def test_en4_failed_drop_after_offload_read_still_returns_the_reply() -> None:
    """EN-4: a store blip after the offload read (which deleted the object) lost the answer."""
    from llm_queue.engine.envelope import Reply

    offload = CountingOffload(delete_on_get=True)
    body = Reply(request_id="r", ok=True, response={"a": 1}).dumps()
    offload.data["ref"] = body
    requests, _ = _requests(offload, take=lambda _k, _a: b"llmq-offload:ref", drop=_down)
    reply = await requests.take_result("r")
    assert reply is not None
    assert reply.response == {"a": 1}


async def test_en11_only_no_such_key_reads_as_gone() -> None:
    """EN-11: any error whose text held "404" was read as an expired result."""
    from botocore.exceptions import ClientError

    from llm_queue.engine.offload import S3Offload

    def error(code: str) -> ClientError:
        return ClientError({"Error": {"Code": code, "Message": "req 404abc"}}, "GetObject")

    class FakeS3:
        def __init__(self, exc: Exception) -> None:
            self.exc = exc

        async def get_object(self, **_: Any) -> Any:
            raise self.exc

    offload = S3Offload("bucket")
    current: list[FakeS3] = []

    async def s3() -> FakeS3:
        return current[-1]

    offload._s3 = s3  # type: ignore[method-assign]  # fake client
    for exc, gone in ((error("SlowDown"), False), (error("NoSuchKey"), True)):
        current.append(FakeS3(exc))
        if gone:
            assert await offload.get(f"{offload.prefix}x") is None
        else:
            with pytest.raises(ClientError):
                await offload.get(f"{offload.prefix}x")
