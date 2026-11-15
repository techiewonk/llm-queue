"""Bug-pinning tests for the S3 result offload from round 3 (R3JE-26..33, Q23, Q24).

Fake store scripts and a fake S3 client stand in, so they run without Docker or moto.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pytest
from botocore.exceptions import ClientError

from llm_queue.engine.envelope import Reply
from llm_queue.engine.errors import CallFailed
from llm_queue.engine.keys import Keys
from llm_queue.engine.offload import MARKER, S3Offload
from llm_queue.engine.requests import RequestStore
from llm_queue.errors import ConfigError

Run = Callable[[list[Any], list[Any]], Any]
# RequestStore registers its scripts in this order (see test_audit_engine.py).
ORDER = ("register", "register_send", "confirm", "abandon", "put", "take", "drop", "status")


def events(caplog: pytest.LogCaptureFixture, name: str) -> list[logging.LogRecord]:
    """The captured records of one structured event."""
    return [r for r in caplog.records if getattr(r, "llmq", {}).get("event") == name]


class Script:
    """A registered script whose reply is `run(keys, args)`; records its calls."""

    def __init__(self, run: Run) -> None:
        self.run = run
        self.args: list[list[Any]] = []

    async def __call__(self, keys: list[Any], args: list[Any]) -> Any:
        self.args.append(args)
        return self.run(keys, args)


class Client:
    """Scripts in registration order, and the plain commands the offload flag uses."""

    def __init__(self, **runs: Run) -> None:
        self.scripts = {name: Script(runs.get(name, lambda _k, _a: None)) for name in ORDER}
        self._order = iter(ORDER)
        self.data: dict[str, bytes] = {}

    def register_script(self, _source: str) -> Script:
        return self.scripts.get(next(self._order, ""), Script(lambda _k, _a: None))

    async def set(self, key: str, value: bytes, **_: Any) -> bool:
        self.data[key] = value
        return True

    async def exists(self, key: str) -> int:
        return int(key in self.data)


class MemoryOffload:
    """An offload in a dict; `errors` are raised by the next reads, in order."""

    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}
        self.errors: list[Exception] = []
        self.deleted: list[str] = []

    async def put(self, key: str, data: bytes) -> str:
        ref = f"r{len(self.data)}"
        self.data[ref] = data
        return ref

    async def get(self, ref: str) -> bytes | None:
        if self.errors:
            raise self.errors.pop(0)
        return self.data.get(ref)

    async def delete(self, ref: str) -> None:
        self.deleted.append(ref)
        self.data.pop(ref, None)


def _store(offload: Any = None, **runs: Run) -> tuple[RequestStore, Client]:
    client = Client(**runs)
    size = 10 if offload is not None else None
    store = RequestStore(client, Keys("t"), offload=offload, offload_bytes=size)  # type: ignore[arg-type]  # fake
    return store, client


def _denied() -> ClientError:
    return ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "GetObject")


async def test_r3je26_no_offload_reads_cleanly_and_keeps_the_result(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """R3JE-26: a process without the offload dropped the marker and raised a raw JSON error."""
    store, client = _store(take=lambda _k, _a: MARKER + b"ref")
    with caplog.at_level(logging.ERROR), pytest.raises(CallFailed) as info:
        await store.take_result("r")
    assert info.value.kind == "result_expired"
    assert client.scripts["take"].args[0][1] == MARKER  # the script keeps the marker
    assert client.scripts["drop"].args == []
    assert len(events(caplog, "offload_missing")) == 1


def test_r3je27_offload_and_bytes_go_together() -> None:
    """R3JE-27: an offload without offload_bytes turned nothing on, with no log."""
    with pytest.raises(ConfigError, match="go together"):
        RequestStore(Client(), Keys("t"), offload=MemoryOffload())  # type: ignore[arg-type]  # fake
    with pytest.raises(ConfigError, match="go together"):
        RequestStore(Client(), Keys("t"), offload_bytes=100)  # type: ignore[arg-type]  # fake
    with pytest.raises(ConfigError, match="1 or more"):
        RequestStore(Client(), Keys("t"), offload=MemoryOffload(), offload_bytes=0)  # type: ignore[arg-type]  # fake


async def test_r3je28_read_errors_are_retried_and_retryable(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R3JE-28: access denied or throttling read as `result_expired`, with nothing logged."""
    monkeypatch.setattr("llm_queue.engine.requests._GET_RETRY_SECS", (0.0, 0.0))
    offload = MemoryOffload()
    offload.data["ref"] = Reply(request_id="r", ok=True, response={"a": 1}).dumps()
    store, _ = _store(offload, take=lambda _k, _a: MARKER + b"ref")
    offload.errors = [_denied()]  # one blip: the second try reads it
    reply = await store.take_result("r")
    assert reply is not None
    offload.errors = [_denied()] * 3
    with caplog.at_level(logging.WARNING), pytest.raises(CallFailed) as info:
        await store.take_result("r")
    assert info.value.kind == "queue_unavailable"
    records = events(caplog, "offload_get_failed")
    assert getattr(records[-1], "llmq", {})["code"] == "AccessDenied"


async def test_r3je29_duplicate_result_deletes_its_object() -> None:
    """R3JE-29: an object uploaded for a result already stored was never deleted."""
    offload = MemoryOffload()
    store, _ = _store(offload, put=lambda _k, _a: 0)
    reply = Reply(request_id="r", ok=True, response={"x": "y" * 50})
    assert await store.put_result(reply, final_state="done") is False
    assert offload.deleted == ["r0"]


async def test_q24_a_pod_without_offload_refuses_to_start() -> None:
    """Q24: a pod without the offload joined a fleet that offloads and lost its results."""
    with_offload, client = _store(MemoryOffload())
    await with_offload.check_offload()
    without = RequestStore(client, Keys("t"))  # type: ignore[arg-type]  # fake
    with pytest.raises(ConfigError, match="result offload"):
        await without.check_offload()


def test_r3je31_prefix_is_normalised() -> None:
    """R3JE-31: `prefix="llmq"` gave `llmq<hash>` keys and `key_prefix="/"` a double slash."""
    assert S3Offload("b", prefix="llmq").prefix == "llmq/"
    assert S3Offload("b", key_prefix="/").prefix == "llmq-results/"
    assert S3Offload("b", prefix="/a//", key_prefix="b/").prefix == "a/b/"
    with pytest.raises(ConfigError):
        S3Offload("b", prefix="a/../b")


class FakeS3:
    """Records put_object arguments; answers the lifecycle read with `rules`."""

    def __init__(self, rules: list[dict[str, Any]] | Exception) -> None:
        self.rules = rules
        self.puts: list[dict[str, Any]] = []

    async def put_object(self, **kwargs: Any) -> None:
        self.puts.append(kwargs)

    async def get_bucket_lifecycle_configuration(self, **_: Any) -> dict[str, Any]:
        if isinstance(self.rules, Exception):
            raise self.rules
        return {"Rules": self.rules}


def _with(offload: S3Offload, s3: FakeS3) -> S3Offload:
    async def client() -> FakeS3:
        return s3

    offload._s3 = client  # type: ignore[method-assign]  # fake client
    return offload


async def test_r3je32_r3je33_put_headers_and_names() -> None:
    """R3JE-32: SSE could not be left out, no bucket key. R3JE-33: the name hashed the id."""
    s3 = FakeS3([])
    plain = _with(S3Offload("b", sse=None), s3)
    name = await plain.put("user-42-doc-7", b"x")
    assert "ServerSideEncryption" not in s3.puts[0]
    kms = _with(S3Offload("b", sse="aws:kms", kms_key_id="k", bucket_key=True), s3)
    await kms.put("r", b"x")
    assert s3.puts[1]["BucketKeyEnabled"] is True
    assert s3.puts[1]["SSEKMSKeyId"] == "k"
    import hashlib

    assert hashlib.sha256(b"user-42-doc-7").hexdigest() not in name
    with pytest.raises(ConfigError):
        S3Offload("b", kms_key_id="k")  # a key without aws:kms


async def test_r3je30_client_gets_short_timeouts(monkeypatch: pytest.MonkeyPatch) -> None:
    """R3JE-30: the S3 client used botocore's 60 s connect and read timeouts."""
    made: list[dict[str, Any]] = []

    class Context:
        async def __aenter__(self) -> object:
            return object()

    class Session:
        def create_client(self, _name: str, **kwargs: Any) -> Context:
            made.append(kwargs)
            return Context()

    offload = S3Offload("b", session=Session())  # type: ignore[arg-type]  # fake
    await offload._s3()
    config = made[0]["config"]
    assert (config.connect_timeout, config.read_timeout) == (3, 10)


async def test_q23_lifecycle_rule_check(caplog: pytest.LogCaptureFixture) -> None:
    """Q23: nothing checked that the bucket expires unread objects."""
    rule = {"Status": "Enabled", "Expiration": {"Days": 1}, "Filter": {"Prefix": "llmq-results/"}}
    assert await _with(S3Offload("b"), FakeS3([rule])).check_lifecycle() is True
    with caplog.at_level(logging.WARNING):
        assert await _with(S3Offload("b"), FakeS3([])).check_lifecycle() is False
    assert len(events(caplog, "offload_no_lifecycle_rule")) == 1
    denied = ClientError({"Error": {"Code": "AccessDenied"}}, "GetBucketLifecycleConfiguration")
    assert await _with(S3Offload("b"), FakeS3(denied)).check_lifecycle() is False
