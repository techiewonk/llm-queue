"""0.2.0 E1: the model factories `chat_model` / `embeddings`, `aliases` and the boot registry."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from llm_queue import ConfigError, LLMQueue
from llm_queue.langchain import (
    aliases,
    boot_registry,
    chat_model,
    embeddings,
    set_default_queue,
)
from llm_queue.registry import validate

GPT = "openai.gpt-oss-120b-1:0"
TITAN = "amazon.titan-embed-text-v2:0"
DOC: dict[str, Any] = {
    "registry_version": 1,
    "accounts": {"aws-a": {"kind": "aws"}},
    "models": {
        "gpt120b": {
            "type": "chat",
            "provider": "bedrock",
            "model_id": GPT,
            "worker_slots": 30,
            "deployments": {"aws-a": {"confirmed": ["ap-south-1"]}},
        },
        "titan": {
            "type": "embedding",
            "provider": "bedrock",
            "model_id": TITAN,
            "deployments": {"aws-a": {"confirmed": ["ap-south-1"]}},
        },
    },
}
DEV = {"LLMQ_ENVIRONMENT": "dev", "LLMQ_REGISTRY": json.dumps(DOC)}
URL = "redis://localhost:6379/0"
STRICT = {"LLMQ_ENVIRONMENT": "prod", "LLMQ_ID_HASH_SALT": "s" * 16, "LLMQ_KEY_PREFIX": "app"}


# E1: factories -------------------------------------------------------------------------------


def test_factories_build_the_queued_class_for_a_known_model() -> None:
    from llm_queue.langchain import QueuedBedrockEmbeddings, QueuedChatBedrockConverse

    chat = chat_model(GPT, registry=DOC, temperature=0.1, max_tokens=50)
    assert isinstance(chat, QueuedChatBedrockConverse)
    assert (chat.model_alias, chat.model_id) == ("gpt120b", GPT)  # found by model id
    assert (chat.temperature, chat.max_tokens) == (0.1, 50)  # **kw passed through
    by_alias = chat_model("gpt120b", registry=DOC, model_id="override")
    assert (by_alias.model_alias, by_alias.model_id) == ("gpt120b", "override")  # **kw wins
    emb = embeddings(TITAN, registry=DOC, dimensions=256)
    assert isinstance(emb, QueuedBedrockEmbeddings)
    assert (emb.model_alias, emb.model_id) == ("titan", TITAN)


def test_factories_return_none_for_a_model_outside_the_registry() -> None:
    assert chat_model("anthropic.claude-x", registry=DOC) is None
    assert embeddings("nope", registry=DOC) is None
    with pytest.raises(ConfigError, match="embedding model"):
        chat_model(TITAN, registry=DOC)


def test_factories_read_the_boot_registry_from_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("LLMQ_REGISTRY_FILE", raising=False)
    monkeypatch.setenv("LLMQ_REGISTRY", json.dumps(DOC))
    assert chat_model(GPT).model_alias == "gpt120b"
    # From a file with a placeholder, as the queue reads it.
    monkeypatch.delenv("LLMQ_REGISTRY")
    doc = json.loads(json.dumps(DOC))
    doc["models"]["titan"]["model_id"] = "${EMBED_ID:-" + TITAN + "}"
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(doc), encoding="utf-8")
    monkeypatch.setenv("LLMQ_REGISTRY_FILE", str(path))
    assert embeddings(TITAN).model_alias == "titan"
    assert embeddings(TITAN, registry=path).model_alias == "titan"
    assert embeddings(TITAN, registry=str(path)).model_alias == "titan"
    assert embeddings(TITAN, registry=json.dumps(DOC)).model_alias == "titan"


def test_factories_take_an_llmqueue_and_pass_it_on() -> None:
    llmq = LLMQueue.from_env({"LLMQ_STORE_URL": URL, **DEV})
    chat = chat_model("gpt120b", registry=llmq)
    assert chat.llmq is llmq
    assert chat_model("not-queued", registry=llmq) is None


def test_aliases_maps_model_ids_to_aliases() -> None:
    assert aliases(DOC) == {GPT: "gpt120b", TITAN: "titan"}
    assert sorted(boot_registry(DOC).models) == ["gpt120b", "titan"]


def test_factories_do_not_import_langchain_for_an_unknown_model() -> None:
    code = (
        "import sys\n"
        "from llm_queue.langchain import chat_model, embeddings\n"
        f"doc = {DOC!r}\n"
        "assert chat_model('x', registry=doc) is None\n"
        "assert embeddings('x', registry=doc) is None\n"
        "loaded = [m for m in sys.modules if m.split('.')[0] in "
        "('langchain_core', 'langchain_aws', 'langchain_openai', 'boto3', 'httpx')]\n"
        "assert not loaded, loaded\n"
    )
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src")}
    subprocess.run([sys.executable, "-c", code], check=True, env=env)


def test_factories_pick_the_openai_adapters_for_openai_models() -> None:
    from llm_queue.langchain import QueuedChatOpenAI, QueuedOpenAIEmbeddings

    doc = json.loads(json.dumps(DOC))
    doc["accounts"]["vllm"] = {"kind": "openai", "base_url": "https://llm.example.com/v1"}
    for alias, kind, model_id in (("local", "chat", "oss-120b"), ("local-emb", "embedding", "e5")):
        doc["models"][alias] = {
            "type": kind,
            "provider": "openai",
            "model_id": model_id,
            "deployments": {"vllm": {"confirmed": ["dc1"]}},
        }
    chat = chat_model("oss-120b", registry=doc, temperature=0)
    assert isinstance(chat, QueuedChatOpenAI)
    assert (chat.model_alias, chat.model_name, chat.temperature) == ("local", "oss-120b", 0)
    emb = embeddings("local-emb", registry=doc)
    assert isinstance(emb, QueuedOpenAIEmbeddings)
    assert (emb.model_alias, emb.model) == ("local-emb", "e5")


# FB-1: the started queue decides; one parse per queue or per file version --------------------


@pytest.fixture
def no_default_queue(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """No started queue, no registry or off switch in the environment; cleared after."""
    for name in ("LLMQ_REGISTRY", "LLMQ_REGISTRY_FILE", "LLMQ_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    set_default_queue(None)
    yield
    set_default_queue(None)


def _count_parses(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls = [0]
    real = validate.load_registry

    def counted(*args: Any, **kw: Any) -> Any:
        calls[0] += 1
        return real(*args, **kw)

    monkeypatch.setattr(validate, "load_registry", counted)
    return calls


@pytest.mark.usefixtures("no_default_queue")
async def test_factories_follow_a_started_queue_switched_off(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # The environment alone would build adapters: the started queue wins.
    monkeypatch.setenv("LLMQ_REGISTRY", json.dumps(DOC))
    assert chat_model(GPT) is not None
    off_by_default = LLMQueue.from_env({"LLMQ_STORE_URL": URL}, defaults={"LLMQ_ENABLED": False})
    # A secrets source that cannot be read, with LLMQ_CONFIG_ERROR_MODE=disable.
    missing = f"file:{tmp_path / 'missing.json'}"
    off_by_secret = LLMQueue.from_env(
        {**DEV, "LLMQ_STORE_URL": URL, "LLMQ_SECRETS_SOURCE": missing},
        defaults={"LLMQ_CONFIG_ERROR_MODE": "disable"},
    )
    for queue in (off_by_default, off_by_secret):
        assert not queue.enabled
        await queue.start()
        try:
            assert chat_model(GPT) is None
            assert embeddings(TITAN) is None
        finally:
            await queue.stop()
        assert chat_model(GPT) is not None  # stopped: the environment again


@pytest.mark.usefixtures("no_default_queue")
def test_factories_use_the_queues_registry_from_defaults_and_secrets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    doc = json.loads(json.dumps(DOC))
    doc["models"]["titan"].update(model_id="${EMBED_ID}", family="titan")
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(doc), encoding="utf-8")
    secret = tmp_path / "secret.json"
    secret.write_text(json.dumps({"EMBED_ID": "secret-embed-id"}), encoding="utf-8")
    env = {"LLMQ_STORE_URL": URL, "LLMQ_ENVIRONMENT": "dev"}
    env["LLMQ_SECRETS_SOURCE"] = f"file:{secret}"
    llmq = LLMQueue.from_env(env, defaults={"LLMQ_REGISTRY_FILE": str(path)})
    with pytest.raises(ConfigError, match="no registry"):  # not in the environment
        chat_model(GPT)
    set_default_queue(llmq)  # as LLMQueue.start() does
    parses = _count_parses(monkeypatch)
    reads = [0]
    real_read = Path.read_text

    def counted_read(self: Path, *args: Any, **kw: Any) -> str:
        reads[0] += 1
        return real_read(self, *args, **kw)

    monkeypatch.setattr(Path, "read_text", counted_read)
    # A ConfigMap edited after boot: the started queue keeps its boot models.
    path.write_text(json.dumps({**doc, "models": {}}), encoding="utf-8")
    for _ in range(20):
        emb = embeddings("secret-embed-id")
        assert emb.model_alias == "titan"
        assert chat_model(GPT).model_alias == "gpt120b"
    assert emb.llmq is None  # the default queue is looked up per call
    assert parses[0] == 1  # one parse per queue
    assert reads[0] == 0  # the boot document from_env read; the file is not read again


@pytest.mark.usefixtures("no_default_queue")
def test_factories_before_start_cache_the_file_and_see_a_change(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "registry.yaml"
    path.write_text(json.dumps(DOC), encoding="utf-8")  # JSON is YAML
    monkeypatch.setenv("LLMQ_REGISTRY_FILE", str(path))
    parses = _count_parses(monkeypatch)
    for _ in range(20):
        assert chat_model(GPT).model_alias == "gpt120b"
    assert parses[0] == 1
    # A ConfigMap update before start: a new mtime and size, read again.
    smaller = {**DOC, "models": {"titan": DOC["models"]["titan"]}}
    path.write_text(json.dumps(smaller), encoding="utf-8")
    later = path.stat().st_mtime_ns + 10**9
    os.utime(path, ns=(later, later))
    assert chat_model(GPT) is None
    assert embeddings(TITAN).model_alias == "titan"
    assert parses[0] == 2
    # A placeholder variable that changes is read again too.
    doc = json.loads(json.dumps(DOC))
    doc["models"]["titan"].update(model_id="${EMBED_ID}", family="titan")
    monkeypatch.delenv("LLMQ_REGISTRY_FILE")
    monkeypatch.setenv("LLMQ_REGISTRY", json.dumps(doc))
    monkeypatch.setenv("EMBED_ID", "one")
    assert embeddings("one").model_alias == "titan"
    monkeypatch.setenv("EMBED_ID", "two")
    assert embeddings("one") is None
    assert embeddings("two").model_alias == "titan"
