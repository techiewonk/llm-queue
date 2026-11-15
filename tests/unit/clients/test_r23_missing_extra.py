"""Every `MissingExtra` raise site names its extra when the optional package is missing (CG-6).

The package is blocked in `sys.modules` (an import of it then fails), so the tests run with
every extra installed.
"""

from __future__ import annotations

import sys
from typing import Any

import pytest
from clients_testkit import registry_doc

from llm_queue import MissingExtra
from llm_queue.clients import ClientPool, route_for
from llm_queue.clients.hub import run
from llm_queue.registry import load_registry


def _block(monkeypatch: pytest.MonkeyPatch, *names: str) -> None:
    for name in names:
        monkeypatch.setitem(sys.modules, name, None)


def test_uvloop(monkeypatch: pytest.MonkeyPatch) -> None:
    """`run(use_uvloop=True)` without uvloop names the `uvloop` extra."""
    _block(monkeypatch, "uvloop")

    async def main() -> None:
        return None

    coro = main()
    try:
        with pytest.raises(MissingExtra, match="uvloop"):
            run(coro, use_uvloop=True)
    finally:
        coro.close()


async def test_openai(monkeypatch: pytest.MonkeyPatch) -> None:
    """An openai account without the SDK names the `openai` extra."""
    view = load_registry(registry_doc("http://127.0.0.1:1"))
    route = route_for(view, next(iter(view.deployments("local"))))
    _block(monkeypatch, "openai")
    pool = ClientPool(secrets={"LLMQ_ACCOUNT_VLLM_KEY": "k"}.get)
    try:
        with pytest.raises(MissingExtra, match="openai"):
            await pool.openai(route)
    finally:
        await pool.aclose()


def test_http(monkeypatch: pytest.MonkeyPatch) -> None:
    """`http_router` without FastAPI names the `http` extra."""
    from llm_queue.engine.http import status_router

    _block(monkeypatch, "fastapi", "fastapi.responses")
    engine: Any = None
    with pytest.raises(MissingExtra, match="http"):
        status_router(engine)


def test_prometheus(monkeypatch: pytest.MonkeyPatch) -> None:
    """`PrometheusSink` without prometheus_client names the `metrics` extra."""
    from llm_queue.engine.metrics import PrometheusSink

    _block(monkeypatch, "prometheus_client")
    with pytest.raises(MissingExtra, match="metrics"):
        PrometheusSink()


@pytest.mark.parametrize(
    ("name", "blocked", "extra"),
    [
        ("QueuedChatBedrockConverse", "langchain_aws", "langchain-aws"),
        ("QueuedChatOpenAI", "langchain_openai", "langchain-openai"),
    ],
)
def test_langchain(monkeypatch: pytest.MonkeyPatch, name: str, blocked: str, extra: str) -> None:
    """A LangChain adapter without its packages names its extra."""
    import llm_queue.langchain as adapters

    module = "llm_queue.langchain." + ("bedrock" if "Bedrock" in name else "openai")
    monkeypatch.delitem(sys.modules, module, raising=False)
    monkeypatch.delitem(vars(adapters), name, raising=False)
    _block(monkeypatch, blocked)
    with pytest.raises(MissingExtra, match=extra):
        getattr(adapters, name)
