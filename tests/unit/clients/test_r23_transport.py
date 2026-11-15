"""aiohttp transport parity with botocore: proxy variables and CA bundles (FC-1, R3SE-12, Q31).

botocore reads `HTTPS_PROXY` / `NO_PROXY` and `REQUESTS_CA_BUNDLE`; the aiohttp transport (the
default) read neither, so a pod behind an egress proxy or a TLS-inspecting gateway timed out
or failed every Bedrock call after the transport switch.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from clients_testkit import StartFake, registry_doc

from llm_queue.clients import ClientPool, Providers, Timing, WarmOptions, route_for
from llm_queue.clients.bedrock_http import BedrockHTTP, ca_bundle
from llm_queue.registry import load_registry
from llm_queue.tuning import BedrockOptions

CHAT = {"messages": [{"role": "user", "content": [{"text": "hello"}]}]}
PLAIN = Timing(5, 5, attempt_secs=5)


async def _open(url: str) -> BedrockHTTP:
    return await BedrockHTTP.open(
        region="ap-south-1",
        credentials=None,
        endpoint_url=url,
        read_timeout_secs=5,
        connect_timeout_secs=5,
        pool_size=2,
        keepalive_secs=5,
    )


@pytest.mark.parametrize("name", ["HTTPS_PROXY", "https_proxy"])
async def test_the_aiohttp_transport_uses_the_proxy_variables(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    """FC-1: `ProxyConfiguration()` had no proxies, so `proxy` was always None."""
    for var in ("HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv(name, "http://proxy.example:3128")
    client = await _open("https://bedrock-runtime.ap-south-1.amazonaws.com")
    try:
        assert client.proxy == "http://proxy.example:3128"
    finally:
        await client.close()
    monkeypatch.setenv("NO_PROXY", ".amazonaws.com")
    client = await _open("https://bedrock-runtime.ap-south-1.amazonaws.com")
    try:
        assert client.proxy is None  # NO_PROXY is honoured as botocore does
    finally:
        await client.close()


async def test_calls_go_through_the_proxy(
    start_fake: StartFake, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FC-1: a call to an unreachable endpoint succeeds through `HTTP_PROXY` (fakellm as proxy).

    aiohttp sends a plain-HTTP request to a proxy with the absolute URL; fakellm routes it by
    path, so the call only works when it went to the proxy.
    """
    fake, _ = await start_fake()
    for var in ("NO_PROXY", "no_proxy", "HTTP_PROXY", "http_proxy"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("HTTP_PROXY", fake)
    endpoint = "http://bedrock.proxied.invalid"
    view = load_registry(registry_doc(fake))
    route = route_for(view, next(d for d in view.deployments("gpt120b") if d.account == "aws-a"))
    providers = Providers(
        ClientPool(
            bedrock_endpoint_url=endpoint,
            credentials=lambda a: {"aws_access_key_id": a.name, "aws_secret_access_key": "x"},
            options=BedrockOptions(transport="aiohttp"),
            warm=WarmOptions(connections=0),
        )
    )
    try:
        result = await providers.get("bedrock").call("converse", CHAT, route, PLAIN)
        assert result.usage.output_tokens
    finally:
        await providers.aclose()


def test_requests_ca_bundle_is_trusted(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """R3SE-12: only `AWS_CA_BUNDLE` or certifi were loaded; `REQUESTS_CA_BUNDLE` was ignored."""
    import certifi

    bundle = tmp_path / "ca.pem"
    bundle.write_bytes(Path(certifi.where()).read_bytes())
    monkeypatch.delenv("AWS_CA_BUNDLE", raising=False)
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(bundle))
    assert ca_bundle() == str(bundle)
    monkeypatch.setenv("AWS_CA_BUNDLE", certifi.where())
    assert ca_bundle() == certifi.where()  # botocore's order: AWS_CA_BUNDLE first


async def test_verify_false_is_allowed_with_a_warning(
    start_fake: StartFake, caplog: pytest.LogCaptureFixture
) -> None:
    """Q31: `verify=False` in `credentials=` turned TLS checks off without a word."""
    url, _ = await start_fake()
    view = load_registry(registry_doc(url))
    route = route_for(view, next(d for d in view.deployments("gpt120b") if d.account == "aws-a"))
    pool = ClientPool(
        bedrock_endpoint_url=url,
        credentials=lambda a: {
            "aws_access_key_id": a.name,
            "aws_secret_access_key": "x",
            "verify": False,
        },
        warm=WarmOptions(connections=0),
    )
    try:
        await pool.bedrock(route)
    finally:
        await pool.aclose()
    assert any("bedrock_tls_verify_off" in r.getMessage() for r in caplog.records)
