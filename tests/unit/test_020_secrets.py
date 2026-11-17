"""0.2.0 secrets: `LLMQ_SECRETS_SOURCE`, `LLMQ_SECRET_NAMES` and `llm_queue.secrets`.

No AWS call is made: the Secrets Manager client is a real botocore client under a `Stubber`,
handed in through the client factory hook.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from pathlib import Path
from typing import Any

import botocore.session
import pytest
from botocore.stub import Stubber

import llm_queue.core as core
import llm_queue.secrets as secrets_module
from llm_queue import ConfigError, LLMQueue
from llm_queue.clients import ClientPool
from llm_queue.engine.jobs import JobSettings
from llm_queue.engine.settings import InfraSettings, Layers
from llm_queue.engine.store_client import FreshPassword, secret_store_client
from llm_queue.secrets import (
    AwsSecretsManagerSource,
    EnvSource,
    FileSource,
    SecretReader,
    SecretSource,
    expand_spec,
    secrets_from_config,
)
from llm_queue.tuning import StoreOptions

SECRET_ID = "dev/researchassistant/secret"
PASSWORD = "pw-Sup3r-s3cret"  # every test checks this never shows
SALT = "salt-0123456789abcdef-secret"
DOC: dict[str, Any] = {
    "registry_version": 1,
    "accounts": {
        "aws-a": {"kind": "aws"},
        "vllm": {
            "kind": "openai",
            "base_url": "http://vllm.local/v1",
            "api_key_ref": "LLMQ_ACCOUNT_VLLM_KEY",
        },
    },
    "models": {
        "titan": {
            "type": "embedding",
            "provider": "bedrock",
            "model_id": "amazon.titan-embed-text-v2:0",
            "deployments": {"aws-a": {"confirmed": ["ap-south-1"]}},
        },
    },
}
DEV = {"LLMQ_ENVIRONMENT": "dev", "LLMQ_REGISTRY": json.dumps(DOC)}


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _stubbed(*replies: dict[str, Any] | Exception) -> tuple[Any, Stubber]:
    """A botocore Secrets Manager client whose `get_secret_value` answers `replies` in order."""
    session = botocore.session.get_session()
    client = session.create_client(
        "secretsmanager",
        region_name="ap-south-1",
        aws_access_key_id="x",
        aws_secret_access_key="y",
    )
    stub = Stubber(client)
    for reply in replies:
        if isinstance(reply, Exception):
            code = str(reply)
            stub.add_client_error(
                "get_secret_value", service_error_code=code, service_message=f"no {PASSWORD}"
            )
        else:
            stub.add_response("get_secret_value", reply, {"SecretId": SECRET_ID})
    stub.activate()
    return client, stub


def _secret(values: dict[str, Any]) -> dict[str, Any]:
    return {
        "ARN": "arn:aws:secretsmanager:ap-south-1:1:secret:x",
        "SecretString": json.dumps(values),
    }


def _factory(client: Any) -> Any:
    seen: list[tuple[str | None, float]] = []

    def make(region: str | None, timeout: float) -> Any:
        seen.append((region, timeout))
        return client

    make.seen = seen  # type: ignore[attr-defined]
    return make


def _no_secret_in(*texts: str) -> None:
    for text in texts:
        assert PASSWORD not in text
        assert SALT not in text


# Sources --------------------------------------------------------------------------------------


def test_aws_secret_json_values_and_a_missing_key() -> None:
    client, stub = _stubbed(_secret({"valkey_key": PASSWORD, "valkey_port": 6380, "off": None}))
    factory = _factory(client)
    reader = secrets_from_config(
        f"aws-secretsmanager:{SECRET_ID}",
        environ={"AWS_REGION": "ap-south-1"},
        client_factory=factory,
        timeout_secs=4,
    )
    assert reader is not None
    assert reader("valkey_key") == PASSWORD
    assert reader("valkey_port") == "6380"  # a JSON number reads as its text
    assert reader("off") is None  # null is unset
    assert reader("missing") is None
    assert factory.seen == [("ap-south-1", 4)]
    stub.assert_no_pending_responses()
    assert isinstance(reader.sources[0], SecretSource)
    assert reader.describe() == {
        "sources": [f"aws-secretsmanager:{SECRET_ID}"],
        "cache_secs": 300.0,
    }
    _no_secret_in(repr(reader), repr(reader.sources[0]), repr(reader.describe()))


@pytest.mark.parametrize("code", ["AccessDeniedException", "ResourceNotFoundException"])
def test_aws_denied_or_missing_secret_is_a_config_error_naming_the_source(code: str) -> None:
    client, _ = _stubbed(Exception(code))
    with pytest.raises(ConfigError) as info:
        secrets_from_config(f"aws-secretsmanager:{SECRET_ID}", client_factory=_factory(client))
    text = str(info.value)
    assert f"aws-secretsmanager:{SECRET_ID}" in text
    assert code in text
    _no_secret_in(text)  # the service message is not repeated


def test_aws_secret_that_is_not_a_json_object() -> None:
    client, _ = _stubbed({"SecretString": f"plain {PASSWORD}"}, {"SecretString": "[1]"})
    source = AwsSecretsManagerSource(SECRET_ID, client_factory=_factory(client))
    with pytest.raises(ConfigError, match="is not valid JSON") as info:
        source.load()
    _no_secret_in(str(info.value))
    with pytest.raises(ConfigError, match="must hold a JSON object"):
        source.load()


def test_aws_region_setting_beats_the_aws_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Through the layers: LLMQ_SECRETS_REGION, else AWS_REGION, else AWS_DEFAULT_REGION."""
    regions: list[str | None] = []

    def fake(region: str | None, timeout: float) -> Any:
        regions.append(region)
        client, _ = _stubbed(_secret({"k": "v"}))
        return client

    monkeypatch.setattr(secrets_module, "_botocore_client", fake)  # the factory hook
    spec = {"LLMQ_SECRETS_SOURCE": f"aws-secretsmanager:{SECRET_ID}"}
    Layers({**spec, "LLMQ_SECRETS_REGION": "eu-west-1", "AWS_REGION": "ap-south-1"})
    Layers({**spec, "AWS_REGION": "ap-south-1", "AWS_DEFAULT_REGION": "us-east-1"})
    Layers({**spec, "AWS_DEFAULT_REGION": "us-east-1", "LLMQ_SECRETS_TIMEOUT_SECS": "3"})
    assert regions == ["eu-west-1", "ap-south-1", "us-east-1"]


def test_file_directory_like_a_kubernetes_secret_volume(tmp_path: Path) -> None:
    (tmp_path / "valkey_key").write_bytes(PASSWORD.encode() + b"\n")
    (tmp_path / "crlf").write_bytes(b"v\r\n")
    (tmp_path / "two_lines").write_bytes(b"a\n\n")
    (tmp_path / "..data").mkdir()
    (tmp_path / "..data" / "x").write_text("hidden", encoding="utf-8")
    reader = secrets_from_config(f"file:{tmp_path}")
    assert reader is not None
    assert reader("valkey_key") == PASSWORD  # one trailing newline stripped
    assert reader("crlf") == "v"
    assert reader("two_lines") == "a\n"  # only one
    assert reader("..data") is None
    assert reader("missing") is None


def test_file_json_and_dotenv(tmp_path: Path) -> None:
    as_json = tmp_path / "secret.json"
    as_json.write_text(json.dumps({"valkey_key": PASSWORD, "port": 6380}), encoding="utf-8")
    dotenv = tmp_path / "app.env"
    dotenv.write_text(
        "# comment\n\nexport A=1\nB = 'two words'\nC=\"x=y\"\nEMPTY=\n", encoding="utf-8"
    )
    reader = secrets_from_config(f"file:{as_json}")
    assert reader is not None
    assert (reader("valkey_key"), reader("port")) == (PASSWORD, "6380")
    values = FileSource(dotenv).load()
    assert dict(values) == {"A": "1", "B": "two words", "C": "x=y", "EMPTY": ""}
    reader = secrets_from_config(f"file:{dotenv}")
    assert reader is not None
    assert reader("EMPTY") is None  # empty counts as unset


def test_file_errors_name_the_source_not_the_values(tmp_path: Path) -> None:
    bad = tmp_path / "bad.env"
    bad.write_text(f"OK=1\n{PASSWORD}\n", encoding="utf-8")
    with pytest.raises(ConfigError) as info:
        secrets_from_config(f"file:{bad}")
    assert f"file:{bad}" in str(info.value)
    assert "line 2 is not KEY=VALUE" in str(info.value)
    _no_secret_in(str(info.value))
    broken = tmp_path / "broken.json"
    broken.write_text('{"k": "' + PASSWORD, encoding="utf-8")
    with pytest.raises(ConfigError, match="is not valid JSON") as info:
        secrets_from_config(f"file:{broken}")
    _no_secret_in(str(info.value))
    with pytest.raises(ConfigError, match="cannot be read"):
        secrets_from_config(f"file:{tmp_path / 'absent'}")


def test_env_source_reads_the_environment_given() -> None:
    reader = secrets_from_config("env", environ={"valkey_key": PASSWORD})
    assert reader is not None
    assert reader("valkey_key") == PASSWORD
    assert isinstance(reader.sources[0], EnvSource)


def test_none_and_bad_specs() -> None:
    assert secrets_from_config("none") is None
    assert secrets_from_config(" none ") is None
    for spec, words in (
        ("none,env", "cannot be combined"),
        ("vault:x", "must be aws-secretsmanager:<secret-id>"),
        ("file:", "must be aws-secretsmanager"),
        ("aws-secretsmanager", "must be aws-secretsmanager"),
        (" , ", "must name a source"),
    ):
        with pytest.raises(ConfigError, match=words):
            secrets_from_config(spec)


def test_sources_are_tried_in_order(tmp_path: Path) -> None:
    (tmp_path / "a").write_text("from-file", encoding="utf-8")
    client, _ = _stubbed(_secret({"a": "from-aws", "b": "from-aws"}))
    reader = secrets_from_config(
        f"file:{tmp_path}, aws-secretsmanager:{SECRET_ID}, env",
        environ={"b": "from-env", "c": "from-env"},
        client_factory=_factory(client),
    )
    assert reader is not None
    assert (reader("a"), reader("b"), reader("c"), reader("d")) == (
        "from-file",
        "from-aws",
        "from-env",
        None,
    )
    assert [s.kind for s in reader.sources] == ["file", "aws-secretsmanager", "env"]


def test_placeholders_in_the_spec_come_from_the_environment(tmp_path: Path) -> None:
    env = {"ENV": "dev"}
    assert expand_spec("aws-secretsmanager:${ENV}/researchassistant/secret", env) == (
        "aws-secretsmanager:dev/researchassistant/secret"
    )
    assert expand_spec("file:${DIR:-/etc/app}", env) == "file:/etc/app"
    with pytest.raises(ConfigError, match="`MISSING` is not set"):
        expand_spec("aws-secretsmanager:${MISSING}/x", env)
    client, stub = _stubbed(_secret({"k": "v"}))
    reader = secrets_from_config(
        "aws-secretsmanager:${ENV}/researchassistant/secret",
        environ=env,
        client_factory=_factory(client),
    )
    assert reader is not None
    assert reader.describe()["sources"] == [f"aws-secretsmanager:{SECRET_ID}"]
    stub.assert_no_pending_responses()  # the stub checked SecretId == the expanded id


# Cache ----------------------------------------------------------------------------------------


def test_values_are_cached_then_read_again_after_cache_secs(tmp_path: Path) -> None:
    key = tmp_path / "valkey_key"
    key.write_text("v1", encoding="utf-8")
    clock = Clock()
    reader = secrets_from_config(f"file:{tmp_path}", cache_secs=60, clock=clock)
    assert reader is not None
    key.write_text("v2", encoding="utf-8")  # rotated
    clock.now += 59
    assert reader("valkey_key") == "v1"  # still cached
    clock.now += 1
    assert reader("valkey_key") == "v2"  # read again after cache_secs


def test_cache_zero_reads_every_lookup(tmp_path: Path) -> None:
    key = tmp_path / "k"
    key.write_text("v1", encoding="utf-8")
    reader = secrets_from_config(f"file:{tmp_path}", cache_secs=0)
    assert reader is not None
    key.write_text("v2", encoding="utf-8")
    assert reader("k") == "v2"


def test_aws_rotation_is_seen_after_the_cache_and_a_failed_refresh_keeps_values(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client, stub = _stubbed(
        _secret({"valkey_key": "old"}),
        _secret({"valkey_key": PASSWORD}),
        Exception("ThrottlingException"),
    )
    clock = Clock()
    reader = secrets_from_config(
        f"aws-secretsmanager:{SECRET_ID}", client_factory=_factory(client), clock=clock
    )
    assert reader is not None
    assert reader("valkey_key") == "old"
    clock.now += 300
    assert reader("valkey_key") == PASSWORD
    clock.now += 300
    with caplog.at_level(logging.WARNING, logger="llm_queue"):
        assert reader("valkey_key") == PASSWORD  # the last values are kept
    assert "secrets_refresh_failed" in caplog.text
    assert "ThrottlingException" in caplog.text
    _no_secret_in(caplog.text)
    stub.assert_no_pending_responses()


# Layers: LLMQ_SECRETS_SOURCE, LLMQ_SECRET_NAMES ------------------------------------------------


def _volume(tmp_path: Path, **values: str) -> Path:
    for name, value in values.items():
        (tmp_path / name).write_text(value + "\n", encoding="utf-8")
    return tmp_path


def test_settings_from_a_source_under_mapped_names(tmp_path: Path) -> None:
    volume = _volume(
        tmp_path,
        valkey_host="cache.internal",
        valkey_port="6380",
        valkey_key=PASSWORD,
        llmq_id_hash_salt=SALT,
    )
    names = {
        "LLMQ_STORE_HOST": "valkey_host",
        "LLMQ_STORE_PORT": "valkey_port",
        "LLMQ_STORE_PASSWORD": "valkey_key",
        "LLMQ_ID_HASH_SALT": "llmq_id_hash_salt",
    }
    settings = InfraSettings.from_env(
        {
            **DEV,
            "LLMQ_STORE_TLS_ENABLED": "false",
            "LLMQ_SECRETS_SOURCE": f"file:{volume}",
            "LLMQ_SECRET_NAMES": json.dumps(names),
        }
    )
    assert settings.store_url.startswith("redis://:")
    assert settings.store_url.endswith("@cache.internal:6380/0")
    assert settings.id_hash_salt == SALT
    assert settings.secrets_source == f"file:{volume}"
    assert settings.secret_sources == (f"file:{volume}",)
    assert settings.secret_names == tuple(sorted(names))
    _no_secret_in(repr(settings))


def test_secret_names_argument_wins_per_setting(tmp_path: Path) -> None:
    volume = _volume(tmp_path, env_key="from-env-map", arg_key="from-arg", other="o")
    layers = Layers(
        {
            "LLMQ_SECRETS_SOURCE": f"file:{volume}",
            "LLMQ_SECRET_NAMES": json.dumps(
                {"LLMQ_ADMIN_TOKEN": "env_key", "LLMQ_DEFAULT_TENANT": "other"}
            ),
        },
        secret_names={"LLMQ_ADMIN_TOKEN": "arg_key"},
    )
    assert layers.get("LLMQ_ADMIN_TOKEN") == "from-arg"
    assert layers.get("LLMQ_DEFAULT_TENANT") == "o"
    assert layers.secret_names == {"LLMQ_ADMIN_TOKEN": "arg_key", "LLMQ_DEFAULT_TENANT": "other"}


def test_secret_names_from_defaults_as_a_mapping(tmp_path: Path) -> None:
    volume = _volume(tmp_path, tenant="acme")
    layers = Layers(
        {},
        defaults={
            "LLMQ_SECRETS_SOURCE": f"file:{volume}",
            "LLMQ_SECRET_NAMES": {"LLMQ_DEFAULT_TENANT": "tenant"},
        },
    )
    assert layers.get("LLMQ_DEFAULT_TENANT") == "acme"


def test_secret_names_are_validated_all_at_once() -> None:
    with pytest.raises(ConfigError) as info:
        Layers(
            {
                "LLMQ_SECRET_NAMES": json.dumps(
                    {
                        "STORE_HOST": "h",
                        "LLMQ_STORE_PORT": "",
                        "LLMQ_SECRETS_SOURCE": "src",
                        "LLMQ_KEY_PREFIX": 3,
                    }
                ),
                "LLMQ_SECRETS_CACHE_SECS": "-1",
            },
            secret_names={"LLMQ_ADMIN_TOKEN": " "},
        )
    text = str(info.value)
    assert "'STORE_HOST' is not an LLMQ_* setting" in text
    assert "LLMQ_SECRET_NAMES: LLMQ_STORE_PORT must name a secret key" in text
    assert "LLMQ_SECRETS_SOURCE is read from the environment, not a secret" in text
    assert "LLMQ_SECRET_NAMES: LLMQ_KEY_PREFIX must name a secret key" in text
    assert "secret_names: LLMQ_ADMIN_TOKEN must name a secret" in text
    assert "LLMQ_SECRETS_CACHE_SECS must be 0 or more" in text
    for bad in ("not json", "[1]"):
        with pytest.raises(ConfigError, match="LLMQ_SECRET_NAMES must be a JSON object"):
            Layers({"LLMQ_SECRET_NAMES": bad})


def test_the_secrets_settings_are_never_read_from_a_secret(tmp_path: Path) -> None:
    volume = _volume(tmp_path, LLMQ_SECRETS_CACHE_SECS="5", LLMQ_STORE_PASSWORD="x")
    layers = Layers({"LLMQ_SECRETS_SOURCE": f"file:{volume}"})
    assert layers.secrets_cache_secs == 300
    assert layers.get("LLMQ_SECRETS_CACHE_SECS") is None
    assert layers.get("LLMQ_STORE_PASSWORD") == "x"


def test_unreadable_source_at_start_names_it(tmp_path: Path) -> None:
    missing = tmp_path / "nope"
    with pytest.raises(ConfigError) as info:
        InfraSettings.from_env({**DEV, "LLMQ_SECRETS_SOURCE": f"file:{missing}"})
    assert str(info.value).startswith("invalid LLMQ settings:")
    assert f"file:{missing}" in str(info.value)


def test_source_placeholder_from_the_environ_layer(tmp_path: Path) -> None:
    (tmp_path / "dev").mkdir()
    _volume(tmp_path / "dev", k="v")
    layers = Layers({"LLMQ_SECRETS_SOURCE": f"file:{tmp_path}/${{ENV}}", "ENV": "dev"})
    assert layers.secret("k") == "v"
    with pytest.raises(ConfigError, match="`ENV` is not set"):
        Layers({"LLMQ_SECRETS_SOURCE": f"file:{tmp_path}/${{ENV}}"})


def test_code_secrets_win_over_the_source_which_is_not_read(tmp_path: Path) -> None:
    layers = Layers(
        {"LLMQ_SECRETS_SOURCE": f"file:{tmp_path / 'absent'}", "LLMQ_KEY_PREFIX": "env"},
        {"LLMQ_KEY_PREFIX": "code"}.get,
        {"LLMQ_KEY_PREFIX": "default", "LLMQ_DEFAULT_TENANT": "d"},
    )
    assert layers.get("LLMQ_KEY_PREFIX") == "code"
    assert layers.get("LLMQ_DEFAULT_TENANT") == "d"  # environment, then defaults, as before
    assert layers.describe_secrets() == {"sources": ["code"], "names": []}


def test_env_source_maps_other_variable_names(tmp_path: Path) -> None:
    layers = Layers(
        {
            "LLMQ_SECRETS_SOURCE": "env",
            "LLMQ_SECRET_NAMES": '{"LLMQ_ADMIN_TOKEN": "APP_ADMIN_TOKEN"}',
            "APP_ADMIN_TOKEN": "tok",
        }
    )
    assert layers.get("LLMQ_ADMIN_TOKEN") == "tok"


# Every reader goes through the source ---------------------------------------------------------


def test_queue_password_jobs_admin_token_and_account_refs_through_the_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    volume = _volume(
        tmp_path,
        valkey_key=PASSWORD,
        admin=PASSWORD + "-admin",
        LLMQ_JOB_CONCURRENCY="7",
        LLMQ_ACCOUNT_VLLM_KEY=PASSWORD + "-vllm",
    )
    stores: list[Any] = []
    pools: list[dict[str, Any]] = []
    real_store, real_pool = secret_store_client, ClientPool

    def store_spy(url: str, lookup: Any, name: str, *args: Any) -> Any:
        client = real_store(url, lookup, name, *args)  # lazy: no connection is made
        stores.append(client)
        return client

    def pool_spy(**kwargs: Any) -> Any:
        pools.append(kwargs)
        return real_pool(**kwargs)

    monkeypatch.setattr(core, "secret_store_client", store_spy)
    monkeypatch.setattr(core, "ClientPool", pool_spy)
    with caplog.at_level(logging.DEBUG, logger="llm_queue"):
        llmq = LLMQueue.from_env(
            {
                "LLMQ_STORE_URL": "redis://localhost:6379/0",
                **DEV,
                "LLMQ_SECRETS_SOURCE": f"file:{volume}",
                "LLMQ_SECRET_NAMES": json.dumps(
                    {"LLMQ_STORE_PASSWORD": "valkey_key", "LLMQ_ADMIN_TOKEN": "admin"}
                ),
            }
        )
    provider = stores[0].connection_pool.connection_kwargs["credential_provider"]
    assert isinstance(provider, FreshPassword)
    assert provider.get_credentials() == (PASSWORD,)
    assert llmq._admin_token() == PASSWORD + "-admin"
    assert llmq._jobs.limits.concurrency == 7
    assert pools[0]["secrets"]("LLMQ_ACCOUNT_VLLM_KEY") == PASSWORD + "-vllm"
    described = llmq.describe()
    assert described["secrets"] == {
        "sources": [f"file:{volume}"],
        "names": ["LLMQ_ADMIN_TOKEN", "LLMQ_STORE_PASSWORD"],
        "cache_secs": 300.0,
    }
    _no_secret_in(repr(described), caplog.text, repr(llmq.settings))


def test_store_password_is_read_through_the_cache_on_each_new_connection(
    tmp_path: Path,
) -> None:
    key = tmp_path / "LLMQ_STORE_PASSWORD"
    key.write_text("v1", encoding="utf-8")
    clock = Clock()
    reader = secrets_from_config(f"file:{tmp_path}", cache_secs=30, clock=clock)
    assert reader is not None
    layers = Layers({"LLMQ_STORE_URL": "redis://localhost:6379/0"}, reader)
    client = secret_store_client(
        "redis://localhost:6379/0", layers.get, "LLMQ_STORE_PASSWORD", 4, StoreOptions()
    )
    provider = client.connection_pool.connection_kwargs["credential_provider"]
    assert provider.get_credentials() == ("v1",)
    key.write_text("v2", encoding="utf-8")  # rotated in the volume
    assert provider.get_credentials() == ("v1",)  # within the cache time
    clock.now += 30
    assert provider.get_credentials() == ("v2",)


def test_job_settings_read_the_source(tmp_path: Path) -> None:
    volume = _volume(tmp_path, LLMQ_JOB_CONCURRENCY="3")
    jobs = JobSettings.from_env(Layers({"LLMQ_SECRETS_SOURCE": f"file:{volume}"}))
    assert jobs.concurrency == 3


async def test_cli_honours_the_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from llm_queue.engine import cli, store_client

    volume = _volume(tmp_path, LLMQ_STORE_PASSWORD=PASSWORD, LLMQ_KEY_PREFIX="from-volume")
    seen: list[tuple[str | None, str]] = []

    class Stop(Exception):
        pass

    def spy(url: str, lookup: Any, name: str, *args: Any) -> Any:
        seen.append((lookup(name), url))
        raise Stop

    monkeypatch.setattr(store_client, "secret_store_client", spy)  # what AdminServices uses
    with pytest.raises(Stop):
        await cli.amain(
            ["models", "list"],
            {
                "LLMQ_STORE_URL": "redis://localhost:6379/0",
                **DEV,
                "LLMQ_SECRETS_SOURCE": f"file:{volume}",
            },
        )
    assert seen == [(PASSWORD, "redis://localhost:6379/0")]


def test_purge_service_honours_the_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from llm_queue.engine import store_client
    from llm_queue.engine.purge import PurgeService

    volume = _volume(tmp_path, LLMQ_STORE_PASSWORD=PASSWORD)
    seen: list[str | None] = []

    class Stop(Exception):
        pass

    def spy(url: str, lookup: Any, name: str, *args: Any) -> Any:
        seen.append(lookup(name))
        raise Stop

    monkeypatch.setattr(store_client, "secret_store_client", spy)
    with pytest.raises(Stop):
        PurgeService.from_env(
            {
                "LLMQ_STORE_URL": "redis://localhost:6379/0",
                **DEV,
                "LLMQ_SECRETS_SOURCE": f"file:{volume}",
            }
        )
    assert seen == [PASSWORD]


def test_describe_without_a_source() -> None:
    llmq = LLMQueue.from_env({"LLMQ_STORE_URL": "redis://localhost:6379/0", **DEV})
    assert llmq.describe()["secrets"] == {"sources": [], "names": [], "cache_secs": 300.0}


def test_secrets_module_reader_is_thread_safe_enough(tmp_path: Path) -> None:
    """Many threads at expiry: each sees a value, the source is read a bounded number of times."""
    import threading

    loads: list[int] = []

    class Counting:
        kind = "count"
        label = "count"

        def load(self) -> dict[str, str]:
            loads.append(1)
            return {"k": "v"}

    clock = Clock()
    reader = SecretReader([Counting()], cache_secs=10, clock=clock)
    reader.check()
    clock.now += 10
    out: list[str | None] = []
    threads = [threading.Thread(target=lambda: out.append(reader("k"))) for _ in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert out == ["v"] * 20
    assert len(loads) == 2  # the start check and one refresh


def test_module_constants_match_the_settings_module() -> None:
    from llm_queue.engine import settings

    assert frozenset(secrets_module.SETTINGS) == settings._SECRETS_SETTINGS


# Never block the event loop (review S-1) -----------------------------------------------------


class _Slow:
    """A source whose second and later reads wait on `gate`; `fail` makes them raise."""

    kind = "slow"
    label = "slow"

    def __init__(self) -> None:
        self.gate = threading.Event()
        self.loads = 0
        self.fail = False

    def load(self) -> dict[str, str]:
        self.loads += 1
        if self.loads > 1:
            self.gate.wait(5)
            if self.fail:
                raise ConfigError("slow: throttled")
        return {"k": f"v{self.loads}"}


async def test_an_expired_source_is_refreshed_off_the_loop_while_the_last_value_serves() -> None:
    source = _Slow()
    clock = Clock()
    reader = SecretReader([source], cache_secs=60, clock=clock)
    reader.check()
    clock.now += 60
    assert reader("k") == "v1"  # stale, at once: the refresh runs on a thread
    assert reader("k") == "v1"  # one refresh at a time
    source.gate.set()
    for _ in range(100):
        if reader("k") == "v2":
            break
        await asyncio.sleep(0.01)
    assert reader("k") == "v2"
    assert source.loads == 2


def test_a_failed_refresh_is_retried_after_thirty_seconds_not_a_cache_period(
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = _Slow()
    source.gate.set()
    clock = Clock()
    reader = SecretReader([source], cache_secs=300, clock=clock)
    reader.check()
    clock.now += 300
    source.fail = True
    with caplog.at_level(logging.WARNING, logger="llm_queue"):
        assert reader("k") == "v1"  # kept
    assert "secrets_refresh_failed" in caplog.text
    source.fail = False
    clock.now += 29
    assert reader("k") == "v1"
    clock.now += 1
    assert reader("k") == "v3"  # tried again after min(300, 30) s


def test_the_admin_token_guard_reads_a_secret_off_the_event_loop() -> None:
    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient

    from llm_queue.engine.http import admin_token_dependency

    lookups: list[int] = []
    handlers: list[int] = []

    def token() -> str:
        lookups.append(threading.get_ident())
        return "s3cret"

    app = FastAPI()
    guard = admin_token_dependency(token, "X-LLMQ-Token")

    @app.get("/x", dependencies=[Depends(guard)])
    async def x() -> dict[str, bool]:
        handlers.append(threading.get_ident())  # async: runs on the loop's thread
        return {"ok": True}

    with TestClient(app) as client:
        assert client.get("/x", headers={"X-LLMQ-Token": "s3cret"}).status_code == 200
        assert client.get("/x", headers={"X-LLMQ-Token": "nope"}).status_code == 403
    assert len(lookups) == 2
    assert handlers[0] not in lookups  # the lookup ran on a worker thread


# Review S-3 -----------------------------------------------------------------------------------


def test_a_setting_named_twice_in_secret_names_is_refused() -> None:
    names = '{"LLMQ_STORE_PASSWORD": "a", "LLMQ_STORE_PASSWORD": "b"}'
    with pytest.raises(ConfigError, match="LLMQ_SECRET_NAMES: LLMQ_STORE_PASSWORD is set twice"):
        Layers({**DEV, "LLMQ_SECRET_NAMES": names})


def test_secret_names_argument_values_are_stripped(tmp_path: Path) -> None:
    volume = tmp_path / "vol"
    volume.mkdir()
    (volume / "valkey_key").write_text(PASSWORD, encoding="utf-8")
    layers = Layers(
        {**DEV, "LLMQ_SECRETS_SOURCE": f"file:{volume}"},
        secret_names={" LLMQ_STORE_PASSWORD ": " valkey_key\n"},
    )
    assert layers.get("LLMQ_STORE_PASSWORD") == PASSWORD
    assert layers.secret_names == {"LLMQ_STORE_PASSWORD": "valkey_key"}


def test_dotenv_inline_comments_and_quotes_follow_the_documented_rule(tmp_path: Path) -> None:
    dotenv = tmp_path / "app.env"
    dotenv.write_text(
        "A=abc # the password\n"
        "B=a#b\n"
        'C="x # y"  # quoted: kept\n'
        "D='q' \n"
        "E=   # only a comment\n"
        "F=tab\t# comment\n",
        encoding="utf-8",
    )
    values = dict(FileSource(dotenv).load())
    assert values == {"A": "abc", "B": "a#b", "C": "x # y", "D": "q", "E": "", "F": "tab"}
    for bad in ('G="open\n', "H='a' trailing\n"):
        dotenv.write_text(bad, encoding="utf-8")
        with pytest.raises(ConfigError, match="line 1 is not KEY=VALUE") as info:
            FileSource(dotenv).load()
        assert "open" not in str(info.value)
        assert "trailing" not in str(info.value)


class _Flaky:
    """An app's own source: works once, then raises a non-`ConfigError` echoing a value."""

    kind = "vault"
    label = "vault:app"

    def __init__(self) -> None:
        self.calls = 0

    def load(self) -> dict[str, str]:
        self.calls += 1
        if self.calls > 1:
            raise RuntimeError(f"vault said no for {PASSWORD}")
        return {"LLMQ_ADMIN_TOKEN": PASSWORD}


def test_a_custom_source_raising_on_a_hot_lookup_keeps_the_last_values(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = Clock()
    source = _Flaky()
    reader = SecretReader([source], cache_secs=10, clock=clock)
    assert reader("LLMQ_ADMIN_TOKEN") == PASSWORD
    clock.now += 11
    with caplog.at_level(logging.WARNING, logger="llm_queue"):
        assert reader("LLMQ_ADMIN_TOKEN") == PASSWORD  # kept, not raised
    assert source.calls == 2
    assert "secrets_refresh_failed" in caplog.text
    assert "RuntimeError" in caplog.text
    _no_secret_in(caplog.text)


def test_a_custom_source_raising_at_start_is_a_config_error_naming_it() -> None:
    source = _Flaky()
    source.calls = 1  # the next load raises
    with pytest.raises(ConfigError, match=r"vault:app: cannot be read \(RuntimeError\)") as info:
        SecretReader([source]).check()
    _no_secret_in(str(info.value))
