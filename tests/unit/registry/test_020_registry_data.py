"""0.2.0 registry as data: placeholders and YAML (H3), auto residency (H4), quotas (H5), and the
derived breaker probe default (D4)."""

from __future__ import annotations

import builtins
import copy
import json
import re
from pathlib import Path
from typing import Any

import pytest

from llm_queue.engine.settings import InfraSettings, read_document
from llm_queue.errors import ConfigError, MissingExtra
from llm_queue.registry import check, load_registry
from llm_queue.registry.resolve import budget_from_quota

FIXTURES = Path(__file__).parent / "fixtures"
NO_ENV: dict[str, str] = {}
DEV = {"LLMQ_STORE_URL": "redis://localhost:6379", "LLMQ_ENVIRONMENT": "dev"}


def _small() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads((FIXTURES / "smallest.json").read_text(encoding="utf-8"))
    return loaded


def _expand(document: str, **env: str) -> Any:
    return json.loads(read_document(document, None, env.get))


# H3: placeholders -------------------------------------------------------------------------


def test_whole_string_placeholders_become_typed_values() -> None:
    doc = json.dumps(
        {
            "list": '${REGIONS:-["ap-south-1"]}',
            "object": "${OBJ}",
            "int": "${RPM}",
            "float": "${PCT}",
            "bool": "${FLAG}",
            "quoted": "${ACCOUNT}",
            "plain": "${NAME}",
        }
    )
    env = {
        "OBJ": '{"a": [1, 2]}',
        "RPM": "120",
        "PCT": "0.9",
        "FLAG": "true",
        "ACCOUNT": '"012345678901"',  # a quoted JSON string stays text
        "NAME": "gpt-oss",
    }
    assert _expand(doc, **env) == {
        "list": ["ap-south-1"],
        "object": {"a": [1, 2]},
        "int": 120,
        "float": 0.9,
        "bool": True,
        "quoted": "012345678901",
        "plain": "gpt-oss",
    }


def test_placeholders_inside_strings_are_text_and_cannot_break_quoting() -> None:
    doc = json.dumps({"id": "arn:${PART}:${REGION:-ap-south-1}/x", "n": "v${N}"})
    tricky = 'a"b\\c\n}'
    assert _expand(doc, PART=tricky, N="5") == {
        "id": f"arn:{tricky}:ap-south-1/x",
        "n": "v5",
    }


def test_default_applies_when_unset_or_empty_and_holds_braces() -> None:
    doc = json.dumps({"a": '${X:-{"k": {"j": 1}}}', "b": "${Y:-fallback}", "c": "${Z}"})
    assert _expand(doc, Y="", Z="") == {"a": {"k": {"j": 1}}, "b": "fallback", "c": ""}


def test_unset_without_default_names_the_variable_and_path() -> None:
    doc = json.dumps({"models": {"m": {"deployments": {"aws": {"confirmed": ["${R1}", "${R2}"]}}}}})
    with pytest.raises(ConfigError) as caught:
        read_document(doc, None, NO_ENV.get)
    text = str(caught.value)
    assert "`R1` is not set (at models.m.deployments.aws.confirmed[0])" in text
    assert "`R2` is not set (at models.m.deployments.aws.confirmed[1])" in text


def test_escape_and_lone_dollar_are_literal() -> None:
    doc = json.dumps({"a": "$${NOT_A_VAR}", "b": "cost $5", "c": "$$x"})
    assert _expand(doc) == {"a": "${NOT_A_VAR}", "b": "cost $5", "c": "$$x"}


@pytest.mark.parametrize(
    ("value", "reason"),
    [("${}", "variable name"), ("${1X}", "variable name"), ("${OPEN", "not closed")],
)
def test_malformed_placeholder_is_refused(value: str, reason: str) -> None:
    with pytest.raises(ConfigError, match=reason):
        read_document(json.dumps({"a": value}), None, NO_ENV.get)


def test_repeated_keys_are_refused_before_and_after_expansion() -> None:
    with pytest.raises(ConfigError, match="repeats keys"):
        read_document('{"rpm": "${A:-1}", "rpm": 2}', None, NO_ENV.get)
    with pytest.raises(ConfigError, match="repeats keys"):
        read_document('{"x": "${OBJ}"}', None, {"OBJ": '{"a": 1, "a": 2}'}.get)


def test_bad_json_value_is_refused_without_showing_it() -> None:
    with pytest.raises(ConfigError, match="`SECRET` \\(at x\\) reads as JSON") as caught:
        read_document('{"x": "${SECRET}"}', None, {"SECRET": "[hunter2"}.get)
    assert "hunter2" not in str(caught.value)


def test_keys_are_not_expanded() -> None:
    with pytest.raises(ConfigError, match="values only"):
        read_document('{"${K}": 1}', None, {"K": "a"}.get)


def test_text_without_placeholders_is_returned_as_written() -> None:
    text = '{"registry_version": 1,  "a": "$x"}'
    assert read_document(text, None, NO_ENV.get) == text


def test_expanded_values_are_not_expanded_again() -> None:
    assert _expand('{"a": "${A}"}', A="${B}", B="no") == {"a": "${B}"}


def test_from_env_reads_placeholders_from_secrets_then_environment() -> None:
    doc = _small()
    doc["models"]["gpt20b"]["model_id"] = "${CHAT_ID}"
    doc["models"]["gpt20b"]["rpm"] = "${CHAT_RPM:-10}"
    env = {**DEV, "LLMQ_REGISTRY": json.dumps(doc), "CHAT_ID": "from-env"}
    settings = InfraSettings.from_env(env, secrets={"CHAT_ID": "from-secret"}.get)
    loaded = json.loads(settings.registry_document())
    assert loaded["models"]["gpt20b"]["model_id"] == "from-secret"
    assert loaded["models"]["gpt20b"]["rpm"] == 10
    assert "from-secret" not in repr(settings)
    with pytest.raises(ConfigError, match="`CHAT_ID` is not set"):
        InfraSettings.from_env({**DEV, "LLMQ_REGISTRY": json.dumps(doc)})


# H3: YAML files ---------------------------------------------------------------------------

YAML_DOC = """\
# comments are why YAML is allowed
registry_version: 1
accounts:
  aws-main: {kind: aws}
models:
  gpt20b:
    type: chat
    provider: bedrock
    model_id: openai.gpt-oss-20b-1:0
    rpm: ${CHAT_RPM:-50}
    deployments:
      aws-main:
        confirmed: ${REGIONS:-["ap-south-1"]}
"""


@pytest.fixture(params=["libyaml", "python"])
def yaml_loader(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> str:
    """Each YAML test runs with the libyaml loader (when built) and the pure-Python one (FB-3)."""
    yaml = pytest.importorskip("yaml")
    if request.param == "libyaml" and not hasattr(yaml, "CSafeLoader"):
        pytest.skip("PyYAML built without libyaml")
    if request.param == "python":
        monkeypatch.delattr(yaml, "CSafeLoader", raising=False)
    return str(request.param)


def test_yaml_file_is_read_and_expanded(tmp_path: Path, yaml_loader: str) -> None:
    pytest.importorskip("yaml")
    path = tmp_path / "registry.yaml"
    path.write_text(YAML_DOC, encoding="utf-8")
    loaded = json.loads(read_document(None, str(path), {"CHAT_RPM": "70"}.get))
    model = loaded["models"]["gpt20b"]
    assert model["rpm"] == 70
    assert model["deployments"]["aws-main"]["confirmed"] == ["ap-south-1"]
    load_registry(loaded)


def test_how_to_yaml_example_loads(tmp_path: Path, yaml_loader: str) -> None:
    """The how-to's placeholder example expands and loads, with defaults and with values."""
    pytest.importorskip("yaml")
    page = (FIXTURES.parents[3] / "docs" / "how-to" / "configure.md").read_text(encoding="utf-8")
    found = re.search(r"```yaml\n(# registry\.yaml.*?)```", page, re.DOTALL)
    assert found, "docs/how-to/configure.md lost its registry.yaml example"
    path = tmp_path / "registry.yaml"
    path.write_text(found.group(1), encoding="utf-8")
    view = load_registry(json.loads(read_document(None, str(path), NO_ENV.get)))
    assert view.value("gpt120b", "rpm") == 450
    assert view.registry.residency == ("ap-south-1",)
    env = {"EMBED_REGIONS": '["ap-south-1", "ap-south-2"]', "REGISTRY_VERSION": "7"}
    view = load_registry(json.loads(read_document(None, str(path), env.get)))
    assert view.registry.version == 7
    assert view.registry.residency == ("ap-south-1", "ap-south-2")


def test_yaml_repeated_key_is_refused(tmp_path: Path, yaml_loader: str) -> None:
    pytest.importorskip("yaml")
    path = tmp_path / "registry.yml"
    path.write_text("registry_version: 1\nmodels: {}\nregistry_version: 2\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="`registry_version` is repeated"):
        read_document(None, str(path), NO_ENV.get)


def test_yaml_non_json_values_are_refused(tmp_path: Path, yaml_loader: str) -> None:
    pytest.importorskip("yaml")
    path = tmp_path / "registry.yaml"
    path.write_text("a: .nan\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="nan"):
        read_document(None, str(path), NO_ENV.get)
    path.write_text("1: x\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="keys must be text"):
        read_document(None, str(path), NO_ENV.get)


def test_yaml_without_the_extra_names_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__

    def no_yaml(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "yaml":
            raise ImportError("no yaml")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_yaml)
    path = tmp_path / "registry.yaml"
    path.write_text(YAML_DOC, encoding="utf-8")
    with pytest.raises(MissingExtra, match=r"llm-queue\[yaml\]"):
        read_document(None, str(path), NO_ENV.get)


# H4: residency "auto" ---------------------------------------------------------------------


def _two_models() -> dict[str, Any]:
    doc = _small()
    embed = {
        "type": "embedding",
        "provider": "bedrock",
        "model_id": "amazon.titan-embed-text-v2:0",
        "deployments": {
            "aws-main": {"confirmed": ["us-east-1"], "fallback": ["eu-west-1"]},
        },
    }
    doc["models"]["titan"] = embed
    return doc


def test_auto_residency_is_every_region_in_use() -> None:
    doc = _two_models()
    doc["residency"] = {"allowed_regions": "auto"}
    view = load_registry(doc)
    assert view.registry.residency == ("ap-south-1", "eu-west-1", "us-east-1")
    assert not any(view.outside_residency(d) for d in view.deployments("titan"))


def test_auto_residency_on_a_model_and_the_list_form_still_work() -> None:
    doc = _two_models()
    doc["models"]["titan"]["residency"] = {"allowed_regions": "auto"}
    doc["residency"] = {"allowed_regions": ["ap-south-1"]}
    view = load_registry(doc)
    assert view.residency("titan") == ("ap-south-1", "eu-west-1", "us-east-1")
    assert view.residency("gpt20b") == ("ap-south-1",)


def test_residency_other_words_are_refused() -> None:
    doc = _small()
    doc["residency"] = {"allowed_regions": "all"}
    _, errors, _ = check(doc)
    assert any('must be a list of strings or "auto"' in e for e in errors)


# H5: quotas -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("quota", "share", "budget"), [(1000, 0.9, 900), (100, 0.29, 29), (7, 0.5, 3), (5, 1.0, 5)]
)
def test_budget_is_the_floor_of_quota_times_share(quota: int, share: float, budget: int) -> None:
    assert budget_from_quota(quota, share) == budget


def test_quota_and_share_give_the_budget_at_each_level() -> None:
    doc = _small()
    model = doc["models"]["gpt20b"]
    model.update(quota_rpm=1000, quota_tpm=200_000, budget_pct=0.9)
    model["deployments"]["aws-main"]["overrides"] = {"ap-south-1": {"rpm": 50}}
    view = load_registry(doc)
    dep = "aws-main/ap-south-1"
    assert view.value("gpt20b", "rpm") == 900
    assert view.value("gpt20b", "tpm") == 180_000
    assert view.effective("gpt20b", "tpm", dep).level == "model"
    assert view.quota("gpt20b", "tpm", dep) == 200_000
    # A more specific plain `rpm` wins over the model's quota.
    assert view.value("gpt20b", "rpm", dep) == 50
    assert view.quota("gpt20b", "rpm", dep) is None


def test_account_share_applies_to_a_model_quota() -> None:
    doc = _small()
    doc["accounts"]["aws-main"]["settings"] = {"budget_pct": 0.5}
    doc["models"]["gpt20b"]["quota_rpm"] = 100
    view = load_registry(doc)
    assert view.value("gpt20b", "rpm", "aws-main/ap-south-1") == 50
    assert view.value("gpt20b", "rpm") == 100  # without a deployment, no account level


def test_both_forms_on_one_level_are_refused() -> None:
    doc = _small()
    doc["models"]["gpt20b"].update(rpm=10, quota_rpm=100)
    _, errors, _ = check(doc)
    assert any("set `rpm` or `quota_rpm`, not both" in e for e in errors)
    doc = _small()
    doc["models"]["gpt20b"]["deployments"]["aws-main"]["overrides"] = {
        "ap-south-1": {"tpm": 10_000, "quota_tpm": 20_000}
    }
    _, errors, _ = check(doc)
    assert any("set `tpm` or `quota_tpm`, not both" in e for e in errors)


def test_both_forms_on_different_levels_are_fine() -> None:
    doc = _small()
    doc["defaults"] = {"rpm": 10}
    doc["models"]["gpt20b"]["quota_rpm"] = 100
    assert load_registry(doc).value("gpt20b", "rpm") == 100


@pytest.mark.parametrize("share", [0, 1.01, -0.5])
def test_share_out_of_range_is_refused(share: float) -> None:
    doc = _small()
    doc["models"]["gpt20b"].update(quota_rpm=100, budget_pct=share)
    _, errors, _ = check(doc)
    assert any("budget_pct" in e for e in errors)


def test_quota_that_rounds_to_zero_is_refused() -> None:
    doc = _small()
    doc["models"]["gpt20b"].update(quota_rpm=1, budget_pct=0.5)
    _, errors, _ = check(doc)
    assert any("`quota_rpm` x `budget_pct` rounds down to 0" in e for e in errors)


# D4: the breaker probe default ------------------------------------------------------------


def test_breaker_probe_default_follows_a_longer_attempt_timeout() -> None:
    doc = _small()
    doc["defaults"] = {"attempt_timeout_secs": 90, "timeout_secs": 300}
    view = load_registry(doc)
    probe = view.effective("gpt20b", "breaker_probe_secs", "aws-main/ap-south-1")
    assert (probe.value, probe.level) == (90.0, "library")
    assert load_registry(_small()).value("gpt20b", "breaker_probe_secs") == 60.0


def test_explicit_breaker_probe_below_the_attempt_timeout_is_still_refused() -> None:
    doc = copy.deepcopy(_small())
    doc["defaults"] = {"attempt_timeout_secs": 90, "timeout_secs": 300, "breaker_probe_secs": 60}
    _, errors, _ = check(doc)
    assert any("breaker_probe_secs" in e and "at least" in e for e in errors)
