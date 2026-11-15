"""0.1.0b2 registry checks (RESCAN R4, R8-R10, X3, X4, X6, C5, O3 bounds, new settings).

Each case is a document the 0.1.0b1 validator accepted (or read wrongly) and must now refuse,
or a value it must now resolve.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from llm_queue.registry import RegistryError, check, load_registry
from llm_queue.registry.schema import host_address, needs_newer_lib, version_key
from llm_queue.registry.validate import parse_document, tpm_refuses

FIXTURES = Path(__file__).parent / "registry" / "fixtures"


@pytest.fixture
def small() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((FIXTURES / "smallest.json").read_text(encoding="utf-8"))
    return data


def _chat(alias: str, **extra: Any) -> dict[str, Any]:
    return {
        "type": "chat",
        "provider": "bedrock",
        "model_id": f"vendor.{alias}",
        "deployments": {"aws-main": {"confirmed": ["ap-south-1"]}},
        **extra,
    }


def _errors(doc: dict[str, Any] | str, **kw: Any) -> list[str]:
    registry, errors, _ = check(doc, **kw)
    assert registry is None, "expected the document to be refused"
    return errors


def _warnings(doc: dict[str, Any]) -> list[str]:
    registry, errors, warnings = check(doc)
    assert registry is not None, errors
    return warnings


def _has(lines: list[str], *parts: str) -> bool:
    return any(all(p in line for p in parts) for line in lines)


# ---------------------------------------------------------------- new settings


def test_converse_api_and_message_cap_settings(small: dict[str, Any]) -> None:
    view = load_registry(small)
    assert view.value("gpt20b", "converse_api") == "converse"
    assert view.value("gpt20b", "max_message_bytes") == 4 * 1024 * 1024
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"].update(converse_api="stream", max_message_bytes=1_000_000)
    view = load_registry(doc)
    assert view.value("gpt20b", "converse_api") == "stream"
    assert view.value("gpt20b", "max_message_bytes") == 1_000_000
    doc["models"]["gpt20b"].update(converse_api="invoke", max_message_bytes=10)
    errors = _errors(doc)
    assert _has(errors, "converse_api")
    assert _has(errors, "max_message_bytes")


@pytest.mark.parametrize("name", ["extra_headers", "extra_body", "timeout", "extra_query", "store"])
def test_inference_extra_refuses_sdk_arguments(small: dict[str, Any], name: str) -> None:
    """X4: `inference.extra` was any object, so `store: true` or `extra_headers` went through."""
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["inference"] = {"extra": {name: True}}
    assert _has(_errors(doc), "inference", name)
    doc["models"]["gpt20b"]["inference"] = {"extra": {"reasoning_effort": "low"}}
    assert check(doc)[0] is not None


def test_alert_thresholds_are_bounded_and_dead_names_gone(small: dict[str, Any]) -> None:
    """O3: thresholds took any number (a 500 % quota alert never fired), four did nothing."""
    doc = copy.deepcopy(small)
    for alerts in ({"quota_used_pct": 150}, {"dlq_messages": 0}):
        doc["defaults"] = {"alerts": alerts}
        assert _has(_errors(doc), "alerts")


@pytest.mark.parametrize(
    "name", ["quota_used_mins", "spill_pct", "spill_window_secs", "waiting_age_secs"]
)
def test_removed_alert_names_warn_until_0_2(small: dict[str, Any], name: str) -> None:
    """Q34 / R3AP-5: a 0.1.0b1 document naming a removed alert stopped the pod from starting."""
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["alerts"] = {name: 5, "dlq_messages": 2}
    warnings = _warnings(doc)
    assert _has(warnings, f"alerts.{name}", "removed in 0.1.0b2")
    view = load_registry(doc)
    assert name not in view.value("gpt20b", "alerts")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_non_finite_numbers_are_refused(small: dict[str, Any], value: float) -> None:
    """R3CF-1: NaN and Infinity passed every range check (every NaN comparison is false)."""
    for name in ("rate_burst", "priority_reserve", "timeout_max_secs", "tpm_output_weight"):
        doc = copy.deepcopy(small)
        doc["models"]["gpt20b"][name] = value
        assert _has(_errors(doc), f"models.gpt20b.{name}", "finite")


def test_nan_in_json_text_is_refused(small: dict[str, Any]) -> None:
    """R3CF-1: `json.loads` accepts NaN; orjson later refused it with a raw JSONDecodeError."""
    text = json.dumps(small)[:-1] + ', "defaults": {"rate_burst": NaN}}'
    assert _has(_errors(text), "NaN")


def test_integers_have_an_upper_bound(small: dict[str, Any]) -> None:
    """R3CF-2 / Q42: `rpm: 2**64` validated, then orjson raised a raw TypeError at boot."""
    for name, value in (("rpm", 2**64), ("max_concurrency", 10**12), ("max_attempts", 10**9)):
        doc = copy.deepcopy(small)
        doc["models"]["gpt20b"][name] = value
        assert _has(_errors(doc), f"models.gpt20b.{name}", "at most")
    doc = copy.deepcopy(small)
    doc["registry_version"] = 2**70
    assert _has(_errors(doc), "registry_version")
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["timeout_secs"] = 10**8  # Q42: a timeout is at most one day
    assert _has(_errors(doc), "timeout_secs", "at most 86400")


def test_retry_multiplier_is_capped(small: dict[str, Any]) -> None:
    """R3CF-3: `retry_multiplier: 1e300` overflowed the backoff on the second retry."""
    doc = copy.deepcopy(small)
    doc["defaults"] = {"retry_multiplier": 1e300}
    assert _has(_errors(doc), "retry_multiplier", "at most 100")


def test_names_refuse_a_trailing_newline(small: dict[str, Any]) -> None:
    """R3CF-4: `$` matched before a trailing newline, so `"acc\\n"` became a store key."""
    doc = copy.deepcopy(small)
    doc["models"]["a\n"] = doc["models"].pop("gpt20b")
    assert _has(_errors(doc), "alias")
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["admission"] = "gate\n"
    assert _has(_errors(doc), "admission", "hook name")


def test_queue_names_are_checked(small: dict[str, Any]) -> None:
    """R3CF-5: `queue.name` took spaces, `{}`, newlines and a job queue's `jobs-` name."""
    for name in ("my queue {x}:é\n", "jobs-x", "q" * 72):
        doc = copy.deepcopy(small)
        doc["models"]["gpt20b"]["queue"] = {"name": name}
        assert _has(_errors(doc), "models.gpt20b.queue")
    doc = copy.deepcopy(small)
    doc["models"]["a" * 65] = doc["models"].pop("gpt20b")
    assert _has(_errors(doc), "alias")


def test_unknown_sub_fields_are_lenient_in_a_stored_document(small: dict[str, Any]) -> None:
    """R3CF-8: a newer pod's sub-field made every older pod refuse the stored document."""
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["health"] = {"new_field": 1, "disable_errors": 4}
    assert _has(_errors(doc), "new_field")  # the process's own config stays strict
    registry, errors, warnings = check(doc, strict=False)
    assert registry is not None, errors
    assert _has(warnings, "health.new_field", "ignored")


def test_padded_ids_and_bad_role_arns_are_refused(small: dict[str, Any]) -> None:
    """R3CF-16: a padded model id or a malformed role ARN failed only at the first call."""
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["model_id"] = " anthropic.a \n"
    assert _has(_errors(doc), "model_id", "spaces")
    doc = copy.deepcopy(small)
    doc["accounts"]["aws-main"]["role_arn"] = "not an arn"
    assert _has(_errors(doc), "role_arn", "IAM role ARN")
    doc["accounts"]["aws-main"]["role_arn"] = "arn:aws:iam::123456789012:role/llm-queue"
    assert check(doc)[0] is not None


def test_retention_defaults_to_one_day(small: dict[str, Any]) -> None:
    """Q27: a queue kept prompts four days by default; the default is now one day."""
    assert load_registry(small).value("gpt20b", "queue")["retention_secs"] == 86_400


def test_queue_retention_is_capped(small: dict[str, Any]) -> None:
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["queue"] = {"dlq_retention_secs": 10**9}
    assert _has(_errors(doc), "dlq_retention_secs", "at most")


# ---------------------------------------------------------------- versions, live edit mode


def test_version_order() -> None:
    order = ["0.1.0.dev1", "0.1.0a1", "0.1.0b1", "0.1.0b2", "0.1.0rc1", "0.1.0", "0.1.0.post1"]
    keys = [version_key(v) for v in order]
    assert all(k is not None for k in keys)
    assert keys == sorted(keys)  # type: ignore[type-var]
    assert version_key("1.0") == version_key("1.0.0")
    assert version_key("next") is None
    assert needs_newer_lib("99.0") is True
    assert needs_newer_lib("0.0.1") is False
    assert needs_newer_lib(None) is False


def test_min_lib_version_refuses_own_config_reads_stored(small: dict[str, Any]) -> None:
    """C5: the process's own config needing a newer library cannot start; a stored one reads."""
    doc = copy.deepcopy(small)
    doc["min_lib_version"] = "99.0"
    assert _has(_errors(doc), "min_lib_version", "99.0")
    registry, _, _ = check(doc, strict=False)
    assert registry is not None
    assert registry.min_lib_version == "99.0"
    doc["min_lib_version"] = "soon"
    assert _has(_errors(doc, strict=False), "min_lib_version")


def test_live_edit_mode_is_a_document_field(small: dict[str, Any]) -> None:
    """R6: the live-edit mode came from each process; now the document carries it."""
    doc = copy.deepcopy(small)
    doc["live_edit"] = "off"
    registry, _, _ = check(doc)
    assert registry is not None
    assert registry.live_edit == "off"
    doc["live_edit"] = "sometimes"
    assert _has(_errors(doc), "live_edit")


# ---------------------------------------------------------------- accounts (X6)


def test_secret_refs_need_the_account_prefix(small: dict[str, Any]) -> None:
    doc = copy.deepcopy(small)
    doc["accounts"]["v"] = {
        "kind": "openai",
        "base_url": "https://llm.example.com/v1",
        "api_key_ref": "DATABASE_PASSWORD",
    }
    assert _has(_errors(doc), "api_key_ref", "LLMQ_ACCOUNT_")
    doc["accounts"]["v"]["api_key_ref"] = "LLMQ_ACCOUNT_V_KEY"
    assert check(doc)[0] is not None


@pytest.mark.parametrize(
    ("url", "fragment"),
    [
        ("http://0xa9fea9fe/v1", "link-local"),  # 169.254.169.254, hex
        ("http://2852039166/v1", "link-local"),  # 169.254.169.254, integer
        ("http://[::ffff:169.254.169.254]/v1", "link-local"),  # IPv4-mapped
        ("https://[fd00:ec2::254]/v1", "metadata"),  # AWS metadata over IPv6
        ("http://[fd12::1]/v1", "https"),  # a ULA is not local (no "." used to mean local)
        ("http://vllm/v1", "https"),  # a single-label name is not local
        ("http://x.localhost/v1", "https"),
        ("https://user:pw@llm.example.com/v1", "user or password"),
        ("https://llm.example.com/v1?api_key=abc", "query"),
    ],
)
def test_base_url_forms(small: dict[str, Any], url: str, fragment: str) -> None:
    doc = copy.deepcopy(small)
    doc["accounts"]["v"] = {"kind": "openai", "base_url": url}
    assert _has(_errors(doc), "base_url", fragment)


def test_local_and_allowed_hosts_are_normalised(small: dict[str, Any]) -> None:
    doc = copy.deepcopy(small)
    doc["accounts"]["v"] = {"kind": "openai", "base_url": "http://0x7f.1:8080/v1"}
    assert check(doc)[0] is not None  # 127.0.0.1 written another way
    doc["accounts"]["v"]["base_url"] = "https://LLM.Example.com./v1"
    assert check(doc, allowed_hosts=("llm.example.com",))[0] is not None
    assert str(host_address("017700000001")) == "127.0.0.1"
    assert host_address("dc1") is None


# ---------------------------------------------------------------- budgets, fallback, residency


def test_tpm_below_one_reservation_is_refused(small: dict[str, Any]) -> None:
    """R4: a TPM below one call's output reservation validated and then admitted nothing."""
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"].update(tpm=1500, inference={"max_tokens": 1000}, tpm_output_weight=2)
    assert _has(_errors(doc), "tpm", "reservation")
    doc["models"]["gpt20b"]["tpm"] = 2000
    view = load_registry(doc)
    keys = [d.key for d in view.deployments("gpt20b")]
    assert tpm_refuses(view, "gpt20b", keys, 2500) is True
    assert tpm_refuses(view, "gpt20b", keys, 2000) is False
    doc["models"]["gpt20b"]["rate_limits"] = "rpm"
    doc["models"]["gpt20b"]["rpm"] = 10
    assert tpm_refuses(load_registry(doc), "gpt20b", keys, 10**6) is False


def test_fallback_cycle_of_three_is_refused(small: dict[str, Any]) -> None:
    """R8: only pairs were checked; a -> b -> c -> a passed a call around for ever."""
    doc = copy.deepcopy(small)
    rule = {"on": ["exhausted"]}
    doc["models"] = {
        "a": _chat("a", model_fallback={**rule, "models": ["b"]}),
        "b": _chat("b", model_fallback={**rule, "models": ["c"]}),
        "c": _chat("c", model_fallback={**rule, "models": ["a"]}),
    }
    assert _has(_errors(doc), "cycle", "a -> b -> c -> a")
    for alias, target in (("a", "b"), ("b", "c"), ("c", "a")):
        doc["models"][alias]["rpm"] = 10
        doc["models"][alias]["model_fallback"]["models"] = [
            {"model": target, "require_headroom": True}
        ]
    assert check(doc)[0] is not None


def test_disabled_fallback_target_warns(small: dict[str, Any]) -> None:
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["model_fallback"] = {"on": ["exhausted"], "models": ["other"]}
    doc["models"]["other"] = _chat("other", enabled=False)
    assert _has(_warnings(doc), "other", "disabled")


def test_fallback_target_must_stay_inside_residency(small: dict[str, Any]) -> None:
    """X3: a model kept in ap-south-1 could fall back to a model served from us-east-1."""
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["residency"] = {"allowed_regions": ["ap-south-1"]}
    doc["models"]["gpt20b"]["model_fallback"] = {"on": ["exhausted"], "models": ["far"]}
    doc["models"]["far"] = _chat("far", deployments={"aws-main": {"confirmed": ["us-east-1"]}})
    assert _has(_errors(doc), "model_fallback", "aws-main/us-east-1", "residency")


def test_residency_leaving_no_deployment_is_an_error(small: dict[str, Any]) -> None:
    """R9: a residency list that excluded every deployment was only a warning."""
    doc = copy.deepcopy(small)
    doc["residency"] = {"allowed_regions": ["eu-west-1"]}
    assert _has(_errors(doc), "residency", "no confirmed deployment")


def test_probe_window_covers_an_attempt(small: dict[str, Any]) -> None:
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"].update(breaker_probe_secs=10, attempt_timeout_secs=30)
    assert _has(_errors(doc), "breaker_probe_secs")


def test_blocked_region_typo_warns(small: dict[str, Any]) -> None:
    doc = copy.deepcopy(small)
    doc["routing"] = {"blocked_regions": ["ap-south-l"]}
    assert _has(_warnings(doc), "blocked_regions", "ap-south-l")


def test_repeated_keys_are_refused(small: dict[str, Any]) -> None:
    """R10: `"rpm": 10, ..., "rpm": 1000` kept the last value silently."""
    text = json.dumps(small)[:-1] + ', "registry_version": 2}'
    assert _has(_errors(text), "repeats", "registry_version")
    with pytest.raises(RegistryError, match="not valid JSON"):
        parse_document("{nope")
    assert parse_document(json.dumps(small)) == small


def test_r3cf10_timeouts_are_checked_against_each_other(small: dict[str, Any]) -> None:
    """R3CF-10: `after_secs` beyond the deadline, retention below the timeout: all silent."""
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["fallback"] = {"on": ["exhausted"], "after_secs": 9999}
    assert _has(_errors(doc), "fallback.after_secs", "timeout_secs", "never fire")
    doc = copy.deepcopy(small)
    model = doc["models"]["gpt20b"]
    model.update(timeout_secs=600, timeout_max_secs=900, queue={"retention_secs": 60})
    model.update(retry_base_secs=100, retry_max_secs=1)
    model["health"] = {"probe_after_secs": 9000, "probe_max_secs": 10}
    warnings = _warnings(doc)
    assert _has(warnings, "queue.retention_secs", "below")
    assert _has(warnings, "retry_base_secs", "retry_max_secs")
    assert _has(warnings, "probe_after_secs", "probe_max_secs")


def test_r3cf11_no_usable_deployment_warns(small: dict[str, Any]) -> None:
    """R3CF-11: a model whose only account is disabled validated with no word."""
    doc = copy.deepcopy(small)
    doc["accounts"]["aws-main"]["enabled"] = False
    assert _has(_warnings(doc), "models.gpt20b", "no usable confirmed deployment")


def test_r3cf12_weighted_priority_needs_a_weight(small: dict[str, Any]) -> None:
    """R3CF-12: all weights 0 was refused for `balance` only, not `balance_priority`."""
    doc = copy.deepcopy(small)
    model = doc["models"]["gpt20b"]
    model["balance_priority"] = "weighted"
    model["deployments"]["aws-main"]["overrides"] = {"ap-south-1": {"weight": 0}}
    assert _has(_errors(doc), "balance_priority", "weight 0")


def test_r3cf16_misleading_values_warn(small: dict[str, Any]) -> None:
    """R3CF-16: duplicates, a rule with no target and settings the model never reads."""
    doc = copy.deepcopy(small)
    model = doc["models"]["gpt20b"]
    model["fallback"] = {"on": ["exhausted", "exhausted"]}
    model["inference_limits"] = {"locked": ["dimensions"]}
    warnings = _warnings(doc)
    assert _has(warnings, "fallback.on", "twice")
    assert _has(warnings, "fallback", "no fallback regions")
    assert _has(warnings, "locked", "dimensions")


def test_r3cf17_messages_name_values_levels_and_paths(small: dict[str, Any]) -> None:
    """R3CF-17: messages gave no values or levels and a path `settings set` cannot take."""
    doc = copy.deepcopy(small)
    doc["defaults"] = {"timeout_secs": 1000}
    assert _has(_errors(doc), "`timeout_secs` 1000 (defaults)", "`timeout_max_secs` 900")
    doc = copy.deepcopy(small)
    doc["models"]["gpt20b"]["attempt_timeout_secs"] = 500
    assert _has(_errors(doc), "models.gpt20b.deployments.aws-main.overrides.ap-south-1")
    text = '{"registry_version": 1, "defaults": {"rpm": 1, "tpm": 5, "rpm": 2}}'
    assert _has(_errors(text), "`rpm` (in the object with tpm)")


def test_r3cf6_cross_provider_fallback_loads_with_a_warning() -> None:
    """R3CF-6a: the docs said a fallback to another provider was refused; it is allowed (Q40).

    Calls are converted between the shapes, streams do not fall back: the validator says so.
    """
    doc = json.loads((FIXTURES / "full.json").read_text(encoding="utf-8"))
    warnings = _warnings(doc)
    assert _has(warnings, "gpt120b.model_fallback", "`gpt120b-vllm` is an openai model")
    assert _has(warnings, "gpt120b.model_fallback", "streams do not fall back")
