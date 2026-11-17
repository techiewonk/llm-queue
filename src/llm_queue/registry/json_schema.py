"""The registry document's JSON Schema, generated from the same checks that validate it.

`registry_schema()` walks `registry.settings.SETTINGS` and the field lists of `registry.schema`,
so the schema cannot drift from validation. Editors use it to complete and check a document
(`"$schema"` is not a registry field: point the editor at the file instead). It covers the
shape, types and ranges; cross-field rules (budgets, fallback cycles, residency) are only in
`registry.validate`. Printed by `llm-queue registry schema`; checked in as
`docs/reference/registry-schema.json` (a unit test keeps it current).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from llm_queue.providers.embeddings import embedding_families
from llm_queue.registry import schema as doc
from llm_queue.registry import settings as st

SCHEMA_ID = "https://techiewonk.github.io/llm-queue/reference/registry-schema.json"


def _pattern(regex: str) -> str:
    """A Python pattern as JSON Schema (ECMA 262) reads it: Python's end anchor becomes `$`."""
    return regex.replace(r"\Z", "$")


_STRING = {"type": "string", "minLength": 1}
_STRINGS = {"type": "array", "items": _STRING}
_CUSTOM = {"type": "string", "pattern": _pattern(st._CUSTOM_RE.pattern)}
_NAME = {"type": "string", "pattern": _pattern(doc.NAME_RE.pattern)}
# The named checks (module functions): their schema by identity.
_NAMED: Mapping[Any, Mapping[str, Any]] = {
    st.boolean: {"type": "boolean"},
    st.text: _STRING,
    st.text_list: _STRINGS,
    st.any_object: {"type": "object"},
    st.float_embeddings: {"const": ["float"]},
    st.strategy: {
        "anyOf": [
            {"enum": list(st.STRATEGIES)},
            _CUSTOM,
        ]
    },
    st.hook_name: {"type": "string", "pattern": _pattern(st._HOOK_RE.pattern)},
    st.queue_name: {
        "type": "string",
        "pattern": _pattern(st.QUEUE_NAME_RE.pattern),
        "not": {"pattern": f"^{st.JOB_QUEUE_PREFIX}"},
    },
    st._tags: {"type": "object", "additionalProperties": {"type": ["string", "number"]}},
    st._causes: {
        "type": "array",
        "minItems": 1,
        "items": {"enum": ["exhausted", "broken", "error"]},
    },
    st._fallback_models: {
        "type": "array",
        "minItems": 1,
        "items": {
            "anyOf": [
                _STRING,
                {
                    "type": "object",
                    "properties": {"model": _STRING, "require_headroom": {"type": "boolean"}},
                    "required": ["model"],
                    "additionalProperties": False,
                },
            ]
        },
    },
    st._sticky_key: {"anyOf": [{"const": "session"}, _CUSTOM]},
    st._stop: {"anyOf": [{"type": "string"}, _STRINGS]},
    st._extra: {
        "type": "object",
        "propertyNames": {
            "pattern": _pattern(st._EXTRA_NAME_RE.pattern),
            "not": {"enum": sorted(st.EXTRA_REFUSED)},
        },
    },
    st._locked: {"type": "array", "items": {"enum": sorted(st.INFERENCE_NAMES)}},
}


def check_schema(check: st.Check) -> dict[str, Any]:
    """The JSON Schema of one setting check; `TypeError` for a check it does not know."""
    if isinstance(check, st.Number):
        out: dict[str, Any] = {"type": "integer" if check.integer else "number"}
        out["exclusiveMinimum" if check.lo_open else "minimum"] = check.lo
        out["maximum"] = check.hi
        return out
    if isinstance(check, st.OneOf):
        return {"enum": list(check.values)}
    if isinstance(check, st.TenantMap):
        return {
            "type": "object",
            "propertyNames": {"minLength": 1, "maxLength": st.MAX_TENANT_ID_CHARS},
            "additionalProperties": check_schema(check.item),
        }
    if isinstance(check, st.ObjCheck):
        properties = {name: check_schema(sub) for name, sub in check.fields.items()}
        out = {"type": "object", "properties": properties, "additionalProperties": False}
        if check.required:
            out["required"] = list(check.required)
        return out
    named = _NAMED.get(check)
    if named is None:
        raise TypeError(f"no JSON Schema for the setting check {check!r}")
    return dict(named)


def _setting(name: str) -> dict[str, Any]:
    """One setting's schema, with its library default and the levels it may be set at."""
    spec = st.SETTINGS[name]
    out = check_schema(spec.check)
    levels = ", ".join(st.LEVEL_NAMES[level] for level in "DAMR" if level in spec.levels)
    out["description"] = f"Levels: {levels}." + (" Deploy only." if spec.deploy_only else "")
    if spec.default is not None and spec.by_type is None:
        out["default"] = spec.default
    return out


def _settings_at(level: str) -> dict[str, Any]:
    return {name: _setting(name) for name in sorted(st.settings_at(level))}


def _object(properties: Mapping[str, Any], required: tuple[str, ...] = ()) -> dict[str, Any]:
    out: dict[str, Any] = {
        "type": "object",
        "properties": dict(properties),
        "additionalProperties": False,
    }
    if required:
        out["required"] = list(required)
    return out


def registry_schema() -> dict[str, Any]:
    """The JSON Schema (draft 2020-12) of the registry document (`LLMQ_REGISTRY`)."""
    secret_ref = {"type": "string", "pattern": _pattern(doc._SECRET_REF_RE.pattern)}
    deployment_settings = _settings_at(st.R)
    profile = _object(
        {
            "name": _NAME,
            "region": _NAME,
            "profile_id": _STRING,
            "routes_to": {"type": "array", "items": _NAME},
            **deployment_settings,
        },
        ("name", "region", "profile_id"),
    )
    entries = {"type": "array", "items": {"anyOf": [_NAME, {"$ref": "#/$defs/profile"}]}}
    deployments = _object(
        {
            "confirmed": entries,
            "fallback": entries,
            "overrides": {
                "type": "object",
                "propertyNames": _NAME,
                "additionalProperties": _object(deployment_settings),
            },
        }
    )
    residency = _object({"allowed_regions": {"anyOf": [_STRINGS, {"const": "auto"}]}})
    account = _object(
        {
            "kind": {"enum": sorted(doc.ACCOUNT_KINDS)},
            "enabled": {"type": "boolean", "default": True},
            "role_arn": {"type": "string", "pattern": _pattern(doc._ROLE_ARN_RE.pattern)},
            "external_id_ref": secret_ref,
            "base_url": {"type": "string", "format": "uri"},
            "api_key_ref": secret_ref,
            "settings": _object(_settings_at(st.A)),
        },
        ("kind",),
    )
    model = _object(
        {
            "type": {"enum": list(doc.MODEL_TYPES)},
            "provider": {"enum": sorted(set(doc.ACCOUNT_KINDS.values()))},
            "model_id": _STRING,
            "family": {"enum": list(embedding_families())},
            "residency": residency,
            "deployments": {
                "type": "object",
                "minProperties": 1,
                "propertyNames": _NAME,
                "additionalProperties": deployments,
            },
            **_settings_at(st.M),
        },
        ("type", "provider", "model_id", "deployments"),
    )
    top = _object(
        {
            "registry_version": {"type": "integer", "minimum": 1, "maximum": st.MAX_NUMBER},
            "min_lib_version": {"type": "string"},
            "live_edit": {"enum": list(doc.LIVE_EDIT_MODES)},
            "defaults": _object(_settings_at(st.D)),
            "routing": _object(
                {
                    "kill_switch": {"type": "boolean", "default": False},
                    "allow_users": {"type": "array", "items": {"type": "string"}},
                    "blocked_regions": {"type": "array", "items": {"type": "string"}},
                    "blocked_accounts": {"type": "array", "items": {"type": "string"}},
                }
            ),
            "residency": residency,
            "accounts": {
                "type": "object",
                "minProperties": 1,
                "propertyNames": _NAME,
                "additionalProperties": {"$ref": "#/$defs/account"},
            },
            "models": {
                "type": "object",
                "minProperties": 1,
                "propertyNames": _NAME,
                "additionalProperties": {"$ref": "#/$defs/model"},
            },
        },
        ("registry_version", "accounts", "models"),
    )
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_ID,
        "title": "llm-queue registry document",
        "description": (
            "Shape, types and ranges of LLMQ_REGISTRY. Cross-field rules (budgets, fallback "
            "cycles, residency) are checked only by the library. Generated by "
            "`llm-queue registry schema`."
        ),
        **top,
        "$defs": {"account": account, "model": model, "profile": profile},
    }
