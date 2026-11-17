"""The generated admin routes without a store: guards, bodies, errors, the index (0.2.0 R).

A fake `AdminServices` records each call (`admin_testkit.FakeAdmin`); the services themselves
run against the real stores in the integration tests.
"""

from __future__ import annotations

from typing import Any

import pytest
from admin_testkit import FakeAdmin, allow, client, mounted
from redis.exceptions import ConnectionError as StoreConnectionError

from llm_queue.engine.admin.context import NotLoaded
from llm_queue.engine.http import ADMIN_HEADER, MAX_EDIT_PATHS, error_response, redacted
from llm_queue.errors import (
    AdminRefused,
    ConfigError,
    LLMQueueError,
    StoreUnavailable,
    UnknownModel,
)
from llm_queue.registry.store import RegistryConflict

ADMIN = {ADMIN_HEADER: "1"}
TOKEN = "X-LLMQ-Token"


def _writes(admin: FakeAdmin, **kw: Any) -> Any:
    return client(admin, dependencies=[allow], allow_writes=True, actor=lambda r: "ann", **kw)


# -- guards ---------------------------------------------------------------------------------


def test_no_guard_mounts_only_the_probes_unless_insecure_reads() -> None:
    """Q13: with no token and no dependencies the reads were served to anyone (FO-14)."""
    bare = client(FakeAdmin())
    assert bare.get("/health/live").status_code == 200
    assert bare.get("/status").status_code == 404
    assert bare.get("/registry").status_code == 404
    open_reads = client(FakeAdmin(), insecure_reads=True)
    assert open_reads.get("/status").status_code == 200
    assert open_reads.post("/registry", json={"values": {"rpm": 1}}).status_code == 405
    with pytest.raises(ConfigError, match="dependencies"):
        client(FakeAdmin(), allow_writes=True, actor=lambda r: "ann")


def test_writes_need_a_write_guard_and_an_actor() -> None:
    with pytest.raises(ConfigError, match="LLMQ_ADMIN_TOKEN or dependencies"):
        client(FakeAdmin(), read_token="r", allow_writes=True, actor=lambda r: "ann")
    with pytest.raises(ConfigError, match="actor"):
        client(FakeAdmin(), dependencies=[allow], allow_writes=True)
    with pytest.raises(ConfigError, match="actor"):
        client(FakeAdmin(), dependencies=[allow], drain_routes=True)
    client(FakeAdmin(), admin_token="w", allow_writes=True)  # the token names the actor


def test_read_token_reads_only_and_admin_token_does_both() -> None:
    """Owner Q4: LLMQ_ADMIN_READ_TOKEN for GET routes, LLMQ_ADMIN_TOKEN for writes and reads."""
    admin = FakeAdmin()
    http = client(admin, admin_token="w", read_token="r", allow_writes=True)
    assert http.get("/status").status_code == 403
    assert http.get("/status", headers={TOKEN: "x"}).status_code == 403
    assert http.get("/status", headers={TOKEN: "r"}).status_code == 200
    assert http.get("/status", headers={TOKEN: "w"}).status_code == 200
    body = {"values": {"rpm": 5}, "model": "m"}
    assert http.post("/registry", json=body, headers={TOKEN: "r"}).status_code == 403
    done = http.post("/registry", json=body, headers={TOKEN: "w", "X-LLMQ-Actor": "bob"})
    assert done.status_code == 200
    assert admin.calls[-1] == (
        "registry.set",
        {"values": {"rpm": 5}, "model": "m", "dry_run": True, "actor": "token:bob"},
    )
    http.post("/registry", json=body, headers={TOKEN: "w"})
    assert admin.calls[-1][1]["actor"] == "token:unknown"  # D5: no claimed name


def test_token_is_read_per_request_and_an_empty_one_refuses() -> None:
    token = {"value": "a"}
    http = client(FakeAdmin(), admin_token=lambda: token["value"])
    assert http.get("/status", headers={TOKEN: "a"}).status_code == 200
    token["value"] = "b"  # rotated: no restart
    assert http.get("/status", headers={TOKEN: "a"}).status_code == 403
    assert http.get("/status", headers={TOKEN: "b"}).status_code == 200
    token["value"] = ""
    assert http.get("/status", headers={TOKEN: ""}).status_code == 403  # fail closed


def test_dependency_writes_need_the_admin_header_and_name_the_app_actor() -> None:
    """X2: a cross-site POST could edit the registry with an admin's cookies."""
    admin = FakeAdmin()
    http = _writes(admin)
    body = {"region": "r1"}
    assert http.post("/blocks", json=body).status_code == 403
    assert admin.calls == []
    assert http.post("/blocks", json=body, headers=ADMIN).status_code == 200
    assert admin.calls[-1] == ("blocks.add", {"region": "r1", "dry_run": True, "actor": "http:ann"})


def test_probes_and_metrics_are_open_everything_else_guarded() -> None:
    http = client(FakeAdmin(), admin_token="w", metrics_route=True)
    assert http.get("/health/live").status_code == 200
    assert http.get("/health/ready").status_code == 200
    assert http.get("/metrics").status_code == 200
    for path in ("/", "/status", "/models", "/queues", "/jobs", "/blocks", "/audit"):
        assert http.get(path).status_code == 403, path


def test_writes_are_not_mounted_without_allow_writes() -> None:
    http = client(FakeAdmin(), dependencies=[allow])
    for path in ("/purge/model", "/registry", "/blocks", "/dlq/redrive", "/requests/cancel"):
        assert http.post(path, json={}, headers=ADMIN).status_code in (404, 405), path


# -- bodies, queries and dry runs -----------------------------------------------------------


@pytest.mark.parametrize(
    ("body", "code"),
    [
        ({}, 400),  # model missing
        ({"model": 1}, 400),
        ({"model": "m", "extra": True}, 400),
        ({"model": "m" * 10_000}, 400),
        ({"model": "m", "dry_run": "no"}, 400),
        ({"model": "m"}, 200),
    ],
)
def test_bodies_are_checked_against_the_table(body: dict[str, Any], code: int) -> None:
    assert _writes(FakeAdmin()).post("/purge/model", json=body, headers=ADMIN).status_code == code


def test_every_write_defaults_to_a_dry_run() -> None:
    admin = FakeAdmin()
    http = _writes(admin)
    http.post("/purge/tenant", json={"tenant": "acme"}, headers=ADMIN)
    http.post("/purge/tenant", json={"tenant": "acme", "dry_run": False}, headers=ADMIN)
    assert [kw["dry_run"] for _, kw in admin.calls] == [True, False]


def test_path_and_query_arguments() -> None:
    admin = FakeAdmin()
    http = _writes(admin)
    http.get("/models/m1/deployments/acct/us-east-1")
    assert admin.calls[-1] == ("deployments.get", {"model": "m1", "deployment": "acct/us-east-1"})
    http.get("/dlq", params={"model": "m1", "priority": "true", "count": "5"})
    assert admin.calls[-1] == ("dlq.list", {"model": "m1", "priority": True, "count": 5})
    assert http.get("/dlq", params={"count": "many"}).status_code == 400
    assert http.get("/dlq", params={"count": "-1"}).status_code == 400
    assert http.get("/requests", params={"model": "m", "state": "odd"}).status_code == 400
    http.post("/requests/cancel", json={"request_ids": ["a", "b"]}, headers=ADMIN)
    assert admin.calls[-1][1]["request_ids"] == ["a", "b"]


@pytest.mark.parametrize("path", ["/status", "/requests", "/requests/count"])
def test_b05_get_routes_take_the_tenant_hash_only(path: str) -> None:
    """B-05: a name in a query string reached proxy and access logs nobody can redact."""
    admin = FakeAdmin()
    http = _writes(admin)
    refused = http.get(path, params={"model": "m", "tenant": "acme"})
    assert refused.status_code == 400
    assert "tenant_hash" in refused.json()["error"]
    assert http.get(path, params={"model": "m", "tenant_hash": "-"}).status_code == 200
    assert admin.calls[-1][1]["tenant_hash"] == "-"


def test_b22_tenant_hash_needs_the_write_guard() -> None:
    """B-22: with the hashes reads show, a read token could confirm which tenant a hash is."""
    http = client(FakeAdmin(), admin_token="w", read_token="r")
    assert http.post("/tenants/hash", json={"tenant": "a"}, headers={TOKEN: "r"}).status_code == 403
    assert http.post("/tenants/hash", json={"tenant": "a"}, headers={TOKEN: "w"}).status_code == 200
    reads_only = client(FakeAdmin(), read_token="r")
    assert reads_only.post("/tenants/hash", json={"tenant": "a"}).status_code == 404  # unmounted


def test_fo15_bounds_on_ids_and_edits() -> None:
    """FO-15: ids, models and edit bodies had no size bound before the store read."""
    http = _writes(FakeAdmin())
    assert http.get("/requests/" + "x" * 10_000).status_code == 400
    assert http.get("/status", params={"model": "m" * 10_000}).status_code == 400
    many = {f"models.m{i}.rpm": 1 for i in range(MAX_EDIT_PATHS + 1)}
    assert http.post("/registry", json={"values": many}, headers=ADMIN).status_code == 400


def test_http_waits_are_always_capped() -> None:
    """Rescan B3-R3: an omitted wait waited LLMQ_PURGE_WAIT_SECS (300 s) per deployment."""
    admin = FakeAdmin()
    http = _writes(admin, wait_cap_secs=60)
    dep = {"model": "m", "deployment": "a/r"}
    for body, want in (({}, 60.0), ({"wait_secs": 500}, 60.0), ({"wait_secs": 5}, 5.0)):
        assert http.post("/purge/deployment", json={**dep, **body}, headers=ADMIN).is_success
        assert admin.calls[-1][1]["wait_secs"] == want
    http.post("/models/m/drain", json={"wait_secs": 900}, headers=ADMIN)
    assert admin.calls[-1][1]["wait_secs"] == 60.0
    for bad in (True, -1):
        body = {**dep, "wait_secs": bad}
        assert http.post("/purge/deployment", json=body, headers=ADMIN).status_code == 400


def test_dead_letter_bodies_need_their_setting() -> None:
    admin = FakeAdmin()
    assert (
        _writes(admin).get("/dlq/1-0", params={"model": "m", "unsafe": "true"}).status_code == 403
    )
    assert _writes(admin).get("/dlq/1-0", params={"model": "m"}).status_code == 200
    shown = _writes(admin, dlq_bodies=True).get("/dlq/1-0", params={"model": "m", "unsafe": "1"})
    assert shown.status_code == 200
    assert admin.calls[-1][1] == {"entry_id": "1-0", "model": "m", "unsafe": True}


# -- errors ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (UnknownModel("nope"), 404),
        (RegistryConflict("lost the race"), 409),
        (AdminRefused("still in use"), 409),
        (NotLoaded("registry not loaded"), 503),
        (StoreUnavailable("down"), 503),
        (StoreConnectionError("refused"), 503),
        (ConfigError("bad"), 400),
        (LLMQueueError("stopped"), 409),
    ],
)
def test_errors_map_to_status_codes(error: Exception, code: int) -> None:
    """Design 2.8 (and EN-10: redis-py's outage was a 500 with a traceback)."""
    assert error_response(error)[0] == code
    http = _writes(FakeAdmin(error))
    reply = http.post("/purge/model", json={"model": "m"}, headers=ADMIN)
    assert reply.status_code == code
    retry = reply.headers.get("Retry-After")
    assert (retry is not None) == isinstance(error, RegistryConflict)  # FO-6


def test_a_bug_is_not_reported_as_a_client_error() -> None:
    """FO-17: any KeyError inside status reading answered 404 'unknown model'."""
    with pytest.raises(KeyError):
        error_response(KeyError("internal"))


def test_a_read_that_finds_nothing_is_404() -> None:
    admin = FakeAdmin()
    admin.results["requests.get"] = None
    assert _writes(admin).get("/requests/r1").status_code == 404


# -- index, drain, misc ---------------------------------------------------------------------


def test_index_lists_exactly_the_mounted_routes_and_is_cached() -> None:
    admin = FakeAdmin()
    app_routes = _writes(admin, drain_routes=True, metrics_route=True, index_cache_secs=60)
    index = app_routes.get("/").json()
    routes = mounted(app_routes)
    assert {(r["method"], r["path"]) for r in index["routes"]} == routes
    app_routes.get("/")
    assert admin.index_calls == 1


def test_drain_routes_are_writes_and_audited_with_the_actor() -> None:
    admin = FakeAdmin()
    http = _writes(admin, drain_routes=True)
    assert http.post("/drain").status_code == 403  # no admin header
    assert http.post("/drain", headers=ADMIN).status_code == 200
    assert http.post("/resume", headers=ADMIN).status_code == 200
    assert http.get("/drain").status_code == 200
    assert admin.calls == [
        ("drain.start", {"actor": "http:ann"}),
        ("drain.resume", {"actor": "http:ann"}),
        ("drain.get", {}),
    ]


def test_fo16_redaction_shares_the_store_set_and_walks_lists() -> None:
    """FO-16: two copies of the secret-field set; a new field had to be added in both."""
    from llm_queue.registry.store import SECRET_FIELDS, audit_diff

    doc = {"accounts": [dict.fromkeys(SECRET_FIELDS, "secret")]}
    assert all(v == "[redacted]" for v in redacted(doc)["accounts"][0].values())
    (entry,) = audit_diff({"a": []}, {"a": [{"api_key_ref": "LLMQ_ACCOUNT_K"}]})
    assert entry["after"] == [{"api_key_ref": "[redacted]"}]


def test_fo9_liveness_does_not_touch_the_store() -> None:
    """FO-9: /health/live ran the full health check, which probed the store inline."""
    admin = FakeAdmin()
    assert client(admin, insecure_reads=True).get("/health/live").json() == {"live": True}
    assert admin.context.engine.health_calls == 0
