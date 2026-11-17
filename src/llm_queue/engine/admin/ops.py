"""The operations table (0.2.0 R3): one row per admin operation, read by every surface.

`OPERATIONS` is the single source of the admin API: `engine/http.py` mounts one route per row
with `http` set, `engine/cli.py` builds one sub-command per row with `cli` set, and the Python
surface is the method `method` names on `llmq.admin` (`AdminServices`). The parity test
(`tests/unit/test_admin_parity.py`) checks the three agree with this table, and
`docs/reference/admin-api.md` has one row per operation id.

Writes take `actor` (from the surface: `--actor`, the app's `actor(request)` or
`token:<X-LLMQ-Actor>`) and, unless `dry_run_default` is None, `dry_run` (default true: report
only).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

Verb = Literal["create", "read", "update", "delete"]
Guard = Literal["open", "read", "write"]
# How a parameter appears on the command line (positional arguments come first, in order).
CliKind = Literal["arg", "opt", "opts", "flag", "noflag", "pairs", "onoff"]
# When a route is mounted: always, with `allow_writes`, with `drain_routes`, `metrics_route`.
Mount = Literal["always", "writes", "drain", "metrics"]


class _Required:
    """The default of a parameter that has none (it must be given)."""

    def __repr__(self) -> str:
        return "REQUIRED"


REQUIRED: Any = _Required()


@dataclass(frozen=True, slots=True)
class Param:
    """One parameter of an operation: its JSON type, default and command-line form.

    `kind` is the accepted JSON type (`str`, `int`, `float` (ints too), `bool`, `list` of
    `item`, `dict`); `cli` how the command line spells it (`flag` is a store-true switch,
    `noflag` a `--no-<name>` switch (absent: None), `opts` a repeated option named `flag`,
    `pairs` `path=value` words, `onoff` a positional `on` / `off`). Over HTTP a `{name}` in the
    route's path takes it from the path (`deployment` from `{account}/{name}`), else from the
    query (GET) or the JSON body (POST).
    """

    name: str
    kind: type
    default: Any = REQUIRED
    cli: CliKind = "opt"
    flag: str | None = None  # the option's spelling when not `--<name with dashes>`
    item: type = str  # list items
    choices: tuple[str, ...] = ()
    help: str = ""

    @property
    def required(self) -> bool:
        """True when the parameter has no default."""
        return self.default is REQUIRED


@dataclass(frozen=True, slots=True)
class Op:
    """One admin operation and its three surfaces.

    `method`: dotted path on `AdminServices` (None for the process routes served by
    `LLMQueue` itself: health, metrics). `missing`: why a surface is absent, by surface name
    (`python`, `cli`, `http`). `store`: the CLI needs the environment's settings and stores
    (False only for `registry schema`).
    """

    id: str
    resource: str
    verb: Verb
    method: str | None
    summary: str
    params: tuple[Param, ...] = ()
    http: tuple[str, str] | None = None
    cli: tuple[str, ...] | None = None
    writes: bool = False
    audit_op: str | None = None
    dry_run_default: bool | None = None
    missing: Mapping[str, str] = field(default_factory=dict)
    mount: Mount = "always"
    store: bool = True

    @property
    def guard(self) -> Guard:
        """`open` (probes, metrics), `write` for writes, else `read`."""
        if self.resource in ("health", "metrics"):
            return "open"
        return "write" if self.writes else "read"


def _w(id: str, resource: str, verb: Verb, summary: str, audit: str, **kw: Any) -> Op:
    """A write with a dry run (the default for writes, D3)."""
    kw.setdefault("method", id)
    kw.setdefault("mount", "writes")
    return Op(
        id, resource, verb, summary=summary, writes=True, audit_op=audit, dry_run_default=True, **kw
    )


def _r(id: str, resource: str, summary: str, **kw: Any) -> Op:
    """A read."""
    kw.setdefault("method", id)
    return Op(id, resource, "read", summary=summary, **kw)


_MODEL = Param("model", str, cli="arg", help="model alias or id")
_DEPLOYMENT = Param("deployment", str, cli="arg", help="deployment account/name")
_OPT_MODEL = Param("model", str, None, help="model alias or id")
_DLQ_WHICH = (
    Param("model", str, None, help="a model's queue"),
    Param("job", str, None, help="a job name's queue"),
    Param("priority", bool, False, cli="flag", help="the model's priority queue"),
)
_TENANT_PICK = (
    Param("tenant", str, None, help="tenant name (hashed, never stored)"),
    Param("tenant_hash", str, None, help="tenant hash (16 hex) or - for none"),
)
_PROCESS = "process-local: on LLMQueue in the process it acts on"
_REQUESTS_STATE = ("waiting", "running", "all")

OPERATIONS: tuple[Op, ...] = (
    # -- index, process, drain ------------------------------------------------------------
    _r("ops.index", "ops", "Every operation, and a live summary of this process and store",
       http=("GET", "/"), cli=("ops",)),
    Op("health.live", "health", "read", None, "Liveness of this process's loops",
       http=("GET", "/health/live"), missing={"python": "LLMQueue.health()", "cli": _PROCESS}),
    Op("health.ready", "health", "read", None,
       "Readiness: registry loaded, not draining, stores' state",
       http=("GET", "/health/ready"), missing={"python": "LLMQueue.health()", "cli": _PROCESS}),
    Op("metrics.get", "metrics", "read", None, "Prometheus exposition of this process's sink",
       http=("GET", "/metrics"), mount="metrics",
       missing={"python": "the metrics sink", "cli": _PROCESS}),
    _r("process.describe", "process", "This process's settings, safe to log",
       method="describe", http=("GET", "/describe"), cli=("describe",)),
    _r("drain.get", "drain", "Whether this process drains, since when, held and running work",
       http=("GET", "/drain"), mount="drain", missing={"cli": _PROCESS}),
    Op("drain.start", "drain", "update", "drain.start",
       "Stop taking work here (readiness fails); held calls go back to the queue",
       http=("POST", "/drain"), writes=True, audit_op="drain", mount="drain",
       missing={"cli": _PROCESS + "; pods use LLMQ_DRAIN_FILE"}),
    Op("drain.resume", "drain", "update", "drain.resume", "Take work again after a drain",
       http=("POST", "/resume"), writes=True, audit_op="resume", mount="drain",
       missing={"cli": _PROCESS}),
    # -- status ---------------------------------------------------------------------------
    _r("status.get", "status", "Live status of every model and job, or of one model",
       params=(_OPT_MODEL,), http=("GET", "/status"), cli=("status",)),
    # -- registry -------------------------------------------------------------------------
    _r("registry.get", "registry", "The live registry (secrets hidden) and its live edits",
       params=(_OPT_MODEL,), http=("GET", "/registry"), cli=("registry", "get")),
    _r("registry.versions", "registry", "Published versions kept for rollback, and the current",
       http=("GET", "/registry/versions"), cli=("registry", "versions")),
    _r("registry.diff", "registry", "What is live but not in this process's registry file",
       http=("GET", "/registry/diff"), cli=("registry", "diff")),
    _r("registry.schema", "registry", "The registry document's JSON Schema",
       http=("GET", "/registry/schema"), cli=("registry", "schema"), store=False),
    _w("registry.set", "registry", "update", "Live edit of registry paths (validated)", "edit",
       params=(Param("values", dict, cli="pairs", help="path=value ... (JSON values)"),
               _OPT_MODEL),
       http=("POST", "/registry"), cli=("registry", "set")),
    _w("registry.publish", "registry", "create",
       "Publish this process's LLMQ_REGISTRY / LLMQ_REGISTRY_FILE by the version rule",
       "publish", http=("POST", "/registry/publish"), cli=("registry", "publish")),
    _w("registry.rollback", "registry", "update",
       "Publish an earlier version's content as a new version", "publish",
       params=(Param("version", int, cli="arg", help="the version to go back to"),),
       http=("POST", "/registry/rollback"), cli=("registry", "rollback")),
    _r("audit.list", "audit", "Latest audit entries, newest first",
       params=(Param("count", int, 20), Param("path", str, "", help="path prefix"),
               Param("op", str, None, help="only this op")),
       http=("GET", "/audit"), cli=("audit", "list")),
    # -- models and deployments -----------------------------------------------------------
    _r("models.list", "models", "Every model with its queues and deployment counts",
       http=("GET", "/models"), cli=("models", "list")),
    _r("models.get", "models", "One model: resolved settings and live status",
       params=(_MODEL,), http=("GET", "/models/{model}"), cli=("models", "get")),
    _w("models.enable", "models", "update", "Take new calls for a model", "edit",
       params=(_MODEL,), http=("POST", "/models/{model}/enable"), cli=("models", "enable")),
    _w("models.disable", "models", "update", "Refuse new calls for a model", "edit",
       params=(_MODEL,), http=("POST", "/models/{model}/disable"), cli=("models", "disable")),
    _w("models.drain", "models", "update",
       "Disable a model and wait for its waiting calls to be served", "model_drain",
       params=(_MODEL, Param("wait_secs", float, 0.0, help="wait this long for the queue")),
       http=("POST", "/models/{model}/drain"), cli=("models", "drain")),
    _r("deployments.list", "deployments", "A model's deployments with live budgets and health",
       params=(_MODEL,), http=("GET", "/models/{model}/deployments"),
       cli=("deployments", "list")),
    _r("deployments.get", "deployments", "One deployment: live state and resolved settings",
       params=(_MODEL, _DEPLOYMENT),
       http=("GET", "/models/{model}/deployments/{account}/{name}"),
       cli=("deployments", "get")),
    _w("deployments.enable", "deployments", "update", "Route calls to a deployment again",
       "edit", params=(_MODEL, _DEPLOYMENT),
       http=("POST", "/models/{model}/deployments/{account}/{name}/enable"),
       cli=("deployments", "enable")),
    _w("deployments.disable", "deployments", "update", "Route no calls to a deployment",
       "edit", params=(_MODEL, _DEPLOYMENT),
       http=("POST", "/models/{model}/deployments/{account}/{name}/disable"),
       cli=("deployments", "disable")),
    # -- blocks ---------------------------------------------------------------------------
    _r("blocks.list", "blocks", "Blocked regions and accounts, kill switch, by whom and since",
       http=("GET", "/blocks"), cli=("blocks", "list")),
    _w("blocks.add", "blocks", "create", "Block a region or an account for every model", "edit",
       params=(Param("region", str, None), Param("account", str, None)),
       http=("POST", "/blocks"), cli=("blocks", "add")),
    _w("blocks.remove", "blocks", "delete", "Lift a region or account block", "edit",
       params=(Param("region", str, None), Param("account", str, None)),
       http=("POST", "/blocks/remove"), cli=("blocks", "remove")),
    _w("blocks.kill_switch", "blocks", "update", "Turn the kill switch on or off", "edit",
       params=(Param("on", bool, cli="onoff", help="on or off"),),
       http=("POST", "/blocks/kill-switch"), cli=("blocks", "kill-switch")),
    # -- requests (calls) -----------------------------------------------------------------
    _r("requests.list", "requests", "Calls of a model in flight, by tenant and state",
       params=(_MODEL, *_TENANT_PICK, Param("state", str, "all", choices=_REQUESTS_STATE),
               Param("limit", int, 100), Param("after", str, None, help="cursor of a page")),
       http=("GET", "/requests"), cli=("requests", "list")),
    _r("requests.count", "requests", "Counts of a model's calls in flight, by state and stage",
       params=(_MODEL, *_TENANT_PICK), http=("GET", "/requests/count"),
       cli=("requests", "count")),
    _r("requests.get", "requests", "One call: its status and whether it is final",
       params=(Param("request_id", str, cli="arg"),), http=("GET", "/requests/{request_id}"),
       cli=("requests", "get")),
    _w("requests.cancel", "requests", "delete",
       "Cancel calls by id, or by model, tenant and state", "requests_cancel",
       params=(Param("model", str, None, help="model alias or id"),
               Param("request_ids", list, None, cli="opts", flag="--id", help="a request id"),
               *_TENANT_PICK, Param("state", str, "waiting", choices=_REQUESTS_STATE)),
       http=("POST", "/requests/cancel"), cli=("requests", "cancel")),
    _w("requests.reprioritize", "requests", "update",
       "Move a model's waiting calls to the priority or the normal class",
       "requests_reprioritize",
       params=(_MODEL, Param("priority", bool, cli="flag", help="to priority (else normal)"),
               *_TENANT_PICK, Param("limit", int, None)),
       http=("POST", "/requests/reprioritize"), cli=("requests", "reprioritize")),
    _w("requests.extend", "requests", "update", "Give a call more time", "requests_extend",
       params=(Param("request_id", str, cli="arg"), Param("timeout_secs", float)),
       http=("POST", "/requests/{request_id}/extend"), cli=("requests", "extend")),
    # -- tenants --------------------------------------------------------------------------
    _r("tenants.list", "tenants", "Tenants with calls in flight on a model (hashes only)",
       params=(_MODEL,), http=("GET", "/tenants"), cli=("tenants", "list")),
    _r("tenants.hash", "tenants", "The keyed hash of a tenant name (needs only the salt)",
       params=(Param("tenant", str, cli="arg"),), http=("POST", "/tenants/hash"),
       cli=("tenants", "hash")),
    # -- queues, dead letters, jobs -------------------------------------------------------
    _r("queues.list", "queues", "Every model and job queue: depth, held, dead letters",
       http=("GET", "/queues"), cli=("queues", "list")),
    _r("dlq.list", "dlq", "Dead letters of a queue, oldest first (no bodies)",
       params=(*_DLQ_WHICH, Param("count", int, 10)), http=("GET", "/dlq"),
       cli=("dlq", "list")),
    _r("dlq.show", "dlq", "One dead letter; its body only with unsafe",
       params=(Param("entry_id", str, cli="arg"), *_DLQ_WHICH,
               Param("unsafe", bool, False, cli="flag", help="include the body (prompts)")),
       http=("GET", "/dlq/{entry_id}"), cli=("dlq", "show")),
    _w("dlq.redrive", "dlq", "update", "Send dead letters back to their queue", "dlq_redrive",
       params=(*_DLQ_WHICH, Param("count", int, 1000)),
       http=("POST", "/dlq/redrive"), cli=("dlq", "redrive")),
    _w("dlq.purge", "dlq", "delete", "Delete dead letters (all, or by entry id)", "dlq_purge",
       params=(*_DLQ_WHICH,
               Param("entry_ids", list, None, cli="opts", flag="--id", help="an entry id")),
       http=("POST", "/dlq/purge"), cli=("dlq", "purge")),
    _r("jobs.list", "jobs", "Every job name: waiting, held, dead letters, consumer",
       http=("GET", "/jobs"), cli=("jobs", "list")),
    # -- purges ---------------------------------------------------------------------------
    _w("purge.model", "purge", "delete", "Delete a removed model's state", "purge_model",
       params=(_MODEL, Param("force", bool, False, cli="flag")),
       http=("POST", "/purge/model"), cli=("purge", "model")),
    _w("purge.deployment", "purge", "delete", "Delete a removed deployment's state",
       "purge_deployment",
       params=(_MODEL, _DEPLOYMENT, Param("force", bool, False, cli="flag"),
               Param("wait", bool, None, cli="noflag"), Param("wait_secs", float, None)),
       http=("POST", "/purge/deployment"), cli=("purge", "deployment")),
    _w("purge.tenant", "purge", "delete", "Delete a tenant's fairness state (and calls)",
       "purge_tenant",
       params=(Param("tenant", str, cli="arg"),
               Param("remove", list, None, help="keys, queued, running (comma list)"),
               Param("models", list, None, help="model aliases (comma list)")),
       http=("POST", "/purge/tenant"), cli=("purge", "tenant")),
    _w("purge.queue", "purge", "delete", "Cancel a model's queued calls (of one tenant)",
       "purge_queue", params=(_MODEL, *_TENANT_PICK),
       http=("POST", "/purge/queue"), cli=("purge", "queue")),
    _w("purge.removed", "purge", "delete",
       "Purge every model and deployment the registry no longer has", "purge_removed",
       params=(Param("force", bool, False, cli="flag"), Param("wait", bool, None, cli="noflag"),
               Param("wait_secs", float, None)),
       http=("POST", "/purge/removed"), cli=("purge", "removed")),
    _r("purge.scheduled", "purge", "Auto purges waiting for their grace",
       http=("GET", "/purge/scheduled"), cli=("purge", "scheduled")),
    _w("purge.unschedule", "purge", "delete", "Drop a scheduled auto purge", "purge_unschedule",
       params=(Param("target", str, cli="arg", help="alias or alias/account/name"),),
       http=("POST", "/purge/unschedule"), cli=("purge", "unschedule")),
)  # fmt: skip

BY_ID: Mapping[str, Op] = {op.id: op for op in OPERATIONS}


def op_row(op: Op) -> dict[str, Any]:
    """An operation as the `GET /` index and `ops` list it."""
    method, path = op.http if op.http else (None, None)
    return {
        "op": op.id,
        "resource": op.resource,
        "verb": op.verb,
        "method": method,
        "path": path,
        "cli": " ".join(op.cli) if op.cli else None,
        "python": f"llmq.admin.{op.method}" if op.method else None,
        "writes": op.writes,
        "guard": op.guard,
        "dry_run_default": op.dry_run_default,
        "audit_op": op.audit_op,
        "summary": op.summary,
    }
