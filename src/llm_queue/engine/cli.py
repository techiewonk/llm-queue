"""The `llm-queue` command line for 0.3 (plan §3.12, §3.18): registry, status, operations.

Every command but `registry schema` reads `LLMQ_*` from the environment
(`InfraSettings.from_env`) and talks to the stores directly; writes go through the registry
store (validated, protected fields, audited with `--actor`, default the OS user). Output is
JSON, one document per command. Commands that delete or move data (`dlq purge`,
`dlq redrive`, `registry gc`, `drain`, `cancel`) need `--yes`; `--dry-run` shows what they
would do.

Exit codes: 0 done, 1 an error (bad input, unknown or expired request, store outage), 3 a
`drain --wait` that timed out with calls still waiting (FO-8).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import getpass
import json
import os
import sys
import time
from collections.abc import Mapping, Sequence
from typing import Any

import orjson
import redis.asyncio
from redis.exceptions import RedisError

from llm_queue._logging import get_logger
from llm_queue.engine._codec import text as _text
from llm_queue.engine.http import redacted
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import RETENTION_SECS, StreamQueues
from llm_queue.engine.requests import RequestStore
from llm_queue.engine.settings import InfraSettings
from llm_queue.engine.status import StatusReader
from llm_queue.engine.store_client import secret_store_client
from llm_queue.errors import ConfigError, LLMQueueError
from llm_queue.registry._lua import AUDIT_MAX_ENTRIES
from llm_queue.registry.json_schema import registry_schema
from llm_queue.registry.store import RegistryStore
from llm_queue.tuning import Tuning

logger = get_logger(__name__)
_GC_BATCH = 500  # keys per UNLINK pipeline in `registry gc`
EXIT_DRAIN_TIMEOUT = 3  # `drain --wait` ran out of time with calls still waiting (FO-8)
MAX_CANCEL_IDS = 1000  # one `cancel` command at most


def default_actor() -> str:
    """The OS user for the audit, or `uid:<n>` when the user has no passwd entry (FO-5).

    `getpass.getuser()` raises in a container running under a UID with no passwd entry and no
    `USER` / `LOGNAME` (`KeyError` on 3.11 / 3.12, `OSError` on 3.13), which failed every
    command, read-only ones included.
    """
    try:
        return getpass.getuser()
    except (KeyError, OSError):
        getuid = getattr(os, "getuid", None)
        if getuid is not None:
            return f"uid:{getuid()}"
        raise ConfigError("cannot tell who you are: pass --actor NAME") from None


def _glob_escape(text: str) -> str:
    """`text` matched literally in a SCAN pattern (glob characters and backslash escaped)."""
    return "".join(f"\\{c}" if c in "*?[]\\" else c for c in text)


def _value(text: str) -> Any:
    """A command-line value: JSON when it parses (`400`, `true`, `["a"]`), else the text."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _confirm(parser: argparse.ArgumentParser) -> None:
    """`--yes` / `--dry-run` on a command that deletes or moves data."""
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--yes", action="store_true", help="do it (the command changes data)")
    group.add_argument("--dry-run", action="store_true", dest="dry_run", help="only report")


def _confirmed(args: argparse.Namespace, what: str) -> bool:
    """True to go ahead, False for a dry run; `ConfigError` when neither flag was given."""
    if args.dry_run:
        return False
    if not args.yes:
        raise ConfigError(f"{what}: pass --yes to do it, or --dry-run to see what it would do")
    return True


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="llm-queue", description=__doc__.splitlines()[0])
    parser.add_argument("--actor", default=None, help="who makes a change (audit)")
    sub = parser.add_subparsers(dest="command", required=True)

    reg = sub.add_parser("registry", help="read and change the registry").add_subparsers(
        dest="action", required=True
    )
    get = reg.add_parser("get")
    get.add_argument("--model")
    setp = reg.add_parser("set", help="live edit: path=value ... (JSON values)")
    setp.add_argument("--model")
    setp.add_argument("pairs", nargs="+")
    for name in ("enable", "disable"):
        p = reg.add_parser(name)
        p.add_argument("model")
        p.add_argument("deployment")
    reg.add_parser("diff", help="what is live but not in this config")
    reg.add_parser("schema", help="print the registry document's JSON Schema (no store needed)")
    log = reg.add_parser("log")
    log.add_argument("--count", type=int, default=20)
    log.add_argument("--path", default="")
    pub = reg.add_parser("publish", help="publish LLMQ_REGISTRY, or roll back")
    pub.add_argument("--from-version", type=int, dest="from_version")
    _confirm(reg.add_parser("gc", help="delete keys and queues of models that no longer exist"))

    settings = sub.add_parser("settings").add_subparsers(dest="action", required=True)
    sget = settings.add_parser("get")
    sget.add_argument("--model", required=True)
    sget.add_argument("--deployment")

    status = sub.add_parser("status")
    status.add_argument("--model")
    request = sub.add_parser("request")
    request.add_argument("request_id")

    for name in ("block", "unblock"):
        p = sub.add_parser(name)
        p.add_argument("--region")
        p.add_argument("--account")

    cancel = sub.add_parser("cancel", help="cancel calls by request id (audited)")
    cancel.add_argument("request_ids", nargs="+")
    _confirm(cancel)

    drain = sub.add_parser(
        "drain",
        help="disable a model for new calls; serve waiting ones "
        "(undo: registry set --model M enabled=true)",
    )
    drain.add_argument("model")
    drain.add_argument("--wait", action="store_true")
    drain.add_argument("--timeout", type=float, default=600.0)
    _confirm(drain)

    dlq = sub.add_parser("dlq").add_subparsers(dest="action", required=True)
    for name in ("list", "show", "redrive", "purge"):
        p = dlq.add_parser(name)
        which = p.add_mutually_exclusive_group(required=True)
        which.add_argument("--model")
        which.add_argument("--job", help="a job name: its dead jobs (Q15)")
        p.add_argument("--priority", action="store_true")
        p.add_argument("--count", type=int, default=10)
        if name == "show":
            p.add_argument("--unsafe", action="store_true", help="print message bodies")
        if name in ("redrive", "purge"):
            _confirm(p)

    queues = sub.add_parser("queues").add_subparsers(dest="action", required=True)
    queues.add_parser("list")
    return parser


class Cli:
    """The command implementations over one environment's stores."""

    def __init__(
        self,
        settings: InfraSettings,
        client: redis.asyncio.Redis,
        actor: str | None,
        queue_client: redis.asyncio.Redis | None = None,
    ) -> None:
        """Wire the stores: queues live on `queue_client` when `LLMQ_QUEUE_STORE_URL` is set.

        `actor` None means the OS user, looked up only when a command writes (FO-5).
        """
        self.settings = settings
        self.client = client
        self.queue_client = queue_client
        self._actor = actor
        self.keys = Keys(settings.key_prefix)
        config = None
        if settings.registry or settings.registry_file:
            config = orjson.loads(settings.registry_document())
        self.registry = RegistryStore(
            client,
            settings.key_prefix,
            config=config,
            live_edit=settings.registry_live_edit,
            allowed_hosts=settings.base_url_allowed_hosts,
            audit_retention_days=settings.audit_retention_days,
        )
        self.queues = StreamQueues(queue_client or client, self.keys)

    @property
    def actor(self) -> str:
        """Who makes a change, for the audit: `--actor`, else the OS user."""
        if self._actor is None:
            self._actor = default_actor()
        return self._actor

    async def loaded(self) -> Any:
        """The stored registry; `ConfigError` when none is published."""
        view = await self.registry.load()
        if view is None:
            raise ConfigError("no registry is published yet (llm-queue registry publish)")
        return view

    def _queue(self, view: Any, model: str, priority: bool) -> str:
        alias = view.registry.alias_of(model)
        name = self.settings.queue_prefix + view.queue_name(alias)
        return f"{name}-priority" if priority else name

    async def run(self, args: argparse.Namespace) -> Any:
        """Run one parsed command; returns what to print."""
        handler = getattr(self, f"_{args.command}")
        return await handler(args)

    async def _registry(self, args: argparse.Namespace) -> Any:
        action = args.action
        if action == "publish":
            if args.from_version is not None:
                result = await self.registry.rollback(args.from_version, actor=self.actor)
            else:
                if self.registry.config is None:
                    raise ConfigError("LLMQ_REGISTRY (or LLMQ_REGISTRY_FILE) is not set")
                result = await self.registry.publish(self.registry.config, actor=self.actor)
            return {"action": result.action, "version": result.version}
        if action == "log":
            return await self.registry.log(args.count, args.path)
        if action == "diff":
            return redacted(await self.registry.diff_with_config())
        view = await self.loaded()
        if action == "get":
            doc = dict(view.registry.raw)
            if args.model:
                doc = doc["models"][view.registry.alias_of(args.model)]
            return redacted(doc)
        if action == "set":
            values = {}
            for pair in args.pairs:
                path, sep, text = pair.partition("=")
                if not sep:
                    raise ConfigError(f"expected path=value, got {pair!r}")
                values[path] = _value(text)
            changed = await self.registry.set(values, actor=self.actor, model=args.model)
            return {"changes": self.registry.changes, "version": changed.registry.version}
        if action in ("enable", "disable"):
            await self.registry.set_enabled(
                args.model, args.deployment, action == "enable", actor=self.actor
            )
            return {"model": args.model, "deployment": args.deployment, action: True}
        if action == "gc":
            return await self._gc(view, _confirmed(args, "registry gc deletes keys"))
        raise ConfigError(f"unknown registry action {action}")

    async def _gc(self, view: Any, go: bool) -> dict[str, Any]:
        """Delete the keys and queues of models that are not in the registry any more.

        A removed model's queues are found by name: `llm-<alias>` or the `queue.name` an older
        published version (kept for rollback) gave it. Job queues (`jobs-*`) and queues of other
        prefixes are never touched.
        """
        known = set(view.registry.models)
        prefix = self.keys.prefix
        doomed: list[tuple[Any, str]] = []
        removed: set[str] = set()
        # The prefix is escaped: a `[`, `*` or `?` in LLMQ_KEY_PREFIX widened the match (EN-30).
        glob = _glob_escape(prefix)
        async for key in self.client.scan_iter(match=f"{glob}:{{m:*", count=500):
            alias = _text(key).split("{m:", 1)[1].split("}", 1)[0]
            if alias not in known:
                doomed.append((self.client, _text(key)))
                removed.add(alias)
        names = {f"llm-{alias}" for alias in removed}
        for raw in (await self.client.hgetall(self.registry.history_key)).values():
            models = orjson.loads(raw).get("models") or {}
            for alias, body in models.items():
                if alias not in known:
                    queue = (
                        (body.get("queue") or {}).get("name") if isinstance(body, dict) else None
                    )
                    names.add(str(queue) if queue else f"llm-{alias}")
        live = {view.queue_name(alias) for alias in known}
        gone = {self.settings.queue_prefix + name for name in names - live}
        gone |= {f"{name}-priority" for name in gone}
        queue_client = self.queue_client or self.client
        async for key in queue_client.scan_iter(match=f"{glob}:{{q:*", count=500):
            if _text(key).split("{q:", 1)[1].split("}", 1)[0] in gone:
                doomed.append((queue_client, _text(key)))
        for name in sorted(gone):
            bell = self.queues.bell(name)
            if await queue_client.exists(bell):
                doomed.append((queue_client, bell))
        if go:  # UNLINK in pipelined batches: one DEL round trip per key before (EN-30)
            for client in {id(c): c for c, _ in doomed}.values():
                mine = [key for c, key in doomed if c is client]
                for start in range(0, len(mine), _GC_BATCH):
                    async with client.pipeline(transaction=False) as pipe:
                        for key in mine[start : start + _GC_BATCH]:
                            pipe.unlink(key)
                        await pipe.execute()
        out: dict[str, Any] = {"deleted_keys" if go else "would_delete": len(doomed)}
        out["queues"] = sorted(gone)
        return out

    async def _settings(self, args: argparse.Namespace) -> Any:
        view = await self.loaded()
        table = view.table(args.model, args.deployment)
        return {name: {"value": e.value, "level": e.level} for name, e in table.items()}

    async def _status(self, args: argparse.Namespace) -> Any:
        view = await self.loaded()
        reader = StatusReader(self.client, self.keys, self.queues, self.settings.queue_prefix)
        if args.model:
            return await reader.model(view, view.registry.alias_of(args.model))
        return await reader.report(view)

    async def _request(self, args: argparse.Namespace) -> Any:
        reader = StatusReader(self.client, self.keys, self.queues, self.settings.queue_prefix)
        return await reader.request(args.request_id) or {"error": "unknown or expired request"}

    async def _block(self, args: argparse.Namespace, on: bool = True) -> Any:
        if not args.region and not args.account:
            raise ConfigError("give --region or --account")
        await self.loaded()
        view = await self.registry.block(
            region=args.region, account=args.account, on=on, actor=self.actor
        )
        routing = view.registry.routing
        return {
            "blocked_regions": list(routing.blocked_regions),
            "blocked_accounts": list(routing.blocked_accounts),
        }

    async def _unblock(self, args: argparse.Namespace) -> Any:
        return await self._block(args, on=False)

    async def _cancel(self, args: argparse.Namespace) -> Any:
        """Cancel calls for every caller of each id, audited like a registry edit (owner Q5).

        Anyone may cancel any id; the audit stream (`registry log`) records who, and each
        cancel is logged as `call_cancel` with `source=cli`.
        """
        ids = list(dict.fromkeys(args.request_ids))
        if len(ids) > MAX_CANCEL_IDS or any(not rid or len(rid) > 256 for rid in ids):
            raise ConfigError(f"give 1-{MAX_CANCEL_IDS} request ids of at most 256 characters")
        if not _confirmed(args, "cancel stops these calls for every caller"):
            return {"would_cancel": ids}
        actor = self.actor
        store = RequestStore(self.client, self.keys, status_ttl_secs=self.settings.status_ttl_secs)
        cancelled = await store.cancel(
            ids, ttl_secs=self.settings.status_ttl_secs, channel=self.keys.cancel_channel
        )
        for rid in ids:
            logger.info("call_cancel", request_id=rid, source="cli", by=actor)
        diff = [{"path": f"requests.{rid}", "before": None, "after": "cancelled"} for rid in ids]
        await self.client.xadd(
            self.registry.audit_key,
            {
                "actor": actor,
                "op": "cancel",
                "version": "",
                "diff": orjson.dumps(diff),
                "ts": f"{time.time():.6f}",  # wall time, like the Lua audit entries
            },
            maxlen=AUDIT_MAX_ENTRIES,
            approximate=True,
        )
        return {"cancelled": cancelled, "requested": len(ids), "actor": actor}

    async def _drain(self, args: argparse.Namespace) -> Any:
        view = await self.loaded()
        alias = view.registry.alias_of(args.model)
        if not _confirmed(args, f"drain disables {alias} for new calls"):
            depth = [await self.queues.depth(self._queue(view, alias, p)) for p in (True, False)]
            return {"model": alias, "would_disable": True, "waiting": sum(d[0] for d in depth)}
        await self.registry.set({"enabled": False}, model=alias, actor=self.actor)
        if not args.wait:
            return {"model": alias, "enabled": False}
        loop = asyncio.get_running_loop()
        until = loop.time() + args.timeout
        while loop.time() < until:
            depth = [await self.queues.depth(self._queue(view, alias, p)) for p in (True, False)]
            if all(total == 0 for total, _, _ in depth):
                return {"model": alias, "enabled": False, "drained": True}
            await asyncio.sleep(1)
        return {"model": alias, "enabled": False, "drained": False}

    async def _dlq(self, args: argparse.Namespace) -> Any:
        if args.job:
            queue = f"{self.settings.queue_prefix}jobs-{args.job}"
            view = None
        else:
            view = await self.loaded()
            queue = self._queue(view, args.model, args.priority)
        if args.action == "list":
            letters = await self.queues.dead_letters(queue, args.count)
            return [{k: v for k, v in letter.items() if k != "body"} for letter in letters]
        if args.action == "show":
            letters = await self.queues.dead_letters(queue, args.count)
            if not args.unsafe:
                return [{k: v for k, v in letter.items() if k != "body"} for letter in letters]
            return [
                {**letter, "body": letter["body"].decode(errors="replace")} for letter in letters
            ]
        if args.action == "redrive":
            if not _confirmed(args, "dlq redrive sends dead letters back to the queue"):
                _, _, dead = await self.queues.depth(queue)
                return {"would_redrive": min(dead, args.count), "queue": queue}
            # The model's own stream retention: the redrive trims like a send (EN-12), and the
            # 4-day default cut a queue that keeps more. Jobs keep the queue default.
            keep = (
                RETENTION_SECS
                if view is None
                else int(view.value(view.registry.alias_of(args.model), "queue")["retention_secs"])
            )
            return {"redriven": await self.queues.redrive(queue, args.count, keep)}
        if not _confirmed(args, "dlq purge deletes every dead letter"):
            _, _, dead = await self.queues.depth(queue)
            return {"would_purge": dead, "queue": queue}
        await self.queues.purge_dead(queue)
        return {"purged": queue}

    async def _queues(self, args: argparse.Namespace) -> Any:
        view = await self.loaded()
        out = {}
        for alias in view.registry.models:
            for priority in (False, True):
                name = self._queue(view, alias, priority)
                total, held, dead = await self.queues.depth(name)
                out[name] = {"messages": total, "held": held, "dead": dead}
        # Job queues too: dead jobs had no CLI at all (R3JE-4, Q15).
        key = f"{self.queues.keys.prefix}:jobs:{self.settings.queue_prefix}"
        for job in sorted(_text(n) for n in await self.queues.client.smembers(key)):
            name = f"{self.settings.queue_prefix}jobs-{job}"
            total, held, dead = await self.queues.depth(name)
            out[name] = {"messages": total, "held": held, "dead": dead}
        return out


def exit_code(result: Any) -> int:
    """The exit code of a printed result: 1 for an error document, 3 for a drain timeout.

    `request ID` printed `{"error": ...}` and `drain --wait` printed `"drained": false`, both
    with exit 0, so a script waiting on them carried on (FO-8).
    """
    if isinstance(result, Mapping):
        if "error" in result:
            return 1
        if result.get("drained") is False:
            return EXIT_DRAIN_TIMEOUT
    return 0


async def amain(argv: Sequence[str], environ: Mapping[str, str] | None = None) -> Any:
    """Parse and run one command; returns its JSON-able result."""
    args = _parser().parse_args(list(argv))
    if args.command == "registry" and args.action == "schema":
        return registry_schema()  # needs no settings and no store: editors and CI use it
    env = dict(os.environ if environ is None else environ)
    settings = InfraSettings.from_env(env)
    # The same clients as the queue's: the password variable wins over one in the URL and is
    # read again on each new connection (rotation); timeouts and retries from `Tuning()`.
    cap, options = settings.store_max_connections, Tuning().store
    client = secret_store_client(settings.store_url, env.get, "LLMQ_STORE_PASSWORD", cap, options)
    queue_client = (
        secret_store_client(
            settings.queue_store_url, env.get, "LLMQ_QUEUE_STORE_PASSWORD", cap, options
        )
        if settings.queue_store_url
        else None
    )
    # An exit stack: when the first close raised, the queue client was never closed (EN-30).
    async with contextlib.AsyncExitStack() as stack:
        if queue_client is not None:
            stack.push_async_callback(queue_client.aclose)
        stack.push_async_callback(client.aclose)
        cli = Cli(settings, client, args.actor, queue_client)
        return await cli.run(args)


def main(argv: Sequence[str] | None = None, environ: Mapping[str, str] | None = None) -> int:
    """Console entry point: print the result as JSON; exit 1 with the message on errors."""
    try:
        result = asyncio.run(amain(sys.argv[1:] if argv is None else argv, environ))
    except (LLMQueueError, KeyError) as exc:  # a bad name or edit: one line, not a traceback
        print(str(exc), file=sys.stderr)
        return 1
    except RedisError as exc:  # an outage printed redis-py's traceback (EN-30)
        print(f"state store error ({type(exc).__name__}): {exc}", file=sys.stderr)
        return 1
    except OSError as exc:  # a file or user lookup (3.13 raises OSError for getuser), FO-5
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, default=str))
    return exit_code(result)
