"""The `llm-queue` command line for 0.3 (plan §3.12, §3.18): registry, status, operations.

Every command reads `LLMQ_*` from the environment (`InfraSettings.from_env`) and talks to the
stores directly; writes go through the registry store (validated, protected fields, audited
with `--actor`, default the OS user). Output is JSON, one document per command. Commands that
delete or move data (`dlq purge`, `dlq redrive`, `registry gc`, `drain`) need `--yes`;
`--dry-run` shows what they would do.
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import os
import sys
from collections.abc import Mapping, Sequence
from typing import Any

import orjson
import redis.asyncio

from llm_queue.engine.http import redacted
from llm_queue.engine.keys import Keys
from llm_queue.engine.queue import StreamQueues
from llm_queue.engine.settings import InfraSettings
from llm_queue.engine.status import StatusReader
from llm_queue.engine.store_client import secret_store_client
from llm_queue.errors import ConfigError, LLMQueueError
from llm_queue.registry.store import RegistryStore
from llm_queue.tuning import Tuning


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


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

    drain = sub.add_parser("drain", help="disable a model for new calls; serve waiting ones")
    drain.add_argument("model")
    drain.add_argument("--wait", action="store_true")
    drain.add_argument("--timeout", type=float, default=600.0)
    _confirm(drain)

    dlq = sub.add_parser("dlq").add_subparsers(dest="action", required=True)
    for name in ("list", "show", "redrive", "purge"):
        p = dlq.add_parser(name)
        p.add_argument("--model", required=True)
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
        actor: str,
        queue_client: redis.asyncio.Redis | None = None,
    ) -> None:
        """Wire the stores: queues live on `queue_client` when `LLMQ_QUEUE_STORE_URL` is set."""
        self.settings = settings
        self.client = client
        self.queue_client = queue_client
        self.actor = actor
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
        async for key in self.client.scan_iter(match=f"{prefix}:{{m:*", count=500):
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
        async for key in queue_client.scan_iter(match=f"{prefix}:{{q:*", count=500):
            if _text(key).split("{q:", 1)[1].split("}", 1)[0] in gone:
                doomed.append((queue_client, _text(key)))
        for name in sorted(gone):
            bell = self.queues.bell(name)
            if await queue_client.exists(bell):
                doomed.append((queue_client, bell))
        if go:
            for client, key in doomed:
                await client.delete(key)
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
            return {"redriven": await self.queues.redrive(queue, args.count)}
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
        return out


async def amain(argv: Sequence[str], environ: Mapping[str, str] | None = None) -> Any:
    """Parse and run one command; returns its JSON-able result."""
    args = _parser().parse_args(list(argv))
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
    try:
        cli = Cli(settings, client, args.actor or getpass.getuser(), queue_client)
        return await cli.run(args)
    finally:
        await client.aclose()
        if queue_client is not None:
            await queue_client.aclose()


def main(argv: Sequence[str] | None = None, environ: Mapping[str, str] | None = None) -> int:
    """Console entry point: print the result as JSON; exit 1 with the message on errors."""
    try:
        result = asyncio.run(amain(sys.argv[1:] if argv is None else argv, environ))
    except (LLMQueueError, KeyError) as exc:  # a bad name or edit: one line, not a traceback
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, default=str))
    return 0
