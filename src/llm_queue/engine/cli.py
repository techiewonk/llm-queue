"""The `llm-queue` command line, generated from the operations table (0.2.0 R).

One sub-command per operation of `engine/admin/ops.py` with a `cli` entry (`llm-queue models
list`, `llm-queue requests cancel --id ID`, ...); each calls the same `llmq.admin` method as
Python and HTTP, over the stores `LLMQ_*` names (`AdminServices.from_env`). Output is JSON, one
document per command. Commands that change something report what they would do unless given
`--yes` (a dry run by default, D3); they are audited with `--actor` (default the OS user).
`registry schema` needs no settings and no store.

Exit codes: 0 done, 1 an error (bad input, unknown or expired item, store outage), 3 a
`models drain --wait-secs` that ran out with calls still waiting (FO-8).
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import inspect
import json
import os
import sys
from collections.abc import Mapping, Sequence
from typing import Any

from redis.exceptions import RedisError

from llm_queue.engine.admin.ops import OPERATIONS, Op, Param
from llm_queue.errors import ConfigError, LLMQueueError

EXIT_DRAIN_TIMEOUT = 3  # `models drain --wait-secs` ran out with calls still waiting (FO-8)


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


def _value(text: str) -> Any:
    """A command-line value: JSON when it parses (`400`, `true`, `["a"]`), else the text."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _pairs(words: Sequence[str]) -> dict[str, Any]:
    """`path=value` words as a mapping (values parsed as JSON when they parse)."""
    values: dict[str, Any] = {}
    for pair in words:
        path, sep, text = pair.partition("=")
        if not sep or not path:
            raise ConfigError(f"expected path=value, got {pair!r}")
        values[path] = _value(text)
    return values


def _onoff(text: str) -> bool:
    if text not in ("on", "off"):
        raise argparse.ArgumentTypeError("on or off")
    return text == "on"


def _scalar(kind: type) -> Any:
    return {int: int, float: float}.get(kind, str)


def _add_param(parser: argparse.ArgumentParser, param: Param) -> None:
    """One parameter as an argument (dest = the parameter's name)."""
    flag = param.flag or "--" + param.name.replace("_", "-")
    extra: dict[str, Any] = {"help": param.help or None}
    if param.choices:
        extra["choices"] = param.choices
    if param.cli == "arg":
        parser.add_argument(param.name, type=_scalar(param.kind), **extra)
    elif param.cli == "pairs":
        parser.add_argument(param.name, nargs="+", metavar="path=value", **extra)
    elif param.cli == "onoff":
        parser.add_argument(param.name, type=_onoff, metavar="on|off", **extra)
    elif param.cli == "flag":
        parser.add_argument(flag, dest=param.name, action="store_true", **extra)
    elif param.cli == "noflag":
        parser.add_argument(
            "--no-" + param.name.replace("_", "-"), dest=param.name, action="store_false",
            default=None, **extra,
        )  # fmt: skip
    elif param.cli == "opts":
        parser.add_argument(flag, dest=param.name, action="append", default=None, **extra)
    elif param.kind is list:  # a comma list
        parser.add_argument(flag, dest=param.name, default=None, **extra)
    else:
        default = None if param.required else param.default
        parser.add_argument(
            flag, dest=param.name, type=_scalar(param.kind), default=default,
            required=param.required, **extra,
        )  # fmt: skip


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="llm-queue", description=__doc__.splitlines()[0])
    parser.add_argument("--actor", default=None, help="who makes a change (audit)")
    sub = parser.add_subparsers(dest="command", required=True)
    groups: dict[str, Any] = {}
    for op in OPERATIONS:
        if op.cli is None:
            continue
        if len(op.cli) == 1:
            leaf = sub.add_parser(op.cli[0], help=op.summary)
        else:
            group = groups.get(op.cli[0])
            if group is None:
                group = sub.add_parser(op.cli[0]).add_subparsers(dest="action", required=True)
                groups[op.cli[0]] = group
            leaf = group.add_parser(op.cli[1], help=op.summary)
        leaf.set_defaults(admin_op=op)  # not `op`: `audit list --op` is a parameter
        for param in op.params:
            _add_param(leaf, param)
        if op.dry_run_default is not None:
            leaf.add_argument("--yes", action="store_true", help="do it (default: a dry run)")
    return parser


def arguments(op: Op, args: argparse.Namespace) -> dict[str, Any]:
    """The method's keyword arguments from parsed arguments (actor and dry run excluded)."""
    out: dict[str, Any] = {}
    for param in op.params:
        value = getattr(args, param.name, None)
        if param.cli == "pairs":
            value = _pairs(value or ())
        elif param.kind is list and isinstance(value, str):
            value = [v.strip() for v in value.split(",") if v.strip()]
        if value is None and not param.required:
            continue
        out[param.name] = value
    return out


async def run(admin: Any, op: Op, args: argparse.Namespace, actor: str | None) -> Any:
    """Run one parsed command on `admin`; returns what to print."""
    kwargs = arguments(op, args)
    if op.writes:
        kwargs["actor"] = actor or default_actor()
        if op.dry_run_default is not None:
            kwargs["dry_run"] = not args.yes
    result = admin.method(op)(**kwargs)
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return {"error": "not found (unknown or expired)"}
    return result


def exit_code(result: Any) -> int:
    """The exit code of a printed result: 1 for an error document, 3 for a drain timeout.

    `requests get ID` printed `{"error": ...}` and a drain that timed out printed `"drained":
    false`, both with exit 0, so a script waiting on them carried on (FO-8).
    """
    if isinstance(result, Mapping):
        if "error" in result:
            return 1
        if result.get("drained") is False:
            return EXIT_DRAIN_TIMEOUT
    return 0


async def amain(argv: Sequence[str], environ: Mapping[str, str] | None = None) -> Any:
    """Parse and run one command; returns its JSON-able result."""
    from llm_queue.engine.admin import AdminServices

    args = _parser().parse_args(list(argv))
    op: Op = args.admin_op
    if not op.store:  # `registry schema`: editors and CI run it with no settings or store
        from llm_queue.registry.json_schema import registry_schema

        return registry_schema()
    env = dict(os.environ if environ is None else environ)
    async with AdminServices.from_env(env) as admin:
        return await run(admin, op, args, args.actor)


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
