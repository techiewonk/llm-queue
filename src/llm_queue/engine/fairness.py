"""Per-tenant fairness: which tenant a call belongs to and what the admission script enforces.

`fairness_for` turns a model's `fairness` setting and a call's tenant into the `Fairness` part
of an admission ask (None when the mode is `off`, so the script runs exactly as without it).
The tenant is `tenant_id`, else `user_id`, from `request_context`, as the keyed hashes the
call's message carries (owner D1, 0.2.0); the registry's tenant names are hashed on every
registry write (D2, review RQ-5). Owner decisions Q28, Q29 and Q50; see
docs/explanation/fairness.md.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from llm_queue.errors import ConfigError

Mode = Literal["share", "quota", "fair_queue"]
MODES: tuple[Mode, ...] = ("share", "quota", "fair_queue")
# Calls with neither a tenant nor a user id share one tenant, so they are not exempt.
NO_TENANT = "\x00no-tenant"
# The tables of a `fairness` block keyed by tenant, and the mark of a block whose keys are
# already hashes (`tenant_keys: hashed`, written by every registry write path).
TENANT_TABLES = ("weights", "quotas")
HASHED = "hashed"
_HASH_RE = re.compile(r"^[0-9a-f]{16}\Z")
# `[...].fairness[.weights|.quotas[.rest]]` at the start of a path or after a dot.
_FAIRNESS_PATH = re.compile(r"^((?:.*\.)?)fairness(?:\.(weights|quotas)(?:\.(.+))?)?\Z")


@dataclass(frozen=True, slots=True)
class Fairness:
    """The fairness part of one ask: the mode, the tenant's hash and its numbers.

    `weight` (share), `rpm` / `tpm` (quota caps), `active_ms` (how long a tenant counts as
    active), `burst` and `window` (the model's `rate_burst` and `rate_window`) and `round`
    (fair_queue: the call's round from an earlier ask, so it keeps its place).
    """

    mode: Mode
    tenant: str
    weight: float = 1.0
    rpm: int | None = None
    tpm: int | None = None
    active_ms: int = 60_000
    burst: float = 0.1
    window: str = "smooth"
    round: float | None = None

    def body(self) -> dict[str, Any]:
        """The `tf` field of the admission script's request (short names, no raw ids)."""
        out: dict[str, Any] = {
            "m": self.mode,
            "h": self.tenant,
            "w": self.weight,
            "act": self.active_ms,
            "bf": self.burst,
            "win": self.window,
        }
        if self.rpm is not None:
            out["rpm"] = self.rpm
        if self.tpm is not None:
            out["tpm"] = self.tpm
        if self.round is not None:
            out["fr"] = self.round
        return out


def tenant_hash_of(
    tenant_hash: str | None, user_hash: str | None, hash_id: Callable[[str], str]
) -> str:
    """The tenant a call counts under, as a keyed hash: its tenant's, else its user's.

    Messages carry only keyed hashes of the tenant and user ids (owner D1, 0.2.0), so the
    worker never sees a raw id. Calls with neither share one tenant (the hash of `NO_TENANT`).
    """
    return tenant_hash or user_hash or hash_id(NO_TENANT)


def hash_tenant_names(document: Any, hash_id: Callable[[str], str]) -> Any:
    """`document` with the tenant names of every `fairness.weights` / `quotas` hashed (D2).

    Run on every registry write path (the boot document, `registry set`, publish, rollback):
    the file names tenants, the store and the workers only ever see their keyed hashes, which
    is what a call's message carries. A hashed block says so (`tenant_keys: hashed`), so an
    exported document is never hashed twice; a 16-hex name in a block of names is refused
    (`ConfigError`: it reads as a hash). Levels `defaults` and `models.*`. Anything that is
    not the expected shape is left for the registry's own checks.
    """
    if not isinstance(document, dict):
        return document
    out = dict(document)
    blocks: list[tuple[dict[str, Any], str]] = []
    if isinstance(out.get("defaults"), dict):
        out["defaults"] = dict(out["defaults"])
        blocks.append((out["defaults"], "defaults.fairness"))
    if isinstance(out.get("models"), dict):
        out["models"] = dict(out["models"])
        for alias, body in out["models"].items():
            if isinstance(body, dict):
                out["models"][alias] = dict(body)
                blocks.append((out["models"][alias], f"models.{alias}.fairness"))
    for parent, path in blocks:
        if isinstance(parent.get("fairness"), dict):
            parent["fairness"] = hash_fairness(parent["fairness"], hash_id, path)
    return out


def hash_fairness(block: Mapping[str, Any], hash_id: Callable[[str], str], path: str) -> Any:
    """One `fairness` block with its tenant names hashed and marked `tenant_keys: hashed`.

    A block already marked keeps its keys (they must all be hashes); a block of names must not
    hold a 16-hex key. A block with no tenant table is returned unchanged.
    """
    out = dict(block)
    tables = [part for part in TENANT_TABLES if isinstance(out.get(part), dict)]
    if not tables:
        return out
    hashed = out.get("tenant_keys") == HASHED
    for part in tables:
        for key in out[part]:
            if not isinstance(key, str) or not key:
                continue
            if hashed and not is_tenant_hash(key):
                raise ConfigError(
                    f"{path}.{part}: `{key}` is not a tenant hash, but the block says "
                    "tenant_keys: hashed (give names in a block without it)"
                )
            if not hashed and is_tenant_hash(key):
                raise ConfigError(
                    f"{path}.{part}: `{key}` looks like a tenant hash; name the tenant (it is "
                    "hashed here), or mark the block tenant_keys: hashed"
                )
        if not hashed:
            out[part] = {
                (hash_id(k) if isinstance(k, str) and k else k): v for k, v in out[part].items()
            }
    out["tenant_keys"] = HASHED
    return out


def hash_tenant_path(path: str, value: Any, hash_id: Callable[[str], str]) -> list[tuple[str, Any]]:
    """A live edit's dotted path and value with the tenant names in it hashed (RQ-5).

    `fairness.weights.<name>`, `fairness.quotas.<name>[.rpm|.tpm]`: the name is hashed;
    `fairness.weights` / `quotas` with an object: its keys; `fairness` with an object: the
    block (`hash_fairness`). A path that hashed a name also marks its block `tenant_keys:
    hashed`. A 16-hex name is refused (`ConfigError`), as in a document. Other paths come
    back as given.
    """
    match = _FAIRNESS_PATH.search(path)
    if match is None:
        return [(path, value)]
    head, table, rest = match.group(1), match.group(2), match.group(3)
    mark = (f"{head}fairness.tenant_keys", HASHED)
    if table is None:
        if isinstance(value, Mapping):
            return [(path, hash_fairness(value, hash_id, path))]
        return [(path, value)]
    if rest is None:
        if isinstance(value, Mapping):
            block = hash_fairness({table: dict(value)}, hash_id, path)
            return [(path, block[table]), mark]
        return [(path, value)]
    name, field_ = rest, ""
    if table == "quotas" and rest.rsplit(".", 1)[-1] in ("rpm", "tpm") and "." in rest:
        name, field_ = rest.rsplit(".", 1)
    if is_tenant_hash(name):
        raise ConfigError(
            f"{path}: `{name}` looks like a tenant hash; give the tenant's name (it is hashed here)"
        )
    hashed = f"{head}fairness.{table}.{hash_id(name)}" + (f".{field_}" if field_ else "")
    return [(hashed, value), mark]


def is_tenant_hash(text: str) -> bool:
    """True for 16 lowercase hex characters: the form of `keyed_hash`."""
    return bool(_HASH_RE.match(text))


def fairness_for(
    setting: Mapping[str, Any],
    tenant: str,
    *,
    burst: float = 0.1,
    window: str = "smooth",
    round_: float | None = None,
) -> Fairness | None:
    """The `Fairness` of one call under a model's resolved `fairness` setting; None when off.

    `tenant` is the call's tenant hash (`tenant_hash_of`); `weights` / `quotas` are keyed by
    hashes too (`hash_tenant_names` at load), so no raw name is needed anywhere here.
    """
    mode = setting.get("mode", "off")
    if mode not in MODES:
        return None
    weight = float(setting.get("default_weight", 1))
    rpm = tpm = None
    if mode == "share":
        weight = float((setting.get("weights") or {}).get(tenant, weight))
    elif mode == "quota":
        own = (setting.get("quotas") or {}).get(tenant) or {}
        rpm = own.get("rpm", setting.get("rpm"))
        tpm = own.get("tpm", setting.get("tpm"))
    return Fairness(
        mode=mode,
        tenant=tenant,
        weight=weight,
        rpm=None if rpm is None else int(rpm),
        tpm=None if tpm is None else int(tpm),
        active_ms=int(float(setting.get("active_secs", 60)) * 1000),
        burst=float(burst),
        window=str(window),
        round=round_ if mode == "fair_queue" else None,
    )
