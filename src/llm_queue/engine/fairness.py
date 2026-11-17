"""Per-tenant fairness: which tenant a call belongs to and what the admission script enforces.

`fairness_for` turns a model's `fairness` setting and a call's tenant into the `Fairness` part
of an admission ask (None when the mode is `off`, so the script runs exactly as without it).
The tenant is `tenant_id`, else `user_id`, from `request_context`, as the keyed hashes the
call's message carries (owner D1, 0.2.0); the registry's tenant names are hashed when the boot
document is read (D2). Owner decisions Q28, Q29 and Q50; see docs/explanation/fairness.md.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

Mode = Literal["share", "quota", "fair_queue"]
MODES: tuple[Mode, ...] = ("share", "quota", "fair_queue")
# Calls with neither a tenant nor a user id share one tenant, so they are not exempt.
NO_TENANT = "\x00no-tenant"


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

    Run on the boot registry (`LLMQ_REGISTRY` / `LLMQ_REGISTRY_FILE`) when it is read: the
    file names tenants, the store and the workers only ever see their keyed hashes, which is
    what a call's message carries. Levels `defaults` and `models.*` (where `fairness` lives).
    Anything that is not the expected shape is left for the registry's own checks.
    """
    if not isinstance(document, dict):
        return document
    out = dict(document)
    blocks: list[tuple[dict[str, Any], str]] = []
    if isinstance(out.get("defaults"), dict):
        out["defaults"] = dict(out["defaults"])
        blocks.append((out["defaults"], "fairness"))
    if isinstance(out.get("models"), dict):
        out["models"] = dict(out["models"])
        for alias, body in out["models"].items():
            if isinstance(body, dict):
                out["models"][alias] = dict(body)
                blocks.append((out["models"][alias], "fairness"))
    for parent, name in blocks:
        fairness = parent.get(name)
        if not isinstance(fairness, dict):
            continue
        fairness = dict(fairness)
        for part in ("weights", "quotas"):
            table = fairness.get(part)
            if isinstance(table, dict):
                fairness[part] = {
                    (hash_id(k) if isinstance(k, str) and k else k): v for k, v in table.items()
                }
        parent[name] = fairness
    return out


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
