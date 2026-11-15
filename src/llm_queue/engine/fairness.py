"""Per-tenant fairness: which tenant a call belongs to and what the admission script enforces.

`fairness_for` turns a model's `fairness` setting and a call's ids into the `Fairness` part of
an admission ask (None when the mode is `off`, so the script runs exactly as without it). The
tenant is `tenant_id`, else `user_id`, from `request_context`; the store only ever sees its
keyed hash. Owner decisions Q28, Q29 and Q50; see docs/explanation/fairness.md.
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


def tenant_of(tenant_id: str | None, user_id: str | None) -> str:
    """The tenant a call counts under: its tenant id, else its user id, else the shared one."""
    return tenant_id or user_id or NO_TENANT


def fairness_for(
    setting: Mapping[str, Any],
    tenant_id: str | None,
    user_id: str | None,
    hash_id: Callable[[str], str],
    *,
    burst: float = 0.1,
    window: str = "smooth",
    round_: float | None = None,
) -> Fairness | None:
    """The `Fairness` of one call under a model's resolved `fairness` setting; None when off.

    The raw tenant id is looked up in `weights` / `quotas` here, in the worker, and only its
    keyed hash (`hash_id`) goes to the store.
    """
    mode = setting.get("mode", "off")
    if mode not in MODES:
        return None
    raw = tenant_of(tenant_id, user_id)
    weight = float(setting.get("default_weight", 1))
    rpm = tpm = None
    if mode == "share":
        weight = float((setting.get("weights") or {}).get(raw, weight))
    elif mode == "quota":
        own = (setting.get("quotas") or {}).get(raw) or {}
        rpm = own.get("rpm", setting.get("rpm"))
        tpm = own.get("tpm", setting.get("tpm"))
    return Fairness(
        mode=mode,
        tenant=hash_id(raw),
        weight=weight,
        rpm=None if rpm is None else int(rpm),
        tpm=None if tpm is None else int(tpm),
        active_ms=int(float(setting.get("active_secs", 60)) * 1000),
        burst=float(burst),
        window=str(window),
        round=round_ if mode == "fair_queue" else None,
    )
