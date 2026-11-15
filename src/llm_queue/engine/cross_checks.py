"""Start-up cross-checks of the registry against this process's settings and `Tuning` (R3CF-10).

The registry validator checks a document on its own (`fallback.after_secs` against the call's
timeout, `queue.retention_secs` against it); it cannot see the process's client timeouts or
TTLs, which differ per pod. These pairs are compared here once the registry is loaded, at
`LLMQueue.start()`. They are warnings: the registry is live and shared, so a pod never refuses
to start over a document other pods run on; a pair that is wrong in the process settings alone
is a `ConfigError` in `Tuning.problems` instead.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from llm_queue.registry import ResolvedRegistry


def start_warnings(
    view: ResolvedRegistry,
    *,
    read_timeouts: Mapping[str, float],
    status_ttl_secs: float,
) -> list[str]:
    """One line per pair that cannot work as set, naming both values (empty when all is well).

    `read_timeouts` maps a provider name (`bedrock`, `openai`) to its client's read timeout
    (`Tuning.bedrock` / `Tuning.openai`); models of other providers skip that check.
    """
    out: list[str] = []
    for alias, entry in view.registry.models.items():
        deployments = view.deployments(alias)
        attempt = max(
            (float(view.value(alias, "attempt_timeout_secs", d.key)) for d in deployments),
            default=float(view.value(alias, "attempt_timeout_secs")),
        )
        read = read_timeouts.get(entry.provider)
        if read is not None and attempt > read:
            # The client cuts a non-stream call at its read timeout, not at the attempt timer.
            out.append(
                f"models.{alias}: attempt_timeout_secs ({attempt:g}) is above "
                f"{entry.provider}.read_timeout_secs ({read:g}) of Tuning: a non-stream attempt "
                f"is cut by the client at {read:g} s"
            )
        timeout = max(
            (float(view.value(alias, "timeout_secs", d.key)) for d in deployments),
            default=float(view.value(alias, "timeout_secs")),
        )
        deadline = min(timeout, float(view.value(alias, "timeout_max_secs")))
        if deadline > status_ttl_secs:
            out.append(
                f"models.{alias}: timeout_secs ({deadline:g}) is above LLMQ_STATUS_TTL_SECS "
                f"({status_ttl_secs:g}): a call still waiting then loses its status record"
            )
    return out
