"""The admin side of requests (0.2.0 T): the request index settings and the `RequestsAdmin` service.

Calls are indexed per model and tenant (`P:{m:ALIAS}:tq:H`, see `requests.py` `_INDEX_FN`), so an
operator can list, count, cancel, reprioritize and extend one tenant's calls on one model
without touching another tenant's. Tenants are named by their keyed hash only (`LLMQ_ID_HASH_SALT`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from llm_queue.engine.settings import InfraSettings


@dataclass(frozen=True, slots=True)
class AdminSettings:
    """The request index and admin operation settings (`LLMQ_*`, design 0.2.0 section 7).

    `request_index`: calls are indexed per model and tenant (`LLMQ_REQUEST_INDEX_ENABLED`).
    `tenant_names`: tenant names are stored beside their hashes (`LLMQ_TENANT_NAMES_ENABLED`).
    `batch`: ids per pipeline or cancel script (`LLMQ_ADMIN_BATCH`); `scan_max`: index entries
    or messages one operation reads (`LLMQ_ADMIN_SCAN_MAX`); `list_max`: the largest list page
    (`LLMQ_REQUESTS_LIST_MAX`); `move_batch` / `move_bytes`: messages and bytes per reprioritize
    script run (`LLMQ_REQUESTS_MOVE_BATCH` / `_BYTES`); `metrics_tenants`: (tenant name, label)
    pairs that get a `tenant` metric label of their own (`LLMQ_METRICS_TENANTS`).
    """

    request_index: bool = True
    tenant_names: bool = False
    batch: int = 500
    scan_max: int = 100_000
    list_max: int = 1_000
    move_batch: int = 50
    move_bytes: int = 8_388_608
    metrics_tenants: tuple[tuple[str, str], ...] = ()

    @classmethod
    def from_infra(cls, settings: InfraSettings) -> AdminSettings:
        """The admin part of the process settings."""
        return cls(
            request_index=settings.request_index_enabled,
            tenant_names=settings.tenant_names_enabled,
            batch=int(settings.admin_batch),
            scan_max=int(settings.admin_scan_max),
            list_max=int(settings.requests_list_max),
            move_batch=int(settings.requests_move_batch),
            move_bytes=int(settings.requests_move_bytes),
            metrics_tenants=tuple(settings.metrics_tenants),
        )
