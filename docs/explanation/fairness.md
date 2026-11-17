# Tenant fairness

How one model's budget is split between tenants, so one busy tenant cannot take all of it.
Off by default: with `fairness.mode` unset or `off`, admission runs exactly as before.

## The tenant of a call

A call's tenant is its `tenant_id`, else its `user_id`, from `request_context(...)` or the
call's own arguments. A call with neither counts under one shared tenant, so calls without ids
are not exempt. Set `tenant_id` in trusted code (a router, after authentication): a caller that
sets its own id can claim another tenant's share. The same applies to `priority=True`.

The raw id never leaves the caller (0.2.0, owner D1). The call's message, its dead letter and
the store keys hold only its keyed hash (`LLMQ_ID_HASH_SALT`, the same hash as sticky keys and
status records); worker-side hooks (balancer, fallback policy, sticky key) see the hashes too,
only the caller's `admission` hook sees the raw ids. Metrics carry a `tenant` label only for
the tenants listed in `LLMQ_METRICS_TENANTS` (by a label you choose), and `status()` shows
counts and hashes. Raw ids appear only where the operator writes them: `weights` and `quotas`
in the registry file, and those names are replaced by their hashes when the file is read
(owner D2), so only hashes reach the store. A live edit (`registry set`) must name the hash
(`llm-queue tenants hash NAME`).

The request index (`P:{m:ALIAS}:tq:H`, 0.2.0) uses the tenant id only: a call with no tenant
is indexed under `-`, never under its user id, because an admin "tenant" is the app's tenant.
For a call with a tenant both hashes are the same.

## Three modes

Set in `defaults` or per model; the most specific level wins, key by key (*merged*).

```json
"fairness": {"mode": "share", "weights": {"acme": 3}, "default_weight": 1, "active_secs": 60}
"fairness": {"mode": "quota", "rpm": 60, "tpm": 200000, "quotas": {"acme": {"rpm": 600}}}
"fairness": {"mode": "fair_queue"}
```

| Mode | Rule | When a tenant is over |
|---|---|---|
| `share` | Each active tenant gets `weight / sum of active weights` of the model's rpm, tpm and slots (confirmed, usable deployments). Unused share is lent | Waits (`reason` `tenant`) only while a tenant **within** its share is waiting; otherwise it borrows |
| `quota` | Hard caps per tenant: `rpm` and `tpm` (or `quotas[tenant]`), no borrowing, in the model's `rate_window` and `rate_burst` | Fails at once: `CallFailed("rate_limited")` with `retry_after_secs` until its clock has room |
| `fair_queue` | No caps. Waiting calls are served round robin between tenants | Never refused for the tenant; it only waits its turn |

**share.** A tenant is active while it asked within `active_secs` (60). Each tenant has a GCRA
clock per budget at its share of the model's budget, and a count of its slots in use. A tenant
inside its share is admitted as before (the model's own budgets still apply); refused for budget,
it is recorded as waiting within its share. A tenant over its share is admitted only when no such
call is waiting, so idle capacity is lent and taken back as soon as someone within their share
needs it. A borrower's clock still runs, at most one minute ahead, so it yields to others for
that long once they come back.

**quota.** The tenant's own `rpm` and `tpm` clocks, charged at grant and corrected at finish
like the deployment budgets. The refusal is final and immediate: a hard cap is not worth
holding the class for (a held call stops the worker reading that class of the model).

**fair_queue.** Each call gets a round on its first admission: one more than the larger of
the round last served and its tenant's last round. Waiting calls reserve budget in round order
instead of queue order, so a tenant that sent 100 calls gets one in every round, like anyone
else. A tenant's lead is capped at 1000 rounds. The round only orders calls that reached
admission (one held call per worker and class); calls still in the stream are read in order.

## Cost and keys

Every step is O(1) or O(log n) in the number of tenants: no per-call scan over tenants.

| Key (under the model's tag `{m:ALIAS}`) | Type | Holds | Expiry |
|---|---|---|---|
| `tf:t:<hash>` | hash | the tenant's clocks (`r_`, `t_` fields) and last round `fq` | 3 min, or 2 x `active_secs`, after its last write |
| `tf:s:<hash>` | zset | the tenant's slots in use, scored by lease end; renewed by the heartbeat | 2 leases (at least 3 min) after its last write |
| `tf:act` | zset | active tenants by their last ask; 16 stale ones leave per ask | 2 x `active_secs` |
| `tf:w` | hash | the active tenants' weights and their running sum `~sum` | 2 x `active_secs` |
| `tf:need` | zset | calls refused while within their share, by waiter expiry | 2 leases (at least 3 min) |
| `tf:fq` | hash | `v`, the round served last | one day |

The keys follow the existing admission scripts: one Lua script, no `KEYS`, several hash tags
per call, so they are **not cluster-safe** (FL-14), like the rest of admission.

Off adds no store work beyond one `HMGET` in FINISH and one `HGET` per renewed grant. Measured
2026-11-15 on a local Valkey 8.1, server time per admission script call (`INFO commandstats`,
6,000 PICK_TAKE + FINISH cycles per variant, interleaved, 100 tenants):

| Variant | us per script call | vs before |
|---|---|---|
| before the feature | 287.0 | |
| off | 288.0 | +0.3 % (noise) |
| `fair_queue` | 312.2 | +9 % |
| `quota` | 325.3 | +13 % |
| `share` | 353.4 | +23 % |

## Watching it

- `llmq_tenant_refusals_total{model, mode}`: quota refusals, and calls that waited for their
  share, once per call. `llmq_admission_refusals_total{reason="tenant"}` counts share waits too.
- `status()` per model: `fairness: {mode, active_tenants, waiting_within_share}` (counts only,
  shown when the mode is on).
- The registry warns about a cap above the model's whole budget (it never binds), a `tpm` cap
  below one call's output reservation (the tenant runs one call at a time), and settings the
  mode never reads; a `quota` with no cap is refused.
