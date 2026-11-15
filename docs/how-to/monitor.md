# Monitor

What to alarm on, what to graph, and where to look first when something goes wrong. Every
metric the library emits is in the [table at the end](#every-metric); the names are fixed by
`llm_queue.engine.metrics.METRICS`, and a test fails when code emits a name the table lacks.

## Where metrics go

`LLMQ_METRICS_SINK` picks the sink, one per process:

- `emf` (default): CloudWatch embedded metric format on stdout, namespace `llm-queue`. Points
  are added up per label set and written as one line per label set about once a second
  (counters summed, gauges last value, samples as a value array). Every line has the
  `environment` dimension (`LLMQ_ENVIRONMENT`), so two environments in one AWS account stay
  apart, and the plain fields `pod` and `version`: searchable in Logs Insights, never
  dimensions, so a pod costs no metric.
- `prometheus` (needs the `metrics` extra): `prometheus_client` metrics in the default
  registry. Serve them with `status_router(engine, metrics_route=True)` (`GET /metrics`, open
  like the health routes) or `llm_queue.engine.metrics.prometheus_text(sink)` in your own route.
  Under a forking server each worker process has its own registry: scrape each process, or use
  `prometheus_client`'s multiprocess mode (gauges then need a `multiprocess_mode`).
- `none`: nothing.

Labels come from the registry (model, deployment, account, region, job) or from short fixed
lists (outcome, reason, source); never a request, user or tenant id.

### Who emits what

The `From` column of the table says which process writes a metric:

- `worker` and `caller`: each process, for the calls it ran or sent. Sum across pods.
- `every pod`: each process describes itself (`llmq_ready`, `llmq_store_up`,
  `llmq_heartbeat_age_seconds`, the `llmq_registry_*` gauges). Look at the min, max, or per pod.
- `alerter`: cluster state (queues, budgets, breakers, alerts), set every 30 s by the one
  process that holds the alert lock. The holder keeps the lock while it runs, and every other
  process removes these series from its own `/metrics`, so `max by (model)` is correct in
  Prometheus. In CloudWatch they are points from one writer at a time; use `Maximum`.
- `llmq_alerts_evaluated_timestamp_seconds` comes from whichever process evaluated last: alarm
  when it stops moving (evaluation stopped on every pod).

## Alarms

Start with these. CloudWatch names are the same as the metric names (namespace `llm-queue`,
dimension `environment` plus the labels); in Prometheus, counters gain `_total` as listed.

| Alarm | CloudWatch | Prometheus |
|---|---|---|
| A model has no usable deployment | `llmq_alert_model_unavailable` Maximum >= 1 for 2 of 3 periods of 1 min | `max by (model) (llmq_alert_model_unavailable) == 1` for 2m |
| Dead letters | `llmq_dead_letters` Maximum > 0 | `max by (model) (llmq_dead_letters) > 0` |
| Dead jobs (Q15) | `llmq_alert_job_dead_letters` Maximum >= 1 | `max by (job) (llmq_job_dead_letters) > 0` |
| Queue stalled | `llmq_alert_queue_stalled` Maximum >= 1 | `max by (model) (llmq_oldest_waiting_seconds) > 600` |
| Error ratio | metric math `failed / all` on `llmq_calls_total` by `outcome` > 5 % for 5 min | `sum by (model) (rate(llmq_calls_total{outcome!="ok"}[5m])) / sum by (model) (rate(llmq_calls_total[5m])) > 0.05` |
| Slow provider | `llmq_provider_seconds` p95 > 30 s | `histogram_quantile(0.95, sum by (le, model) (rate(llmq_provider_seconds_bucket[5m]))) > 30` |
| Long waits for budget | `llmq_admission_wait_seconds` p95 > 60 s | `histogram_quantile(0.95, sum by (le, model) (rate(llmq_admission_wait_seconds_bucket[5m]))) > 60` |
| Store trouble | `llmq_store_errors_total` Sum > 0 for 3 periods; `llmq_store_up` Minimum < 1 | `sum(rate(llmq_store_errors_total[5m])) > 0`, `min(llmq_store_up) < 1` |
| Grants lost | `llmq_grants_lost_total` Sum > 0 | `sum(rate(llmq_grants_lost_total[5m])) > 0` |
| Pod on its own config | `llmq_registry_on_own_config` Maximum >= 1 | `max(llmq_registry_on_own_config) == 1` |
| Stored registry refused (Q25) | `llmq_registry_refused_total` Sum > 0 | `increase(llmq_registry_refused_total[10m]) > 0` |
| Not ready on the eviction policy (Q10) | `llmq_store_eviction_unsafe` Maximum >= 1 | `max(llmq_store_eviction_unsafe) == 1` |
| Replicas short (Q9) | `llmq_send_not_replicated_total` Sum > 0 for 5 min | `sum by (scope) (rate(llmq_send_not_replicated_total[5m])) > 0` |
| Credentials | `llmq_credentials_failed_total` Sum > 0 | `sum by (account, region) (increase(llmq_credentials_failed_total[5m])) > 0` |
| Background loop died | `llmq_loop_died_total` Sum > 0 | `increase(llmq_loop_died_total[5m]) > 0` |
| Alert evaluation stopped | `llmq_alerts_evaluated_timestamp_seconds`, missing data treated as breaching, 3 periods | `time() - max(llmq_alerts_evaluated_timestamp_seconds) > 120` |

The quota alerts (`quota_near_limit`, `quota_above_observed`) read per-minute counters: they
turn on at once and off after two clear samples in a row, so they do not flip at each minute
rollover. Their gauges suit dashboards better than paging.

## Dashboards

- Traffic: `llmq_calls_total` by outcome and model; `llmq_caller_failures_total` by outcome
  (failures no worker counted: `queue_full`, `queue_unavailable`, `rejected`, caller deadlines).
- Latency: p50 / p95 / p99 of `llmq_call_seconds` (end to end), `llmq_provider_seconds` (per
  attempt), `llmq_first_token_seconds` (streams), `llmq_wait_seconds` (until a worker took the
  call) and `llmq_admission_wait_seconds` (until its first grant, the wait for budget included).
- Backlog: `llmq_queue_waiting` (no worker took it), `llmq_held_waiting` (taken, waiting for
  budget), `llmq_in_progress`, `llmq_oldest_waiting_seconds`.
- Budget: `llmq_rpm_used / llmq_rpm_limit`, `llmq_tpm_used / llmq_tpm_limit`,
  `llmq_concurrency_in_use / llmq_concurrency_limit` per deployment; `llmq_throttles_1m`
  and `llmq_attempts_total{result="throttled"}` by account and region.
- Routing: `llmq_deployment_usable`, `llmq_breaker_open`, `llmq_kill_switch`,
  `llmq_region_blocked`.
- Pods: `llmq_ready`, `llmq_store_up`, `llmq_held_calls`, `llmq_heartbeat_age_seconds`,
  `llmq_outage_memo_active`, `llmq_build_info` (which version runs where).

## Logs Insights

Each call writes one `llmq_call_finished` record (fields on `record.llmq` when the app renders
them as JSON): request id, model, outcome, path (`queue`, `direct`, `static_cap`,
`dead_letter`), account, region, deployment, attempts, worker, tokens, `queue_wait_ms`,
`admit_wait_ms`, `duration_ms`, `provider_ms`, `error` (cut to 300 characters) and the hashed
user and tenant. No prompt or answer text.

```text
# Tokens by tenant, last day
filter event = "llmq_call_finished" and outcome = "ok"
| stats sum(tokens_in) as input, sum(tokens_out) as output by tenant
| sort output desc

# Failures by deployment
filter event = "llmq_call_finished" and outcome != "ok"
| stats count(*) as failed by outcome, deployment, region
| sort failed desc

# Alert changes
filter event = "llmq_alert"
| fields @timestamp, alert, on, model, deployment, detail
| sort @timestamp desc

# Lines held back during an outage (one line per event and key every 10 s)
filter ispresent(suppressed)
| stats sum(suppressed) as held_back by event
```

Store-outage lines (`receive_failed`, `heartbeat_failed`, `store_down`, `hold_failed`,
`run_failed`, `model_unavailable`) go out at most once per event and key (model or step) every
10 s; the next line carries `suppressed=<n>`. The counters carry the rate.

## Incidents

| Symptom | Look at | Then |
|---|---|---|
| Stuck queue | `llmq_oldest_waiting_seconds`, `llmq_held_waiting`, `llmq_queue_waiting` | Held but not granted: budget (`llmq_rpm_used` vs limit). Not taken: workers (`llmq_ready`, `llmq_loop_died_total`) |
| Lease leak | `llmq_swept_total`, `llmq_stuck_calls_total` | `held_call_stuck` lines name the call and its grants |
| Budget exhausted | `llmq_admission_wait_seconds`, `llmq_rpm_used / llmq_rpm_limit` | `llm-queue status --model M` |
| Throttled region | `llmq_attempts_total{result="throttled"}` by region, `llmq_alert_quota_above_observed` | Block the region (`llm-queue block --region R`) or lower its rpm |
| Store failover | `llmq_store_up`, `llmq_store_errors_total`, `llmq_outage_memo_active` | `/health/ready` body per pod; `llmq_store_resets_total` after it |
| Credentials | `llmq_credentials_failed_total` by account, region | `account_credentials_failed` lines carry `error_type` and the message |
| Dead loop | `llmq_loop_died_total` by loop | `/health/live` body names it (`reason`); `background_loop_died` line |
| Slow provider | `llmq_provider_seconds` p95 by region | `llmq_breaker_open`, `deployment_disabled` |
| Dead letters | `llmq_dead_letters`, `llmq_job_dead_letters` | `llm-queue dlq list --model M` (and `--priority`) |
| Cancels | `llmq_cancels_total` by source | `call_cancel` lines carry `source` and the request id |

## Every metric

Counters drop the `_total` suffix in `prometheus_client`'s own name and add it back in the
exposition. Histograms of waits use buckets from 0.05 s to 600 s; call and provider durations
from 0.1 s to 600 s. Alert gauges are named `llmq_alert_<name>`, labels `model` (and
`deployment` for deployment alerts, `job` for `job_dead_letters`), value 1 while on.

| Name | Kind | Labels | Unit | From | Meaning |
|---|---|---|---|---|---|
| `llmq_calls_total` | counter | model, account, region, outcome | Count | worker | Calls finished by a worker, by outcome (ok or the error kind); account and region of the last deployment tried, '-' when none was |
| `llmq_caller_failures_total` | counter | model, outcome | Count | caller | Every failure a caller saw, by kind: also those no worker counts (rejected, queue_full, queue_unavailable, a caller-side deadline) |
| `llmq_attempts_total` | counter | model, account, region, result | Count | worker | Provider attempts by result: ok, throttled, retryable, model_error, auth, target, or the request error kind |
| `llmq_tokens_input_total` | counter | model, account, region | Count | worker | Input tokens of successful calls |
| `llmq_tokens_output_total` | counter | model, account, region | Count | worker | Output tokens of successful calls |
| `llmq_fallback_total` | counter | kind, model | Count | worker | Successful calls served by a fallback |
| `llmq_wait_seconds` | histogram | model, priority | Seconds | worker | Queue wait: from send until a worker took the call (budget hold not included) |
| `llmq_admission_wait_seconds` | histogram | model, priority | Seconds | worker | From send until the call's first grant: queue wait plus the wait for budget |
| `llmq_call_seconds` | histogram | model, outcome | Seconds | worker | From send until the call finished, by outcome |
| `llmq_provider_seconds` | histogram | model, account, region | Seconds | worker | Duration of each provider attempt |
| `llmq_first_token_seconds` | histogram | model, account, region | Seconds | worker | Streams: from the attempt's start to its first chunk |
| `llmq_admission_refusals_total` | counter | model, reason | Count | worker | Calls refused by admission, once each |
| `llmq_retries_total` | counter | model, reason | Count | worker | Retries after a failed attempt, by error kind |
| `llmq_grants_lost_total` | counter | model, step | Count | worker | Grants found gone at renew or finish |
| `llmq_store_errors_total` | counter | where | Count | every pod | Store errors by step |
| `llmq_bad_messages_total` | counter | model | Count | worker | Messages that could not be read (dead-lettered) |
| `llmq_newer_messages_total` | counter | model | Count | worker | Messages from a newer library, given back |
| `llmq_swept_total` | counter | model | Count | worker | Lapsed leases and in-progress entries the sweep removed |
| `llmq_reclaimed_total` | counter | model | Count | worker | Messages taken over from a worker that went quiet |
| `llmq_stuck_calls_total` | counter | model | Count | worker | Held calls freed because nothing worked on them |
| `llmq_store_resets_total` | counter | - | Count | worker | State store resets (lost keys) this pod detected |
| `llmq_credentials_failed_total` | counter | account, region | Count | worker | Credential or auth failures |
| `llmq_loop_died_total` | counter | loop | Count | every pod | Background loops that ended with an error |
| `llmq_jobs_no_consumer_total` | counter | job | Count | caller | Jobs sent with no consumer group |
| `llmq_job_ack_failed_total` | counter | job | Count | worker | Job acknowledgements that failed |
| `llmq_registry_reload_errors_total` | counter | error | Count | every pod | Registry reloads that failed |
| `llmq_registry_refused_total` | counter | reason | Count | every pod | Stored registry documents this pod refused (owner Q25) |
| `llmq_send_not_replicated_total` | counter | scope | Count | caller | Writes that reached fewer replicas than asked, sent anyway (owner Q9) |
| `llmq_cancels_total` | counter | source | Count | caller | Cancels asked, by source: api, caller_task, fail_fast, admin, cli, http, other |
| `llmq_outage_memo_set_total` | counter | - | Count | every pod | Times this pod started remembering a store outage (owner Q11) |
| `llmq_store_pool_busy_total` | counter | model | Count | worker | Calls refused because the store pool was full |
| `llmq_pubsub_resubscribes_total` | counter | reader | Count | every pod | Stale subscribers reset |
| `llmq_stale_acks_total` | counter | model | Count | worker | Acks refused: another worker took the message over |
| `llmq_jobs_total` | counter | job, outcome | Count | worker | Jobs finished by outcome (ok, failed, dead, ...) |
| `llmq_hook_failed_total` | counter | kind, hook | Count | every pod | App hooks that raised |
| `llmq_queue_waiting` | gauge | model, priority | Count | alerter | Calls in the queue no worker has taken |
| `llmq_held_waiting` | gauge | model, priority | Count | alerter | Calls a worker took that still wait for budget (a grant) |
| `llmq_in_progress` | gauge | model | Count | alerter | Calls running at a provider |
| `llmq_dead_letters` | gauge | model | Count | alerter | Dead letters, priority and normal queues together |
| `llmq_oldest_waiting_seconds` | gauge | model | Seconds | alerter | Age of the oldest call in the model's queues |
| `llmq_breaker_open` | gauge | model, deployment | None | alerter | 1 when the deployment's breaker is open |
| `llmq_deployment_usable` | gauge | model, deployment | None | alerter | 1 when the deployment can take calls (enabled, unblocked, healthy) |
| `llmq_rpm_used` | gauge | model, deployment | Count | alerter | Requests used in the current minute |
| `llmq_rpm_limit` | gauge | model, deployment | Count | alerter | The deployment's RPM limit |
| `llmq_tpm_used` | gauge | model, deployment | Count | alerter | Tokens used in the current minute |
| `llmq_tpm_limit` | gauge | model, deployment | Count | alerter | The deployment's TPM limit |
| `llmq_concurrency_in_use` | gauge | model, deployment | Count | alerter | Calls holding a concurrency slot |
| `llmq_concurrency_limit` | gauge | model, deployment | Count | alerter | The adaptive concurrency limit now |
| `llmq_throttles_1m` | gauge | model, deployment | Count | alerter | Provider throttles, the larger of last and this minute |
| `llmq_provider_latency_seconds` | gauge | model, deployment | Seconds | alerter | Smoothed provider latency from the health hash |
| `llmq_kill_switch` | gauge | - | None | alerter | 1 while the routing kill switch is on |
| `llmq_region_blocked` | gauge | region | None | alerter | 1 while the region is blocked by routing |
| `llmq_job_dead_letters` | gauge | job | Count | alerter | Dead jobs kept, per job name (owner Q15) |
| `llmq_job_waiting` | gauge | job | Count | alerter | Jobs waiting, per job name |
| `llmq_alerts_evaluated_timestamp_seconds` | gauge | - | Seconds | alerter | Unix time of the last alert evaluation (alarm when it stops moving) |
| `llmq_registry_invalid_reads` | gauge | - | Count | every pod | Stored registry reads refused as invalid |
| `llmq_registry_stored_refused` | gauge | - | None | every pod | 1 when the stored registry was refused |
| `llmq_registry_lib_too_old` | gauge | - | None | every pod | 1 when the stored registry needs a newer library |
| `llmq_registry_on_own_config` | gauge | - | None | every pod | 1 when this pod runs on its own config |
| `llmq_build_info` | gauge | version | None | every pod | Always 1; the library version as a label |
| `llmq_store_up` | gauge | store | None | every pod | 1 when the store takes writes, 0 when down or read-only |
| `llmq_held_calls` | gauge | - | Count | every pod | Calls this pod holds (waiting for budget or running) |
| `llmq_outage_memo_active` | gauge | - | None | every pod | 1 while this pod remembers a store outage (owner Q11) |
| `llmq_store_eviction_unsafe` | gauge | store | None | every pod | 1 when the store's eviction policy may evict queue keys (owner Q10) |
| `llmq_ready` | gauge | - | None | every pod | 1 when this pod is ready (0: draining, no registry, or evicting store) |
| `llmq_heartbeat_age_seconds` | gauge | - | Seconds | every pod | Seconds since this pod's heartbeat last made progress |
