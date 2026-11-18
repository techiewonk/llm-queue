# Log events

Every structured log event the library writes, by name. Each line is `event key=value ...`
(structlog key-value format; the app configures the renderer). Use the name to search and alarm;
the fields are listed in [Monitor](../how-to/monitor.md) for the ones worth alarming on.
`llmq_admin` is written once per audited admin write (`kind`, `actor`, `op`, `path`, `after`:
counts, never a tenant name), purges included (which also write `llmq_purge`); `call_cancel` carries `source` (`api`, `admin`, `purge`, ...).

| Event | Level | Where |
|---|---|---|
| `account_credentials_failed` | error | engine/runtime.py, engine/local.py |
| `admin_routes_unauthenticated` | warning | engine/http.py |
| `ack_stale_receipt` | warning | engine/runtime.py |
| `admission_config_missing` | error | engine/runtime.py |
| `alerts_failed` | warning | engine/runtime.py |
| `audit_entry_unreadable` | warning | registry/store.py |
| `background_loop_died` | error | engine/runtime.py |
| `background_task_failed` | warning | context.py (a `create_background_task` task raised) |
| `background_thread_failed` | warning | context.py (a `run_in_thread` function raised) |
| `bad_dead_letter` | warning | engine/runtime.py |
| `bad_message` | warning | engine/runtime.py |
| `bell_failed` | warning | engine/runtime.py |
| `call_cancel` | info | engine/admin/requests.py, engine/purge.py, engine/requests_admin.py, engine/runtime.py |
| `caller_gone` | warning | engine/runtime.py |
| `cancel_after_stop` | warning | engine/runtime.py |
| `cancel_cleanup_failed` | warning | engine/runtime.py |
| `cancel_failed` | warning | engine/runtime.py |
| `cancel_reader_error` | warning | engine/runtime.py |
| `chunk_entry_bad` | warning | engine/streaming.py |
| `chunk_flush_failed` | warning | engine/streaming.py |
| `chunk_reader_error` | warning | engine/streaming.py |
| `chunk_reader_recovered` | info | engine/streaming.py |
| `chunk_reader_wake_failed` | warning | engine/streaming.py |
| `client_build_failed` | warning | clients/pool.py |
| `client_close_failed` | debug | clients/hub.py |
| `client_config_problem` | warning | clients/pool.py |
| `client_pool_resized` | info | clients/pool.py |
| `client_warm_failed` | debug | clients/pool.py |
| `close_step_failed` | warning | engine/runtime.py |
| `config_cross_check` | warning | core.py |
| `config_cross_check_skipped` | debug | core.py |
| `confirm_failed` | warning | engine/runtime.py |
| `dead_letter_reply_failed` | warning | engine/runtime.py |
| `debug_capture_failed` | warning | engine/runtime.py |
| `deployment_disabled` | error | engine/runtime.py, engine/local.py |
| `deployment_failing_last_usable` | error | engine/runtime.py |
| `deployment_recovered` | info | engine/runtime.py |
| `direct_run_after_result_failed` | warning | engine/runtime.py (a step after a direct run stored its success raised; the caller gets the success) |
| `direct_run_failed` | warning | engine/runtime.py |
| `drain_callback_failed` | warning | drain.py |
| `drain_file_seen` | info | drain.py |
| `emf_lines_dropped` | warning | engine/metrics.py |
| `fallback_shape_skipped` | info | engine/runtime.py |
| `finish_failed` | warning | engine/requests.py, engine/runtime.py |
| `finish_skipped` | warning | engine/admission.py |
| `give_back_failed` | warning | engine/runtime.py |
| `grant_for_released_call` | warning | engine/runtime.py |
| `grant_lost` | warning | engine/admission.py |
| `granted_deployment_removed` | warning | engine/runtime.py |
| `grants_reregistered` | warning | engine/runtime.py |
| `held_call_stuck` | error | engine/runtime.py |
| `hold_lost` | warning | engine/runtime.py |
| `hook_failed` | warning | engine/hooks.py |
| `hook_not_registered` | warning | engine/hooks.py |
| `job_ack_failed` | warning | engine/jobs.py |
| `job_ack_gave_up` | error | engine/jobs.py |
| `job_dead_lettered` | error | engine/jobs.py |
| `job_dedupe_not_kept` | warning | engine/jobs.py |
| `job_done` | info | engine/jobs.py |
| `job_duplicate_run` | warning | engine/jobs.py |
| `job_duplicate_send` | info | engine/jobs.py |
| `job_extend_failed` | warning | engine/jobs.py |
| `job_lost_visibility` | warning | engine/jobs.py |
| `job_no_consumer` | warning | engine/jobs.py |
| `job_receive_failed` | warning | engine/jobs.py |
| `job_redelivered_while_running` | warning | engine/jobs.py |
| `job_return_not_counted` | warning | engine/jobs.py |
| `job_returned` | warning | engine/jobs.py |
| `job_task_failed` | warning | engine/jobs.py |
| `job_upkeep_failed` | warning | engine/jobs.py |
| `key_expiry_changed` | warning | engine/runtime.py |
| `key_expiry_conflict` | warning | engine/runtime.py |
| `key_expiry_publish_failed` | warning | engine/runtime.py |
| `llmq_admin` | info | engine/audit.py |
| `llmq_call_finished` | info | engine/runtime.py |
| `llmq_configured` | info | core.py (once per `start()`, every mode: the `describe()` summary: version, worker id, roles, environment, store `scheme://host:port` (no password), prefixes, key expiry, pool, metrics sink, registry version and models, `tenant_default_hash`; never a salt, token, tenant name or registry body) |
| `llmq_configured_failed` | warning | core.py (`describe()` raised at start, e.g. a registry file changed since `from_env`; the start goes on) |
| `llmq_disabled` | info | disabled.py (`LLMQ_ENABLED=false`: once at `start()`, with `describe()`: version, `enabled: false`, environment, whether a drain file is watched) |
| `llmq_purge` | info | engine/purge.py |
| `llmq_registry_audit` | info | registry/store.py |
| `llmq_start_failed` | error | fastapi.py (`on_start_error="log"`: the app stays up, not ready) |
| `local_attempt_failed` | warning | engine/local.py (one provider attempt on the local executor: `local` mode or `store_down: static_cap`, field `path`) |
| `local_mode_started` | info | core.py (`LLMQ_EXECUTION_MODE=local`: process count, concurrency, `budgets` (per model the rpm / tpm this process may use), an `LLMQ_ROLE` that does not apply) |
| `message_needs_newer_library` | warning | engine/runtime.py |
| `metric_dropped` | warning | engine/metrics.py |
| `model_loop_died` | error | engine/runtime.py |
| `offload_close_failed` | warning | core.py |
| `offload_delete_failed` | warning | engine/offload.py, engine/requests.py |
| `offload_get_failed` | warning | engine/requests.py |
| `offload_kms_default_key` | warning | engine/offload.py |
| `offload_lifecycle_not_checked` | info | engine/offload.py, engine/requests.py |
| `offload_missing` | error | engine/requests.py |
| `offload_no_lifecycle_rule` | warning | engine/offload.py |
| `offload_put_failed` | warning | engine/requests.py |
| `openai_aiohttp_unavailable` | warning | clients/pool.py |
| `policy_read_failed` | debug | engine/runtime.py |
| `pool_replan_failed` | warning | engine/runtime.py |
| `prober_failed` | warning | engine/runtime.py |
| `pubsub_stale` | warning | engine/runtime.py |
| `purge_deferred` | warning | engine/purge.py (a due auto purge waits: the stored registry could not be read, was refused, or this process runs on its own config) |
| `purge_failed` | warning | engine/purge.py, engine/runtime.py |
| `purge_scheduled` | info | engine/purge.py |
| `purge_unscheduled` | info | engine/purge.py |
| `queue_store_down` | warning | engine/runtime.py |
| `receive_reclaimed` | warning | engine/runtime.py |
| `redelivered_while_held` | warning | engine/runtime.py |
| `redrive_dropped_bodyless` | warning | engine/queue.py |
| `redrive_stopped_full` | warning | engine/queue.py |
| `registry_edit_refused` | warning | registry/store.py |
| `registry_invalid` | warning | registry/store.py |
| `registry_invalid_replaced` | error | registry/store.py |
| `registry_invalid_restoring_config` | error | registry/store.py |
| `registry_metric_failed` | debug | registry/store.py |
| `registry_needs_newer_library` | warning | registry/store.py |
| `registry_older_ignored` | info | registry/store.py |
| `registry_placeholders` | info | engine/settings.py (the `names` the boot registry's placeholders read; never a value) |
| `registry_published` | info | registry/store.py |
| `registry_reload_failed` | error | registry/store.py |
| `registry_reload_failed` | warning | registry/store.py |
| `registry_reset_live_edits_lost` | error | registry/store.py |
| `registry_restore_failed` | error | registry/store.py |
| `registry_step_failed` | error | engine/runtime.py |
| `registry_store_unreachable_at_boot` | warning | registry/store.py |
| `registry_store_unreachable_at_start` | error | engine/runtime.py |
| `registry_stored_refused_running_on_config` | error | registry/store.py |
| `registry_unreachable` | warning | registry/store.py |
| `registry_version_not_bumped` | warning | registry/store.py |
| `registry_warning` | warning | registry/store.py |
| `release_claims_failed` | warning | engine/runtime.py |
| `release_return_failed` | warning | engine/runtime.py |
| `replicas_short` | warning | engine/store_guard.py |
| `request_extended` | info | engine/requests_admin.py |
| `request_moved` | info | engine/requests_admin.py |
| `request_records_repaired` | warning | engine/requests_admin.py |
| `request_records_stale` | error | engine/requests_admin.py |
| `request_unusable` | error | engine/runtime.py, engine/local.py |
| `result_drop_skipped` | warning | engine/requests.py |
| `result_reader_error` | warning | engine/runtime.py |
| `result_sweep_failed` | warning | engine/runtime.py |
| `return_failed` | warning | engine/runtime.py |
| `second_llm_queue_started` | warning | core.py |
| `secrets_refresh_failed` | warning | secrets.py (the last values are kept) |
| `setting_unknown` | warning | engine/settings.py (an `LLMQ_*` name no setting reads is set, outside strict mode: `setting`) |
| `settle_replayed` | info | engine/runtime.py (the end-of-call script ran again after a lost reply: not a double run) |
| `shutdown_give_back_failed` | warning | engine/runtime.py |
| `start_rollback_failed` | warning | core.py |
| `static_cap_process_count_unset` | warning | core.py (queue mode: `models` with `store_down: static_cap` while `LLMQ_LOCAL_PROCESS_COUNT` is 1) |
| `state_store_promoted` | error | engine/runtime.py |
| `state_store_reset_detected` | error | engine/runtime.py |
| `queue_store_unreachable_at_start` | error | engine/runtime.py |
| `state_store_unreachable_at_start` | error | engine/runtime.py |
| `stop_metrics_flush_failed` | warning | engine/runtime.py, engine/local.py |
| `stop_reclaim_failed` | warning | engine/runtime.py |
| `stop_return_failed` | warning | engine/runtime.py |
| `stop_return_timeout` | warning | engine/runtime.py |
| `stop_step_failed` | warning | core.py |
| `stop_task_failed` | warning | engine/runtime.py |
| `store_check_skipped` | debug | engine/store_checks.py |
| `store_clock_jump` | error | engine/runtime.py |
| `store_eviction_policy` | warning | engine/store_checks.py |
| `store_pool_busy` | warning | engine/store_client.py |
| `store_pool_small` | warning | core.py |
| `store_probe_failed` | warning | engine/runtime.py |
| `stream_call_failed_after_end` | warning | engine/runtime.py |
| `stream_end_failed` | warning | engine/runtime.py |
| `stream_task_failed` | warning | engine/streaming.py |
| `sweep_failed` | warning | engine/runtime.py |
| `sync_call_on_event_loop` | warning | core.py |
| `sync_engine_close_failed` | warning | core.py |
| `ttl_sweep_failed` | warning | engine/runtime.py |
| `undo_skipped` | warning | engine/admission.py |
| `waiting_calls_lost` | warning | engine/runtime.py |
| `warm_clients_failed` | warning | engine/runtime.py |
| `worker_slots_changed_needs_restart` | warning | engine/runtime.py |
