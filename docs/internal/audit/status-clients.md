# Status: clients audit (CL-1 .. CL-21)

Branch `fix/audit-clients` from `dev/0.1.0b2` (3812dc6). Tests named below live in
`tests/unit/clients/test_b2_audit_clients.py` unless another file is given.
Checks at f5b7a55: `ruff check .`, `ruff format --check .`, `mypy` (142 files, clean),
`pytest -q tests/unit` 486 passed, 3 xfailed (strict, CL-4 botocore).

| ID | status | commit | test | note |
|---|---|---|---|---|
| CL-1 | fixed (clients part) | 6a620f5 | `test_a_dropped_stream_is_a_transport_error` | The classifier was already right; the test pins that a mid-stream cut is a transport error on both transports. The `worker_lost` mapping in `engine/runtime.py` belongs to the runtime agent (not touched here). |
| CL-2 | fixed | 6a620f5 | `test_aclose_never_releases_a_retired_lease_twice` | Retire task pops each lease as it releases it (shielded); `aclose` waits for the tasks, then releases only what is left. |
| CL-3 | fixed | 6a620f5 | `test_a_retired_client_waits_for_its_calls` | `Lease.inflight` + `ClientPool.borrow_bedrock` / `borrow_openai` around every provider attempt; retired leases released when idle (1 s grace, 1 h cap). |
| CL-4 | fixed (aiohttp) | f2b4d4a | `test_a_redirect_is_never_followed` | `allow_redirects=False`. The audit's premise is wrong for the botocore transport: aiobotocore follows redirects (aiohttp default, no setting in aiobotocore 2.23-3.x); pinned by a strict xfail. |
| CL-5 | fixed | f691991 | `tests/unit/providers/test_classify_table.py::test_transport_errors_classify_alike_on_both_bedrock_transports` | `_FATAL_TRANSPORT` (TLS, proxy, InvalidURL, TooManyRedirects) checked first; `BadStatusLine` cause counts as a reset. |
| CL-6 | fixed | b50c470 | `test_extra_client_arguments_work_on_both_transports`, `test_a_missing_api_key_fails_over_and_is_listed_at_warm` | Unknown `credentials=` keys: that account falls back to botocore with one warning. Missing `api_key_ref`: `CredentialsUnavailable`; `ClientPool.problems()` + `warm()` log them at start. Note: runtime only passes Bedrock routes to `warm`; passing OpenAI routes is a one-line change in runtime.py (other owner). |
| CL-7 | fixed | b50c470 | `test_no_credentials_is_retried_once_keys_appear` | Empty chain raises `CredentialsUnavailable` before a client is cached (both transports); `nocredentials` / `partialcredentials` are auth families. |
| CL-8 | fixed | b50c470 | `test_a_slow_refresh_is_not_cancelled_by_the_attempt_timer`, `test_the_credentials_callable_does_not_block_the_loop` | Shared shielded refresh task; STS `AioConfig` (10 s read, 2 attempts, unverified values); `credentials` / `secrets` in `to_thread` on async paths. `openai()` stays sync (secret read once per account build). Timeouts still surface as `AttemptTimeout`, not `CredentialsUnavailable`. |
| CL-9 | fixed | 6a620f5 | `test_a_body_cut_is_not_quick_retried` | No quick retry once any stream event arrived (sink or not) or when the chain has `ClientPayloadError` / `ResponseStreamingError` (`is_body_cut`); stale-socket retry kept and tested. |
| CL-10 | fixed | 6a620f5 | `test_a_failing_sink_releases_the_connection_at_once` | `contextlib.aclosing` in `_converse_stream` / `_chat`; `paced` acloses its iterator. |
| CL-11 | fixed | 6a620f5 | `test_a_cancelled_release_stays_cancelled` | `gather(return_exceptions=True)`; close still runs in `finally`. |
| CL-12 | fixed (clients part) | 6a620f5 | mypy (`_fits` check in pool.py) | `BedrockClient` Protocol returned by `ClientPool.bedrock()`; mypy checks `BedrockHTTP` against it. Typing `BedrockOptions.transport` as a `Literal` is in `tuning.py`, outside this scope: hand-off. |
| CL-13 | fixed | f691991 | `test_a_corrupt_stream_frame_is_retryable`, `test_a_reply_that_is_not_json_is_retryable` | eventstream `ParserError` retryable on both; aiohttp JSON errors raise `StreamTruncated`. botocore turns a non-JSON 200 into `{"message": ...}` (not changed). |
| CL-14 | fixed | f5b7a55 | `test_pool_size_follows_the_slots_without_a_cap`, `test_b2_clients_hub.py::test_pool_size_has_a_floor_and_no_cap` | Took the audit's alternative: pool size = slots (floor 10), no 200 cap, so callers never queue for a socket. A separate local error kind would need runtime.py. |
| CL-15 | fixed | 6a620f5 | `test_b2_bedrock_transport.py::test_public_endpoints_tls_and_refusals`, `test_tuning.py` | `read_timeout_secs` / `connect_timeout_secs`, `_read_secs` / `_connect_secs`. |
| CL-16 | fixed (clients part) | 6a620f5 | review | Numbers next to the transport (bedrock_http.py docstring); deepcopy marked unverified. The comment at `tuning.py:44` is outside this scope: hand-off. |
| CL-17 | fixed | f5b7a55 | `test_an_unknown_model_is_a_404_from_the_queued_transport` | Dead `KeyError` branch removed; `UnknownModel` (ConfigError) maps to 404. |
| CL-18 | fixed | 6a620f5 | `test_importing_the_hub_registers_no_fork_hook` | Registered on first `acquire` / `acquire_now`. |
| CL-19 | fixed | f5b7a55 | `test_clients.py::test_long_healthy_answer_is_not_cut` | Warm call before the timed one. |
| CL-20 | fixed (clients part) | f5b7a55 | `test_keep_warm_reads_internals_that_still_exist`; `test_b2_clients_hub.py::test_warm_opens_connections_without_model_calls` | The pyproject comment on the `aiobotocore<4` bound is outside this scope: hand-off. |
| CL-21 | fixed | b50c470 | review | `_lock()` inlined; orphaned comment removed. |
