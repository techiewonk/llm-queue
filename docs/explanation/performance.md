# Performance (measured)

Measured numbers, with where each comes from. Nothing here is an estimate unless it says so.

Numbers from the CHANGELOG "Performance" section and [plan §13](https://github.com/techiewonk/llm-queue/blob/main/docs/ACCOUNT_ROUTING_PLAN.md) (2026-11-13, Python 3.12, fakellm, worker
processes as pods on one machine).

| What | Measured |
|---|---|
| Store scripts per queued call | 11.6 (B19, 2,000 calls in 10 s). By the code: caller `REGISTER`, `SEND`, `CONFIRM`, `RESULT_TAKE`; worker `RECEIVE` (batched), `CALLERS_ALIVE`, `PICK_TAKE`, first-attempt counter, `FINISH`, `RESULT_PUT`, `STATUS_SET`, `ACK`; plus doorbell and heartbeat. About 4 ms of store time per call on an in-VPC store (plan estimate) |
| Store work vs process count | 248 commands per call on 2 processes, 252 on 8 (B22) |
| Admission | `PICK_TAKE` p99 0.94 ms with 20 models x 30 deployments (B91; plan asks 2 ms) |
| Status | 20 x 30 report (210 KB) in 47 ms, 375 ms cold; 6.5 s before one script per model (B91) |
| Queue overhead | +0 ms at p95 over a direct call, 141 ms both (B34); CHANGELOG: 0-15 ms p95; 290 calls/s on 4 workers |
| Waiting callers | 2,000 waiting: 1,976 store calls/s before, 499/s after (pub/sub + sweep, B56) |
| Budgets | 1,800 of 1,800 a minute on 9 deployments, none above its RPM, priority wait p95 0.27 s (B17); normal class at 100 % of its 80 % (B56 / B93) |
| Recovery | Waiter pod killed: next waiter within 1.3 s smooth, 0.02 s after the minute fixed (B94); first call after idle 125 ms (B92) |
| Memory | Bulk Cohere embedding (300 x 96 texts): store +23 MB at peak, +582 MB before deleting results on first read (B97; CHANGELOG states 31 MB vs 587 MB). Process memory: **unverified** |
| CPU per call | botocore parameter validation off: 8.5 to 3.5 ms per attempt at 218 KB; no deep copy (1.86 ms saved); envelope encode 342 to 57 us; UUIDv7 0.49 us; `hiredis` parsing 39-75x faster |
