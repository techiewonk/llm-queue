# Stability policy

- **Public API** = the names in `llm_queue.__all__`, the registry document's setting
  names and the `llm-queue` commands. Anything else may change without notice.
- **`llm_queue.run`** is part of that API under this generic name: it runs a coroutine like
  `asyncio.run`, on uvloop when the `uvloop` extra is installed (`use_uvloop=` forces it on or
  off). It is kept as `run`, not renamed (FO-23); import it as `llm_queue.run` or
  `from llm_queue import run as run_main` if the bare name clashes in your module.
- **0.x releases:** a minor release may break the public API; every break is in CHANGELOG.md.
- **Message format:** queue messages carry a format version `v`. A worker returns a message newer
  than it understands to the queue instead of dropping it. Workers of two releases share a queue
  only where CHANGELOG.md says so: 0.1.0b1 and 0.1.0b2 do not (drain 0.1.0b1 first; see
  `docs/how-to/upgrade.md`).
- **Store schema:** the key layout has a schema number; a change to it ships with a migration note.
- **Supported versions:** Python 3.12-3.14 (unit and integration tests run on each in CI; 3.11 was dropped in 0.1.0b2); Valkey 8
  and Redis 7.2+ as one primary with replicas (tested on Valkey 8.1 and Redis 7.4). Cluster mode
  is not supported (one admission touches keys of several hash tags) and `start()` refuses it;
  `maxmemory-policy noeviction` is recommended (a warning at start otherwise, and in strict mode
  readiness fails). The client is
  redis-py 6 or later (`redis>=6,<9`). The store must allow Lua scripts, pub/sub and blocking
  list reads (`BLPOP`, the queue doorbell); ElastiCache for Valkey / Redis OSS does.
- **Dependencies:** ranges with upper bounds in `pyproject.toml` (`aiobotocore<4`, `redis<9`,
  ...); a release may raise a bound, never drop one without a test on the new major.
- **Settings:** the `LLMQ_*` names and the registry setting names are public API; removing or
  renaming one, or changing a default that changes behaviour, is a breaking change listed in
  CHANGELOG.md (under "Changed (behaviour)").
