# Stability policy

- **Public API** is exactly this; anything else may change without notice:
  - the names in `llm_queue.__all__` (`DrainWatcher` among them);
  - the public submodules: `llm_queue.fastapi` (`install`, `serve`, `mount`),
    `llm_queue.langchain` (its `__all__`: `chat_model`, `embeddings`, `aliases`, the `Queued*`
    adapters, `set_default_queue` / `get_default_queue`), `llm_queue.secrets`
    (`secrets_from_config`, `SecretReader`, `SecretSource`) and `llm_queue.drain`
    (`DrainWatcher`);
  - the `llmq.admin` methods: one per row of the operations table in
    `docs/reference/admin-api.md`, with the parameters and answers listed there. `llmq.admin`
    is the one Python write path: the services behind it (`PurgeService`, `RegistryStore`) are
    internal, and only their return types (`PurgeReport`, `PublishResult`) are public;
  - the HTTP routes and the `llm-queue` commands in `docs/reference/cli-http.md` (paths,
    methods, status codes and the JSON answers the operations table documents);
  - the `LLMQ_*` settings (`docs/reference/environment.md`) and the registry document's
    setting names.
- **`llm_queue.run`** is part of that API under this generic name: it runs a coroutine like
  `asyncio.run`, on uvloop when the `uvloop` extra is installed (`use_uvloop=` forces it on or
  off). It is kept as `run`, not renamed (FO-23); import it as `llm_queue.run` or
  `from llm_queue import run as run_main` if the bare name clashes in your module.
- **0.x releases:** a minor release may break the public API; every break is in CHANGELOG.md.
- **Message format:** queue messages carry a format version `v`. A worker returns a message newer
  than it understands to the queue instead of dropping it. Workers of two releases share a queue
  only where CHANGELOG.md says so: 0.1.0b1 and 0.1.0b2 do not (drain 0.1.0b1 first);
  0.1.0b2 and 0.1.0b3 do on a `noeviction` store (on a `volatile-*` store, upgrade every pod);
  0.1.1 and 0.2.0 do not (0.2.0 messages carry hashed tenant and user ids, format 3): drain
  every 0.1.1 worker first, then upgrade the whole fleet.
  See `docs/how-to/upgrade.md`.
- **Store schema:** the key layout has a schema number; a change to it ships with a migration note.
- **Supported versions:** Python 3.12-3.14 (unit and integration tests run on each in CI; 3.11 was dropped in 0.1.0b2); Valkey 8
  and Redis 7.2+ as one primary with replicas (tested on Valkey 8.1 and Redis 7.4). Cluster mode
  is not supported (one admission touches keys of several hash tags) and `start()` refuses it;
  `maxmemory-policy noeviction` is recommended; a `volatile-*` policy works with key expiry
  `swept` (the default `LLMQ_KEY_EXPIRY=auto` picks it). Any other policy logs a warning at
  start, and in strict mode readiness fails. The client is
  redis-py 6 or later (`redis>=6,<9`). The store must allow Lua scripts, pub/sub and blocking
  list reads (`BLPOP`, the queue doorbell); ElastiCache for Valkey / Redis OSS does.
- **Dependencies:** ranges with upper bounds in `pyproject.toml` (`aiobotocore<4`, `redis<9`,
  ...); a release may raise a bound, never drop one without a test on the new major.
- **Settings:** the `LLMQ_*` names and the registry setting names are public API; removing or
  renaming one, or changing a default that changes behaviour, is a breaking change listed in
  CHANGELOG.md (under "Changed (behaviour)").
