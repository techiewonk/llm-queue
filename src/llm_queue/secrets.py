"""Where secrets come from: `LLMQ_SECRETS_SOURCE` and `LLMQ_SECRET_NAMES` (0.2.0).

A source spec is an ordered, comma-separated list; each name is looked up in each source in
turn and the first that has it wins:

* `aws-secretsmanager:<secret-id>`: one AWS Secrets Manager secret holding a JSON object
  (botocore; region `LLMQ_SECRETS_REGION`, else `AWS_REGION` / `AWS_DEFAULT_REGION`;
  credentials from the default chain, a pod role on EKS);
* `file:<path>`: a directory (one file per key, as a Kubernetes secret volume mounts it; one
  trailing newline stripped), a `.json` file (an object) or any other file read as `.env`
  lines (`KEY=VALUE`; see `FileSource` for comments and quotes);
* `env`: the environment;
* `none` (the default): no external source.

`${NAME}` / `${NAME:-default}` in the spec are expanded from the environment
(`aws-secretsmanager:${ENV}/app/secret`). Each source's values are cached for
`LLMQ_SECRETS_CACHE_SECS` and read again after, so a rotated value is picked up without a
restart; a source that cannot be read at start is a `ConfigError` naming it, and a refresh
that fails later keeps the last values (logged). A key no source has reads as unset (None).

Values never appear in a log line, an error message, `repr` or `describe()`: those show the
source kinds and ids (which are not secrets) and the names of mapped settings only.
"""

from __future__ import annotations

import json
import math
import os
import random
import re
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import orjson

from llm_queue._logging import get_logger
from llm_queue.errors import ConfigError

logger = get_logger(__name__)

SecretLookup = Callable[[str], "str | None"]  # a secret's value by name, None when unset
# Builds the Secrets Manager client for a region (None: botocore's own lookup); tests pass a
# fake through `secrets_from_config(client_factory=...)`.
ClientFactory = Callable[[str | None, float], Any]

# The settings this module reads (from the environment or `defaults=`, never from a secret).
SOURCE_VAR = "LLMQ_SECRETS_SOURCE"
NAMES_VAR = "LLMQ_SECRET_NAMES"
REGION_VAR = "LLMQ_SECRETS_REGION"
CACHE_VAR = "LLMQ_SECRETS_CACHE_SECS"
TIMEOUT_VAR = "LLMQ_SECRETS_TIMEOUT_SECS"
SETTINGS = (SOURCE_VAR, NAMES_VAR, REGION_VAR, CACHE_VAR, TIMEOUT_VAR)
SOURCE_KINDS = ("aws-secretsmanager", "file", "env", "none")
CACHE_SECS = 300.0  # LLMQ_SECRETS_CACHE_SECS default
RETRY_SECS = 30.0  # a failed refresh is tried again after min(cache_secs, this), jittered
# The retry is spread over +-20 %, so the pods of a fleet that lost Secrets Manager together do
# not all ask again in the same second (review S-2).
RETRY_JITTER = 0.2
# A store login refused with these values re-reads the sources (`invalidate`), at most this often:
# a rotated password is picked up at once, a wrong one does not hammer the source.
INVALIDATE_MIN_SECS = 5.0
MAX_CACHE_SECS = 86_400.0
TIMEOUT_SECS = 10.0  # LLMQ_SECRETS_TIMEOUT_SECS default: connect and read, each
MIN_TIMEOUT_SECS, MAX_TIMEOUT_SECS = 1.0, 300.0
# A Kubernetes secret volume holds `..data` and `..<timestamp>` entries next to the keys.
_HIDDEN = "."
_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_ENV_LINE = re.compile(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_.-]*)\s*=(.*)$")
_ENV_COMMENT = re.compile(r"\s#")  # an inline comment: `#` after whitespace, unquoted values


@runtime_checkable
class SecretSource(Protocol):
    """One place secrets are read from, all its values at once.

    `kind` is one of `SOURCE_KINDS` (or an app's own word); `label` names the source for logs
    and errors (kind and id, never a value). `load` returns every key the source holds now and
    raises `ConfigError` (naming the source, not its values) when it cannot be read.
    """

    @property
    def kind(self) -> str:
        """The source kind, e.g. `aws-secretsmanager`."""
        ...

    @property
    def label(self) -> str:
        """`kind:id` (or the kind alone), safe to log."""
        ...

    def load(self) -> Mapping[str, str]:
        """Every key and value the source holds now."""
        ...


def _text(value: Any) -> str | None:
    """A JSON value as setting text: strings as they are, scalars as JSON, null as unset."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return orjson.dumps(value).decode()


def _json_object(raw: str | bytes, label: str) -> dict[str, str]:
    """The JSON object in `raw` as text values; the parser's message is not kept (no values)."""
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError:
        raise ConfigError(f"secrets source {label}: is not valid JSON") from None
    if not isinstance(data, dict):
        raise ConfigError(f"secrets source {label}: must hold a JSON object")
    return {str(k): v for k, v in ((k, _text(v)) for k, v in data.items()) if v is not None}


class AwsSecretsManagerSource:
    """One Secrets Manager secret whose `SecretString` is a JSON object of keys."""

    kind = "aws-secretsmanager"

    def __init__(
        self,
        secret_id: str,
        *,
        region: str | None = None,
        timeout_secs: float = TIMEOUT_SECS,
        client_factory: ClientFactory | None = None,
    ) -> None:
        """Read `secret_id` in `region`; the client is made on the first load."""
        self.secret_id = secret_id
        self.region = region
        self._timeout = timeout_secs
        self._factory = client_factory or _botocore_client
        self._client: Any = None
        self._lock = threading.Lock()

    @property
    def label(self) -> str:
        """`aws-secretsmanager:<secret-id>`."""
        return f"{self.kind}:{self.secret_id}"

    def __repr__(self) -> str:
        return f"AwsSecretsManagerSource({self.secret_id!r}, region={self.region!r})"

    def load(self) -> Mapping[str, str]:
        """`GetSecretValue`, parsed; `ConfigError` with the AWS error code when it fails."""
        try:
            with self._lock:
                if self._client is None:
                    self._client = self._factory(self.region, self._timeout)
            reply = self._client.get_secret_value(SecretId=self.secret_id)
        except ConfigError:
            raise
        except Exception as exc:  # botocore's ClientError, NoRegionError, NoCredentialsError ...
            raise ConfigError(
                f"secrets source {self.label}: cannot be read ({_reason(exc)})"
            ) from None
        text = reply.get("SecretString")
        if text is None:
            raise ConfigError(f"secrets source {self.label}: has no SecretString (binary secret)")
        return _json_object(text, self.label)


def _reason(exc: BaseException) -> str:
    """An AWS error code or the exception's class: never its message (it may echo input)."""
    response = getattr(exc, "response", None)
    code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
    if code:
        return str(code)
    if type(exc).__name__ == "NoRegionError":
        return "no region: set LLMQ_SECRETS_REGION or AWS_REGION"
    return type(exc).__name__


def _botocore_client(region: str | None, timeout_secs: float) -> Any:
    """A botocore Secrets Manager client on the default credential chain (pod role on EKS)."""
    import botocore.session
    from botocore.config import Config

    config = Config(
        connect_timeout=timeout_secs,
        read_timeout=timeout_secs,
        retries={"max_attempts": 3, "mode": "standard"},
    )
    session = botocore.session.get_session()
    return session.create_client("secretsmanager", region_name=region, config=config)


class FileSource:
    """A directory of files (one per key), a `.json` object file or a `.env` file.

    `.env`: `#` lines and blanks skipped, `export ` allowed, an inline comment is `#` after
    whitespace, quoted values kept as written; anything else is refused by line number.
    """

    kind = "file"

    def __init__(self, path: str | os.PathLike[str]) -> None:
        """Read `path` on each load (its form is decided then, so a mount may appear later)."""
        self.path = Path(path)

    @property
    def label(self) -> str:
        """`file:<path>`."""
        return f"{self.kind}:{self.path}"

    def __repr__(self) -> str:
        return f"FileSource({str(self.path)!r})"

    def load(self) -> Mapping[str, str]:
        """The keys of the directory or file; `ConfigError` when it cannot be read."""
        try:
            if self.path.is_dir():
                return self._directory()
            raw = self.path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            reason = (
                exc.strerror if isinstance(exc, OSError) and exc.strerror else type(exc).__name__
            )
            raise ConfigError(f"secrets source {self.label}: cannot be read ({reason})") from None
        if self.path.suffix.lower() == ".json":
            return _json_object(raw, self.label)
        return self._dotenv(raw)

    def _directory(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for entry in self.path.iterdir():
            if entry.name.startswith(_HIDDEN) or not entry.is_file():
                continue
            text = entry.read_bytes().decode("utf-8")  # as written: no newline translation
            out[entry.name] = text[:-2] if text.endswith("\r\n") else text.removesuffix("\n")
        return out

    def _dotenv(self, raw: str) -> dict[str, str]:
        out: dict[str, str] = {}
        for number, line in enumerate(raw.splitlines(), 1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            found = _ENV_LINE.match(stripped)
            value = _env_value(found.group(2)) if found is not None else None
            if found is None or value is None:  # the line is not shown: it may hold a value
                raise ConfigError(f"secrets source {self.label}: line {number} is not KEY=VALUE")
            out[found.group(1)] = value
        return out


def _env_value(raw: str) -> str | None:
    """A `.env` value (the text after `=`) by `FileSource`'s rules; None for a bad quote."""
    text = raw.strip()
    if text[:1] in ("'", '"'):
        end = text.find(text[0], 1)
        rest = text[end + 1 :].strip() if end > 0 else "x"
        return text[1:end] if not rest or rest.startswith("#") else None
    return _ENV_COMMENT.split(raw, maxsplit=1)[0].strip()


class EnvSource:
    """The environment (a mapping; default `os.environ`, read live)."""

    kind = "env"
    label = "env"

    def __init__(self, environ: Mapping[str, str] | None = None) -> None:
        """Read `environ` (default `os.environ`)."""
        self._environ = os.environ if environ is None else environ

    def __repr__(self) -> str:
        return "EnvSource()"

    def load(self) -> Mapping[str, str]:
        """The environment itself (no copy: it is read per lookup)."""
        return self._environ


class _Cached:
    """One source's values and when they were read."""

    __slots__ = ("flag", "lock", "read_at", "refreshing", "source", "values")

    def __init__(self, source: SecretSource) -> None:
        self.source = source
        self.values: Mapping[str, str] | None = None
        self.read_at = 0.0
        self.lock = threading.Lock()
        self.refreshing = False  # a background refresh runs (`SecretReader._values`)
        self.flag = threading.Lock()  # guards `refreshing` only: never held during a read


class SecretReader:
    """A `secrets=` lookup over ordered sources, each cached for `cache_secs`.

    Thread-safe; on an event loop an expired source serves its last values while a thread
    reads it again. A failed refresh keeps the last values and is retried later (jittered).
    """

    def __init__(
        self,
        sources: Iterable[SecretSource],
        *,
        cache_secs: float = CACHE_SECS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Look names up in `sources` in order; `cache_secs` 0 reads them on every lookup."""
        self._sources = [_Cached(source) for source in sources]
        self.cache_secs = cache_secs
        self._clock = clock

    @property
    def sources(self) -> tuple[SecretSource, ...]:
        """The sources, in lookup order."""
        return tuple(c.source for c in self._sources)

    def __repr__(self) -> str:
        labels = ", ".join(c.source.label for c in self._sources)
        return f"SecretReader([{labels}], cache_secs={self.cache_secs:g})"

    def check(self) -> None:
        """Read every source now; `ConfigError` naming each one that cannot be read."""
        problems = []
        for cached in self._sources:
            try:
                self._refresh(cached, strict=True)
            except ConfigError as exc:
                problems.append(str(exc))
        if problems:
            raise ConfigError("secrets:\n  " + "\n  ".join(problems))

    def __call__(self, name: str) -> str | None:
        """`name` from the first source that has it, None when none does."""
        for cached in self._sources:
            values = self._values(cached)
            value = values.get(name) if values is not None else None
            if value is not None and value != "":
                return value
        return None

    def describe(self) -> dict[str, Any]:
        """The sources (kind and id) and the cache time, no values."""
        return {
            "sources": [c.source.label for c in self._sources],
            "cache_secs": self.cache_secs,
        }

    def invalidate(self) -> None:
        """Read every source again on the next lookup (a store login was refused, review S-2).

        A source read in the last `INVALIDATE_MIN_SECS` is kept, so a wrong value cannot turn
        every failed connection into a Secrets Manager call.
        """
        now = self._clock()
        for cached in self._sources:
            if now - cached.read_at >= INVALIDATE_MIN_SECS:
                cached.read_at = -math.inf

    def _values(self, cached: _Cached) -> Mapping[str, str] | None:
        if cached.values is not None and self._fresh(cached):
            return cached.values
        if cached.values is not None and self.cache_secs > 0 and _on_event_loop():
            self._refresh_later(cached)
            return cached.values
        return self._refresh(cached, strict=cached.values is None)

    def _refresh_later(self, cached: _Cached) -> None:
        """Read `cached` again on a daemon thread (at most one at a time per source)."""
        with cached.flag:  # not `lock`: a refresh holds that for the whole read
            if cached.refreshing:
                return
            cached.refreshing = True

        def run() -> None:
            try:
                self._refresh(cached, strict=False)
            finally:
                cached.refreshing = False

        threading.Thread(target=run, name="llmq-secrets-refresh", daemon=True).start()

    def _fresh(self, cached: _Cached) -> bool:
        return self.cache_secs > 0 and self._clock() - cached.read_at < self.cache_secs

    def _refresh(self, cached: _Cached, *, strict: bool) -> Mapping[str, str] | None:
        """Read the source again; a failure raises when `strict`, else keeps the last values."""
        with cached.lock:
            if cached.values is not None and self._fresh(cached):
                return cached.values  # another thread refreshed it meanwhile
            try:
                values = cached.source.load()
            except Exception as exc:
                # An app's own source may raise anything; its message may echo a value, so only
                # a `ConfigError` (ours: no values) is shown, else the exception's class.
                if not isinstance(exc, ConfigError):
                    exc = ConfigError(
                        f"secrets source {cached.source.label}: cannot be read "
                        f"({type(exc).__name__})"
                    )
                if strict:
                    raise exc from None
                logger.warning("secrets_refresh_failed", source=cached.source.label, error=str(exc))
                # Fresh again for min(cache_secs, RETRY_SECS): retried soon, not a period later.
                retry = min(self.cache_secs, RETRY_SECS)
                retry *= random.uniform(1 - RETRY_JITTER, 1 + RETRY_JITTER)
                cached.read_at = self._clock() - self.cache_secs + retry
                return cached.values
            cached.values = values
            cached.read_at = self._clock()
            return values


def _on_event_loop() -> bool:
    """Whether this thread is running an asyncio event loop."""
    import asyncio

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def expand_spec(spec: str, environ: Mapping[str, str] | None = None) -> str:
    """`spec` with `${NAME}` / `${NAME:-default}` expanded from `environ` (`os.environ`)."""
    env = os.environ if environ is None else environ
    missing: list[str] = []

    def one(found: re.Match[str]) -> str:
        name, default = found.group(1), found.group(2)
        value = env.get(name)
        if not value:
            if default is not None:
                return default
            missing.append(name)
            return ""
        return value

    out = _PLACEHOLDER.sub(one, spec)
    if missing:
        raise ConfigError("LLMQ_SECRETS_SOURCE: " + ", ".join(f"`{n}` is not set" for n in missing))
    return out


def parse_sources(
    spec: str,
    *,
    environ: Mapping[str, str] | None = None,
    region: str | None = None,
    timeout_secs: float = TIMEOUT_SECS,
    client_factory: ClientFactory | None = None,
) -> list[SecretSource]:
    """The sources a spec names, in order; `[]` for `none`. `ConfigError` for a bad spec."""
    text = expand_spec(spec, environ)
    items = [item.strip() for item in text.split(",") if item.strip()]
    if not items:
        raise ConfigError("LLMQ_SECRETS_SOURCE must name a source (or `none`)")
    if "none" in items:
        if len(items) > 1:
            raise ConfigError("LLMQ_SECRETS_SOURCE: `none` cannot be combined with other sources")
        return []
    out: list[SecretSource] = []
    for item in items:
        kind, _, target = item.partition(":")
        kind, target = kind.strip().lower(), target.strip()
        if kind == "env" and not target:
            out.append(EnvSource(environ))
        elif kind == "file" and target:
            out.append(FileSource(target))
        elif kind == "aws-secretsmanager" and target:
            out.append(
                AwsSecretsManagerSource(
                    target,
                    region=region or _aws_region(environ),
                    timeout_secs=timeout_secs,
                    client_factory=client_factory,
                )
            )
        else:
            raise ConfigError(
                f"LLMQ_SECRETS_SOURCE entry `{kind}` must be aws-secretsmanager:<secret-id>, "
                "file:<path>, env or none"
            )
    return out


def _aws_region(environ: Mapping[str, str] | None) -> str | None:
    env = os.environ if environ is None else environ
    return env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION") or None


def secrets_from_config(
    spec: str,
    *,
    environ: Mapping[str, str] | None = None,
    region: str | None = None,
    cache_secs: float = CACHE_SECS,
    timeout_secs: float = TIMEOUT_SECS,
    client_factory: ClientFactory | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> SecretReader | None:
    """The `secrets=` lookup a source spec describes, read once now; None for `none`.

    The same thing `LLMQ_SECRETS_SOURCE` builds, for an app that wants it in code:
    `LLMQueue.from_env(secrets=secrets_from_config("aws-secretsmanager:prod/app"))`.
    `environ` (default `os.environ`) expands placeholders and backs `env`; `region` defaults
    to `AWS_REGION` / `AWS_DEFAULT_REGION`. `ConfigError` names a source that cannot be read.
    """
    sources = parse_sources(
        spec,
        environ=environ,
        region=region,
        timeout_secs=timeout_secs,
        client_factory=client_factory,
    )
    if not sources:
        return None
    reader = SecretReader(sources, cache_secs=cache_secs, clock=clock)
    reader.check()
    return reader


def parse_secret_names(value: Any) -> tuple[dict[str, str], list[str]]:
    """`LLMQ_SECRET_NAMES` (JSON text or a mapping) and its problems, listed at once."""
    if value is None or value == "":
        return {}, []
    repeated: list[str] = []
    if isinstance(value, str):

        def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
            seen: dict[str, Any] = {}
            for key, item in items:
                if key in seen:
                    repeated.append(key)
                seen[key] = item
            return seen

        try:
            value = json.loads(value, object_pairs_hook=pairs)
        except ValueError:
            return {}, ["LLMQ_SECRET_NAMES must be a JSON object of setting names to secret keys"]
    if not isinstance(value, Mapping):
        return {}, ["LLMQ_SECRET_NAMES must be a JSON object of setting names to secret keys"]
    names: dict[str, str] = {}
    problems = [f"LLMQ_SECRET_NAMES: {name} is set twice" for name in dict.fromkeys(repeated)]
    for name, secret in value.items():
        if not isinstance(secret, str) or not secret.strip():
            problems.append(f"LLMQ_SECRET_NAMES: {name} must name a secret key")
            continue
        names[str(name).strip()] = secret.strip()
    return names, problems


def read_number(
    raw: Any, name: str, default: float, lo: float, hi: float, problems: list[str]
) -> float:
    """`raw` as a number from `lo` to `hi` (problems in the settings' wording)."""
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        problems.append(f"{name} must be a number")
        return default
    if value != value or value in (float("inf"), float("-inf")):
        problems.append(f"{name} must be a finite number")
        return default
    if not lo <= value <= hi:
        floor = "0 or more" if lo == 0 else f"at least {lo:g}"
        problems.append(
            f"{name} must be {floor}" if value < lo else f"{name} must be at most {hi:g}"
        )
        return default
    return value
