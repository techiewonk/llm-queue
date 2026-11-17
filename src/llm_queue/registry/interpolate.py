"""`${NAME}` placeholders in the boot registry document, and YAML registry files (H3).

The document is parsed first (JSON with the repeated-key check, or YAML with its own), then
placeholders are expanded in the parsed strings, never in the text: a value with quotes,
backslashes or newlines cannot break the document's quoting. Forms, inside any string value:

* `${NAME}`: the value of `NAME`; unset is a `ConfigError` naming it and the JSON path.
* `${NAME:-default}`: `default` when `NAME` is unset or empty (as in a shell).
* `$${`: a literal `${`.

A placeholder may not read a credential (`CREDENTIAL_NAMES`: the store passwords, the admin
tokens, the id salt, the secrets settings and the AWS keys): the expanded document is stored
and served by `GET /registry`, so one would leak in clear (review B-09, owner Q11).

A string that is exactly one placeholder becomes a typed value when the text it expands to is
a JSON list, object, number, `true` / `false` or a quoted JSON string (the escape that keeps
`"12345"` text). Expanded text is never expanded again, and keys are never expanded. Only the
boot document (`LLMQ_REGISTRY` / `LLMQ_REGISTRY_FILE`) goes through here; a stored document and
live edits are taken as they are. Values never appear in an error message.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable
from typing import Any

from llm_queue.errors import ConfigError, MissingExtra
from llm_queue.registry.validate import parse_document

Lookup = Callable[[str], "str | None"]

_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# A JSON number, or true / false: a whole-string placeholder expanding to one becomes that type.
_SCALAR_RE = re.compile(r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?\Z|true\Z|false\Z")
YAML_SUFFIXES = (".yaml", ".yml")
# Names a placeholder may never read (any case): `LLMQ_*_PASSWORD`, `LLMQ_*_TOKEN`, the id
# salt, `LLMQ_SECRET*` / `LLMQ_SECRETS_*` and the AWS credential variables.
CREDENTIAL_NAMES = re.compile(
    r"LLMQ_\w+_(?:PASSWORD|TOKEN)|LLMQ_ID_HASH_SALT|LLMQ_SECRETS?(?:_\w+)?"
    r"|AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN",
    re.IGNORECASE,
)


def credential_name(name: str) -> bool:
    """True when `name` is a credential a registry placeholder may not read."""
    return CREDENTIAL_NAMES.fullmatch(name) is not None


class _Placeholder:
    """One `${NAME}` / `${NAME:-default}` found in a string."""

    __slots__ = ("default", "name")

    def __init__(self, name: str, default: str | None) -> None:
        self.name = name
        self.default = default


def _tokens(text: str) -> list[str | _Placeholder] | str:
    """`text` as literal pieces and placeholders, or the reason it is malformed."""
    out: list[str | _Placeholder] = []
    literal: list[str] = []
    i = 0
    while i < len(text):
        if text.startswith("$${", i):
            literal.append("${")
            i += 3
            continue
        if not text.startswith("${", i):
            literal.append(text[i])
            i += 1
            continue
        found = _NAME_RE.match(text, i + 2)
        if found is None:
            return "`${` must be followed by a variable name (write `$${` for a literal `${`)"
        name, j = found.group(), found.end()
        default: str | None = None
        if text.startswith(":-", j):
            # The default runs to the matching `}`, so a JSON object default keeps its braces.
            depth, k = 0, j + 2
            while k < len(text) and not (text[k] == "}" and depth == 0):
                depth += {"{": 1, "}": -1}.get(text[k], 0)
                k += 1
            default, j = text[j + 2 : k], k
        if j >= len(text) or text[j] != "}":
            return f"the placeholder for `{name}` is not closed with `}}`"
        if literal:
            out.append("".join(literal))
            literal = []
        out.append(_Placeholder(name, default))
        i = j + 1
    if literal:
        out.append("".join(literal))
    return out


class _Expander:
    """Expands one document, collecting every problem with its path."""

    def __init__(self, lookup: Lookup) -> None:
        self.lookup = lookup
        self.problems: list[str] = []
        self.used: set[str] = set()

    def value_of(self, ph: _Placeholder, path: str) -> str | None:
        where = path or "the top level"
        if credential_name(ph.name):  # never looked up, default or not
            self.problems.append(
                f"`{ph.name}` (at {where}) names a credential, which a registry placeholder may "
                "not read: the document is stored and served"
            )
            return None
        self.used.add(ph.name)
        value = self.lookup(ph.name)
        if ph.default is not None and not value:
            return ph.default
        if value is None:
            self.problems.append(f"`{ph.name}` is not set (at {path or 'the top level'})")
        return value

    def string(self, text: str, path: str) -> Any:
        if "$" not in text:
            return text
        tokens = _tokens(text)
        if isinstance(tokens, str):
            self.problems.append(f"{path}: {tokens}")
            return text
        if len(tokens) == 1 and isinstance(tokens[0], _Placeholder):
            value = self.value_of(tokens[0], path)
            return None if value is None else self.typed(value, tokens[0].name, path)
        parts = []
        for token in tokens:
            if isinstance(token, _Placeholder):
                token = self.value_of(token, path) or ""
            parts.append(token)
        return "".join(parts)

    def typed(self, value: str, name: str, path: str) -> Any:
        """A whole-string placeholder's value: JSON when it reads as JSON, else the text."""
        stripped = value.strip()
        if not (stripped[:1] in ("[", "{", '"') or _SCALAR_RE.match(stripped)):
            return value
        try:
            parsed = parse_document(stripped)
        except ConfigError as exc:
            # The parser's message names a line and column, never the text itself.
            reason = "; ".join(getattr(exc, "problems", [])) or "not valid JSON"
            self.problems.append(f"`{name}` (at {path}) reads as JSON but is not valid: {reason}")
            return None
        return parsed

    def walk(self, node: Any, path: str) -> Any:
        if isinstance(node, dict):
            out = {}
            for key, item in node.items():
                tokens = _tokens(key)
                if isinstance(tokens, str) or any(isinstance(t, _Placeholder) for t in tokens):
                    where = f"{path}.{key}" if path else key
                    self.problems.append(f"{where}: placeholders are expanded in values only")
                    continue
                plain = "".join(t for t in tokens if isinstance(t, str))
                out[plain] = self.walk(item, f"{path}.{plain}" if path else plain)
            return out
        if isinstance(node, list):
            return [self.walk(item, f"{path}[{i}]") for i, item in enumerate(node)]
        if isinstance(node, str):
            return self.string(node, path)
        return node


def expand(document: Any, lookup: Lookup, *, used: set[str] | None = None) -> Any:
    """`document` with every placeholder expanded; `ConfigError` listing every problem.

    `used` (when given) collects the names the placeholders read, never their values.
    """
    expander = _Expander(lookup)
    out = expander.walk(document, "")
    if used is not None:
        used |= expander.used
    if expander.problems:
        raise ConfigError("registry placeholders:\n  " + "\n  ".join(expander.problems))
    return out


def load_yaml(text: str) -> Any:
    """A YAML registry document; refuses repeated keys, non-text keys and NaN / infinity.

    `MissingExtra` (install `llm-queue[yaml]`) when PyYAML is not installed.
    """
    try:
        import yaml  # optional extra, imported only for a YAML file
    except ImportError as exc:
        raise MissingExtra("a YAML LLMQ_REGISTRY_FILE", "yaml") from exc

    problems: list[str] = []
    # A SafeLoader of its own (built per call, so the constructor below never touches PyYAML's
    # shared SafeLoader) that records a repeated key instead of keeping the last one.
    loader_class: Any = type("RegistryLoader", (yaml.SafeLoader,), {})

    def mapping(loader: Any, node: Any, deep: bool = False) -> dict[Any, Any]:
        loader.flatten_mapping(node)
        out: dict[Any, Any] = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            line = key_node.start_mark.line + 1
            if not isinstance(key, str):
                problems.append(f"line {line}: keys must be text")
                continue
            if key in out:
                problems.append(f"line {line}: `{key}` is repeated in one mapping")
            out[key] = loader.construct_object(value_node, deep=deep)
        return out

    loader_class.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
    try:
        document = yaml.load(text, Loader=loader_class)  # a SafeLoader: no Python objects
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        where = f" (line {mark.line + 1})" if mark is not None else ""
        raise ConfigError(f"registry file is not valid YAML{where}") from exc
    if problems:
        raise ConfigError("registry file: " + "; ".join(problems))
    if _has_non_finite(document):
        raise ConfigError("registry file has .nan or .inf, which are not JSON numbers")
    return document


def _has_non_finite(node: Any) -> bool:
    if isinstance(node, float):
        return not math.isfinite(node)
    if isinstance(node, dict):
        return any(_has_non_finite(v) for v in node.values())
    if isinstance(node, list):
        return any(_has_non_finite(v) for v in node)
    return False


def to_json(document: Any) -> str:
    """The expanded document as JSON text, checked again by `parse_document`."""
    try:
        text = json.dumps(document, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:  # a YAML date, set or NaN
        raise ConfigError(f"registry cannot be written as JSON: {exc}") from exc
    parse_document(text)
    return text
