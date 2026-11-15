"""R3OB-9: every log event the library writes is listed in docs/reference/log-events.md."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EVENT = re.compile(r'logger\.(?:debug|info|warning|error|exception)\(\s*"([a-z0-9_]+)"')


def test_every_log_event_is_documented() -> None:
    """R3OB-9: event names followed no list; a new event must be added to the reference page."""
    page = (ROOT / "docs" / "reference" / "log-events.md").read_text(encoding="utf-8")
    listed = set(re.findall(r"^\| `([a-z0-9_]+)` \|", page, re.MULTILINE))
    emitted = {
        name
        for path in (ROOT / "src" / "llm_queue").rglob("*.py")
        for name in EVENT.findall(path.read_text(encoding="utf-8"))
    }
    assert emitted - listed == set(), "add these events to docs/reference/log-events.md"
