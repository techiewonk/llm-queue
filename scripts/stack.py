"""Start, rebuild, check or stop the local Docker test stack (Valkey, Redis, fakellm).

The stack is what every stack-backed check of `scripts/check.py` runs against; nothing in it
calls a real LLM.

    python scripts/stack.py up        # start (builds fakellm when its image is missing)
    python scripts/stack.py rebuild   # rebuild fakellm after changing tools/fakellm, restart
    python scripts/stack.py status    # which services answer; resets a stuck memory limit
    python scripts/stack.py down
"""

from __future__ import annotations

import argparse
import socket
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker" / "compose.yaml")]
SERVICES = {"valkey": 6379, "redis": 6380, "fakellm": 8080}
STORES = {"valkey": 6379, "redis": 6380}
PASSWORD = "localdev"  # docker/compose.yaml


def answers(port: int) -> bool:
    """True when something listens on localhost:`port`."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


def repair_memory_limit() -> list[str]:
    """Reset a store's `maxmemory` left above 0 by a killed S9 drill; one line per repair.

    S9 lowers the stack Valkey's memory limit for 5 s; a driver killed hard in that window
    left every later check refusing writes, with errors that look like library bugs (FT-19).
    """
    import redis

    repaired = []
    for name, port in STORES.items():
        if not answers(port):
            continue
        client = redis.Redis(host="127.0.0.1", port=port, password=PASSWORD)
        try:
            limit = int(client.config_get("maxmemory").get("maxmemory") or 0)
            if limit:
                client.config_set("maxmemory", 0)
                repaired.append(f"{name}: maxmemory was {limit}, reset to 0 (a killed S9?)")
        finally:
            client.close()
    return repaired


def status() -> int:
    """Print one line per service; exit 1 when any is down."""
    down = 0
    for name, port in SERVICES.items():
        ok = answers(port)
        down += not ok
        print(f"  {'up  ' if ok else 'DOWN'}  {name} (:{port})")
    for line in repair_memory_limit():
        print(f"  REPAIRED  {line}")
    return 1 if down else 0


def main() -> int:
    """Run one stack command."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("up", "rebuild", "status", "down"))
    command = parser.parse_args().command
    if command == "status":
        return status()
    if command == "down":
        return subprocess.run([*COMPOSE, "down"], check=False).returncode
    args = [*COMPOSE, "up", "-d", "--wait", "--remove-orphans"]
    if command == "rebuild":
        args.insert(-2, "--build")
        args += ["--force-recreate", "fakellm", "valkey", "redis"]
    code = subprocess.run(args, check=False).returncode
    return code or status()


if __name__ == "__main__":
    sys.exit(main())
