"""Run the usage samples in `samples/`, each as its own process, on fakellm or real Bedrock.

Each sample is plain app code (`LLMQueue.from_env()`); this script only sets the `LLMQ_*`
environment: the local Valkey of `scripts/stack.py`, a fresh key prefix per run (deleted after),
and `samples/registry.json` with its region set.

    python scripts/samples.py                    # every sample on fakellm (no real LLM)
    python scripts/samples.py 03_embed           # one sample
    python scripts/samples.py --live             # real Bedrock: AWS keys from --env-file
    python scripts/samples.py --live --env-file ../../.env
    python scripts/samples.py --live --store env  # the store of the env file (TLS)

`--live` reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` and
`AWS_MODEL_REGION` from the env file (default `.env`) and never prints them. Live runs make a
few small calls per sample (well under a cent in total).

`--store env` uses the env file's store instead of the local Valkey: `LLMQ_STORE_URL`, or
`rediss://` built from the app's `valkey_host`, `valkey_port` and `valkey_key`. The run touches
only its own key prefix and deletes it at the end, so a shared store is safe.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

from uuid_utils import uuid7

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"
_VENV_PY = ROOT / ".venv" / ("Scripts" if os.name == "nt" else "bin") / "python"
# The project venv, else the interpreter running this script.
_FOUND = _VENV_PY.with_suffix(".exe" if os.name == "nt" else "").exists()
PY = str(_VENV_PY) if _FOUND else sys.executable
LOCAL_STORE_URL = "redis://:localdev@127.0.0.1:6379/0"
FAKELLM_URL = "http://127.0.0.1:8080"
AWS_KEYS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")


def registry(region: str) -> str:
    """`samples/registry.json` with every deployment and the residency list on `region`."""
    doc = json.loads((SAMPLES / "registry.json").read_text(encoding="utf-8"))
    doc["residency"]["allowed_regions"] = [region]
    for model in doc["models"].values():
        for dep in model["deployments"].values():
            dep["confirmed"] = [region]
    return json.dumps(doc)


def store_url(store: str, env_file: Path) -> str:
    """The local Valkey, or the env file's store (never printed: it holds the password)."""
    if store == "local":
        return LOCAL_STORE_URL
    from dotenv import dotenv_values

    values = dotenv_values(env_file)
    if values.get("LLMQ_STORE_URL"):
        return str(values["LLMQ_STORE_URL"])
    host = values.get("valkey_host")
    if not host:
        raise SystemExit(f"{env_file}: neither LLMQ_STORE_URL nor valkey_host is set")
    password = quote(str(values.get("valkey_key") or ""), safe="")
    auth = f":{password}@" if password else ""
    return f"rediss://{auth}{host}:{values.get('valkey_port') or 6379}/0"


def environment(live: bool, env_file: Path, prefix: str, store: str) -> dict[str, str]:
    """The process environment of one run (no AWS keys from the shell leak into fake runs)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AWS_", "LLMQ_"))}
    if live:
        from dotenv import dotenv_values

        values = dotenv_values(env_file)
        missing = [k for k in AWS_KEYS[:2] if not values.get(k)]
        if missing:
            raise SystemExit(f"{env_file}: {', '.join(missing)} not set")
        env.update({k: str(values[k]) for k in AWS_KEYS if values.get(k)})
        region = str(values.get("AWS_MODEL_REGION") or "us-east-1")
    else:
        env.update({"AWS_ACCESS_KEY_ID": "aws-main", "AWS_SECRET_ACCESS_KEY": "fake"})
        env["LLMQ_BEDROCK_ENDPOINT_URL"] = FAKELLM_URL
        region = "us-east-1"
    env.update(
        {
            "LLMQ_STORE_URL": store,
            "LLMQ_KEY_PREFIX": prefix,
            "LLMQ_QUEUE_PREFIX": prefix.replace(":", "-") + "-",
            "LLMQ_REGISTRY": registry(region),
            "LLMQ_METRICS_SINK": "none",
            "PYTHONIOENCODING": "utf-8",
            # This checkout's library, even when the venv's editable install points elsewhere.
            "PYTHONPATH": os.pathsep.join(filter(None, [str(ROOT / "src"), env.get("PYTHONPATH")])),
        }
    )
    if store == LOCAL_STORE_URL:  # no TLS, no salt: only allowed in a test environment
        env["LLMQ_ENVIRONMENT"] = "test"
    else:  # strict, as in an app: a salt from its secrets; here a fresh one per run
        env.setdefault("LLMQ_ID_HASH_SALT", uuid7().hex)
    where = "local Valkey" if store == LOCAL_STORE_URL else "the env file's store (TLS)"
    if store.startswith("redis://") and store != LOCAL_STORE_URL:
        where = "the env file's store (no TLS)"
    print(
        f"target: {'real Bedrock' if live else 'fakellm'} in {region}; store: {where}; "
        f"key prefix {prefix}"
    )
    return env


def cleanup(prefix: str, store: str) -> None:
    """Delete every key and queue the run made (only under its own prefix)."""
    import redis

    client = redis.Redis.from_url(store)
    queue_prefix = prefix.replace(":", "-") + "-"
    for pattern in (f"{prefix}:*", f"{queue_prefix}*", f"*{{{queue_prefix}*"):
        keys = list(client.scan_iter(match=pattern, count=500))
        if keys:
            client.delete(*keys)
    client.close()


def main() -> int:
    """Run the chosen samples; exit 1 if any fails."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("names", nargs="*", help="sample names (default: all)")
    parser.add_argument("--live", action="store_true", help="call real Bedrock")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument(
        "--store", choices=("local", "env"), default="local", help="local Valkey or the env file's"
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="folder for each sample's output and a summary (default: reports/samples-...)",
    )
    parser.add_argument("--no-save", action="store_true", help="print only; write no files")
    args = parser.parse_args()
    found = sorted(p.stem for p in SAMPLES.glob("[0-9]*.py"))
    names = args.names or found
    unknown = [n for n in names if n not in found]
    if unknown:
        print(f"unknown sample(s): {', '.join(unknown)}; have {', '.join(found)}")
        return 2
    prefix = f"llmqsample:{uuid7().hex[-8:]}"  # a v7 starts with the time: take its tail
    store = store_url(args.store, args.env_file)
    env = environment(args.live, args.env_file, prefix, store)
    target = "live" if args.live else "fakellm"
    out_dir = None
    if not args.no_save:  # every run keeps its results (reports/ is git-ignored)
        stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        out_dir = args.out or ROOT / "reports" / f"samples-{target}-{stamp}"
        out_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []
    failed = []
    try:
        for name in names:
            started = time.monotonic()
            with tempfile.TemporaryDirectory() as cwd:  # samples never depend on the cwd
                proc = subprocess.run(
                    [PY, str(SAMPLES / f"{name}.py")],
                    cwd=cwd,
                    env=env,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    timeout=args.timeout,
                    check=False,
                )
            ok = proc.returncode == 0
            secs = time.monotonic() - started
            print(f"{'PASS' if ok else 'FAIL'}  {name} ({secs:.1f}s)")
            results.append({"sample": name, "ok": ok, "secs": round(secs, 1)})
            if out_dir is not None:  # the full output, stderr included (no secrets: never printed)
                (out_dir / f"{name}.log").write_text(
                    proc.stdout + ("\n--- stderr ---\n" + proc.stderr if proc.stderr else ""),
                    encoding="utf-8",
                )
            out = proc.stdout.rstrip() if ok else (proc.stdout + proc.stderr).rstrip()[-4000:]
            if out:
                print("    " + out.replace("\n", "\n    "))
            if not ok:
                failed.append(name)
    finally:
        cleanup(prefix, store)
    print("ALL PASS" if not failed else f"FAILED: {', '.join(failed)}")
    if out_dir is not None:
        summary = {
            "target": target,
            "store": "local" if store == LOCAL_STORE_URL else "env file",
            "when": datetime.datetime.now().isoformat(timespec="seconds"),
            "all_pass": not failed,
            "samples": results,
        }
        (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        lines = [f"# Samples on {target}, {summary['when']}", "", "| Sample | Result | Secs |"]
        lines += ["|---|---|---|"] + [
            f"| {r['sample']} | {'PASS' if r['ok'] else 'FAIL'} | {r['secs']} |" for r in results
        ]
        (out_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"results saved in {out_dir}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
