"""Follow a release: the tag's `publish` workflow run, its jobs, then the version on PyPI.

Needs no `gh` and no token: it reads the public GitHub API (60 requests an hour without a
token, so it polls every 2 minutes; set `GITHUB_TOKEN` for more) and PyPI's JSON API.

    python scripts/watch_release.py v0.1.1            # wait for the run (90 min at most)
    python scripts/watch_release.py v0.1.1 --once     # one look, no waiting

Exit 0 when the run succeeded and PyPI lists the version; 1 when the run failed or the version
is missing; 2 when it timed out.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = "publish"


def repo_from_git(root: Path = ROOT) -> str:
    """`owner/name` of the `origin` remote (https or ssh URL)."""
    url = subprocess.run(
        ["git", "-C", str(root), "remote", "get-url", "origin"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()  # fmt: skip
    match = re.search(r"github\.com[:/](.+?)(?:\.git)?$", url)
    if not match:
        raise SystemExit(f"origin is not a GitHub URL: {url}")
    return match.group(1)


def get(url: str) -> Any:
    """GET a JSON document (GitHub's API headers; `GITHUB_TOKEN` when set)."""
    headers = {"User-Agent": "llm-queue-release-watch", "Accept": "application/vnd.github+json"}
    if os.environ.get("GITHUB_TOKEN") and "api.github.com" in url:
        headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as r:
        return json.load(r)


def find_run(runs: dict[str, Any], tag: str) -> dict[str, Any] | None:
    """The newest `publish` run of `tag` in a workflow-runs listing."""
    for run in runs.get("workflow_runs", []):
        if run.get("name") == WORKFLOW and run.get("head_branch") == tag:
            return dict(run)
    return None


def job_lines(jobs: dict[str, Any]) -> list[str]:
    """One `outcome  name` line per job."""
    return [f"  {job.get('conclusion') or job.get('status'):10} {job['name']}" for job in
            jobs.get("jobs", [])]  # fmt: skip


def on_pypi(info: dict[str, Any], version: str) -> bool:
    """True when PyPI's JSON for the project lists `version` with at least one file."""
    return bool(info.get("releases", {}).get(version))


def main() -> int:
    """Wait for the run, print its jobs, check PyPI."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("tag", help="the release tag, e.g. v0.1.1")
    parser.add_argument("--repo", default=None, help="owner/name (default: the origin remote)")
    parser.add_argument("--project", default="llm-queue", help="the PyPI project")
    parser.add_argument("--every", type=float, default=120, help="seconds between looks")
    parser.add_argument("--timeout", type=float, default=90 * 60, help="give up after (s)")
    parser.add_argument("--once", action="store_true", help="look once and report")
    args = parser.parse_args()
    repo = args.repo or repo_from_git()
    version = args.tag.removeprefix("v")
    deadline = time.monotonic() + args.timeout
    run = None
    while True:
        try:
            run = find_run(get(f"https://api.github.com/repos/{repo}/actions/runs?per_page=20"),
                           args.tag)  # fmt: skip
        except Exception as exc:  # a rate limit or the network: look again later
            print(f"{time.strftime('%H:%M')} look failed: {exc}", flush=True)
        if run:
            print(f"{time.strftime('%H:%M')} {run['status']} {run.get('conclusion')}", flush=True)
            if run["status"] == "completed" or args.once:
                break
        elif args.once:
            print(f"no {WORKFLOW} run for {args.tag} yet")
            return 2
        if time.monotonic() > deadline:
            print("timed out")
            return 2
        time.sleep(args.every)
    print(run["html_url"])
    print("\n".join(job_lines(get(run["jobs_url"] + "?per_page=50"))))
    listed = on_pypi(get(f"https://pypi.org/pypi/{args.project}/json"), version)
    print(f"PyPI {args.project} {version}: {'listed' if listed else 'not listed'}")
    return 0 if run.get("conclusion") == "success" and listed else 1


if __name__ == "__main__":
    sys.exit(main())
