"""Install llm-queue into a fresh virtualenv and check it imports: the post-release smoke test.

Builds a throwaway venv with `uv` (Python 3.12 by default), installs the package (a version
from PyPI, from TestPyPI, or a local wheel), then checks the version, the public API, the
LangChain adapters (with the `langchain-aws` extra) and the `llm-queue` command. The venv is
deleted afterwards unless `--keep`.

    python scripts/smoke_install.py 0.1.1                  # from PyPI
    python scripts/smoke_install.py 0.1.1 --testpypi       # the wheel from TestPyPI
    python scripts/smoke_install.py dist/llm_queue-0.1.1-py3-none-any.whl
    python scripts/smoke_install.py 0.1.1 --python 3.14 --extras langchain-aws,http
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

CHECK = """
import sys
import llm_queue
from llm_queue import LLMQueue, CallFailed, PurgeService, PurgeReport, PurgeRefused
want = sys.argv[1]
assert want in ("", llm_queue.__version__), (llm_queue.__version__, want)
if "langchain" in sys.argv[2]:
    from llm_queue.langchain import QueuedBedrockEmbeddings, QueuedChatBedrockConverse
print("llm-queue", llm_queue.__version__, "imports OK")
"""


def requirement(target: str, extras: str) -> tuple[str, str]:
    """(pip requirement, expected version) for a version, a wheel path or `latest`."""
    tail = f"[{extras}]" if extras else ""
    if target.endswith(".whl"):
        wheel = Path(target).resolve()
        version = wheel.name.split("-")[1]
        return f"llm-queue{tail} @ {wheel.as_uri()}", version
    if target == "latest":
        return f"llm-queue{tail}", ""
    return f"llm-queue{tail}=={target}", target


def commands(venv: Path, req: str, *, python: str, testpypi: bool) -> list[list[str]]:
    """The uv commands that build the venv and install `req` into it."""
    py = str(venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python"))
    install = ["uv", "pip", "install", "--python", py, "--prerelease=allow", req]
    if testpypi:  # the package from TestPyPI, its dependencies from PyPI
        install += ["--index", "https://test.pypi.org/simple/", "--index-strategy",
                    "unsafe-best-match"]  # fmt: skip
    return [["uv", "venv", "-q", "--python", python, str(venv)], install]


def main() -> int:
    """Build, install, check; exit 1 on any failure."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("target", help="a version (0.1.1), `latest`, or a wheel path")
    parser.add_argument("--python", default="3.12")
    parser.add_argument("--extras", default="langchain-aws", help="comma list ('' for none)")
    parser.add_argument("--testpypi", action="store_true")
    parser.add_argument("--keep", action="store_true", help="keep the venv and print its path")
    args = parser.parse_args()
    req, version = requirement(args.target, args.extras)
    venv = Path(tempfile.mkdtemp(prefix="llmq-smoke-")) / "venv"
    try:
        for cmd in commands(venv, req, python=args.python, testpypi=args.testpypi):
            print("$", " ".join(cmd), flush=True)
            subprocess.run(cmd, check=True)
        bin_dir = venv / ("Scripts" if sys.platform == "win32" else "bin")
        py = bin_dir / ("python.exe" if sys.platform == "win32" else "python")
        subprocess.run([str(py), "-c", CHECK, version, args.extras], check=True)
        cli = bin_dir / ("llm-queue.exe" if sys.platform == "win32" else "llm-queue")
        subprocess.run([str(cli), "--help"], check=True, stdout=subprocess.DEVNULL)
        print("llm-queue command OK")
    except subprocess.CalledProcessError as exc:
        print(f"FAIL: {exc}")
        return 1
    finally:
        if args.keep:
            print(f"venv kept: {venv}")
        else:
            shutil.rmtree(venv.parent, ignore_errors=True)
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
