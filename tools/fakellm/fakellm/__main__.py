"""CLI: `python -m fakellm --host 0.0.0.0 --port 8080 --scenario path --seed 42`.

Flags fall back to FAKELLM_HOST, FAKELLM_PORT, FAKELLM_SCENARIO and FAKELLM_SEED, so the
Docker image is configured from compose environment blocks only.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os

from fakellm.app import create_app
from fakellm.behaviour import Engine
from fakellm.scenario import Scenario, load_scenario
from fakellm.serve import serve


def main(argv: list[str] | None = None) -> None:
    """Parse flags, load the scenario and serve until interrupted."""
    env = os.environ
    parser = argparse.ArgumentParser(prog="fakellm", description=__doc__)
    parser.add_argument("--host", default=env.get("FAKELLM_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(env.get("FAKELLM_PORT", "8080")))
    parser.add_argument("--scenario", default=env.get("FAKELLM_SCENARIO") or None)
    seed_env = env.get("FAKELLM_SEED")
    parser.add_argument("--seed", type=int, default=int(seed_env) if seed_env else None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scenario = load_scenario(args.scenario) if args.scenario else Scenario()
    engine = Engine(scenario, seed=args.seed)
    logging.info(
        "fakellm: scenario=%s seed=%s keys=%d", args.scenario, engine.seed, len(scenario.models)
    )
    # Not web.run_app: it never re-opens a listening socket asyncio closed (see serve.py).
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve(create_app(engine=engine), args.host, args.port))


if __name__ == "__main__":
    main()
