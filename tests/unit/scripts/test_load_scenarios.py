"""Self-tests of the 0.2.0 load scenarios and gates: D1, T1 at scale, BL1, G-T1, G-T2 (W5c).

No stack is needed: the run plan, the same-age comparison of T1, the script ids G-T1 reads in
the slow log, and failover.py's image switch.
"""

from __future__ import annotations

import hashlib
import importlib
import math
import subprocess
import sys
from pathlib import Path
from typing import Any

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

scenarios: Any = importlib.import_module("scenarios")
gates: Any = importlib.import_module("gates")
failover: Any = importlib.import_module("failover")


def test_d1_runs_only_at_its_own_rate() -> None:
    """D1 sets its own rate: one unit (`at`) in every matrix, none for another load."""
    assert scenarios.plan(["D1"], None, "full") == [("D1", "at")]
    assert scenarios.plan(["D1"], None, "default") == [("D1", "at")]
    assert scenarios.plan(["D1"], ["burst"]) == []
    assert scenarios.BY_ID["D1"].runner is scenarios.rolling_deploy


def test_d1_held_model_keeps_up_and_waits_at_the_pause() -> None:
    """D1's held calls wait for budget but never pile up, and some wait when pod 1 pauses.

    The normal class gets `1 - priority_reserve` of the rate (chat default 0.2): at exactly the
    rate sent, normal calls got 12 a second of 15, the backlog was 4-5 s deep at the pause and
    the hand-off check failed on the budget, not on the drain.
    """
    held = scenarios.D1_HELD_MODEL
    reserve = held.get("priority_reserve", 0.2)
    normal_rate = held["rpm"] / 60 * (1 - reserve)
    sent_rate = scenarios.D1_RATE * scenarios.D1_HELD_SHARE
    assert normal_rate > sent_rate
    batch = sent_rate * scenarios.D1_HELD_EVERY_SECS
    burst = max(1, math.floor(held["rpm"] * (1 - reserve) * held["rate_burst"]))
    waits = (batch - burst) / normal_rate  # until the batch's last call starts
    assert scenarios.D1_POLL_SECS + 0.3 < waits < scenarios.D1_HANDOFF_SECS - 0.5
    for spans in (scenarios.D1_SECS, scenarios.D1_QUICK_SECS):
        assert spans["before"] % scenarios.D1_HELD_EVERY_SECS == 0  # the pause follows a batch


def test_t1_runs_under_every_load_with_swept_expiry() -> None:
    """T1 at scale under every load (burst included), with the swept leak check after it."""
    assert scenarios.plan(["T1"], None, "full") == [("T1", load) for load in scenarios.LOADS]
    t1 = scenarios.BY_ID["T1"]
    assert t1.env["LLMQ_KEY_EXPIRY_MODE"] == "swept"
    assert t1.extra is scenarios.swept_leak_check


def test_t1_operations_keep_the_design_times() -> None:
    """At 300 calls a tenant the move, cancel and kill fall at 10, 20 and 25 s."""
    if scenarios.T1_CALLS == 300:  # LLMQ_T1_CALLS unset
        assert scenarios.T1_AT == {"move": 10.0, "cancel": 20.0, "kill": 25.0}
    assert scenarios.T1_AT["move"] < scenarios.T1_AT["cancel"] < scenarios.T1_AT["kill"]


def _result(sent: float, wait: float | None) -> Any:
    if wait is None:
        return gates.Result(sent, False, "cancelled")
    return gates.Result(sent, True, wait_secs=wait)


def test_moved_first_counts_same_age_buckets() -> None:
    """Buckets by send second holding both kinds; a never-started call begins at infinity."""
    wave = scenarios.Wave()
    wave.calls = {
        "m1": ("A", _result(3.2, 1.0)),
        "u1": ("A", _result(3.6, 2.0)),
        "m2": ("A", _result(4.1, 1.0)),
        "u2": ("A", _result(4.4, None)),  # cancelled while waiting
        "m3": ("A", _result(5.0, 3.0)),  # no unmoved call of its age
        "b1": ("B", _result(3.1, 0.1)),  # B is not compared
    }
    wave.moved, wave.unmoved = {"m1", "m2", "m3"}, {"u1", "u2"}
    assert scenarios.moved_first(wave) == (2, 2)
    wave.calls["u1"] = ("A", _result(3.6, 0.1))  # the unmoved one began first
    assert scenarios.moved_first(wave) == (2, 1)


def test_b_wait_p95_reads_only_served_b_calls() -> None:
    """B's p95 wait ignores A and failed calls."""
    wave = scenarios.Wave()
    wave.calls = {
        "a": ("A", _result(0, 9.0)),
        "b-fail": ("B", _result(0, None)),
        **{f"b{i}": ("B", _result(0, i / 10)) for i in range(20)},
    }
    assert math.isclose(wave.b_wait_p95(), 1.9)


def test_exit_secs_is_infinite_past_the_cap() -> None:
    """A process still running at the cap has not exited within the grace period."""
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(5)"])
    try:
        assert scenarios._exit_secs(proc, scenarios.time.monotonic(), 0.2) == math.inf
    finally:
        proc.kill()
        proc.wait()
    done = subprocess.Popen([sys.executable, "-c", "pass"])
    assert scenarios._exit_secs(done, scenarios.time.monotonic(), 30) < 30


def test_new_gates_are_listed_and_slow_ones_left_out_by_quick() -> None:
    """BL1, G-T1, G-T2 are gates; the two long ones are left out by `--quick`."""
    assert {"BL1", "G-T1", "G-T2"} <= set(gates.GATES)
    assert {"BL1", "G-T2"} <= gates.SLOW_GATES
    assert "G-T1" not in gates.SLOW_GATES


def test_index_cost_reads_the_scripts_redis_runs() -> None:
    """The sha G-T1 matches in the slow log is the one redis-py registers for the script."""
    from llm_queue.engine.expiry import prelude
    from llm_queue.engine.requests import REGISTER_SEND

    want = hashlib.sha1((prelude("p:x") + REGISTER_SEND).encode()).hexdigest()
    assert gates._sha("p:x", REGISTER_SEND) == want
    assert gates._sha("p:x", REGISTER_SEND) != gates._sha("p:y", REGISTER_SEND)


def test_failover_image_switches_the_cli() -> None:
    """A Redis image runs redis-cli / redis-server; a Valkey one valkey-cli / valkey-server."""
    image, cli, server = failover.IMAGE, failover.CLI, failover.SERVER
    try:
        failover.use_image(failover.REDIS_74)
        assert (failover.IMAGE, failover.CLI, failover.SERVER) == (
            "redis:7.4-alpine",
            "redis-cli",
            "redis-server",
        )
        failover.use_image("valkey/valkey:8.1-alpine")
        assert (failover.CLI, failover.SERVER) == ("valkey-cli", "valkey-server")
    finally:
        failover.IMAGE, failover.CLI, failover.SERVER = image, cli, server
