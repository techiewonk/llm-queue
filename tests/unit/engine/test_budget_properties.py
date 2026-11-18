"""Property tests of the Python-side budget math (RESCAN T7): estimate, used tokens, refunds.

The Lua admission scripts read the store clock (`TIME`) themselves, so their window math can
only be property-tested once a clock can be injected (Handoff); these cover the helpers the
runtime uses to reserve and settle tokens.
"""

from __future__ import annotations

import math
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from llm_queue.clients.providers import Usage
from llm_queue.engine.tokens import Estimate, estimate, refund, used_tokens

PROPS = settings(database=None, deadline=None, max_examples=300)
tokens = st.integers(min_value=0, max_value=2_000_000)
weights = st.floats(min_value=0.0, max_value=10.0, allow_nan=False)
outcomes = st.sampled_from(["ok", "unsent", "throttle", "fail", "cut"])


@PROPS
@given(st.integers(0, 100_000), st.integers(0, 100_000), outcomes, st.none() | tokens)
def test_refund_never_exceeds_the_reservation(
    inputs: int, output: int, outcome: str, used: int | None
) -> None:
    """No outcome gives back more than was taken; only a call never sent returns the request."""
    reserved = Estimate(inputs, output)
    back = refund(reserved, outcome, used)
    assert back.tokens <= reserved.total
    assert back.requests == (1 if outcome == "unsent" else 0)
    charged = reserved.total - back.tokens
    if outcome == "ok" and used is not None:
        assert charged == used  # corrected to the real usage
    elif outcome != "ok":
        assert 0 <= charged <= reserved.total  # never a negative charge
    if outcome == "fail":
        assert charged == inputs  # the input is kept, the output given back


@PROPS
@given(st.text(max_size=400), st.none() | st.integers(1, 100_000), st.integers(1, 8192), weights)
def test_estimate_reserves_input_plus_weighted_output(
    text: str, max_tokens: int | None, default_out: int, weight: float
) -> None:
    params: dict[str, Any] = {"messages": [{"role": "user", "content": [{"text": text}]}]}
    if max_tokens is not None:
        params["inferenceConfig"] = {"maxTokens": max_tokens}
    chat = estimate(
        "converse",
        params,
        embedding=False,
        default_output_tokens=default_out,
        output_weight=weight,
    )
    assert chat.output == math.ceil((max_tokens or default_out) * weight)
    assert chat.total == chat.input + chat.output
    assert chat.input >= 0
    emb = estimate("converse", params, embedding=True, default_output_tokens=default_out)
    assert emb == Estimate(chat.input, 0)  # embeddings reserve no output


@PROPS
@given(st.none() | tokens, st.none() | tokens, tokens, tokens, weights)
def test_used_tokens_counts_cache_reads_only_when_asked(
    inp: int | None, out: int | None, read: int, write: int, weight: float
) -> None:
    usage = Usage(
        input_tokens=inp, output_tokens=out, cache_read_tokens=read, cache_write_tokens=write
    )
    plain = used_tokens(usage, output_weight=weight)
    cached = used_tokens(usage, output_weight=weight, count_cached=True)
    if inp is None and out is None:
        assert plain is None
        assert cached is None
        return
    assert plain is not None
    assert cached == plain + read
    assert plain == (inp or 0) + write + math.ceil((out or 0) * weight)
    assert plain >= 0
