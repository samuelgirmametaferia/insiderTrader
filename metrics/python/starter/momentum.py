"""Small deterministic momentum metric for the isolated Python worker.

The score is deliberately bounded so it is safe to compose with other
evidence. It is a return signal, not an order instruction.
"""

from __future__ import annotations

from insidertrader.metric_sdk.contracts import MetricContext, MetricOutput


METRIC_ID = "momentum.return_clamped.v1"
TTL_NS = 1_000_000_000


def evaluate(context: MetricContext) -> MetricOutput:
    """Convert the latest return into a bounded momentum score."""
    value = context.feature("return")
    score = max(-1.0, min(1.0, value))
    return MetricOutput(
        metric_id=METRIC_ID,
        instrument_id=context.instrument_id,
        generated_mono_ns=context.now_mono_ns,
        ttl_ns=TTL_NS,
        score=score,
        confidence=1.0,
        uncertainty=max(0.0, abs(value) - 1.0),
    )
