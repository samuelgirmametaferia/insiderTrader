"""Reference Python metric used by the out-of-process worker."""

from insidertrader.metric_sdk.contracts import MetricContext, MetricOutput


def evaluate(context: MetricContext) -> MetricOutput:
    """Emit a bounded one-period return score with explicit uncertainty."""
    value = context.feature("return")
    score = max(-1.0, min(1.0, value))
    return MetricOutput(
        metric_id="returns.python.v1",
        instrument_id=context.instrument_id,
        generated_mono_ns=context.now_mono_ns,
        ttl_ns=1_000_000_000,
        score=score,
        confidence=1.0,
        uncertainty=abs(value - score),
    )
