"""Conservative starter strategy for the Python worker boundary."""

from __future__ import annotations

from insidertrader.strategy_sdk.contracts import Action, Proposal, StrategyContext


STRATEGY_ID = "equity.python.momentum_threshold.v1"
HORIZON_NS = 900_000_000_000
TTL_NS = 5_000_000_000
ENTRY_THRESHOLD = 0.002


def evaluate(context: StrategyContext) -> Proposal:
    """Emit a target recommendation, or explicit NoAction when neutral."""
    metric = next(
        (item for item in context.metrics if item.get("metric_id") == "momentum.return_clamped.v1"),
        None,
    )
    score = float(metric["score"]) if metric is not None else 0.0
    if score >= ENTRY_THRESHOLD:
        action = Action(kind="target_quantity", value=1)
    elif score <= -ENTRY_THRESHOLD:
        action = Action(kind="target_quantity", value=-1)
    else:
        action = Action(kind="no_action")
    return Proposal(
        proposal_id=context.now_mono_ns or 1,
        strategy_id=STRATEGY_ID,
        instrument_id=context.instrument_id,
        action=action,
        confidence=min(1.0, abs(score)),
        horizon_ns=HORIZON_NS,
        ttl_ns=TTL_NS,
        generated_mono_ns=context.now_mono_ns,
        evidence=("metric:momentum.return_clamped.v1",),
    )
