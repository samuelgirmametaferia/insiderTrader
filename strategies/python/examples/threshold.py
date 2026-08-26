"""Reference Python strategy for the isolated strategy worker."""

from insidertrader.strategy_sdk.contracts import Action, Proposal, StrategyContext


def evaluate(context: StrategyContext) -> Proposal:
    """Convert the first declared metric into a bounded target proposal."""
    score = float(context.metrics[0]["score"]) if context.metrics else 0.0
    action = Action(kind="target_quantity", value=1 if score > 0 else -1 if score < 0 else None)
    if score == 0:
        action = Action(kind="no_action")
    return Proposal(
        proposal_id=context.now_mono_ns or 1,
        strategy_id="equity.python.threshold.v1",
        instrument_id=context.instrument_id,
        action=action,
        confidence=min(1.0, abs(score)),
        horizon_ns=60_000_000_000,
        ttl_ns=5_000_000_000,
        generated_mono_ns=context.now_mono_ns,
        evidence=("metric:score",),
    )
