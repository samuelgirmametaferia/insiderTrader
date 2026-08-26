"""Deterministic threshold strategy usable by research and worker processes."""

from __future__ import annotations

from dataclasses import dataclass

from insidertrader.metric_sdk import MetricOutput

from .contracts import Action, Proposal, ProposalError, StrategyManifest, validate_proposal


@dataclass(slots=True)
class ThresholdStrategy:
    """Emit typed directional proposals from one fresh bounded metric."""

    strategy_id: str
    metric_id: str
    entry_threshold: float
    exit_threshold: float
    quantity_ticks: int
    horizon_ns: int
    ttl_ns: int
    _next_proposal_id: int = 1

    def __post_init__(self) -> None:
        if not self.strategy_id.strip() or not self.metric_id.strip():
            raise ProposalError("strategy and metric IDs are required")
        if self.entry_threshold <= self.exit_threshold or self.exit_threshold < 0.0:
            raise ProposalError("entry threshold must exceed non-negative exit threshold")
        if self.quantity_ticks == 0 or self.horizon_ns <= 0 or self.ttl_ns <= 0:
            raise ProposalError("strategy quantity and timing must be positive")
        self.quantity_ticks = abs(self.quantity_ticks)

    @property
    def manifest(self) -> StrategyManifest:
        return StrategyManifest(
            strategy_id=self.strategy_id,
            mode="deterministic",
            metric_ids=(self.metric_id,),
            strategy_dependencies=(),
            horizon_ns=self.horizon_ns,
            ttl_ns=self.ttl_ns,
            period_ns=self.ttl_ns,
            deadline_ns=self.ttl_ns,
            priority="FAST",
        )

    def evaluate(self, instrument_id: int, now_mono_ns: int, metrics: tuple[MetricOutput, ...]) -> Proposal:
        metric = next(
            (
                item
                for item in metrics
                if item.metric_id == self.metric_id
                and item.instrument_id == instrument_id
                and item.is_fresh(now_mono_ns)
            ),
            None,
        )
        if metric is None:
            action = Action("no_action")
            confidence = 0.0
            evidence = (f"metric:{self.metric_id}:stale-or-missing",)
        elif metric.score >= self.entry_threshold:
            action = Action("increase", self.quantity_ticks)
            confidence = max(0.0, min(1.0, metric.confidence * (1.0 - metric.uncertainty)))
            evidence = (f"metric:{self.metric_id}",)
        elif metric.score <= -self.entry_threshold:
            action = Action("decrease", self.quantity_ticks)
            confidence = max(0.0, min(1.0, metric.confidence * (1.0 - metric.uncertainty)))
            evidence = (f"metric:{self.metric_id}",)
        elif abs(metric.score) <= self.exit_threshold:
            action = Action("close")
            confidence = max(0.0, min(1.0, metric.confidence * (1.0 - metric.uncertainty)))
            evidence = (f"metric:{self.metric_id}",)
        else:
            action = Action("no_action")
            confidence = 0.0
            evidence = (f"metric:{self.metric_id}:between-thresholds",)
        proposal = Proposal(
            proposal_id=self._next_proposal_id,
            strategy_id=self.strategy_id,
            instrument_id=instrument_id,
            action=action,
            confidence=confidence,
            horizon_ns=self.horizon_ns,
            ttl_ns=self.ttl_ns,
            generated_mono_ns=now_mono_ns,
            evidence=evidence,
        )
        self._next_proposal_id += 1
        return validate_proposal(proposal, now_mono_ns)
