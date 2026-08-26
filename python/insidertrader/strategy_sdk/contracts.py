"""Wire-compatible strategy proposal contracts.

The strategy boundary ends at a typed proposal. It deliberately has no broker
client, credentials, or order-submission API.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Literal, Mapping

from insidertrader.metric_sdk import MetricOutput

ActionKind = Literal["no_action", "target_quantity", "target_weight", "increase", "decrease", "close"]


class ProposalError(ValueError):
    """A strategy manifest or proposal failed validation."""


@dataclass(frozen=True, slots=True)
class Action:
    kind: ActionKind
    value: int | float | None = None

    def validate(self) -> None:
        if self.kind in {"no_action", "close"}:
            if self.value is not None:
                raise ProposalError("action value is not allowed")
            return
        if self.kind in {"target_quantity", "increase", "decrease"}:
            if not isinstance(self.value, int) or isinstance(self.value, bool) or self.value == 0:
                raise ProposalError("quantity action requires a non-zero integer")
            return
        if self.kind == "target_weight":
            if not isinstance(self.value, (int, float)) or isinstance(self.value, bool):
                raise ProposalError("weight action requires a number")
            if not isfinite(float(self.value)) or not -1.0 <= float(self.value) <= 1.0:
                raise ProposalError("target weight must be finite and within [-1, 1]")
            return
        raise ProposalError("unknown strategy action")


@dataclass(frozen=True, slots=True)
class StrategyManifest:
    strategy_id: str
    mode: Literal["deterministic", "contextual"]
    metric_ids: tuple[str, ...]
    strategy_dependencies: tuple[str, ...]
    horizon_ns: int
    ttl_ns: int
    period_ns: int
    deadline_ns: int
    priority: Literal["FAST", "NORMAL", "BACKGROUND"]

    def validate(self) -> None:
        if not self.strategy_id.strip() or self.mode not in {"deterministic", "contextual"}:
            raise ProposalError("strategy identity/mode is invalid")
        if min(self.horizon_ns, self.ttl_ns, self.period_ns, self.deadline_ns) <= 0:
            raise ProposalError("strategy timing values must be positive")
        if self.ttl_ns > self.horizon_ns * 10 or self.deadline_ns > self.period_ns:
            raise ProposalError("strategy ttl/deadline exceeds declared horizon/period")
        if self.priority not in {"FAST", "NORMAL", "BACKGROUND"}:
            raise ProposalError("unknown strategy priority")
        if any(not item.strip() for item in (*self.metric_ids, *self.strategy_dependencies)):
            raise ProposalError("strategy dependencies must be non-empty")
        if len(set(self.metric_ids)) != len(self.metric_ids) or len(set(self.strategy_dependencies)) != len(self.strategy_dependencies):
            raise ProposalError("strategy dependencies must be unique")


@dataclass(frozen=True, slots=True)
class Proposal:
    proposal_id: int
    strategy_id: str
    instrument_id: int
    action: Action
    confidence: float
    horizon_ns: int
    ttl_ns: int
    generated_mono_ns: int
    evidence: tuple[str, ...] = ()

    def to_wire(self) -> dict[str, object]:
        value = self.action.value
        return {
            "proposal_id": str(self.proposal_id),
            "strategy_id": self.strategy_id,
            "instrument_id": str(self.instrument_id),
            "action": {"type": self.action.kind, **({"value": value} if value is not None else {})},
            "confidence": self.confidence,
            "horizon_ns": self.horizon_ns,
            "ttl_ns": self.ttl_ns,
            "generated_mono_ns": self.generated_mono_ns,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """Bounded context supplied to an isolated Python strategy."""

    instrument_id: int
    now_mono_ns: int
    metrics: tuple[Mapping[str, object], ...]


def validate_proposal(proposal: Proposal, now_mono_ns: int) -> Proposal:
    proposal.action.validate()
    if proposal.proposal_id <= 0 or not proposal.strategy_id.strip() or proposal.instrument_id <= 0:
        raise ProposalError("proposal identity is invalid")
    if not isfinite(proposal.confidence) or not 0.0 <= proposal.confidence <= 1.0:
        raise ProposalError("proposal confidence is invalid")
    if proposal.horizon_ns <= 0 or proposal.ttl_ns <= 0 or proposal.ttl_ns > proposal.horizon_ns * 10:
        raise ProposalError("proposal horizon/ttl is invalid")
    if proposal.generated_mono_ns < 0 or now_mono_ns < proposal.generated_mono_ns:
        raise ProposalError("proposal timestamp is in the future")
    if now_mono_ns - proposal.generated_mono_ns >= proposal.ttl_ns:
        raise ProposalError("proposal is expired")
    if any(not evidence.strip() for evidence in proposal.evidence):
        raise ProposalError("proposal evidence references must be non-empty")
    return proposal


def fresh_metrics(metrics: tuple[MetricOutput, ...], now_mono_ns: int) -> tuple[MetricOutput, ...]:
    """Return only outputs valid at the injected decision time."""
    return tuple(metric for metric in metrics if metric.is_fresh(now_mono_ns))
