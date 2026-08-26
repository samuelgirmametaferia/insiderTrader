"""Wire-compatible metric contracts.

Metrics are descriptive/predictive computations. They cannot submit orders and
their output is accepted only after the same bounds and freshness checks used by
the Rust host have succeeded.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Mapping


class MetricError(ValueError):
    """A metric manifest, context, or output failed validation."""


@dataclass(frozen=True, slots=True)
class MetricDescriptor:
    metric_id: str
    inputs: tuple[str, ...]
    ttl_ns: int
    min_score: float | None = None
    max_score: float | None = None

    def validate(self) -> None:
        if not self.metric_id.strip() or self.ttl_ns <= 0:
            raise MetricError("metric descriptor identity/ttl is invalid")
        if any(not item.strip() for item in self.inputs):
            raise MetricError("metric input names must be non-empty")
        if self.min_score is not None and not isfinite(self.min_score):
            raise MetricError("metric minimum is non-finite")
        if self.max_score is not None and not isfinite(self.max_score):
            raise MetricError("metric maximum is non-finite")
        if self.min_score is not None and self.max_score is not None and self.min_score > self.max_score:
            raise MetricError("metric score bounds are inverted")


@dataclass(frozen=True, slots=True)
class MetricManifest:
    descriptor: MetricDescriptor
    period_ns: int
    deadline_ns: int
    budget_ns: int
    priority: str = "NORMAL"

    def validate(self) -> None:
        self.descriptor.validate()
        if self.period_ns <= 0 or self.deadline_ns <= 0 or self.budget_ns <= 0:
            raise MetricError("metric scheduling values must be positive")
        if self.budget_ns > self.deadline_ns:
            raise MetricError("metric budget exceeds deadline")
        if self.priority not in {"FAST", "NORMAL", "BACKGROUND"}:
            raise MetricError("unknown metric priority")


@dataclass(frozen=True, slots=True)
class MetricContext:
    instrument_id: int
    now_mono_ns: int
    features: Mapping[str, float]

    def feature(self, name: str) -> float:
        try:
            value = float(self.features[name])
        except (KeyError, TypeError, ValueError) as error:
            raise MetricError(f"missing metric feature: {name}") from error
        if not isfinite(value):
            raise MetricError(f"non-finite metric feature: {name}")
        return value


@dataclass(frozen=True, slots=True)
class MetricOutput:
    metric_id: str
    instrument_id: int
    generated_mono_ns: int
    ttl_ns: int
    score: float
    confidence: float
    uncertainty: float

    def is_fresh(self, now_mono_ns: int) -> bool:
        age = now_mono_ns - self.generated_mono_ns
        return age >= 0 and age <= self.ttl_ns

    def to_wire(self) -> dict[str, object]:
        """Return the bounded JSON object consumed by a worker bridge."""
        return {
            "metric_id": self.metric_id,
            "instrument_id": str(self.instrument_id),
            "generated_mono_ns": self.generated_mono_ns,
            "ttl_ns": self.ttl_ns,
            "score": self.score,
            "confidence": self.confidence,
            "uncertainty": self.uncertainty,
        }


def validate_output(output: MetricOutput, descriptor: MetricDescriptor) -> MetricOutput:
    """Validate and return an output; no partially valid output is published."""
    descriptor.validate()
    if output.metric_id != descriptor.metric_id:
        raise MetricError("metric output identity mismatch")
    if output.instrument_id <= 0 or output.generated_mono_ns < 0:
        raise MetricError("metric output identity/time is invalid")
    if output.ttl_ns <= 0 or output.ttl_ns > descriptor.ttl_ns:
        raise MetricError("metric output ttl is invalid")
    if not isfinite(output.score) or not isfinite(output.confidence) or not isfinite(output.uncertainty):
        raise MetricError("metric output contains a non-finite value")
    if not 0.0 <= output.confidence <= 1.0 or output.uncertainty < 0.0:
        raise MetricError("metric confidence/uncertainty is invalid")
    if descriptor.min_score is not None and output.score < descriptor.min_score:
        raise MetricError("metric score is below the declared minimum")
    if descriptor.max_score is not None and output.score > descriptor.max_score:
        raise MetricError("metric score is above the declared maximum")
    return output
