//! Typed strategy proposals. Strategies recommend targets; they never submit orders.

#![forbid(unsafe_code)]

use insider_common_types::{InstrumentId, MonoTime, ProposalId};
use insider_metric_sdk::MetricOutput;
use std::sync::atomic::{AtomicU64, Ordering};

/// Supported strategy recommendation actions.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Explicitly abstain.
    NoAction,
    /// Request an absolute target quantity in canonical ticks.
    TargetQuantity {
        /// Absolute target quantity in canonical ticks.
        quantity_ticks: i64,
    },
    /// Request a portfolio weight.
    TargetWeight {
        /// Target portfolio weight in `[-1, 1]`.
        weight: f64,
    },
    /// Increase exposure by canonical quantity ticks.
    Increase {
        /// Quantity to add in canonical ticks.
        quantity_ticks: i64,
    },
    /// Decrease exposure by canonical quantity ticks.
    Decrease {
        /// Quantity to remove in canonical ticks.
        quantity_ticks: i64,
    },
    /// Close the current exposure.
    Close,
}

/// Strategy proposal validation failure.
#[derive(Clone, Debug, PartialEq)]
pub enum ProposalError {
    /// Proposal identity or strategy version is missing.
    MissingIdentity,
    /// Action contains an invalid quantity or weight.
    InvalidAction,
    /// Confidence is outside `[0, 1]` or non-finite.
    InvalidConfidence,
    /// TTL or horizon is invalid.
    InvalidHorizon,
}

/// Execution mode declared by a strategy package.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StrategyMode {
    /// Pure deterministic evaluation suitable for hot-path/replay execution.
    Deterministic,
    /// Evaluation may consume asynchronously refreshed context snapshots.
    Contextual,
}

/// Machine-checkable strategy package contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StrategyManifest {
    /// Immutable strategy ID including its version.
    pub strategy_id: String,
    /// Declared execution mode.
    pub mode: StrategyMode,
    /// Exact metric IDs consumed by the strategy.
    pub metric_ids: Vec<String>,
    /// Strategy IDs whose outputs this strategy consumes.
    pub strategy_dependencies: Vec<String>,
    /// Decision horizon in nanoseconds.
    pub horizon_ns: u64,
    /// Proposal freshness TTL in nanoseconds.
    pub ttl_ns: u64,
    /// Desired evaluation period in nanoseconds.
    pub period_ns: u64,
    /// Maximum evaluation latency in nanoseconds.
    pub deadline_ns: u64,
    /// Scheduler priority class.
    pub priority: StrategyPriority,
}

/// Scheduler priority declared by a strategy package.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StrategyPriority {
    /// Deadline-sensitive deterministic strategy.
    Fast,
    /// Normal decision-plane strategy.
    Normal,
    /// Asynchronous contextual/research strategy.
    Background,
}

impl StrategyManifest {
    /// Validates identity, dependencies, and scheduling bounds.
    ///
    /// # Errors
    /// Returns [`ProposalError`] when any manifest field is blank or invalid.
    pub fn validate(&self) -> Result<(), ProposalError> {
        if self.strategy_id.trim().is_empty()
            || self.horizon_ns == 0
            || self.ttl_ns == 0
            || self.ttl_ns > self.horizon_ns.saturating_mul(10)
            || self.period_ns == 0
            || self.deadline_ns == 0
            || self.deadline_ns > self.period_ns
            || self.metric_ids.iter().any(|id| id.trim().is_empty())
            || self
                .strategy_dependencies
                .iter()
                .any(|id| id.trim().is_empty())
        {
            return Err(ProposalError::InvalidHorizon);
        }
        let mut unique = std::collections::BTreeSet::new();
        if self.metric_ids.iter().any(|id| !unique.insert(id)) {
            return Err(ProposalError::InvalidAction);
        }
        let mut strategies = std::collections::BTreeSet::new();
        if self
            .strategy_dependencies
            .iter()
            .any(|id| !strategies.insert(id))
        {
            return Err(ProposalError::InvalidAction);
        }
        Ok(())
    }
}

/// A validated recommendation handed to coordinator/portfolio layers.
#[derive(Clone, Debug, PartialEq)]
pub struct Proposal {
    /// Immutable proposal identity.
    pub proposal_id: ProposalId,
    /// Immutable strategy ID/version.
    pub strategy_id: String,
    /// Instrument identity.
    pub instrument_id: InstrumentId,
    /// Action recommendation.
    pub action: Action,
    /// Confidence in `[0, 1]`.
    pub confidence: f64,
    /// Expected holding horizon.
    pub horizon_ns: u64,
    /// Expiry after which the proposal cannot be acted on.
    pub ttl_ns: u64,
    /// Metric evidence references.
    pub evidence: Vec<String>,
    /// Creation time.
    pub generated_mono: MonoTime,
}

impl Proposal {
    /// Validates a proposal before it enters the coordinator.
    ///
    /// # Errors
    /// Returns [`ProposalError`] for missing identity, invalid action/risk
    /// values, or invalid horizon/TTL.
    pub fn validate(&self, now: MonoTime) -> Result<(), ProposalError> {
        if self.strategy_id.trim().is_empty() || self.proposal_id.get() == 0 {
            return Err(ProposalError::MissingIdentity);
        }
        if !self.confidence.is_finite() || !(0.0..=1.0).contains(&self.confidence) {
            return Err(ProposalError::InvalidConfidence);
        }
        if self.horizon_ns == 0
            || self.ttl_ns == 0
            || self.ttl_ns > self.horizon_ns.saturating_mul(10)
        {
            return Err(ProposalError::InvalidHorizon);
        }
        match self.action {
            Action::NoAction | Action::Close => {}
            Action::TargetQuantity { quantity_ticks }
            | Action::Increase { quantity_ticks }
            | Action::Decrease { quantity_ticks }
                if quantity_ticks != 0 => {}
            Action::TargetWeight { weight }
                if weight.is_finite() && (-1.0..=1.0).contains(&weight) => {}
            _ => return Err(ProposalError::InvalidAction),
        }
        if now < self.generated_mono
            || now
                .as_nanos()
                .saturating_sub(self.generated_mono.as_nanos())
                >= self.ttl_ns
        {
            return Err(ProposalError::InvalidHorizon);
        }
        Ok(())
    }
}

/// Inputs visible to one strategy evaluation.
pub struct StrategyContext<'a> {
    /// Current monotonic time.
    pub now: MonoTime,
    /// Instrument being evaluated.
    pub instrument_id: InstrumentId,
    /// Fresh metric outputs keyed by metric ID.
    pub metrics: &'a [MetricOutput],
}

/// Strategy module boundary. Implementations return recommendations only.
pub trait Strategy: Send + Sync {
    /// Immutable strategy identifier/version.
    fn strategy_id(&self) -> &str;
    /// Declares the strategy's exact metric dependencies and timing bounds.
    /// Implementations that predate manifests receive a conservative
    /// deterministic default; production packages should override this.
    fn manifest(&self) -> StrategyManifest {
        StrategyManifest {
            strategy_id: self.strategy_id().to_owned(),
            mode: StrategyMode::Deterministic,
            metric_ids: Vec::new(),
            strategy_dependencies: Vec::new(),
            period_ns: 1,
            deadline_ns: 1,
            priority: StrategyPriority::Fast,
            horizon_ns: 1,
            ttl_ns: 1,
        }
    }
    /// Evaluates current evidence into one proposal or abstention.
    ///
    /// # Errors
    /// Returns [`ProposalError`] when a generated proposal is invalid.
    fn evaluate(&self, context: &StrategyContext<'_>) -> Result<Proposal, ProposalError>;
}

/// Deterministic single-metric threshold strategy suitable for a first live
/// strategy and for replay parity. It never submits orders itself.
pub struct ThresholdStrategy {
    strategy_id: String,
    metric_id: String,
    entry_threshold: f64,
    exit_threshold: f64,
    quantity_ticks: i64,
    horizon_ns: u64,
    ttl_ns: u64,
    next_proposal: AtomicU64,
}

impl ThresholdStrategy {
    /// Creates a threshold strategy. `entry_threshold` must exceed
    /// `exit_threshold`, and both must be positive and finite.
    #[must_use]
    pub fn new(
        strategy_id: impl Into<String>,
        metric_id: impl Into<String>,
        entry_threshold: f64,
        exit_threshold: f64,
        quantity_ticks: i64,
        horizon_ns: u64,
        ttl_ns: u64,
    ) -> Option<Self> {
        Self::new_with_proposal_seed(
            strategy_id,
            metric_id,
            entry_threshold,
            exit_threshold,
            quantity_ticks,
            horizon_ns,
            ttl_ns,
            1,
        )
    }

    /// Creates a threshold strategy with an explicit deterministic proposal
    /// sequence seed for durable live or replay boundaries.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_proposal_seed(
        strategy_id: impl Into<String>,
        metric_id: impl Into<String>,
        entry_threshold: f64,
        exit_threshold: f64,
        quantity_ticks: i64,
        horizon_ns: u64,
        ttl_ns: u64,
        proposal_seed: u64,
    ) -> Option<Self> {
        let strategy_id = strategy_id.into();
        let metric_id = metric_id.into();
        let quantity_ticks = quantity_ticks.checked_abs()?;
        if proposal_seed == 0
            || strategy_id.trim().is_empty()
            || metric_id.trim().is_empty()
            || !entry_threshold.is_finite()
            || !exit_threshold.is_finite()
            || entry_threshold <= exit_threshold
            || exit_threshold < 0.0
            || quantity_ticks == 0
            || horizon_ns == 0
            || ttl_ns == 0
            || ttl_ns > horizon_ns.saturating_mul(10)
        {
            return None;
        }
        Some(Self {
            strategy_id,
            metric_id,
            entry_threshold,
            exit_threshold,
            quantity_ticks,
            horizon_ns,
            ttl_ns,
            next_proposal: AtomicU64::new(proposal_seed),
        })
    }
}

impl Strategy for ThresholdStrategy {
    fn strategy_id(&self) -> &str {
        &self.strategy_id
    }

    fn manifest(&self) -> StrategyManifest {
        StrategyManifest {
            strategy_id: self.strategy_id.clone(),
            mode: StrategyMode::Deterministic,
            metric_ids: vec![self.metric_id.clone()],
            strategy_dependencies: Vec::new(),
            period_ns: self.ttl_ns,
            deadline_ns: self.ttl_ns,
            priority: StrategyPriority::Fast,
            horizon_ns: self.horizon_ns,
            ttl_ns: self.ttl_ns,
        }
    }

    fn evaluate(&self, context: &StrategyContext<'_>) -> Result<Proposal, ProposalError> {
        let metric = context.metrics.iter().find(|metric| {
            metric.metric_id == self.metric_id
                && metric.instrument_id == context.instrument_id
                && metric.is_fresh(context.now)
        });
        let (action, confidence, evidence) = if let Some(metric) = metric {
            let action = if metric.score >= self.entry_threshold {
                Action::Increase {
                    quantity_ticks: self.quantity_ticks,
                }
            } else if metric.score <= -self.entry_threshold {
                Action::Decrease {
                    quantity_ticks: self.quantity_ticks,
                }
            } else if metric.score.abs() <= self.exit_threshold {
                Action::Close
            } else {
                Action::NoAction
            };
            (
                action,
                (metric.confidence * (1.0 - metric.uncertainty).max(0.0)).clamp(0.0, 1.0),
                vec![format!("metric:{}", metric.metric_id)],
            )
        } else {
            // Missing or stale evidence is a valid abstention, not a worker
            // failure. The coordinator can account for this trigger without
            // creating an actionable target.
            (
                Action::NoAction,
                0.0,
                vec![format!("metric:{}:stale-or-missing", self.metric_id)],
            )
        };
        let proposal_id = ProposalId::new(u128::from(
            self.next_proposal.fetch_add(1, Ordering::Relaxed),
        ))
        .map_err(|_| ProposalError::MissingIdentity)?;
        let proposal = Proposal {
            proposal_id,
            strategy_id: self.strategy_id.clone(),
            instrument_id: context.instrument_id,
            action,
            confidence,
            horizon_ns: self.horizon_ns,
            ttl_ns: self.ttl_ns,
            evidence,
            generated_mono: context.now,
        };
        proposal.validate(context.now).map(|()| proposal)
    }
}

#[cfg(test)]
mod tests {
    use insider_common_types::{InstrumentId, MonoTime, ProposalId};

    use insider_metric_sdk::MetricOutput;

    use super::{Action, Proposal, ProposalError, Strategy, StrategyContext, ThresholdStrategy};

    fn proposal(action: Action) -> Option<Proposal> {
        Some(Proposal {
            proposal_id: ProposalId::new(1).ok()?,
            strategy_id: String::from("test.v1"),
            instrument_id: InstrumentId::new(1).ok()?,
            action,
            confidence: 0.8,
            horizon_ns: 1_000,
            ttl_ns: 100,
            evidence: vec![String::from("metric")],
            generated_mono: MonoTime::from_nanos(10),
        })
    }

    #[test]
    fn proposal_validation_accepts_targets_and_rejects_invalid_actions() {
        let Some(valid) = proposal(Action::TargetWeight { weight: 0.5 }) else {
            return;
        };
        assert!(valid.validate(MonoTime::from_nanos(20)).is_ok());
        let Some(invalid) = proposal(Action::TargetQuantity { quantity_ticks: 0 }) else {
            return;
        };
        assert_eq!(
            invalid.validate(MonoTime::from_nanos(20)),
            Err(ProposalError::InvalidAction)
        );
        let Some(expired) = proposal(Action::NoAction) else {
            return;
        };
        assert_eq!(
            expired.validate(MonoTime::from_nanos(111)),
            Err(ProposalError::InvalidHorizon)
        );
    }

    #[test]
    fn threshold_strategy_emits_typed_directional_proposals_from_fresh_metric() {
        let Some(instrument) = InstrumentId::new(7).ok() else {
            return;
        };
        let Some(strategy) =
            ThresholdStrategy::new("momentum.v1", "momentum", 0.5, 0.1, 10, 1_000, 100)
        else {
            return;
        };
        let metric = MetricOutput {
            metric_id: "momentum".into(),
            instrument_id: instrument,
            generated_mono: MonoTime::from_nanos(10),
            ttl_ns: 100,
            score: 0.8,
            confidence: 0.9,
            uncertainty: 0.1,
        };
        let result = strategy.evaluate(&StrategyContext {
            now: MonoTime::from_nanos(20),
            instrument_id: instrument,
            metrics: &[metric],
        });
        assert!(result.is_ok_and(|proposal| matches!(
            proposal.action,
            Action::Increase { quantity_ticks: 10 }
        )));
    }

    #[test]
    fn threshold_strategy_abstains_explicitly_when_evidence_is_stale_or_missing() {
        let Some(instrument) = InstrumentId::new(7).ok() else {
            return;
        };
        let Some(strategy) =
            ThresholdStrategy::new("momentum.v1", "momentum", 0.5, 0.1, 10, 1_000, 100)
        else {
            return;
        };
        let result = strategy.evaluate(&StrategyContext {
            now: MonoTime::from_nanos(20),
            instrument_id: instrument,
            metrics: &[],
        });
        assert!(result.is_ok_and(|proposal| {
            matches!(proposal.action, Action::NoAction)
                && proposal.confidence == 0.0
                && proposal
                    .evidence
                    .iter()
                    .any(|item| item.contains("stale-or-missing"))
        }));
    }
}
