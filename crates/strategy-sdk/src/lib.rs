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

/// Scheduling behavior when a declared metric snapshot is unavailable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MissingEvidencePolicy {
    /// Do not invoke implementations that cannot safely evaluate incomplete
    /// snapshots. This is the compatibility default for existing packages.
    SkipEvaluation,
    /// Invoke the strategy so it can emit an explicit, attributed `NoAction`.
    EvaluateNoAction,
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
    /// Behavior when one of the declared metrics is absent or stale.
    pub missing_evidence: MissingEvidencePolicy,
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
            missing_evidence: MissingEvidencePolicy::SkipEvaluation,
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
            missing_evidence: MissingEvidencePolicy::EvaluateNoAction,
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

/// Typed reasons emitted by the conservative starter trend strategy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrendRationale {
    /// One or more declared metric snapshots were missing, stale, or for a
    /// different instrument.
    MissingOrStaleEvidence,
    /// Metric warm-up confidence has not reached the configured threshold.
    EvidenceWarmingUp,
    /// A metric violated its declared finite/range semantics.
    InvalidEvidence,
    /// Current relative spread exceeds the strategy's liquidity guard.
    SpreadGuard,
    /// Trend magnitude is below the entry threshold.
    TrendBelowEntry,
    /// Trend magnitude is within the explicit close band.
    TrendNeutral,
    /// Positive trend passed evidence and liquidity checks.
    LongTrend,
    /// Negative trend passed evidence and liquidity checks.
    ShortTrend,
    /// Target quantity was reduced to the configured volatility budget.
    VolatilityScaled,
    /// Volatility scaling reduced the target below one canonical quantity tick.
    RiskBudgetTooSmall,
}

impl TrendRationale {
    /// Stable code recorded in proposal evidence and journals.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::MissingOrStaleEvidence => "MISSING_OR_STALE_EVIDENCE",
            Self::EvidenceWarmingUp => "EVIDENCE_WARMING_UP",
            Self::InvalidEvidence => "INVALID_EVIDENCE",
            Self::SpreadGuard => "SPREAD_GUARD",
            Self::TrendBelowEntry => "TREND_BELOW_ENTRY",
            Self::TrendNeutral => "TREND_NEUTRAL",
            Self::LongTrend => "LONG_TREND",
            Self::ShortTrend => "SHORT_TREND",
            Self::VolatilityScaled => "VOLATILITY_SCALED",
            Self::RiskBudgetTooSmall => "RISK_BUDGET_TOO_SMALL",
        }
    }
}

/// Immutable configuration for [`VolatilityScaledTrendStrategy`].
#[derive(Clone, Debug, PartialEq)]
pub struct VolatilityScaledTrendConfig {
    /// Immutable strategy identity including version.
    pub strategy_id: String,
    /// Normalized fast/slow trend metric identity.
    pub trend_metric_id: String,
    /// Normalized realized-volatility/ATR metric identity.
    pub volatility_metric_id: String,
    /// Relative bid/ask spread metric identity.
    pub spread_metric_id: String,
    /// Absolute normalized trend required to open a target.
    pub entry_threshold: f64,
    /// Absolute normalized trend at or below which exposure should close.
    pub exit_threshold: f64,
    /// Maximum accepted relative bid/ask spread.
    pub max_spread: f64,
    /// Normalized volatility budget used to scale target quantity.
    pub target_volatility: f64,
    /// Minimum adjusted confidence required from every metric.
    pub min_confidence: f64,
    /// Maximum absolute target in canonical quantity ticks.
    pub base_quantity_ticks: i64,
    /// Expected holding horizon.
    pub horizon_ns: u64,
    /// Proposal freshness TTL.
    pub ttl_ns: u64,
}

/// Deterministic cross-asset starter strategy combining trend, volatility, and
/// liquidity evidence.
///
/// It emits absolute target quantities only. It never constructs or submits a
/// broker order, and it explicitly abstains while evidence is stale or warming
/// up.
pub struct VolatilityScaledTrendStrategy {
    config: VolatilityScaledTrendConfig,
    base_quantity_ticks: u32,
    next_proposal: AtomicU64,
}

impl VolatilityScaledTrendStrategy {
    /// Builds a strategy with a deterministic proposal sequence starting at 1.
    #[must_use]
    pub fn new(config: VolatilityScaledTrendConfig) -> Option<Self> {
        Self::new_with_proposal_seed(config, 1)
    }

    /// Builds a strategy with an explicit deterministic live/replay sequence.
    ///
    /// The strategy host serializes evaluation of an instance. Given the same
    /// ordered trigger tape and seed, live and replay therefore assign the
    /// same proposal IDs. Concurrent callers outside that host still receive
    /// unique IDs, but their relative ID assignment is intentionally not an
    /// ordering contract.
    #[must_use]
    pub fn new_with_proposal_seed(
        config: VolatilityScaledTrendConfig,
        proposal_seed: u64,
    ) -> Option<Self> {
        let ids = [
            config.strategy_id.as_str(),
            config.trend_metric_id.as_str(),
            config.volatility_metric_id.as_str(),
            config.spread_metric_id.as_str(),
        ];
        let unique_metrics = std::collections::BTreeSet::from([
            config.trend_metric_id.as_str(),
            config.volatility_metric_id.as_str(),
            config.spread_metric_id.as_str(),
        ]);
        let base_quantity_ticks = u32::try_from(config.base_quantity_ticks.checked_abs()?).ok()?;
        if proposal_seed == 0
            || ids.iter().any(|id| id.trim().is_empty() || id.len() > 128)
            || unique_metrics.len() != 3
            || !config.entry_threshold.is_finite()
            || !config.exit_threshold.is_finite()
            || !config.max_spread.is_finite()
            || !config.target_volatility.is_finite()
            || !config.min_confidence.is_finite()
            || !(0.0..=1.0).contains(&config.entry_threshold)
            || config.entry_threshold <= config.exit_threshold
            || config.exit_threshold < 0.0
            || !(0.0..=1.0).contains(&config.max_spread)
            || config.max_spread == 0.0
            || !(0.0..=1.0).contains(&config.target_volatility)
            || config.target_volatility == 0.0
            || !(0.0..=1.0).contains(&config.min_confidence)
            || base_quantity_ticks == 0
            || config.horizon_ns == 0
            || config.ttl_ns == 0
            || config.ttl_ns > config.horizon_ns.saturating_mul(10)
        {
            return None;
        }
        Some(Self {
            config,
            base_quantity_ticks,
            next_proposal: AtomicU64::new(proposal_seed),
        })
    }

    fn proposal(
        &self,
        context: &StrategyContext<'_>,
        action: Action,
        confidence: f64,
        evidence: Vec<String>,
    ) -> Result<Proposal, ProposalError> {
        let proposal_id = ProposalId::new(u128::from(
            self.next_proposal.fetch_add(1, Ordering::Relaxed),
        ))
        .map_err(|_| ProposalError::MissingIdentity)?;
        let proposal = Proposal {
            proposal_id,
            strategy_id: self.config.strategy_id.clone(),
            instrument_id: context.instrument_id,
            action,
            confidence,
            horizon_ns: self.config.horizon_ns,
            ttl_ns: self.config.ttl_ns,
            evidence,
            generated_mono: context.now,
        };
        proposal.validate(context.now).map(|()| proposal)
    }

    fn rationale(code: TrendRationale) -> String {
        format!("rationale:{}", code.code())
    }

    fn metric_evidence(metric: &MetricOutput) -> String {
        format!(
            "metric:{}:mono={}:score={:016x}:confidence={:016x}:uncertainty={:016x}",
            metric.metric_id,
            metric.generated_mono.as_nanos(),
            metric.score.to_bits(),
            metric.confidence.to_bits(),
            metric.uncertainty.to_bits()
        )
    }

    fn find_metric<'a>(
        context: &'a StrategyContext<'_>,
        metric_id: &str,
    ) -> Option<&'a MetricOutput> {
        let mut matches = context.metrics.iter().filter(|metric| {
            metric.metric_id == metric_id
                && metric.instrument_id == context.instrument_id
                && metric.is_fresh(context.now)
        });
        let metric = matches.next()?;
        matches.next().is_none().then_some(metric)
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn scaled_quantity(base_quantity_ticks: u32, scale: f64) -> Option<i64> {
        if !scale.is_finite() || !(0.0..=1.0).contains(&scale) {
            return None;
        }
        let scaled = (f64::from(base_quantity_ticks) * scale).floor();
        if !(0.0..=f64::from(base_quantity_ticks)).contains(&scaled) {
            return None;
        }
        // The range check plus the u32-sized input make this conversion exact
        // with respect to the strategy's canonical whole-tick output domain.
        Some(i64::from(scaled as u32))
    }
}

impl Strategy for VolatilityScaledTrendStrategy {
    fn strategy_id(&self) -> &str {
        &self.config.strategy_id
    }

    fn manifest(&self) -> StrategyManifest {
        StrategyManifest {
            strategy_id: self.config.strategy_id.clone(),
            mode: StrategyMode::Deterministic,
            metric_ids: vec![
                self.config.trend_metric_id.clone(),
                self.config.volatility_metric_id.clone(),
                self.config.spread_metric_id.clone(),
            ],
            missing_evidence: MissingEvidencePolicy::EvaluateNoAction,
            strategy_dependencies: Vec::new(),
            horizon_ns: self.config.horizon_ns,
            ttl_ns: self.config.ttl_ns,
            period_ns: self.config.ttl_ns,
            deadline_ns: self.config.ttl_ns.min(10_000_000),
            priority: StrategyPriority::Fast,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn evaluate(&self, context: &StrategyContext<'_>) -> Result<Proposal, ProposalError> {
        let Some(trend) = Self::find_metric(context, &self.config.trend_metric_id) else {
            return self.proposal(
                context,
                Action::NoAction,
                0.0,
                vec![
                    Self::rationale(TrendRationale::MissingOrStaleEvidence),
                    format!("metric:{}:stale-or-missing", self.config.trend_metric_id),
                ],
            );
        };
        let Some(volatility) = Self::find_metric(context, &self.config.volatility_metric_id) else {
            return self.proposal(
                context,
                Action::NoAction,
                0.0,
                vec![
                    Self::rationale(TrendRationale::MissingOrStaleEvidence),
                    format!(
                        "metric:{}:stale-or-missing",
                        self.config.volatility_metric_id
                    ),
                ],
            );
        };
        let Some(spread) = Self::find_metric(context, &self.config.spread_metric_id) else {
            return self.proposal(
                context,
                Action::NoAction,
                0.0,
                vec![
                    Self::rationale(TrendRationale::MissingOrStaleEvidence),
                    format!("metric:{}:stale-or-missing", self.config.spread_metric_id),
                ],
            );
        };
        let metrics = [trend, volatility, spread];
        let valid = metrics.iter().all(|metric| {
            metric.score.is_finite()
                && metric.confidence.is_finite()
                && metric.uncertainty.is_finite()
                && (0.0..=1.0).contains(&metric.confidence)
                && metric.uncertainty >= 0.0
        }) && (-1.0..=1.0).contains(&trend.score)
            && volatility.score >= 0.0
            && spread.score >= 0.0;
        let mut evidence = metrics
            .iter()
            .map(|metric| Self::metric_evidence(metric))
            .collect::<Vec<_>>();
        if !valid {
            evidence.push(Self::rationale(TrendRationale::InvalidEvidence));
            return self.proposal(context, Action::NoAction, 0.0, evidence);
        }
        let adjusted_confidence = metrics
            .iter()
            .map(|metric| metric.confidence / (1.0 + metric.uncertainty))
            .fold(1.0_f64, f64::min)
            .clamp(0.0, 1.0);
        if adjusted_confidence < self.config.min_confidence {
            evidence.push(Self::rationale(TrendRationale::EvidenceWarmingUp));
            return self.proposal(context, Action::NoAction, adjusted_confidence, evidence);
        }
        if spread.score > self.config.max_spread {
            evidence.push(Self::rationale(TrendRationale::SpreadGuard));
            return self.proposal(context, Action::NoAction, adjusted_confidence, evidence);
        }
        let trend_magnitude = trend.score.abs();
        if trend_magnitude <= self.config.exit_threshold {
            evidence.push(Self::rationale(TrendRationale::TrendNeutral));
            return self.proposal(context, Action::Close, adjusted_confidence, evidence);
        }
        if trend_magnitude < self.config.entry_threshold {
            evidence.push(Self::rationale(TrendRationale::TrendBelowEntry));
            return self.proposal(context, Action::NoAction, adjusted_confidence, evidence);
        }
        let scale = if volatility.score <= self.config.target_volatility {
            1.0
        } else {
            self.config.target_volatility / volatility.score
        };
        let quantity = Self::scaled_quantity(self.base_quantity_ticks, scale)
            .ok_or(ProposalError::InvalidAction)?;
        if quantity == 0 {
            evidence.push(Self::rationale(TrendRationale::RiskBudgetTooSmall));
            return self.proposal(context, Action::NoAction, adjusted_confidence, evidence);
        }
        let (target, direction) = if trend.score.is_sign_positive() {
            (quantity, TrendRationale::LongTrend)
        } else {
            (
                quantity.checked_neg().ok_or(ProposalError::InvalidAction)?,
                TrendRationale::ShortTrend,
            )
        };
        evidence.push(Self::rationale(direction));
        if scale < 1.0 {
            evidence.push(Self::rationale(TrendRationale::VolatilityScaled));
        }
        self.proposal(
            context,
            Action::TargetQuantity {
                quantity_ticks: target,
            },
            adjusted_confidence,
            evidence,
        )
    }
}

#[cfg(test)]
mod tests {
    use insider_common_types::{InstrumentId, MonoTime, ProposalId};

    use insider_metric_sdk::MetricOutput;

    use super::{
        Action, Proposal, ProposalError, Strategy, StrategyContext, ThresholdStrategy,
        TrendRationale, VolatilityScaledTrendConfig, VolatilityScaledTrendStrategy,
    };

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

    fn starter_config() -> VolatilityScaledTrendConfig {
        VolatilityScaledTrendConfig {
            strategy_id: String::from("cross_asset.volatility_scaled_trend.v1"),
            trend_metric_id: String::from("trend.v1"),
            volatility_metric_id: String::from("atr.v1"),
            spread_metric_id: String::from("spread.v1"),
            entry_threshold: 0.01,
            exit_threshold: 0.002,
            max_spread: 0.005,
            target_volatility: 0.015,
            min_confidence: 0.65,
            base_quantity_ticks: 10,
            horizon_ns: 1_000,
            ttl_ns: 100,
        }
    }

    fn metric(
        id: &str,
        instrument_id: InstrumentId,
        score: f64,
        confidence: f64,
        uncertainty: f64,
        generated_mono: u64,
        ttl_ns: u64,
    ) -> MetricOutput {
        MetricOutput {
            metric_id: id.to_owned(),
            instrument_id,
            generated_mono: MonoTime::from_nanos(generated_mono),
            ttl_ns,
            score,
            confidence,
            uncertainty,
        }
    }

    #[test]
    fn volatility_scaled_trend_emits_bounded_target_with_typed_evidence() {
        let Some(instrument) = InstrumentId::new(9).ok() else {
            return;
        };
        let Some(strategy) = VolatilityScaledTrendStrategy::new(starter_config()) else {
            return;
        };
        let metrics = [
            metric("trend.v1", instrument, 0.02, 1.0, 0.0, 10, 100),
            metric("atr.v1", instrument, 0.03, 1.0, 0.0, 10, 100),
            metric("spread.v1", instrument, 0.001, 1.0, 0.0, 10, 100),
        ];
        let Ok(proposal) = strategy.evaluate(&StrategyContext {
            now: MonoTime::from_nanos(20),
            instrument_id: instrument,
            metrics: &metrics,
        }) else {
            return;
        };
        assert_eq!(
            proposal.action,
            Action::TargetQuantity { quantity_ticks: 5 }
        );
        assert!(
            proposal
                .evidence
                .iter()
                .any(|item| item == &format!("rationale:{}", TrendRationale::LongTrend.code()))
        );
        assert!(proposal.evidence.iter().any(|item| {
            item == &format!("rationale:{}", TrendRationale::VolatilityScaled.code())
        }));
        assert_eq!(strategy.manifest().metric_ids.len(), 3);
    }

    #[test]
    fn starter_strategy_abstains_for_stale_duplicate_warming_and_wide_evidence() {
        let Some(instrument) = InstrumentId::new(10).ok() else {
            return;
        };
        let Some(strategy) = VolatilityScaledTrendStrategy::new(starter_config()) else {
            return;
        };
        let base = [
            metric("trend.v1", instrument, 0.02, 1.0, 0.0, 10, 10),
            metric("atr.v1", instrument, 0.01, 1.0, 0.0, 10, 10),
            metric("spread.v1", instrument, 0.001, 1.0, 0.0, 10, 10),
        ];
        let at_boundary = strategy.evaluate(&StrategyContext {
            now: MonoTime::from_nanos(20),
            instrument_id: instrument,
            metrics: &base,
        });
        assert!(
            at_boundary
                .is_ok_and(|proposal| matches!(proposal.action, Action::TargetQuantity { .. }))
        );
        let stale = strategy.evaluate(&StrategyContext {
            now: MonoTime::from_nanos(21),
            instrument_id: instrument,
            metrics: &base,
        });
        assert!(stale.is_ok_and(|proposal| {
            matches!(proposal.action, Action::NoAction)
                && proposal.evidence.iter().any(|item| {
                    item == &format!(
                        "rationale:{}",
                        TrendRationale::MissingOrStaleEvidence.code()
                    )
                })
        }));

        let duplicates = [
            metric("trend.v1", instrument, 0.02, 1.0, 0.0, 20, 100),
            metric("trend.v1", instrument, 0.03, 1.0, 0.0, 20, 100),
            metric("atr.v1", instrument, 0.01, 1.0, 0.0, 20, 100),
            metric("spread.v1", instrument, 0.001, 1.0, 0.0, 20, 100),
        ];
        let duplicate = strategy.evaluate(&StrategyContext {
            now: MonoTime::from_nanos(21),
            instrument_id: instrument,
            metrics: &duplicates,
        });
        assert!(duplicate.is_ok_and(|proposal| matches!(proposal.action, Action::NoAction)));

        let warming = [
            metric("trend.v1", instrument, 0.02, 0.5, 0.0, 20, 100),
            metric("atr.v1", instrument, 0.01, 1.0, 0.0, 20, 100),
            metric("spread.v1", instrument, 0.001, 1.0, 0.0, 20, 100),
        ];
        let warming = strategy.evaluate(&StrategyContext {
            now: MonoTime::from_nanos(21),
            instrument_id: instrument,
            metrics: &warming,
        });
        assert!(warming.is_ok_and(|proposal| {
            matches!(proposal.action, Action::NoAction)
                && proposal.evidence.iter().any(|item| {
                    item == &format!("rationale:{}", TrendRationale::EvidenceWarmingUp.code())
                })
        }));

        let wide = [
            metric("trend.v1", instrument, 0.02, 1.0, 0.0, 20, 100),
            metric("atr.v1", instrument, 0.01, 1.0, 0.0, 20, 100),
            metric("spread.v1", instrument, 0.006, 1.0, 0.0, 20, 100),
        ];
        let wide = strategy.evaluate(&StrategyContext {
            now: MonoTime::from_nanos(21),
            instrument_id: instrument,
            metrics: &wide,
        });
        assert!(wide.is_ok_and(|proposal| {
            matches!(proposal.action, Action::NoAction)
                && proposal.evidence.iter().any(|item| {
                    item == &format!("rationale:{}", TrendRationale::SpreadGuard.code())
                })
        }));
    }

    #[test]
    fn starter_strategy_replay_is_identical_for_same_ordered_trigger_tape() {
        let Some(instrument) = InstrumentId::new(12).ok() else {
            return;
        };
        let Some(live) =
            VolatilityScaledTrendStrategy::new_with_proposal_seed(starter_config(), 41)
        else {
            return;
        };
        let Some(replay) =
            VolatilityScaledTrendStrategy::new_with_proposal_seed(starter_config(), 41)
        else {
            return;
        };
        for now in [20, 30, 40] {
            let metrics = [
                metric("trend.v1", instrument, -0.02, 0.9, 0.01, now, 100),
                metric("atr.v1", instrument, 0.01, 0.9, 0.01, now, 100),
                metric("spread.v1", instrument, 0.001, 1.0, 0.0, now, 100),
            ];
            let context = StrategyContext {
                now: MonoTime::from_nanos(now),
                instrument_id: instrument,
                metrics: &metrics,
            };
            assert_eq!(live.evaluate(&context), replay.evaluate(&context));
        }
    }
}
