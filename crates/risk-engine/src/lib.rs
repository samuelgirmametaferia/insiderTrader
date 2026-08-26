//! Deterministic pre-trade limits independent of strategy/LLM confidence.

#![forbid(unsafe_code)]

use insider_common_types::InstrumentId;
use insider_market_types::AssetClass;
use insider_portfolio::{Portfolio, Target};
use std::collections::BTreeMap;

/// Runtime risk state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum State {
    /// All risk-approved actions are allowed.
    Running,
    /// Only reductions toward zero are allowed.
    ReduceOnly,
    /// New orders are forbidden; cancellation/reconciliation remains allowed.
    CancelOnly,
    /// All target mutations are forbidden.
    Halted,
}

/// Hard risk limits in canonical quantity/notional ticks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    /// Maximum absolute quantity in one instrument.
    pub max_position_ticks: i64,
    /// Maximum absolute order quantity.
    pub max_order_ticks: i64,
    /// Maximum gross notional, represented as quantity*price ticks.
    pub max_gross_notional_ticks: i128,
}

/// One immutable limit revision effective at a decision-clock boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimedLimits {
    /// Monotonic decision timestamp at which this revision becomes active.
    pub effective_mono_ns: u64,
    /// Limits active from that timestamp onward.
    pub limits: Limits,
}

/// Identity fields used to resolve scoped risk policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskScope<'a> {
    /// Account identity.
    pub account_id: &'a str,
    /// Strategy identity, when the request is strategy-attributed.
    pub strategy_id: Option<&'a str>,
    /// Canonical asset class, when resolved by instrument master.
    pub asset_class: Option<AssetClass>,
    /// Canonical instrument identity.
    pub instrument_id: InstrumentId,
}

/// Failure installing a scoped risk revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyError {
    /// Account, strategy, or revision identity is invalid.
    InvalidIdentity,
    /// A revision contains a non-positive hard limit.
    InvalidLimits,
}

/// Deterministic system/account/strategy/asset/instrument risk policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedRiskPolicy {
    system: Vec<TimedLimits>,
    accounts: BTreeMap<String, Vec<TimedLimits>>,
    strategies: BTreeMap<String, Vec<TimedLimits>>,
    assets: BTreeMap<AssetClass, Vec<TimedLimits>>,
    instruments: BTreeMap<InstrumentId, Vec<TimedLimits>>,
}

/// Stable, cloneable representation used to persist a scoped policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedRiskPolicySnapshot {
    /// System-wide revisions.
    pub system: Vec<TimedLimits>,
    /// Account revisions.
    pub accounts: BTreeMap<String, Vec<TimedLimits>>,
    /// Strategy revisions.
    pub strategies: BTreeMap<String, Vec<TimedLimits>>,
    /// Asset-class revisions.
    pub assets: BTreeMap<AssetClass, Vec<TimedLimits>>,
    /// Instrument revisions.
    pub instruments: BTreeMap<InstrumentId, Vec<TimedLimits>>,
}

impl ScopedRiskPolicy {
    /// Creates a policy with one system-wide revision.
    ///
    /// # Errors
    /// Returns [`PolicyError::InvalidLimits`] when a hard limit is non-positive.
    pub fn new(limits: Limits) -> Result<Self, PolicyError> {
        validate_limits(limits)?;
        Ok(Self {
            system: vec![TimedLimits {
                effective_mono_ns: 0,
                limits,
            }],
            accounts: BTreeMap::new(),
            strategies: BTreeMap::new(),
            assets: BTreeMap::new(),
            instruments: BTreeMap::new(),
        })
    }

    /// Returns a deterministic snapshot suitable for journaling.
    #[must_use]
    pub fn snapshot(&self) -> ScopedRiskPolicySnapshot {
        ScopedRiskPolicySnapshot {
            system: self.system.clone(),
            accounts: self.accounts.clone(),
            strategies: self.strategies.clone(),
            assets: self.assets.clone(),
            instruments: self.instruments.clone(),
        }
    }

    /// Restores a validated policy snapshot.
    ///
    /// # Errors
    /// Returns [`PolicyError`] when any identity, revision, or limit is invalid.
    pub fn from_snapshot(snapshot: ScopedRiskPolicySnapshot) -> Result<Self, PolicyError> {
        if snapshot.system.is_empty()
            || snapshot.system.len() > 256
            || snapshot.accounts.len() > 1024
            || snapshot.strategies.len() > 1024
            || snapshot.assets.len() > 16
            || snapshot.instruments.len() > 16_384
        {
            return Err(PolicyError::InvalidIdentity);
        }
        if snapshot
            .accounts
            .values()
            .chain(snapshot.strategies.values())
            .chain(snapshot.assets.values())
            .chain(snapshot.instruments.values())
            .any(|revisions| revisions.len() > 256)
        {
            return Err(PolicyError::InvalidIdentity);
        }
        let mut policy = Self {
            system: Vec::new(),
            accounts: BTreeMap::new(),
            strategies: BTreeMap::new(),
            assets: BTreeMap::new(),
            instruments: BTreeMap::new(),
        };
        for revision in snapshot.system {
            policy.set_system(revision)?;
        }
        for (identity, revisions) in snapshot.accounts {
            for revision in revisions {
                policy.set_account(identity.clone(), revision)?;
            }
        }
        for (identity, revisions) in snapshot.strategies {
            for revision in revisions {
                policy.set_strategy(identity.clone(), revision)?;
            }
        }
        for (asset, revisions) in snapshot.assets {
            for revision in revisions {
                policy.set_asset(asset, revision)?;
            }
        }
        for (instrument, revisions) in snapshot.instruments {
            for revision in revisions {
                policy.set_instrument(instrument, revision)?;
            }
        }
        Ok(policy)
    }

    /// Installs a system-wide revision.
    ///
    /// # Errors
    /// Returns [`PolicyError`] when the revision limits are invalid.
    pub fn set_system(&mut self, revision: TimedLimits) -> Result<(), PolicyError> {
        validate_revision(revision)?;
        upsert_revision(&mut self.system, revision);
        Ok(())
    }

    /// Installs an account-scoped revision.
    ///
    /// # Errors
    /// Returns [`PolicyError`] when the account identity or limits are invalid.
    pub fn set_account(
        &mut self,
        account_id: impl Into<String>,
        revision: TimedLimits,
    ) -> Result<(), PolicyError> {
        let account_id = account_id.into();
        if account_id.trim().is_empty() {
            return Err(PolicyError::InvalidIdentity);
        }
        validate_revision(revision)?;
        upsert_revision(self.accounts.entry(account_id).or_default(), revision);
        Ok(())
    }

    /// Installs a strategy-scoped revision.
    ///
    /// # Errors
    /// Returns [`PolicyError`] when the strategy identity or limits are invalid.
    pub fn set_strategy(
        &mut self,
        strategy_id: impl Into<String>,
        revision: TimedLimits,
    ) -> Result<(), PolicyError> {
        let strategy_id = strategy_id.into();
        if strategy_id.trim().is_empty() {
            return Err(PolicyError::InvalidIdentity);
        }
        validate_revision(revision)?;
        upsert_revision(self.strategies.entry(strategy_id).or_default(), revision);
        Ok(())
    }

    /// Installs an asset-class-scoped revision.
    ///
    /// # Errors
    /// Returns [`PolicyError`] when the revision limits are invalid.
    pub fn set_asset(
        &mut self,
        asset_class: AssetClass,
        revision: TimedLimits,
    ) -> Result<(), PolicyError> {
        validate_revision(revision)?;
        upsert_revision(self.assets.entry(asset_class).or_default(), revision);
        Ok(())
    }

    /// Installs an instrument-scoped revision.
    ///
    /// # Errors
    /// Returns [`PolicyError`] when the revision limits are invalid.
    pub fn set_instrument(
        &mut self,
        instrument_id: InstrumentId,
        revision: TimedLimits,
    ) -> Result<(), PolicyError> {
        validate_revision(revision)?;
        upsert_revision(self.instruments.entry(instrument_id).or_default(), revision);
        Ok(())
    }

    /// Resolves the most-specific revision effective at `now_mono_ns`.
    #[must_use]
    pub fn limits_at(&self, scope: RiskScope<'_>, now_mono_ns: u64) -> Limits {
        let mut selected = latest_revision(&self.system, now_mono_ns).map_or_else(
            || Limits {
                max_position_ticks: 0,
                max_order_ticks: 0,
                max_gross_notional_ticks: 0,
            },
            |revision| revision.limits,
        );
        let account_revisions = if scope.account_id.trim().is_empty() {
            None
        } else {
            self.accounts.get(scope.account_id)
        };
        for revisions in [
            account_revisions,
            scope.strategy_id.and_then(|id| self.strategies.get(id)),
            scope.asset_class.and_then(|class| self.assets.get(&class)),
            self.instruments.get(&scope.instrument_id),
        ]
        .into_iter()
        .flatten()
        {
            if let Some(revision) = latest_revision(revisions, now_mono_ns) {
                selected = revision.limits;
            }
        }
        selected
    }
}

fn validate_limits(limits: Limits) -> Result<(), PolicyError> {
    (limits.max_position_ticks > 0
        && limits.max_order_ticks > 0
        && limits.max_gross_notional_ticks > 0)
        .then_some(())
        .ok_or(PolicyError::InvalidLimits)
}

fn validate_revision(revision: TimedLimits) -> Result<(), PolicyError> {
    validate_limits(revision.limits)
}

fn upsert_revision(revisions: &mut Vec<TimedLimits>, revision: TimedLimits) {
    if let Some(existing) = revisions
        .iter_mut()
        .find(|existing| existing.effective_mono_ns == revision.effective_mono_ns)
    {
        *existing = revision;
    } else {
        revisions.push(revision);
        revisions.sort_by_key(|revision| revision.effective_mono_ns);
    }
}

fn latest_revision(revisions: &[TimedLimits], now_mono_ns: u64) -> Option<TimedLimits> {
    revisions
        .iter()
        .rev()
        .find(|revision| revision.effective_mono_ns <= now_mono_ns)
        .copied()
}

/// Optional contextual pre-trade guardrails. `None` disables only that guard;
/// hard position/order/gross limits remain enforced by [`Limits`].
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Guardrails {
    /// Maximum gross leverage ratio.
    pub max_leverage: Option<f64>,
    /// Maximum drawdown in basis points.
    pub max_drawdown_bps: Option<i64>,
    /// Maximum predicted volatility in basis points.
    pub max_predicted_volatility_bps: Option<i64>,
    /// Maximum order participation in basis points.
    pub max_participation_bps: Option<i64>,
    /// Maximum currently outstanding orders.
    pub max_outstanding_orders: Option<u64>,
    /// Maximum observed command/message rate per second.
    pub max_message_rate: Option<u64>,
    /// Maximum absolute deviation from the trusted reference price in bps.
    pub max_price_deviation_bps: Option<i64>,
}

/// Runtime observations supplied by market/account/session supervisors.
#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct RiskInputs {
    /// Current gross leverage ratio.
    pub leverage: f64,
    /// Current drawdown in basis points.
    pub drawdown_bps: i64,
    /// Predicted volatility in basis points.
    pub predicted_volatility_bps: i64,
    /// Proposed order participation in basis points.
    pub participation_bps: i64,
    /// Number of working/outstanding orders.
    pub outstanding_orders: u64,
    /// Current command/message rate per second.
    pub message_rate: u64,
    /// Signed target/reference price deviation in basis points.
    pub price_deviation_bps: i64,
    /// Whether prices/FX inputs are fresh and trusted.
    pub prices_fresh: bool,
    /// Whether the instrument/account/session clock is healthy.
    pub clock_healthy: bool,
    /// Whether the broker session is connected and authorized.
    pub broker_session_healthy: bool,
    /// Whether this command duplicates an already durable intent.
    pub duplicate_intent: bool,
}

/// Deterministic price shock applied to every reconciled position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StressScenario {
    /// Stable scenario name.
    pub name: String,
    /// Signed price shock in basis points, e.g. `-2000` for `-20%`.
    pub shock_bps: i64,
}

/// Result of one portfolio stress scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StressResult {
    /// Scenario name.
    pub name: String,
    /// Mark-to-shock P&L in reporting-currency ticks.
    pub pnl_ticks: i128,
    /// Whether the absolute loss exceeds the configured gross limit.
    pub breached: bool,
}

/// Risk decision outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    /// Target can be applied unchanged.
    Allow,
    /// Target was reduced to the safe quantity.
    Resize {
        /// Safe signed target quantity.
        quantity_ticks: i64,
    },
    /// Target cannot be applied.
    Deny(Reason),
}

/// Stable denial reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reason {
    /// Current risk state forbids the action.
    Halted,
    /// Configured limit is non-positive.
    InvalidLimit,
    /// Position cap was exceeded.
    PositionLimit,
    /// Order cap was exceeded.
    OrderLimit,
    /// Gross notional cap was exceeded.
    GrossExposure,
    /// No trusted mark exists for the target instrument.
    MissingPrice,
    /// Canonical notional arithmetic overflowed; approval fails closed.
    ArithmeticOverflow,
    /// Price/FX data is stale or unavailable.
    StaleData,
    /// Injected/system clock health is not trusted.
    UnhealthyClock,
    /// Broker session is not healthy enough for a new order.
    UnhealthyBrokerSession,
    /// The command repeats a durable order intent.
    DuplicateIntent,
    /// Leverage guardrail exceeded.
    Leverage,
    /// Drawdown guardrail exceeded.
    Drawdown,
    /// Predicted volatility guardrail exceeded.
    PredictedVolatility,
    /// Participation guardrail exceeded.
    Participation,
    /// Outstanding-order guardrail exceeded.
    OutstandingOrders,
    /// Message-rate guardrail exceeded.
    MessageRate,
    /// Price-deviation guardrail exceeded.
    PriceDeviation,
}

/// Failure changing the persistent risk state machine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StateTransitionError {
    /// The requested transition is not valid from the current state.
    InvalidTransition {
        /// Existing state.
        from: State,
        /// Requested state.
        to: State,
    },
    /// Returning to a less restrictive state requires an identified operator.
    AuthorizationRequired,
}

/// Independent risk evaluator.
#[derive(Clone)]
pub struct RiskEngine {
    limits: Limits,
    guardrails: Guardrails,
    state: State,
}

impl RiskEngine {
    /// Creates a running risk engine.
    #[must_use]
    pub const fn new(limits: Limits) -> Self {
        Self {
            limits,
            guardrails: Guardrails {
                max_leverage: None,
                max_drawdown_bps: None,
                max_predicted_volatility_bps: None,
                max_participation_bps: None,
                max_outstanding_orders: None,
                max_message_rate: None,
                max_price_deviation_bps: None,
            },
            state: State::Running,
        }
    }

    /// Creates a risk engine with the supplied contextual guardrails enabled.
    #[must_use]
    pub const fn new_with_guardrails(limits: Limits, guardrails: Guardrails) -> Self {
        Self {
            limits,
            guardrails,
            state: State::Running,
        }
    }

    /// Returns the currently enforced risk state.
    #[must_use]
    pub const fn state(&self) -> State {
        self.state
    }

    /// Returns the immutable hard limits.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        self.limits
    }

    /// Clones the current state and guardrails with a resolved hard-limit set.
    /// This is used by scoped policy resolution without mutating the live
    /// engine configuration.
    #[must_use]
    pub const fn with_limits(&self, limits: Limits) -> Self {
        Self {
            limits,
            guardrails: self.guardrails,
            state: self.state,
        }
    }

    /// Returns the immutable contextual guardrails.
    #[must_use]
    pub const fn guardrails(&self) -> Guardrails {
        self.guardrails
    }

    /// Replaces contextual guardrails as one runtime policy snapshot.
    pub fn set_guardrails(&mut self, guardrails: Guardrails) {
        self.guardrails = guardrails;
    }

    /// Changes state through the explicit risk state machine.
    ///
    /// Entering a restrictive state may be automated. Any transition that
    /// broadens permissions requires a non-empty authorization identity and is
    /// intended to be journaled by the service host.
    ///
    /// # Errors
    /// Returns [`StateTransitionError`] when the transition is invalid or has
    /// no operator authorization.
    pub fn transition(
        &mut self,
        next: State,
        authorization: &str,
    ) -> Result<(), StateTransitionError> {
        self.validate_transition(next, authorization)?;
        self.state = next;
        Ok(())
    }

    /// Validates a transition without mutating the state.
    ///
    /// # Errors
    /// Returns [`StateTransitionError`] when authorization or transition rules
    /// reject the requested state.
    pub fn validate_transition(
        &self,
        next: State,
        authorization: &str,
    ) -> Result<(), StateTransitionError> {
        if self.state == next {
            return Ok(());
        }
        let broadens_permissions = !matches!(
            (self.state, next),
            (
                State::Running,
                State::ReduceOnly | State::CancelOnly | State::Halted
            ) | (State::ReduceOnly, State::CancelOnly | State::Halted)
                | (State::CancelOnly, State::Halted)
        );
        if broadens_permissions && authorization.trim().is_empty() {
            return Err(StateTransitionError::AuthorizationRequired);
        }
        Ok(())
    }

    /// Checks a target against current portfolio and hard limits.
    #[must_use]
    pub fn check(&self, portfolio: &Portfolio, target: &Target) -> Decision {
        if matches!(self.state, State::Halted | State::CancelOnly) {
            return Decision::Deny(Reason::Halted);
        }
        if self.limits.max_position_ticks <= 0
            || self.limits.max_order_ticks <= 0
            || self.limits.max_gross_notional_ticks <= 0
        {
            return Decision::Deny(Reason::InvalidLimit);
        }
        let current = portfolio
            .position(target.instrument_id)
            .map_or(0, |position| position.quantity_ticks);
        let Some(target_price) = portfolio.mark_price(target.instrument_id).or_else(|| {
            portfolio
                .position(target.instrument_id)
                .map(|position| position.mark_price_ticks)
        }) else {
            return Decision::Deny(Reason::MissingPrice);
        };
        if target_price <= 0 {
            return Decision::Deny(Reason::MissingPrice);
        }
        if matches!(self.state, State::ReduceOnly)
            && !moves_toward_zero(current, target.quantity_ticks)
        {
            return Decision::Deny(Reason::Halted);
        }
        let position_limited = if target.quantity_ticks.unsigned_abs()
            > self.limits.max_position_ticks.unsigned_abs()
        {
            target.quantity_ticks.signum() * self.limits.max_position_ticks
        } else {
            target.quantity_ticks
        };
        let Some(order_delta) = position_limited.checked_sub(current) else {
            return Decision::Deny(Reason::ArithmeticOverflow);
        };
        let order = order_delta.unsigned_abs();
        let safe_target = if order > self.limits.max_order_ticks.unsigned_abs() {
            let capped_order = i64::try_from(self.limits.max_order_ticks.unsigned_abs())
                .map_err(|_| Reason::ArithmeticOverflow);
            let Ok(capped_order) = capped_order else {
                return Decision::Deny(Reason::ArithmeticOverflow);
            };
            let Some(delta) = position_limited.signum().checked_mul(capped_order) else {
                return Decision::Deny(Reason::ArithmeticOverflow);
            };
            let Some(value) = current.checked_add(delta) else {
                return Decision::Deny(Reason::ArithmeticOverflow);
            };
            value
        } else {
            position_limited
        };
        let gross_positions =
            portfolio
                .positions()
                .try_fold(0_i128, |total, (instrument, position)| {
                    let quantity = if instrument == target.instrument_id {
                        safe_target
                    } else {
                        position.quantity_ticks
                    };
                    let mark = portfolio
                        .mark_price(instrument)
                        .unwrap_or(position.mark_price_ticks);
                    if mark <= 0 {
                        return None;
                    }
                    i128::from(quantity.unsigned_abs())
                        .checked_mul(i128::from(mark.unsigned_abs()))
                        .and_then(|notional| total.checked_add(notional))
                });
        let Some(mut gross) = gross_positions else {
            return Decision::Deny(Reason::ArithmeticOverflow);
        };
        if portfolio.position(target.instrument_id).is_none() {
            let Some(notional) = i128::from(safe_target.unsigned_abs())
                .checked_mul(i128::from(target_price.unsigned_abs()))
            else {
                return Decision::Deny(Reason::ArithmeticOverflow);
            };
            let Some(total) = gross.checked_add(notional) else {
                return Decision::Deny(Reason::ArithmeticOverflow);
            };
            gross = total;
        }
        if gross > self.limits.max_gross_notional_ticks {
            return Decision::Deny(Reason::GrossExposure);
        }
        if safe_target == target.quantity_ticks {
            Decision::Allow
        } else {
            Decision::Resize {
                quantity_ticks: safe_target,
            }
        }
    }

    /// Checks a target using the most-specific limits effective at the supplied
    /// decision timestamp. Contextual guardrails and persistent risk state are
    /// still enforced by this engine.
    #[must_use]
    pub fn check_scoped(
        &self,
        portfolio: &Portfolio,
        target: &Target,
        policy: &ScopedRiskPolicy,
        scope: RiskScope<'_>,
        now_mono_ns: u64,
    ) -> Decision {
        let scoped = Self {
            limits: policy.limits_at(scope, now_mono_ns),
            guardrails: self.guardrails,
            state: self.state,
        };
        scoped.check(portfolio, target)
    }

    /// Applies contextual guardrails and checks a target against a resolved
    /// scoped limit revision.
    #[must_use]
    pub fn check_with_guardrails_scoped(
        &self,
        portfolio: &Portfolio,
        target: &Target,
        policy: &ScopedRiskPolicy,
        scope: RiskScope<'_>,
        now_mono_ns: u64,
        inputs: RiskInputs,
    ) -> Decision {
        let scoped = Self {
            limits: policy.limits_at(scope, now_mono_ns),
            guardrails: self.guardrails,
            state: self.state,
        };
        scoped.check_with_guardrails(portfolio, target, scoped.guardrails, inputs)
    }

    /// Applies contextual guardrails before the canonical portfolio limits.
    ///
    /// Invalid/non-finite observations fail closed as `StaleData`; no LLM or
    /// strategy confidence can override these checks.
    #[must_use]
    pub fn check_with_guardrails(
        &self,
        portfolio: &Portfolio,
        target: &Target,
        guardrails: Guardrails,
        inputs: RiskInputs,
    ) -> Decision {
        if !inputs.prices_fresh {
            return Decision::Deny(Reason::StaleData);
        }
        if !inputs.clock_healthy {
            return Decision::Deny(Reason::UnhealthyClock);
        }
        if !inputs.broker_session_healthy {
            return Decision::Deny(Reason::UnhealthyBrokerSession);
        }
        if inputs.duplicate_intent {
            return Decision::Deny(Reason::DuplicateIntent);
        }
        if !inputs.leverage.is_finite()
            || inputs.leverage < 0.0
            || inputs.drawdown_bps < 0
            || inputs.predicted_volatility_bps < 0
            || inputs.participation_bps < 0
            || inputs.price_deviation_bps == i64::MIN
        {
            return Decision::Deny(Reason::StaleData);
        }
        if guardrails
            .max_leverage
            .is_some_and(|limit| !limit.is_finite() || limit < 0.0 || inputs.leverage > limit)
        {
            return Decision::Deny(Reason::Leverage);
        }
        if guardrails
            .max_drawdown_bps
            .is_some_and(|limit| limit < 0 || inputs.drawdown_bps > limit)
        {
            return Decision::Deny(Reason::Drawdown);
        }
        if guardrails
            .max_predicted_volatility_bps
            .is_some_and(|limit| limit < 0 || inputs.predicted_volatility_bps > limit)
        {
            return Decision::Deny(Reason::PredictedVolatility);
        }
        if guardrails
            .max_participation_bps
            .is_some_and(|limit| limit < 0 || inputs.participation_bps > limit)
        {
            return Decision::Deny(Reason::Participation);
        }
        if guardrails
            .max_outstanding_orders
            .is_some_and(|limit| inputs.outstanding_orders > limit)
        {
            return Decision::Deny(Reason::OutstandingOrders);
        }
        if guardrails
            .max_message_rate
            .is_some_and(|limit| inputs.message_rate > limit)
        {
            return Decision::Deny(Reason::MessageRate);
        }
        if guardrails.max_price_deviation_bps.is_some_and(|limit| {
            limit < 0 || inputs.price_deviation_bps.unsigned_abs() > limit.unsigned_abs()
        }) {
            return Decision::Deny(Reason::PriceDeviation);
        }
        self.check(portfolio, target)
    }

    /// Evaluates deterministic parallel price shocks over reconciled positions.
    ///
    /// # Errors
    /// Returns [`Reason::InvalidLimit`] for invalid gross limits or scenario
    /// names, and [`Reason::ArithmeticOverflow`] when checked stress arithmetic
    /// cannot be represented.
    pub fn stress(
        &self,
        portfolio: &Portfolio,
        scenarios: &[StressScenario],
    ) -> Result<Vec<StressResult>, Reason> {
        if self.limits.max_gross_notional_ticks <= 0 {
            return Err(Reason::InvalidLimit);
        }
        scenarios
            .iter()
            .map(|scenario| {
                if scenario.name.trim().is_empty() {
                    return Err(Reason::InvalidLimit);
                }
                let pnl_ticks =
                    portfolio
                        .positions()
                        .try_fold(0_i128, |total, (_, position)| {
                            let notional = i128::from(position.quantity_ticks)
                                .checked_mul(i128::from(position.mark_price_ticks))
                                .ok_or(Reason::ArithmeticOverflow)?;
                            let shocked = notional
                                .checked_mul(i128::from(scenario.shock_bps))
                                .ok_or(Reason::ArithmeticOverflow)?
                                / 10_000;
                            total.checked_add(shocked).ok_or(Reason::ArithmeticOverflow)
                        })?;
                Ok(StressResult {
                    name: scenario.name.clone(),
                    pnl_ticks,
                    breached: pnl_ticks.unsigned_abs()
                        > self.limits.max_gross_notional_ticks.unsigned_abs(),
                })
            })
            .collect()
    }
}

fn moves_toward_zero(current: i64, target: i64) -> bool {
    match current.cmp(&0) {
        std::cmp::Ordering::Greater => (0..=current).contains(&target),
        std::cmp::Ordering::Less => (current..=0).contains(&target),
        std::cmp::Ordering::Equal => target == 0,
    }
}

#[cfg(test)]
mod tests {
    use insider_common_types::{InstrumentId, ProposalId};
    use insider_portfolio::{Portfolio, Position, Target};

    use super::{
        Decision, Guardrails, Limits, Reason, RiskEngine, RiskInputs, RiskScope, ScopedRiskPolicy,
        State, StressScenario, TimedLimits,
    };

    #[test]
    fn scoped_policy_resolves_specific_effective_revision_deterministically() {
        let Some(instrument_id) = InstrumentId::new(77).ok() else {
            return;
        };
        let base = Limits {
            max_position_ticks: 100,
            max_order_ticks: 100,
            max_gross_notional_ticks: 10_000,
        };
        let account = Limits {
            max_position_ticks: 20,
            max_order_ticks: 20,
            max_gross_notional_ticks: 2_000,
        };
        let instrument = Limits {
            max_position_ticks: 5,
            max_order_ticks: 5,
            max_gross_notional_ticks: 500,
        };
        let mut policy = ScopedRiskPolicy::new(base).unwrap_or_else(|_| unreachable!());
        assert!(
            policy
                .set_account(
                    "acct",
                    TimedLimits {
                        effective_mono_ns: 10,
                        limits: account
                    }
                )
                .is_ok()
        );
        assert!(
            policy
                .set_instrument(
                    instrument_id,
                    TimedLimits {
                        effective_mono_ns: 20,
                        limits: instrument
                    }
                )
                .is_ok()
        );
        let resolved = policy.limits_at(
            RiskScope {
                account_id: "acct",
                strategy_id: None,
                asset_class: None,
                instrument_id,
            },
            25,
        );
        assert_eq!(resolved, instrument);
        assert_eq!(
            policy.limits_at(
                RiskScope {
                    account_id: "acct",
                    strategy_id: None,
                    asset_class: None,
                    instrument_id: InstrumentId::new(78).unwrap_or(instrument_id),
                },
                15,
            ),
            account
        );
    }

    #[test]
    fn risk_resizes_position_and_order_limits_and_halts_safely() {
        let Some(instrument_id) = InstrumentId::new(1).ok() else {
            return;
        };
        let Some(proposal_id) = ProposalId::new(1).ok() else {
            return;
        };
        let mut portfolio = Portfolio::new();
        portfolio.set_position(
            instrument_id,
            Position {
                quantity_ticks: 2,
                mark_price_ticks: 10,
            },
        );
        let target = Target {
            instrument_id,
            quantity_ticks: 20,
            proposal_id,
        };
        let mut risk = RiskEngine::new(Limits {
            max_position_ticks: 10,
            max_order_ticks: 5,
            max_gross_notional_ticks: 1_000,
        });
        assert_eq!(
            risk.check(&portfolio, &target),
            Decision::Resize { quantity_ticks: 7 }
        );
        let tight_gross = RiskEngine::new(Limits {
            max_position_ticks: 10,
            max_order_ticks: 5,
            max_gross_notional_ticks: 50,
        });
        assert_eq!(
            tight_gross.check(&portfolio, &target),
            Decision::Deny(Reason::GrossExposure)
        );
        assert!(
            risk.transition(State::Halted, "system-risk-trigger")
                .is_ok()
        );
        assert_eq!(
            risk.check(&portfolio, &target),
            Decision::Deny(Reason::Halted)
        );
    }

    #[test]
    fn reduce_only_rejects_reversals_but_allows_closing() {
        let Some(instrument_id) = InstrumentId::new(1).ok() else {
            return;
        };
        let Some(proposal_id) = ProposalId::new(1).ok() else {
            return;
        };
        let mut portfolio = Portfolio::new();
        portfolio.set_position(
            instrument_id,
            Position {
                quantity_ticks: 10,
                mark_price_ticks: 10,
            },
        );
        let limits = Limits {
            max_position_ticks: 100,
            max_order_ticks: 100,
            max_gross_notional_ticks: 100_000,
        };
        let mut risk = RiskEngine::new(limits);
        assert!(
            risk.transition(State::ReduceOnly, "system-risk-trigger")
                .is_ok()
        );
        assert_eq!(
            risk.check(
                &portfolio,
                &Target {
                    instrument_id,
                    quantity_ticks: 4,
                    proposal_id
                }
            ),
            Decision::Allow
        );
        assert_eq!(
            risk.check(
                &portfolio,
                &Target {
                    instrument_id,
                    quantity_ticks: 0,
                    proposal_id
                }
            ),
            Decision::Allow
        );
        assert_eq!(
            risk.check(
                &portfolio,
                &Target {
                    instrument_id,
                    quantity_ticks: -1,
                    proposal_id
                }
            ),
            Decision::Deny(Reason::Halted)
        );
    }

    #[test]
    fn stress_scenarios_report_shocked_pnl_and_limit_breaches() {
        let Some(instrument_id) = InstrumentId::new(1).ok() else {
            return;
        };
        let mut portfolio = Portfolio::new();
        portfolio.set_position(
            instrument_id,
            Position {
                quantity_ticks: 10,
                mark_price_ticks: 100,
            },
        );
        let risk = RiskEngine::new(Limits {
            max_position_ticks: 100,
            max_order_ticks: 100,
            max_gross_notional_ticks: 50,
        });
        let Ok(results) = risk.stress(
            &portfolio,
            &[StressScenario {
                name: "down-10".into(),
                shock_bps: -1_000,
            }],
        ) else {
            return;
        };
        assert_eq!(results.first().map(|result| result.pnl_ticks), Some(-100));
        assert_eq!(results.first().map(|result| result.breached), Some(true));
    }

    #[test]
    fn opening_target_uses_standalone_trusted_mark_and_fails_without_one() {
        let Some(instrument_id) = InstrumentId::new(9).ok() else {
            return;
        };
        let Some(proposal_id) = ProposalId::new(9).ok() else {
            return;
        };
        let mut portfolio = Portfolio::new();
        let risk = RiskEngine::new(Limits {
            max_position_ticks: 100,
            max_order_ticks: 100,
            max_gross_notional_ticks: 1_000,
        });
        let target = Target {
            instrument_id,
            quantity_ticks: 5,
            proposal_id,
        };
        assert_eq!(
            risk.check(&portfolio, &target),
            Decision::Deny(Reason::MissingPrice)
        );
        assert!(portfolio.set_mark_price(instrument_id, 100).is_ok());
        assert_eq!(risk.check(&portfolio, &target), Decision::Allow);
    }

    #[test]
    fn contextual_guardrails_fail_closed_before_portfolio_limits() {
        let Some(instrument_id) = InstrumentId::new(12).ok() else {
            return;
        };
        let Some(proposal_id) = ProposalId::new(12).ok() else {
            return;
        };
        let mut portfolio = Portfolio::new();
        assert!(portfolio.set_mark_price(instrument_id, 100).is_ok());
        let risk = RiskEngine::new(Limits {
            max_position_ticks: 100,
            max_order_ticks: 100,
            max_gross_notional_ticks: 100_000,
        });
        let target = Target {
            instrument_id,
            quantity_ticks: 1,
            proposal_id,
        };
        let inputs = RiskInputs {
            leverage: 1.0,
            drawdown_bps: 0,
            predicted_volatility_bps: 0,
            participation_bps: 0,
            outstanding_orders: 0,
            message_rate: 0,
            price_deviation_bps: 0,
            prices_fresh: false,
            clock_healthy: true,
            broker_session_healthy: true,
            duplicate_intent: false,
        };
        assert_eq!(
            risk.check_with_guardrails(&portfolio, &target, Guardrails::default(), inputs),
            Decision::Deny(Reason::StaleData)
        );
        let inputs = RiskInputs {
            prices_fresh: true,
            ..inputs
        };
        assert_eq!(
            risk.check_with_guardrails(
                &portfolio,
                &target,
                Guardrails {
                    max_leverage: Some(0.5),
                    ..Guardrails::default()
                },
                inputs
            ),
            Decision::Deny(Reason::Leverage)
        );
    }
}
