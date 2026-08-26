//! Deterministic journal replay with explicit sequence guarantees.

#![forbid(unsafe_code)]

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "replay";

use insider_common_types::{MonoTime, WallTime};
use insider_journal::{Journal, JournalError, Record};
use std::ops::Range;
use std::path::Path;

/// Failure applying a historical fill.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerError {
    /// Price or signed quantity is invalid.
    InvalidValue,
    /// Arithmetic overflowed the ledger representation.
    Overflow,
}

/// Mark-to-market accounting snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerSnapshot {
    /// Signed position quantity.
    pub position_ticks: i64,
    /// Average open cost in price ticks.
    pub average_cost_ticks: i64,
    /// Cash balance in notional ticks.
    pub cash_ticks: i128,
    /// Cumulative realized P&L in notional ticks.
    pub realized_pnl_ticks: i128,
    /// Equity at the supplied mark.
    pub equity_ticks: i128,
}

/// Deterministic single-instrument replay ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::struct_field_names)]
pub struct BacktestLedger {
    initial_cash_ticks: i128,
    position_ticks: i64,
    average_cost_ticks: i64,
    cash_ticks: i128,
    realized_pnl_ticks: i128,
}

impl BacktestLedger {
    /// Creates a flat ledger with the supplied initial cash.
    #[must_use]
    pub const fn new(initial_cash_ticks: i128) -> Self {
        Self {
            initial_cash_ticks,
            position_ticks: 0,
            average_cost_ticks: 0,
            cash_ticks: initial_cash_ticks,
            realized_pnl_ticks: 0,
        }
    }

    /// Applies one signed fill: positive quantity buys, negative quantity sells.
    ///
    /// # Errors
    /// Returns `LedgerError` for invalid values or arithmetic overflow.
    pub fn apply_fill(&mut self, quantity_ticks: i64, price_ticks: i64) -> Result<(), LedgerError> {
        if quantity_ticks == 0 || price_ticks <= 0 {
            return Err(LedgerError::InvalidValue);
        }
        let notional = i128::from(quantity_ticks)
            .checked_mul(i128::from(price_ticks))
            .ok_or(LedgerError::Overflow)?;
        self.cash_ticks = self
            .cash_ticks
            .checked_sub(notional)
            .ok_or(LedgerError::Overflow)?;
        if self.position_ticks == 0 || self.position_ticks.signum() == quantity_ticks.signum() {
            let old_abs = i128::from(self.position_ticks.unsigned_abs());
            let fill_abs = i128::from(quantity_ticks.unsigned_abs());
            let total_qty = old_abs.checked_add(fill_abs).ok_or(LedgerError::Overflow)?;
            let total_cost = old_abs
                .checked_mul(i128::from(self.average_cost_ticks))
                .ok_or(LedgerError::Overflow)?
                .checked_add(
                    fill_abs
                        .checked_mul(i128::from(price_ticks))
                        .ok_or(LedgerError::Overflow)?,
                )
                .ok_or(LedgerError::Overflow)?;
            self.average_cost_ticks =
                i64::try_from(total_cost / total_qty).map_err(|_| LedgerError::Overflow)?;
            self.position_ticks = self
                .position_ticks
                .checked_add(quantity_ticks)
                .ok_or(LedgerError::Overflow)?;
            return Ok(());
        }
        let closing = self
            .position_ticks
            .unsigned_abs()
            .min(quantity_ticks.unsigned_abs());
        let price_delta = if self.position_ticks > 0 {
            i128::from(price_ticks) - i128::from(self.average_cost_ticks)
        } else {
            i128::from(self.average_cost_ticks) - i128::from(price_ticks)
        };
        self.realized_pnl_ticks = self
            .realized_pnl_ticks
            .checked_add(price_delta * i128::from(closing))
            .ok_or(LedgerError::Overflow)?;
        let previous_sign = self.position_ticks.signum();
        let remaining = quantity_ticks
            .checked_add(self.position_ticks)
            .ok_or(LedgerError::Overflow)?;
        self.position_ticks = remaining;
        if remaining == 0 || remaining.signum() != previous_sign {
            self.average_cost_ticks = if remaining == 0 { 0 } else { price_ticks };
        }
        Ok(())
    }

    /// Applies a realized transaction fee without changing position state.
    ///
    /// # Errors
    /// Returns [`LedgerError::Overflow`] if the cash balance cannot represent
    /// the fee-adjusted value.
    pub fn apply_fee(&mut self, fee_ticks: i128) -> Result<(), LedgerError> {
        if fee_ticks < 0 {
            return Err(LedgerError::InvalidValue);
        }
        self.cash_ticks = self
            .cash_ticks
            .checked_sub(fee_ticks)
            .ok_or(LedgerError::Overflow)?;
        Ok(())
    }

    /// Returns a mark-to-market snapshot.
    #[must_use]
    pub fn mark(&self, price_ticks: i64) -> Option<LedgerSnapshot> {
        if price_ticks <= 0 {
            return None;
        }
        let equity = self
            .cash_ticks
            .checked_add(i128::from(self.position_ticks).checked_mul(i128::from(price_ticks))?)?;
        Some(LedgerSnapshot {
            position_ticks: self.position_ticks,
            average_cost_ticks: self.average_cost_ticks,
            cash_ticks: self.cash_ticks,
            realized_pnl_ticks: self.realized_pnl_ticks,
            equity_ticks: equity,
        })
    }

    /// Returns initial cash for report metadata.
    #[must_use]
    pub const fn initial_cash_ticks(&self) -> i128 {
        self.initial_cash_ticks
    }
}

/// One point-in-time input to an event-driven backtest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BacktestEvent {
    /// A historically executed fill. Quantity is signed (buy positive, sell negative).
    Fill {
        /// Strictly increasing event sequence.
        sequence: u64,
        /// Signed quantity in canonical quantity ticks.
        quantity_ticks: i64,
        /// Execution price in canonical price ticks.
        price_ticks: i64,
        /// Explicit fee charged by the execution venue.
        fee_ticks: i128,
    },
    /// A point-in-time mark used to value the ledger.
    Mark {
        /// Strictly increasing event sequence.
        sequence: u64,
        /// Positive mark price in canonical price ticks.
        price_ticks: i64,
    },
}

impl BacktestEvent {
    fn sequence(self) -> u64 {
        match self {
            Self::Fill { sequence, .. } | Self::Mark { sequence, .. } => sequence,
        }
    }
}

/// A deterministic equity observation produced by a backtest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EquityPoint {
    /// Source event sequence.
    pub sequence: u64,
    /// Mark-to-market snapshot at this sequence.
    pub snapshot: LedgerSnapshot,
}

/// Aggregate result of one event-driven backtest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BacktestReport {
    /// Number of applied events.
    pub event_count: usize,
    /// Ordered mark-to-market equity curve.
    pub equity_curve: Vec<EquityPoint>,
    /// Final marked snapshot, if at least one mark was supplied.
    pub final_snapshot: Option<LedgerSnapshot>,
    /// Largest peak-to-trough equity loss in notional ticks.
    pub max_drawdown_ticks: i128,
    /// Sum of all explicit transaction fees.
    pub total_fees_ticks: i128,
}

impl BacktestReport {
    /// Returns marked period returns derived only from the recorded equity
    /// curve. The first mark establishes the base and is not a return.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn period_returns(&self) -> Vec<f64> {
        self.equity_curve
            .windows(2)
            .filter_map(|window| {
                let previous = window[0].snapshot.equity_ticks;
                if previous == 0 {
                    return None;
                }
                let change = window[1].snapshot.equity_ticks.checked_sub(previous)?;
                Some(change as f64 / previous as f64)
            })
            .collect()
    }

    /// Computes return statistics from this report's marked equity curve.
    ///
    /// # Errors
    /// Returns [`StatisticalError`] when fewer than two valid marked returns
    /// exist or the curve contains non-finite values after conversion.
    pub fn statistics(&self) -> Result<ReturnStatistics, StatisticalError> {
        return_statistics(&self.period_returns())
    }

    /// Computes the Deflated Sharpe probability for this report.
    ///
    /// `tested_configurations` must include every parameter/configuration trial
    /// considered before selecting this report.
    ///
    /// # Errors
    /// Returns [`StatisticalError`] when statistics cannot be computed.
    pub fn deflated_sharpe(&self, tested_configurations: usize) -> Result<f64, StatisticalError> {
        let statistics = self.statistics()?;
        deflated_sharpe_ratio(
            statistics.sharpe,
            tested_configurations,
            statistics.observations,
            statistics.skewness,
            statistics.excess_kurtosis,
        )
    }
}

/// Failure while applying a backtest event stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BacktestError {
    /// Event sequences must be strictly increasing.
    NonMonotonicSequence,
    /// An event contains invalid or overflowing accounting values.
    Ledger(LedgerError),
    /// No mark was available for a report that requires valuation.
    MissingMark,
}

impl From<LedgerError> for BacktestError {
    fn from(error: LedgerError) -> Self {
        Self::Ledger(error)
    }
}

/// Runs one deterministic, single-instrument event stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BacktestRunner {
    ledger: BacktestLedger,
    last_sequence: Option<u64>,
    event_count: usize,
    last_mark: Option<i64>,
    equity_curve: Vec<EquityPoint>,
    total_fees_ticks: i128,
}

impl BacktestRunner {
    /// Creates a runner with the supplied initial cash balance.
    #[must_use]
    pub fn new(initial_cash_ticks: i128) -> Self {
        Self {
            ledger: BacktestLedger::new(initial_cash_ticks),
            last_sequence: None,
            event_count: 0,
            last_mark: None,
            equity_curve: Vec::new(),
            total_fees_ticks: 0,
        }
    }

    /// Applies one event and records marks without consulting system time.
    ///
    /// # Errors
    /// Returns [`BacktestError`] for out-of-order input, invalid accounting,
    /// or an invalid mark.
    pub fn apply(&mut self, event: BacktestEvent) -> Result<(), BacktestError> {
        let sequence = event.sequence();
        if self.last_sequence.is_some_and(|last| sequence <= last) {
            return Err(BacktestError::NonMonotonicSequence);
        }
        match event {
            BacktestEvent::Fill {
                fee_ticks,
                quantity_ticks,
                price_ticks,
                ..
            } => {
                self.ledger.apply_fill(quantity_ticks, price_ticks)?;
                self.ledger.apply_fee(fee_ticks)?;
                self.total_fees_ticks = self
                    .total_fees_ticks
                    .checked_add(fee_ticks)
                    .ok_or(BacktestError::Ledger(LedgerError::Overflow))?;
            }
            BacktestEvent::Mark { price_ticks, .. } => {
                let snapshot = self
                    .ledger
                    .mark(price_ticks)
                    .ok_or(BacktestError::Ledger(LedgerError::InvalidValue))?;
                self.last_mark = Some(price_ticks);
                self.equity_curve.push(EquityPoint { sequence, snapshot });
            }
        }
        self.last_sequence = Some(sequence);
        self.event_count = self.event_count.saturating_add(1);
        Ok(())
    }

    /// Finishes the run and computes the deterministic report.
    ///
    /// # Errors
    /// Returns [`BacktestError::MissingMark`] when no valuation event was
    /// supplied; an unmarked run is not a valid performance report.
    pub fn finish(self) -> Result<BacktestReport, BacktestError> {
        if self.last_mark.is_none() {
            return Err(BacktestError::MissingMark);
        }
        let mut peak = i128::MIN;
        let mut max_drawdown = 0_i128;
        for point in &self.equity_curve {
            peak = peak.max(point.snapshot.equity_ticks);
            max_drawdown = max_drawdown.max(peak - point.snapshot.equity_ticks);
        }
        Ok(BacktestReport {
            event_count: self.event_count,
            final_snapshot: self.equity_curve.last().map(|point| point.snapshot),
            equity_curve: self.equity_curve,
            max_drawdown_ticks: max_drawdown,
            total_fees_ticks: self.total_fees_ticks,
        })
    }
}

/// Chronological walk-forward split configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalkForwardConfig {
    /// Number of observations available to the first training window.
    pub train_events: usize,
    /// Number of observations in each scored test window.
    pub test_events: usize,
    /// Number of observations excluded between train and test windows.
    pub embargo_events: usize,
    /// Number of final observations locked away from all fold scoring.
    pub holdout_events: usize,
}

/// One leakage-safe chronological fold.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalkForwardFold {
    /// Zero-based fold number.
    pub index: usize,
    /// Inclusive training start.
    pub train_start: usize,
    /// Exclusive training end.
    pub train_end: usize,
    /// Inclusive test start after embargo.
    pub test_start: usize,
    /// Exclusive test end.
    pub test_end: usize,
}

/// A validated walk-forward plan with a permanently locked holdout range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalkForwardPlan {
    /// Ordered scored folds.
    pub folds: Vec<WalkForwardFold>,
    /// Inclusive holdout start, or the input length when no holdout is configured.
    pub holdout_start: usize,
}

/// Reports produced by a leakage-safe walk-forward run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalkForwardReport {
    /// The exact split plan used.
    pub plan: WalkForwardPlan,
    /// One deterministic report per scored test fold.
    pub fold_reports: Vec<BacktestReport>,
    /// Final holdout report, evaluated only when configured.
    pub holdout_report: Option<BacktestReport>,
}

/// Validation failures for chronological research runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalkForwardError {
    /// A split dimension was zero or mathematically inconsistent.
    InvalidConfig,
    /// The input tape is too short for the requested windows.
    InsufficientEvents,
    /// A fold's test tape could not produce a valid backtest report.
    Backtest(BacktestError),
}

impl From<BacktestError> for WalkForwardError {
    fn from(error: BacktestError) -> Self {
        Self::Backtest(error)
    }
}

/// Builds an expanding-window plan without ever allowing test observations to
/// enter a prior training window or the locked final holdout.
///
/// # Errors
/// Returns [`WalkForwardError`] when the configuration cannot produce at least
/// one disjoint scored fold before the holdout.
pub fn plan_walk_forward(
    event_count: usize,
    config: WalkForwardConfig,
) -> Result<WalkForwardPlan, WalkForwardError> {
    if config.train_events == 0 || config.test_events == 0 {
        return Err(WalkForwardError::InvalidConfig);
    }
    let holdout_start = event_count
        .checked_sub(config.holdout_events)
        .ok_or(WalkForwardError::InsufficientEvents)?;
    if holdout_start <= config.train_events {
        return Err(WalkForwardError::InsufficientEvents);
    }
    let mut folds = Vec::new();
    let mut train_end = config.train_events;
    while let Some(test_start) = train_end.checked_add(config.embargo_events) {
        let Some(test_end) = test_start.checked_add(config.test_events) else {
            break;
        };
        if test_end > holdout_start {
            break;
        }
        folds.push(WalkForwardFold {
            index: folds.len(),
            train_start: 0,
            train_end,
            test_start,
            test_end,
        });
        train_end = test_end;
    }
    if folds.is_empty() {
        return Err(WalkForwardError::InsufficientEvents);
    }
    Ok(WalkForwardPlan {
        folds,
        holdout_start,
    })
}

/// Runs deterministic accounting over each test fold and the locked holdout.
/// Training and embargo observations are intentionally not passed to the
/// accounting runner; a strategy/model layer can consume those ranges for
/// fitting, but cannot accidentally score on them through this API.
///
/// # Errors
/// Returns [`WalkForwardError`] when the split is invalid or any fold lacks a
/// valid marked backtest report.
pub fn run_walk_forward(
    events: &[BacktestEvent],
    initial_cash_ticks: i128,
    config: WalkForwardConfig,
) -> Result<WalkForwardReport, WalkForwardError> {
    let plan = plan_walk_forward(events.len(), config)?;
    let run = |slice: &[BacktestEvent]| -> Result<BacktestReport, WalkForwardError> {
        let mut runner = BacktestRunner::new(initial_cash_ticks);
        for event in slice {
            runner.apply(*event)?;
        }
        Ok(runner.finish()?)
    };
    let fold_reports = plan
        .folds
        .iter()
        .map(|fold| run(&events[fold.test_start..fold.test_end]))
        .collect::<Result<Vec<_>, _>>()?;
    let holdout_report = (config.holdout_events > 0)
        .then(|| run(&events[plan.holdout_start..]))
        .transpose()?;
    Ok(WalkForwardReport {
        plan,
        fold_reports,
        holdout_report,
    })
}

/// Combinatorial purged cross-validation configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CpcvConfig {
    /// Number of contiguous chronological groups.
    pub groups: usize,
    /// Number of groups assigned to the test side of each split.
    pub test_groups: usize,
    /// Number of observations removed around every test group.
    pub embargo_events: usize,
    /// Number of final observations excluded from every split.
    pub holdout_events: usize,
}

/// One leakage-safe combinatorial split.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CpcvSplit {
    /// Training ranges, after purging test and embargo observations.
    pub train_ranges: Vec<Range<usize>>,
    /// Test ranges, in chronological order.
    pub test_ranges: Vec<Range<usize>>,
}

/// CPCV planning failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CpcvError {
    /// Group/test dimensions are invalid or cannot cover the tape.
    InvalidConfig,
    /// The tape is too short to create the requested groups and holdout.
    InsufficientEvents,
}

fn choose_cpcv(
    next: usize,
    groups: usize,
    want: usize,
    selected: &mut Vec<usize>,
    out: &mut Vec<Vec<usize>>,
) {
    if selected.len() == want {
        out.push(selected.clone());
        return;
    }
    let remaining = want - selected.len();
    for group in next..=groups.saturating_sub(remaining) {
        selected.push(group);
        choose_cpcv(group + 1, groups, want, selected, out);
        selected.pop();
    }
}

/// Builds all `n choose k` chronological test combinations. Training ranges are
/// purged on both sides of each test range by `embargo_events` and never include
/// the locked holdout.
///
/// # Errors
/// Returns [`CpcvError`] when the requested split dimensions cannot be formed.
pub fn plan_cpcv(event_count: usize, config: CpcvConfig) -> Result<Vec<CpcvSplit>, CpcvError> {
    if config.groups == 0 || config.test_groups == 0 || config.test_groups > config.groups {
        return Err(CpcvError::InvalidConfig);
    }
    let usable = event_count
        .checked_sub(config.holdout_events)
        .ok_or(CpcvError::InsufficientEvents)?;
    if usable < config.groups {
        return Err(CpcvError::InsufficientEvents);
    }
    let base = usable / config.groups;
    let remainder = usable % config.groups;
    let mut bounds = Vec::with_capacity(config.groups + 1);
    bounds.push(0);
    for group in 0..config.groups {
        let width = base + usize::from(group < remainder);
        bounds.push(bounds[group] + width);
    }
    let mut combinations = Vec::new();
    choose_cpcv(
        0,
        config.groups,
        config.test_groups,
        &mut Vec::new(),
        &mut combinations,
    );
    let mut splits = Vec::with_capacity(combinations.len());
    for selected in combinations {
        let mut test_ranges = selected
            .iter()
            .map(|&group| bounds[group]..bounds[group + 1])
            .collect::<Vec<_>>();
        test_ranges.sort_by_key(|range| range.start);
        let mut train_ranges = Vec::new();
        let mut cursor = 0;
        for test in &test_ranges {
            let start = test.start.saturating_sub(config.embargo_events);
            let end = (test.end + config.embargo_events).min(usable);
            if cursor < start {
                train_ranges.push(cursor..start);
            }
            cursor = cursor.max(end);
        }
        if cursor < usable {
            train_ranges.push(cursor..usable);
        }
        splits.push(CpcvSplit {
            train_ranges,
            test_ranges,
        });
    }
    Ok(splits)
}

/// Sample statistics for a finite return series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReturnStatistics {
    /// Number of observations.
    pub observations: usize,
    /// Arithmetic mean return.
    pub mean: f64,
    /// Sample standard deviation.
    pub standard_deviation: f64,
    /// Annualization-free Sharpe ratio (`mean / standard_deviation`).
    pub sharpe: f64,
    /// Central-moment skewness.
    pub skewness: f64,
    /// Excess kurtosis (normal distribution is zero).
    pub excess_kurtosis: f64,
}

/// Statistical validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticalError {
    /// Fewer than two finite observations were supplied.
    InsufficientObservations,
    /// A return, score, or computed statistic was not finite.
    NonFinite,
    /// The supplied trial/path matrix is empty, ragged, or inconsistent.
    InvalidMatrix,
}

/// Computes deterministic distribution statistics for a return series.
///
/// The function intentionally does not annualize: callers must supply the
/// sampling-period conversion appropriate to their instrument and session.
///
/// # Errors
/// Returns [`StatisticalError`] when the series is too short, non-finite, or
/// has zero variance.
#[allow(clippy::cast_precision_loss)]
pub fn return_statistics(returns: &[f64]) -> Result<ReturnStatistics, StatisticalError> {
    if returns.len() < 2 {
        return Err(StatisticalError::InsufficientObservations);
    }
    if returns.iter().any(|value| !value.is_finite()) {
        return Err(StatisticalError::NonFinite);
    }
    let observations = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / observations;
    let centered = returns.iter().map(|value| value - mean).collect::<Vec<_>>();
    let second = centered.iter().map(|value| value * value).sum::<f64>() / (observations - 1.0);
    if !second.is_finite() || second <= 0.0 {
        return Err(StatisticalError::InsufficientObservations);
    }
    let standard_deviation = second.sqrt();
    let population_second = second * (observations - 1.0) / observations;
    let third = centered.iter().map(|value| value.powi(3)).sum::<f64>() / observations;
    let fourth = centered.iter().map(|value| value.powi(4)).sum::<f64>() / observations;
    let skewness = third / population_second.powf(1.5);
    let excess_kurtosis = fourth / population_second.powi(2) - 3.0;
    let sharpe = mean / standard_deviation;
    let result = ReturnStatistics {
        observations: returns.len(),
        mean,
        standard_deviation,
        sharpe,
        skewness,
        excess_kurtosis,
    };
    if [
        result.mean,
        result.standard_deviation,
        result.sharpe,
        result.skewness,
        result.excess_kurtosis,
    ]
    .iter()
    .any(|value| !value.is_finite())
    {
        return Err(StatisticalError::NonFinite);
    }
    Ok(result)
}

/// Computes the Deflated Sharpe Ratio probability.
///
/// This uses the standard finite-sample Sharpe variance correction for skew
/// and excess kurtosis, with the expected-maximum-Sharpe approximation
/// `sqrt(2 ln(trials))` to account for multiple tested configurations. The
/// returned value is `P(true Sharpe > 0)` after that multiple-testing penalty.
///
/// # Errors
/// Returns [`StatisticalError`] for invalid counts or non-finite inputs.
#[allow(clippy::cast_precision_loss)]
pub fn deflated_sharpe_ratio(
    observed_sharpe: f64,
    trials: usize,
    observations: usize,
    skewness: f64,
    excess_kurtosis: f64,
) -> Result<f64, StatisticalError> {
    if trials == 0 || observations < 2 {
        return Err(StatisticalError::InsufficientObservations);
    }
    if [observed_sharpe, skewness, excess_kurtosis]
        .iter()
        .any(|value| !value.is_finite())
    {
        return Err(StatisticalError::NonFinite);
    }
    let variance = (1.0 - skewness * observed_sharpe
        + ((excess_kurtosis + 2.0) / 4.0) * observed_sharpe.powi(2))
        / (observations as f64 - 1.0);
    if !variance.is_finite() || variance <= 0.0 {
        return Err(StatisticalError::NonFinite);
    }
    let expected_max = if trials == 1 {
        0.0
    } else {
        (2.0 * (trials as f64).ln()).sqrt()
    };
    let z = (observed_sharpe - expected_max) / variance.sqrt();
    Ok(normal_cdf(z))
}

/// Estimates Probability of Backtest Overfitting from CPCV path scores.
///
/// Each matrix is `[trial][path]`. For every path, the trial with the highest
/// mean in-sample score is selected; its mean out-of-sample rank is measured
/// against all trials. PBO is the fraction of paths where that selected trial
/// lands below the median out-of-sample rank.
///
/// # Errors
/// Returns [`StatisticalError::InvalidMatrix`] for empty or ragged matrices.
#[allow(clippy::cast_precision_loss)]
pub fn probability_of_backtest_overfitting(
    in_sample: &[Vec<f64>],
    out_of_sample: &[Vec<f64>],
) -> Result<f64, StatisticalError> {
    if in_sample.is_empty()
        || in_sample.len() != out_of_sample.len()
        || in_sample[0].is_empty()
        || in_sample.iter().any(|row| row.len() != in_sample[0].len())
        || out_of_sample
            .iter()
            .any(|row| row.len() != in_sample[0].len())
    {
        return Err(StatisticalError::InvalidMatrix);
    }
    if in_sample
        .iter()
        .chain(out_of_sample.iter())
        .flatten()
        .any(|value| !value.is_finite())
    {
        return Err(StatisticalError::NonFinite);
    }
    let paths = in_sample[0].len();
    let mut overfit = 0_usize;
    for path in 0..paths {
        let selected = (0..in_sample.len())
            .max_by(|&left, &right| {
                let left_mean = in_sample[left].iter().sum::<f64>() / paths as f64;
                let right_mean = in_sample[right].iter().sum::<f64>() / paths as f64;
                left_mean
                    .partial_cmp(&right_mean)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| right.cmp(&left))
            })
            .ok_or(StatisticalError::InvalidMatrix)?;
        let selected_score = out_of_sample[selected][path];
        let rank = out_of_sample
            .iter()
            .filter(|row| row[path] <= selected_score)
            .count();
        if rank * 2 <= out_of_sample.len() {
            overfit = overfit.saturating_add(1);
        }
    }
    Ok(overfit as f64 / paths as f64)
}

fn normal_cdf(value: f64) -> f64 {
    1.0f64.midpoint(erf_approx(value / 2.0_f64.sqrt()))
}

fn erf_approx(value: f64) -> f64 {
    let sign = if value < 0.0 { -1.0 } else { 1.0 };
    let x = value.abs();
    let t = 1.0 / (1.0 + 0.327_591_1 * x);
    let polynomial =
        (((((1.061_405_429 * t - 1.453_152_027) * t) + 1.421_413_741) * t - 0.284_496_736) * t
            + 0.254_829_592)
            * t;
    sign * (1.0 - polynomial * (-x * x).exp())
}

/// Replay failure, including a non-monotonic journal sequence.
#[derive(Debug)]
pub enum ReplayError {
    /// Journal could not be opened or scanned.
    Journal(JournalError),
    /// Records violated the deterministic ordering contract.
    NonMonotonic {
        /// Last sequence observed.
        previous: u64,
        /// Sequence that violated ordering.
        current: u64,
    },
    /// Consumer rejected a replayed record.
    Apply(String),
    /// Injected replay clock arithmetic overflowed.
    TimeOverflow,
}

impl From<JournalError> for ReplayError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

/// Replays valid records in journal order and returns the count applied.
///
/// # Errors
/// Returns [`ReplayError`] if the journal cannot be read or sequence order is
/// not strictly increasing.
pub fn replay_path<F>(path: impl AsRef<Path>, mut apply: F) -> Result<usize, ReplayError>
where
    F: FnMut(&Record) -> Result<(), String>,
{
    replay_path_with_config(path, ReplayConfig::default(), |record, _context| {
        apply(record)
    })
}

/// Immutable deterministic clock/seed configuration for replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplayConfig {
    /// Monotonic timestamp assigned to the first record.
    pub initial_mono: MonoTime,
    /// Wall timestamp assigned to the first record.
    pub initial_wall: WallTime,
    /// Injected time increment between consecutive records.
    pub step_ns: u64,
    /// Seed propagated through the replay context.
    pub seed: u64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            initial_mono: MonoTime::from_nanos(0),
            initial_wall: WallTime::from_unix_nanos(0),
            step_ns: 1,
            seed: 0x9e37_79b9_7f4a_7c15,
        }
    }
}

/// Deterministic context supplied to one replayed journal record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplayContext {
    /// Journal sequence of the record being applied.
    pub sequence: u64,
    /// Zero-based position in the valid record stream.
    pub ordinal: u64,
    /// Injected monotonic timestamp; never reads system time.
    pub mono_time: MonoTime,
    /// Injected wall timestamp; never reads system time.
    pub wall_time: WallTime,
    /// Deterministically mixed seed for this record.
    pub seed: u64,
}

/// A normalized historical news item with an explicit availability time.
/// Publication time alone is not sufficient for point-in-time replay: the
/// item becomes visible only at `available_at` (receive/knowledge time).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayNewsItem {
    /// Stable provider/content identity.
    pub id: String,
    /// Timestamp at which the replay system may first observe the item.
    pub available_at: WallTime,
    /// Opaque normalized news payload.
    pub payload: Vec<u8>,
}

impl ReplayContext {
    /// Returns whether a news item was available at this replay point.
    #[must_use]
    pub fn news_available(&self, item: &ReplayNewsItem) -> bool {
        item.available_at.as_unix_nanos() <= self.wall_time.as_unix_nanos()
    }
}

/// Bounded, deterministically ordered point-in-time news source for replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayNewsBook {
    items: Vec<ReplayNewsItem>,
    capacity: usize,
}

impl ReplayNewsBook {
    /// Creates an empty news source with a hard item bound.
    #[must_use]
    pub fn new(capacity: usize) -> Option<Self> {
        (capacity > 0).then_some(Self {
            items: Vec::new(),
            capacity,
        })
    }

    /// Adds one item, retaining deterministic availability order.
    ///
    /// # Errors
    /// Returns `ReplayError::Apply` when the identity is blank or payload is
    /// empty. Capacity eviction removes the oldest availability item.
    pub fn insert(&mut self, item: ReplayNewsItem) -> Result<(), ReplayError> {
        if item.id.trim().is_empty() || item.payload.is_empty() {
            return Err(ReplayError::Apply("invalid replay news item".into()));
        }
        if self.items.iter().any(|existing| existing.id == item.id) {
            return Ok(());
        }
        self.items.push(item);
        self.items.sort_by(|left, right| {
            left.available_at
                .as_unix_nanos()
                .cmp(&right.available_at.as_unix_nanos())
                .then_with(|| left.id.cmp(&right.id))
        });
        if self.items.len() > self.capacity {
            self.items.remove(0);
        }
        Ok(())
    }

    /// Returns only news available at the supplied replay context.
    #[must_use]
    pub fn available_at(&self, context: &ReplayContext) -> Vec<ReplayNewsItem> {
        self.items
            .iter()
            .filter(|item| context.news_available(item))
            .cloned()
            .collect()
    }
}

/// Replays records with an injected clock and deterministic per-record seed.
///
/// The callback receives only records whose journal sequence is strictly
/// increasing. Time advances from [`ReplayConfig`] rather than consulting the
/// host clock, so live/replay logic can share the same clock contract.
///
/// # Errors
/// Returns [`ReplayError`] for journal failures, non-monotonic sequences,
/// injected-time overflow, or callback rejection.
pub fn replay_path_with_config<F>(
    path: impl AsRef<Path>,
    config: ReplayConfig,
    mut apply: F,
) -> Result<usize, ReplayError>
where
    F: FnMut(&Record, &ReplayContext) -> Result<(), String>,
{
    let journal = Journal::open(path)?;
    let records = journal.scan()?.records;
    let mut previous = None;
    for (ordinal, record) in records.iter().enumerate() {
        if let Some(last) = previous
            && record.sequence <= last
        {
            return Err(ReplayError::NonMonotonic {
                previous: last,
                current: record.sequence,
            });
        }
        let ordinal = u64::try_from(ordinal).map_err(|_| ReplayError::TimeOverflow)?;
        let elapsed = config
            .step_ns
            .checked_mul(ordinal)
            .ok_or(ReplayError::TimeOverflow)?;
        let mono_time = config
            .initial_mono
            .checked_add(elapsed)
            .ok_or(ReplayError::TimeOverflow)?;
        let wall_delta = i64::try_from(elapsed).map_err(|_| ReplayError::TimeOverflow)?;
        let wall_nanos = config
            .initial_wall
            .as_unix_nanos()
            .checked_add(wall_delta)
            .ok_or(ReplayError::TimeOverflow)?;
        let context = ReplayContext {
            sequence: record.sequence,
            ordinal,
            mono_time,
            wall_time: WallTime::from_unix_nanos(wall_nanos),
            seed: mix_seed(config.seed, record.sequence, ordinal),
        };
        apply(record, &context).map_err(ReplayError::Apply)?;
        previous = Some(record.sequence);
    }
    Ok(records.len())
}

fn mix_seed(seed: u64, sequence: u64, ordinal: u64) -> u64 {
    let mut value = seed ^ sequence.rotate_left(17) ^ ordinal.wrapping_mul(0x9e37_79b9);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use insider_common_types::{MonoTime, WallTime};
    use insider_journal::Journal;

    use super::{
        BacktestError, BacktestEvent, BacktestLedger, BacktestRunner, CpcvConfig, ReplayConfig,
        SUBSYSTEM_ID, deflated_sharpe_ratio, plan_cpcv, probability_of_backtest_overfitting,
        replay_path_with_config, return_statistics,
    };

    #[test]
    fn subsystem_id_is_non_empty_and_ascii() {
        assert!(!SUBSYSTEM_ID.is_empty());
        assert!(SUBSYSTEM_ID.is_ascii());
    }

    #[test]
    fn ledger_tracks_average_cost_realized_pnl_and_equity() {
        let mut ledger = BacktestLedger::new(10_000);
        assert!(ledger.apply_fill(10, 100).is_ok());
        assert!(ledger.apply_fill(-4, 110).is_ok());
        let snapshot = ledger.mark(110);
        assert_eq!(snapshot.map(|snapshot| snapshot.position_ticks), Some(6));
        assert_eq!(
            snapshot.map(|snapshot| snapshot.realized_pnl_ticks),
            Some(40)
        );
        assert_eq!(snapshot.map(|snapshot| snapshot.equity_ticks), Some(10_100));
    }

    #[test]
    fn configured_replay_context_is_clocked_and_seeded_without_system_time() {
        let path = std::env::temp_dir().join(format!(
            "insider-replay-{}-{}.journal",
            std::process::id(),
            1_u64
        ));
        let _ = std::fs::remove_file(&path);
        let journal = Journal::open(&path).ok();
        let Some(journal) = journal else {
            return;
        };
        assert!(journal.append(b"one").is_ok());
        assert!(journal.append(b"two").is_ok());
        let mut contexts = Vec::new();
        let count = replay_path_with_config(
            &path,
            ReplayConfig {
                initial_mono: MonoTime::from_nanos(100),
                initial_wall: WallTime::from_unix_nanos(1_000),
                step_ns: 25,
                seed: 7,
            },
            |record, context| {
                contexts.push((
                    record.sequence,
                    context.mono_time,
                    context.wall_time,
                    context.seed,
                ));
                Ok(())
            },
        );
        assert_eq!(count.ok(), Some(2));
        assert_eq!(contexts[0].0, 0);
        assert_eq!(contexts[1].1.as_nanos(), 125);
        assert_eq!(contexts[1].2.as_unix_nanos(), 1_025);
        assert_ne!(contexts[0].3, contexts[1].3);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn event_runner_accounts_fees_and_drawdown_in_sequence() {
        let mut runner = BacktestRunner::new(1_000);
        assert!(
            runner
                .apply(BacktestEvent::Fill {
                    sequence: 1,
                    quantity_ticks: 2,
                    price_ticks: 100,
                    fee_ticks: 3,
                })
                .is_ok()
        );
        assert!(
            runner
                .apply(BacktestEvent::Mark {
                    sequence: 2,
                    price_ticks: 110,
                })
                .is_ok()
        );
        assert!(
            runner
                .apply(BacktestEvent::Mark {
                    sequence: 3,
                    price_ticks: 90,
                })
                .is_ok()
        );
        let report = runner.finish().ok();
        assert_eq!(report.as_ref().map(|value| value.event_count), Some(3));
        assert_eq!(report.as_ref().map(|value| value.total_fees_ticks), Some(3));
        assert_eq!(report.map(|value| value.max_drawdown_ticks), Some(40));

        let mut unordered = BacktestRunner::new(1_000);
        assert!(
            unordered
                .apply(BacktestEvent::Mark {
                    sequence: 2,
                    price_ticks: 100,
                })
                .is_ok()
        );
        assert_eq!(
            unordered
                .apply(BacktestEvent::Mark {
                    sequence: 2,
                    price_ticks: 100,
                })
                .err(),
            Some(BacktestError::NonMonotonicSequence)
        );
    }

    #[test]
    fn cpcv_purges_embargo_and_locks_holdout() {
        let splits = plan_cpcv(
            20,
            CpcvConfig {
                groups: 4,
                test_groups: 2,
                embargo_events: 1,
                holdout_events: 2,
            },
        );
        assert!(splits.is_ok());
        let splits = splits.unwrap_or_default();
        assert_eq!(splits.len(), 6);
        for split in splits {
            assert!(split.test_ranges.iter().all(|range| range.end <= 18));
            assert!(split.train_ranges.iter().all(|range| range.end <= 18));
            for test in &split.test_ranges {
                assert!(
                    split
                        .train_ranges
                        .iter()
                        .all(|train| train.end < test.start || test.end <= train.start)
                );
            }
        }
    }

    #[test]
    fn statistical_validation_penalizes_multiple_trials_and_detects_overfit() {
        let statistics = return_statistics(&[0.01, 0.02, -0.005, 0.015, 0.01]);
        assert!(statistics.is_ok());
        let Some(statistics) = statistics.ok() else {
            return;
        };
        let probability = deflated_sharpe_ratio(
            statistics.sharpe,
            1,
            statistics.observations,
            statistics.skewness,
            statistics.excess_kurtosis,
        );
        assert!(probability.is_ok_and(|value| (0.0..=1.0).contains(&value)));
        let pbo = probability_of_backtest_overfitting(
            &[vec![10.0, 10.0], vec![1.0, 1.0]],
            &[vec![-1.0, -1.0], vec![2.0, 2.0]],
        );
        assert_eq!(pbo.ok(), Some(1.0));
    }

    #[test]
    fn backtest_report_exposes_curve_statistics_and_deflated_sharpe() {
        let mut runner = BacktestRunner::new(1_000);
        assert!(
            runner
                .apply(BacktestEvent::Fill {
                    sequence: 1,
                    quantity_ticks: 1,
                    price_ticks: 100,
                    fee_ticks: 0,
                })
                .is_ok()
        );
        for (sequence, price_ticks) in [(2, 110), (3, 105), (4, 115)] {
            assert!(
                runner
                    .apply(BacktestEvent::Mark {
                        sequence,
                        price_ticks,
                    })
                    .is_ok()
            );
        }
        let report = runner.finish();
        assert!(report.is_ok());
        let Some(report) = report.ok() else {
            return;
        };
        assert_eq!(report.period_returns().len(), 2);
        assert!(report.statistics().is_ok());
        assert!(report.deflated_sharpe(3).is_ok());
    }
}
