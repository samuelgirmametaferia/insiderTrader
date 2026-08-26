//! Portfolio positions and proposal-to-target conversion.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use insider_common_types::InstrumentId;
use insider_strategy_sdk::{Action, Proposal};

/// Position snapshot in canonical quantity/price ticks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Position {
    /// Signed quantity; positive long, negative short.
    pub quantity_ticks: i64,
    /// Last trusted mark price.
    pub mark_price_ticks: i64,
}

/// Portfolio target generated from a proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Target {
    /// Instrument identity.
    pub instrument_id: InstrumentId,
    /// Desired signed quantity.
    pub quantity_ticks: i64,
    /// Source proposal identity.
    pub proposal_id: insider_common_types::ProposalId,
}

/// One proposal-derived candidate supplied to the deterministic portfolio
/// optimizer. All quantities are signed canonical ticks and all prices are
/// positive reporting-currency ticks.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OptimizationCandidate {
    /// Desired absolute target produced by a strategy proposal.
    pub target: Target,
    /// Current signed portfolio quantity for this instrument.
    pub current_quantity_ticks: i64,
    /// Trusted mark used for notional constraints.
    pub mark_price_ticks: i64,
    /// Expected net return score in basis points over the proposal horizon.
    pub expected_return_bps: f64,
    /// Uncertainty penalty in basis points.
    pub uncertainty_bps: f64,
    /// Maximum absolute quantity this candidate may request from liquidity.
    pub max_participation_quantity_ticks: Option<i64>,
}

/// Explicit aggregate constraints for one optimization cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OptimizationConstraints {
    /// Maximum sum of absolute marked target notionals.
    pub max_gross_notional_ticks: i128,
    /// Maximum absolute signed net marked target notional.
    pub max_net_notional_ticks: i128,
    /// Maximum absolute marked position notional per instrument.
    pub max_position_notional_ticks: i128,
    /// Maximum aggregate absolute quantity turnover.
    pub max_turnover_quantity_ticks: i64,
}

/// Why one optimizer candidate was accepted, resized, or rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimizationDisposition {
    /// Requested target was accepted unchanged.
    Accepted,
    /// Target was reduced to satisfy one or more constraints.
    Resized,
    /// Candidate could not receive any admissible quantity.
    Rejected,
}

/// Deterministic per-candidate optimization diagnostic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OptimizationDecision {
    /// Source proposal identity.
    pub proposal_id: insider_common_types::ProposalId,
    /// Resulting target quantity.
    pub target_quantity_ticks: i64,
    /// Applied disposition.
    pub disposition: OptimizationDisposition,
}

/// Output of one deterministic aggregate target optimization.
#[derive(Clone, Debug, PartialEq)]
pub struct OptimizationResult {
    /// Accepted/resized targets in deterministic score order.
    pub targets: Vec<Target>,
    /// Diagnostics for every input candidate, including rejected candidates.
    pub decisions: Vec<OptimizationDecision>,
    /// Aggregate marked gross target notional.
    pub gross_notional_ticks: i128,
    /// Aggregate signed net target notional.
    pub net_notional_ticks: i128,
    /// Aggregate absolute quantity turnover.
    pub turnover_quantity_ticks: i64,
}

/// Failure constructing an aggregate optimization result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimizationError {
    /// A constraint is zero/negative or otherwise unusable.
    InvalidConstraints,
    /// A candidate has an invalid mark, score, or quantity.
    InvalidCandidate,
    /// Checked aggregate arithmetic overflowed.
    Overflow,
}

/// Allocates targets in deterministic descending net-score order.
///
/// This is intentionally a bounded, auditable allocator rather than a hidden
/// numerical solver. It applies per-instrument, gross/net, liquidity, and
/// turnover constraints while retaining a decision for every candidate. Fixed
/// inputs produce byte-stable ordering and quantities; callers can later swap
/// in a more sophisticated optimizer behind this contract without changing
/// risk or execution interfaces.
///
/// # Errors
/// Returns [`OptimizationError`] when constraints/candidates are invalid or
/// checked aggregate arithmetic cannot represent the result.
#[allow(clippy::too_many_lines)]
pub fn optimize_targets(
    candidates: &[OptimizationCandidate],
    constraints: OptimizationConstraints,
) -> Result<OptimizationResult, OptimizationError> {
    if constraints.max_gross_notional_ticks <= 0
        || constraints.max_net_notional_ticks <= 0
        || constraints.max_position_notional_ticks <= 0
        || constraints.max_turnover_quantity_ticks <= 0
    {
        return Err(OptimizationError::InvalidConstraints);
    }
    for candidate in candidates {
        if candidate.current_quantity_ticks == i64::MIN
            || candidate.mark_price_ticks <= 0
            || !candidate.expected_return_bps.is_finite()
            || !candidate.uncertainty_bps.is_finite()
            || candidate.uncertainty_bps < 0.0
            || candidate.target.quantity_ticks == i64::MIN
            || candidate
                .max_participation_quantity_ticks
                .is_some_and(|value| value <= 0)
        {
            return Err(OptimizationError::InvalidCandidate);
        }
    }
    let mut order: Vec<usize> = (0..candidates.len()).collect();
    order.sort_by(|left, right| {
        let l = candidates[*left].expected_return_bps - candidates[*left].uncertainty_bps;
        let r = candidates[*right].expected_return_bps - candidates[*right].uncertainty_bps;
        r.partial_cmp(&l)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                candidates[*left]
                    .target
                    .instrument_id
                    .cmp(&candidates[*right].target.instrument_id)
            })
            .then_with(|| {
                candidates[*left]
                    .target
                    .proposal_id
                    .cmp(&candidates[*right].target.proposal_id)
            })
    });

    let mut targets = Vec::new();
    let mut decisions = Vec::with_capacity(candidates.len());
    let mut gross = 0_i128;
    let mut net = 0_i128;
    let mut turnover = 0_i64;
    let mut allocated_instruments = std::collections::BTreeSet::new();
    for index in order {
        let candidate = candidates[index];
        let requested = candidate.target.quantity_ticks;
        let current = candidate.current_quantity_ticks;
        let max_position_quantity = i64::try_from(
            constraints.max_position_notional_ticks / i128::from(candidate.mark_price_ticks),
        )
        .map_err(|_| OptimizationError::Overflow)?;
        let mut target_quantity = requested.clamp(-max_position_quantity, max_position_quantity);
        if let Some(max_liquidity) = candidate.max_participation_quantity_ticks {
            let max_target = current
                .checked_add(max_liquidity)
                .ok_or(OptimizationError::Overflow)?;
            let min_target = current
                .checked_sub(max_liquidity)
                .ok_or(OptimizationError::Overflow)?;
            target_quantity = target_quantity.clamp(min_target, max_target);
        }
        let desired_delta = target_quantity
            .checked_sub(current)
            .ok_or(OptimizationError::Overflow)?;
        let remaining_turnover = constraints
            .max_turnover_quantity_ticks
            .checked_sub(turnover)
            .ok_or(OptimizationError::Overflow)?;
        let delta = desired_delta.clamp(-remaining_turnover, remaining_turnover);
        target_quantity = current
            .checked_add(delta)
            .ok_or(OptimizationError::Overflow)?;
        // One target per instrument is admitted in a cycle. This prevents
        // ordering-dependent double counting when opposing strategies have not
        // yet been netted by the coordinator.
        if !allocated_instruments.insert(candidate.target.instrument_id) {
            target_quantity = current;
        }
        let target_notional = i128::from(target_quantity)
            .checked_mul(i128::from(candidate.mark_price_ticks))
            .ok_or(OptimizationError::Overflow)?;
        let within_aggregate_limits = gross
            .checked_add(target_notional.abs())
            .is_some_and(|value| value <= constraints.max_gross_notional_ticks)
            && net
                .checked_add(target_notional)
                .is_some_and(|value| value.abs() <= constraints.max_net_notional_ticks);
        if !within_aggregate_limits {
            target_quantity = current;
        }
        let final_delta = target_quantity
            .checked_sub(current)
            .ok_or(OptimizationError::Overflow)?;
        let final_notional = i128::from(target_quantity)
            .checked_mul(i128::from(candidate.mark_price_ticks))
            .ok_or(OptimizationError::Overflow)?;
        gross = gross
            .checked_add(final_notional.abs())
            .ok_or(OptimizationError::Overflow)?;
        net = net
            .checked_add(final_notional)
            .ok_or(OptimizationError::Overflow)?;
        turnover = turnover
            .checked_add(final_delta.abs())
            .ok_or(OptimizationError::Overflow)?;
        let disposition = if final_delta
            == requested
                .checked_sub(current)
                .ok_or(OptimizationError::Overflow)?
        {
            OptimizationDisposition::Accepted
        } else if final_delta == 0 {
            OptimizationDisposition::Rejected
        } else {
            OptimizationDisposition::Resized
        };
        decisions.push(OptimizationDecision {
            proposal_id: candidate.target.proposal_id,
            target_quantity_ticks: target_quantity,
            disposition,
        });
        if final_delta != 0 {
            targets.push(Target {
                quantity_ticks: target_quantity,
                ..candidate.target
            });
        }
    }
    Ok(OptimizationResult {
        targets,
        decisions,
        gross_notional_ticks: gross,
        net_notional_ticks: net,
        turnover_quantity_ticks: turnover,
    })
}

/// Portfolio conversion failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetError {
    /// Target quantity would overflow its representation.
    Overflow,
    /// Proposal action cannot be converted without a current position.
    MissingPosition,
    /// Target weight was non-finite or outside `[-1, 1]`.
    InvalidWeight,
}

/// Accounting failure while applying an authoritative fill.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountingError {
    /// Fill quantity or price was not strictly positive.
    InvalidFill,
    /// Cash, position, or notional arithmetic exceeded the canonical range.
    Overflow,
    /// A corporate-action factor or dividend amount is invalid.
    InvalidCorporateAction,
    /// A corporate action would create a fractional canonical quantity.
    NonRepresentableCorporateAction,
}

/// Corporate action applied to one canonical position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CorporateActionKind {
    /// Changes quantity by `numerator / denominator` and inversely adjusts cost
    /// basis and mark so the marked notional is conserved.
    Split {
        /// New shares per old share.
        numerator: u64,
        /// Old shares represented by the numerator.
        denominator: u64,
    },
    /// Pays `amount_ticks` per signed quantity; short positions pay the amount.
    CashDividend {
        /// Cash amount per signed quantity.
        amount_ticks: i64,
    },
    /// Exercises a long option or settles an assigned option into an
    /// underlying position. Quantities and cash are signed deltas.
    OptionExercise {
        /// Underlying instrument receiving the position delta.
        underlying_instrument_id: InstrumentId,
        /// Signed option quantity removed from the option position.
        option_quantity_delta_ticks: i64,
        /// Signed underlying quantity created by the exercise.
        underlying_quantity_delta_ticks: i64,
        /// Signed cash settlement in reporting-currency ticks.
        cash_delta_ticks: i64,
    },
    /// Broker assignment settlement; fields have the same signed semantics as
    /// [`CorporateActionKind::OptionExercise`].
    OptionAssignment {
        /// Underlying instrument receiving the position delta.
        underlying_instrument_id: InstrumentId,
        /// Signed option quantity removed from the option position.
        option_quantity_delta_ticks: i64,
        /// Signed underlying quantity created by assignment.
        underlying_quantity_delta_ticks: i64,
        /// Signed cash settlement in reporting-currency ticks.
        cash_delta_ticks: i64,
    },
    /// Expires an option position and applies any broker cash settlement.
    OptionExpiry {
        /// Signed option quantity removed at expiry.
        option_quantity_delta_ticks: i64,
        /// Signed cash settlement in reporting-currency ticks.
        cash_delta_ticks: i64,
    },
    /// Futures variation-margin cash movement for the instrument.
    FuturesVariationMargin {
        /// Signed reporting-currency cash movement.
        cash_delta_ticks: i64,
    },
}

/// Immutable portfolio accounting record for one corporate action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CorporateActionEntry {
    /// Instrument affected by the action.
    pub instrument_id: InstrumentId,
    /// Applied action and validated parameters.
    pub kind: CorporateActionKind,
    /// Signed quantity before the action.
    pub quantity_before_ticks: i64,
    /// Signed quantity after the action.
    pub quantity_after_ticks: i64,
    /// Reporting-currency cash movement, if any.
    pub cash_delta_ticks: i64,
}

/// Account receiving one immutable fill posting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PostingAccount {
    /// Reporting-currency cash.
    Cash,
    /// Instrument inventory at execution value.
    Position,
    /// Realized transaction fee expense.
    Fees,
}

/// One signed double-entry posting. A complete fill must sum to zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerPosting {
    /// Posting account.
    pub account: PostingAccount,
    /// Signed amount in reporting-currency ticks.
    pub amount_ticks: i128,
}

/// Immutable double-entry record generated from one broker fill.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FillLedgerEntry {
    /// Instrument affected by the fill.
    pub instrument_id: InstrumentId,
    /// Signed quantity change; positive is buy.
    pub quantity_ticks: i64,
    /// Execution price in ticks.
    pub price_ticks: i64,
    /// Fee charged in cash ticks.
    pub fee_ticks: i64,
    /// Cash movement including the fee; buys decrease cash.
    pub cash_delta_ticks: i64,
    /// Realized `PnL` caused by closing an existing lot.
    pub realized_pnl_ticks: i64,
    /// Explicit balanced postings generated by the fill.
    pub postings: [LedgerPosting; 3],
}

impl FillLedgerEntry {
    /// Returns whether the postings balance exactly to zero.
    #[must_use]
    pub fn is_balanced(&self) -> bool {
        self.postings.iter().try_fold(0_i128, |total, posting| {
            total.checked_add(posting.amount_ticks)
        }) == Some(0)
    }
}

/// Authoritative position/cash snapshot used for target conversion.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Portfolio {
    positions: BTreeMap<InstrumentId, Position>,
    marks: BTreeMap<InstrumentId, i64>,
    /// Average execution cost is separate from the live valuation mark.
    /// Keeping this ledger independent prevents quote updates from changing
    /// realized-P&L accounting.
    cost_basis: BTreeMap<InstrumentId, i64>,
    /// Cash in the account's reporting currency ticks.
    pub cash_ticks: i64,
    /// Cumulative realized `PnL` in reporting-currency ticks.
    pub realized_pnl_ticks: i64,
    /// Cumulative fees in reporting-currency ticks.
    pub fees_ticks: i64,
    ledger: Vec<FillLedgerEntry>,
    corporate_actions: Vec<CorporateActionEntry>,
    peak_equity_ticks: Option<i128>,
}

impl Portfolio {
    /// Creates an empty portfolio.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets an externally reconciled position.
    pub fn set_position(&mut self, instrument_id: InstrumentId, position: Position) {
        if position.mark_price_ticks > 0 {
            self.marks.insert(instrument_id, position.mark_price_ticks);
            self.cost_basis
                .insert(instrument_id, position.mark_price_ticks);
        }
        self.positions.insert(instrument_id, position);
        self.record_equity_peak();
    }

    /// Replaces positions with an authoritative broker snapshot while
    /// preserving trusted marks for instruments that remain present.
    /// Zero-quantity entries are omitted so the projection cannot retain stale
    /// exposure after a broker reports a flat account.
    pub fn reconcile_positions<I>(&mut self, positions: I, cash_ticks: Option<i64>)
    where
        I: IntoIterator<Item = (InstrumentId, i64)>,
    {
        let previous = std::mem::take(&mut self.positions);
        let previous_cost_basis = std::mem::take(&mut self.cost_basis);
        for (instrument_id, quantity_ticks) in positions {
            if quantity_ticks == 0 {
                continue;
            }
            let mark_price_ticks = previous.get(&instrument_id).map_or_else(
                || self.marks.get(&instrument_id).copied().unwrap_or(0),
                |position| position.mark_price_ticks,
            );
            let cost_basis_ticks = previous_cost_basis
                .get(&instrument_id)
                .copied()
                .unwrap_or(mark_price_ticks);
            self.positions.insert(
                instrument_id,
                Position {
                    quantity_ticks,
                    mark_price_ticks,
                },
            );
            if cost_basis_ticks > 0 {
                self.cost_basis.insert(instrument_id, cost_basis_ticks);
            }
        }
        if let Some(cash_ticks) = cash_ticks {
            self.cash_ticks = cash_ticks;
        }
        self.record_equity_peak();
    }

    /// Updates the trusted mark for an instrument without creating a position.
    ///
    /// This is the market-data-to-risk handoff used when evaluating an opening
    /// target. A zero or negative mark is rejected so callers cannot accidentally
    /// make an instrument appear tradable with an invalid price.
    ///
    /// # Errors
    /// Returns [`AccountingError::InvalidFill`] when `price_ticks` is not positive.
    pub fn set_mark_price(
        &mut self,
        instrument_id: InstrumentId,
        price_ticks: i64,
    ) -> Result<(), AccountingError> {
        if price_ticks <= 0 {
            return Err(AccountingError::InvalidFill);
        }
        self.marks.insert(instrument_id, price_ticks);
        if let Some(position) = self.positions.get_mut(&instrument_id) {
            position.mark_price_ticks = price_ticks;
        }
        self.record_equity_peak();
        Ok(())
    }

    /// Applies one authoritative fill and records its cash/PnL double entry.
    ///
    /// Position `mark_price_ticks` is the valuation mark. Average execution
    /// cost is retained in the portfolio ledger and is not changed by quote
    /// updates.
    ///
    /// # Errors
    /// Returns [`AccountingError`] for invalid values or checked arithmetic
    /// overflow. No portfolio field changes on an error.
    #[allow(clippy::too_many_lines)]
    pub fn apply_fill(
        &mut self,
        instrument_id: InstrumentId,
        quantity_ticks: i64,
        price_ticks: i64,
        fee_ticks: i64,
    ) -> Result<FillLedgerEntry, AccountingError> {
        if quantity_ticks == 0 || price_ticks <= 0 || fee_ticks < 0 {
            return Err(AccountingError::InvalidFill);
        }
        let previous = self.position(instrument_id).unwrap_or(Position {
            quantity_ticks: 0,
            mark_price_ticks: price_ticks,
        });
        let previous_cost = self
            .cost_basis
            .get(&instrument_id)
            .copied()
            .unwrap_or(previous.mark_price_ticks);
        let same_direction = previous.quantity_ticks == 0
            || previous.quantity_ticks.signum() == quantity_ticks.signum();
        let (next_quantity, next_average, realized) = if same_direction {
            let next_quantity = previous
                .quantity_ticks
                .checked_add(quantity_ticks)
                .ok_or(AccountingError::Overflow)?;
            let old_notional = i128::from(previous.quantity_ticks.unsigned_abs())
                .checked_mul(i128::from(previous_cost))
                .ok_or(AccountingError::Overflow)?;
            let new_notional = i128::from(quantity_ticks.unsigned_abs())
                .checked_mul(i128::from(price_ticks))
                .ok_or(AccountingError::Overflow)?;
            let average = i64::try_from(
                old_notional
                    .checked_add(new_notional)
                    .ok_or(AccountingError::Overflow)?
                    / i128::from(next_quantity.unsigned_abs()),
            )
            .map_err(|_| AccountingError::Overflow)?;
            (next_quantity, average, 0_i64)
        } else {
            let close_quantity = previous
                .quantity_ticks
                .unsigned_abs()
                .min(quantity_ticks.unsigned_abs());
            let realized = i128::from(close_quantity)
                .checked_mul(i128::from(price_ticks) - i128::from(previous_cost))
                .ok_or(AccountingError::Overflow)?
                .checked_mul(i128::from(-quantity_ticks.signum()))
                .ok_or(AccountingError::Overflow)?;
            let remaining = previous
                .quantity_ticks
                .checked_add(quantity_ticks)
                .ok_or(AccountingError::Overflow)?;
            let average = if remaining == 0 {
                0
            } else if remaining.signum() == previous.quantity_ticks.signum() {
                previous_cost
            } else {
                price_ticks
            };
            (
                remaining,
                average,
                i64::try_from(realized).map_err(|_| AccountingError::Overflow)?,
            )
        };
        let cash_delta = i128::from(quantity_ticks)
            .checked_mul(i128::from(price_ticks))
            .ok_or(AccountingError::Overflow)?
            .checked_neg()
            .and_then(|value| value.checked_sub(i128::from(fee_ticks)))
            .ok_or(AccountingError::Overflow)?;
        let next_cash = i128::from(self.cash_ticks)
            .checked_add(cash_delta)
            .ok_or(AccountingError::Overflow)?;
        let next_realized = i128::from(self.realized_pnl_ticks)
            .checked_add(i128::from(realized))
            .ok_or(AccountingError::Overflow)?;
        let next_fees = i128::from(self.fees_ticks)
            .checked_add(i128::from(fee_ticks))
            .ok_or(AccountingError::Overflow)?;
        let cash_delta_ticks = i64::try_from(cash_delta).map_err(|_| AccountingError::Overflow)?;
        let entry = FillLedgerEntry {
            instrument_id,
            quantity_ticks,
            price_ticks,
            fee_ticks,
            cash_delta_ticks,
            realized_pnl_ticks: realized,
            postings: [
                LedgerPosting {
                    account: PostingAccount::Cash,
                    amount_ticks: cash_delta,
                },
                LedgerPosting {
                    account: PostingAccount::Position,
                    amount_ticks: i128::from(quantity_ticks)
                        .checked_mul(i128::from(price_ticks))
                        .ok_or(AccountingError::Overflow)?,
                },
                LedgerPosting {
                    account: PostingAccount::Fees,
                    amount_ticks: i128::from(fee_ticks),
                },
            ],
        };
        self.store_fill_position(
            instrument_id,
            next_quantity,
            next_average,
            price_ticks,
            previous.mark_price_ticks,
        );
        self.cash_ticks = i64::try_from(next_cash).map_err(|_| AccountingError::Overflow)?;
        self.realized_pnl_ticks =
            i64::try_from(next_realized).map_err(|_| AccountingError::Overflow)?;
        self.fees_ticks = i64::try_from(next_fees).map_err(|_| AccountingError::Overflow)?;
        self.ledger.push(entry);
        self.record_equity_peak();
        Ok(entry)
    }

    fn store_fill_position(
        &mut self,
        instrument_id: InstrumentId,
        quantity_ticks: i64,
        average_cost_ticks: i64,
        price_ticks: i64,
        previous_mark_ticks: i64,
    ) {
        self.marks.insert(instrument_id, price_ticks);
        let valuation_mark =
            self.marks
                .get(&instrument_id)
                .copied()
                .unwrap_or(if previous_mark_ticks > 0 {
                    previous_mark_ticks
                } else {
                    price_ticks
                });
        self.positions.insert(
            instrument_id,
            Position {
                quantity_ticks,
                mark_price_ticks: valuation_mark,
            },
        );
        if quantity_ticks == 0 {
            self.cost_basis.remove(&instrument_id);
        } else {
            self.cost_basis.insert(instrument_id, average_cost_ticks);
        }
    }

    /// Returns the immutable fill ledger in insertion order.
    #[must_use]
    pub fn ledger(&self) -> &[FillLedgerEntry] {
        &self.ledger
    }

    /// Returns false if any retained fill entry is not double-entry balanced.
    #[must_use]
    pub fn ledger_is_balanced(&self) -> bool {
        self.ledger.iter().all(FillLedgerEntry::is_balanced)
    }

    /// Applies a split to a position without changing marked notional.
    /// Quantities must remain exactly representable in canonical integer units;
    /// no partial mutation occurs when arithmetic or precision validation fails.
    ///
    /// # Errors
    /// Returns [`AccountingError::InvalidCorporateAction`] for zero factors,
    /// [`AccountingError::NonRepresentableCorporateAction`] for fractional
    /// quantities, or [`AccountingError::Overflow`] for checked arithmetic.
    pub fn apply_split(
        &mut self,
        instrument_id: InstrumentId,
        numerator: u64,
        denominator: u64,
    ) -> Result<CorporateActionEntry, AccountingError> {
        if numerator == 0 || denominator == 0 {
            return Err(AccountingError::InvalidCorporateAction);
        }
        let previous = self.position(instrument_id).unwrap_or(Position {
            quantity_ticks: 0,
            mark_price_ticks: 0,
        });
        let quantity_product = i128::from(previous.quantity_ticks)
            .checked_mul(i128::from(numerator))
            .ok_or(AccountingError::Overflow)?;
        let denominator_i128 = i128::from(denominator);
        if quantity_product % denominator_i128 != 0 {
            return Err(AccountingError::NonRepresentableCorporateAction);
        }
        let next_quantity = i64::try_from(quantity_product / denominator_i128)
            .map_err(|_| AccountingError::Overflow)?;
        let next_cost = self
            .cost_basis
            .get(&instrument_id)
            .copied()
            .map(|cost| scale_price(cost, denominator, numerator))
            .transpose()?
            .unwrap_or(0);
        let next_mark = self
            .marks
            .get(&instrument_id)
            .copied()
            .or_else(|| (previous.mark_price_ticks > 0).then_some(previous.mark_price_ticks))
            .map(|mark| scale_price(mark, denominator, numerator))
            .transpose()?
            .unwrap_or(0);
        if next_quantity != 0 {
            self.positions.insert(
                instrument_id,
                Position {
                    quantity_ticks: next_quantity,
                    mark_price_ticks: next_mark,
                },
            );
            if next_mark > 0 {
                self.marks.insert(instrument_id, next_mark);
            }
            if next_cost > 0 {
                self.cost_basis.insert(instrument_id, next_cost);
            }
        } else {
            self.positions.remove(&instrument_id);
            self.marks.remove(&instrument_id);
            self.cost_basis.remove(&instrument_id);
        }
        let entry = CorporateActionEntry {
            instrument_id,
            kind: CorporateActionKind::Split {
                numerator,
                denominator,
            },
            quantity_before_ticks: previous.quantity_ticks,
            quantity_after_ticks: next_quantity,
            cash_delta_ticks: 0,
        };
        self.corporate_actions.push(entry);
        self.record_equity_peak();
        Ok(entry)
    }

    /// Applies a cash dividend using the signed position quantity.
    ///
    /// # Errors
    /// Returns [`AccountingError::InvalidCorporateAction`] for a negative
    /// dividend or [`AccountingError::Overflow`] for checked cash arithmetic.
    pub fn apply_cash_dividend(
        &mut self,
        instrument_id: InstrumentId,
        amount_ticks: i64,
    ) -> Result<CorporateActionEntry, AccountingError> {
        if amount_ticks < 0 {
            return Err(AccountingError::InvalidCorporateAction);
        }
        let quantity = self
            .position(instrument_id)
            .map_or(0, |position| position.quantity_ticks);
        let cash_delta = i128::from(quantity)
            .checked_mul(i128::from(amount_ticks))
            .ok_or(AccountingError::Overflow)?;
        let next_cash = i128::from(self.cash_ticks)
            .checked_add(cash_delta)
            .ok_or(AccountingError::Overflow)?;
        let cash_delta_ticks = i64::try_from(cash_delta).map_err(|_| AccountingError::Overflow)?;
        let next_cash_ticks = i64::try_from(next_cash).map_err(|_| AccountingError::Overflow)?;
        self.cash_ticks = next_cash_ticks;
        let entry = CorporateActionEntry {
            instrument_id,
            kind: CorporateActionKind::CashDividend { amount_ticks },
            quantity_before_ticks: quantity,
            quantity_after_ticks: quantity,
            cash_delta_ticks,
        };
        self.corporate_actions.push(entry);
        self.record_equity_peak();
        Ok(entry)
    }

    /// Applies an option exercise settlement atomically across option,
    /// underlying, and cash positions.
    ///
    /// # Errors
    /// Returns [`AccountingError::InvalidCorporateAction`] for a zero option
    /// delta or self-settlement, or [`AccountingError::Overflow`] when any
    /// position/cash arithmetic is not representable.
    pub fn apply_option_exercise(
        &mut self,
        option_instrument_id: InstrumentId,
        underlying_instrument_id: InstrumentId,
        option_quantity_delta_ticks: i64,
        underlying_quantity_delta_ticks: i64,
        cash_delta_ticks: i64,
    ) -> Result<CorporateActionEntry, AccountingError> {
        self.apply_option_settlement(
            option_instrument_id,
            CorporateActionKind::OptionExercise {
                underlying_instrument_id,
                option_quantity_delta_ticks,
                underlying_quantity_delta_ticks,
                cash_delta_ticks,
            },
        )
    }

    /// Applies an option assignment settlement atomically across option,
    /// underlying, and cash positions.
    ///
    /// # Errors
    /// Returns [`AccountingError`] when the signed deltas are invalid or
    /// checked position/cash arithmetic overflows.
    pub fn apply_option_assignment(
        &mut self,
        option_instrument_id: InstrumentId,
        underlying_instrument_id: InstrumentId,
        option_quantity_delta_ticks: i64,
        underlying_quantity_delta_ticks: i64,
        cash_delta_ticks: i64,
    ) -> Result<CorporateActionEntry, AccountingError> {
        self.apply_option_settlement(
            option_instrument_id,
            CorporateActionKind::OptionAssignment {
                underlying_instrument_id,
                option_quantity_delta_ticks,
                underlying_quantity_delta_ticks,
                cash_delta_ticks,
            },
        )
    }

    /// Applies option expiry by removing the signed option quantity delta and
    /// recording any broker settlement.
    ///
    /// # Errors
    /// Returns [`AccountingError`] for a zero delta or checked arithmetic
    /// overflow.
    pub fn apply_option_expiry(
        &mut self,
        option_instrument_id: InstrumentId,
        option_quantity_delta_ticks: i64,
        cash_delta_ticks: i64,
    ) -> Result<CorporateActionEntry, AccountingError> {
        if option_quantity_delta_ticks == 0 || option_instrument_id.get() == 0 {
            return Err(AccountingError::InvalidCorporateAction);
        }
        let quantity_before = self
            .position(option_instrument_id)
            .map_or(0, |position| position.quantity_ticks);
        let quantity_after = quantity_before
            .checked_add(option_quantity_delta_ticks)
            .ok_or(AccountingError::Overflow)?;
        let cash_after = i128::from(self.cash_ticks)
            .checked_add(i128::from(cash_delta_ticks))
            .ok_or(AccountingError::Overflow)?;
        let cash_after = i64::try_from(cash_after).map_err(|_| AccountingError::Overflow)?;
        self.store_position_delta(option_instrument_id, quantity_after);
        self.cash_ticks = cash_after;
        Ok(CorporateActionEntry {
            instrument_id: option_instrument_id,
            kind: CorporateActionKind::OptionExpiry {
                option_quantity_delta_ticks,
                cash_delta_ticks,
            },
            quantity_before_ticks: quantity_before,
            quantity_after_ticks: quantity_after,
            cash_delta_ticks,
        })
    }

    /// Applies futures variation margin without changing the futures position.
    ///
    /// # Errors
    /// Returns [`AccountingError::Overflow`] when cash cannot represent the
    /// settlement.
    pub fn apply_futures_variation_margin(
        &mut self,
        instrument_id: InstrumentId,
        cash_delta_ticks: i64,
    ) -> Result<CorporateActionEntry, AccountingError> {
        if instrument_id.get() == 0 {
            return Err(AccountingError::InvalidCorporateAction);
        }
        let quantity = self
            .position(instrument_id)
            .map_or(0, |position| position.quantity_ticks);
        self.apply_cash_delta(cash_delta_ticks)?;
        Ok(CorporateActionEntry {
            instrument_id,
            kind: CorporateActionKind::FuturesVariationMargin { cash_delta_ticks },
            quantity_before_ticks: quantity,
            quantity_after_ticks: quantity,
            cash_delta_ticks,
        })
    }

    #[allow(clippy::manual_let_else)]
    fn apply_option_settlement(
        &mut self,
        option_instrument_id: InstrumentId,
        kind: CorporateActionKind,
    ) -> Result<CorporateActionEntry, AccountingError> {
        let (underlying_instrument_id, option_delta, underlying_delta, cash_delta) = match kind {
            CorporateActionKind::OptionExercise {
                underlying_instrument_id,
                option_quantity_delta_ticks,
                underlying_quantity_delta_ticks,
                cash_delta_ticks,
            }
            | CorporateActionKind::OptionAssignment {
                underlying_instrument_id,
                option_quantity_delta_ticks,
                underlying_quantity_delta_ticks,
                cash_delta_ticks,
            } => (
                underlying_instrument_id,
                option_quantity_delta_ticks,
                underlying_quantity_delta_ticks,
                cash_delta_ticks,
            ),
            _ => return Err(AccountingError::InvalidCorporateAction),
        };
        if option_instrument_id == underlying_instrument_id
            || option_delta == 0
            || underlying_delta == 0
        {
            return Err(AccountingError::InvalidCorporateAction);
        }
        let option_before = self
            .position(option_instrument_id)
            .map_or(0, |position| position.quantity_ticks);
        let option_after = option_before
            .checked_add(option_delta)
            .ok_or(AccountingError::Overflow)?;
        let underlying_before = self
            .position(underlying_instrument_id)
            .map_or(0, |position| position.quantity_ticks);
        let underlying_after = underlying_before
            .checked_add(underlying_delta)
            .ok_or(AccountingError::Overflow)?;
        let cash_after = i128::from(self.cash_ticks)
            .checked_add(i128::from(cash_delta))
            .ok_or(AccountingError::Overflow)?;
        let cash_after = i64::try_from(cash_after).map_err(|_| AccountingError::Overflow)?;
        self.store_position_delta(option_instrument_id, option_after);
        self.store_position_delta(underlying_instrument_id, underlying_after);
        self.cash_ticks = cash_after;
        let entry = CorporateActionEntry {
            instrument_id: option_instrument_id,
            kind,
            quantity_before_ticks: option_before,
            quantity_after_ticks: option_after,
            cash_delta_ticks: cash_delta,
        };
        self.corporate_actions.push(entry);
        self.record_equity_peak();
        Ok(entry)
    }

    fn store_position_delta(&mut self, instrument_id: InstrumentId, quantity: i64) {
        let mark = self
            .position(instrument_id)
            .map_or(0, |position| position.mark_price_ticks);
        if quantity == 0 {
            self.positions.remove(&instrument_id);
            self.cost_basis.remove(&instrument_id);
        } else {
            self.positions.insert(
                instrument_id,
                Position {
                    quantity_ticks: quantity,
                    mark_price_ticks: mark,
                },
            );
        }
    }

    fn apply_cash_delta(&mut self, cash_delta: i64) -> Result<(), AccountingError> {
        let next = i128::from(self.cash_ticks)
            .checked_add(i128::from(cash_delta))
            .ok_or(AccountingError::Overflow)?;
        self.cash_ticks = i64::try_from(next).map_err(|_| AccountingError::Overflow)?;
        Ok(())
    }

    /// Returns immutable corporate-action accounting records in application order.
    #[must_use]
    pub fn corporate_actions(&self) -> &[CorporateActionEntry] {
        &self.corporate_actions
    }

    /// Reads a current position.
    #[must_use]
    pub fn position(&self, instrument_id: InstrumentId) -> Option<Position> {
        self.positions.get(&instrument_id).copied()
    }

    /// Returns the latest trusted mark, including marks for instruments with no position.
    #[must_use]
    pub fn mark_price(&self, instrument_id: InstrumentId) -> Option<i64> {
        self.marks.get(&instrument_id).copied()
    }

    /// Returns the average execution cost retained for a live position.
    #[must_use]
    pub fn average_cost_price(&self, instrument_id: InstrumentId) -> Option<i64> {
        self.cost_basis.get(&instrument_id).copied()
    }

    /// Returns all reconciled positions in canonical instrument order.
    pub fn positions(&self) -> impl Iterator<Item = (InstrumentId, Position)> + '_ {
        self.positions
            .iter()
            .map(|(instrument, position)| (*instrument, *position))
    }

    /// Returns marked account equity in reporting-currency ticks.
    #[must_use]
    pub fn equity_ticks(&self) -> Option<i128> {
        let positions =
            self.positions
                .values()
                .try_fold(i128::from(self.cash_ticks), |total, position| {
                    let notional = i128::from(position.quantity_ticks)
                        .checked_mul(i128::from(position.mark_price_ticks))?;
                    total.checked_add(notional)
                })?;
        Some(positions)
    }

    /// Returns the highest marked equity observed by this portfolio.
    #[must_use]
    pub const fn peak_equity_ticks(&self) -> Option<i128> {
        self.peak_equity_ticks
    }

    /// Returns peak-to-equity drawdown in basis points when the high-water
    /// mark is positive and the arithmetic is representable.
    #[must_use]
    pub fn drawdown_bps(&self) -> Option<i64> {
        let peak = self.peak_equity_ticks?;
        let equity = self.equity_ticks()?;
        if peak <= 0 || equity >= peak {
            return Some(0);
        }
        i64::try_from(peak.checked_sub(equity)?.checked_mul(10_000)? / peak).ok()
    }

    /// Restores a journaled high-water mark during startup replay. Recovery
    /// cannot lower an already observed peak.
    pub fn restore_peak_equity_ticks(&mut self, peak: Option<i128>) {
        if let Some(peak) = peak {
            self.peak_equity_ticks = Some(self.peak_equity_ticks.unwrap_or(peak).max(peak));
        }
    }

    fn record_equity_peak(&mut self) {
        if let Some(equity) = self.equity_ticks() {
            self.peak_equity_ticks = Some(
                self.peak_equity_ticks
                    .map_or(equity, |peak| peak.max(equity)),
            );
        }
    }

    /// Converts a strategy action to an absolute target.
    ///
    /// # Errors
    /// Returns [`TargetError::MissingPosition`] for relative actions without a
    /// reconciled position, or [`TargetError::Overflow`] for arithmetic overflow.
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    pub fn target_from_proposal(&self, proposal: &Proposal) -> Result<Target, TargetError> {
        let current = self
            .position(proposal.instrument_id)
            .map_or(0, |position| position.quantity_ticks);
        let quantity = match proposal.action {
            Action::NoAction => current,
            Action::TargetQuantity { quantity_ticks } => quantity_ticks,
            Action::Increase { quantity_ticks } => current
                .checked_add(quantity_ticks)
                .ok_or(TargetError::Overflow)?,
            Action::Decrease { quantity_ticks } => current
                .checked_sub(quantity_ticks)
                .ok_or(TargetError::Overflow)?,
            Action::Close => 0,
            Action::TargetWeight { weight } => {
                if !weight.is_finite() || !(-1.0..=1.0).contains(&weight) {
                    return Err(TargetError::InvalidWeight);
                }
                let mark_price_ticks = self
                    .mark_price(proposal.instrument_id)
                    .or_else(|| {
                        self.position(proposal.instrument_id)
                            .map(|position| position.mark_price_ticks)
                    })
                    .ok_or(TargetError::MissingPosition)?;
                if mark_price_ticks <= 0 {
                    return Err(TargetError::MissingPosition);
                }
                let equity = self.equity_ticks().ok_or(TargetError::Overflow)?;
                #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
                let desired = (equity as f64 * weight / mark_price_ticks as f64).round();
                if !desired.is_finite() || desired < i64::MIN as f64 || desired > i64::MAX as f64 {
                    return Err(TargetError::Overflow);
                }
                #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
                {
                    desired as i64
                }
            }
        };
        Ok(Target {
            instrument_id: proposal.instrument_id,
            quantity_ticks: quantity,
            proposal_id: proposal.proposal_id,
        })
    }
}

fn scale_price(price_ticks: i64, numerator: u64, denominator: u64) -> Result<i64, AccountingError> {
    if price_ticks <= 0 || numerator == 0 || denominator == 0 {
        return Err(AccountingError::InvalidCorporateAction);
    }
    let scaled = i128::from(price_ticks)
        .checked_mul(i128::from(numerator))
        .ok_or(AccountingError::Overflow)?;
    let rounded = scaled
        .checked_add(i128::from(denominator) / 2)
        .ok_or(AccountingError::Overflow)?
        / i128::from(denominator);
    i64::try_from(rounded).map_err(|_| AccountingError::Overflow)
}

#[cfg(test)]
mod tests {
    use insider_common_types::{InstrumentId, MonoTime, ProposalId};
    use insider_strategy_sdk::{Action, Proposal};

    use super::{
        CorporateActionKind, FillLedgerEntry, OptimizationCandidate, OptimizationConstraints,
        OptimizationDisposition, Portfolio, Position, Target, optimize_targets,
    };

    fn proposal(action: Action) -> Option<Proposal> {
        Some(Proposal {
            proposal_id: ProposalId::new(1).ok()?,
            strategy_id: String::from("s.v1"),
            instrument_id: InstrumentId::new(1).ok()?,
            action,
            confidence: 0.8,
            horizon_ns: 100,
            ttl_ns: 10,
            evidence: Vec::new(),
            generated_mono: MonoTime::from_nanos(1),
        })
    }

    #[test]
    fn relative_actions_use_reconciled_position_and_close_zeroes_it() {
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
        let Some(increase) = proposal(Action::Increase { quantity_ticks: 3 }) else {
            return;
        };
        assert_eq!(
            portfolio
                .target_from_proposal(&increase)
                .ok()
                .map(|target| target.quantity_ticks),
            Some(13)
        );
        let Some(close) = proposal(Action::Close) else {
            return;
        };
        assert_eq!(
            portfolio
                .target_from_proposal(&close)
                .ok()
                .map(|target| target.quantity_ticks),
            Some(0)
        );
    }

    #[test]
    fn target_weight_uses_marked_equity_and_rejects_invalid_weights() {
        let Some(instrument_id) = InstrumentId::new(1).ok() else {
            return;
        };
        let mut portfolio = Portfolio::new();
        portfolio.cash_ticks = 900;
        portfolio.set_position(
            instrument_id,
            Position {
                quantity_ticks: 1,
                mark_price_ticks: 100,
            },
        );
        let Some(weight) = proposal(Action::TargetWeight { weight: 0.5 }) else {
            return;
        };
        assert_eq!(
            portfolio
                .target_from_proposal(&weight)
                .ok()
                .map(|target| target.quantity_ticks),
            Some(5)
        );
        let Some(flat_instrument) = InstrumentId::new(2).ok() else {
            return;
        };
        assert!(portfolio.set_mark_price(flat_instrument, 50).is_ok());
        let mut opening = weight;
        opening.instrument_id = flat_instrument;
        assert_eq!(
            portfolio
                .target_from_proposal(&opening)
                .ok()
                .map(|target| target.quantity_ticks),
            Some(10)
        );
        let Some(invalid) = proposal(Action::TargetWeight { weight: 2.0 }) else {
            return;
        };
        assert_eq!(
            portfolio.target_from_proposal(&invalid),
            Err(super::TargetError::InvalidWeight)
        );
    }

    #[test]
    fn optimizer_applies_deterministic_gross_and_turnover_bounds() {
        let Some(first) = InstrumentId::new(1).ok() else {
            return;
        };
        let Some(second) = InstrumentId::new(2).ok() else {
            return;
        };
        let Some(first_proposal) = ProposalId::new(1).ok() else {
            return;
        };
        let Some(second_proposal) = ProposalId::new(2).ok() else {
            return;
        };
        let candidates = [
            OptimizationCandidate {
                target: Target {
                    instrument_id: first,
                    quantity_ticks: 10,
                    proposal_id: first_proposal,
                },
                current_quantity_ticks: 0,
                mark_price_ticks: 100,
                expected_return_bps: 900.0,
                uncertainty_bps: 100.0,
                max_participation_quantity_ticks: None,
            },
            OptimizationCandidate {
                target: Target {
                    instrument_id: second,
                    quantity_ticks: 20,
                    proposal_id: second_proposal,
                },
                current_quantity_ticks: 0,
                mark_price_ticks: 100,
                expected_return_bps: 800.0,
                uncertainty_bps: 100.0,
                max_participation_quantity_ticks: None,
            },
        ];
        let result = optimize_targets(
            &candidates,
            OptimizationConstraints {
                max_gross_notional_ticks: 1_500,
                max_net_notional_ticks: 1_500,
                max_position_notional_ticks: 5_000,
                max_turnover_quantity_ticks: 10,
            },
        )
        .unwrap_or_else(|_| unreachable!());
        assert_eq!(result.targets.len(), 1);
        assert_eq!(result.targets[0].instrument_id, first);
        assert_eq!(result.gross_notional_ticks, 1_000);
        assert_eq!(result.turnover_quantity_ticks, 10);
        assert_eq!(
            result.decisions[1].disposition,
            OptimizationDisposition::Rejected
        );
    }

    #[test]
    fn fill_ledger_balances_cash_fees_average_cost_and_realized_pnl() {
        let Some(instrument) = InstrumentId::new(7).ok() else {
            return;
        };
        let mut portfolio = Portfolio::new();
        portfolio.cash_ticks = 10_000;
        assert!(portfolio.apply_fill(instrument, 10, 100, 2).is_ok());
        assert!(portfolio.set_mark_price(instrument, 120).is_ok());
        assert!(portfolio.apply_fill(instrument, -4, 110, 1).is_ok());
        assert_eq!(
            portfolio.position(instrument).map(|p| p.quantity_ticks),
            Some(6)
        );
        assert_eq!(
            portfolio.position(instrument).map(|p| p.mark_price_ticks),
            Some(110)
        );
        assert_eq!(portfolio.cash_ticks, 9_437);
        assert_eq!(portfolio.realized_pnl_ticks, 40);
        assert_eq!(portfolio.fees_ticks, 3);
        assert_eq!(portfolio.ledger().len(), 2);
        assert!(portfolio.ledger_is_balanced());
        assert!(portfolio.ledger().iter().all(FillLedgerEntry::is_balanced));
    }

    #[test]
    fn corporate_actions_adjust_position_basis_and_cash_atomically() {
        let Some(instrument) = InstrumentId::new(8).ok() else {
            return;
        };
        let mut portfolio = Portfolio::new();
        portfolio.cash_ticks = 1_000;
        portfolio.set_position(
            instrument,
            Position {
                quantity_ticks: 10,
                mark_price_ticks: 100,
            },
        );
        assert!(portfolio.apply_split(instrument, 2, 1).is_ok());
        assert_eq!(
            portfolio.position(instrument).map(|p| p.quantity_ticks),
            Some(20)
        );
        assert_eq!(portfolio.average_cost_price(instrument), Some(50));
        assert_eq!(portfolio.mark_price(instrument), Some(50));
        assert!(portfolio.apply_cash_dividend(instrument, 3).is_ok());
        assert_eq!(portfolio.cash_ticks, 1_060);
        assert!(matches!(
            portfolio
                .corporate_actions()
                .first()
                .map(|entry| entry.kind),
            Some(CorporateActionKind::Split {
                numerator: 2,
                denominator: 1
            })
        ));
        assert!(portfolio.apply_split(instrument, 1, 3).is_err());
        assert_eq!(
            portfolio.position(instrument).map(|p| p.quantity_ticks),
            Some(20)
        );
    }

    #[test]
    fn derivative_settlements_update_option_underlying_and_margin_cash() {
        let Some(option) = InstrumentId::new(80).ok() else {
            return;
        };
        let Some(underlying) = InstrumentId::new(81).ok() else {
            return;
        };
        let mut portfolio = Portfolio::new();
        portfolio.cash_ticks = 1_000;
        portfolio.set_position(
            option,
            Position {
                quantity_ticks: 10,
                mark_price_ticks: 5,
            },
        );
        assert!(
            portfolio
                .apply_option_exercise(option, underlying, -10, 100, -500)
                .is_ok()
        );
        assert_eq!(portfolio.position(option), None);
        assert_eq!(
            portfolio.position(underlying).map(|p| p.quantity_ticks),
            Some(100)
        );
        assert_eq!(portfolio.cash_ticks, 500);
        assert!(
            portfolio
                .apply_futures_variation_margin(underlying, 75)
                .is_ok()
        );
        assert_eq!(portfolio.cash_ticks, 575);
    }

    #[test]
    fn high_water_mark_tracks_marks_and_restores_monotonically() {
        let Some(instrument) = InstrumentId::new(9).ok() else {
            return;
        };
        let mut portfolio = Portfolio::new();
        portfolio.cash_ticks = 1_000;
        portfolio.set_position(
            instrument,
            Position {
                quantity_ticks: 1,
                mark_price_ticks: 100,
            },
        );
        assert_eq!(portfolio.peak_equity_ticks(), Some(1_100));
        assert!(portfolio.set_mark_price(instrument, 200).is_ok());
        assert_eq!(portfolio.peak_equity_ticks(), Some(1_200));
        assert!(portfolio.set_mark_price(instrument, 150).is_ok());
        assert_eq!(portfolio.drawdown_bps(), Some(416));

        portfolio.restore_peak_equity_ticks(Some(1_150));
        assert_eq!(portfolio.peak_equity_ticks(), Some(1_200));
        portfolio.restore_peak_equity_ticks(Some(1_300));
        assert_eq!(portfolio.peak_equity_ticks(), Some(1_300));
        assert_eq!(portfolio.drawdown_bps(), Some(1_153));
    }
}
