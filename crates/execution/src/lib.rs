//! Risk-gated execution planning with deterministic, idempotent client IDs.

#![forbid(unsafe_code)]

use insider_broker_api::BrokerEvent;
use insider_broker_api::{
    BrokerGateway, Capabilities, OrderIntent, OrderState, OrderType, Side, TimeInForce,
};
use insider_common_types::{AccountId, TraceId};
use insider_portfolio::{Portfolio, Target};
use insider_risk_engine::{Decision, Reason, RiskEngine, RiskInputs};

/// Execution planning failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanError {
    /// Target equals current position.
    NoDelta,
    /// Independent risk engine denied the target.
    RiskDenied(Reason),
    /// Broker capabilities do not support the planned order.
    UnsupportedOrder,
    /// Child-order schedule has invalid bounds or weights.
    InvalidSchedule,
    /// A child-order lifecycle transition or fill quantity was invalid.
    InvalidChildTransition,
}

/// One immutable execution measurement captured from the broker/event journal.
/// All prices and quantities remain integer ticks; no floating point is used in
/// cost or shortfall calculations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcaFill {
    /// Filled quantity in canonical positive ticks.
    pub quantity_ticks: i64,
    /// Execution price in canonical integer ticks.
    pub price_ticks: i64,
}

/// Inputs required to calculate transaction-cost analysis for one parent order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TcaInput {
    /// Buy or sell side; this determines the sign of implementation shortfall.
    pub side: Side,
    /// Arrival/decision price captured before the first send attempt.
    pub arrival_price_ticks: i64,
    /// Monotonic send timestamp.
    pub sent_mono_ns: u64,
    /// Optional broker acknowledgement timestamp.
    pub ack_mono_ns: Option<u64>,
    /// Fill events in journal order.
    pub fills: Vec<TcaFill>,
    /// Optional observed market volume over the measurement window.
    pub market_volume_ticks: Option<i64>,
    /// Optional quoted spread at arrival, in price ticks.
    pub arrival_spread_ticks: Option<i64>,
}

/// Integer-exact TCA result. Optional fields are absent when the source event
/// stream did not provide the measurement; callers must not infer them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TcaResult {
    /// Total filled quantity.
    pub filled_quantity_ticks: i64,
    /// Volume-weighted average execution price, represented as a rational pair.
    pub average_fill_price_numerator: i128,
    /// Positive filled quantity denominator for the average price fraction.
    pub average_fill_price_denominator: i64,
    /// Signed implementation shortfall in tick-value units. Positive means cost.
    pub implementation_shortfall_tick_value: i128,
    /// Send-to-acknowledgement latency, when acknowledgement timing exists.
    pub ack_latency_ns: Option<u64>,
    /// Filled quantity divided by observed market volume, in basis points.
    pub participation_bps: Option<u32>,
    /// Arrival spread, when supplied by the market-data snapshot.
    pub arrival_spread_ticks: Option<i64>,
}

/// Computes TCA without inventing missing broker measurements.
///
/// # Errors
/// Returns [`PlanError::InvalidChildTransition`] when a required value is
/// non-positive, timestamps are out of order, arithmetic overflows, or a fill
/// set is empty.
pub fn calculate_tca(input: &TcaInput) -> Result<TcaResult, PlanError> {
    if input.arrival_price_ticks <= 0
        || input.fills.is_empty()
        || input
            .fills
            .iter()
            .any(|fill| fill.quantity_ticks <= 0 || fill.price_ticks <= 0)
        || input.market_volume_ticks.is_some_and(|volume| volume <= 0)
        || input.arrival_spread_ticks.is_some_and(|spread| spread < 0)
    {
        return Err(PlanError::InvalidChildTransition);
    }
    if let Some(ack) = input.ack_mono_ns
        && ack < input.sent_mono_ns
    {
        return Err(PlanError::InvalidChildTransition);
    }
    let filled = input
        .fills
        .iter()
        .try_fold(0_i64, |total, fill| total.checked_add(fill.quantity_ticks))
        .ok_or(PlanError::InvalidChildTransition)?;
    let notional = input
        .fills
        .iter()
        .try_fold(0_i128, |total, fill| {
            total.checked_add(i128::from(fill.quantity_ticks) * i128::from(fill.price_ticks))
        })
        .ok_or(PlanError::InvalidChildTransition)?;
    let arrival_notional = i128::from(filled) * i128::from(input.arrival_price_ticks);
    let delta = notional
        .checked_sub(arrival_notional)
        .ok_or(PlanError::InvalidChildTransition)?;
    let implementation_shortfall_tick_value = match input.side {
        Side::Buy => delta,
        Side::Sell => delta
            .checked_neg()
            .ok_or(PlanError::InvalidChildTransition)?,
    };
    let participation_bps = input.market_volume_ticks.map(|volume| {
        u32::try_from((i128::from(filled) * 10_000 / i128::from(volume)).min(i128::from(u32::MAX)))
            .unwrap_or(u32::MAX)
    });
    Ok(TcaResult {
        filled_quantity_ticks: filled,
        average_fill_price_numerator: notional,
        average_fill_price_denominator: filled,
        implementation_shortfall_tick_value,
        ack_latency_ns: input.ack_mono_ns.map(|ack| ack - input.sent_mono_ns),
        participation_bps,
        arrival_spread_ticks: input.arrival_spread_ticks,
    })
}

/// Deterministic child-order scheduling policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Schedule {
    /// Submit one child immediately.
    Immediate,
    /// Split into equal slices at a fixed interval.
    Twap {
        /// Number of child orders.
        slices: usize,
        /// Nanoseconds between child due times.
        interval_ns: u64,
    },
    /// Split according to positive relative volume weights.
    Vwap {
        /// Relative volume weights, one per child.
        weights: Vec<u32>,
    },
    /// Participate at a fixed rate of observed market volume.
    Pov {
        /// Participation in basis points, constrained to `1..=10_000`.
        participation_bps: u32,
        /// Nanoseconds between child orders.
        interval_ns: u64,
        /// Observed market volume for each planned slice.
        market_volume_ticks: Vec<i64>,
    },
    /// Front-loads quantity according to an urgency budget for implementation
    /// shortfall minimization. Urgency `0` is uniform; `10_000` is maximally
    /// front-loaded while preserving at least one tick per child.
    ImplementationShortfall {
        /// Number of child orders.
        slices: usize,
        /// Nanoseconds between child due times.
        interval_ns: u64,
        /// Front-loading urgency in basis points, constrained to `0..=10_000`.
        urgency_bps: u32,
    },
    /// Selects POV in benign conditions and front-loaded implementation
    /// shortfall when spread or volatility is stressed. Selection is made once
    /// from the supplied snapshot so replay and live scheduling share exactly
    /// the same child-order sequence.
    Adaptive {
        /// Maximum number of child orders in the selected schedule.
        slices: usize,
        /// Nanoseconds between child due times.
        interval_ns: u64,
        /// Desired participation/urgency in basis points.
        urgency_bps: u32,
        /// Observed spread in price ticks.
        spread_ticks: i64,
        /// Maximum spread for POV selection.
        max_spread_ticks: i64,
        /// Observed volatility in basis points.
        volatility_bps: u32,
        /// Maximum volatility for POV selection.
        max_volatility_bps: u32,
        /// Observed market volume per candidate slice.
        market_volume_ticks: Vec<i64>,
    },
}

/// Deterministic limit-price policy derived from a validated top-of-book quote.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LimitStyle {
    /// Cross the spread up to the current opposite quote to seek immediacy.
    Marketable,
    /// Rest at the current same-side quote to prioritize price improvement.
    Passive,
}

/// Minimal quote required to derive a limit price without provider-specific
/// types. Prices are canonical positive integer ticks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionQuote {
    /// Best bid in integer ticks.
    pub bid_ticks: i64,
    /// Best ask in integer ticks.
    pub ask_ticks: i64,
}

/// Converts a market intent into a deterministic marketable/passive limit
/// intent. The returned intent preserves identity, quantity, side, account,
/// time-in-force, and trace while changing only order type and limit price.
///
/// # Errors
/// Returns [`PlanError::InvalidSchedule`] for malformed or crossed quotes, or
/// [`PlanError::UnsupportedOrder`] when the input is not a market order.
pub fn apply_limit_style(
    intent: &OrderIntent,
    quote: ExecutionQuote,
    style: LimitStyle,
) -> Result<OrderIntent, PlanError> {
    if intent.order_type != OrderType::Market
        || intent.quantity_ticks <= 0
        || quote.bid_ticks <= 0
        || quote.ask_ticks < quote.bid_ticks
    {
        return Err(if intent.order_type == OrderType::Market {
            PlanError::InvalidSchedule
        } else {
            PlanError::UnsupportedOrder
        });
    }
    let limit_price_ticks = match (intent.side, style) {
        (Side::Buy, LimitStyle::Marketable) | (Side::Sell, LimitStyle::Passive) => quote.ask_ticks,
        (Side::Buy, LimitStyle::Passive) | (Side::Sell, LimitStyle::Marketable) => quote.bid_ticks,
    };
    let mut planned = intent.clone();
    planned.order_type = OrderType::Limit;
    planned.limit_price_ticks = Some(limit_price_ticks);
    Ok(planned)
}

/// One deterministic child order derived from a parent intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildOrder {
    /// Parent client order ID.
    pub parent_client_order_id: String,
    /// Stable one-based child sequence.
    pub child_sequence: u32,
    /// Derived child client order ID.
    pub client_order_id: String,
    /// Absolute child quantity.
    pub quantity_ticks: i64,
    /// Relative due time from plan creation.
    pub due_after_ns: u64,
    /// Parent side.
    pub side: Side,
    /// Parent order type.
    pub order_type: OrderType,
    /// Parent limit price, if any.
    pub limit_price_ticks: Option<i64>,
}

/// Lifecycle state for one planned child order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildState {
    /// Due time has not arrived.
    Pending,
    /// Claimed for one send attempt; a transport failure is ambiguous.
    Sending,
    /// Transport accepted the request but no acknowledgement arrived.
    Sent,
    /// Broker acknowledged the child.
    Acknowledged,
    /// Some, but not all, quantity filled.
    PartiallyFilled,
    /// Entire child quantity filled.
    Filled,
    /// Cancellation request is in flight.
    CancelPending,
    /// Broker cancelled the child.
    Cancelled,
    /// Broker rejected the child.
    Rejected,
    /// Send or callback outcome is ambiguous; reconciliation is required.
    Unknown,
}

/// Authoritative mutable record for a planned child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildRecord {
    /// Immutable child order details.
    pub order: ChildOrder,
    /// Current lifecycle state.
    pub state: ChildState,
    /// Cumulative positive fill quantity.
    pub filled_quantity_ticks: i64,
    /// Broker order ID once acknowledged.
    pub broker_order_id: Option<String>,
}

/// Durable parent execution plan with idempotent child claims and callbacks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildPlan {
    /// Parent client order ID.
    pub parent_client_order_id: String,
    /// Monotonic creation timestamp.
    pub created_mono_ns: u64,
    /// Child records in deterministic sequence order.
    pub children: Vec<ChildRecord>,
}

impl ChildPlan {
    /// Creates a child state machine from a prevalidated parent intent/schedule.
    ///
    /// # Errors
    /// Returns [`PlanError::InvalidSchedule`] when child identity or quantities
    /// are invalid.
    pub fn new(
        intent: &OrderIntent,
        schedule: &Schedule,
        created_mono_ns: u64,
    ) -> Result<Self, PlanError> {
        let children = plan_children(intent, schedule)?;
        if children.is_empty()
            || children.iter().enumerate().any(|(index, child)| {
                child.parent_client_order_id != intent.client_order_id
                    || child.child_sequence != u32::try_from(index + 1).unwrap_or(u32::MAX)
                    || child.quantity_ticks <= 0
            })
        {
            return Err(PlanError::InvalidSchedule);
        }
        Ok(Self {
            parent_client_order_id: intent.client_order_id.clone(),
            created_mono_ns,
            children: children
                .into_iter()
                .map(|order| ChildRecord {
                    order,
                    state: ChildState::Pending,
                    filled_quantity_ticks: 0,
                    broker_order_id: None,
                })
                .collect(),
        })
    }

    /// Claims every pending child whose due time has arrived. A claimed child
    /// can never be returned by this method again, preventing duplicate sends.
    ///
    /// # Errors
    /// Returns [`PlanError::InvalidSchedule`] if a due timestamp overflows.
    pub fn claim_due(&mut self, now_mono_ns: u64) -> Result<Vec<ChildOrder>, PlanError> {
        let mut claimed = Vec::new();
        for record in &mut self.children {
            let due = self
                .created_mono_ns
                .checked_add(record.order.due_after_ns)
                .ok_or(PlanError::InvalidSchedule)?;
            if record.state == ChildState::Pending && due <= now_mono_ns {
                record.state = ChildState::Sending;
                claimed.push(record.order.clone());
            }
        }
        Ok(claimed)
    }

    /// Converts a child into a broker-neutral intent with a stable child ID.
    #[must_use]
    pub fn child_intent(parent: &OrderIntent, child: &ChildOrder) -> OrderIntent {
        OrderIntent {
            intent_id: format!("{}-child-{}", parent.intent_id, child.child_sequence),
            account_id: parent.account_id,
            instrument_id: parent.instrument_id,
            client_order_id: child.client_order_id.clone(),
            side: child.side,
            quantity_ticks: child.quantity_ticks,
            order_type: child.order_type,
            limit_price_ticks: child.limit_price_ticks,
            time_in_force: parent.time_in_force,
            state: OrderState::RiskApproved,
            trace_id: parent.trace_id,
        }
    }

    /// Records that the broker transport returned success for one claimed
    /// child. It does not imply acknowledgement; callbacks/reconciliation
    /// remain authoritative.
    ///
    /// # Errors
    /// Returns [`PlanError::InvalidChildTransition`] for unknown or duplicate
    /// claims.
    pub fn mark_sent(&mut self, client_order_id: &str) -> Result<(), PlanError> {
        let record = self.record_mut(client_order_id)?;
        if record.state == ChildState::Sending {
            record.state = ChildState::Sent;
            Ok(())
        } else {
            Err(PlanError::InvalidChildTransition)
        }
    }

    /// Marks an ambiguous transport failure and prevents automatic resend.
    ///
    /// # Errors
    /// Returns [`PlanError::InvalidChildTransition`] for unknown or terminal IDs.
    pub fn mark_unknown(&mut self, client_order_id: &str) -> Result<(), PlanError> {
        let record = self.record_mut(client_order_id)?;
        if matches!(record.state, ChildState::Sending | ChildState::Sent) {
            record.state = ChildState::Unknown;
            Ok(())
        } else {
            Err(PlanError::InvalidChildTransition)
        }
    }

    /// Applies one normalized broker callback idempotently.
    ///
    /// # Errors
    /// Returns [`PlanError::InvalidChildTransition`] for unknown IDs, illegal
    /// transitions, non-positive fills, or overfills.
    pub fn apply_broker_event(&mut self, event: &BrokerEvent) -> Result<(), PlanError> {
        let client_id = match event {
            BrokerEvent::Acknowledged {
                client_order_id, ..
            }
            | BrokerEvent::Filled {
                client_order_id, ..
            }
            | BrokerEvent::Rejected {
                client_order_id, ..
            }
            | BrokerEvent::Cancelled { client_order_id } => client_order_id,
        };
        let record = self.record_mut(client_id)?;
        match event {
            BrokerEvent::Acknowledged {
                broker_order_id, ..
            } if matches!(
                record.state,
                ChildState::Sending
                    | ChildState::Sent
                    | ChildState::Acknowledged
                    | ChildState::Unknown
            ) =>
            {
                record.state = ChildState::Acknowledged;
                record.broker_order_id = Some(broker_order_id.clone());
                Ok(())
            }
            BrokerEvent::Filled { quantity_ticks, .. }
                if *quantity_ticks > 0
                    && matches!(
                        record.state,
                        ChildState::Sending
                            | ChildState::Sent
                            | ChildState::Acknowledged
                            | ChildState::PartiallyFilled
                            | ChildState::Filled
                            | ChildState::Unknown
                    ) =>
            {
                if record.state == ChildState::Filled {
                    return Ok(());
                }
                let total = record
                    .filled_quantity_ticks
                    .checked_add(*quantity_ticks)
                    .ok_or(PlanError::InvalidChildTransition)?;
                if total > record.order.quantity_ticks {
                    return Err(PlanError::InvalidChildTransition);
                }
                record.filled_quantity_ticks = total;
                record.state = if total == record.order.quantity_ticks {
                    ChildState::Filled
                } else {
                    ChildState::PartiallyFilled
                };
                Ok(())
            }
            BrokerEvent::Cancelled { .. }
                if matches!(
                    record.state,
                    ChildState::Sending
                        | ChildState::Sent
                        | ChildState::Acknowledged
                        | ChildState::CancelPending
                        | ChildState::Unknown
                ) =>
            {
                record.state = ChildState::Cancelled;
                Ok(())
            }
            BrokerEvent::Rejected { .. }
                if matches!(
                    record.state,
                    ChildState::Sending
                        | ChildState::Sent
                        | ChildState::Acknowledged
                        | ChildState::Unknown
                ) =>
            {
                record.state = ChildState::Rejected;
                Ok(())
            }
            _ => Err(PlanError::InvalidChildTransition),
        }
    }

    /// Requests cancellation for one active child without resending it.
    ///
    /// # Errors
    /// Returns [`PlanError::InvalidChildTransition`] when the child is not active.
    pub fn request_cancel(&mut self, client_order_id: &str) -> Result<(), PlanError> {
        let record = self.record_mut(client_order_id)?;
        if matches!(
            record.state,
            ChildState::Sent | ChildState::Acknowledged | ChildState::PartiallyFilled
        ) {
            record.state = ChildState::CancelPending;
            Ok(())
        } else {
            Err(PlanError::InvalidChildTransition)
        }
    }

    /// Returns whether all child quantity has reached a terminal outcome.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.children.iter().all(|record| {
            matches!(
                record.state,
                ChildState::Filled | ChildState::Cancelled | ChildState::Rejected
            )
        })
    }

    fn record_mut(&mut self, client_order_id: &str) -> Result<&mut ChildRecord, PlanError> {
        self.children
            .iter_mut()
            .find(|record| record.order.client_order_id == client_order_id)
            .ok_or(PlanError::InvalidChildTransition)
    }
}

/// Builds child orders without side effects.
///
/// # Errors
/// Returns [`PlanError::InvalidSchedule`] for zero slices, invalid weights,
/// quantity overflow, or a schedule that would emit zero-sized children.
#[allow(clippy::too_many_lines)]
pub fn plan_children(
    intent: &OrderIntent,
    schedule: &Schedule,
) -> Result<Vec<ChildOrder>, PlanError> {
    if intent.quantity_ticks <= 0 {
        return Err(PlanError::InvalidSchedule);
    }
    let (quantities, interval_ns) = match schedule {
        Schedule::Immediate => (vec![intent.quantity_ticks], 0_u64),
        Schedule::Twap {
            slices,
            interval_ns,
        } => {
            let maximum_slices =
                usize::try_from(intent.quantity_ticks).map_err(|_| PlanError::InvalidSchedule)?;
            if *slices == 0 || *interval_ns == 0 || *slices > maximum_slices {
                return Err(PlanError::InvalidSchedule);
            }
            let slices_i64 = i64::try_from(*slices).map_err(|_| PlanError::InvalidSchedule)?;
            let base = intent.quantity_ticks / slices_i64;
            let remainder = intent.quantity_ticks % slices_i64;
            let remainder_usize =
                usize::try_from(remainder).map_err(|_| PlanError::InvalidSchedule)?;
            let values = (0..*slices)
                .map(|index| base + i64::from(index < remainder_usize))
                .collect::<Vec<_>>();
            (values, *interval_ns)
        }
        Schedule::Vwap { weights } => {
            if weights.is_empty() || weights.contains(&0) {
                return Err(PlanError::InvalidSchedule);
            }
            let total_weight = weights
                .iter()
                .try_fold(0_u64, |total, weight| total.checked_add(u64::from(*weight)))
                .ok_or(PlanError::InvalidSchedule)?;
            let total_quantity =
                u128::try_from(intent.quantity_ticks).map_err(|_| PlanError::InvalidSchedule)?;
            let mut quantities = weights
                .iter()
                .map(|weight| {
                    i64::try_from((total_quantity * u128::from(*weight)) / u128::from(total_weight))
                        .map_err(|_| PlanError::InvalidSchedule)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let assigned = quantities.iter().sum::<i64>();
            let mut remainder = intent.quantity_ticks - assigned;
            for quantity in &mut quantities {
                if remainder == 0 {
                    break;
                }
                *quantity += 1;
                remainder -= 1;
            }
            if remainder != 0 || quantities.iter().any(|quantity| *quantity <= 0) {
                return Err(PlanError::InvalidSchedule);
            }
            (quantities, 0)
        }
        Schedule::Pov {
            participation_bps,
            interval_ns,
            market_volume_ticks,
        } => {
            if *participation_bps == 0
                || *participation_bps > 10_000
                || *interval_ns == 0
                || market_volume_ticks.is_empty()
                || market_volume_ticks.iter().any(|volume| *volume <= 0)
            {
                return Err(PlanError::InvalidSchedule);
            }
            let mut remaining = intent.quantity_ticks;
            let mut quantities = Vec::new();
            for volume in market_volume_ticks {
                if remaining == 0 {
                    break;
                }
                let capacity = i64::try_from(
                    (u128::try_from(*volume).map_err(|_| PlanError::InvalidSchedule)?
                        * u128::from(*participation_bps))
                        / 10_000,
                )
                .map_err(|_| PlanError::InvalidSchedule)?;
                if capacity <= 0 {
                    continue;
                }
                let quantity = capacity.min(remaining);
                quantities.push(quantity);
                remaining -= quantity;
            }
            if remaining != 0 {
                return Err(PlanError::InvalidSchedule);
            }
            (quantities, *interval_ns)
        }
        Schedule::ImplementationShortfall {
            slices,
            interval_ns,
            urgency_bps,
        } => {
            let maximum_slices =
                usize::try_from(intent.quantity_ticks).map_err(|_| PlanError::InvalidSchedule)?;
            if *slices == 0
                || *slices > maximum_slices
                || *interval_ns == 0
                || *urgency_bps > 10_000
            {
                return Err(PlanError::InvalidSchedule);
            }
            let slices_u128 = u128::try_from(*slices).map_err(|_| PlanError::InvalidSchedule)?;
            let urgency = u128::from(*urgency_bps);
            let mut weights = Vec::with_capacity(*slices);
            for index in 0..*slices {
                let remaining =
                    u128::try_from(*slices - index - 1).map_err(|_| PlanError::InvalidSchedule)?;
                weights.push(10_000_u128 + urgency * remaining / slices_u128.max(1));
            }
            let total_weight = weights
                .iter()
                .try_fold(0_u128, |sum, weight| sum.checked_add(*weight))
                .ok_or(PlanError::InvalidSchedule)?;
            let quantity =
                u128::try_from(intent.quantity_ticks).map_err(|_| PlanError::InvalidSchedule)?;
            let mut quantities = weights
                .iter()
                .map(|weight| {
                    i64::try_from(quantity * *weight / total_weight)
                        .map_err(|_| PlanError::InvalidSchedule)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let assigned = quantities.iter().sum::<i64>();
            let mut remainder = intent.quantity_ticks - assigned;
            for child in &mut quantities {
                if remainder == 0 {
                    break;
                }
                *child += 1;
                remainder -= 1;
            }
            if remainder != 0 || quantities.iter().any(|child| *child <= 0) {
                return Err(PlanError::InvalidSchedule);
            }
            (quantities, *interval_ns)
        }
        Schedule::Adaptive {
            slices,
            interval_ns,
            urgency_bps,
            spread_ticks,
            max_spread_ticks,
            volatility_bps,
            max_volatility_bps,
            market_volume_ticks,
        } => {
            if *slices == 0
                || *interval_ns == 0
                || *urgency_bps == 0
                || *urgency_bps > 10_000
                || *spread_ticks < 0
                || *max_spread_ticks <= 0
                || *volatility_bps > 100_000
                || *max_volatility_bps == 0
                || market_volume_ticks.is_empty()
                || market_volume_ticks.len() > *slices
            {
                return Err(PlanError::InvalidSchedule);
            }
            let selected =
                if *spread_ticks <= *max_spread_ticks && *volatility_bps <= *max_volatility_bps {
                    Schedule::Pov {
                        participation_bps: *urgency_bps,
                        interval_ns: *interval_ns,
                        market_volume_ticks: market_volume_ticks.clone(),
                    }
                } else {
                    Schedule::ImplementationShortfall {
                        slices: (*slices).min(
                            usize::try_from(intent.quantity_ticks)
                                .map_err(|_| PlanError::InvalidSchedule)?,
                        ),
                        interval_ns: *interval_ns,
                        urgency_bps: (*urgency_bps).max(7_500),
                    }
                };
            return plan_children(intent, &selected);
        }
    };
    quantities
        .into_iter()
        .enumerate()
        .map(|(index, quantity)| {
            let child_sequence =
                u32::try_from(index + 1).map_err(|_| PlanError::InvalidSchedule)?;
            let due_after_ns = interval_ns
                .checked_mul(u64::from(child_sequence.saturating_sub(1)))
                .ok_or(PlanError::InvalidSchedule)?;
            Ok(ChildOrder {
                parent_client_order_id: intent.client_order_id.clone(),
                child_sequence,
                client_order_id: format!("{}-child-{child_sequence}", intent.client_order_id),
                quantity_ticks: quantity,
                due_after_ns,
                side: intent.side,
                order_type: intent.order_type,
                limit_price_ticks: intent.limit_price_ticks,
            })
        })
        .collect()
}

/// One canonical fill used for transaction-cost analysis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FillSample {
    /// Filled quantity in positive ticks.
    pub quantity_ticks: i64,
    /// Execution price in positive ticks.
    pub price_ticks: i64,
}

/// One fill observation with the reference data required for detailed TCA.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcaObservation {
    /// Decision/reference price captured before routing.
    pub decision_price_ticks: i64,
    /// Arrival price at the order boundary.
    pub arrival_price_ticks: i64,
    /// Midpoint at broker send.
    pub send_mid_ticks: i64,
    /// Midpoint at broker acknowledgement.
    pub acknowledgement_mid_ticks: i64,
    /// Fill quantity and price.
    pub fill: FillSample,
    /// Monotonic decision timestamp.
    pub decision_mono_ns: u64,
    /// Monotonic send timestamp.
    pub send_mono_ns: u64,
    /// Monotonic acknowledgement timestamp.
    pub acknowledgement_mono_ns: u64,
    /// Monotonic fill timestamp.
    pub fill_mono_ns: u64,
    /// Quoted spread at arrival, in price ticks.
    pub spread_ticks: i64,
    /// Market volume during the measurement window.
    pub market_volume_ticks: i64,
    /// Midpoint after the fill window for adverse-selection measurement.
    pub post_fill_mid_ticks: i64,
}

/// TCA calculation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TcaError {
    /// Arrival price or fill values are invalid.
    InvalidValue,
    /// No positive quantity was filled.
    NoFills,
    /// Integer arithmetic overflowed.
    Overflow,
    /// Event timestamps are not monotonic.
    InvalidTiming,
    /// A detailed measurement reference value is missing or invalid.
    MissingReference,
}

/// Deterministic execution-quality report.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TcaReport {
    /// Requested order quantity.
    pub requested_quantity_ticks: i64,
    /// Quantity filled.
    pub filled_quantity_ticks: i64,
    /// Filled quantity divided by requested quantity.
    pub fill_ratio: f64,
    /// Volume-weighted average execution price.
    pub vwap_ticks: i64,
    /// Signed slippage in price ticks; positive means worse than arrival.
    pub slippage_ticks: i64,
    /// Signed slippage in basis points; positive means worse than arrival.
    pub slippage_bps: f64,
}

impl TcaReport {
    /// Computes TCA against the order's arrival price.
    ///
    /// # Errors
    /// Returns [`TcaError`] for invalid values, empty fills, or overflow.
    #[allow(clippy::cast_precision_loss)]
    pub fn calculate(
        intent: &OrderIntent,
        arrival_price_ticks: i64,
        fills: &[FillSample],
    ) -> Result<Self, TcaError> {
        if intent.quantity_ticks <= 0 || arrival_price_ticks <= 0 {
            return Err(TcaError::InvalidValue);
        }
        let mut quantity = 0_i64;
        let mut notional = 0_i128;
        for fill in fills {
            if fill.quantity_ticks <= 0 || fill.price_ticks <= 0 {
                return Err(TcaError::InvalidValue);
            }
            quantity = quantity
                .checked_add(fill.quantity_ticks)
                .ok_or(TcaError::Overflow)?;
            notional = notional
                .checked_add(i128::from(fill.quantity_ticks) * i128::from(fill.price_ticks))
                .ok_or(TcaError::Overflow)?;
        }
        if quantity <= 0 {
            return Err(TcaError::NoFills);
        }
        let vwap =
            i64::try_from(notional / i128::from(quantity)).map_err(|_| TcaError::Overflow)?;
        let slippage = match intent.side {
            Side::Buy => vwap.checked_sub(arrival_price_ticks),
            Side::Sell => arrival_price_ticks.checked_sub(vwap),
        }
        .ok_or(TcaError::Overflow)?;
        let slippage_bps = (slippage as f64 / arrival_price_ticks as f64) * 10_000.0;
        Ok(Self {
            requested_quantity_ticks: intent.quantity_ticks,
            filled_quantity_ticks: quantity,
            fill_ratio: (quantity as f64 / intent.quantity_ticks as f64).min(1.0),
            vwap_ticks: vwap,
            slippage_ticks: slippage,
            slippage_bps,
        })
    }
}

/// Detailed execution-quality report required for strategy/TCA feedback.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DetailedTcaReport {
    /// Basic fill/VWAP report for compatibility with existing consumers.
    pub basic: TcaReport,
    /// Decision-to-send latency across observed fills.
    pub decision_to_send_ns: u64,
    /// Send-to-acknowledgement latency across observed fills.
    pub send_to_acknowledgement_ns: u64,
    /// Acknowledgement-to-fill latency across observed fills.
    pub acknowledgement_to_fill_ns: u64,
    /// Quantity-weighted quoted spread in price ticks.
    pub average_spread_ticks: f64,
    /// Filled quantity as basis points of observed market volume.
    pub participation_bps: f64,
    /// Signed post-fill adverse selection; positive means the market moved
    /// against the order side after execution.
    pub adverse_selection_ticks: f64,
    /// Decision-price implementation shortfall in ticks.
    pub implementation_shortfall_ticks: i64,
}

impl DetailedTcaReport {
    /// Computes detailed TCA from immutable observations and broker intent.
    ///
    /// Every observation must carry a complete monotonic timing chain and
    /// positive reference prices. The function rejects overfills and missing
    /// market-volume/post-fill references instead of fabricating metrics.
    ///
    /// # Errors
    /// Returns [`TcaError`] when an observation is invalid, incomplete, or
    /// arithmetic exceeds the canonical range.
    #[allow(clippy::cast_precision_loss)]
    pub fn calculate(
        intent: &OrderIntent,
        observations: &[TcaObservation],
    ) -> Result<Self, TcaError> {
        if intent.quantity_ticks <= 0 || observations.is_empty() {
            return Err(TcaError::NoFills);
        }
        let mut fills = Vec::with_capacity(observations.len());
        let mut total_quantity = 0_i64;
        let mut spread_notional = 0_i128;
        let mut market_volume = 0_i64;
        let mut adverse_notional = 0_i128;
        let mut first_decision = u64::MAX;
        let mut first_send = u64::MAX;
        let mut first_ack = u64::MAX;
        let mut last_send = 0_u64;
        let mut last_ack = 0_u64;
        let mut last_fill = 0_u64;
        let mut decision_notional = 0_i128;
        for observation in observations {
            let fill = observation.fill;
            if observation.decision_price_ticks <= 0
                || observation.arrival_price_ticks <= 0
                || observation.send_mid_ticks <= 0
                || observation.acknowledgement_mid_ticks <= 0
                || observation.post_fill_mid_ticks <= 0
                || fill.quantity_ticks <= 0
                || fill.price_ticks <= 0
                || observation.spread_ticks < 0
                || observation.market_volume_ticks <= 0
            {
                return Err(TcaError::MissingReference);
            }
            if observation.decision_mono_ns > observation.send_mono_ns
                || observation.send_mono_ns > observation.acknowledgement_mono_ns
                || observation.acknowledgement_mono_ns > observation.fill_mono_ns
            {
                return Err(TcaError::InvalidTiming);
            }
            total_quantity = total_quantity
                .checked_add(fill.quantity_ticks)
                .ok_or(TcaError::Overflow)?;
            market_volume = market_volume
                .checked_add(observation.market_volume_ticks)
                .ok_or(TcaError::Overflow)?;
            spread_notional = spread_notional
                .checked_add(i128::from(fill.quantity_ticks) * i128::from(observation.spread_ticks))
                .ok_or(TcaError::Overflow)?;
            let adverse = match intent.side {
                Side::Buy => observation.post_fill_mid_ticks - fill.price_ticks,
                Side::Sell => fill.price_ticks - observation.post_fill_mid_ticks,
            };
            adverse_notional = adverse_notional
                .checked_add(i128::from(fill.quantity_ticks) * i128::from(adverse))
                .ok_or(TcaError::Overflow)?;
            decision_notional = decision_notional
                .checked_add(
                    i128::from(fill.quantity_ticks) * i128::from(observation.decision_price_ticks),
                )
                .ok_or(TcaError::Overflow)?;
            first_decision = first_decision.min(observation.decision_mono_ns);
            first_send = first_send.min(observation.send_mono_ns);
            first_ack = first_ack.min(observation.acknowledgement_mono_ns);
            last_send = last_send.max(observation.send_mono_ns);
            last_ack = last_ack.max(observation.acknowledgement_mono_ns);
            last_fill = last_fill.max(observation.fill_mono_ns);
            fills.push(fill);
        }
        if total_quantity > intent.quantity_ticks {
            return Err(TcaError::InvalidValue);
        }
        let basic = TcaReport::calculate(intent, observations[0].arrival_price_ticks, &fills)?;
        let average_spread_ticks = spread_notional as f64 / total_quantity as f64;
        let participation_bps = total_quantity as f64 / market_volume as f64 * 10_000.0;
        let adverse_selection_ticks = adverse_notional as f64 / total_quantity as f64;
        let decision_vwap = decision_notional / i128::from(total_quantity);
        let implementation = match intent.side {
            Side::Buy => i128::from(basic.vwap_ticks).checked_sub(decision_vwap),
            Side::Sell => decision_vwap.checked_sub(i128::from(basic.vwap_ticks)),
        }
        .ok_or(TcaError::Overflow)?;
        Ok(Self {
            basic,
            decision_to_send_ns: first_send.saturating_sub(first_decision),
            send_to_acknowledgement_ns: last_ack.saturating_sub(first_send),
            acknowledgement_to_fill_ns: last_fill.saturating_sub(first_ack),
            average_spread_ticks,
            participation_bps,
            adverse_selection_ticks,
            implementation_shortfall_ticks: i64::try_from(implementation)
                .map_err(|_| TcaError::Overflow)?,
        })
    }
}

/// Durable order-state transition failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionError {
    /// Client order ID is not known locally.
    UnknownOrder,
    /// The requested lifecycle transition is not legal.
    InvalidTransition {
        /// Existing local state.
        from: OrderState,
        /// Requested next state.
        to: OrderState,
    },
    /// Broker event belongs to a different client order.
    MismatchedOrder,
}

/// Failure while submitting a persisted order intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmitError {
    /// The local order could not enter the send lifecycle.
    Transition(TransitionError),
    /// The adapter failed after the intent was persisted; reconciliation is required.
    Gateway(String),
}

/// Failure while requesting cancellation of a working order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CancelError {
    /// The local lifecycle could not enter `CancelPending` or `Unknown`.
    Transition(TransitionError),
    /// The broker does not advertise cancellation support.
    Unsupported,
    /// Transport failure; reconciliation is required.
    Gateway(String),
}

/// Failure while requesting replacement of a working order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplaceError {
    /// Local order lifecycle rejected replacement.
    Transition(TransitionError),
    /// Broker does not advertise cancel/replace support.
    Unsupported,
    /// Replacement transport is ambiguous and requires reconciliation.
    Gateway(String),
    /// Replacement quantity is invalid or below the already filled amount.
    InvalidQuantity,
}

/// Locally authoritative order record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderRecord {
    /// Durable intent.
    pub intent: OrderIntent,
    /// Broker order ID after acknowledgement.
    pub broker_order_id: Option<String>,
    /// Filled quantity accumulated from broker events.
    pub filled_quantity_ticks: i64,
}

/// In-memory projection of the durable order journal.
#[derive(Default)]
pub struct OrderBook {
    orders: std::collections::BTreeMap<String, OrderRecord>,
}

impl OrderBook {
    /// Creates an empty order projection.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Persists a newly risk-approved intent before sending to a broker.
    ///
    /// # Errors
    /// Returns [`TransitionError::InvalidTransition`] when the intent is not
    /// in `RiskApproved` state or [`TransitionError::MismatchedOrder`] for a duplicate ID.
    pub fn insert(&mut self, intent: OrderIntent) -> Result<(), TransitionError> {
        if intent.state != OrderState::RiskApproved {
            return Err(TransitionError::InvalidTransition {
                from: intent.state,
                to: OrderState::RiskApproved,
            });
        }
        if self.orders.contains_key(&intent.client_order_id) {
            return Err(TransitionError::MismatchedOrder);
        }
        self.orders.insert(
            intent.client_order_id.clone(),
            OrderRecord {
                intent,
                broker_order_id: None,
                filled_quantity_ticks: 0,
            },
        );
        Ok(())
    }

    /// Advances a local order state after a durable event.
    ///
    /// # Errors
    /// Returns [`TransitionError`] when the order is unknown or the transition
    /// is not legal.
    pub fn transition(
        &mut self,
        client_order_id: &str,
        next: OrderState,
    ) -> Result<(), TransitionError> {
        let Some(record) = self.orders.get_mut(client_order_id) else {
            return Err(TransitionError::UnknownOrder);
        };
        if !legal_transition(record.intent.state, next) {
            return Err(TransitionError::InvalidTransition {
                from: record.intent.state,
                to: next,
            });
        }
        record.intent.state = next;
        Ok(())
    }

    /// Marks an uncertain send. Unknown orders cannot be retried blindly.
    ///
    /// # Errors
    /// Returns [`TransitionError`] when the client ID is unknown or cannot
    /// legally enter `Unknown` from its current state.
    pub fn mark_unknown(&mut self, client_order_id: &str) -> Result<(), TransitionError> {
        self.transition(client_order_id, OrderState::Unknown)
    }

    /// Applies an authoritative broker event and updates local state.
    ///
    /// # Errors
    /// Returns [`TransitionError`] when the event references an unknown or
    /// mismatched order, or the implied transition is illegal.
    pub fn apply_broker_event(&mut self, event: BrokerEvent) -> Result<(), TransitionError> {
        let client_order_id = match &event {
            BrokerEvent::Acknowledged {
                client_order_id, ..
            }
            | BrokerEvent::Filled {
                client_order_id, ..
            }
            | BrokerEvent::Rejected {
                client_order_id, ..
            }
            | BrokerEvent::Cancelled { client_order_id } => client_order_id,
        };
        let Some(record) = self.orders.get_mut(client_order_id) else {
            return Err(TransitionError::UnknownOrder);
        };
        match event {
            BrokerEvent::Acknowledged {
                broker_order_id, ..
            } => {
                if matches!(
                    record.intent.state,
                    OrderState::Acknowledged
                        | OrderState::PartiallyFilled
                        | OrderState::Filled
                        | OrderState::CancelPending
                        | OrderState::Cancelled
                        | OrderState::Rejected
                ) {
                    return if record.broker_order_id.as_deref() == Some(&broker_order_id) {
                        Ok(())
                    } else {
                        Err(TransitionError::MismatchedOrder)
                    };
                }
                record.broker_order_id = Some(broker_order_id);
                transition_record(record, OrderState::Acknowledged)
            }
            BrokerEvent::Filled {
                quantity_ticks,
                price_ticks,
                ..
            } => {
                if quantity_ticks <= 0 || price_ticks <= 0 {
                    return Err(TransitionError::MismatchedOrder);
                }
                if record.intent.state == OrderState::Filled {
                    return Ok(());
                }
                let next_filled = record
                    .filled_quantity_ticks
                    .checked_add(quantity_ticks)
                    .ok_or(TransitionError::MismatchedOrder)?;
                if next_filled > record.intent.quantity_ticks {
                    return Err(TransitionError::MismatchedOrder);
                }
                record.filled_quantity_ticks = next_filled;
                transition_record(
                    record,
                    if record.filled_quantity_ticks >= record.intent.quantity_ticks {
                        OrderState::Filled
                    } else {
                        OrderState::PartiallyFilled
                    },
                )
            }
            BrokerEvent::Rejected { .. } => {
                if record.intent.state == OrderState::Rejected {
                    Ok(())
                } else {
                    transition_record(record, OrderState::Rejected)
                }
            }
            BrokerEvent::Cancelled { .. } => {
                if record.intent.state == OrderState::Cancelled {
                    Ok(())
                } else {
                    transition_record(record, OrderState::Cancelled)
                }
            }
        }
    }

    /// Applies a broker-reported cumulative fill count without inventing a
    /// fill price. This is used by account snapshots, where the broker may
    /// report total filled quantity separately from its latest event.
    ///
    /// # Errors
    /// Returns [`TransitionError`] for an unknown order, decreasing/overfilled
    /// cumulative quantity, or an illegal lifecycle transition.
    pub fn apply_cumulative_fill(
        &mut self,
        client_order_id: &str,
        cumulative_quantity_ticks: i64,
    ) -> Result<(), TransitionError> {
        let Some(record) = self.orders.get_mut(client_order_id) else {
            return Err(TransitionError::UnknownOrder);
        };
        if cumulative_quantity_ticks < record.filled_quantity_ticks
            || cumulative_quantity_ticks > record.intent.quantity_ticks
        {
            return Err(TransitionError::MismatchedOrder);
        }
        if cumulative_quantity_ticks == record.filled_quantity_ticks {
            return Ok(());
        }
        let next_state = if cumulative_quantity_ticks >= record.intent.quantity_ticks {
            OrderState::Filled
        } else {
            OrderState::PartiallyFilled
        };
        transition_record(record, next_state)?;
        record.filled_quantity_ticks = cumulative_quantity_ticks;
        Ok(())
    }

    /// Returns an order record by stable client ID.
    #[must_use]
    pub fn get(&self, client_order_id: &str) -> Option<&OrderRecord> {
        self.orders.get(client_order_id)
    }

    /// Returns all client order IDs in stable order for reconciliation sweeps.
    pub fn client_order_ids(&self) -> impl Iterator<Item = &str> {
        self.orders.keys().map(String::as_str)
    }

    /// Returns a stable immutable snapshot for read-model/IPC consumers.
    #[must_use]
    pub fn records(&self) -> Vec<OrderRecord> {
        self.orders.values().cloned().collect()
    }

    /// Marks an order replace-pending and records the requested fields locally.
    ///
    /// # Errors
    /// Returns [`ReplaceError`] when the order is not working or the quantity
    /// would be below the already-filled amount.
    pub fn prepare_replace(
        &mut self,
        client_order_id: &str,
        quantity_ticks: i64,
        limit_price_ticks: Option<i64>,
    ) -> Result<(), ReplaceError> {
        let Some(record) = self.orders.get_mut(client_order_id) else {
            return Err(ReplaceError::Transition(TransitionError::UnknownOrder));
        };
        if quantity_ticks <= 0 || quantity_ticks < record.filled_quantity_ticks {
            return Err(ReplaceError::InvalidQuantity);
        }
        if !matches!(
            record.intent.state,
            OrderState::Acknowledged | OrderState::PartiallyFilled
        ) {
            return Err(ReplaceError::Transition(
                TransitionError::InvalidTransition {
                    from: record.intent.state,
                    to: OrderState::ReplacePending,
                },
            ));
        }
        record.intent.quantity_ticks = quantity_ticks;
        record.intent.limit_price_ticks = limit_price_ticks;
        record.intent.state = OrderState::ReplacePending;
        Ok(())
    }
}

/// Persists an intent before sending it and makes transport ambiguity explicit.
///
/// A successful adapter call leaves the order in `Sent`. Any adapter error
/// leaves it in `Unknown`, because the broker may have accepted the order
/// before the transport failure was observed. Call reconciliation before any
/// retry.
///
/// # Errors
/// Returns [`SubmitError::Transition`] for an invalid local lifecycle and
/// [`SubmitError::Gateway`] for an adapter failure after marking the order
/// `Unknown`.
pub fn submit_order(
    book: &mut OrderBook,
    gateway: &dyn BrokerGateway,
    intent: &OrderIntent,
) -> Result<(), SubmitError> {
    let client_order_id = intent.client_order_id.clone();
    book.insert(intent.clone())
        .map_err(SubmitError::Transition)?;
    for state in [OrderState::Queued, OrderState::Sending] {
        if let Err(error) = book.transition(&client_order_id, state) {
            return Err(SubmitError::Transition(error));
        }
    }
    if let Err(error) = gateway.send(intent) {
        let transition = book.mark_unknown(&client_order_id);
        if let Err(transition_error) = transition {
            return Err(SubmitError::Transition(transition_error));
        }
        return Err(SubmitError::Gateway(error));
    }
    book.transition(&client_order_id, OrderState::Sent)
        .map_err(SubmitError::Transition)
}

/// Persists a cancel-pending transition before requesting broker cancellation.
///
/// A successful transport call leaves the order in `CancelPending` until an
/// authoritative `Cancelled` or `Filled` event arrives. A transport failure
/// moves it to `Unknown`; callers must reconcile before retrying.
///
/// # Errors
/// Returns [`CancelError`] when cancellation is unsupported, the lifecycle is
/// invalid, or the broker call is ambiguous.
pub fn cancel_order(
    book: &mut OrderBook,
    gateway: &dyn BrokerGateway,
    client_order_id: &str,
) -> Result<(), CancelError> {
    if !gateway.capabilities().cancel_replace {
        return Err(CancelError::Unsupported);
    }
    book.transition(client_order_id, OrderState::CancelPending)
        .map_err(CancelError::Transition)?;
    if let Err(error) = gateway.cancel(client_order_id) {
        book.transition(client_order_id, OrderState::Unknown)
            .map_err(CancelError::Transition)?;
        return Err(CancelError::Gateway(error));
    }
    Ok(())
}

/// Persists replacement intent before requesting broker cancel/replace.
///
/// A successful transport call leaves the order in `ReplacePending` until an
/// authoritative broker acknowledgement/fill arrives. Transport failure moves
/// it to `Unknown` and forbids blind retry.
///
/// # Errors
/// Returns [`ReplaceError`] when validation, lifecycle, capability, or broker
/// transport rejects the request.
pub fn replace_order(
    book: &mut OrderBook,
    gateway: &dyn BrokerGateway,
    client_order_id: &str,
    quantity_ticks: i64,
    limit_price_ticks: Option<i64>,
) -> Result<(), ReplaceError> {
    if !gateway.capabilities().cancel_replace {
        return Err(ReplaceError::Unsupported);
    }
    book.prepare_replace(client_order_id, quantity_ticks, limit_price_ticks)?;
    if let Err(error) = gateway.replace(client_order_id, quantity_ticks, limit_price_ticks) {
        book.transition(client_order_id, OrderState::Unknown)
            .map_err(ReplaceError::Transition)?;
        return Err(ReplaceError::Gateway(error));
    }
    Ok(())
}

fn transition_record(record: &mut OrderRecord, next: OrderState) -> Result<(), TransitionError> {
    if !legal_transition(record.intent.state, next) {
        return Err(TransitionError::InvalidTransition {
            from: record.intent.state,
            to: next,
        });
    }
    record.intent.state = next;
    Ok(())
}

fn legal_transition(from: OrderState, to: OrderState) -> bool {
    // Lifecycle transport transitions are normally applied in memory after
    // the intent is journaled. On restart, the first durable broker event may
    // be the only persisted evidence of acknowledgement/fill/rejection, so
    // authoritative broker events must be able to advance directly from the
    // persisted RiskApproved intent.
    matches!(
        (from, to),
        (
            OrderState::RiskApproved,
            OrderState::Queued
                | OrderState::Sending
                | OrderState::Acknowledged
                | OrderState::PartiallyFilled
                | OrderState::Filled
                | OrderState::Rejected
                | OrderState::Cancelled
                | OrderState::Unknown
        ) | (
            OrderState::Queued,
            OrderState::Sending | OrderState::Unknown
        ) | (OrderState::Sending, OrderState::Sent | OrderState::Unknown)
            | (
                OrderState::Sent | OrderState::ReplacePending,
                OrderState::Acknowledged
                    | OrderState::PartiallyFilled
                    | OrderState::Filled
                    | OrderState::Rejected
                    | OrderState::Unknown
            )
            | (
                OrderState::Acknowledged,
                OrderState::PartiallyFilled
                    | OrderState::Filled
                    | OrderState::CancelPending
                    | OrderState::Rejected
                    | OrderState::Unknown
            )
            | (
                OrderState::PartiallyFilled,
                OrderState::PartiallyFilled
                    | OrderState::Filled
                    | OrderState::CancelPending
                    | OrderState::Unknown
            )
            | (
                OrderState::CancelPending,
                OrderState::Cancelled | OrderState::Filled | OrderState::Unknown
            )
            | (
                OrderState::Unknown,
                OrderState::Acknowledged
                    | OrderState::Rejected
                    | OrderState::PartiallyFilled
                    | OrderState::Filled
                    | OrderState::Cancelled
            )
    )
}

/// Builds a broker-neutral order intent from a target after risk approval.
///
/// # Errors
/// Returns [`PlanError`] when no order is required, risk denies/resizes to no
/// delta, or the requested broker capabilities are unavailable.
pub fn plan_target(
    account_id: AccountId,
    trace_id: TraceId,
    portfolio: &Portfolio,
    target: &Target,
    risk: &RiskEngine,
    capabilities: Capabilities,
) -> Result<OrderIntent, PlanError> {
    plan_target_with_guardrails(
        account_id,
        trace_id,
        portfolio,
        target,
        risk,
        capabilities,
        None,
    )
}

/// Builds an order intent using the configured contextual risk guardrails.
/// `inputs` must be derived from authoritative runtime observations by the
/// caller; the evaluator fails closed for unhealthy or incomplete inputs.
///
/// # Errors
/// Returns [`PlanError`] when risk denies the target, no delta exists, or the
/// broker capabilities cannot represent the resulting order.
pub fn plan_target_with_guardrails(
    account_id: AccountId,
    trace_id: TraceId,
    portfolio: &Portfolio,
    target: &Target,
    risk: &RiskEngine,
    capabilities: Capabilities,
    inputs: Option<RiskInputs>,
) -> Result<OrderIntent, PlanError> {
    let current = portfolio
        .position(target.instrument_id)
        .map_or(0, |position| position.quantity_ticks);
    let decision = match inputs {
        Some(inputs) => risk.check_with_guardrails(portfolio, target, risk.guardrails(), inputs),
        None => risk.check(portfolio, target),
    };
    let quantity = match decision {
        Decision::Allow => target.quantity_ticks,
        Decision::Resize { quantity_ticks } => quantity_ticks,
        Decision::Deny(reason) => return Err(PlanError::RiskDenied(reason)),
    };
    let delta = quantity.checked_sub(current).ok_or(PlanError::NoDelta)?;
    if delta == 0 {
        return Err(PlanError::NoDelta);
    }
    let absolute = delta.checked_abs().ok_or(PlanError::NoDelta)?;
    if !capabilities.market || absolute <= 0 {
        return Err(PlanError::UnsupportedOrder);
    }
    let side = if delta > 0 { Side::Buy } else { Side::Sell };
    let intent_id = format!("intent-{}-{}", account_id, target.proposal_id);
    let client_order_id = format!("client-{intent_id}");
    Ok(OrderIntent {
        intent_id,
        account_id,
        instrument_id: target.instrument_id,
        client_order_id,
        side,
        quantity_ticks: absolute,
        order_type: OrderType::Market,
        limit_price_ticks: None,
        time_in_force: TimeInForce::Day,
        state: OrderState::RiskApproved,
        trace_id,
    })
}

#[cfg(test)]
mod tests {
    use insider_broker_api::{BrokerEvent, OrderState, OrderType, Side, TimeInForce};
    use insider_common_types::{AccountId, InstrumentId, TraceId};

    use super::{
        DetailedTcaReport, FillSample, OrderBook, PlanError, Schedule, TcaFill, TcaInput,
        TcaObservation, TcaReport, TcaResult, calculate_tca, plan_children,
    };

    #[test]
    fn tca_reports_buy_slippage_and_partial_fill() {
        let Some(account_id) = AccountId::new(1).ok() else {
            return;
        };
        let Some(instrument_id) = InstrumentId::new(2).ok() else {
            return;
        };
        let Some(trace_id) = TraceId::new(3).ok() else {
            return;
        };
        let intent = insider_broker_api::OrderIntent {
            intent_id: "intent".into(),
            account_id,
            instrument_id,
            client_order_id: "client".into(),
            side: insider_broker_api::Side::Buy,
            quantity_ticks: 10,
            order_type: OrderType::Market,
            limit_price_ticks: None,
            time_in_force: TimeInForce::Day,
            state: insider_broker_api::OrderState::Sent,
            trace_id,
        };
        let report = TcaReport::calculate(
            &intent,
            100,
            &[FillSample {
                quantity_ticks: 5,
                price_ticks: 101,
            }],
        );
        assert!(report.is_ok_and(|report| report.vwap_ticks == 101
            && report.slippage_ticks == 1
            && (report.fill_ratio - 0.5).abs() < f64::EPSILON));
    }

    #[test]
    fn broker_fill_requires_positive_execution_price() {
        let Some(account_id) = AccountId::new(1).ok() else {
            return;
        };
        let Some(instrument_id) = InstrumentId::new(2).ok() else {
            return;
        };
        let Some(trace_id) = TraceId::new(3).ok() else {
            return;
        };
        let intent = insider_broker_api::OrderIntent {
            intent_id: "intent".into(),
            account_id,
            instrument_id,
            client_order_id: "client".into(),
            side: Side::Buy,
            quantity_ticks: 10,
            order_type: OrderType::Market,
            limit_price_ticks: None,
            time_in_force: TimeInForce::Day,
            state: OrderState::RiskApproved,
            trace_id,
        };
        let mut book = OrderBook::new();
        assert!(book.insert(intent).is_ok());
        for state in [OrderState::Queued, OrderState::Sending, OrderState::Sent] {
            assert!(book.transition("client", state).is_ok());
        }
        assert!(
            book.apply_broker_event(BrokerEvent::Filled {
                client_order_id: "client".into(),
                quantity_ticks: 5,
                price_ticks: 0,
            })
            .is_err()
        );
        assert_eq!(
            book.get("client")
                .map(|record| record.filled_quantity_ticks),
            Some(0)
        );
    }

    #[test]
    fn detailed_tca_reports_latency_participation_and_adverse_selection() {
        let Some(account_id) = AccountId::new(1).ok() else {
            return;
        };
        let Some(instrument_id) = InstrumentId::new(2).ok() else {
            return;
        };
        let Some(trace_id) = TraceId::new(3).ok() else {
            return;
        };
        let intent = insider_broker_api::OrderIntent {
            intent_id: "intent".into(),
            account_id,
            instrument_id,
            client_order_id: "client".into(),
            side: insider_broker_api::Side::Buy,
            quantity_ticks: 10,
            order_type: OrderType::Market,
            limit_price_ticks: None,
            time_in_force: TimeInForce::Day,
            state: insider_broker_api::OrderState::Sent,
            trace_id,
        };
        let report = DetailedTcaReport::calculate(
            &intent,
            &[TcaObservation {
                decision_price_ticks: 99,
                arrival_price_ticks: 100,
                send_mid_ticks: 100,
                acknowledgement_mid_ticks: 101,
                fill: FillSample {
                    quantity_ticks: 5,
                    price_ticks: 101,
                },
                decision_mono_ns: 10,
                send_mono_ns: 20,
                acknowledgement_mono_ns: 35,
                fill_mono_ns: 50,
                spread_ticks: 2,
                market_volume_ticks: 100,
                post_fill_mid_ticks: 103,
            }],
        );
        assert!(report.is_ok_and(|report| {
            report.decision_to_send_ns == 10
                && report.send_to_acknowledgement_ns == 15
                && report.acknowledgement_to_fill_ns == 15
                && (report.participation_bps - 500.0).abs() < f64::EPSILON
                && (report.adverse_selection_ticks - 2.0).abs() < f64::EPSILON
                && report.implementation_shortfall_ticks == 2
        }));
    }

    #[test]
    fn pov_planner_conserves_quantity_and_rejects_insufficient_volume() {
        let Some(account_id) = AccountId::new(1).ok() else {
            return;
        };
        let Some(instrument_id) = InstrumentId::new(2).ok() else {
            return;
        };
        let Some(trace_id) = TraceId::new(3).ok() else {
            return;
        };
        let intent = insider_broker_api::OrderIntent {
            intent_id: "intent".into(),
            account_id,
            instrument_id,
            client_order_id: "client".into(),
            side: insider_broker_api::Side::Buy,
            quantity_ticks: 10,
            order_type: OrderType::Market,
            limit_price_ticks: None,
            time_in_force: TimeInForce::Day,
            state: insider_broker_api::OrderState::RiskApproved,
            trace_id,
        };
        let schedule = Schedule::Pov {
            participation_bps: 2_500,
            interval_ns: 10,
            market_volume_ticks: vec![20, 20],
        };
        let children = plan_children(&intent, &schedule);
        assert!(children.is_ok_and(|children| {
            children
                .iter()
                .map(|child| child.quantity_ticks)
                .sum::<i64>()
                == 10
                && children
                    .iter()
                    .map(|child| child.due_after_ns)
                    .collect::<Vec<_>>()
                    == vec![0, 10]
        }));
        assert!(
            plan_children(
                &intent,
                &Schedule::Pov {
                    participation_bps: 2_500,
                    interval_ns: 10,
                    market_volume_ticks: vec![20],
                }
            )
            .is_err()
        );
    }

    #[test]
    fn implementation_shortfall_front_loads_without_quantity_loss() {
        let Some(account_id) = AccountId::new(1).ok() else {
            return;
        };
        let Some(instrument_id) = InstrumentId::new(2).ok() else {
            return;
        };
        let Some(trace_id) = TraceId::new(3).ok() else {
            return;
        };
        let intent = insider_broker_api::OrderIntent {
            intent_id: "intent".into(),
            account_id,
            instrument_id,
            client_order_id: "client".into(),
            side: Side::Buy,
            quantity_ticks: 20,
            order_type: OrderType::Market,
            limit_price_ticks: None,
            time_in_force: TimeInForce::Day,
            state: insider_broker_api::OrderState::RiskApproved,
            trace_id,
        };
        let Ok(children) = plan_children(
            &intent,
            &Schedule::ImplementationShortfall {
                slices: 4,
                interval_ns: 10,
                urgency_bps: 10_000,
            },
        ) else {
            return;
        };
        assert_eq!(
            children
                .iter()
                .map(|child| child.quantity_ticks)
                .sum::<i64>(),
            20
        );
        assert!(children.first().is_some_and(|first| {
            children
                .last()
                .is_some_and(|last| first.quantity_ticks > last.quantity_ticks)
        }));
        assert_eq!(
            children
                .iter()
                .map(|child| child.due_after_ns)
                .collect::<Vec<_>>(),
            vec![0, 10, 20, 30]
        );
    }

    #[test]
    fn adaptive_schedule_switches_to_front_loaded_execution_when_stressed() {
        let Some(account_id) = AccountId::new(1).ok() else {
            return;
        };
        let Some(instrument_id) = InstrumentId::new(2).ok() else {
            return;
        };
        let Some(trace_id) = TraceId::new(3).ok() else {
            return;
        };
        let intent = insider_broker_api::OrderIntent {
            intent_id: "intent".into(),
            account_id,
            instrument_id,
            client_order_id: "client".into(),
            side: Side::Buy,
            quantity_ticks: 20,
            order_type: OrderType::Market,
            limit_price_ticks: None,
            time_in_force: TimeInForce::Day,
            state: insider_broker_api::OrderState::RiskApproved,
            trace_id,
        };
        let schedule = Schedule::Adaptive {
            slices: 4,
            interval_ns: 10,
            urgency_bps: 2_500,
            spread_ticks: 8,
            max_spread_ticks: 2,
            volatility_bps: 100,
            max_volatility_bps: 500,
            market_volume_ticks: vec![20, 20, 20, 20],
        };
        let Ok(children) = plan_children(&intent, &schedule) else {
            return;
        };
        assert_eq!(
            children
                .iter()
                .map(|child| child.quantity_ticks)
                .sum::<i64>(),
            20
        );
        assert!(children.first().is_some_and(|first| {
            children
                .last()
                .is_some_and(|last| first.quantity_ticks > last.quantity_ticks)
        }));
    }

    #[test]
    fn integer_tca_matches_hand_calculation_for_buy() {
        let result = calculate_tca(&TcaInput {
            side: Side::Buy,
            arrival_price_ticks: 100,
            sent_mono_ns: 10,
            ack_mono_ns: Some(25),
            fills: vec![
                TcaFill {
                    quantity_ticks: 3,
                    price_ticks: 101,
                },
                TcaFill {
                    quantity_ticks: 2,
                    price_ticks: 99,
                },
            ],
            market_volume_ticks: Some(100),
            arrival_spread_ticks: Some(2),
        });
        assert_eq!(
            result,
            Ok(TcaResult {
                filled_quantity_ticks: 5,
                average_fill_price_numerator: 501,
                average_fill_price_denominator: 5,
                implementation_shortfall_tick_value: 1,
                ack_latency_ns: Some(15),
                participation_bps: Some(500),
                arrival_spread_ticks: Some(2),
            })
        );
    }

    #[test]
    fn integer_tca_sell_reverses_shortfall_sign_and_rejects_bad_timestamps() {
        let result = calculate_tca(&TcaInput {
            side: Side::Sell,
            arrival_price_ticks: 100,
            sent_mono_ns: 25,
            ack_mono_ns: Some(10),
            fills: vec![TcaFill {
                quantity_ticks: 5,
                price_ticks: 99,
            }],
            market_volume_ticks: None,
            arrival_spread_ticks: None,
        });
        assert_eq!(result, Err(PlanError::InvalidChildTransition));
        let result = calculate_tca(&TcaInput {
            side: Side::Sell,
            arrival_price_ticks: 100,
            sent_mono_ns: 10,
            ack_mono_ns: Some(25),
            fills: vec![TcaFill {
                quantity_ticks: 5,
                price_ticks: 99,
            }],
            market_volume_ticks: None,
            arrival_spread_ticks: None,
        });
        assert_eq!(
            result.map(|value| value.implementation_shortfall_tick_value),
            Ok(5)
        );
    }
}
