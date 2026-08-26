//! Broker-neutral order contracts and explicit acknowledgement state.

#![forbid(unsafe_code)]
#![allow(missing_docs)]

use insider_common_types::{AccountId, InstrumentId, TraceId};

/// Canonical account-value key for reporting-currency cash ticks.
pub const ACCOUNT_VALUE_CASH_TICKS: &str = "cash_ticks";

/// Broker transport/session health exposed without adapter-specific types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerHealth {
    /// No health observation is available yet.
    Unknown,
    /// Authenticated and ready for requests.
    Healthy,
    /// Connected but reconciliation or operator action is required.
    Degraded,
    /// Disconnected or unavailable.
    Unavailable,
}

/// Order direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Side {
    Buy,
    Sell,
}

/// Broker-neutral order type.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OrderType {
    Market,
    Limit,
}

/// Time-in-force policy.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum TimeInForce {
    Day,
    GoodTilCancel,
    ImmediateOrCancel,
}

/// Explicit order lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderState {
    Created,
    RiskApproved,
    Queued,
    Sending,
    Sent,
    Acknowledged,
    PartiallyFilled,
    Filled,
    CancelPending,
    ReplacePending,
    Cancelled,
    Rejected,
    Expired,
    Unknown,
}

/// Durable broker-neutral order intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderIntent {
    /// Stable intent identity generated before any send.
    pub intent_id: String,
    /// Account identity.
    pub account_id: AccountId,
    /// Instrument identity.
    pub instrument_id: InstrumentId,
    /// Deterministic client order ID.
    pub client_order_id: String,
    /// Signed target delta represented as side plus absolute quantity.
    pub side: Side,
    /// Positive quantity ticks.
    pub quantity_ticks: i64,
    /// Order type.
    pub order_type: OrderType,
    /// Optional limit price ticks.
    pub limit_price_ticks: Option<i64>,
    /// Time in force.
    pub time_in_force: TimeInForce,
    /// Current lifecycle state.
    pub state: OrderState,
    /// Decision trace.
    pub trace_id: TraceId,
}

/// Broker event normalized by an adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerEvent {
    /// Broker acknowledged the client order.
    Acknowledged {
        client_order_id: String,
        broker_order_id: String,
    },
    /// Broker reports a fill.
    Filled {
        client_order_id: String,
        quantity_ticks: i64,
        price_ticks: i64,
    },
    /// Broker rejected the order.
    Rejected {
        client_order_id: String,
        reason: String,
    },
    /// Cancel completed.
    Cancelled { client_order_id: String },
}

/// One order as reported by a broker account snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerOrderSnapshot {
    /// Broker-correlated client order ID.
    pub client_order_id: String,
    /// Latest normalized event for the order.
    pub event: BrokerEvent,
    /// Cumulative filled quantity when the broker supplies it.
    pub filled_quantity_ticks: Option<i64>,
}

/// One canonical position reported by a broker account snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrokerPositionSnapshot {
    /// Canonical instrument identity.
    pub instrument_id: InstrumentId,
    /// Signed quantity in canonical ticks.
    pub quantity_ticks: i64,
}

/// Authoritative broker state used during startup and reconnect reconciliation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BrokerSnapshot {
    /// Orders available to the adapter for reconciliation.
    pub orders: Vec<BrokerOrderSnapshot>,
    /// Current signed positions.
    pub positions: Vec<BrokerPositionSnapshot>,
    /// Account values in canonical reporting-currency ticks.
    pub account_values: std::collections::BTreeMap<String, i128>,
}

/// Capability declaration used to reject unsupported order combinations pre-send.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct Capabilities {
    pub market: bool,
    pub limit: bool,
    pub fractional_quantity: bool,
    pub cancel_replace: bool,
}

/// Immutable instrument precision and asset class required for broker preflight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TradingSpec {
    /// Canonical asset class.
    pub asset_class: insider_market_types::AssetClass,
    /// Positive quantity increment in canonical ticks.
    pub quantity_increment_ticks: i64,
    /// Positive price increment in canonical ticks.
    pub price_increment_ticks: i64,
}

/// One supported broker combination.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CapabilityKey {
    /// Supported asset class.
    pub asset_class: insider_market_types::AssetClass,
    /// Supported order type.
    pub order_type: OrderType,
    /// Supported time in force.
    pub time_in_force: TimeInForce,
}

/// Versioned capability matrix used by adapter preflight.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityMatrix {
    /// Matrix revision recorded with every preflight decision.
    pub version: String,
    supported: std::collections::BTreeSet<CapabilityKey>,
}

/// Deterministic broker preflight failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreflightError {
    /// Capability tuple is not certified by this matrix.
    UnsupportedCapability(CapabilityKey),
    /// Quantity or increment metadata is invalid.
    InvalidQuantity,
    /// Limit price or price increment metadata is invalid.
    InvalidPrice,
    /// Matrix revision is blank.
    InvalidMatrix,
}

impl CapabilityMatrix {
    /// Creates a matrix with a non-empty revision and no capabilities.
    ///
    /// # Errors
    /// Returns [`PreflightError::InvalidMatrix`] when the revision is blank.
    pub fn new(version: impl Into<String>) -> Result<Self, PreflightError> {
        let version = version.into();
        if version.trim().is_empty() {
            return Err(PreflightError::InvalidMatrix);
        }
        Ok(Self {
            version,
            supported: std::collections::BTreeSet::new(),
        })
    }

    /// Adds one certified asset/order/time-in-force combination.
    pub fn allow(&mut self, key: CapabilityKey) {
        self.supported.insert(key);
    }

    /// Returns whether a combination is explicitly certified.
    #[must_use]
    pub fn supports(&self, key: CapabilityKey) -> bool {
        self.supported.contains(&key)
    }

    /// Validates an intent against capabilities and canonical increments.
    ///
    /// # Errors
    /// Returns [`PreflightError`] before any broker transport call is possible.
    pub fn preflight(&self, intent: &OrderIntent, spec: TradingSpec) -> Result<(), PreflightError> {
        let key = CapabilityKey {
            asset_class: spec.asset_class,
            order_type: intent.order_type,
            time_in_force: intent.time_in_force,
        };
        if !self.supports(key) {
            return Err(PreflightError::UnsupportedCapability(key));
        }
        if spec.quantity_increment_ticks <= 0
            || intent.quantity_ticks <= 0
            || intent.quantity_ticks % spec.quantity_increment_ticks != 0
        {
            return Err(PreflightError::InvalidQuantity);
        }
        match intent.order_type {
            OrderType::Market if intent.limit_price_ticks.is_some() => {
                Err(PreflightError::InvalidPrice)
            }
            OrderType::Market => Ok(()),
            OrderType::Limit => {
                let Some(price) = intent.limit_price_ticks else {
                    return Err(PreflightError::InvalidPrice);
                };
                if spec.price_increment_ticks <= 0
                    || price <= 0
                    || price % spec.price_increment_ticks != 0
                {
                    return Err(PreflightError::InvalidPrice);
                }
                Ok(())
            }
        }
    }
}

/// Adapter boundary; implementations must preserve client-order idempotency.
pub trait BrokerGateway: Send + Sync {
    /// Reports current transport/session health. Adapters that cannot expose
    /// a session state remain explicitly `Unknown`.
    fn health(&self) -> BrokerHealth {
        BrokerHealth::Unknown
    }
    /// Reports adapter capabilities.
    fn capabilities(&self) -> Capabilities;
    /// Sends a pre-persisted order intent.
    ///
    /// # Errors
    /// Returns a broker/transport error without changing local state; callers
    /// must reconcile `Unknown` before retrying.
    fn send(&self, intent: &OrderIntent) -> Result<(), String>;
    /// Reconciles an uncertain client order ID.
    ///
    /// # Errors
    /// Returns an adapter error when authoritative broker state is unavailable.
    fn reconcile(&self, client_order_id: &str) -> Result<Option<BrokerEvent>, String>;

    /// Requests authoritative broker state for reconciliation.
    ///
    /// # Errors
    /// Returns an adapter error when snapshot state is unavailable.
    fn snapshot(&self) -> Result<BrokerSnapshot, String> {
        Err(String::from("broker snapshot unsupported"))
    }

    /// Requests cancellation of a working order when supported.
    ///
    /// The default keeps existing adapters source-compatible while making
    /// unsupported cancellation explicit.
    ///
    /// # Errors
    /// Returns an unsupported-operation diagnostic by default.
    fn cancel(&self, _client_order_id: &str) -> Result<(), String> {
        Err(String::from("cancel unsupported"))
    }

    /// Requests replacement of a working order's quantity/limit.
    ///
    /// # Errors
    /// Returns an unsupported-operation diagnostic by default.
    fn replace(
        &self,
        _client_order_id: &str,
        _quantity_ticks: i64,
        _limit_price_ticks: Option<i64>,
    ) -> Result<(), String> {
        Err(String::from("replace unsupported"))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CapabilityKey, CapabilityMatrix, OrderIntent, OrderState, OrderType, PreflightError, Side,
        TimeInForce, TradingSpec,
    };
    use insider_common_types::{AccountId, InstrumentId, TraceId};

    #[test]
    fn capability_matrix_preflights_asset_order_and_precision() {
        let mut matrix = CapabilityMatrix::new("ibkr-v1").ok();
        assert!(matrix.is_some());
        let key = CapabilityKey {
            asset_class: insider_market_types::AssetClass::Equity,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Day,
        };
        if let Some(matrix) = matrix.as_mut() {
            matrix.allow(key);
            let intent = OrderIntent {
                intent_id: "intent-1".into(),
                account_id: AccountId::new(1)
                    .ok()
                    .unwrap_or_else(|| std::process::abort()),
                instrument_id: InstrumentId::new(2)
                    .ok()
                    .unwrap_or_else(|| std::process::abort()),
                client_order_id: "client-1".into(),
                side: Side::Buy,
                quantity_ticks: 10,
                order_type: OrderType::Limit,
                limit_price_ticks: Some(100),
                time_in_force: TimeInForce::Day,
                state: OrderState::RiskApproved,
                trace_id: TraceId::new(3)
                    .ok()
                    .unwrap_or_else(|| std::process::abort()),
            };
            let spec = TradingSpec {
                asset_class: insider_market_types::AssetClass::Equity,
                quantity_increment_ticks: 5,
                price_increment_ticks: 5,
            };
            assert!(matrix.preflight(&intent, spec).is_ok());
            let mut invalid = intent;
            invalid.limit_price_ticks = Some(101);
            assert_eq!(
                matrix.preflight(&invalid, spec),
                Err(PreflightError::InvalidPrice)
            );
            invalid.limit_price_ticks = Some(100);
            invalid.quantity_ticks = 3;
            assert_eq!(
                matrix.preflight(&invalid, spec),
                Err(PreflightError::InvalidQuantity)
            );
        }
    }

    #[test]
    fn matrix_denies_uncertified_combinations_before_transport() {
        let matrix = CapabilityMatrix::new("ibkr-v1").ok();
        assert!(matrix.is_some_and(|matrix| !matrix.supports(CapabilityKey {
            asset_class: insider_market_types::AssetClass::Option,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Day,
        })));
    }
}
