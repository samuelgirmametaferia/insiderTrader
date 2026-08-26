//! Reconciles uncertain local orders against authoritative broker state.

#![forbid(unsafe_code)]

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "reconciliation";

use insider_broker_api::{
    BrokerEvent, BrokerGateway, BrokerOrderSnapshot, BrokerSnapshot, OrderState,
};
use insider_execution::{OrderBook, TransitionError};

/// Result of reconciling one client order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileStatus {
    /// The broker returned an event and the local projection applied it.
    Resolved,
    /// The broker has no record yet; the order remains uncertain.
    StillUnknown,
}

/// Classification of one order during a reconciliation sweep.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SweepFinding {
    /// Broker state was applied locally.
    Resolved,
    /// Broker has not exposed the order yet.
    StillUnknown,
    /// The query or local transition failed and needs operator attention.
    Failed(String),
}

/// Bounded aggregate result from one reconciliation pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SweepReport {
    /// Stable client-order finding pairs.
    pub findings: Vec<(String, SweepFinding)>,
}

/// Classification of an order observed during a full broker snapshot sweep.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotFinding {
    /// The broker event was applied to a known local order.
    Applied,
    /// The broker reported an order absent from the local journal.
    ExternalOrder,
    /// A locally working/uncertain order was absent from the broker snapshot.
    MissingAtBroker,
    /// The broker event could not advance the local lifecycle.
    Failed(String),
}

/// Result of comparing a complete broker snapshot to local order state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotReport {
    /// Stable client-order findings in deterministic order.
    pub findings: Vec<(String, SnapshotFinding)>,
    /// Broker-reported positions retained for the caller's portfolio
    /// reconciliation step.
    pub positions: Vec<insider_broker_api::BrokerPositionSnapshot>,
    /// Broker-reported account values retained for the caller's accounting
    /// reconciliation step.
    pub account_values: std::collections::BTreeMap<String, i128>,
}

/// Applies a complete broker snapshot without sending or retrying any order.
///
/// Known orders are advanced only through the normal order-book transition
/// rules. External broker activity and locally missing broker orders remain
/// visible as findings for operator review; they are never silently invented
/// in the local journal.
#[must_use]
pub fn reconcile_snapshot(book: &mut OrderBook, snapshot: BrokerSnapshot) -> SnapshotReport {
    let mut findings = Vec::new();
    let mut observed = std::collections::BTreeSet::new();
    for order in snapshot.orders {
        let client_order_id = order.client_order_id.clone();
        observed.insert(client_order_id.clone());
        let finding = if book.get(&client_order_id).is_none() {
            SnapshotFinding::ExternalOrder
        } else {
            match apply_snapshot_order(book, order) {
                Ok(()) => SnapshotFinding::Applied,
                Err(error) => SnapshotFinding::Failed(format!("{error:?}")),
            }
        };
        findings.push((client_order_id, finding));
    }
    let missing = book
        .client_order_ids()
        .filter(|client_order_id| {
            !observed.contains(*client_order_id)
                && book.get(client_order_id).is_some_and(|record| {
                    matches!(
                        record.intent.state,
                        OrderState::Queued
                            | OrderState::Sending
                            | OrderState::Sent
                            | OrderState::Acknowledged
                            | OrderState::PartiallyFilled
                            | OrderState::CancelPending
                            | OrderState::ReplacePending
                            | OrderState::Unknown
                    )
                })
        })
        .map(|client_order_id| (client_order_id.to_owned(), SnapshotFinding::MissingAtBroker));
    findings.extend(missing);
    findings.sort_by(|left, right| left.0.cmp(&right.0));
    SnapshotReport {
        findings,
        positions: snapshot.positions,
        account_values: snapshot.account_values,
    }
}

fn apply_snapshot_order(
    book: &mut OrderBook,
    order: BrokerOrderSnapshot,
) -> Result<(), TransitionError> {
    let Some(record) = book.get(&order.client_order_id) else {
        return Err(TransitionError::UnknownOrder);
    };
    let current_filled = record.filled_quantity_ticks;
    let requested_quantity = record.intent.quantity_ticks;
    let event = match (order.event, order.filled_quantity_ticks) {
        (
            BrokerEvent::Filled {
                client_order_id,
                quantity_ticks: _,
                price_ticks,
            },
            Some(cumulative),
        ) => {
            if cumulative < current_filled || cumulative > requested_quantity {
                return Err(TransitionError::MismatchedOrder);
            }
            (cumulative > current_filled).then_some(BrokerEvent::Filled {
                client_order_id,
                quantity_ticks: cumulative - current_filled,
                price_ticks,
            })
        }
        (event, _) => Some(event),
    };
    if let Some(event) = event {
        book.apply_broker_event(event)?;
    }
    if let Some(cumulative) = order.filled_quantity_ticks {
        book.apply_cumulative_fill(&order.client_order_id, cumulative)?;
    }
    Ok(())
}

/// Failure while querying or applying broker state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconcileError {
    /// The adapter could not provide authoritative state.
    Gateway(String),
    /// The returned event could not be applied to the local order book.
    Transition(TransitionError),
}

/// Reconciles only orders explicitly marked `Unknown`.
///
/// This boundary prevents a transport timeout from causing a blind resend: the
/// caller must first persist `Unknown`, then invoke this operation until the
/// broker reports a terminal or in-flight event.
///
/// # Errors
/// Returns [`ReconcileError::Gateway`] when the adapter cannot query state,
/// or [`ReconcileError::Transition`] when the local order projection rejects
/// the broker event.
pub fn reconcile_unknown(
    gateway: &dyn BrokerGateway,
    book: &mut OrderBook,
    client_order_id: &str,
) -> Result<ReconcileStatus, ReconcileError> {
    let Some(record) = book.get(client_order_id) else {
        return Err(ReconcileError::Transition(TransitionError::UnknownOrder));
    };
    if record.intent.state != insider_broker_api::OrderState::Unknown {
        return Err(ReconcileError::Transition(
            TransitionError::InvalidTransition {
                from: record.intent.state,
                to: insider_broker_api::OrderState::Unknown,
            },
        ));
    }
    let event = gateway
        .reconcile(client_order_id)
        .map_err(ReconcileError::Gateway)?;
    match event {
        Some(event) => {
            book.apply_broker_event(event)
                .map_err(ReconcileError::Transition)?;
            Ok(ReconcileStatus::Resolved)
        }
        None => Ok(ReconcileStatus::StillUnknown),
    }
}

/// Reconciles every locally unknown order exactly once in client-ID order.
///
/// This function never calls `send`; callers may retry the sweep after the
/// broker session is healthy without risking duplicate orders.
#[must_use]
pub fn reconcile_all_unknown(gateway: &dyn BrokerGateway, book: &mut OrderBook) -> SweepReport {
    let ids: Vec<String> = book
        .client_order_ids()
        .filter(|client_order_id| {
            book.get(client_order_id).is_some_and(|record| {
                record.intent.state == insider_broker_api::OrderState::Unknown
            })
        })
        .map(str::to_owned)
        .collect();
    let findings = ids
        .into_iter()
        .map(|client_order_id| {
            let finding = match reconcile_unknown(gateway, book, &client_order_id) {
                Ok(ReconcileStatus::Resolved) => SweepFinding::Resolved,
                Ok(ReconcileStatus::StillUnknown) => SweepFinding::StillUnknown,
                Err(error) => SweepFinding::Failed(format!("{error:?}")),
            };
            (client_order_id, finding)
        })
        .collect();
    SweepReport { findings }
}

#[cfg(test)]
mod tests {
    use super::{
        ReconcileStatus, SUBSYSTEM_ID, SnapshotFinding, reconcile_snapshot, reconcile_unknown,
    };
    use insider_broker_api::{
        BrokerEvent, BrokerGateway, BrokerOrderSnapshot, BrokerSnapshot, Capabilities, OrderIntent,
        OrderState,
    };
    use insider_common_types::{AccountId, InstrumentId, TraceId};
    use insider_execution::OrderBook;

    struct StubGateway(Option<BrokerEvent>);

    impl BrokerGateway for StubGateway {
        fn capabilities(&self) -> Capabilities {
            Capabilities {
                market: true,
                limit: true,
                fractional_quantity: false,
                cancel_replace: false,
            }
        }

        fn send(&self, _intent: &OrderIntent) -> Result<(), String> {
            Ok(())
        }

        fn reconcile(&self, _client_order_id: &str) -> Result<Option<BrokerEvent>, String> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn subsystem_id_is_non_empty_and_ascii() {
        assert!(!SUBSYSTEM_ID.is_empty());
        assert!(SUBSYSTEM_ID.is_ascii());
    }

    #[test]
    fn unknown_order_is_resolved_without_resend() {
        let account = AccountId::new(1)
            .ok()
            .unwrap_or_else(|| std::process::abort());
        let instrument = InstrumentId::new(2)
            .ok()
            .unwrap_or_else(|| std::process::abort());
        let trace = TraceId::new(3)
            .ok()
            .unwrap_or_else(|| std::process::abort());
        let intent = OrderIntent {
            intent_id: "intent-1".into(),
            client_order_id: "client-1".into(),
            account_id: account,
            instrument_id: instrument,
            side: insider_broker_api::Side::Buy,
            quantity_ticks: 10,
            order_type: insider_broker_api::OrderType::Market,
            limit_price_ticks: None,
            time_in_force: insider_broker_api::TimeInForce::Day,
            state: OrderState::RiskApproved,
            trace_id: trace,
        };
        let mut book = OrderBook::new();
        assert!(book.insert(intent).is_ok());
        assert!(book.mark_unknown("client-1").is_ok());
        let gateway = StubGateway(Some(BrokerEvent::Acknowledged {
            client_order_id: "client-1".into(),
            broker_order_id: "paper-1".into(),
        }));
        assert_eq!(
            reconcile_unknown(&gateway, &mut book, "client-1"),
            Ok(ReconcileStatus::Resolved)
        );
        assert_eq!(
            book.get("client-1").map(|r| r.intent.state),
            Some(OrderState::Acknowledged)
        );
    }

    #[test]
    fn snapshot_exposes_external_and_missing_orders_without_inventing_state() {
        let account = AccountId::new(1)
            .ok()
            .unwrap_or_else(|| std::process::abort());
        let instrument = InstrumentId::new(2)
            .ok()
            .unwrap_or_else(|| std::process::abort());
        let trace = TraceId::new(3)
            .ok()
            .unwrap_or_else(|| std::process::abort());
        let intent = OrderIntent {
            intent_id: "intent-1".into(),
            client_order_id: "local-1".into(),
            account_id: account,
            instrument_id: instrument,
            side: insider_broker_api::Side::Buy,
            quantity_ticks: 10,
            order_type: insider_broker_api::OrderType::Market,
            limit_price_ticks: None,
            time_in_force: insider_broker_api::TimeInForce::Day,
            state: OrderState::RiskApproved,
            trace_id: trace,
        };
        let mut book = OrderBook::new();
        assert!(book.insert(intent).is_ok());
        assert!(book.mark_unknown("local-1").is_ok());
        let report = reconcile_snapshot(
            &mut book,
            BrokerSnapshot {
                orders: vec![
                    BrokerOrderSnapshot {
                        client_order_id: "external-1".into(),
                        filled_quantity_ticks: None,
                        event: BrokerEvent::Acknowledged {
                            client_order_id: "external-1".into(),
                            broker_order_id: "broker-1".into(),
                        },
                    },
                    BrokerOrderSnapshot {
                        client_order_id: "local-1".into(),
                        filled_quantity_ticks: Some(10),
                        event: BrokerEvent::Acknowledged {
                            client_order_id: "local-1".into(),
                            broker_order_id: "broker-local".into(),
                        },
                    },
                ],
                positions: Vec::new(),
                account_values: std::collections::BTreeMap::new(),
            },
        );
        assert!(
            report
                .findings
                .contains(&("external-1".into(), SnapshotFinding::ExternalOrder))
        );
        assert!(
            report
                .findings
                .contains(&("local-1".into(), SnapshotFinding::Applied))
        );
        assert_eq!(
            book.get("local-1")
                .map(|record| (record.intent.state, record.filled_quantity_ticks)),
            Some((OrderState::Filled, 10))
        );
    }
}
