//! Deterministic paper broker implementing the broker-neutral gateway contract.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use insider_broker_api::{
    BrokerEvent, BrokerGateway, BrokerHealth, BrokerOrderSnapshot, BrokerSnapshot, Capabilities,
    OrderIntent, OrderType, Side,
};
use insider_common_types::InstrumentId;

/// Paper-broker failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SimError {
    /// Client order ID was already accepted.
    DuplicateClientOrder,
    /// No positive mark is available.
    UnknownPrice,
    /// Quantity is zero or negative.
    InvalidQuantity,
    /// Requested order feature is unsupported.
    UnsupportedOrder,
}

/// Deterministic in-memory exchange simulator.
pub struct PaperBroker {
    prices: Mutex<BTreeMap<InstrumentId, i64>>,
    events: Mutex<VecDeque<BrokerEvent>>,
    seen: Mutex<BTreeMap<String, BrokerEvent>>,
    accepted: Mutex<BTreeMap<String, OrderIntent>>,
    resting: Mutex<BTreeMap<String, OrderIntent>>,
    next_order_id: Mutex<u64>,
    capabilities: Capabilities,
}

impl PaperBroker {
    /// Creates a paper broker with market and limit support.
    #[must_use]
    pub fn new() -> Self {
        Self {
            prices: Mutex::new(BTreeMap::new()),
            events: Mutex::new(VecDeque::new()),
            seen: Mutex::new(BTreeMap::new()),
            accepted: Mutex::new(BTreeMap::new()),
            resting: Mutex::new(BTreeMap::new()),
            next_order_id: Mutex::new(1),
            capabilities: Capabilities {
                market: true,
                limit: true,
                fractional_quantity: false,
                cancel_replace: true,
            },
        }
    }

    /// Sets the deterministic mark used for subsequent marketable fills.
    ///
    /// # Errors
    /// Returns [`SimError::UnknownPrice`] when `price_ticks` is not positive
    /// or the simulator lock is unavailable.
    pub fn set_price(&self, instrument_id: InstrumentId, price_ticks: i64) -> Result<(), SimError> {
        if price_ticks <= 0 {
            return Err(SimError::UnknownPrice);
        }
        let Ok(mut prices) = self.prices.lock() else {
            return Err(SimError::UnknownPrice);
        };
        prices.insert(instrument_id, price_ticks);
        drop(prices);
        self.match_resting(instrument_id, price_ticks)?;
        Ok(())
    }

    fn match_resting(&self, instrument_id: InstrumentId, price_ticks: i64) -> Result<(), SimError> {
        let candidates = self
            .resting
            .lock()
            .map_err(|_| SimError::UnknownPrice)?
            .iter()
            .filter(|(_, intent)| {
                intent.instrument_id == instrument_id
                    && intent
                        .limit_price_ticks
                        .is_some_and(|limit| match intent.side {
                            Side::Buy => limit >= price_ticks,
                            Side::Sell => limit <= price_ticks,
                        })
            })
            .map(|(client, _)| client.clone())
            .collect::<Vec<_>>();
        for client_order_id in candidates {
            let intent = self
                .resting
                .lock()
                .map_err(|_| SimError::UnknownPrice)?
                .remove(&client_order_id);
            let Some(intent) = intent else { continue };
            let filled = BrokerEvent::Filled {
                client_order_id: client_order_id.clone(),
                quantity_ticks: intent.quantity_ticks,
                price_ticks,
            };
            self.seen
                .lock()
                .map_err(|_| SimError::UnknownPrice)?
                .insert(client_order_id, filled.clone());
            self.events
                .lock()
                .map_err(|_| SimError::UnknownPrice)?
                .push_back(filled);
        }
        Ok(())
    }

    /// Drains normalized broker events in deterministic emission order.
    pub fn drain_events(&self) -> Vec<BrokerEvent> {
        self.events
            .lock()
            .map_or_else(|_| Vec::new(), |mut events| events.drain(..).collect())
    }
}

impl Default for PaperBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl BrokerGateway for PaperBroker {
    fn health(&self) -> BrokerHealth {
        BrokerHealth::Healthy
    }
    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    fn send(&self, intent: &OrderIntent) -> Result<(), String> {
        if intent.quantity_ticks <= 0 {
            return Err(format!("{:?}", SimError::InvalidQuantity));
        }
        if !matches!(intent.order_type, OrderType::Market | OrderType::Limit) {
            return Err(format!("{:?}", SimError::UnsupportedOrder));
        }
        let price = self
            .prices
            .lock()
            .map_err(|_| format!("{:?}", SimError::UnknownPrice))?
            .get(&intent.instrument_id)
            .copied()
            .ok_or_else(|| format!("{:?}", SimError::UnknownPrice))?;
        if let Some(existing) = self
            .seen
            .lock()
            .map_err(|_| String::from("seen lock poisoned"))?
            .get(&intent.client_order_id)
        {
            return Err(format!("duplicate {existing:?}"));
        }
        self.accepted
            .lock()
            .map_err(|_| String::from("accepted lock poisoned"))?
            .insert(intent.client_order_id.clone(), intent.clone());
        let mut order_id = self
            .next_order_id
            .lock()
            .map_err(|_| String::from("order id lock poisoned"))?;
        let broker_order_id = format!("paper-{order_id}");
        *order_id = order_id.saturating_add(1);
        let acknowledged = BrokerEvent::Acknowledged {
            client_order_id: intent.client_order_id.clone(),
            broker_order_id,
        };
        let quantity = if matches!(intent.side, Side::Buy | Side::Sell) {
            intent.quantity_ticks
        } else {
            return Err(format!("{:?}", SimError::InvalidQuantity));
        };
        let marketable = match intent.order_type {
            OrderType::Market => true,
            OrderType::Limit => intent
                .limit_price_ticks
                .is_some_and(|limit| match intent.side {
                    Side::Buy => limit >= price,
                    Side::Sell => limit <= price,
                }),
        };
        let mut events = self
            .events
            .lock()
            .map_err(|_| String::from("events lock poisoned"))?;
        events.push_back(acknowledged);
        drop(events);
        if marketable {
            let filled = BrokerEvent::Filled {
                client_order_id: intent.client_order_id.clone(),
                quantity_ticks: quantity,
                price_ticks: price,
            };
            self.seen
                .lock()
                .map_err(|_| String::from("seen lock poisoned"))?
                .insert(intent.client_order_id.clone(), filled.clone());
            self.events
                .lock()
                .map_err(|_| String::from("events lock poisoned"))?
                .push_back(filled);
        } else {
            self.seen
                .lock()
                .map_err(|_| String::from("seen lock poisoned"))?
                .insert(
                    intent.client_order_id.clone(),
                    BrokerEvent::Acknowledged {
                        client_order_id: intent.client_order_id.clone(),
                        broker_order_id: format!("paper-{order_id}"),
                    },
                );
            self.resting
                .lock()
                .map_err(|_| String::from("resting lock poisoned"))?
                .insert(intent.client_order_id.clone(), intent.clone());
        }
        Ok(())
    }

    fn reconcile(&self, client_order_id: &str) -> Result<Option<BrokerEvent>, String> {
        self.seen
            .lock()
            .map_err(|_| String::from("seen lock poisoned"))
            .map(|seen| seen.get(client_order_id).cloned())
    }

    fn snapshot(&self) -> Result<BrokerSnapshot, String> {
        let seen = self
            .seen
            .lock()
            .map_err(|_| String::from("seen lock poisoned"))?;
        let orders = seen
            .iter()
            .map(|(client_order_id, event)| BrokerOrderSnapshot {
                client_order_id: client_order_id.clone(),
                filled_quantity_ticks: match event {
                    BrokerEvent::Filled { quantity_ticks, .. } => Some(*quantity_ticks),
                    _ => None,
                },
                event: event.clone(),
            })
            .collect();
        let accepted = self
            .accepted
            .lock()
            .map_err(|_| String::from("accepted lock poisoned"))?;
        let mut quantities = BTreeMap::<InstrumentId, i64>::new();
        for (client_order_id, event) in &*seen {
            let BrokerEvent::Filled { quantity_ticks, .. } = event else {
                continue;
            };
            let Some(intent) = accepted.get(client_order_id) else {
                return Err(String::from("filled order missing accepted intent"));
            };
            let signed = if intent.side == Side::Buy {
                *quantity_ticks
            } else {
                quantity_ticks.saturating_neg()
            };
            let entry = quantities.entry(intent.instrument_id).or_default();
            *entry = entry
                .checked_add(signed)
                .ok_or_else(|| String::from("paper position overflow"))?;
        }
        let positions = quantities
            .into_iter()
            .filter(|(_, quantity_ticks)| *quantity_ticks != 0)
            .map(
                |(instrument_id, quantity_ticks)| insider_broker_api::BrokerPositionSnapshot {
                    instrument_id,
                    quantity_ticks,
                },
            )
            .collect();
        Ok(BrokerSnapshot {
            orders,
            positions,
            account_values: BTreeMap::new(),
        })
    }

    fn cancel(&self, client_order_id: &str) -> Result<(), String> {
        let removed = self
            .resting
            .lock()
            .map_err(|_| String::from("resting lock poisoned"))?
            .remove(client_order_id);
        if removed.is_none() {
            return Err(String::from("order not working"));
        }
        let event = BrokerEvent::Cancelled {
            client_order_id: client_order_id.to_owned(),
        };
        self.seen
            .lock()
            .map_err(|_| String::from("seen lock poisoned"))?
            .insert(client_order_id.to_owned(), event.clone());
        self.events
            .lock()
            .map_err(|_| String::from("events lock poisoned"))?
            .push_back(event);
        Ok(())
    }

    fn replace(
        &self,
        client_order_id: &str,
        quantity_ticks: i64,
        limit_price_ticks: Option<i64>,
    ) -> Result<(), String> {
        if quantity_ticks <= 0 {
            return Err(format!("{:?}", SimError::InvalidQuantity));
        }
        let mut resting = self
            .resting
            .lock()
            .map_err(|_| String::from("resting lock poisoned"))?;
        let intent = resting
            .get_mut(client_order_id)
            .ok_or_else(|| String::from("order not working"))?;
        if matches!(intent.order_type, OrderType::Limit) && limit_price_ticks.is_none() {
            return Err(format!("{:?}", SimError::UnsupportedOrder));
        }
        intent.quantity_ticks = quantity_ticks;
        intent.limit_price_ticks = limit_price_ticks;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use insider_broker_api::{
        BrokerEvent, BrokerGateway, OrderIntent, OrderState, OrderType, Side, TimeInForce,
    };
    use insider_common_types::{AccountId, InstrumentId, TraceId};

    use super::PaperBroker;

    #[test]
    fn paper_broker_fills_deterministically_and_reconciles_duplicates() {
        let Some(instrument_id) = InstrumentId::new(1).ok() else {
            return;
        };
        let Some(account_id) = AccountId::new(1).ok() else {
            return;
        };
        let Some(trace_id) = TraceId::new(1).ok() else {
            return;
        };
        let broker = PaperBroker::new();
        assert!(broker.set_price(instrument_id, 101).is_ok());
        let intent = OrderIntent {
            intent_id: String::from("intent-1"),
            account_id,
            instrument_id,
            client_order_id: String::from("client-1"),
            side: Side::Buy,
            quantity_ticks: 3,
            order_type: OrderType::Market,
            limit_price_ticks: None,
            time_in_force: TimeInForce::Day,
            state: OrderState::RiskApproved,
            trace_id,
        };
        assert!(broker.send(&intent).is_ok());
        assert!(broker.send(&intent).is_err());
        let events = broker.drain_events();
        assert!(matches!(
            events.first(),
            Some(BrokerEvent::Acknowledged { .. })
        ));
        assert!(matches!(
            events.get(1),
            Some(BrokerEvent::Filled {
                quantity_ticks: 3,
                price_ticks: 101,
                ..
            })
        ));
        assert!(matches!(
            broker.reconcile("client-1"),
            Ok(Some(BrokerEvent::Filled { .. }))
        ));
        let Ok(snapshot) = broker.snapshot() else {
            return;
        };
        assert_eq!(snapshot.positions.len(), 1);
        assert_eq!(snapshot.positions[0].instrument_id, instrument_id);
        assert_eq!(snapshot.positions[0].quantity_ticks, 3);
    }

    #[test]
    fn limit_orders_rest_until_marketable_and_can_be_cancelled() {
        let Some(instrument_id) = InstrumentId::new(2).ok() else {
            return;
        };
        let Some(account_id) = AccountId::new(2).ok() else {
            return;
        };
        let Some(trace_id) = TraceId::new(2).ok() else {
            return;
        };
        let broker = PaperBroker::new();
        assert!(broker.set_price(instrument_id, 100).is_ok());
        let intent = OrderIntent {
            intent_id: "intent-2".into(),
            account_id,
            instrument_id,
            client_order_id: "client-2".into(),
            side: Side::Buy,
            quantity_ticks: 2,
            order_type: OrderType::Limit,
            limit_price_ticks: Some(99),
            time_in_force: TimeInForce::Day,
            state: OrderState::RiskApproved,
            trace_id,
        };
        assert!(broker.send(&intent).is_ok());
        assert!(
            broker
                .drain_events()
                .iter()
                .all(|event| matches!(event, BrokerEvent::Acknowledged { .. }))
        );
        assert!(broker.set_price(instrument_id, 99).is_ok());
        assert!(broker.drain_events().iter().any(|event| matches!(
            event,
            BrokerEvent::Filled {
                quantity_ticks: 2,
                price_ticks: 99,
                ..
            }
        )));
        assert!(broker.set_price(instrument_id, 100).is_ok());
        let cancel = OrderIntent {
            client_order_id: "client-3".into(),
            intent_id: "intent-3".into(),
            limit_price_ticks: Some(98),
            ..intent
        };
        assert!(broker.send(&cancel).is_ok());
        assert!(broker.cancel("client-3").is_ok());
        assert!(
            broker
                .drain_events()
                .iter()
                .any(|event| matches!(event, BrokerEvent::Cancelled { .. }))
        );
    }
}
