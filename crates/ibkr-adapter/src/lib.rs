//! Transport-isolated Interactive Brokers gateway adapter.
//!
//! The adapter owns IBKR session/callback correlation. Domain crates only see
//! the broker-neutral [`BrokerGateway`] contract and normalized events.

#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Read;
use std::sync::Mutex;

use insider_broker_api::{
    BrokerEvent, BrokerGateway, BrokerHealth, BrokerSnapshot, Capabilities, OrderIntent, OrderType,
};

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "ibkr_adapter";
/// Maximum callbacks retained between adapter and engine drains.
pub const MAX_CALLBACK_QUEUE: usize = 8_192;
const MAX_HTTP_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_HTTP_TIMEOUT_MS: u64 = 120_000;

/// Bounded HTTPS request used by the Client Portal transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRequest {
    /// HTTP method.
    pub method: String,
    /// Fully qualified HTTPS endpoint.
    pub url: String,
    /// JSON body, when required.
    pub body: Option<Vec<u8>>,
}

/// Bounded response returned by an injected HTTP implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpResponse {
    /// HTTP status.
    pub status: u16,
    /// Response bytes.
    pub body: Vec<u8>,
}

/// Normalized market-data snapshot returned by the IBKR Client Portal.
/// Prices remain floating-point at the provider boundary; the engine converts
/// them to canonical integer ticks using the instrument's increment.
#[derive(Clone, Debug, PartialEq)]
pub struct IbkrQuoteSnapshot {
    /// IBKR contract identifier.
    pub conid: i64,
    /// Best bid price, when currently quoted.
    pub bid: Option<f64>,
    /// Best ask price, when currently quoted.
    pub ask: Option<f64>,
    /// Last traded price, when available.
    pub last: Option<f64>,
    /// Best bid size, when available.
    pub bid_size: Option<f64>,
    /// Best ask size, when available.
    pub ask_size: Option<f64>,
}

/// HTTP boundary for production Client Portal or deterministic fixtures.
pub trait HttpTransport: Send + Sync {
    /// Sends one bounded request.
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, String>;
}

/// Rustls-backed HTTPS transport for a local or remote IBKR Client Portal
/// Gateway. Credentials/session cookies remain owned by the gateway process.
pub struct ReqwestHttpTransport {
    client: reqwest::blocking::Client,
}

impl ReqwestHttpTransport {
    /// Creates a TLS client with a bounded timeout.
    pub fn new(timeout_ms: u64) -> Result<Self, String> {
        if timeout_ms == 0 || timeout_ms > MAX_HTTP_TIMEOUT_MS {
            return Err("IBKR HTTP timeout is outside bounds".into());
        }
        let timeout = std::time::Duration::from_millis(timeout_ms);
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("IBKR HTTP client: {error}"))?;
        Ok(Self { client })
    }
}

impl HttpTransport for ReqwestHttpTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        validate_https_url(&request.url, "IBKR request URL")?;
        let mut builder = match request.method.as_str() {
            "GET" => self.client.get(&request.url),
            "POST" => self.client.post(&request.url),
            "DELETE" => self.client.delete(&request.url),
            _ => return Err("unsupported IBKR HTTP method".into()),
        };
        if let Some(body) = request.body {
            if body.len() > MAX_HTTP_RESPONSE_BYTES {
                return Err("IBKR request exceeds bound".into());
            }
            builder = builder
                .header("content-type", "application/json")
                .body(body);
        }
        let response = builder
            .send()
            .map_err(|error| format!("IBKR transport: {error}"))?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_HTTP_RESPONSE_BYTES as u64)
        {
            return Err("IBKR response exceeds bound".into());
        }
        let status = response.status().as_u16();
        let body = read_bounded_response(response)?;
        Ok(HttpResponse { status, body })
    }
}

fn read_bounded_response(reader: impl Read) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    reader
        .take((MAX_HTTP_RESPONSE_BYTES as u64).saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|error| format!("IBKR response body: {error}"))?;
    if body.len() > MAX_HTTP_RESPONSE_BYTES {
        return Err("IBKR response exceeds bound".into());
    }
    Ok(body)
}

/// Client Portal Gateway configuration. `account_id` is the IBKR account
/// identifier, while canonical instrument IDs are sent as IBKR `conid`s.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientPortalConfig {
    /// HTTPS Client Portal Gateway root, for example `https://127.0.0.1:5000`.
    pub base_url: String,
    /// IBKR account identifier.
    pub account_id: String,
}

/// Concrete Client Portal implementation of the broker-neutral transport.
pub struct ClientPortalTransport<T> {
    transport: T,
    config: ClientPortalConfig,
    broker_order_ids: Mutex<BTreeMap<String, String>>,
}

impl<T: HttpTransport> ClientPortalTransport<T> {
    /// Creates a validated Client Portal transport.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(transport: T, config: ClientPortalConfig) -> Result<Self, String> {
        validate_https_url(&config.base_url, "IBKR base URL")?;
        if config.account_id.trim().is_empty() || config.account_id.len() > 128 {
            return Err("invalid IBKR Client Portal configuration".into());
        }
        Ok(Self {
            transport,
            config: ClientPortalConfig {
                base_url: config.base_url.trim_end_matches('/').to_owned(),
                account_id: config.account_id.trim().to_owned(),
            },
            broker_order_ids: Mutex::new(BTreeMap::new()),
        })
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.config.base_url, path)
    }

    /// Verifies that the Client Portal Gateway session is authenticated and
    /// connected before the broker adapter is made send-capable.
    ///
    /// # Errors
    /// Returns a transport/HTTP/shape error, or a stable diagnostic when IBKR
    /// reports an unauthenticated, disconnected, or competing session.
    pub fn verify_session(&self) -> Result<(), String> {
        let value = self.request("GET", "/v1/api/iserver/auth/status", None)?;
        let authenticated = value
            .get("authenticated")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| "IBKR auth status missing authenticated flag".to_owned())?;
        let connected = value
            .get("connected")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let competing = value
            .get("competing")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if !authenticated {
            return Err("IBKR Client Portal session is not authenticated".into());
        }
        if !connected {
            return Err("IBKR Client Portal session is not connected".into());
        }
        if competing {
            return Err("IBKR Client Portal session is competing".into());
        }
        Ok(())
    }

    /// Fetches one authoritative top-of-book snapshot for a contract.
    ///
    /// IBKR returns quote fields as either numbers or `{value: ...}` objects;
    /// this adapter normalizes both forms and rejects malformed/non-finite
    /// values while allowing legitimately unavailable last/size fields.
    pub fn market_snapshot(&self, conid: i64) -> Result<IbkrQuoteSnapshot, String> {
        if conid <= 0 {
            return Err("IBKR conid must be positive".into());
        }
        let value = self.request(
            "GET",
            &format!("/v1/api/iserver/marketdata/snapshot?conids={conid}&fields=31,84,85,86,88"),
            None,
        )?;
        let item = value
            .as_array()
            .and_then(|items| items.first())
            .or_else(|| (!value.is_null()).then_some(&value))
            .ok_or_else(|| "IBKR market snapshot is empty".to_owned())?;
        let snapshot = IbkrQuoteSnapshot {
            conid,
            bid: snapshot_field(item, "84"),
            ask: snapshot_field(item, "86"),
            last: snapshot_field(item, "31"),
            bid_size: snapshot_field(item, "85"),
            ask_size: snapshot_field(item, "88"),
        };
        if snapshot
            .bid
            .is_some_and(|value| !value.is_finite() || value <= 0.0)
            || snapshot
                .ask
                .is_some_and(|value| !value.is_finite() || value <= 0.0)
            || snapshot
                .bid_size
                .is_some_and(|value| !value.is_finite() || value < 0.0)
            || snapshot
                .ask_size
                .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err("IBKR market snapshot contains invalid values".into());
        }
        if let (Some(bid), Some(ask)) = (snapshot.bid, snapshot.ask)
            && bid > ask
        {
            return Err("IBKR market snapshot is crossed".into());
        }
        Ok(snapshot)
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<serde_json::Value, String> {
        let response = self.transport.send(HttpRequest {
            method: method.to_owned(),
            url: self.endpoint(path),
            body,
        })?;
        if !(200..300).contains(&response.status) {
            return Err(format!(
                "IBKR Client Portal HTTP status {}",
                response.status
            ));
        }
        if response.body.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_slice(&response.body)
            .map_err(|_| "IBKR Client Portal returned invalid JSON".into())
    }

    fn orders(&self) -> Result<Vec<serde_json::Value>, String> {
        let value = self.request(
            "GET",
            &format!("/v1/api/iserver/account/{}/orders", self.config.account_id),
            None,
        )?;
        value
            .get("orders")
            .or_else(|| value.as_array().map(|_| &value))
            .and_then(serde_json::Value::as_array)
            .cloned()
            .ok_or_else(|| "IBKR orders response missing orders array".into())
    }

    fn trades(&self) -> Result<Vec<serde_json::Value>, String> {
        let value = self.request("GET", "/v1/api/iserver/account/trades", None)?;
        value
            .get("trades")
            .or_else(|| value.as_array().map(|_| &value))
            .and_then(serde_json::Value::as_array)
            .cloned()
            .ok_or_else(|| "IBKR trades response missing trades array".into())
    }

    fn order_event(order: &serde_json::Value) -> Option<(String, BrokerEvent, Option<i64>)> {
        let client_id = order
            .get("cOID")
            .or_else(|| order.get("clientOrderId"))
            .and_then(serde_json::Value::as_str)?
            .to_owned();
        let broker_id = order
            .get("orderId")
            .or_else(|| order.get("id"))
            .and_then(value_as_string)?;
        let status = order
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_ascii_uppercase();
        let filled = order
            .get("filledQuantity")
            .and_then(value_as_i64)
            .unwrap_or(0);
        let event = if status == "FILLED" {
            BrokerEvent::Filled {
                client_order_id: client_id.clone(),
                quantity_ticks: filled,
                price_ticks: order.get("avgPrice").and_then(value_as_i64).unwrap_or(0),
            }
        } else if status == "CANCELLED" || status == "CANCELED" {
            BrokerEvent::Cancelled {
                client_order_id: client_id.clone(),
            }
        } else if status == "REJECTED" || status == "INACTIVE" {
            BrokerEvent::Rejected {
                client_order_id: client_id.clone(),
                reason: order
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("IBKR rejected order")
                    .chars()
                    .take(512)
                    .collect(),
            }
        } else {
            BrokerEvent::Acknowledged {
                client_order_id: client_id.clone(),
                broker_order_id: broker_id.clone(),
            }
        };
        Some((broker_id, event, (filled > 0).then_some(filled)))
    }

    fn trade_event(trade: &serde_json::Value) -> Option<(String, BrokerEvent, i64)> {
        let nested_order = trade.get("order");
        let client_id = trade
            .get("cOID")
            .or_else(|| trade.get("clientOrderId"))
            .or_else(|| nested_order.and_then(|order| order.get("cOID")))
            .or_else(|| nested_order.and_then(|order| order.get("clientOrderId")))
            .and_then(serde_json::Value::as_str)?
            .to_owned();
        let broker_id = trade
            .get("orderId")
            .or_else(|| nested_order.and_then(|order| order.get("orderId")))
            .and_then(value_as_string)?;
        let execution = trade.get("execution").unwrap_or(trade);
        let quantity = execution
            .get("quantity")
            .or_else(|| execution.get("size"))
            .or_else(|| trade.get("quantity"))
            .and_then(value_as_i64)?;
        let price = execution
            .get("price")
            .or_else(|| execution.get("avgPrice"))
            .or_else(|| trade.get("price"))
            .and_then(value_as_i64)?;
        if quantity <= 0 || price <= 0 {
            return None;
        }
        Some((
            broker_id,
            BrokerEvent::Filled {
                client_order_id: client_id,
                quantity_ticks: quantity,
                price_ticks: price,
            },
            quantity,
        ))
    }

    fn broker_order_id(&self, client_order_id: &str) -> Result<String, String> {
        if let Some(id) = self
            .broker_order_ids
            .lock()
            .map_err(|_| "IBKR order map poisoned".to_owned())?
            .get(client_order_id)
            .cloned()
        {
            return Ok(id);
        }
        let orders = self.orders()?;
        let (broker_id, _, _) = orders
            .iter()
            .filter_map(Self::order_event)
            .find(|(_, event, _)| callback_client_id(event) == client_order_id)
            .ok_or_else(|| "IBKR client order ID not found during reconciliation".to_owned())?;
        self.broker_order_ids
            .lock()
            .map_err(|_| "IBKR order map poisoned".to_owned())?
            .insert(client_order_id.to_owned(), broker_id.clone());
        Ok(broker_id)
    }
}

fn validate_https_url(value: &str, label: &str) -> Result<(), String> {
    if value.len() > 2_048 || value.chars().any(char::is_whitespace) {
        return Err(format!("{label} exceeds 2048 bytes or contains whitespace"));
    }
    let Some(authority_and_path) = value.strip_prefix("https://") else {
        return Err(format!("{label} requires HTTPS"));
    };
    let authority = authority_and_path
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if authority.is_empty() || authority.contains('@') {
        return Err(format!("{label} has an invalid HTTPS authority"));
    }
    Ok(())
}

impl<T: HttpTransport> IbkrTransport for ClientPortalTransport<T> {
    fn market_snapshot(&self, conid: i64) -> Result<IbkrQuoteSnapshot, String> {
        Self::market_snapshot(self, conid)
    }

    fn place_order(&self, request: IbkrOrderRequest) -> Result<(), String> {
        let intent = &request.intent;
        let conid = i64::try_from(intent.instrument_id.get())
            .map_err(|_| "instrument ID cannot be represented as IBKR conid".to_owned())?;
        let mut order = serde_json::Map::new();
        order.insert("acctId".into(), self.config.account_id.clone().into());
        order.insert("conid".into(), conid.into());
        order.insert(
            "side".into(),
            match intent.side {
                insider_broker_api::Side::Buy => "BUY",
                insider_broker_api::Side::Sell => "SELL",
            }
            .into(),
        );
        order.insert("quantity".into(), intent.quantity_ticks.into());
        order.insert(
            "orderType".into(),
            match intent.order_type {
                OrderType::Market => "MKT",
                OrderType::Limit => "LMT",
            }
            .into(),
        );
        order.insert(
            "tif".into(),
            match intent.time_in_force {
                insider_broker_api::TimeInForce::Day => "DAY",
                insider_broker_api::TimeInForce::GoodTilCancel => "GTC",
                insider_broker_api::TimeInForce::ImmediateOrCancel => "IOC",
            }
            .into(),
        );
        order.insert("cOID".into(), request.client_order_id.clone().into());
        if let Some(price) = intent.limit_price_ticks {
            order.insert("price".into(), price.into());
        }
        let response = self.request(
            "POST",
            &format!("/v1/api/iserver/account/{}/orders", self.config.account_id),
            Some(
                serde_json::to_vec(&serde_json::json!({ "orders": [order] }))
                    .map_err(|_| "IBKR order JSON encoding failed".to_owned())?,
            ),
        )?;
        let broker_id = response
            .get("order_id")
            .or_else(|| response.get("orderId"))
            .and_then(value_as_string)
            .or_else(|| {
                response
                    .as_array()
                    .and_then(|items| items.first())
                    .and_then(|item| item.get("order_id").or_else(|| item.get("orderId")))
                    .and_then(value_as_string)
            })
            .ok_or_else(|| "IBKR order response did not contain an order ID".to_owned())?;
        self.broker_order_ids
            .lock()
            .map_err(|_| "IBKR order map poisoned".to_owned())?
            .insert(request.client_order_id, broker_id);
        Ok(())
    }

    fn query_order(&self, client_order_id: &str) -> Result<Option<BrokerEvent>, String> {
        for order in self.orders()? {
            if let Some((broker_id, event, _)) = Self::order_event(&order)
                && callback_client_id(&event) == client_order_id
            {
                self.broker_order_ids
                    .lock()
                    .map_err(|_| "IBKR order map poisoned".to_owned())?
                    .insert(client_order_id.to_owned(), broker_id);
                return Ok(Some(event));
            }
        }
        for trade in self.trades()? {
            if let Some((broker_id, event, _)) = Self::trade_event(&trade)
                && callback_client_id(&event) == client_order_id
            {
                self.broker_order_ids
                    .lock()
                    .map_err(|_| "IBKR order map poisoned".to_owned())?
                    .insert(client_order_id.to_owned(), broker_id);
                return Ok(Some(event));
            }
        }
        Ok(None)
    }

    fn cancel_order(&self, client_order_id: &str) -> Result<(), String> {
        let broker_id = self.broker_order_id(client_order_id)?;
        self.request(
            "DELETE",
            &format!(
                "/v1/api/iserver/account/{}/order/{}",
                self.config.account_id, broker_id
            ),
            None,
        )?;
        Ok(())
    }

    fn replace_order(
        &self,
        client_order_id: &str,
        quantity_ticks: i64,
        limit_price_ticks: Option<i64>,
    ) -> Result<(), String> {
        let broker_id = self.broker_order_id(client_order_id)?;
        let mut body = serde_json::Map::new();
        body.insert("quantity".into(), quantity_ticks.into());
        if let Some(price) = limit_price_ticks {
            body.insert("price".into(), price.into());
        }
        self.request(
            "POST",
            &format!(
                "/v1/api/iserver/account/{}/order/{}",
                self.config.account_id, broker_id
            ),
            Some(
                serde_json::to_vec(&body)
                    .map_err(|_| "IBKR replacement JSON encoding failed".to_owned())?,
            ),
        )?;
        Ok(())
    }

    fn snapshot(&self) -> Result<BrokerSnapshot, String> {
        let mut snapshot = BrokerSnapshot::default();
        for order in self.orders()? {
            if let Some((broker_id, event, filled)) = Self::order_event(&order) {
                let client_id = callback_client_id(&event).to_owned();
                self.broker_order_ids
                    .lock()
                    .map_err(|_| "IBKR order map poisoned".to_owned())?
                    .insert(client_id.clone(), broker_id);
                snapshot
                    .orders
                    .push(insider_broker_api::BrokerOrderSnapshot {
                        client_order_id: client_id,
                        event,
                        filled_quantity_ticks: filled,
                    });
            }
        }
        for trade in self.trades()? {
            if let Some((broker_id, event, quantity)) = Self::trade_event(&trade) {
                let client_id = callback_client_id(&event).to_owned();
                self.broker_order_ids
                    .lock()
                    .map_err(|_| "IBKR order map poisoned".to_owned())?
                    .insert(client_id.clone(), broker_id);
                if let Some(existing) = snapshot
                    .orders
                    .iter_mut()
                    .find(|order| order.client_order_id == client_id)
                {
                    existing.event = event;
                    existing.filled_quantity_ticks = Some(quantity);
                } else {
                    snapshot
                        .orders
                        .push(insider_broker_api::BrokerOrderSnapshot {
                            client_order_id: client_id,
                            event,
                            filled_quantity_ticks: Some(quantity),
                        });
                }
            }
        }
        let positions = self.request(
            "GET",
            &format!("/v1/api/portfolio/{}/positions/0", self.config.account_id),
            None,
        )?;
        if let Some(items) = positions.as_array() {
            for item in items {
                let Some(conid) = item.get("conid").and_then(value_as_i64) else {
                    continue;
                };
                let quantity = item
                    .get("position")
                    .and_then(value_as_i64)
                    .ok_or_else(|| "IBKR position missing quantity".to_owned())?;
                snapshot
                    .positions
                    .push(insider_broker_api::BrokerPositionSnapshot {
                        instrument_id: insider_common_types::InstrumentId::new(
                            u128::try_from(conid)
                                .map_err(|_| "IBKR conid is negative".to_owned())?,
                        )
                        .map_err(|_| "IBKR conid is invalid".to_owned())?,
                        quantity_ticks: quantity,
                    });
            }
        }
        let accounts = self.request(
            "GET",
            &format!("/v1/api/portfolio/{}/accounts", self.config.account_id),
            None,
        )?;
        if let Some(item) = accounts.as_array().and_then(|items| items.first())
            && let Some(cash) = item
                .get("totalcashvalue")
                .or_else(|| item.get("cashbalance"))
                .and_then(value_as_i64)
        {
            snapshot.account_values.insert(
                insider_broker_api::ACCOUNT_VALUE_CASH_TICKS.into(),
                i128::from(cash),
            );
        }
        if !snapshot
            .account_values
            .contains_key(insider_broker_api::ACCOUNT_VALUE_CASH_TICKS)
        {
            return Err("IBKR account snapshot missing cash value".into());
        }
        Ok(snapshot)
    }
}

fn value_as_string(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .or_else(|| value.as_i64().map(|number| number.to_string()))
}

fn value_as_i64(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| {
            value
                .as_f64()
                .and_then(|number| number.is_finite().then(|| number.trunc()))
                .and_then(float_to_i64)
        })
        .or_else(|| {
            value.as_str().and_then(|text| {
                text.parse::<i64>().ok().or_else(|| {
                    text.parse::<f64>()
                        .ok()
                        .filter(|number| number.is_finite())
                        .map(f64::trunc)
                        .and_then(float_to_i64)
                })
            })
        })
}

fn snapshot_field(item: &serde_json::Value, key: &str) -> Option<f64> {
    let value = item.get(key)?;
    let value = value
        .get("value")
        .or_else(|| value.get("raw"))
        .unwrap_or(value);
    value_as_f64(value)
}

fn value_as_f64(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .filter(|number| number.is_finite())
        .or_else(|| {
            value
                .as_i64()
                .and_then(|number| number.to_string().parse::<f64>().ok())
                .filter(|number| number.is_finite())
        })
        .or_else(|| {
            value
                .as_str()
                .and_then(|text| text.parse::<f64>().ok())
                .filter(|number| number.is_finite())
        })
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn float_to_i64(value: f64) -> Option<i64> {
    (value >= i64::MIN as f64 && value <= i64::MAX as f64).then_some(value as i64)
}

/// Adapter session health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionState {
    /// No authenticated socket/session exists.
    Disconnected,
    /// Connection/authentication is in progress.
    Connecting,
    /// Requests and callbacks are accepted.
    Ready,
    /// Transport is connected but reconciliation is required before sends.
    Degraded,
}

/// Request sent to an injected IBKR client transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IbkrOrderRequest {
    /// Stable local client order ID used for callback correlation.
    pub client_order_id: String,
    /// Broker-neutral order intent.
    pub intent: OrderIntent,
}

/// Transport boundary for an IBKR API client or deterministic fake.
pub trait IbkrTransport: Send + Sync {
    /// Sends one order request to IBKR.
    ///
    /// # Errors
    /// Returns a transport/API diagnostic. The caller must reconcile before retrying.
    fn place_order(&self, request: IbkrOrderRequest) -> Result<(), String>;
    /// Requests authoritative state for one client order.
    ///
    /// # Errors
    /// Returns a transport/API diagnostic.
    fn query_order(&self, client_order_id: &str) -> Result<Option<BrokerEvent>, String>;
    /// Sends cancellation for one client order.
    ///
    /// # Errors
    /// Returns a transport/API diagnostic.
    fn cancel_order(&self, client_order_id: &str) -> Result<(), String>;
    /// Sends a quantity/limit replacement.
    ///
    /// # Errors
    /// Returns a transport/API diagnostic.
    fn replace_order(
        &self,
        client_order_id: &str,
        quantity_ticks: i64,
        limit_price_ticks: Option<i64>,
    ) -> Result<(), String>;

    /// Requests an authoritative account snapshot from IBKR.
    /// Implementations must populate canonical identities and cumulative fill
    /// quantities. Returning an error keeps the engine in reconciliation mode.
    fn snapshot(&self) -> Result<BrokerSnapshot, String>;

    /// Requests an authoritative market-data snapshot when the transport
    /// supports IBKR market-data endpoints.
    fn market_snapshot(&self, _conid: i64) -> Result<IbkrQuoteSnapshot, String> {
        Err("IBKR market-data snapshots are unsupported by this transport".into())
    }
}

struct AdapterState {
    session: SessionState,
    accepted: BTreeMap<String, OrderIntent>,
    pending: BTreeSet<String>,
    callbacks: VecDeque<BrokerEvent>,
}

/// IBKR gateway with explicit session gating and callback deduplication.
pub struct IbkrGateway<T> {
    transport: T,
    capabilities: Capabilities,
    state: Mutex<AdapterState>,
}

impl<T: IbkrTransport> IbkrGateway<T> {
    /// Creates a disconnected adapter. `connect_ready` must be called only
    /// after the underlying IBKR session has authenticated successfully.
    #[must_use]
    pub fn new(transport: T, capabilities: Capabilities) -> Self {
        Self {
            transport,
            capabilities,
            state: Mutex::new(AdapterState {
                session: SessionState::Disconnected,
                accepted: BTreeMap::new(),
                pending: BTreeSet::new(),
                callbacks: VecDeque::new(),
            }),
        }
    }

    /// Marks the adapter as connecting.
    pub fn begin_connect(&self) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "adapter state poisoned".to_owned())?;
        if state.session == SessionState::Ready {
            return Err("already connected".into());
        }
        state.session = SessionState::Connecting;
        Ok(())
    }

    /// Publishes a successfully authenticated session.
    pub fn connect_ready(&self) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "adapter state poisoned".to_owned())?;
        state.session = SessionState::Ready;
        Ok(())
    }

    /// Marks the session degraded; new sends are blocked until reconciliation.
    pub fn mark_degraded(&self) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "adapter state poisoned".to_owned())?;
        state.session = SessionState::Degraded;
        Ok(())
    }

    /// Marks the transport disconnected without discarding accepted IDs.
    pub fn disconnect(&self) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "adapter state poisoned".to_owned())?;
        state.session = SessionState::Disconnected;
        Ok(())
    }

    /// Returns current session state.
    pub fn session_state(&self) -> Result<SessionState, String> {
        self.state
            .lock()
            .map(|state| state.session)
            .map_err(|_| "adapter state poisoned".into())
    }

    /// Accepts one normalized IBKR callback, ignoring exact duplicates and
    /// retaining the event for the engine's event loop.
    pub fn on_callback(&self, event: BrokerEvent) -> Result<bool, String> {
        let client = callback_client_id(&event);
        if client.trim().is_empty() {
            return Err("callback client order ID is empty".into());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| "adapter state poisoned".to_owned())?;
        if state.callbacks.iter().any(|existing| existing == &event) {
            return Ok(false);
        }
        if state.callbacks.len() >= MAX_CALLBACK_QUEUE {
            return Err("broker callback queue is full; reconciliation required".into());
        }
        state.callbacks.push_back(event);
        Ok(true)
    }

    /// Drains callbacks in arrival order for the engine journal/reconciliation loop.
    pub fn drain_callbacks(&self) -> Result<Vec<BrokerEvent>, String> {
        self.state
            .lock()
            .map(|mut state| state.callbacks.drain(..).collect())
            .map_err(|_| "adapter state poisoned".into())
    }
}

impl<T: IbkrTransport> BrokerGateway for IbkrGateway<T> {
    fn health(&self) -> BrokerHealth {
        match self.session_state() {
            Ok(SessionState::Ready) => BrokerHealth::Healthy,
            Ok(SessionState::Degraded) => BrokerHealth::Degraded,
            Ok(SessionState::Disconnected) => BrokerHealth::Unavailable,
            Ok(SessionState::Connecting) | Err(_) => BrokerHealth::Unknown,
        }
    }
    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    fn send(&self, intent: &OrderIntent) -> Result<(), String> {
        validate_client_order_id(&intent.client_order_id)?;
        if intent.quantity_ticks <= 0 {
            return Err("order quantity must be positive".into());
        }
        match intent.order_type {
            OrderType::Market if !self.capabilities.market => {
                return Err("broker does not support market orders".into());
            }
            OrderType::Limit if !self.capabilities.limit => {
                return Err("broker does not support limit orders".into());
            }
            OrderType::Limit if intent.limit_price_ticks.is_none_or(|price| price <= 0) => {
                return Err("limit order price must be positive".into());
            }
            OrderType::Market if intent.limit_price_ticks.is_some() => {
                return Err("market orders cannot carry a limit price".into());
            }
            _ => {}
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| "adapter state poisoned".to_owned())?;
        if state.session != SessionState::Ready {
            return Err(format!("session not ready: {:?}", state.session));
        }
        if let Some(existing) = state.accepted.get(&intent.client_order_id) {
            if existing == intent {
                return Ok(());
            }
            return Err("client order ID reused with different intent".into());
        }
        if !state.pending.insert(intent.client_order_id.clone()) {
            return Err("client order ID send already in flight".into());
        }
        drop(state);
        let transport_result = self.transport.place_order(IbkrOrderRequest {
            client_order_id: intent.client_order_id.clone(),
            intent: intent.clone(),
        });
        let mut state = self
            .state
            .lock()
            .map_err(|_| "adapter state poisoned".to_owned())?;
        state.pending.remove(&intent.client_order_id);
        if let Err(error) = transport_result {
            // A transport error is ambiguous: IBKR may have accepted the
            // request before the client observed the failure. Stop further
            // mutation until the engine performs reconciliation.
            state.session = SessionState::Degraded;
            return Err(error);
        }
        state
            .accepted
            .insert(intent.client_order_id.clone(), intent.clone());
        Ok(())
    }

    fn reconcile(&self, client_order_id: &str) -> Result<Option<BrokerEvent>, String> {
        if client_order_id.trim().is_empty() {
            return Err("client order ID is empty".into());
        }
        let event = self.transport.query_order(client_order_id)?;
        if let Some(event) = event.clone() {
            self.on_callback(event)?;
        }
        Ok(event)
    }

    fn snapshot(&self) -> Result<BrokerSnapshot, String> {
        let snapshot = self.transport.snapshot()?;
        validate_snapshot(&snapshot)?;
        Ok(snapshot)
    }

    fn cancel(&self, client_order_id: &str) -> Result<(), String> {
        validate_client_order_id(client_order_id)?;
        if !self.capabilities.cancel_replace {
            return Err("broker does not support cancel/replace".into());
        }
        self.require_ready()?;
        match self.transport.cancel_order(client_order_id) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.mark_degraded()?;
                Err(error)
            }
        }
    }

    fn replace(
        &self,
        client_order_id: &str,
        quantity_ticks: i64,
        limit_price_ticks: Option<i64>,
    ) -> Result<(), String> {
        validate_client_order_id(client_order_id)?;
        if !self.capabilities.cancel_replace {
            return Err("broker does not support cancel/replace".into());
        }
        if quantity_ticks <= 0 || limit_price_ticks.is_some_and(|price| price <= 0) {
            return Err("replacement quantity and price must be positive".into());
        }
        self.require_ready()?;
        match self
            .transport
            .replace_order(client_order_id, quantity_ticks, limit_price_ticks)
        {
            Ok(()) => Ok(()),
            Err(error) => {
                self.mark_degraded()?;
                Err(error)
            }
        }
    }
}

impl<T: IbkrTransport> IbkrGateway<T> {
    /// Fetches a provider-authoritative market snapshot after session gating.
    pub fn market_snapshot(&self, conid: i64) -> Result<IbkrQuoteSnapshot, String> {
        self.require_ready()?;
        self.transport.market_snapshot(conid)
    }

    fn require_ready(&self) -> Result<(), String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "adapter state poisoned".to_owned())?;
        if state.session != SessionState::Ready {
            return Err(format!("session not ready: {:?}", state.session));
        }
        Ok(())
    }
}

fn validate_client_order_id(client_order_id: &str) -> Result<(), String> {
    if client_order_id.trim().is_empty() || client_order_id.len() > 256 {
        return Err("client order ID is invalid".into());
    }
    Ok(())
}

fn callback_client_id(event: &BrokerEvent) -> &str {
    match event {
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
    }
}

/// Validates a broker snapshot before it reaches portfolio or reconciliation
/// state. Incomplete responses gate trading instead of being interpreted as a
/// flat account.
fn validate_snapshot(snapshot: &BrokerSnapshot) -> Result<(), String> {
    let mut order_ids = BTreeSet::new();
    for order in &snapshot.orders {
        if order.client_order_id.trim().is_empty()
            || !order_ids.insert(order.client_order_id.clone())
        {
            return Err("snapshot contains a blank or duplicate client order ID".into());
        }
        if order
            .filled_quantity_ticks
            .is_some_and(|quantity| quantity < 0)
        {
            return Err("snapshot contains a negative cumulative fill quantity".into());
        }
        if callback_client_id(&order.event) != order.client_order_id {
            return Err("snapshot order event/client correlation mismatch".into());
        }
    }
    let mut instruments = BTreeSet::new();
    for position in &snapshot.positions {
        if !instruments.insert(position.instrument_id) {
            return Err("snapshot contains duplicate instrument positions".into());
        }
    }
    if snapshot
        .account_values
        .keys()
        .any(|key| key.trim().is_empty())
    {
        return Err("snapshot contains a blank account value key".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{
        IbkrGateway, IbkrOrderRequest, IbkrTransport, MAX_CALLBACK_QUEUE, MAX_HTTP_RESPONSE_BYTES,
        SessionState, read_bounded_response, validate_https_url,
    };
    use insider_broker_api::{
        BrokerEvent, BrokerGateway, BrokerSnapshot, Capabilities, OrderIntent, OrderState,
        OrderType, Side, TimeInForce,
    };
    use insider_common_types::{AccountId, InstrumentId, TraceId};

    struct FakeTransport {
        placed: Mutex<Vec<IbkrOrderRequest>>,
        snapshot: BrokerSnapshot,
    }

    impl IbkrTransport for FakeTransport {
        fn place_order(&self, request: IbkrOrderRequest) -> Result<(), String> {
            self.placed
                .lock()
                .map_err(|_| "poisoned".to_owned())?
                .push(request);
            Ok(())
        }
        fn query_order(&self, _client_order_id: &str) -> Result<Option<BrokerEvent>, String> {
            Ok(None)
        }
        fn cancel_order(&self, _client_order_id: &str) -> Result<(), String> {
            Ok(())
        }
        fn replace_order(
            &self,
            _client_order_id: &str,
            _quantity_ticks: i64,
            _limit_price_ticks: Option<i64>,
        ) -> Result<(), String> {
            Ok(())
        }
        fn snapshot(&self) -> Result<BrokerSnapshot, String> {
            Ok(self.snapshot.clone())
        }
    }

    fn intent() -> Option<OrderIntent> {
        Some(OrderIntent {
            intent_id: "intent-1".into(),
            account_id: AccountId::new(1).ok()?,
            instrument_id: InstrumentId::new(2).ok()?,
            client_order_id: "client-1".into(),
            side: Side::Buy,
            quantity_ticks: 10,
            order_type: OrderType::Market,
            limit_price_ticks: None,
            time_in_force: TimeInForce::Day,
            state: OrderState::RiskApproved,
            trace_id: TraceId::new(3).ok()?,
        })
    }

    #[test]
    fn session_gates_send_and_retries_are_idempotent() {
        let transport = FakeTransport {
            placed: Mutex::new(Vec::new()),
            snapshot: BrokerSnapshot::default(),
        };
        let gateway = IbkrGateway::new(
            transport,
            Capabilities {
                market: true,
                limit: true,
                fractional_quantity: false,
                cancel_replace: true,
            },
        );
        assert_eq!(
            gateway.session_state().ok(),
            Some(SessionState::Disconnected)
        );
        let Some(order) = intent() else { return };
        assert!(gateway.send(&order).is_err());
        assert!(gateway.connect_ready().is_ok());
        assert!(gateway.send(&order).is_ok());
        assert!(gateway.send(&order).is_ok());
        let callback = BrokerEvent::Acknowledged {
            client_order_id: "client-1".into(),
            broker_order_id: "ib-1".into(),
        };
        assert_eq!(gateway.on_callback(callback.clone()).ok(), Some(true));
        assert_eq!(gateway.on_callback(callback).ok(), Some(false));
        assert_eq!(
            gateway.drain_callbacks().ok().map(|events| events.len()),
            Some(1)
        );
    }

    #[test]
    fn callback_queue_is_bounded_and_fails_closed() {
        let transport = FakeTransport {
            placed: Mutex::new(Vec::new()),
            snapshot: BrokerSnapshot::default(),
        };
        let gateway = IbkrGateway::new(
            transport,
            Capabilities {
                market: true,
                limit: true,
                fractional_quantity: false,
                cancel_replace: true,
            },
        );
        for index in 0..MAX_CALLBACK_QUEUE {
            assert!(
                gateway
                    .on_callback(BrokerEvent::Cancelled {
                        client_order_id: format!("client-{index}"),
                    })
                    .is_ok()
            );
        }
        assert!(
            gateway
                .on_callback(BrokerEvent::Cancelled {
                    client_order_id: "overflow".into(),
                })
                .is_err()
        );
    }

    #[test]
    fn capability_matrix_rejects_unsupported_requests_before_session_or_transport() {
        let transport = FakeTransport {
            placed: Mutex::new(Vec::new()),
            snapshot: BrokerSnapshot::default(),
        };
        let gateway = IbkrGateway::new(
            transport,
            Capabilities {
                market: false,
                limit: true,
                fractional_quantity: false,
                cancel_replace: false,
            },
        );
        let Some(mut order) = intent() else { return };
        assert!(matches!(
            gateway.send(&order),
            Err(error) if error == "broker does not support market orders"
        ));
        order.order_type = OrderType::Limit;
        order.limit_price_ticks = Some(100);
        assert!(matches!(
            gateway.cancel("client-1"),
            Err(error) if error == "broker does not support cancel/replace"
        ));
        assert!(matches!(
            gateway.replace("client-1", 10, Some(101)),
            Err(error) if error == "broker does not support cancel/replace"
        ));
    }

    #[test]
    fn ibkr_response_reader_enforces_bound_without_full_buffering() {
        assert!(
            read_bounded_response(std::io::Cursor::new(vec![
                0_u8;
                MAX_HTTP_RESPONSE_BYTES + 1
            ]))
            .is_err()
        );
        let exact =
            read_bounded_response(std::io::Cursor::new(vec![1_u8; MAX_HTTP_RESPONSE_BYTES]));
        assert_eq!(
            exact.ok().map(|body| body.len()),
            Some(MAX_HTTP_RESPONSE_BYTES)
        );
    }

    #[test]
    fn ibkr_urls_require_bounded_https_authority() {
        assert!(validate_https_url("https://127.0.0.1:5000/v1", "url").is_ok());
        assert!(validate_https_url("http://127.0.0.1:5000/v1", "url").is_err());
        assert!(validate_https_url("https:///missing-authority", "url").is_err());
        assert!(validate_https_url("https://provider.example/a b", "url").is_err());
        assert!(validate_https_url(&format!("https://{}", "x".repeat(2_050)), "url").is_err());
    }
}
