//! Deterministic market-data provider adapters.
//!
//! The file adapter is intentionally strict and bounded. It is useful for
//! replay, paper trading, and fixture ingestion, while live adapters can feed
//! the same [`insider_market_data::MarketEvent`] contract without changing the
//! hot path.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use insider_common_types::{InstrumentId, MonoTime, WallTime};
use insider_market_data::{Bar, BookDelta, BookSide, MarketEvent, Quote, Trade};

const MAX_LINE_BYTES: usize = 4_096;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_TIMEOUT_MS: u64 = 120_000;

fn valid_https_base_url(value: &str) -> bool {
    let authority = value
        .strip_prefix("https://")
        .and_then(|rest| rest.split(['/', '?', '#']).next())
        .unwrap_or_default();
    value.len() <= 2_048
        && !value.chars().any(char::is_whitespace)
        && !authority.is_empty()
        && !authority.contains('@')
        && reqwest::Url::parse(value).is_ok_and(|url| {
            url.scheme() == "https"
                && url.host_str().is_some_and(|host| !host.is_empty())
                && url.username().is_empty()
                && url.password().is_none()
        })
}

/// HTTP request used by market-data adapters.
#[derive(Clone, Eq, PartialEq)]
pub struct HttpRequest {
    /// HTTP method.
    pub method: String,
    /// Fully encoded HTTPS URL.
    pub url: String,
    /// Request headers.
    pub headers: BTreeMap<String, String>,
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .field("header_names", &self.headers.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// HTTP response returned by a market-data transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response headers.
    pub headers: BTreeMap<String, String>,
    /// Bounded response body.
    pub body: Vec<u8>,
}

/// Transport boundary for live or deterministic market-data adapters.
pub trait HttpTransport: Send + Sync {
    /// Performs one bounded request.
    ///
    /// # Errors
    /// Returns a transport diagnostic when the request cannot be completed.
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, String>;
}

/// Production TLS transport for market-data HTTP endpoints.
pub struct ReqwestHttpTransport {
    client: reqwest::blocking::Client,
}

impl ReqwestHttpTransport {
    /// Builds an HTTPS-only client with bounded timeouts.
    ///
    /// # Errors
    /// Returns a diagnostic when the timeout is outside bounds or the TLS
    /// client cannot be constructed.
    pub fn new(timeout_ms: u64) -> Result<Self, String> {
        if timeout_ms == 0 || timeout_ms > MAX_TIMEOUT_MS {
            return Err("market HTTP timeout is outside bounds".into());
        }
        let timeout = std::time::Duration::from_millis(timeout_ms);
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("market HTTP client: {error}"))?;
        Ok(Self { client })
    }
}

impl HttpTransport for ReqwestHttpTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        if request.method != "GET" || !valid_https_base_url(&request.url) {
            return Err("market HTTP transport requires HTTPS GET".into());
        }
        let mut builder = self.client.get(&request.url);
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        let response = builder
            .send()
            .map_err(|error| format!("transport: {error}"))?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        {
            return Err("market response exceeds bound".into());
        }
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect::<BTreeMap<_, _>>();
        let body = read_bounded_body(response)?;
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

fn read_bounded_body(reader: impl Read) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    reader
        .take((MAX_RESPONSE_BYTES as u64).saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|error| format!("transport body: {error}"))?;
    if body.len() > MAX_RESPONSE_BYTES {
        return Err("market response exceeds bound".into());
    }
    Ok(body)
}

/// One normalized Yahoo Finance chart candle in canonical integer ticks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct YahooBar {
    /// Canonical instrument identity.
    pub bar: Bar,
    /// Provider sequence derived from ascending chart timestamp.
    pub sequence: u64,
}

/// Yahoo Finance `/v8/finance/chart/{symbol}` adapter for historical bars.
pub struct YahooChartProvider<T> {
    transport: T,
    base_url: String,
    symbol: String,
    instrument_id: InstrumentId,
    interval: String,
    range: String,
    interval_ns: u64,
    price_scale: i64,
    next_sequence: Mutex<u64>,
}

/// Immutable Yahoo chart adapter configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct YahooChartConfig {
    /// HTTPS endpoint root.
    pub base_url: String,
    /// Provider symbol.
    pub symbol: String,
    /// Canonical instrument identity.
    pub instrument_id: InstrumentId,
    /// Yahoo interval such as `1m` or `1d`.
    pub interval: String,
    /// Yahoo range such as `1d` or `1y`.
    pub range: String,
    /// Canonical bar width.
    pub interval_ns: u64,
    /// Decimal price multiplier used to create integer ticks.
    pub price_scale: i64,
}

/// One normalized Yahoo Finance quote in canonical integer ticks.
pub struct YahooQuoteProvider<T> {
    transport: T,
    base_url: String,
    symbol: String,
    instrument_id: InstrumentId,
    price_scale: i64,
    next_sequence: Mutex<u64>,
}

/// Immutable Yahoo quote adapter configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct YahooQuoteConfig {
    /// HTTPS endpoint root.
    pub base_url: String,
    /// Provider symbol.
    pub symbol: String,
    /// Canonical instrument identity.
    pub instrument_id: InstrumentId,
    /// Decimal price multiplier used to create integer ticks.
    pub price_scale: i64,
}

impl<T: HttpTransport> YahooChartProvider<T> {
    /// Creates a chart adapter. Prices are converted to integer ticks using
    /// `price_scale`; no floating-point value crosses the provider boundary.
    ///
    /// # Errors
    /// Returns a diagnostic when endpoint, symbol, interval, range, or tick
    /// conversion bounds are invalid.
    pub fn new(transport: T, config: YahooChartConfig) -> Result<Self, String> {
        let YahooChartConfig {
            base_url: configured_base_url,
            symbol: configured_symbol,
            instrument_id,
            interval: configured_interval,
            range: configured_range,
            interval_ns,
            price_scale,
        } = config;
        let base_url = configured_base_url.trim_end_matches('/').to_owned();
        let symbol = configured_symbol.trim().to_ascii_uppercase();
        let interval = configured_interval.trim().to_owned();
        let range = configured_range.trim().to_owned();
        if !valid_https_base_url(&base_url)
            || symbol.is_empty()
            || interval.is_empty()
            || range.is_empty()
            || interval_ns == 0
            || price_scale <= 0
        {
            return Err("invalid Yahoo chart configuration".into());
        }
        Ok(Self {
            transport,
            base_url,
            symbol,
            instrument_id,
            interval,
            range,
            interval_ns,
            price_scale,
            next_sequence: Mutex::new(0),
        })
    }

    /// Fetches and normalizes one bounded historical chart response.
    ///
    /// # Errors
    /// Returns a transport, HTTP status, JSON-shape, timestamp, or OHLCV
    /// invariant diagnostic.
    pub fn fetch(&self) -> Result<Vec<YahooBar>, String> {
        let url = format!(
            "{}/v8/finance/chart/{}?interval={}&range={}",
            self.base_url,
            encode_component(&self.symbol),
            encode_component(&self.interval),
            encode_component(&self.range)
        );
        let mut headers = BTreeMap::new();
        headers.insert("accept".into(), "application/json".into());
        let response = self.transport.send(HttpRequest {
            method: "GET".into(),
            url,
            headers,
        })?;
        if !(200..300).contains(&response.status) {
            return Err(format!("Yahoo chart HTTP status {}", response.status));
        }
        if response.body.len() > MAX_RESPONSE_BYTES {
            return Err("Yahoo chart response exceeds bound".into());
        }
        let mut bars = parse_yahoo_chart(
            &response.body,
            self.instrument_id,
            self.interval_ns,
            self.price_scale,
        )?;
        let mut next_sequence = self
            .next_sequence
            .lock()
            .map_err(|_| "Yahoo sequence state poisoned".to_owned())?;
        for bar in &mut bars {
            *next_sequence = next_sequence
                .checked_add(1)
                .ok_or("Yahoo sequence overflow".to_owned())?;
            bar.sequence = *next_sequence;
        }
        Ok(bars)
    }
}

impl<T: HttpTransport> YahooQuoteProvider<T> {
    /// Creates a bounded Yahoo quote adapter.
    ///
    /// # Errors
    /// Returns an error when the endpoint, symbol, or tick scale is invalid.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(transport: T, config: YahooQuoteConfig) -> Result<Self, String> {
        let base_url = config.base_url.trim_end_matches('/').to_owned();
        let symbol = config.symbol.trim().to_ascii_uppercase();
        if !valid_https_base_url(&base_url) || symbol.is_empty() || config.price_scale <= 0 {
            return Err("invalid Yahoo quote configuration".into());
        }
        Ok(Self {
            transport,
            base_url,
            symbol,
            instrument_id: config.instrument_id,
            price_scale: config.price_scale,
            next_sequence: Mutex::new(0),
        })
    }

    /// Fetches and normalizes one quote response.
    ///
    /// # Errors
    /// Returns a transport, HTTP, JSON-shape, price, spread, or sequence
    /// validation error.
    pub fn fetch(
        &self,
        received_mono: MonoTime,
        fallback_exchange_time: WallTime,
    ) -> Result<Quote, String> {
        let url = format!(
            "{}/v7/finance/quote?symbols={}",
            self.base_url,
            encode_component(&self.symbol)
        );
        let mut headers = BTreeMap::new();
        headers.insert("accept".into(), "application/json".into());
        let response = self.transport.send(HttpRequest {
            method: "GET".into(),
            url,
            headers,
        })?;
        if !(200..300).contains(&response.status) {
            return Err(format!("Yahoo quote HTTP status {}", response.status));
        }
        if response.body.len() > MAX_RESPONSE_BYTES {
            return Err("Yahoo quote response exceeds bound".into());
        }
        let root: serde_json::Value = serde_json::from_slice(&response.body)
            .map_err(|_| "invalid Yahoo quote JSON".to_owned())?;
        let item = root
            .get("quoteResponse")
            .and_then(|value| value.get("result"))
            .and_then(serde_json::Value::as_array)
            .and_then(|values| values.first())
            .ok_or_else(|| "Yahoo quote result missing".to_owned())?;
        let last = item
            .get("regularMarketPrice")
            .and_then(|value| json_price(value, self.price_scale))
            .ok_or_else(|| "Yahoo quote price missing".to_owned())?;
        let bid = item
            .get("bid")
            .and_then(|value| json_price(value, self.price_scale))
            .unwrap_or(last);
        let ask = item
            .get("ask")
            .and_then(|value| json_price(value, self.price_scale))
            .unwrap_or(last);
        if bid <= 0 || ask <= 0 || bid > ask {
            return Err("Yahoo quote spread is invalid".into());
        }
        let exchange_seconds = item
            .get("regularMarketTime")
            .and_then(serde_json::Value::as_i64)
            .filter(|seconds| *seconds > 0);
        let exchange_time = exchange_seconds
            .and_then(|seconds| seconds.checked_mul(1_000_000_000))
            .map_or(fallback_exchange_time, WallTime::from_unix_nanos);
        let mut sequence = self
            .next_sequence
            .lock()
            .map_err(|_| "Yahoo quote sequence state poisoned".to_owned())?;
        let timestamp_sequence = exchange_seconds
            .and_then(|seconds| u64::try_from(seconds).ok())
            .filter(|seconds| *seconds > 0);
        let next = timestamp_sequence.map_or_else(
            || sequence.checked_add(1),
            |seconds| Some(seconds.max(sequence.saturating_add(1))),
        );
        *sequence = next.ok_or_else(|| "Yahoo quote sequence overflow".to_owned())?;
        Ok(Quote {
            instrument_id: self.instrument_id,
            sequence: *sequence,
            exchange_time,
            received_mono,
            bid_ticks: bid,
            ask_ticks: ask,
            bid_quantity_ticks: 1,
            ask_quantity_ticks: 1,
        })
    }
}

fn parse_yahoo_chart(
    body: &[u8],
    instrument_id: InstrumentId,
    interval_ns: u64,
    price_scale: i64,
) -> Result<Vec<YahooBar>, String> {
    let root: serde_json::Value = serde_json::from_slice(body).map_err(|_| "invalid Yahoo JSON")?;
    let result = root
        .get("chart")
        .and_then(|value| value.get("result"))
        .and_then(serde_json::Value::as_array)
        .and_then(|values| values.first())
        .ok_or("Yahoo chart result missing")?;
    let timestamps = result
        .get("timestamp")
        .and_then(serde_json::Value::as_array)
        .ok_or("Yahoo chart timestamps missing")?;
    let quote = result
        .get("indicators")
        .and_then(|value| value.get("quote"))
        .and_then(serde_json::Value::as_array)
        .and_then(|values| values.first())
        .ok_or("Yahoo chart quote missing")?;
    let open = quote
        .get("open")
        .and_then(serde_json::Value::as_array)
        .ok_or("Yahoo open missing")?;
    let high = quote
        .get("high")
        .and_then(serde_json::Value::as_array)
        .ok_or("Yahoo high missing")?;
    let low = quote
        .get("low")
        .and_then(serde_json::Value::as_array)
        .ok_or("Yahoo low missing")?;
    let close = quote
        .get("close")
        .and_then(serde_json::Value::as_array)
        .ok_or("Yahoo close missing")?;
    let volume = quote
        .get("volume")
        .and_then(serde_json::Value::as_array)
        .ok_or("Yahoo volume missing")?;
    let lengths = [
        timestamps.len(),
        open.len(),
        high.len(),
        low.len(),
        close.len(),
        volume.len(),
    ];
    if lengths.iter().any(|length| *length != lengths[0]) {
        return Err("Yahoo chart arrays have different lengths".into());
    }
    let mut bars = Vec::new();
    let mut previous_timestamp = None;
    for index in 0..timestamps.len() {
        let timestamp = timestamps[index]
            .as_i64()
            .ok_or("Yahoo timestamp invalid")?;
        if timestamp <= 0 || previous_timestamp.is_some_and(|previous| timestamp <= previous) {
            return Err("Yahoo timestamps are not strictly increasing".into());
        }
        previous_timestamp = Some(timestamp);
        let Some(open) = json_price(&open[index], price_scale) else {
            continue;
        };
        let Some(high) = json_price(&high[index], price_scale) else {
            continue;
        };
        let Some(low) = json_price(&low[index], price_scale) else {
            continue;
        };
        let Some(close) = json_price(&close[index], price_scale) else {
            continue;
        };
        let volume = volume[index].as_i64().ok_or("Yahoo volume invalid")?;
        if high < low || open < low || open > high || close < low || close > high || volume <= 0 {
            return Err("Yahoo candle violates OHLCV invariants".into());
        }
        let start_ns = timestamp
            .checked_mul(1_000_000_000)
            .ok_or("Yahoo timestamp overflow")?;
        bars.push(YahooBar {
            bar: Bar {
                instrument_id,
                start_time: WallTime::from_unix_nanos(start_ns),
                interval_ns,
                open_ticks: open,
                high_ticks: high,
                low_ticks: low,
                close_ticks: close,
                volume_ticks: volume,
            },
            sequence: 0,
        });
    }
    Ok(bars)
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn json_price(value: &serde_json::Value, scale: i64) -> Option<i64> {
    let price = value.as_f64()?;
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    let scaled = price * scale as f64;
    if !scaled.is_finite() || scaled > i64::MAX as f64 {
        return None;
    }
    Some(scaled.round() as i64)
}

fn encode_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => vec![byte as char],
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

/// Errors emitted by the strict line-oriented market feed adapter.
#[derive(Debug)]
pub enum FileProviderError {
    /// The feed could not be opened or read.
    Io(std::io::Error),
    /// A row violated the canonical feed contract.
    InvalidRow {
        /// One-based source line number.
        line: usize,
        /// Stable parse-failure category.
        reason: &'static str,
    },
    /// The configured batch or line bound was invalid.
    Bounds(&'static str),
}

impl From<std::io::Error> for FileProviderError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// One parsed feed item with its source cursor for audit and restart.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MarketRecord {
    /// Zero-based source line number.
    pub line: usize,
    /// Canonical event consumed by the runtime hub.
    pub event: MarketEvent,
}

/// Strict, restartable file market-data provider.
pub struct FileMarketProvider {
    path: PathBuf,
    next_line: usize,
    max_batch: usize,
}

impl FileMarketProvider {
    /// Opens a provider with a hard maximum records returned per call.
    ///
    /// # Errors
    /// Returns [`FileProviderError::Bounds`] when `max_batch` is zero or too
    /// large, or [`FileProviderError::Io`] when the source cannot be opened.
    pub fn open(path: impl AsRef<Path>, max_batch: usize) -> Result<Self, FileProviderError> {
        if max_batch == 0 || max_batch > 10_000 {
            return Err(FileProviderError::Bounds("max_batch"));
        }
        let path = path.as_ref().to_path_buf();
        File::open(&path)?;
        Ok(Self {
            path,
            next_line: 0,
            max_batch,
        })
    }

    /// Returns the next source cursor, which is safe to persist after the
    /// caller has durably accepted the returned records.
    #[must_use]
    pub const fn next_line(&self) -> usize {
        self.next_line
    }

    /// Restores a previously committed line cursor.
    ///
    /// # Errors
    /// Returns [`FileProviderError::Bounds`] when the cursor cannot be
    /// represented by the provider's bounded line index.
    pub fn seek_line(&mut self, line: usize) -> Result<(), FileProviderError> {
        if line > usize::MAX / 2 {
            return Err(FileProviderError::Bounds("line cursor"));
        }
        self.next_line = line;
        Ok(())
    }

    /// Reads and parses at most the configured batch size. A malformed row is
    /// returned without advancing past that row, so retry cannot silently skip
    /// source data.
    ///
    /// # Errors
    /// Returns [`FileProviderError`] for I/O, bounds, or malformed rows.
    pub fn next_batch(&mut self) -> Result<Vec<MarketRecord>, FileProviderError> {
        let file = File::open(&self.path)?;
        let mut records = Vec::with_capacity(self.max_batch);
        for (line_index, line_result) in BufReader::new(file).lines().enumerate() {
            if line_index < self.next_line {
                continue;
            }
            if records.len() == self.max_batch {
                break;
            }
            let line = line_result?;
            if line.len() > MAX_LINE_BYTES {
                return Err(FileProviderError::Bounds("feed line"));
            }
            if line.trim().is_empty() || line.trim_start().starts_with('#') {
                self.next_line = line_index + 1;
                continue;
            }
            let event = parse_row(line_index + 1, &line)?;
            records.push(MarketRecord {
                line: line_index,
                event,
            });
            self.next_line = line_index + 1;
        }
        Ok(records)
    }
}

fn parse_row(line: usize, value: &str) -> Result<MarketEvent, FileProviderError> {
    let fields: Vec<&str> = value.split(',').map(str::trim).collect();
    let kind = fields.first().copied().unwrap_or_default();
    match kind {
        "Q" if fields.len() == 9 => Ok(MarketEvent::Quote(Quote {
            instrument_id: parse_instrument(line, fields[1])?,
            sequence: parse_u64(line, fields[2], "sequence")?,
            exchange_time: WallTime::from_unix_nanos(parse_i64(line, fields[3], "exchange_time")?),
            received_mono: MonoTime::from_nanos(parse_u64(line, fields[4], "received_mono")?),
            bid_ticks: parse_i64(line, fields[5], "bid")?,
            ask_ticks: parse_i64(line, fields[6], "ask")?,
            bid_quantity_ticks: parse_i64(line, fields[7], "bid_quantity")?,
            ask_quantity_ticks: parse_i64(line, fields[8], "ask_quantity")?,
        })),
        "T" if fields.len() == 7 => Ok(MarketEvent::Trade(Trade {
            instrument_id: parse_instrument(line, fields[1])?,
            sequence: parse_u64(line, fields[2], "sequence")?,
            exchange_time: WallTime::from_unix_nanos(parse_i64(line, fields[3], "exchange_time")?),
            received_mono: MonoTime::from_nanos(parse_u64(line, fields[4], "received_mono")?),
            price_ticks: parse_i64(line, fields[5], "price")?,
            quantity_ticks: parse_i64(line, fields[6], "quantity")?,
        })),
        "B" if fields.len() == 6 => Ok(MarketEvent::Book(BookDelta {
            instrument_id: parse_instrument(line, fields[1])?,
            sequence: parse_u64(line, fields[2], "sequence")?,
            side: match fields[3] {
                "bid" | "BID" => BookSide::Bid,
                "ask" | "ASK" => BookSide::Ask,
                _ => {
                    return Err(FileProviderError::InvalidRow {
                        line,
                        reason: "book side",
                    });
                }
            },
            price_ticks: parse_i64(line, fields[4], "price")?,
            quantity_ticks: parse_i64(line, fields[5], "quantity")?,
        })),
        _ => Err(FileProviderError::InvalidRow {
            line,
            reason: "row shape",
        }),
    }
}

fn parse_instrument(line: usize, value: &str) -> Result<InstrumentId, FileProviderError> {
    value
        .parse::<u128>()
        .ok()
        .and_then(|value| InstrumentId::new(value).ok())
        .ok_or(FileProviderError::InvalidRow {
            line,
            reason: "instrument id",
        })
}

fn parse_u64(line: usize, value: &str, reason: &'static str) -> Result<u64, FileProviderError> {
    value
        .parse()
        .map_err(|_| FileProviderError::InvalidRow { line, reason })
}

fn parse_i64(line: usize, value: &str, reason: &'static str) -> Result<i64, FileProviderError> {
    value
        .parse()
        .map_err(|_| FileProviderError::InvalidRow { line, reason })
}

#[cfg(test)]
mod tests {
    use super::{
        FileMarketProvider, FileProviderError, HttpRequest, MAX_RESPONSE_BYTES, read_bounded_body,
        valid_https_base_url,
    };
    use insider_market_data::MarketEvent;
    use std::fs;

    #[test]
    fn market_provider_urls_require_authority_without_credentials() {
        assert!(valid_https_base_url("https://query.example/v8"));
        assert!(!valid_https_base_url("https:///missing"));
        assert!(!valid_https_base_url("https://user:pass@query.example"));
        assert!(!valid_https_base_url("http://query.example"));
        let oversized = format!("https://query.example/{}", "a".repeat(2_040));
        assert!(!valid_https_base_url(&oversized));
    }

    #[test]
    fn file_provider_commits_cursor_only_after_parsed_rows() {
        let path =
            std::env::temp_dir().join(format!("insider-market-feed-{}.csv", std::process::id()));
        assert!(fs::write(
            &path,
            "# kind,id,seq,time,mono,bid,ask,bid_qty,ask_qty\nQ,7,1,1000,1,99,100,2,3\nT,7,2,1000,2,100,4\n"
        )
        .is_ok());
        let Ok(mut provider) = FileMarketProvider::open(&path, 2) else {
            return;
        };
        let first = provider.next_batch().ok();
        assert_eq!(first.as_ref().map(Vec::len), Some(2));
        assert!(matches!(
            first
                .and_then(|items| items.first().copied())
                .map(|item| item.event),
            Some(MarketEvent::Quote(_))
        ));
        assert_eq!(provider.next_line(), 3);
        assert!(provider.next_batch().is_ok());
        assert!(fs::remove_file(path).is_ok());

        let invalid = FileMarketProvider::open("/does/not/exist", 1);
        assert!(matches!(invalid, Err(FileProviderError::Io(_))));
    }

    #[test]
    fn http_body_reader_stops_at_response_bound() {
        let oversized = vec![0_u8; MAX_RESPONSE_BYTES + 1];
        assert!(read_bounded_body(std::io::Cursor::new(oversized)).is_err());
        let bounded = vec![1_u8; MAX_RESPONSE_BYTES];
        assert_eq!(
            read_bounded_body(std::io::Cursor::new(bounded))
                .ok()
                .map(|body| body.len()),
            Some(MAX_RESPONSE_BYTES)
        );
    }

    #[test]
    fn request_debug_redacts_header_values() {
        let request = HttpRequest {
            method: "GET".into(),
            url: "https://market.example.test".into(),
            headers: std::collections::BTreeMap::from([(
                String::from("authorization"),
                String::from("secret"),
            )]),
        };
        let rendered = format!("{request:?}");
        assert!(rendered.contains("authorization"));
        assert!(!rendered.contains("secret"));
    }
}
