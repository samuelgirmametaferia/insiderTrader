//! Canonical market-event validation, sequencing, and quality state.

#![forbid(unsafe_code)]

use insider_common_types::{InstrumentId, MonoTime, WallTime};
use std::collections::BTreeMap;

/// Validation failure for a canonical market event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventError {
    /// A non-positive price or size was supplied.
    NonPositiveValue,
    /// The exchange timestamp is after receive time.
    FutureExchangeTime,
    /// A sequence number of zero is invalid.
    InvalidSequence,
    /// A stream gap requires snapshot/backfill before state can continue.
    SequenceGap {
        /// Next sequence required for contiguous state.
        expected: u64,
        /// Sequence received after the gap.
        received: u64,
    },
}

/// Canonical bid/ask quote.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Quote {
    /// Instrument identity.
    pub instrument_id: InstrumentId,
    /// Provider sequence number.
    pub sequence: u64,
    /// Exchange event time.
    pub exchange_time: WallTime,
    /// Local monotonic receive time.
    pub received_mono: MonoTime,
    /// Bid price in integer ticks.
    pub bid_ticks: i64,
    /// Ask price in integer ticks.
    pub ask_ticks: i64,
    /// Bid quantity in integer ticks.
    pub bid_quantity_ticks: i64,
    /// Ask quantity in integer ticks.
    pub ask_quantity_ticks: i64,
}

/// Side of a level-2 book update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookSide {
    /// Bid price levels, ordered highest first when quoted.
    Bid,
    /// Ask price levels, ordered lowest first when quoted.
    Ask,
}

/// Incremental level-2 update. Quantity zero removes a level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BookDelta {
    /// Instrument identity.
    pub instrument_id: InstrumentId,
    /// Provider sequence number.
    pub sequence: u64,
    /// Side being changed.
    pub side: BookSide,
    /// Price in integer ticks.
    pub price_ticks: i64,
    /// New quantity in integer ticks, or zero to delete.
    pub quantity_ticks: i64,
}

/// Failure applying a book delta.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookError {
    /// Price is not positive or quantity is negative.
    InvalidLevel,
    /// The delta sequence requires a snapshot/backfill first.
    Gap {
        /// Sequence required for contiguous state.
        expected: u64,
        /// Sequence that exposed the gap.
        received: u64,
    },
    /// Applying the update would cross the book.
    Crossed,
}

/// Canonical bounded level-2 projection for one instrument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBook {
    instrument_id: InstrumentId,
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    sequence: SequenceTracker,
    health: Quality,
    depth_limit: usize,
}

impl OrderBook {
    /// Creates an empty book with a hard maximum number of levels per side.
    #[must_use]
    pub fn new(instrument_id: InstrumentId, depth_limit: usize) -> Option<Self> {
        (depth_limit > 0).then_some(Self {
            instrument_id,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            sequence: SequenceTracker::default(),
            health: Quality::Stale,
            depth_limit,
        })
    }

    /// Applies one contiguous update. Gaps never partially mutate the book.
    ///
    /// # Errors
    /// Returns [`BookError`] for invalid levels, sequence gaps, or crossed books.
    pub fn apply(&mut self, delta: BookDelta) -> Result<SequenceStatus, BookError> {
        if delta.instrument_id != self.instrument_id
            || delta.price_ticks <= 0
            || delta.quantity_ticks < 0
        {
            return Err(BookError::InvalidLevel);
        }
        let previous_sequence = self.sequence;
        let status = self.sequence.observe(delta.sequence);
        if let SequenceStatus::Gap { expected, received } = status {
            self.sequence = previous_sequence;
            self.health = Quality::Degraded;
            return Err(BookError::Gap { expected, received });
        }
        if matches!(status, SequenceStatus::Duplicate) {
            self.health = Quality::Degraded;
            return Ok(status);
        }
        let backup = match delta.side {
            BookSide::Bid => self.bids.clone(),
            BookSide::Ask => self.asks.clone(),
        };
        {
            let levels = match delta.side {
                BookSide::Bid => &mut self.bids,
                BookSide::Ask => &mut self.asks,
            };
            if delta.quantity_ticks == 0 {
                levels.remove(&delta.price_ticks);
            } else {
                levels.insert(delta.price_ticks, delta.quantity_ticks);
            }
            while levels.len() > self.depth_limit {
                let key = match delta.side {
                    BookSide::Bid => *levels.keys().next().unwrap_or(&delta.price_ticks),
                    BookSide::Ask => *levels.keys().next_back().unwrap_or(&delta.price_ticks),
                };
                levels.remove(&key);
            }
        }
        if self.is_crossed() {
            match delta.side {
                BookSide::Bid => self.bids = backup,
                BookSide::Ask => self.asks = backup,
            }
            self.health = Quality::Degraded;
            return Err(BookError::Crossed);
        }
        self.health = Quality::Good;
        Ok(status)
    }

    /// Returns the best bid and ask as a validated quote tuple.
    #[must_use]
    pub fn top(&self) -> Option<(i64, i64, i64, i64)> {
        let (&bid, &bid_qty) = self.bids.iter().next_back()?;
        let (&ask, &ask_qty) = self.asks.iter().next()?;
        Some((bid, bid_qty, ask, ask_qty))
    }

    /// Returns current stream quality.
    #[must_use]
    pub const fn health(&self) -> Quality {
        self.health
    }

    /// Marks the book stale until a valid delta or snapshot is accepted.
    pub fn mark_stale(&mut self) {
        self.health = Quality::Stale;
    }

    /// Returns the last accepted provider sequence.
    #[must_use]
    pub const fn sequence(&self) -> Option<u64> {
        self.sequence.last()
    }

    /// Replaces deltas with an authoritative provider snapshot after a gap.
    ///
    /// # Errors
    /// Returns [`BookError`] when the sequence, levels, depth, or crossing is
    /// invalid. Existing state is unchanged on failure.
    pub fn replace_snapshot(
        &mut self,
        sequence: u64,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
    ) -> Result<(), BookError> {
        if sequence == 0 || bids.len() > self.depth_limit || asks.len() > self.depth_limit {
            return Err(BookError::InvalidLevel);
        }
        let mut next_bids = BTreeMap::new();
        let mut next_asks = BTreeMap::new();
        for &(price, quantity) in bids {
            if price <= 0 || quantity <= 0 || next_bids.insert(price, quantity).is_some() {
                return Err(BookError::InvalidLevel);
            }
        }
        for &(price, quantity) in asks {
            if price <= 0 || quantity <= 0 || next_asks.insert(price, quantity).is_some() {
                return Err(BookError::InvalidLevel);
            }
        }
        if next_bids
            .keys()
            .next_back()
            .zip(next_asks.keys().next())
            .is_some_and(|(bid, ask)| bid >= ask)
        {
            return Err(BookError::Crossed);
        }
        self.sequence
            .reset(sequence)
            .map_err(|_| BookError::InvalidLevel)?;
        self.bids = next_bids;
        self.asks = next_asks;
        self.health = Quality::Good;
        Ok(())
    }

    fn is_crossed(&self) -> bool {
        self.bids
            .keys()
            .next_back()
            .zip(self.asks.keys().next())
            .is_some_and(|(bid, ask)| bid >= ask)
    }
}

impl Quote {
    /// Validates a quote before it can enter feature state.
    ///
    /// # Errors
    /// Returns [`EventError`] for invalid sequence, prices, quantities, or time order.
    pub fn validate(&self, receive_wall: WallTime) -> Result<(), EventError> {
        if self.sequence == 0 {
            return Err(EventError::InvalidSequence);
        }
        if self.bid_ticks <= 0
            || self.ask_ticks <= 0
            || self.bid_quantity_ticks <= 0
            || self.ask_quantity_ticks <= 0
        {
            return Err(EventError::NonPositiveValue);
        }
        if self.exchange_time > receive_wall {
            return Err(EventError::FutureExchangeTime);
        }
        if self.bid_ticks > self.ask_ticks {
            return Err(EventError::NonPositiveValue);
        }
        Ok(())
    }
}

/// Canonical trade print.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Trade {
    /// Instrument identity.
    pub instrument_id: InstrumentId,
    /// Provider sequence number.
    pub sequence: u64,
    /// Exchange event time.
    pub exchange_time: WallTime,
    /// Local monotonic receive time.
    pub received_mono: MonoTime,
    /// Trade price in integer ticks.
    pub price_ticks: i64,
    /// Trade quantity in integer ticks.
    pub quantity_ticks: i64,
}

impl Trade {
    /// Validates a trade before it can enter feature state.
    ///
    /// # Errors
    /// Returns [`EventError`] for invalid sequence, values, or time order.
    pub fn validate(&self, receive_wall: WallTime) -> Result<(), EventError> {
        if self.sequence == 0 {
            return Err(EventError::InvalidSequence);
        }
        if self.price_ticks <= 0 || self.quantity_ticks <= 0 {
            return Err(EventError::NonPositiveValue);
        }
        if self.exchange_time > receive_wall {
            return Err(EventError::FutureExchangeTime);
        }
        Ok(())
    }
}

/// Canonical event-time OHLCV bar.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Bar {
    /// Instrument identity.
    pub instrument_id: InstrumentId,
    /// Inclusive start of the bar interval in exchange nanoseconds.
    pub start_time: WallTime,
    /// Interval width in nanoseconds.
    pub interval_ns: u64,
    /// First trade price in the interval.
    pub open_ticks: i64,
    /// Highest trade price in the interval.
    pub high_ticks: i64,
    /// Lowest trade price in the interval.
    pub low_ticks: i64,
    /// Last trade price in the interval.
    pub close_ticks: i64,
    /// Sum of trade quantities in the interval.
    pub volume_ticks: i64,
}

/// Result of applying a trade to an event-time bar stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BarUpdate {
    /// A previously unseen interval was created.
    New(Bar),
    /// A visible interval changed due to a late/corrected trade.
    Correction(Bar),
    /// A duplicate provider sequence was ignored.
    Duplicate,
}

/// Bounded event-time bar aggregator retaining corrections as explicit updates.
pub struct BarAggregator {
    instrument_id: InstrumentId,
    interval_ns: u64,
    capacity: usize,
    bars: BTreeMap<i64, BarState>,
    sequence: SequenceTracker,
}

struct BarState {
    bar: Bar,
    open_trade_time: i64,
    close_trade_time: i64,
}

impl BarAggregator {
    /// Creates an aggregator with a bounded historical bar count.
    #[must_use]
    pub fn new(instrument_id: InstrumentId, interval_ns: u64, capacity: usize) -> Option<Self> {
        (interval_ns > 0 && i64::try_from(interval_ns).is_ok() && capacity > 0).then_some(Self {
            instrument_id,
            interval_ns,
            capacity,
            bars: BTreeMap::new(),
            sequence: SequenceTracker::default(),
        })
    }

    /// Applies a validated trade using exchange event time, not receive time.
    ///
    /// Late trades update the existing bar and return `Correction`; callers
    /// must publish that correction as a new event rather than rewriting a
    /// previously emitted historical record.
    ///
    /// # Errors
    /// Returns [`EventError`] when the trade is invalid for this instrument.
    pub fn ingest(
        &mut self,
        trade: Trade,
        receive_wall: WallTime,
    ) -> Result<BarUpdate, EventError> {
        if trade.instrument_id != self.instrument_id {
            return Err(EventError::NonPositiveValue);
        }
        trade.validate(receive_wall)?;
        match self.sequence.observe(trade.sequence) {
            SequenceStatus::Duplicate => return Ok(BarUpdate::Duplicate),
            SequenceStatus::Gap { expected, received } => {
                return Err(EventError::SequenceGap { expected, received });
            }
            SequenceStatus::Initial | SequenceStatus::Contiguous => {}
        }
        let timestamp = trade.exchange_time.as_unix_nanos();
        let interval = i64::try_from(self.interval_ns).map_err(|_| EventError::NonPositiveValue)?;
        let start = timestamp.div_euclid(interval).saturating_mul(interval);
        let start_time = WallTime::from_unix_nanos(start);
        let update = if let Some(state) = self.bars.get_mut(&start) {
            state.bar.high_ticks = state.bar.high_ticks.max(trade.price_ticks);
            state.bar.low_ticks = state.bar.low_ticks.min(trade.price_ticks);
            if timestamp < state.open_trade_time {
                state.open_trade_time = timestamp;
                state.bar.open_ticks = trade.price_ticks;
            }
            if timestamp >= state.close_trade_time {
                state.close_trade_time = timestamp;
                state.bar.close_ticks = trade.price_ticks;
            }
            state.bar.volume_ticks = state.bar.volume_ticks.saturating_add(trade.quantity_ticks);
            BarUpdate::Correction(state.bar)
        } else {
            let bar = Bar {
                instrument_id: self.instrument_id,
                start_time,
                interval_ns: self.interval_ns,
                open_ticks: trade.price_ticks,
                high_ticks: trade.price_ticks,
                low_ticks: trade.price_ticks,
                close_ticks: trade.price_ticks,
                volume_ticks: trade.quantity_ticks,
            };
            self.bars.insert(
                start,
                BarState {
                    bar,
                    open_trade_time: timestamp,
                    close_trade_time: timestamp,
                },
            );
            while self.bars.len() > self.capacity {
                let Some(first) = self.bars.keys().next().copied() else {
                    break;
                };
                self.bars.remove(&first);
            }
            BarUpdate::New(bar)
        };
        Ok(update)
    }

    /// Inserts one provider-supplied historical bar after validating its
    /// canonical interval, OHLCV invariants, and monotonically sequenced
    /// backfill cursor. Existing bars are returned as corrections.
    ///
    /// # Errors
    /// Returns [`EventError`] for a mismatched instrument/interval, malformed
    /// OHLCV values, or a sequence gap.
    pub fn ingest_bar(&mut self, bar: Bar, sequence: u64) -> Result<BarUpdate, EventError> {
        if bar.instrument_id != self.instrument_id
            || bar.interval_ns != self.interval_ns
            || bar.start_time.as_unix_nanos() < 0
            || bar.open_ticks <= 0
            || bar.high_ticks < bar.low_ticks
            || bar.low_ticks <= 0
            || bar.open_ticks < bar.low_ticks
            || bar.open_ticks > bar.high_ticks
            || bar.close_ticks < bar.low_ticks
            || bar.close_ticks > bar.high_ticks
            || bar.volume_ticks <= 0
        {
            return Err(EventError::NonPositiveValue);
        }
        match self.sequence.observe(sequence) {
            SequenceStatus::Duplicate => return Ok(BarUpdate::Duplicate),
            SequenceStatus::Gap { expected, received } => {
                return Err(EventError::SequenceGap { expected, received });
            }
            SequenceStatus::Initial | SequenceStatus::Contiguous => {}
        }
        let key = bar.start_time.as_unix_nanos();
        let update = if let Some(state) = self.bars.get_mut(&key) {
            state.bar = bar;
            BarUpdate::Correction(bar)
        } else {
            self.bars.insert(
                key,
                BarState {
                    bar,
                    open_trade_time: key,
                    close_trade_time: key
                        .saturating_add(i64::try_from(self.interval_ns).unwrap_or(i64::MAX)),
                },
            );
            while self.bars.len() > self.capacity {
                let Some(first) = self.bars.keys().next().copied() else {
                    break;
                };
                self.bars.remove(&first);
            }
            BarUpdate::New(bar)
        };
        Ok(update)
    }

    /// Returns a retained bar by its event-time start.
    #[must_use]
    pub fn get(&self, start_time: WallTime) -> Option<Bar> {
        self.bars
            .get(&start_time.as_unix_nanos())
            .map(|state| state.bar)
    }

    /// Returns the retained event-time bars from oldest to newest.
    #[must_use]
    pub fn all(&self) -> Vec<Bar> {
        self.bars.values().map(|state| state.bar).collect()
    }

    /// Resets the trade sequence after an authoritative feed snapshot.
    ///
    /// # Errors
    /// Returns [`EventError::InvalidSequence`] for a zero snapshot sequence.
    pub fn recover_after_snapshot(&mut self, sequence: u64) -> Result<(), EventError> {
        self.sequence.reset(sequence)
    }
}

/// Sequence transition observed for one provider stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SequenceStatus {
    /// First event in the stream.
    Initial,
    /// Exactly the expected next event.
    Contiguous,
    /// A duplicate or older event that must not mutate state.
    Duplicate,
    /// One or more events are missing and a snapshot/backfill is required.
    Gap {
        /// Sequence required to continue contiguously.
        expected: u64,
        /// Sequence that exposed the gap.
        received: u64,
    },
}

/// Per-stream sequence tracker.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SequenceTracker {
    last: Option<u64>,
}

impl SequenceTracker {
    /// Records a sequence and returns its transition status.
    pub fn observe(&mut self, sequence: u64) -> SequenceStatus {
        if sequence == 0 {
            return SequenceStatus::Duplicate;
        }
        let status = match self.last {
            None => SequenceStatus::Initial,
            Some(last) if sequence <= last => SequenceStatus::Duplicate,
            Some(last) if sequence == last.saturating_add(1) => SequenceStatus::Contiguous,
            Some(last) => SequenceStatus::Gap {
                expected: last.saturating_add(1),
                received: sequence,
            },
        };
        if matches!(status, SequenceStatus::Initial | SequenceStatus::Contiguous) {
            self.last = Some(sequence);
        }
        status
    }

    /// Resets the accepted sequence after a verified snapshot/backfill.
    ///
    /// # Errors
    /// Returns [`EventError::InvalidSequence`] when the snapshot sequence is zero.
    pub fn reset(&mut self, sequence: u64) -> Result<(), EventError> {
        if sequence == 0 {
            return Err(EventError::InvalidSequence);
        }
        self.last = Some(sequence);
        Ok(())
    }

    /// Returns the most recently accepted sequence.
    #[must_use]
    pub const fn last(&self) -> Option<u64> {
        self.last
    }
}

/// Quality state used by downstream metrics and risk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Quality {
    /// Stream is current and contiguous.
    Good,
    /// A gap or duplicate was observed; state needs repair or annotation.
    Degraded,
    /// No valid event has arrived within the configured freshness window.
    Stale,
}

/// Tracks freshness and sequence quality for one stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamHealth {
    /// Last local receive time.
    pub last_received: Option<MonoTime>,
    /// Current quality.
    pub quality: Quality,
    /// Sequence state.
    pub sequence: SequenceTracker,
}

/// A normalized event accepted by [`MarketDataHub`]. Provider adapters must
/// convert their payloads into one of these canonical variants before entering
/// the runtime.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MarketEvent {
    /// Top-of-book quote update.
    Quote(Quote),
    /// Trade print used by features and event-time bars.
    Trade(Trade),
    /// Incremental level-2 book update.
    Book(BookDelta),
}

/// Result of ingesting one canonical event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestOutcome {
    /// Event was accepted and state was advanced.
    Accepted(SequenceStatus),
    /// Event was an already-seen provider sequence.
    Duplicate,
    /// A snapshot/backfill is required before this stream can continue.
    Gap {
        /// First sequence missing from the stream.
        expected: u64,
        /// Sequence that exposed the gap.
        received: u64,
    },
}

/// Bounded latest-state view for one instrument.
#[derive(Clone, Debug, PartialEq)]
pub struct MarketSnapshot {
    /// Instrument identity.
    pub instrument_id: InstrumentId,
    /// Latest validated quote, if one has arrived.
    pub quote: Option<Quote>,
    /// Latest validated trade, if one has arrived.
    pub trade: Option<Trade>,
    /// Bounded recent trade prints in exchange-event order.
    pub trades: Vec<Trade>,
    /// Stream health for quote, trade, and book feeds.
    pub quote_health: StreamHealth,
    /// Stream health for trade events.
    pub trade_health: StreamHealth,
    /// Stream health for level-2 events.
    pub book_health: Option<Quality>,
    /// Best bid/ask and quantities from the current level-2 book.
    pub book_top: Option<(i64, i64, i64, i64)>,
    /// Retained event-time bars, including corrected latest values.
    pub bars: Vec<Bar>,
}

struct InstrumentStreams {
    quote: Option<Quote>,
    trade: Option<Trade>,
    trades: Vec<Trade>,
    quote_health: StreamHealth,
    trade_health: StreamHealth,
    book: Option<OrderBook>,
    bars: Option<BarAggregator>,
}

/// Bounded in-memory market-state ingress used by providers, replay, and live
/// execution. It deliberately owns no network connection: adapters supply
/// canonical events and explicitly recover a stream after an authoritative
/// snapshot. This keeps live and replay semantics identical.
pub struct MarketDataHub {
    instruments: BTreeMap<InstrumentId, InstrumentStreams>,
    max_instruments: usize,
    default_depth: usize,
    bar_interval_ns: Option<u64>,
    bar_capacity: usize,
}

impl MarketDataHub {
    /// Creates a hub with hard bounds for instrument and retained market state.
    #[must_use]
    pub fn new(
        max_instruments: usize,
        default_depth: usize,
        bar_interval_ns: Option<u64>,
        bar_capacity: usize,
    ) -> Option<Self> {
        (max_instruments > 0
            && default_depth > 0
            && bar_capacity > 0
            && bar_interval_ns.is_none_or(|interval| interval > 0))
        .then_some(Self {
            instruments: BTreeMap::new(),
            max_instruments,
            default_depth,
            bar_interval_ns,
            bar_capacity,
        })
    }

    /// Registers an instrument stream before accepting provider events.
    ///
    /// # Errors
    /// Returns `false` when the instrument is already registered or the hard
    /// instrument bound has been reached.
    pub fn register(&mut self, instrument_id: InstrumentId) -> bool {
        if self.instruments.contains_key(&instrument_id)
            || self.instruments.len() >= self.max_instruments
        {
            return false;
        }
        let book = OrderBook::new(instrument_id, self.default_depth);
        let bars = self
            .bar_interval_ns
            .and_then(|interval| BarAggregator::new(instrument_id, interval, self.bar_capacity));
        self.instruments.insert(
            instrument_id,
            InstrumentStreams {
                quote: None,
                trade: None,
                trades: Vec::new(),
                quote_health: StreamHealth::default(),
                trade_health: StreamHealth::default(),
                book,
                bars,
            },
        );
        true
    }

    /// Returns whether an instrument has been registered.
    #[must_use]
    pub fn contains(&self, instrument_id: InstrumentId) -> bool {
        self.instruments.contains_key(&instrument_id)
    }

    /// Ingests one event without partially mutating state on validation or gap.
    ///
    /// # Errors
    /// Returns [`EventError`] or [`BookError`] for malformed canonical events.
    pub fn ingest(
        &mut self,
        event: MarketEvent,
        receive_wall: WallTime,
    ) -> Result<IngestOutcome, HubError> {
        let instrument_id = match event {
            MarketEvent::Quote(value) => value.instrument_id,
            MarketEvent::Trade(value) => value.instrument_id,
            MarketEvent::Book(value) => value.instrument_id,
        };
        let streams = self
            .instruments
            .get_mut(&instrument_id)
            .ok_or(HubError::Unregistered(instrument_id))?;
        match event {
            MarketEvent::Quote(value) => {
                value.validate(receive_wall)?;
                let status = streams
                    .quote_health
                    .observe(value.sequence, value.received_mono);
                match status {
                    SequenceStatus::Gap { expected, received } => {
                        Err(HubError::Gap { expected, received })
                    }
                    SequenceStatus::Duplicate => Ok(IngestOutcome::Duplicate),
                    accepted => {
                        streams.quote = Some(value);
                        Ok(IngestOutcome::Accepted(accepted))
                    }
                }
            }
            MarketEvent::Trade(value) => {
                value.validate(receive_wall)?;
                let previous_health = streams.trade_health;
                let status = streams
                    .trade_health
                    .observe(value.sequence, value.received_mono);
                match status {
                    SequenceStatus::Gap { expected, received } => {
                        Err(HubError::Gap { expected, received })
                    }
                    SequenceStatus::Duplicate => Ok(IngestOutcome::Duplicate),
                    accepted => {
                        if let Some(bars) = streams.bars.as_mut()
                            && let Err(error) = bars.ingest(value, receive_wall)
                        {
                            // Health and bars are one logical trade projection.
                            // Restore the health transition if aggregation
                            // rejects the event so callers do not observe a
                            // partially accepted trade.
                            streams.trade_health = previous_health;
                            return Err(error.into());
                        }
                        streams.trade = Some(value);
                        streams.trades.push(value);
                        if streams.trades.len() > 512 {
                            let remove = streams.trades.len() - 512;
                            streams.trades.drain(..remove);
                        }
                        Ok(IngestOutcome::Accepted(accepted))
                    }
                }
            }
            MarketEvent::Book(value) => {
                let book = streams.book.as_mut().ok_or(HubError::BookUnavailable)?;
                match book.apply(value) {
                    Ok(SequenceStatus::Duplicate) => Ok(IngestOutcome::Duplicate),
                    Ok(status) => Ok(IngestOutcome::Accepted(status)),
                    Err(BookError::Gap { expected, received }) => {
                        Err(HubError::Gap { expected, received })
                    }
                    Err(error) => Err(HubError::Book(error)),
                }
            }
        }
    }

    /// Ingests one historical OHLCV bar into a registered instrument stream.
    /// This is separate from tick-event sequencing so backfill providers can
    /// populate charts without fabricating trades or affecting execution.
    ///
    /// # Errors
    /// Returns [`HubError`] when the instrument is unregistered, bars are not
    /// configured, or the bar violates canonical validation/sequencing.
    pub fn ingest_bar(&mut self, bar: Bar, sequence: u64) -> Result<BarUpdate, HubError> {
        let streams = self
            .instruments
            .get_mut(&bar.instrument_id)
            .ok_or(HubError::Unregistered(bar.instrument_id))?;
        streams
            .bars
            .as_mut()
            .ok_or(HubError::BarsUnavailable)
            .and_then(|bars| bars.ingest_bar(bar, sequence).map_err(HubError::Event))
    }

    /// Installs an authoritative level-2 snapshot after a detected gap.
    ///
    /// # Errors
    /// Returns [`HubError::Book`] when the snapshot is invalid.
    pub fn recover_book(
        &mut self,
        instrument_id: InstrumentId,
        sequence: u64,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
    ) -> Result<(), HubError> {
        let streams = self
            .instruments
            .get_mut(&instrument_id)
            .ok_or(HubError::Unregistered(instrument_id))?;
        streams
            .book
            .as_mut()
            .ok_or(HubError::BookUnavailable)?
            .replace_snapshot(sequence, bids, asks)
            .map_err(HubError::Book)
    }

    /// Resets a quote or trade stream after a verified provider snapshot.
    ///
    /// # Errors
    /// Returns [`HubError::Event`] for a zero sequence.
    pub fn recover_stream(
        &mut self,
        instrument_id: InstrumentId,
        kind: StreamKind,
        sequence: u64,
        received: MonoTime,
    ) -> Result<(), HubError> {
        let streams = self
            .instruments
            .get_mut(&instrument_id)
            .ok_or(HubError::Unregistered(instrument_id))?;
        match kind {
            StreamKind::Quote => streams
                .quote_health
                .recover_after_snapshot(sequence, received)?,
            StreamKind::Trade => {
                streams
                    .trade_health
                    .recover_after_snapshot(sequence, received)?;
                if let Some(bars) = streams.bars.as_mut() {
                    bars.recover_after_snapshot(sequence)?;
                }
            }
            StreamKind::Book => {
                return Err(HubError::BookRecoveryRequiresLevels);
            }
        }
        Ok(())
    }

    /// Marks all feeds stale when their configured freshness deadline elapses.
    pub fn mark_stale(&mut self, now: MonoTime, max_age_ns: u64) {
        for streams in self.instruments.values_mut() {
            streams.quote_health.mark_stale_if_due(now, max_age_ns);
            streams.trade_health.mark_stale_if_due(now, max_age_ns);
        }
    }

    /// Returns the bounded latest state for one registered instrument.
    #[must_use]
    pub fn snapshot(&self, instrument_id: InstrumentId) -> Option<MarketSnapshot> {
        let streams = self.instruments.get(&instrument_id)?;
        Some(MarketSnapshot {
            instrument_id,
            quote: streams.quote,
            trade: streams.trade,
            trades: streams.trades.clone(),
            quote_health: streams.quote_health,
            trade_health: streams.trade_health,
            book_health: streams.book.as_ref().map(OrderBook::health),
            book_top: streams.book.as_ref().and_then(OrderBook::top),
            bars: streams
                .bars
                .as_ref()
                .map_or_else(Vec::new, BarAggregator::all),
        })
    }

    /// Returns all registered latest states in canonical instrument order.
    /// The returned vector is bounded by the hub's registration limit.
    #[must_use]
    pub fn snapshots(&self) -> Vec<MarketSnapshot> {
        self.instruments
            .iter()
            .map(|(instrument_id, streams)| MarketSnapshot {
                instrument_id: *instrument_id,
                quote: streams.quote,
                trade: streams.trade,
                trades: streams.trades.clone(),
                quote_health: streams.quote_health,
                trade_health: streams.trade_health,
                book_health: streams.book.as_ref().map(OrderBook::health),
                book_top: streams.book.as_ref().and_then(OrderBook::top),
                bars: streams
                    .bars
                    .as_ref()
                    .map_or_else(Vec::new, BarAggregator::all),
            })
            .collect()
    }

    /// Returns bounded aggregate feed-health counts for supervision. An
    /// instrument is unhealthy when any configured quote, trade, or book
    /// stream is not `Good`; registration without a first valid event remains
    /// unhealthy rather than being mistaken for a live feed.
    #[must_use]
    pub fn health_counts(&self) -> (usize, usize) {
        let total = self.instruments.len();
        let unhealthy = self
            .instruments
            .values()
            .filter(|streams| {
                streams.quote_health.quality != Quality::Good
                    || streams.trade_health.quality != Quality::Good
                    || streams
                        .book
                        .as_ref()
                        .is_some_and(|book| book.health() != Quality::Good)
            })
            .count();
        (total, unhealthy)
    }
}

/// Stream kind used for explicit snapshot recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamKind {
    /// Top-of-book quote feed.
    Quote,
    /// Trade-print feed.
    Trade,
    /// Level-2 book feed; use [`MarketDataHub::recover_book`] to recover it.
    Book,
}

/// Market-data hub failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HubError {
    /// Event was received before the instrument was registered.
    Unregistered(InstrumentId),
    /// Canonical event failed validation.
    Event(EventError),
    /// Book update failed validation or crossed the market.
    Book(BookError),
    /// A stream gap requires an authoritative snapshot.
    Gap {
        /// First sequence missing from the stream.
        expected: u64,
        /// Sequence that exposed the gap.
        received: u64,
    },
    /// A registered stream was configured without a book.
    BookUnavailable,
    /// A registered stream was configured without historical bars.
    BarsUnavailable,
    /// Book recovery must include levels, not only a sequence.
    BookRecoveryRequiresLevels,
}

impl From<EventError> for HubError {
    fn from(error: EventError) -> Self {
        Self::Event(error)
    }
}

impl Default for StreamHealth {
    fn default() -> Self {
        Self {
            last_received: None,
            quality: Quality::Stale,
            sequence: SequenceTracker::default(),
        }
    }
}

impl StreamHealth {
    /// Accepts an event's sequence and receive time.
    pub fn observe(&mut self, sequence: u64, received: MonoTime) -> SequenceStatus {
        let status = self.sequence.observe(sequence);
        self.last_received = Some(received);
        self.quality = if matches!(
            status,
            SequenceStatus::Gap { .. } | SequenceStatus::Duplicate
        ) {
            Quality::Degraded
        } else {
            Quality::Good
        };
        status
    }

    /// Marks the stream stale when its freshness deadline has elapsed.
    pub fn mark_stale_if_due(&mut self, now: MonoTime, max_age_ns: u64) -> bool {
        let stale = self
            .last_received
            .is_none_or(|last| now.as_nanos().saturating_sub(last.as_nanos()) > max_age_ns);
        if stale {
            self.quality = Quality::Stale;
        }
        stale
    }

    /// Marks a stream healthy after a verified snapshot has been installed.
    ///
    /// # Errors
    /// Returns [`EventError::InvalidSequence`] when `sequence` is zero.
    pub fn recover_after_snapshot(
        &mut self,
        sequence: u64,
        received: MonoTime,
    ) -> Result<(), EventError> {
        self.sequence.reset(sequence)?;
        self.last_received = Some(received);
        self.quality = Quality::Good;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use insider_common_types::{MonoTime, WallTime};

    use super::{
        BarAggregator, BarUpdate, BookDelta, BookError, BookSide, EventError, IngestOutcome,
        MarketDataHub, MarketEvent, Quality, SequenceStatus, SequenceTracker, StreamHealth, Trade,
    };

    #[test]
    fn sequence_tracker_requires_repair_for_gaps_and_ignores_duplicates() {
        let mut tracker = SequenceTracker::default();
        assert_eq!(tracker.observe(1), SequenceStatus::Initial);
        assert_eq!(tracker.observe(2), SequenceStatus::Contiguous);
        assert_eq!(
            tracker.observe(4),
            SequenceStatus::Gap {
                expected: 3,
                received: 4
            }
        );
        assert_eq!(
            tracker.observe(4),
            SequenceStatus::Gap {
                expected: 3,
                received: 4
            }
        );
        assert_eq!(tracker.last(), Some(2));
        assert!(tracker.reset(4).is_ok());
        assert_eq!(tracker.observe(5), SequenceStatus::Contiguous);
    }

    #[test]
    fn health_transitions_to_stale_after_freshness_deadline() {
        let mut health = StreamHealth::default();
        assert_eq!(
            health.observe(1, MonoTime::from_nanos(10)),
            SequenceStatus::Initial
        );
        assert_eq!(health.quality, Quality::Good);
        assert!(!health.mark_stale_if_due(MonoTime::from_nanos(20), 10));
        assert!(health.mark_stale_if_due(MonoTime::from_nanos(21), 10));
        assert_eq!(health.quality, Quality::Stale);
        let _ = WallTime::from_unix_nanos(0);
    }

    #[test]
    fn order_book_rejects_gaps_and_crosses_without_partial_state() {
        let Some(instrument) = insider_common_types::InstrumentId::new(9).ok() else {
            return;
        };
        let Some(mut book) = super::OrderBook::new(instrument, 2) else {
            return;
        };
        assert!(
            book.apply(BookDelta {
                instrument_id: instrument,
                sequence: 1,
                side: BookSide::Bid,
                price_ticks: 99,
                quantity_ticks: 10
            })
            .is_ok()
        );
        assert!(
            book.apply(BookDelta {
                instrument_id: instrument,
                sequence: 3,
                side: BookSide::Ask,
                price_ticks: 100,
                quantity_ticks: 8
            })
            .is_err()
        );
        assert_eq!(book.sequence(), Some(1));
        assert!(
            book.apply(BookDelta {
                instrument_id: instrument,
                sequence: 2,
                side: BookSide::Ask,
                price_ticks: 100,
                quantity_ticks: 8
            })
            .is_ok()
        );
        assert_eq!(book.top(), Some((99, 10, 100, 8)));
        assert_eq!(
            book.apply(BookDelta {
                instrument_id: instrument,
                sequence: 3,
                side: BookSide::Ask,
                price_ticks: 98,
                quantity_ticks: 4
            }),
            Err(BookError::Crossed)
        );
        assert_eq!(book.top(), Some((99, 10, 100, 8)));
    }

    #[test]
    fn bars_use_event_time_and_emit_corrections_for_late_trades() {
        let Some(instrument) = insider_common_types::InstrumentId::new(10).ok() else {
            return;
        };
        let Some(mut bars) = BarAggregator::new(instrument, 1_000, 4) else {
            return;
        };
        let trade = |sequence, timestamp, price| Trade {
            instrument_id: instrument,
            sequence,
            exchange_time: WallTime::from_unix_nanos(timestamp),
            received_mono: MonoTime::from_nanos(sequence),
            price_ticks: price,
            quantity_ticks: 2,
        };
        assert!(matches!(
            bars.ingest(trade(1, 1_500, 105), WallTime::from_unix_nanos(2_000)),
            Ok(BarUpdate::New(_))
        ));
        let update = bars.ingest(trade(2, 1_100, 100), WallTime::from_unix_nanos(2_000));
        assert!(matches!(update, Ok(BarUpdate::Correction(_))));
        let bar = bars.get(WallTime::from_unix_nanos(1_000));
        assert_eq!(
            bar.map(|bar| (bar.open_ticks, bar.close_ticks, bar.volume_ticks)),
            Some((100, 105, 4))
        );
        assert!(matches!(
            bars.ingest(trade(2, 1_100, 100), WallTime::from_unix_nanos(2_000)),
            Ok(BarUpdate::Duplicate)
        ));
    }

    #[test]
    fn bars_stop_on_sequence_gap_until_explicit_snapshot_recovery() {
        let Some(instrument) = insider_common_types::InstrumentId::new(11).ok() else {
            return;
        };
        let Some(mut bars) = BarAggregator::new(instrument, 1_000, 4) else {
            return;
        };
        let trade = |sequence| Trade {
            instrument_id: instrument,
            sequence,
            exchange_time: WallTime::from_unix_nanos(1_000),
            received_mono: MonoTime::from_nanos(sequence),
            price_ticks: 100,
            quantity_ticks: 1,
        };
        assert!(
            bars.ingest(trade(1), WallTime::from_unix_nanos(2_000))
                .is_ok()
        );
        assert_eq!(
            bars.ingest(trade(3), WallTime::from_unix_nanos(2_000)),
            Err(EventError::SequenceGap {
                expected: 2,
                received: 3
            })
        );
        assert!(bars.recover_after_snapshot(3).is_ok());
        assert!(
            bars.ingest(trade(4), WallTime::from_unix_nanos(2_000))
                .is_ok()
        );
    }

    #[test]
    fn hub_requires_registration_and_explicit_recovery_after_gap() {
        let Some(instrument) = insider_common_types::InstrumentId::new(12).ok() else {
            return;
        };
        let Some(mut hub) = MarketDataHub::new(2, 4, Some(1_000), 8) else {
            return;
        };
        assert!(hub.register(instrument));
        let quote = |sequence| super::Quote {
            instrument_id: instrument,
            sequence,
            exchange_time: WallTime::from_unix_nanos(1_000),
            received_mono: MonoTime::from_nanos(sequence),
            bid_ticks: 99,
            ask_ticks: 100,
            bid_quantity_ticks: 2,
            ask_quantity_ticks: 2,
        };
        assert!(matches!(
            hub.ingest(
                MarketEvent::Quote(quote(1)),
                WallTime::from_unix_nanos(2_000)
            ),
            Ok(IngestOutcome::Accepted(SequenceStatus::Initial))
        ));
        assert!(matches!(
            hub.ingest(
                MarketEvent::Quote(quote(3)),
                WallTime::from_unix_nanos(2_000)
            ),
            Err(super::HubError::Gap {
                expected: 2,
                received: 3
            })
        ));
        assert!(
            hub.recover_stream(
                instrument,
                super::StreamKind::Quote,
                3,
                MonoTime::from_nanos(3)
            )
            .is_ok()
        );
        assert!(matches!(
            hub.ingest(
                MarketEvent::Quote(quote(4)),
                WallTime::from_unix_nanos(2_000)
            ),
            Ok(IngestOutcome::Accepted(SequenceStatus::Contiguous))
        ));
        assert_eq!(
            hub.snapshot(instrument)
                .and_then(|value| value.quote)
                .map(|value| value.sequence),
            Some(4)
        );
    }
}
