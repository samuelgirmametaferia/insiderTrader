//! Canonical, venue-aware market and instrument types.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fmt;

use insider_common_types::InstrumentId;

/// Supported canonical asset classes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AssetClass {
    /// Common stock.
    Equity,
    /// Exchange-traded fund.
    Etf,
    /// Listed option contract.
    Option,
    /// Listed future contract.
    Future,
    /// Foreign exchange spot/forward contract.
    Fx,
    /// Crypto spot/derivative contract.
    Crypto,
}

/// Instrument lifecycle/identity validation failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstrumentError {
    /// A required identifier or symbol was empty.
    EmptyField(&'static str),
    /// Currency was not a three-letter ISO-like code.
    InvalidCurrency,
    /// Price/quantity increment was zero or negative.
    InvalidIncrement(&'static str),
    /// Contract metadata is invalid for its asset class.
    InvalidContract(&'static str),
}

impl fmt::Display for InstrumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyField(field) => write!(formatter, "{field} must not be empty"),
            Self::InvalidCurrency => formatter.write_str("currency must be three ASCII letters"),
            Self::InvalidIncrement(field) => write!(formatter, "{field} must be positive"),
            Self::InvalidContract(field) => write!(formatter, "invalid contract field: {field}"),
        }
    }
}

impl std::error::Error for InstrumentError {}

/// Contract-specific metadata. Decimal values are represented as integer ticks
/// in the canonical domain to prevent binary floating-point order errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Contract {
    /// Equity/ETF listing metadata.
    Listing,
    /// Option contract metadata.
    Option {
        /// Underlying instrument identity.
        underlying: InstrumentId,
        /// Expiry as an exchange-local calendar day number.
        expiry_day: u32,
        /// Strike in canonical price ticks.
        strike_ticks: i64,
        /// True for call, false for put.
        is_call: bool,
    },
    /// Futures contract metadata.
    Future {
        /// Contract root symbol.
        root: String,
        /// Expiry as an exchange-local calendar day number.
        expiry_day: u32,
        /// Contract multiplier in quantity ticks.
        multiplier_ticks: i64,
    },
    /// FX pair metadata.
    Fx {
        /// Base currency code.
        base: String,
        /// Quote currency code.
        quote: String,
    },
    /// Crypto pair metadata.
    Crypto {
        /// Base asset code.
        base: String,
        /// Quote asset code.
        quote: String,
    },
}

/// Construction fields for an [`Instrument`].
pub struct InstrumentSpec {
    /// Internal immutable identity.
    pub id: InstrumentId,
    /// Canonical symbol.
    pub symbol: String,
    /// Asset class.
    pub asset_class: AssetClass,
    /// Listing venue.
    pub venue: String,
    /// Settlement/quote currency.
    pub currency: String,
    /// Minimum price increment in integer ticks.
    pub price_increment_ticks: i64,
    /// Minimum quantity increment in integer units.
    pub quantity_increment_ticks: i64,
    /// Asset-class-specific metadata.
    pub contract: Contract,
    /// Provider-specific symbol.
    pub provider_symbol: String,
}

/// Immutable canonical instrument definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Instrument {
    /// Internal immutable identity.
    pub id: InstrumentId,
    /// Canonical symbol; never used alone as an authoritative key.
    pub symbol: String,
    /// Asset class.
    pub asset_class: AssetClass,
    /// Listing venue/exchange identifier.
    pub venue: String,
    /// Settlement/quote currency.
    pub currency: String,
    /// Minimum price increment in integer ticks.
    pub price_increment_ticks: i64,
    /// Minimum quantity increment in integer units.
    pub quantity_increment_ticks: i64,
    /// Asset-class-specific contract fields.
    pub contract: Contract,
    /// Provider-specific symbol kept at the adapter boundary.
    pub provider_symbol: String,
}

impl Instrument {
    /// Validates and constructs an instrument definition.
    ///
    /// # Errors
    /// Returns [`InstrumentError`] when identity, precision, currency, or
    /// asset-class contract metadata is invalid.
    pub fn new(spec: InstrumentSpec) -> Result<Self, InstrumentError> {
        let InstrumentSpec {
            id,
            symbol,
            asset_class,
            venue,
            currency,
            price_increment_ticks,
            quantity_increment_ticks,
            contract,
            provider_symbol,
        } = spec;
        for (value, field) in [
            (&symbol, "symbol"),
            (&venue, "venue"),
            (&provider_symbol, "provider_symbol"),
        ] {
            if value.trim().is_empty() {
                return Err(InstrumentError::EmptyField(field));
            }
        }
        if currency.len() != 3 || !currency.bytes().all(|byte| byte.is_ascii_uppercase()) {
            return Err(InstrumentError::InvalidCurrency);
        }
        if price_increment_ticks <= 0 {
            return Err(InstrumentError::InvalidIncrement("price_increment_ticks"));
        }
        if quantity_increment_ticks <= 0 {
            return Err(InstrumentError::InvalidIncrement(
                "quantity_increment_ticks",
            ));
        }
        match (&asset_class, &contract) {
            (AssetClass::Equity | AssetClass::Etf, Contract::Listing) => {}
            (
                AssetClass::Option,
                Contract::Option {
                    expiry_day,
                    strike_ticks,
                    ..
                },
            ) if *expiry_day > 0 && *strike_ticks > 0 => {}
            (
                AssetClass::Future,
                Contract::Future {
                    expiry_day,
                    multiplier_ticks,
                    root,
                },
            ) if *expiry_day > 0 && *multiplier_ticks > 0 && !root.is_empty() => {}
            (AssetClass::Fx, Contract::Fx { base, quote })
            | (AssetClass::Crypto, Contract::Crypto { base, quote })
                if base.len() == 3 && quote.len() == 3 && base != quote => {}
            _ => {
                return Err(InstrumentError::InvalidContract(
                    "asset class does not match contract metadata",
                ));
            }
        }
        Ok(Self {
            id,
            symbol,
            asset_class,
            venue,
            currency,
            price_increment_ticks,
            quantity_increment_ticks,
            contract,
            provider_symbol,
        })
    }

    /// Returns whether a price expressed in ticks obeys the instrument increment.
    #[must_use]
    pub const fn accepts_price_ticks(&self, ticks: i64) -> bool {
        ticks >= 0 && ticks % self.price_increment_ticks == 0
    }

    /// Returns whether a quantity expressed in ticks obeys the instrument increment.
    #[must_use]
    pub const fn accepts_quantity_ticks(&self, ticks: i64) -> bool {
        ticks >= 0 && ticks % self.quantity_increment_ticks == 0
    }

    /// Returns whether the contract was valid on an exchange-local day number.
    #[must_use]
    pub const fn valid_on_day(&self, day: u32) -> bool {
        match &self.contract {
            Contract::Option { expiry_day, .. } | Contract::Future { expiry_day, .. } => {
                day <= *expiry_day
            }
            Contract::Listing | Contract::Fx { .. } | Contract::Crypto { .. } => true,
        }
    }
}

/// Versioned corporate-action payload retained separately from instrument
/// identity. `knowledge_day` is the point-in-time boundary used by replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CorporateAction {
    /// Share split or reverse split. A 2-for-1 split is `2/1`.
    Split {
        /// New shares per old share.
        numerator: u64,
        /// Old shares represented by the numerator.
        denominator: u64,
    },
    /// Cash dividend in canonical currency ticks per share.
    CashDividend {
        /// Dividend amount in currency ticks.
        amount_ticks: i64,
    },
    /// Issuer symbol change effective on the action day.
    SymbolChange {
        /// Replacement canonical symbol.
        new_symbol: String,
    },
}

/// Immutable corporate-action record with announcement and knowledge timing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorporateActionRecord {
    /// Instrument affected by the action.
    pub instrument_id: InstrumentId,
    /// Exchange-local effective day.
    pub effective_day: u32,
    /// Exchange-local announcement day.
    pub announced_day: u32,
    /// Day on which the runtime learned this version.
    pub knowledge_day: u32,
    /// Monotonic source version for corrections.
    pub version: u64,
    /// Action payload.
    pub action: CorporateAction,
}

/// Corporate-action validation or lookup failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CorporateActionError {
    /// A date or version relationship is invalid.
    InvalidTiming,
    /// Action-specific fields are invalid.
    InvalidPayload,
    /// A version was already present with different content.
    ConflictingVersion,
    /// Integer adjustment overflowed.
    Overflow,
}

/// Point-in-time corporate-action history.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CorporateActionHistory {
    records: BTreeMap<InstrumentId, BTreeMap<u64, CorporateActionRecord>>,
}

impl CorporateActionHistory {
    /// Inserts one immutable action version.
    ///
    /// # Errors
    /// Returns [`CorporateActionError`] for invalid timing, payload, or a
    /// conflicting existing version.
    pub fn insert(&mut self, record: CorporateActionRecord) -> Result<(), CorporateActionError> {
        if record.effective_day < record.announced_day
            || record.announced_day > record.knowledge_day
            || record.version == 0
        {
            return Err(CorporateActionError::InvalidTiming);
        }
        match &record.action {
            CorporateAction::Split {
                numerator,
                denominator,
            } if *numerator > 0 && *denominator > 0 => {}
            CorporateAction::CashDividend { amount_ticks } if *amount_ticks >= 0 => {}
            CorporateAction::SymbolChange { new_symbol } if !new_symbol.trim().is_empty() => {}
            _ => return Err(CorporateActionError::InvalidPayload),
        }
        let versions = self.records.entry(record.instrument_id).or_default();
        if let Some(existing) = versions.get(&record.version) {
            return if existing == &record {
                Ok(())
            } else {
                Err(CorporateActionError::ConflictingVersion)
            };
        }
        versions.insert(record.version, record);
        Ok(())
    }

    /// Returns actions that were knowable by `knowledge_day`, in effective-day
    /// and version order for deterministic replay.
    #[must_use]
    pub fn known_as_of(
        &self,
        instrument_id: InstrumentId,
        knowledge_day: u32,
    ) -> Vec<&CorporateActionRecord> {
        let mut actions: Vec<_> = self
            .records
            .get(&instrument_id)
            .into_iter()
            .flat_map(|versions| versions.values())
            .filter(|record| record.knowledge_day <= knowledge_day)
            .collect();
        actions.sort_by_key(|record| (record.effective_day, record.version));
        actions
    }

    /// Applies known split factors to a historical price using deterministic
    /// nearest-integer rounding. Cash dividends and symbol changes remain
    /// available through [`Self::known_as_of`] for caller-specific treatment.
    ///
    /// # Errors
    /// Returns [`CorporateActionError::Overflow`] for invalid arithmetic.
    pub fn adjusted_price_ticks(
        &self,
        instrument_id: InstrumentId,
        price_ticks: i64,
        observation_day: u32,
        knowledge_day: u32,
    ) -> Result<i64, CorporateActionError> {
        if price_ticks < 0 {
            return Err(CorporateActionError::InvalidPayload);
        }
        let mut numerator = 1_i128;
        let mut denominator = 1_i128;
        for record in self.known_as_of(instrument_id, knowledge_day) {
            if record.effective_day <= observation_day {
                continue;
            }
            if let CorporateAction::Split {
                numerator: split_numerator,
                denominator: split_denominator,
            } = record.action
            {
                denominator = denominator
                    .checked_mul(i128::from(split_numerator))
                    .ok_or(CorporateActionError::Overflow)?;
                numerator = numerator
                    .checked_mul(i128::from(split_denominator))
                    .ok_or(CorporateActionError::Overflow)?;
            }
        }
        let scaled = i128::from(price_ticks)
            .checked_mul(numerator)
            .ok_or(CorporateActionError::Overflow)?;
        let rounded = (scaled + denominator / 2) / denominator;
        i64::try_from(rounded).map_err(|_| CorporateActionError::Overflow)
    }
}

/// A point-in-time FX quote with explicit source and knowledge provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FxRate {
    /// Base ISO currency.
    pub base: String,
    /// Quote ISO currency.
    pub quote: String,
    /// Positive rational numerator.
    pub numerator: u64,
    /// Positive rational denominator.
    pub denominator: u64,
    /// Provider/source identifier.
    pub source: String,
    /// Exchange/source timestamp in nanoseconds.
    pub as_of_ns: i64,
    /// Runtime knowledge timestamp in nanoseconds.
    pub knowledge_ns: i64,
}

/// FX rate-book validation and conversion errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FxError {
    /// Currency code, rational, source, or timestamp is invalid.
    InvalidRate,
    /// No rate was knowable at the requested point in time.
    MissingRate,
    /// Conversion arithmetic overflowed.
    Overflow,
}

/// Versioned FX rates keyed by currency pair.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FxRateBook {
    rates: BTreeMap<(String, String), Vec<FxRate>>,
}

impl FxRateBook {
    /// Adds a validated rate, retaining source corrections in timestamp order.
    ///
    /// # Errors
    /// Returns [`FxError::InvalidRate`] for malformed currency/rational data.
    pub fn insert(&mut self, rate: FxRate) -> Result<(), FxError> {
        if !valid_currency(&rate.base)
            || !valid_currency(&rate.quote)
            || rate.base == rate.quote
            || rate.numerator == 0
            || rate.denominator == 0
            || rate.source.trim().is_empty()
            || rate.as_of_ns < 0
            || rate.knowledge_ns < rate.as_of_ns
        {
            return Err(FxError::InvalidRate);
        }
        let rates = self
            .rates
            .entry((rate.base.clone(), rate.quote.clone()))
            .or_default();
        if let Some(existing) = rates
            .iter_mut()
            .find(|existing| existing.as_of_ns == rate.as_of_ns && existing.source == rate.source)
        {
            *existing = rate;
        } else {
            rates.push(rate);
            rates.sort_by_key(|rate| (rate.as_of_ns, rate.knowledge_ns));
        }
        Ok(())
    }

    /// Converts an amount using the latest rate knowable at `knowledge_ns`.
    ///
    /// # Errors
    /// Returns [`FxError`] when no point-in-time rate exists or arithmetic is
    /// outside the canonical integer range.
    pub fn convert_ticks(
        &self,
        amount_ticks: i64,
        base: &str,
        quote: &str,
        as_of_ns: i64,
        knowledge_ns: i64,
    ) -> Result<i64, FxError> {
        if amount_ticks < 0 || as_of_ns < 0 || knowledge_ns < 0 {
            return Err(FxError::InvalidRate);
        }
        let rates = self
            .rates
            .get(&(base.to_owned(), quote.to_owned()))
            .ok_or(FxError::MissingRate)?;
        let rate = rates
            .iter()
            .filter(|rate| rate.as_of_ns <= as_of_ns && rate.knowledge_ns <= knowledge_ns)
            .max_by_key(|rate| (rate.as_of_ns, rate.knowledge_ns))
            .ok_or(FxError::MissingRate)?;
        let converted = i128::from(amount_ticks)
            .checked_mul(i128::from(rate.numerator))
            .ok_or(FxError::Overflow)?;
        i64::try_from((converted + i128::from(rate.denominator) / 2) / i128::from(rate.denominator))
            .map_err(|_| FxError::Overflow)
    }
}

fn valid_currency(value: &str) -> bool {
    value.len() == 3 && value.bytes().all(|byte| byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use insider_common_types::InstrumentId;

    use super::{
        AssetClass, Contract, CorporateAction, CorporateActionHistory, CorporateActionRecord,
        FxRate, FxRateBook, Instrument, InstrumentError, InstrumentSpec,
    };

    fn stock() -> Result<Instrument, InstrumentError> {
        let Some(id) = InstrumentId::new(1).ok() else {
            return Err(InstrumentError::EmptyField("id"));
        };
        Instrument::new(InstrumentSpec {
            id,
            symbol: String::from("AAPL"),
            asset_class: AssetClass::Equity,
            venue: String::from("NASDAQ"),
            currency: String::from("USD"),
            price_increment_ticks: 5,
            quantity_increment_ticks: 1,
            contract: Contract::Listing,
            provider_symbol: String::from("AAPL"),
        })
    }

    #[test]
    fn canonical_stock_validates_precision_and_identity() {
        let Ok(stock) = stock() else {
            return;
        };
        assert!(stock.accepts_price_ticks(100));
        assert!(!stock.accepts_price_ticks(101));
        assert_eq!(stock.symbol, "AAPL");
    }

    #[test]
    fn mismatched_contract_and_invalid_currency_are_rejected() {
        let Some(id) = InstrumentId::new(2).ok() else {
            return;
        };
        let wrong = Instrument::new(InstrumentSpec {
            id,
            symbol: String::from("AAPL"),
            asset_class: AssetClass::Option,
            venue: String::from("NASDAQ"),
            currency: String::from("USD"),
            price_increment_ticks: 1,
            quantity_increment_ticks: 1,
            contract: Contract::Listing,
            provider_symbol: String::from("AAPL"),
        });
        assert!(matches!(wrong, Err(InstrumentError::InvalidContract(_))));
        let invalid_currency = Instrument::new(InstrumentSpec {
            id,
            symbol: String::from("AAPL"),
            asset_class: AssetClass::Equity,
            venue: String::from("NASDAQ"),
            currency: String::from("US"),
            price_increment_ticks: 1,
            quantity_increment_ticks: 1,
            contract: Contract::Listing,
            provider_symbol: String::from("AAPL"),
        });
        assert_eq!(invalid_currency, Err(InstrumentError::InvalidCurrency));
    }

    #[test]
    fn corporate_actions_and_fx_are_point_in_time_and_provenanced() {
        let Some(instrument_id) = InstrumentId::new(3).ok() else {
            return;
        };
        let mut actions = CorporateActionHistory::default();
        assert!(
            actions
                .insert(CorporateActionRecord {
                    instrument_id,
                    effective_day: 20,
                    announced_day: 10,
                    knowledge_day: 12,
                    version: 1,
                    action: CorporateAction::Split {
                        numerator: 2,
                        denominator: 1,
                    },
                })
                .is_ok()
        );
        assert_eq!(actions.known_as_of(instrument_id, 11).len(), 0);
        assert_eq!(
            actions
                .adjusted_price_ticks(instrument_id, 101, 10, 12)
                .ok(),
            Some(51)
        );

        let mut fx = FxRateBook::default();
        assert!(
            fx.insert(FxRate {
                base: "EUR".into(),
                quote: "USD".into(),
                numerator: 11,
                denominator: 10,
                source: "fixture".into(),
                as_of_ns: 100,
                knowledge_ns: 110,
            })
            .is_ok()
        );
        assert_eq!(
            fx.convert_ticks(100, "EUR", "USD", 100, 110).ok(),
            Some(110)
        );
        assert!(fx.convert_ticks(100, "EUR", "USD", 100, 109).is_err());
    }
}
