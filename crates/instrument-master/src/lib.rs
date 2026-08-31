//! Canonical instrument registry and unambiguous provider-symbol resolution.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use insider_common_types::InstrumentId;
use insider_market_types::{AssetClass, Instrument};

/// Lookup failure that callers must handle before creating market/order state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolveError {
    /// No instrument is known for the provider/venue/symbol tuple.
    NotFound,
    /// More than one canonical instrument matches.
    Ambiguous(Vec<InstrumentId>),
    /// A requested identity is not present in the catalog.
    UnknownId,
    /// The symbol exists but every matching contract is outside its validity window.
    Stale(Vec<InstrumentId>),
    /// The symbol exists but every matching contract is outside the enabled asset universe.
    Unsupported(Vec<InstrumentId>),
    /// Provider identity is blank or exceeds the catalog bound.
    InvalidProvider,
}

/// Provider-qualified lookup key.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProviderKey {
    /// Provider name, such as `ibkr`.
    pub provider: String,
    /// Venue/exchange identifier.
    pub venue: String,
    /// Provider's symbol/contract identifier.
    pub symbol: String,
}

/// Exchange-local session calendar using integer day/minute coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradingCalendar {
    /// First regular-session minute, inclusive, in `[0, 1440)`.
    pub open_minute: u16,
    /// Last regular-session minute, exclusive, in `(0, 1440]`.
    pub close_minute: u16,
    /// Weekday numbers closed by convention: Monday `0` through Sunday `6`.
    pub closed_weekdays: std::collections::BTreeSet<u8>,
    /// Exchange-local day numbers closed for holidays.
    pub holidays: BTreeSet<u32>,
}

impl TradingCalendar {
    /// Creates a regular weekday session calendar.
    #[must_use]
    pub fn weekday_session(open_minute: u16, close_minute: u16) -> Option<Self> {
        (open_minute < close_minute && close_minute <= 1_440).then(|| Self {
            open_minute,
            close_minute,
            closed_weekdays: BTreeSet::from([5, 6]),
            holidays: BTreeSet::new(),
        })
    }

    /// Returns whether an exchange-local day/minute is in a regular session.
    #[must_use]
    pub fn is_open(&self, day: u32, minute: u16) -> bool {
        let weekday = (day % 7) as u8;
        !self.closed_weekdays.contains(&weekday)
            && !self.holidays.contains(&day)
            && (self.open_minute..self.close_minute).contains(&minute)
    }

    /// Adds a holiday to the immutable-by-convention calendar value.
    pub fn add_holiday(&mut self, day: u32) {
        self.holidays.insert(day);
    }
}

/// Immutable-in-read catalog of canonical instruments.
#[derive(Default)]
pub struct Catalog {
    instruments: BTreeMap<InstrumentId, Instrument>,
    provider_keys: BTreeMap<ProviderKey, BTreeSet<InstrumentId>>,
}

impl Catalog {
    /// Creates an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts an instrument and indexes its provider identity.
    ///
    /// # Errors
    /// Returns [`ResolveError::UnknownId`] if an existing identity is replaced
    /// with a different instrument definition.
    pub fn insert(&mut self, instrument: Instrument, provider: String) -> Result<(), ResolveError> {
        if provider.trim().is_empty() || provider.len() > 64 {
            return Err(ResolveError::InvalidProvider);
        }
        if let Some(existing) = self.instruments.get(&instrument.id) {
            if existing != &instrument {
                return Err(ResolveError::UnknownId);
            }
            let key = ProviderKey {
                provider,
                venue: instrument.venue.clone(),
                symbol: instrument.provider_symbol.clone(),
            };
            self.provider_keys
                .entry(key)
                .or_default()
                .insert(instrument.id);
            return Ok(());
        }
        let key = ProviderKey {
            provider,
            venue: instrument.venue.clone(),
            symbol: instrument.provider_symbol.clone(),
        };
        self.provider_keys
            .entry(key)
            .or_default()
            .insert(instrument.id);
        self.instruments.insert(instrument.id, instrument);
        Ok(())
    }

    /// Resolves a provider identity, preserving ambiguity instead of guessing.
    ///
    /// # Errors
    /// Returns [`ResolveError`] when no safe canonical identity can be returned.
    pub fn resolve(&self, key: &ProviderKey) -> Result<&Instrument, ResolveError> {
        let Some(ids) = self.provider_keys.get(key) else {
            return Err(ResolveError::NotFound);
        };
        if ids.len() != 1 {
            return Err(ResolveError::Ambiguous(ids.iter().copied().collect()));
        }
        let Some(id) = ids.first() else {
            return Err(ResolveError::NotFound);
        };
        self.instruments.get(id).ok_or(ResolveError::UnknownId)
    }

    /// Resolves a display symbol without guessing across ambiguous contracts.
    ///
    /// Validity and enabled asset classes are checked before returning an ID;
    /// callers must handle stale, unsupported, and ambiguous results explicitly.
    ///
    /// # Errors
    /// Returns [`ResolveError`] when the display symbol cannot identify exactly
    /// one currently supported canonical instrument.
    pub fn resolve_symbol(
        &self,
        symbol: &str,
        day: u32,
        supported_assets: &BTreeSet<AssetClass>,
    ) -> Result<&Instrument, ResolveError> {
        let normalized = symbol.trim().to_uppercase();
        let candidates: Vec<InstrumentId> = self
            .instruments
            .values()
            .filter(|instrument| instrument.symbol.to_uppercase() == normalized)
            .map(|instrument| instrument.id)
            .collect();
        if candidates.is_empty() {
            return Err(ResolveError::NotFound);
        }
        let current: Vec<InstrumentId> = candidates
            .iter()
            .copied()
            .filter(|id| {
                self.instruments.get(id).is_some_and(|instrument| {
                    instrument.valid_on_day(day)
                        && supported_assets.contains(&instrument.asset_class)
                })
            })
            .collect();
        if current.len() > 1 {
            return Err(ResolveError::Ambiguous(current));
        }
        if let Some(id) = current.first() {
            return self.instruments.get(id).ok_or(ResolveError::UnknownId);
        }
        let valid: Vec<InstrumentId> = candidates
            .iter()
            .copied()
            .filter(|id| {
                self.instruments
                    .get(id)
                    .is_some_and(|instrument| instrument.valid_on_day(day))
            })
            .collect();
        if valid.is_empty() {
            Err(ResolveError::Stale(candidates))
        } else {
            Err(ResolveError::Unsupported(valid))
        }
    }

    /// Looks up an immutable canonical identity.
    #[must_use]
    pub fn get(&self, id: InstrumentId) -> Option<&Instrument> {
        self.instruments.get(&id)
    }

    /// Number of canonical instruments.
    #[must_use]
    pub fn len(&self) -> usize {
        self.instruments.len()
    }

    /// Whether no instruments are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.instruments.is_empty()
    }

    /// Iterates over every canonical definition for runtime registration.
    pub fn instruments(&self) -> impl Iterator<Item = &Instrument> {
        self.instruments.values()
    }

    /// Returns whether an instrument is contract-valid and tradable at a
    /// calendar coordinate.
    #[must_use]
    pub fn tradable_at(
        &self,
        id: InstrumentId,
        day: u32,
        minute: u16,
        calendar: &TradingCalendar,
    ) -> bool {
        self.get(id)
            .is_some_and(|instrument| instrument.valid_on_day(day) && calendar.is_open(day, minute))
    }
}

#[cfg(test)]
mod tests {
    use insider_common_types::InstrumentId;
    use insider_market_types::{AssetClass, Contract, Instrument, InstrumentSpec};

    use super::{Catalog, ProviderKey, ResolveError};

    fn instrument(id: u128, symbol: &str, venue: &str) -> Option<Instrument> {
        Instrument::new(InstrumentSpec {
            id: InstrumentId::new(id).ok()?,
            symbol: symbol.to_owned(),
            asset_class: AssetClass::Equity,
            venue: venue.to_owned(),
            currency: String::from("USD"),
            price_increment_ticks: 1,
            quantity_increment_ticks: 1,
            contract: Contract::Listing,
            provider_symbol: symbol.to_owned(),
        })
        .ok()
    }

    #[test]
    fn provider_lookup_returns_canonical_identity() {
        let Some(value) = instrument(1, "AAPL", "NASDAQ") else {
            return;
        };
        let mut catalog = Catalog::new();
        assert!(catalog.insert(value, String::from("ibkr")).is_ok());
        let key = ProviderKey {
            provider: String::from("ibkr"),
            venue: String::from("NASDAQ"),
            symbol: String::from("AAPL"),
        };
        assert_eq!(
            catalog.resolve(&key).ok().map(|item| item.id),
            InstrumentId::new(1).ok()
        );
    }

    #[test]
    fn duplicate_provider_symbol_is_ambiguous_not_guessed() {
        let Some(first) = instrument(1, "ABC", "SMART") else {
            return;
        };
        let Some(second) = instrument(2, "ABC", "SMART") else {
            return;
        };
        let mut catalog = Catalog::new();
        assert!(catalog.insert(first, String::from("ibkr")).is_ok());
        assert!(catalog.insert(second, String::from("ibkr")).is_ok());
        let key = ProviderKey {
            provider: String::from("ibkr"),
            venue: String::from("SMART"),
            symbol: String::from("ABC"),
        };
        assert!(
            matches!(catalog.resolve(&key), Err(ResolveError::Ambiguous(ids)) if ids.len() == 2)
        );
    }

    #[test]
    fn one_canonical_instrument_is_indexed_for_multiple_providers() {
        let Some(value) = instrument(7, "MSFT", "NASDAQ") else {
            return;
        };
        let mut catalog = Catalog::new();
        assert!(catalog.insert(value.clone(), String::from("ibkr")).is_ok());
        assert!(catalog.insert(value, String::from("yahoo")).is_ok());
        for provider in ["ibkr", "yahoo"] {
            let key = ProviderKey {
                provider: provider.to_owned(),
                venue: String::from("NASDAQ"),
                symbol: String::from("MSFT"),
            };
            assert_eq!(
                catalog.resolve(&key).ok().map(|item| item.id),
                InstrumentId::new(7).ok()
            );
        }
    }

    #[test]
    fn provider_identity_is_bounded_before_indexing() {
        let Some(value) = instrument(8, "TSLA", "NASDAQ") else {
            return;
        };
        let mut catalog = Catalog::new();
        assert!(matches!(
            catalog.insert(value.clone(), String::from(" ")),
            Err(ResolveError::InvalidProvider)
        ));
        assert!(matches!(
            catalog.insert(value, "x".repeat(65)),
            Err(ResolveError::InvalidProvider)
        ));
        assert!(catalog.is_empty());
    }
}
