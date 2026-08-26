//! Versioned, atomically published configuration snapshots.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, RwLock};

/// Typed configuration scalar.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// UTF-8 configuration string.
    String(String),
    /// Signed integer configuration value.
    Integer(i64),
    /// Floating-point configuration value.
    Float(f64),
    /// Boolean configuration value.
    Boolean(bool),
}

/// Immutable key/value configuration map.
pub type Settings = BTreeMap<String, Value>;

const MAX_CFG_BYTES: usize = 1_048_576;
const MAX_KEY_BYTES: usize = 256;
const MAX_STRING_BYTES: usize = 16_384;

/// Parses deterministic `.cfg` text (`key = value`) into typed settings.
/// Strings use JSON-style double quotes; comments begin with `#`.
///
/// # Errors
/// Returns a stable diagnostic for malformed lines, duplicate keys, invalid
/// scalar values, or bounded-input violations.
pub fn parse_cfg(text: &str) -> Result<Settings, String> {
    if text.len() > MAX_CFG_BYTES {
        return Err("configuration exceeds 1 MiB bound".into());
    }
    let mut settings = BTreeMap::new();
    for (line_number, raw) in text.lines().enumerate() {
        let line = strip_comment(raw).trim();
        if line.is_empty() {
            continue;
        }
        let (key, raw_value) = line
            .split_once('=')
            .ok_or_else(|| format!("line {}: expected key = value", line_number + 1))?;
        let key = key.trim();
        let raw_value = raw_value.trim();
        if key.is_empty() || key.len() > MAX_KEY_BYTES || !valid_key(key) {
            return Err(format!(
                "line {}: invalid configuration key",
                line_number + 1
            ));
        }
        if settings.contains_key(key) {
            return Err(format!(
                "line {}: duplicate configuration key",
                line_number + 1
            ));
        }
        let value = if raw_value.starts_with('"') {
            if !raw_value.ends_with('"') || raw_value.len() < 2 {
                return Err(format!("line {}: unterminated string", line_number + 1));
            }
            let value = raw_value[1..raw_value.len() - 1]
                .replace("\\\\", "\\")
                .replace("\\\"", "\"");
            if value.len() > MAX_STRING_BYTES {
                return Err(format!("line {}: string exceeds bound", line_number + 1));
            }
            Value::String(value)
        } else if raw_value == "true" || raw_value == "false" {
            Value::Boolean(raw_value == "true")
        } else if raw_value.contains('.') || raw_value.contains('e') || raw_value.contains('E') {
            Value::Float(
                raw_value.parse::<f64>().map_err(|_| {
                    format!("line {}: invalid floating-point value", line_number + 1)
                })?,
            )
        } else {
            Value::Integer(
                raw_value
                    .parse::<i64>()
                    .map_err(|_| format!("line {}: invalid integer value", line_number + 1))?,
            )
        };
        if let Value::Float(value) = value {
            if !value.is_finite() {
                return Err(format!("line {}: non-finite float", line_number + 1));
            }
            settings.insert(key.to_owned(), Value::Float(value));
        } else {
            settings.insert(key.to_owned(), value);
        }
    }
    Ok(settings)
}

/// Removes an unquoted `#` comment marker while preserving hashes inside a
/// quoted string (for example a URL fragment or a provider secret reference).
fn strip_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && byte == b'\\' {
            escaped = true;
            continue;
        }
        if byte == b'"' {
            quoted = !quoted;
        } else if byte == b'#' && !quoted {
            return &line[..index];
        }
    }
    line
}

/// Renders settings into deterministic `.cfg` text sorted by key.
#[must_use]
pub fn render_cfg(settings: &Settings) -> String {
    let mut output = String::new();
    for (key, value) in settings {
        output.push_str(key);
        output.push_str(" = ");
        match value {
            Value::String(value) => {
                output.push('"');
                output.push_str(&value.replace('\\', "\\\\").replace('"', "\\\""));
                output.push('"');
            }
            Value::Integer(value) => output.push_str(&value.to_string()),
            Value::Float(value) => {
                let _ = write!(output, "{value:.17}");
            }
            Value::Boolean(value) => output.push_str(if *value { "true" } else { "false" }),
        }
        output.push('\n');
    }
    output
}

fn valid_key(key: &str) -> bool {
    key.bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// A published configuration version.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    /// Monotonically increasing version.
    pub version: u64,
    /// Immutable settings.
    pub settings: Arc<Settings>,
}

/// Failure to publish a configuration candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReloadError {
    /// Candidate did not satisfy validation.
    Invalid(String),
    /// Another reload won the compare-and-swap race.
    Conflict {
        /// Version supplied by the writer.
        expected: u64,
        /// Version currently published.
        actual: u64,
    },
    /// Internal state lock was poisoned.
    Unavailable,
}

/// Atomic configuration store. Readers never observe a partially validated map.
pub struct ConfigStore {
    current: RwLock<Snapshot>,
}

impl ConfigStore {
    /// Creates a store at version one.
    #[must_use]
    pub fn new(settings: Settings) -> Self {
        Self {
            current: RwLock::new(Snapshot {
                version: 1,
                settings: Arc::new(settings),
            }),
        }
    }

    /// Returns the current immutable snapshot.
    ///
    /// # Errors
    /// Returns [`ReloadError::Unavailable`] if the store lock is poisoned.
    pub fn snapshot(&self) -> Result<Snapshot, ReloadError> {
        self.current
            .read()
            .map(|snapshot| snapshot.clone())
            .map_err(|_| ReloadError::Unavailable)
    }

    /// Validates and publishes a candidate only if the expected version is current.
    ///
    /// # Errors
    /// Returns [`ReloadError::Invalid`] for rejected settings,
    /// [`ReloadError::Conflict`] for a stale version, or
    /// [`ReloadError::Unavailable`] if the store lock is poisoned.
    pub fn reload<F>(
        &self,
        expected_version: u64,
        candidate: Settings,
        validate: F,
    ) -> Result<Snapshot, ReloadError>
    where
        F: FnOnce(&Settings) -> Result<(), String>,
    {
        validate(&candidate).map_err(ReloadError::Invalid)?;
        let Ok(mut current) = self.current.write() else {
            return Err(ReloadError::Unavailable);
        };
        if current.version != expected_version {
            return Err(ReloadError::Conflict {
                expected: expected_version,
                actual: current.version,
            });
        }
        let next = Snapshot {
            version: current.version.saturating_add(1),
            settings: Arc::new(candidate),
        };
        *current = next.clone();
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{ConfigStore, ReloadError, Value, parse_cfg};

    fn settings(value: i64) -> BTreeMap<String, Value> {
        BTreeMap::from([(String::from("limit"), Value::Integer(value))])
    }

    #[test]
    fn hash_inside_quoted_value_is_not_a_comment() {
        let parsed_result =
            parse_cfg("endpoint = \"https://example.test/v1#fragment\" # trailing comment\n");
        assert!(parsed_result.is_ok(), "quoted hash should parse");
        let parsed = parsed_result.unwrap_or_default();
        assert_eq!(
            parsed.get("endpoint"),
            Some(&Value::String(String::from(
                "https://example.test/v1#fragment"
            )))
        );
    }

    #[test]
    fn checked_in_example_configuration_is_valid() {
        let settings_result = parse_cfg(include_str!("../../../config/example.cfg"));
        assert!(
            settings_result.is_ok(),
            "checked-in example cfg should remain parseable"
        );
        let settings = settings_result.unwrap_or_default();
        assert_eq!(settings.get("risk.max_leverage"), Some(&Value::Float(2.0)));
        assert_eq!(
            settings.get("risk.max_message_rate"),
            Some(&Value::Integer(20))
        );
        assert_eq!(
            settings.get("news.retry_max_ms"),
            Some(&Value::Integer(60_000))
        );
        assert_eq!(
            settings.get("reconciliation.poll_ms"),
            Some(&Value::Integer(30_000))
        );
        assert_eq!(
            settings.get("alerts.webhook_timeout_ms"),
            Some(&Value::Integer(2_000))
        );
        assert_eq!(
            settings.get("market.yahoo_price_scale"),
            Some(&Value::Integer(10_000))
        );
    }

    #[test]
    fn invalid_reload_does_not_publish_or_increment_version() {
        let store = ConfigStore::new(settings(1));
        let result = store.reload(1, settings(-1), |candidate| {
            if matches!(candidate.get("limit"), Some(Value::Integer(value)) if *value >= 0) {
                Ok(())
            } else {
                Err(String::from("limit must be non-negative"))
            }
        });
        assert!(matches!(result, Err(ReloadError::Invalid(_))));
        assert_eq!(
            store.snapshot().ok().map(|snapshot| snapshot.version),
            Some(1)
        );
    }

    #[test]
    fn successful_reload_is_atomic_and_stale_writer_is_rejected() {
        let store = ConfigStore::new(settings(1));
        assert_eq!(
            store
                .reload(1, settings(2), |_| Ok(()))
                .ok()
                .map(|snapshot| snapshot.version),
            Some(2)
        );
        let stale = store.reload(1, settings(3), |_| Ok(()));
        assert_eq!(
            stale,
            Err(ReloadError::Conflict {
                expected: 1,
                actual: 2
            })
        );
        assert_eq!(
            store
                .snapshot()
                .ok()
                .and_then(|snapshot| snapshot.settings.get("limit").cloned()),
            Some(Value::Integer(2))
        );
    }
}
