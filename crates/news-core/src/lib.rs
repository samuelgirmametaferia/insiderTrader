//! Provider-neutral news normalization, deduplication, and deterministic ranking.

#![forbid(unsafe_code)]

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "news_core";

use std::collections::{BTreeMap, BTreeSet};

/// Provider adapter boundary. HTTP/auth/rate limiting stay outside the core.
pub trait NewsProvider: Send + Sync {
    /// Provider identity used in normalized records.
    fn provider_id(&self) -> &str;
    /// Fetches a bounded batch as of the supplied wall-clock time.
    ///
    /// # Errors
    /// Returns a provider/transport diagnostic while leaving the local store unchanged.
    fn fetch(&self, now_ms: i64) -> Result<Vec<NewsItem>, String>;
}

/// A bounded provider page and the cursor for the next page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBatch {
    /// Normalized or raw items returned by the provider.
    pub items: Vec<NewsItem>,
    /// Opaque cursor to request after this page is durably stored.
    pub next_cursor: Option<String>,
}

/// Cursor-capable provider boundary for restart-safe pagination.
pub trait CursorProvider: Send + Sync {
    /// Returns the provider manifest when the adapter declares one.
    fn manifest(&self) -> Option<insider_provider_core::ProviderManifest> {
        None
    }

    /// Provider identity used in normalized records.
    fn provider_id(&self) -> &str;
    /// Fetches one bounded page from an opaque cursor.
    ///
    /// # Errors
    /// Returns a provider/transport diagnostic without changing the local cursor.
    fn fetch_page(&self, cursor: Option<&str>, now_ms: i64) -> Result<ProviderBatch, String>;
}

/// Durable cursor projection boundary.
///
/// Implementations normally append the cursor update to the journal or commit
/// it in a transactional read-model database. The generation argument is an
/// optimistic-concurrency token: a stale worker must not overwrite a newer
/// cursor.
pub trait CursorCommitter {
    /// Commits a fetched page after all normalized items have been stored.
    ///
    /// Implementations that need durable article provenance can override this
    /// hook to persist the accepted page before committing the cursor. The
    /// default preserves the original cursor-only contract.
    ///
    /// # Errors
    /// Returns a persistence or optimistic-concurrency diagnostic.
    fn commit_page(
        &mut self,
        provider_id: &str,
        expected_generation: u64,
        next_cursor: Option<&str>,
        _items: &[NewsItem],
    ) -> Result<(), String> {
        self.commit_cursor(provider_id, expected_generation, next_cursor)
    }

    /// Commits a provider cursor after its page has been stored.
    ///
    /// # Errors
    /// Returns a persistence or optimistic-concurrency diagnostic. The caller
    /// keeps its previous in-memory cursor when this fails.
    fn commit_cursor(
        &mut self,
        provider_id: &str,
        expected_generation: u64,
        next_cursor: Option<&str>,
    ) -> Result<(), String>;
}

/// Durable pagination state owned by the caller's journal/read model.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CursorState {
    /// Last committed provider cursor.
    pub cursor: Option<String>,
    /// Monotonic commit generation.
    pub generation: u64,
}

/// Classification used by provider adapters to decide whether a failed
/// request is safe to retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    /// Provider explicitly requested a delay (for example HTTP 429).
    RateLimited {
        /// Provider-advertised delay before retrying.
        retry_after_ms: u64,
    },
    /// A bounded transport/5xx failure that may succeed later.
    Transient,
    /// Authentication, schema, or other non-retryable failure.
    Permanent,
}

/// Exponential retry policy with a hard attempt and delay bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Maximum number of retries after the initial request.
    pub max_retries: u32,
    /// Delay before the first transient retry.
    pub base_delay_ms: u64,
    /// Maximum delay for any retry.
    pub max_delay_ms: u64,
}

impl RetryPolicy {
    /// Validates and constructs a retry policy.
    #[must_use]
    pub const fn new(max_retries: u32, base_delay_ms: u64, max_delay_ms: u64) -> Option<Self> {
        if base_delay_ms > 0 && max_delay_ms >= base_delay_ms {
            Some(Self {
                max_retries,
                base_delay_ms,
                max_delay_ms,
            })
        } else {
            None
        }
    }
}

/// Decision returned after classifying one provider failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDecision {
    /// Do not issue a request until this wall-clock time.
    RetryAt(i64),
    /// Route the failure to a dead-letter diagnostic.
    DeadLetter,
}

/// Bounded retry state owned by one provider cursor worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub struct RetryState {
    /// Number of retries already scheduled for the current failure series.
    pub retries: u32,
    /// Earliest allowed next request, if one is scheduled.
    pub next_retry_ms: Option<i64>,
}

impl RetryState {
    /// Records one failure and returns a deterministic retry/dead-letter action.
    #[must_use]
    pub fn on_failure(
        &mut self,
        now_ms: i64,
        class: RetryClass,
        policy: RetryPolicy,
    ) -> RetryDecision {
        let delay = match class {
            RetryClass::Permanent => return RetryDecision::DeadLetter,
            RetryClass::RateLimited { retry_after_ms } => retry_after_ms.min(policy.max_delay_ms),
            RetryClass::Transient => {
                if self.retries >= policy.max_retries {
                    return RetryDecision::DeadLetter;
                }
                let shift = self.retries.min(63);
                policy
                    .base_delay_ms
                    .saturating_mul(1_u64 << shift)
                    .min(policy.max_delay_ms)
            }
        };
        if matches!(class, RetryClass::RateLimited { .. }) && self.retries >= policy.max_retries {
            return RetryDecision::DeadLetter;
        }
        self.retries = self.retries.saturating_add(1);
        let retry_at = now_ms.saturating_add(i64::try_from(delay).unwrap_or(i64::MAX));
        self.next_retry_ms = Some(retry_at);
        RetryDecision::RetryAt(retry_at)
    }

    /// Clears retry state after a successful page commit.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Returns whether a request is currently permitted.
    #[must_use]
    pub fn ready(&self, now_ms: i64) -> bool {
        self.next_retry_ms.is_none_or(|retry_at| now_ms >= retry_at)
    }
}

/// Fixed-window request limiter for provider workers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestLimiter {
    max_requests: u32,
    window_ms: i64,
    window_start_ms: Option<i64>,
    requests: u32,
}

impl RequestLimiter {
    /// Creates a limiter with a finite request count and wall-clock window.
    #[must_use]
    pub const fn new(max_requests: u32, window_ms: i64) -> Option<Self> {
        if max_requests > 0 && window_ms > 0 {
            Some(Self {
                max_requests,
                window_ms,
                window_start_ms: None,
                requests: 0,
            })
        } else {
            None
        }
    }

    /// Consumes one request slot or returns the earliest allowed timestamp.
    ///
    /// # Errors
    /// Returns the wall-clock timestamp at which the next request is allowed
    /// when the current window is exhausted or time moved backwards.
    pub fn allow(&mut self, now_ms: i64) -> Result<(), i64> {
        let start = *self.window_start_ms.get_or_insert(now_ms);
        if now_ms < start {
            return Err(start);
        }
        if now_ms.saturating_sub(start) >= self.window_ms {
            self.window_start_ms = Some(now_ms);
            self.requests = 0;
        }
        if self.requests >= self.max_requests {
            return Err(self
                .window_start_ms
                .unwrap_or(now_ms)
                .saturating_add(self.window_ms));
        }
        self.requests = self.requests.saturating_add(1);
        Ok(())
    }
}

/// Ingestion failure from an upstream provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IngestError {
    /// Provider request failed.
    Provider(String),
    /// Provider returned more items than the configured bounded page.
    PageTooLarge {
        /// Number of items received.
        received: usize,
        /// Maximum accepted page size.
        maximum: usize,
    },
    /// Cursor persistence failed after the page was safely stored.
    CursorCommit(String),
    /// The provider returned a non-advancing cursor with data.
    CursorStalled,
}

/// Counts from one provider poll.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngestReport {
    /// Newly accepted items.
    pub accepted: usize,
    /// Items recognized as URL/hash/ID duplicates.
    pub duplicates: usize,
    /// Items rejected by normalization or validation.
    pub rejected: usize,
}

/// Durable diagnostic retained when a provider page cannot be retried safely.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetter {
    /// Provider that failed.
    pub provider_id: String,
    /// Cursor that remains safe to retry or investigate.
    pub cursor: Option<String>,
    /// Wall-clock time of the final failure.
    pub failed_at_ms: i64,
    /// Retry classification at exhaustion.
    pub class: RetryClass,
    /// Redacted bounded diagnostic.
    pub message: String,
}

/// Result of one bounded provider-worker poll.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PollOutcome {
    /// A page was stored and its cursor committed.
    Ingested(IngestReport),
    /// Polling is intentionally deferred until this timestamp.
    Deferred(i64),
    /// The page was routed to the bounded dead-letter queue.
    DeadLettered,
}

/// Provider registration failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryError {
    /// Provider identity is blank.
    EmptyProviderId,
    /// A provider with the same identity is already registered.
    DuplicateProvider,
    /// A provider configuration contains an invalid bound.
    InvalidConfiguration,
    /// The requested provider is not registered.
    UnknownProvider,
}

/// Operational health of one external provider worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderHealth {
    /// The provider has not completed a poll yet.
    Unknown,
    /// The most recent poll committed successfully.
    Healthy,
    /// The provider is temporarily deferred by a rate limit or retry policy.
    CoolingDown,
    /// A poll completed with a recoverable failure.
    Degraded,
    /// A failure exhausted retry capacity and entered dead-letter handling.
    Failed,
}

/// Read-only operational status for one registered provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderStatus {
    /// Provider identity.
    pub provider_id: String,
    /// Last committed cursor.
    pub cursor: Option<String>,
    /// Retry timestamp, if the provider is cooling down.
    pub next_retry_ms: Option<i64>,
    /// Number of retained dead-letter records.
    pub dead_letter_count: usize,
    /// Current worker health classification.
    pub health: ProviderHealth,
    /// Timestamp of the last successful poll.
    pub last_success_ms: Option<i64>,
    /// Timestamp of the last failed poll.
    pub last_failure_ms: Option<i64>,
    /// Consecutive recoverable failures since the last successful poll.
    pub consecutive_failures: u32,
}

/// Durable provider-worker state captured for restart recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderStateSnapshot {
    /// Provider identity.
    pub provider_id: String,
    /// Last committed provider cursor.
    pub cursor: Option<String>,
    /// Cursor optimistic-concurrency generation.
    pub generation: u64,
    /// Retry cooldown timestamp, if active.
    pub next_retry_ms: Option<i64>,
    /// Retry attempts in the current failure series.
    pub retries: u32,
    /// Bounded dead-letter diagnostics.
    pub dead_letters: Vec<DeadLetter>,
    /// Health classification at the last persisted poll boundary.
    pub health: ProviderHealth,
    /// Timestamp of the last successful poll.
    pub last_success_ms: Option<i64>,
    /// Timestamp of the last failed poll.
    pub last_failure_ms: Option<i64>,
    /// Consecutive failures at the persisted boundary.
    pub consecutive_failures: u32,
}

/// Versioned wire encoding for provider state snapshots.
pub const PROVIDER_STATE_MAGIC: &[u8] = b"IT_NEWS_PROVIDER_STATE_V2\0";
const PROVIDER_STATE_MAGIC_V1: &[u8] = b"IT_NEWS_PROVIDER_STATE_V1\0";

/// Serializes one provider snapshot with strict field bounds.
///
/// # Errors
/// Returns [`RegistryError::InvalidConfiguration`] when an identity, cursor,
/// message, or dead-letter list exceeds the bounded wire format.
pub fn encode_provider_state(snapshot: &ProviderStateSnapshot) -> Result<Vec<u8>, RegistryError> {
    if snapshot.provider_id.trim().is_empty()
        || snapshot.provider_id.len() > u16::MAX as usize
        || snapshot.dead_letters.len() > 1024
    {
        return Err(RegistryError::InvalidConfiguration);
    }
    let mut output = PROVIDER_STATE_MAGIC.to_vec();
    push_bounded_string(&mut output, &snapshot.provider_id)?;
    push_bounded_string(&mut output, snapshot.cursor.as_deref().unwrap_or_default())?;
    output.extend_from_slice(&snapshot.generation.to_le_bytes());
    output.extend_from_slice(&snapshot.next_retry_ms.unwrap_or_default().to_le_bytes());
    output.push(u8::from(snapshot.next_retry_ms.is_some()));
    output.extend_from_slice(&snapshot.retries.to_le_bytes());
    output.push(match snapshot.health {
        ProviderHealth::Unknown => 0,
        ProviderHealth::Healthy => 1,
        ProviderHealth::CoolingDown => 2,
        ProviderHealth::Degraded => 3,
        ProviderHealth::Failed => 4,
    });
    for timestamp in [snapshot.last_success_ms, snapshot.last_failure_ms] {
        output.extend_from_slice(&timestamp.unwrap_or_default().to_le_bytes());
        output.push(u8::from(timestamp.is_some()));
    }
    output.extend_from_slice(&snapshot.consecutive_failures.to_le_bytes());
    output.extend_from_slice(
        &(u16::try_from(snapshot.dead_letters.len()).unwrap_or(u16::MAX)).to_le_bytes(),
    );
    for dead_letter in &snapshot.dead_letters {
        push_bounded_string(&mut output, &dead_letter.provider_id)?;
        push_bounded_string(
            &mut output,
            dead_letter.cursor.as_deref().unwrap_or_default(),
        )?;
        output.extend_from_slice(&dead_letter.failed_at_ms.to_le_bytes());
        output.push(match dead_letter.class {
            RetryClass::RateLimited { .. } => 1,
            RetryClass::Transient => 2,
            RetryClass::Permanent => 3,
        });
        if let RetryClass::RateLimited { retry_after_ms } = dead_letter.class {
            output.extend_from_slice(&retry_after_ms.to_le_bytes());
        } else {
            output.extend_from_slice(&0_u64.to_le_bytes());
        }
        push_bounded_string(&mut output, &dead_letter.message)?;
    }
    Ok(output)
}

/// Decodes and validates one provider state snapshot.
///
/// # Errors
/// Returns [`RegistryError::InvalidConfiguration`] for malformed, truncated,
/// oversized, or unknown-version bytes.
pub fn decode_provider_state(payload: &[u8]) -> Result<ProviderStateSnapshot, RegistryError> {
    let is_v2 = payload.starts_with(PROVIDER_STATE_MAGIC);
    let magic = if is_v2 {
        PROVIDER_STATE_MAGIC
    } else if payload.starts_with(PROVIDER_STATE_MAGIC_V1) {
        PROVIDER_STATE_MAGIC_V1
    } else {
        return Err(RegistryError::InvalidConfiguration);
    };
    let mut cursor = magic.len();
    let provider_id = read_bounded_string(payload, &mut cursor)?;
    let cursor_value = read_bounded_string(payload, &mut cursor)?;
    let generation = read_u64_bounded(payload, &mut cursor)?;
    let retry_at = read_i64_bounded(payload, &mut cursor)?;
    let has_retry = read_u8_bounded(payload, &mut cursor)?;
    let retries = read_u32_bounded(payload, &mut cursor)?;
    let (health, last_success_ms, last_failure_ms, consecutive_failures) = if is_v2 {
        let health = match read_u8_bounded(payload, &mut cursor)? {
            0 => ProviderHealth::Unknown,
            1 => ProviderHealth::Healthy,
            2 => ProviderHealth::CoolingDown,
            3 => ProviderHealth::Degraded,
            4 => ProviderHealth::Failed,
            _ => return Err(RegistryError::InvalidConfiguration),
        };
        let mut read_timestamp = || -> Result<Option<i64>, RegistryError> {
            let value = read_i64_bounded(payload, &mut cursor)?;
            let present = read_u8_bounded(payload, &mut cursor)?;
            if present > 1 {
                return Err(RegistryError::InvalidConfiguration);
            }
            Ok((present == 1).then_some(value))
        };
        let last_success_ms = read_timestamp()?;
        let last_failure_ms = read_timestamp()?;
        let consecutive_failures = read_u32_bounded(payload, &mut cursor)?;
        (
            health,
            last_success_ms,
            last_failure_ms,
            consecutive_failures,
        )
    } else {
        let health = if has_retry == 1 {
            ProviderHealth::CoolingDown
        } else {
            ProviderHealth::Unknown
        };
        (health, None, None, 0)
    };
    let count = usize::from(read_u16_bounded(payload, &mut cursor)?);
    if provider_id.trim().is_empty() || has_retry > 1 || count > 1024 {
        return Err(RegistryError::InvalidConfiguration);
    }
    let mut dead_letters = Vec::with_capacity(count);
    for _ in 0..count {
        let dead_provider = read_bounded_string(payload, &mut cursor)?;
        let dead_cursor = read_bounded_string(payload, &mut cursor)?;
        let failed_at_ms = read_i64_bounded(payload, &mut cursor)?;
        let class_code = read_u8_bounded(payload, &mut cursor)?;
        let retry_after_ms = read_u64_bounded(payload, &mut cursor)?;
        let message = read_bounded_string(payload, &mut cursor)?;
        let class = match class_code {
            1 => RetryClass::RateLimited { retry_after_ms },
            2 => RetryClass::Transient,
            3 => RetryClass::Permanent,
            _ => return Err(RegistryError::InvalidConfiguration),
        };
        dead_letters.push(DeadLetter {
            provider_id: dead_provider,
            cursor: (!dead_cursor.is_empty()).then_some(dead_cursor),
            failed_at_ms,
            class,
            message,
        });
    }
    if cursor != payload.len() {
        return Err(RegistryError::InvalidConfiguration);
    }
    Ok(ProviderStateSnapshot {
        provider_id,
        cursor: (!cursor_value.is_empty()).then_some(cursor_value),
        generation,
        next_retry_ms: (has_retry == 1).then_some(retry_at),
        retries,
        dead_letters,
        health,
        last_success_ms,
        last_failure_ms,
        consecutive_failures,
    })
}

fn push_bounded_string(output: &mut Vec<u8>, value: &str) -> Result<(), RegistryError> {
    let bytes = value.as_bytes();
    let length = u16::try_from(bytes.len()).map_err(|_| RegistryError::InvalidConfiguration)?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn read_bounded_string(payload: &[u8], cursor: &mut usize) -> Result<String, RegistryError> {
    let length = usize::from(read_u16_bounded(payload, cursor)?);
    let end = cursor
        .checked_add(length)
        .ok_or(RegistryError::InvalidConfiguration)?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or(RegistryError::InvalidConfiguration)?;
    *cursor = end;
    String::from_utf8(bytes.to_vec()).map_err(|_| RegistryError::InvalidConfiguration)
}

fn read_u8_bounded(payload: &[u8], cursor: &mut usize) -> Result<u8, RegistryError> {
    let value = *payload
        .get(*cursor)
        .ok_or(RegistryError::InvalidConfiguration)?;
    *cursor += 1;
    Ok(value)
}

fn read_u16_bounded(payload: &[u8], cursor: &mut usize) -> Result<u16, RegistryError> {
    let end = cursor
        .checked_add(2)
        .ok_or(RegistryError::InvalidConfiguration)?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or(RegistryError::InvalidConfiguration)?;
    *cursor = end;
    Ok(u16::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| RegistryError::InvalidConfiguration)?,
    ))
}

fn read_u32_bounded(payload: &[u8], cursor: &mut usize) -> Result<u32, RegistryError> {
    let end = cursor
        .checked_add(4)
        .ok_or(RegistryError::InvalidConfiguration)?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or(RegistryError::InvalidConfiguration)?;
    *cursor = end;
    Ok(u32::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| RegistryError::InvalidConfiguration)?,
    ))
}

fn read_u64_bounded(payload: &[u8], cursor: &mut usize) -> Result<u64, RegistryError> {
    let end = cursor
        .checked_add(8)
        .ok_or(RegistryError::InvalidConfiguration)?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or(RegistryError::InvalidConfiguration)?;
    *cursor = end;
    Ok(u64::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| RegistryError::InvalidConfiguration)?,
    ))
}

fn read_i64_bounded(payload: &[u8], cursor: &mut usize) -> Result<i64, RegistryError> {
    let end = cursor
        .checked_add(8)
        .ok_or(RegistryError::InvalidConfiguration)?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or(RegistryError::InvalidConfiguration)?;
    *cursor = end;
    Ok(i64::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| RegistryError::InvalidConfiguration)?,
    ))
}

struct ProviderSlot {
    provider: Box<dyn CursorProvider>,
    state: CursorState,
    retry: RetryState,
    limiter: RequestLimiter,
    retry_policy: RetryPolicy,
    max_items: usize,
    dead_letter_capacity: usize,
    dead_letters: Vec<DeadLetter>,
    health: ProviderHealth,
    last_success_ms: Option<i64>,
    last_failure_ms: Option<i64>,
    consecutive_failures: u32,
}

/// Bounded provider registry and fallback polling boundary.
pub struct ProviderRegistry {
    providers: BTreeMap<String, ProviderSlot>,
}

impl ProviderRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            providers: BTreeMap::new(),
        }
    }

    /// Registers one independent cursor/retry/rate-limit state machine.
    ///
    /// # Errors
    /// Returns [`RegistryError`] for duplicate identities or invalid bounds.
    pub fn register(
        &mut self,
        provider: Box<dyn CursorProvider>,
        retry_policy: RetryPolicy,
        max_requests: u32,
        window_ms: i64,
        max_items: usize,
        dead_letter_capacity: usize,
    ) -> Result<(), RegistryError> {
        let provider_id = provider.provider_id().trim().to_owned();
        if let Some(manifest) = provider.manifest() {
            manifest
                .validate()
                .map_err(|_| RegistryError::InvalidConfiguration)?;
            if manifest.provider_id != provider_id
                || manifest.kind != insider_provider_core::ProviderKind::News
            {
                return Err(RegistryError::InvalidConfiguration);
            }
        }
        let Some(limiter) = RequestLimiter::new(max_requests, window_ms) else {
            return Err(RegistryError::InvalidConfiguration);
        };
        if provider_id.is_empty() {
            return Err(RegistryError::EmptyProviderId);
        }
        if max_items == 0 || self.providers.contains_key(&provider_id) {
            return Err(if max_items == 0 {
                RegistryError::InvalidConfiguration
            } else {
                RegistryError::DuplicateProvider
            });
        }
        self.providers.insert(
            provider_id,
            ProviderSlot {
                provider,
                state: CursorState::default(),
                retry: RetryState::default(),
                limiter,
                retry_policy,
                max_items,
                dead_letter_capacity,
                dead_letters: Vec::new(),
                health: ProviderHealth::Unknown,
                last_success_ms: None,
                last_failure_ms: None,
                consecutive_failures: 0,
            },
        );
        Ok(())
    }

    /// Returns registered provider identities in deterministic order.
    #[must_use]
    pub fn provider_ids(&self) -> Vec<String> {
        self.providers.keys().cloned().collect()
    }

    /// Captures all provider state in deterministic provider-ID order.
    #[must_use]
    pub fn snapshots(&self) -> Vec<ProviderStateSnapshot> {
        self.providers
            .iter()
            .map(|(provider_id, slot)| ProviderStateSnapshot {
                provider_id: provider_id.clone(),
                cursor: slot.state.cursor.clone(),
                generation: slot.state.generation,
                next_retry_ms: slot.retry.next_retry_ms,
                retries: slot.retry.retries,
                dead_letters: slot.dead_letters.clone(),
                health: slot.health,
                last_success_ms: slot.last_success_ms,
                last_failure_ms: slot.last_failure_ms,
                consecutive_failures: slot.consecutive_failures,
            })
            .collect()
    }

    /// Restores one previously captured provider state after registration.
    ///
    /// # Errors
    /// Returns [`RegistryError::UnknownProvider`] for an unregistered provider
    /// or [`RegistryError::InvalidConfiguration`] when the snapshot exceeds
    /// the provider's configured dead-letter bound.
    pub fn restore_snapshot(
        &mut self,
        snapshot: ProviderStateSnapshot,
    ) -> Result<(), RegistryError> {
        let Some(slot) = self.providers.get_mut(&snapshot.provider_id) else {
            return Err(RegistryError::UnknownProvider);
        };
        if snapshot.dead_letters.len() > slot.dead_letter_capacity {
            return Err(RegistryError::InvalidConfiguration);
        }
        slot.state = CursorState {
            cursor: snapshot.cursor,
            generation: snapshot.generation,
        };
        slot.retry = RetryState {
            retries: snapshot.retries,
            next_retry_ms: snapshot.next_retry_ms,
        };
        slot.dead_letters = snapshot.dead_letters;
        slot.health = snapshot.health;
        slot.last_success_ms = snapshot.last_success_ms;
        slot.last_failure_ms = snapshot.last_failure_ms;
        slot.consecutive_failures = snapshot.consecutive_failures;
        Ok(())
    }

    /// Returns operational status for a provider.
    #[must_use]
    pub fn status(&self, provider_id: &str) -> Option<ProviderStatus> {
        self.providers.get(provider_id).map(|slot| ProviderStatus {
            provider_id: provider_id.to_owned(),
            cursor: slot.state.cursor.clone(),
            next_retry_ms: slot.retry.next_retry_ms,
            dead_letter_count: slot.dead_letters.len(),
            health: slot.health,
            last_success_ms: slot.last_success_ms,
            last_failure_ms: slot.last_failure_ms,
            consecutive_failures: slot.consecutive_failures,
        })
    }

    /// Polls one provider using its own cursor and retry state.
    ///
    /// # Errors
    /// Returns [`RegistryError::UnknownProvider`] when the identity is absent.
    pub fn poll<C, F>(
        &mut self,
        provider_id: &str,
        store: &mut NewsStore,
        committer: &mut C,
        now_ms: i64,
        classify: F,
    ) -> Result<PollOutcome, RegistryError>
    where
        C: CursorCommitter,
        F: Fn(&str) -> RetryClass,
    {
        let Some(slot) = self.providers.get_mut(provider_id) else {
            return Err(RegistryError::UnknownProvider);
        };
        let outcome = poll_cursor_provider(
            slot.provider.as_ref(),
            store,
            &mut slot.state,
            committer,
            &mut slot.retry,
            &mut slot.limiter,
            now_ms,
            slot.retry_policy,
            slot.max_items,
            &mut slot.dead_letters,
            slot.dead_letter_capacity,
            classify,
        );
        match outcome {
            PollOutcome::Ingested(_) => {
                slot.health = ProviderHealth::Healthy;
                slot.last_success_ms = Some(now_ms);
                slot.consecutive_failures = 0;
            }
            PollOutcome::Deferred(_) => {
                slot.health = if slot.retry.next_retry_ms.is_some() {
                    ProviderHealth::CoolingDown
                } else {
                    ProviderHealth::Degraded
                };
                if slot.retry.next_retry_ms.is_some() {
                    slot.last_failure_ms = Some(now_ms);
                    slot.consecutive_failures = slot.consecutive_failures.saturating_add(1);
                }
            }
            PollOutcome::DeadLettered => {
                slot.health = ProviderHealth::Failed;
                slot.last_failure_ms = Some(now_ms);
                slot.consecutive_failures = slot.consecutive_failures.saturating_add(1);
            }
        }
        Ok(outcome)
    }

    /// Polls providers in deterministic priority order until one ingests new
    /// items. Deferred/dead-letter outcomes do not prevent fallback providers.
    ///
    /// # Errors
    /// Returns [`RegistryError::UnknownProvider`] if a requested identity is
    /// not registered.
    pub fn poll_fallback<C, F>(
        &mut self,
        priority: &[&str],
        store: &mut NewsStore,
        committer: &mut C,
        now_ms: i64,
        classify: F,
    ) -> Result<Vec<(String, PollOutcome)>, RegistryError>
    where
        C: CursorCommitter,
        F: Fn(&str) -> RetryClass + Copy,
    {
        let mut outcomes = Vec::with_capacity(priority.len());
        for provider_id in priority {
            let outcome = self.poll(provider_id, store, committer, now_ms, classify)?;
            let ingested_new_items =
                matches!(outcome, PollOutcome::Ingested(report) if report.accepted > 0);
            outcomes.push(((*provider_id).to_owned(), outcome));
            if ingested_new_items {
                break;
            }
        }
        Ok(outcomes)
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Executes one cursor-provider poll with rate limiting, retry policy, and a
/// bounded dead-letter queue. The provider cursor is advanced only by
/// [`ingest_cursor_with_commit`], after item storage succeeds.
///
/// # Errors
/// Returns no error for provider failures: they become `Deferred` or
/// `DeadLettered` outcomes so a worker loop cannot crash the engine. A caller
/// still receives storage/validation failures through the same classification
/// path and can inspect the dead-letter record.
#[allow(clippy::too_many_arguments)]
pub fn poll_cursor_provider<P, C, F>(
    provider: &P,
    store: &mut NewsStore,
    state: &mut CursorState,
    committer: &mut C,
    retry: &mut RetryState,
    limiter: &mut RequestLimiter,
    now_ms: i64,
    retry_policy: RetryPolicy,
    max_items: usize,
    dead_letters: &mut Vec<DeadLetter>,
    dead_letter_capacity: usize,
    classify: F,
) -> PollOutcome
where
    P: CursorProvider + ?Sized,
    C: CursorCommitter,
    F: Fn(&str) -> RetryClass,
{
    if !retry.ready(now_ms) {
        return PollOutcome::Deferred(retry.next_retry_ms.unwrap_or(now_ms));
    }
    match limiter.allow(now_ms) {
        Ok(()) => {}
        Err(at) => return PollOutcome::Deferred(at),
    }
    let result = ingest_cursor_with_commit(provider, store, state, committer, now_ms, max_items);
    match result {
        Ok(report) => {
            retry.reset();
            PollOutcome::Ingested(report)
        }
        Err(error) => {
            let message = match &error {
                IngestError::Provider(message) | IngestError::CursorCommit(message) => {
                    message.clone()
                }
                IngestError::PageTooLarge { received, maximum } => {
                    format!("provider page {received} exceeds maximum {maximum}")
                }
                IngestError::CursorStalled => "provider cursor stalled".to_owned(),
            };
            let class = classify(&message);
            match retry.on_failure(now_ms, class, retry_policy) {
                RetryDecision::RetryAt(at) => PollOutcome::Deferred(at),
                RetryDecision::DeadLetter => {
                    if dead_letter_capacity > 0 {
                        if dead_letters.len() >= dead_letter_capacity {
                            dead_letters.remove(0);
                        }
                        dead_letters.push(DeadLetter {
                            provider_id: provider.provider_id().to_owned(),
                            cursor: state.cursor.clone(),
                            failed_at_ms: now_ms,
                            class,
                            message: message.chars().take(512).collect(),
                        });
                    }
                    retry.reset();
                    PollOutcome::DeadLettered
                }
            }
        }
    }
}

/// Versioned deterministic weights for chart/news ranking.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RankWeights {
    /// Weight for a direct symbol match.
    pub direct_symbol: f64,
    /// Weight applied to recency decay.
    pub recency: f64,
    /// Weight for an item with no published timestamp.
    pub undated_penalty: f64,
}

impl Default for RankWeights {
    fn default() -> Self {
        Self {
            direct_symbol: 1_000.0,
            recency: 1.0,
            undated_penalty: -100.0,
        }
    }
}

/// Explainable score returned by the news ranker.
#[derive(Clone, Debug, PartialEq)]
pub struct NewsScore {
    /// Stable item ID.
    pub item_id: String,
    /// Final weighted score.
    pub score: f64,
    /// Whether the item directly names the requested symbol.
    pub direct_symbol: bool,
    /// Recency component before weighting.
    pub recency_component: f64,
}

/// One bounded page of news and a stable continuation cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewsPage {
    /// Items in deterministic feed order.
    pub items: Vec<NewsItem>,
    /// ID to pass as the next cursor, or `None` at end of feed.
    pub next_cursor: Option<String>,
    /// Deterministic relevance scores in basis points keyed by item ID.
    /// Scores are omitted for unranked feeds and therefore default to zero.
    pub relevance_scores_bps: BTreeMap<String, u16>,
}

/// Authoritative article detail assembled from the current item, retained
/// corrections, and deterministic exact-title cluster membership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewsDetail {
    /// Current normalized article version.
    pub current: NewsItem,
    /// Older immutable versions retained for audit/replay.
    pub versions: Vec<NewsItem>,
    /// Normalized-title cluster identity.
    pub cluster_id: String,
    /// Other current item IDs in the same exact-title cluster.
    pub related_item_ids: Vec<String>,
}

/// Deterministic exact-title event cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewsCluster {
    /// Stable cluster key derived from the normalized title.
    pub cluster_id: String,
    /// Item IDs in receipt order.
    pub item_ids: Vec<String>,
}

/// Canonical normalized news item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewsItem {
    /// Stable provider identifier.
    pub id: String,
    /// Provider name.
    pub provider: String,
    /// Canonical article URL.
    pub canonical_url: String,
    /// Publisher name.
    pub source_name: String,
    /// Normalized headline.
    pub title: String,
    /// Optional normalized summary.
    pub summary_text: Option<String>,
    /// Published Unix milliseconds, if supplied by provider.
    pub published_at_ms: Option<i64>,
    /// Receipt Unix milliseconds.
    pub received_at_ms: i64,
    /// Canonical instrument symbols.
    pub symbols: BTreeSet<String>,
    /// Lowercase content hash supplied or calculated by adapter.
    pub content_hash: String,
}

const MAX_NEWS_ID_BYTES: usize = 256;
const MAX_PROVIDER_BYTES: usize = 64;
const MAX_URL_BYTES: usize = 2_048;
const MAX_SOURCE_BYTES: usize = 512;
const MAX_TITLE_BYTES: usize = 16_384;
const MAX_SUMMARY_BYTES: usize = 131_072;
const MAX_CONTENT_HASH_BYTES: usize = 256;
const MAX_SYMBOLS: usize = 256;
const MAX_SYMBOL_BYTES: usize = 32;

/// Normalizes provider text and symbols before deduplication.
///
/// # Errors
/// Returns `NewsError` when the normalized item is invalid.
pub fn normalize(mut item: NewsItem, provider_id: &str) -> Result<NewsItem, NewsError> {
    if provider_id.trim().is_empty() {
        return Err(NewsError::EmptyField("provider"));
    }
    provider_id.trim().clone_into(&mut item.provider);
    item.canonical_url = item.canonical_url.trim().trim_end_matches('/').to_owned();
    item.title = item.title.split_whitespace().collect::<Vec<_>>().join(" ");
    item.summary_text = item
        .summary_text
        .map(|summary| summary.split_whitespace().collect::<Vec<_>>().join(" "));
    item.symbols = item
        .symbols
        .into_iter()
        .map(|symbol| symbol.trim().to_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect();
    item.validate()?;
    Ok(item)
}

/// Polls a provider and applies normalized items to a deduplicated store.
///
/// # Errors
/// Returns `IngestError::Provider` when the provider fetch fails. Individual
/// malformed items are counted as rejected and do not abort the batch.
pub fn ingest<P: NewsProvider>(
    provider: &P,
    store: &mut NewsStore,
    now_ms: i64,
) -> Result<IngestReport, IngestError> {
    let items = provider.fetch(now_ms).map_err(IngestError::Provider)?;
    let mut report = IngestReport {
        accepted: 0,
        duplicates: 0,
        rejected: 0,
    };
    for item in items {
        let Ok(normalized) = normalize(item, provider.provider_id()) else {
            report.rejected += 1;
            continue;
        };
        match store.insert(normalized) {
            Ok(true) => report.accepted += 1,
            Ok(false) => report.duplicates += 1,
            Err(_) => report.rejected += 1,
        }
    }
    Ok(report)
}

/// Ingests one cursor page and commits its cursor only after all items are
/// normalized and stored. A provider failure or storage error leaves the old
/// cursor intact so restart can safely retry the page.
///
/// # Errors
/// Returns [`IngestError::Provider`] on fetch failure. The cursor is never
/// advanced on that path.
pub fn ingest_cursor<P: CursorProvider>(
    provider: &P,
    store: &mut NewsStore,
    state: &mut CursorState,
    now_ms: i64,
    max_items: usize,
) -> Result<IngestReport, IngestError> {
    let mut noop = NoopCommitter;
    ingest_cursor_with_commit(provider, store, state, &mut noop, now_ms, max_items)
}

/// Ingests one cursor page and durably commits its cursor after storage.
///
/// Item writes happen before the committer is called. If the process fails
/// between those operations, retrying the old cursor is safe because the store
/// deduplicates immutable article identities/content hashes. If the commit
/// fails, the caller's cursor state and generation remain unchanged.
///
/// # Errors
/// Returns [`IngestError::CursorCommit`] when cursor persistence fails and
/// [`IngestError::CursorStalled`] when a non-empty page does not advance.
pub fn ingest_cursor_with_commit<P: CursorProvider + ?Sized, C: CursorCommitter>(
    provider: &P,
    store: &mut NewsStore,
    state: &mut CursorState,
    committer: &mut C,
    now_ms: i64,
    max_items: usize,
) -> Result<IngestReport, IngestError> {
    if max_items == 0 {
        return Ok(IngestReport {
            accepted: 0,
            duplicates: 0,
            rejected: 0,
        });
    }
    let batch = provider
        .fetch_page(state.cursor.as_deref(), now_ms)
        .map_err(IngestError::Provider)?;
    if batch.items.len() > max_items {
        return Err(IngestError::PageTooLarge {
            received: batch.items.len(),
            maximum: max_items,
        });
    }
    if !batch.items.is_empty() && batch.next_cursor.as_deref() == state.cursor.as_deref() {
        return Err(IngestError::CursorStalled);
    }
    let mut report = IngestReport {
        accepted: 0,
        duplicates: 0,
        rejected: 0,
    };
    let mut normalized_items = Vec::with_capacity(batch.items.len());
    for item in batch.items {
        let Ok(normalized) = normalize(item, provider.provider_id()) else {
            report.rejected += 1;
            continue;
        };
        normalized_items.push(normalized.clone());
        match store.insert(normalized) {
            Ok(true) => report.accepted += 1,
            Ok(false) => report.duplicates += 1,
            Err(_) => report.rejected += 1,
        }
    }
    let next_generation = state.generation.saturating_add(1);
    committer
        .commit_page(
            provider.provider_id(),
            state.generation,
            batch.next_cursor.as_deref(),
            &normalized_items,
        )
        .map_err(IngestError::CursorCommit)?;
    state.cursor = batch.next_cursor;
    state.generation = next_generation;
    Ok(report)
}

struct NoopCommitter;

impl CursorCommitter for NoopCommitter {
    fn commit_cursor(
        &mut self,
        _provider_id: &str,
        _expected_generation: u64,
        _next_cursor: Option<&str>,
    ) -> Result<(), String> {
        Ok(())
    }
}

impl NewsItem {
    /// Validates required identity and timestamp fields.
    ///
    /// # Errors
    /// Returns [`NewsError`] when a required field is empty or a timestamp is invalid.
    pub fn validate(&self) -> Result<(), NewsError> {
        for (name, value) in [
            ("id", self.id.as_str()),
            ("provider", self.provider.as_str()),
            ("canonical_url", self.canonical_url.as_str()),
            ("source_name", self.source_name.as_str()),
            ("title", self.title.as_str()),
            ("content_hash", self.content_hash.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(NewsError::EmptyField(name));
            }
        }
        for (name, value, maximum) in [
            ("id", self.id.as_str(), MAX_NEWS_ID_BYTES),
            ("provider", self.provider.as_str(), MAX_PROVIDER_BYTES),
            ("canonical_url", self.canonical_url.as_str(), MAX_URL_BYTES),
            ("source_name", self.source_name.as_str(), MAX_SOURCE_BYTES),
            ("title", self.title.as_str(), MAX_TITLE_BYTES),
            (
                "content_hash",
                self.content_hash.as_str(),
                MAX_CONTENT_HASH_BYTES,
            ),
        ] {
            if value.len() > maximum {
                return Err(NewsError::FieldTooLarge(name));
            }
        }
        let Some(authority) = self.canonical_url.strip_prefix("https://") else {
            return Err(NewsError::InvalidUrl);
        };
        let authority_host = authority.split(['/', '?', '#']).next().unwrap_or_default();
        if authority.is_empty()
            || authority.starts_with('/')
            || authority_host.is_empty()
            || authority_host.contains('@')
            || authority.chars().any(char::is_whitespace)
        {
            return Err(NewsError::InvalidUrl);
        }
        if self
            .summary_text
            .as_deref()
            .is_some_and(|summary| summary.len() > MAX_SUMMARY_BYTES)
        {
            return Err(NewsError::FieldTooLarge("summary_text"));
        }
        if self.symbols.len() > MAX_SYMBOLS
            || self
                .symbols
                .iter()
                .any(|symbol| symbol.is_empty() || symbol.len() > MAX_SYMBOL_BYTES)
        {
            return Err(NewsError::FieldTooLarge("symbols"));
        }
        if self.received_at_ms < 0 || self.published_at_ms.is_some_and(|time| time < 0) {
            return Err(NewsError::InvalidTimestamp);
        }
        Ok(())
    }
}

/// News validation or storage failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NewsError {
    /// A required field was blank.
    EmptyField(&'static str),
    /// A normalized field exceeded its storage or transport bound.
    FieldTooLarge(&'static str),
    /// The canonical article URL is not an HTTPS URL with an authority.
    InvalidUrl,
    /// A timestamp was negative.
    InvalidTimestamp,
}

/// Result of inserting an immutable article version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VersionInsert {
    /// First version for the article identity.
    New,
    /// Exact content version was already stored.
    Duplicate,
    /// A new corrected version replaced the current projection.
    Correction,
}

/// Deduplicated in-memory news index.
#[allow(clippy::struct_field_names)]
pub struct NewsStore {
    capacity: usize,
    by_id: BTreeMap<String, NewsItem>,
    by_url: BTreeMap<String, String>,
    by_hash: BTreeMap<String, String>,
    versions: BTreeMap<String, Vec<NewsItem>>,
}

const MAX_NEWS_ITEMS: usize = 100_000;
const MAX_VERSIONS_PER_ITEM: usize = 32;

impl Default for NewsStore {
    fn default() -> Self {
        Self::with_capacity(100_000)
    }
}

impl NewsStore {
    /// Creates a news store with a hard retained-item bound.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity: capacity.clamp(1, MAX_NEWS_ITEMS),
            by_id: BTreeMap::new(),
            by_url: BTreeMap::new(),
            by_hash: BTreeMap::new(),
            versions: BTreeMap::new(),
        }
    }

    /// Returns whether the current projection already contains an item ID.
    #[must_use]
    pub fn contains_id(&self, id: &str) -> bool {
        self.by_id.contains_key(id)
    }

    /// Returns whether an exact article content version is already retained.
    #[must_use]
    pub fn contains_version(&self, id: &str, content_hash: &str) -> bool {
        self.versions.get(id).is_some_and(|versions| {
            versions
                .iter()
                .any(|item| item.content_hash == content_hash)
        })
    }

    /// Inserts an item, returning `true` only when it is new.
    ///
    /// # Errors
    /// Returns [`NewsError`] when the item fails canonical validation.
    pub fn insert(&mut self, item: NewsItem) -> Result<bool, NewsError> {
        item.validate()?;
        if self.by_id.contains_key(&item.id)
            || self.by_url.contains_key(&item.canonical_url)
            || self.by_hash.contains_key(&item.content_hash)
        {
            return Ok(false);
        }
        self.by_url
            .insert(item.canonical_url.clone(), item.id.clone());
        self.by_hash
            .insert(item.content_hash.clone(), item.id.clone());
        self.versions
            .entry(item.id.clone())
            .or_default()
            .push(item.clone());
        self.by_id.insert(item.id.clone(), item);
        self.evict_if_needed();
        Ok(true)
    }

    /// Inserts a provider correction without deleting prior article versions.
    ///
    /// Exact URL/hash duplicates remain idempotent. A repeated article ID with
    /// changed content is retained in `versions` and becomes the current item.
    ///
    /// # Errors
    /// Returns [`NewsError`] when the item fails canonical validation.
    pub fn insert_versioned(&mut self, item: NewsItem) -> Result<VersionInsert, NewsError> {
        item.validate()?;
        if self
            .by_id
            .get(&item.id)
            .is_some_and(|current| current.content_hash == item.content_hash)
            || self.by_hash.contains_key(&item.content_hash)
        {
            return Ok(VersionInsert::Duplicate);
        }
        if self
            .by_url
            .get(&item.canonical_url)
            .is_some_and(|existing_id| existing_id != &item.id)
        {
            return Ok(VersionInsert::Duplicate);
        }
        if self.by_id.contains_key(&item.id) {
            if let Some(current) = self.by_id.get(&item.id)
                && current.canonical_url != item.canonical_url
                && self
                    .by_url
                    .get(&current.canonical_url)
                    .is_some_and(|id| id == &item.id)
            {
                self.by_url.remove(&current.canonical_url);
            }
            self.by_url
                .insert(item.canonical_url.clone(), item.id.clone());
            self.by_hash
                .insert(item.content_hash.clone(), item.id.clone());
            let versions = self.versions.entry(item.id.clone()).or_default();
            versions.push(item.clone());
            while versions.len() > MAX_VERSIONS_PER_ITEM {
                let removed = versions.remove(0);
                if self
                    .by_hash
                    .get(&removed.content_hash)
                    .is_some_and(|id| id == &item.id)
                {
                    self.by_hash.remove(&removed.content_hash);
                }
            }
            self.by_id.insert(item.id.clone(), item);
            self.evict_if_needed();
            return Ok(VersionInsert::Correction);
        }
        self.insert(item)?;
        Ok(VersionInsert::New)
    }

    fn evict_if_needed(&mut self) {
        while self.by_id.len() > self.capacity {
            let Some(oldest_id) = self
                .by_id
                .values()
                .min_by(|left, right| {
                    left.received_at_ms
                        .cmp(&right.received_at_ms)
                        .then_with(|| left.id.cmp(&right.id))
                })
                .map(|item| item.id.clone())
            else {
                return;
            };
            if let Some(item) = self.by_id.remove(&oldest_id) {
                self.by_url.remove(&item.canonical_url);
                if let Some(versions) = self.versions.remove(&oldest_id) {
                    for version in versions {
                        if self
                            .by_hash
                            .get(&version.content_hash)
                            .is_some_and(|id| id == &oldest_id)
                        {
                            self.by_hash.remove(&version.content_hash);
                        }
                    }
                } else {
                    self.by_hash.remove(&item.content_hash);
                }
            }
        }
    }

    /// Returns every retained immutable version for an article identity.
    #[must_use]
    pub fn versions(&self, id: &str) -> &[NewsItem] {
        self.versions.get(id).map_or(&[], Vec::as_slice)
    }

    /// Returns items sorted by deterministic relevance for a symbol and time.
    #[must_use]
    pub fn relevant(&self, symbol: &str, now_ms: i64, limit: usize) -> Vec<&NewsItem> {
        self.ranked(symbol, now_ms, limit, RankWeights::default())
            .into_iter()
            .filter(|(_, score)| score.direct_symbol)
            .map(|(item, _)| item)
            .collect()
    }

    /// Ranks all stored items and returns component scores for explainability.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn ranked(
        &self,
        symbol: &str,
        now_ms: i64,
        limit: usize,
        weights: RankWeights,
    ) -> Vec<(&NewsItem, NewsScore)> {
        let normalized_symbol = symbol.trim().to_uppercase();
        let mut scored: Vec<(&NewsItem, NewsScore)> = self
            .by_id
            .values()
            .filter(|item| item.received_at_ms <= now_ms)
            .map(|item| {
                let direct = item.symbols.contains(&normalized_symbol);
                let recency_component = item.published_at_ms.map_or(0.0, |published| {
                    (86_400_000_i64.saturating_sub(now_ms.saturating_sub(published).max(0))).max(0)
                        as f64
                        / 86_400_000.0
                });
                let score = if weights.direct_symbol.is_finite()
                    && weights.recency.is_finite()
                    && weights.undated_penalty.is_finite()
                {
                    if item.published_at_ms.is_some() {
                        (if direct { 1.0 } else { 0.0 }) * weights.direct_symbol
                            + recency_component * weights.recency
                    } else {
                        (if direct { 1.0 } else { 0.0 }) * weights.direct_symbol
                            + weights.undated_penalty
                    }
                } else {
                    0.0
                };
                (
                    item,
                    NewsScore {
                        item_id: item.id.clone(),
                        score,
                        direct_symbol: direct,
                        recency_component,
                    },
                )
            })
            .collect();
        scored.sort_by(|(left_item, left_score), (right_item, right_score)| {
            right_score
                .score
                .total_cmp(&left_score.score)
                .then_with(|| left_item.id.cmp(&right_item.id))
        });
        scored.into_iter().take(limit).collect()
    }

    /// Returns the complete feed in deterministic publish/receive order.
    #[must_use]
    pub fn all(&self, limit: usize) -> Vec<&NewsItem> {
        let mut items: Vec<&NewsItem> = self.by_id.values().collect();
        items.sort_by(|left, right| {
            right
                .published_at_ms
                .cmp(&left.published_at_ms)
                .then_with(|| right.received_at_ms.cmp(&left.received_at_ms))
                .then_with(|| left.id.cmp(&right.id))
        });
        items.into_iter().take(limit).collect()
    }

    /// Returns a bounded page of the complete feed using an item-ID cursor.
    /// Cursor pagination is stable for an immutable store snapshot and avoids
    /// offset drift as the UI requests successive pages.
    #[must_use]
    pub fn all_page(&self, after_id: Option<&str>, limit: usize) -> NewsPage {
        self.all_page_at(after_id, limit, i64::MAX)
    }

    /// Returns a bounded complete-news page visible at the supplied
    /// point-in-time receipt cutoff. This is the historical-safe counterpart
    /// to [`Self::all_page`].
    #[must_use]
    pub fn all_page_at(&self, after_id: Option<&str>, limit: usize, as_of_ms: i64) -> NewsPage {
        let limit = limit.min(500);
        if limit == 0 {
            return NewsPage {
                items: Vec::new(),
                next_cursor: None,
                relevance_scores_bps: BTreeMap::new(),
            };
        }
        let mut ordered: Vec<&NewsItem> = self
            .by_id
            .values()
            .filter(|item| item.received_at_ms <= as_of_ms)
            .collect();
        ordered.sort_by(|left, right| {
            right
                .published_at_ms
                .cmp(&left.published_at_ms)
                .then_with(|| right.received_at_ms.cmp(&left.received_at_ms))
                .then_with(|| left.id.cmp(&right.id))
        });
        let start = after_id
            .and_then(|cursor| ordered.iter().position(|item| item.id == cursor))
            .map_or(0, |position| position.saturating_add(1));
        let ordered_count = ordered.len();
        let page = ordered
            .into_iter()
            .skip(start)
            .take(limit)
            .collect::<Vec<_>>();
        let has_more = start.saturating_add(page.len()) < ordered_count;
        let next_cursor = has_more
            .then(|| page.last().map(|item| item.id.clone()))
            .flatten();
        NewsPage {
            items: page.into_iter().cloned().collect(),
            next_cursor,
            relevance_scores_bps: BTreeMap::new(),
        }
    }

    /// Returns a cursor page of directly symbol-relevant news in ranked order.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn relevant_page(
        &self,
        symbol: &str,
        now_ms: i64,
        after_id: Option<&str>,
        limit: usize,
    ) -> NewsPage {
        let normalized = symbol.trim().to_uppercase();
        let mut ranked = self
            .by_id
            .values()
            .filter(|item| item.symbols.contains(&normalized) && item.received_at_ms <= now_ms)
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            let score = |item: &NewsItem| {
                item.published_at_ms.map_or(-100.0, |published| {
                    (86_400_000_i64
                        .saturating_sub(now_ms.saturating_sub(published).max(0))
                        .max(0) as f64)
                        / 86_400_000.0
                })
            };
            score(right)
                .total_cmp(&score(left))
                .then_with(|| left.id.cmp(&right.id))
        });
        let start = after_id
            .and_then(|cursor| ranked.iter().position(|item| item.id == cursor))
            .map_or(0, |position| position.saturating_add(1));
        let ranked_count = ranked.len();
        let limit = limit.min(500);
        let page = ranked
            .into_iter()
            .skip(start)
            .take(limit)
            .collect::<Vec<_>>();
        let has_more = start.saturating_add(page.len()) < ranked_count;
        let next_cursor = has_more
            .then(|| page.last().map(|item| item.id.clone()))
            .flatten();
        let relevance_scores_bps = page
            .iter()
            .map(|item| {
                let remaining_ms = item.published_at_ms.map_or(0, |published| {
                    86_400_000_i64
                        .saturating_sub(now_ms.saturating_sub(published).max(0))
                        .max(0)
                });
                let score_bps = 10_000_i64.saturating_mul(remaining_ms) / 86_400_000_i64;
                (
                    item.id.clone(),
                    u16::try_from(score_bps.clamp(0, 10_000)).unwrap_or(0),
                )
            })
            .collect();
        NewsPage {
            items: page.into_iter().cloned().collect(),
            next_cursor,
            relevance_scores_bps,
        }
    }

    /// Clusters exact normalized titles without deleting article versions.
    #[must_use]
    pub fn clusters(&self) -> Vec<NewsCluster> {
        let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for item in self.by_id.values() {
            let key = item
                .title
                .split_whitespace()
                .map(str::to_lowercase)
                .collect::<Vec<_>>()
                .join(" ");
            grouped.entry(key).or_default().push(item.id.clone());
        }
        grouped
            .into_iter()
            .map(|(cluster_id, item_ids)| NewsCluster {
                cluster_id,
                item_ids,
            })
            .collect()
    }

    /// Returns an item by canonical provider-independent ID.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<&NewsItem> {
        self.by_id.get(id)
    }

    /// Returns an article detail view without exposing mutable store state.
    #[must_use]
    pub fn detail(&self, id: &str) -> Option<NewsDetail> {
        self.detail_at(id, i64::MAX)
    }

    /// Returns the article detail view as it was known at the supplied
    /// receipt-time cutoff. Corrections received later are excluded.
    #[must_use]
    pub fn detail_at(&self, id: &str, as_of_ms: i64) -> Option<NewsDetail> {
        let current = self
            .versions(id)
            .iter()
            .filter(|item| item.received_at_ms <= as_of_ms)
            .max_by_key(|item| item.received_at_ms)
            .cloned()?;
        let cluster_id = current
            .title
            .split_whitespace()
            .map(str::to_lowercase)
            .collect::<Vec<_>>()
            .join(" ");
        let related_item_ids = self
            .by_id
            .values()
            .filter(|item| {
                item.id != current.id
                    && item.received_at_ms <= as_of_ms
                    && item
                        .title
                        .split_whitespace()
                        .map(str::to_lowercase)
                        .collect::<Vec<_>>()
                        .join(" ")
                        == cluster_id
            })
            .map(|item| item.id.clone())
            .collect();
        Some(NewsDetail {
            versions: self
                .versions(&current.id)
                .iter()
                .filter(|item| item.received_at_ms <= as_of_ms)
                .cloned()
                .collect(),
            current,
            cluster_id,
            related_item_ids,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CursorCommitter, CursorProvider, CursorState, NewsItem, NewsProvider, NewsStore,
        ProviderBatch, SUBSYSTEM_ID, VersionInsert, ingest, ingest_cursor_with_commit,
    };
    use std::collections::BTreeSet;

    #[test]
    fn subsystem_id_is_non_empty_and_ascii() {
        assert!(!SUBSYSTEM_ID.is_empty());
        assert!(SUBSYSTEM_ID.is_ascii());
    }

    fn item(id: &str, url: &str, hash: &str, symbol: &str, published: i64) -> NewsItem {
        NewsItem {
            id: id.into(),
            provider: "test".into(),
            canonical_url: url.into(),
            source_name: "wire".into(),
            title: id.into(),
            summary_text: None,
            published_at_ms: Some(published),
            received_at_ms: 2_000,
            symbols: BTreeSet::from([symbol.into()]),
            content_hash: hash.into(),
        }
    }

    #[test]
    fn deduplicates_url_and_hash_and_ranks_direct_symbol_news() {
        let mut store = NewsStore::default();
        assert!(
            store
                .insert(item("a", "https://a", "ha", "ABC", 1_900))
                .is_ok_and(|new| new)
        );
        assert!(
            store
                .insert(item("b", "https://a", "hb", "ABC", 1_950))
                .is_ok_and(|new| !new)
        );
        assert!(
            store
                .insert(item("c", "https://c", "ha", "ABC", 1_950))
                .is_ok_and(|new| !new)
        );
        assert_eq!(store.relevant("ABC", 2_000, 10).len(), 1);
    }

    #[test]
    fn point_in_time_retrieval_hides_news_not_yet_received() {
        let mut store = NewsStore::default();
        let mut available = item(
            "available",
            "https://available",
            "available-hash",
            "ABC",
            1_900,
        );
        available.received_at_ms = 1_950;
        let mut future = item("future", "https://future", "future-hash", "ABC", 1_800);
        future.received_at_ms = 2_100;
        assert!(store.insert(available).is_ok_and(|inserted| inserted));
        assert!(store.insert(future).is_ok_and(|inserted| inserted));
        assert_eq!(
            store
                .relevant("ABC", 2_000, 10)
                .iter()
                .map(|news| news.id.as_str())
                .collect::<Vec<_>>(),
            vec!["available"]
        );
        assert_eq!(
            store
                .relevant_page("ABC", 2_000, None, 10)
                .items
                .iter()
                .map(|news| news.id.as_str())
                .collect::<Vec<_>>(),
            vec!["available"]
        );
        assert_eq!(
            store
                .all_page_at(None, 10, 2_000)
                .items
                .iter()
                .map(|news| news.id.as_str())
                .collect::<Vec<_>>(),
            vec!["available"]
        );
        assert!(
            store
                .all_page_at(Some("available"), 10, 2_000)
                .items
                .is_empty()
        );
    }

    #[test]
    fn detail_preserves_corrections_and_exact_title_cluster_membership() {
        let mut store = NewsStore::default();
        let mut current = item("a", "https://a", "ha", "ABC", 1_900);
        current.title = "Issuer raises outlook".into();
        assert!(store.insert(current.clone()).is_ok_and(|inserted| inserted));
        let mut correction = current.clone();
        correction.content_hash = "ha-v2".into();
        correction.summary_text = Some("Updated guidance".into());
        assert!(
            store
                .insert_versioned(correction)
                .is_ok_and(|result| matches!(result, super::VersionInsert::Correction))
        );
        let mut related = item("b", "https://b", "hb", "ABC", 1_901);
        related.title = "Issuer raises outlook".into();
        assert!(store.insert(related).is_ok_and(|inserted| inserted));
        let Some(detail) = store.detail("a") else {
            return;
        };
        assert_eq!(detail.versions.len(), 2);
        assert_eq!(
            detail.current.summary_text.as_deref(),
            Some("Updated guidance")
        );
        assert_eq!(detail.related_item_ids, vec![String::from("b")]);
    }

    #[test]
    fn versioned_url_collision_does_not_orphan_existing_item() {
        let mut store = NewsStore::default();
        assert!(
            store
                .insert(item("a", "https://same", "hash-a", "ABC", 1_900))
                .is_ok()
        );
        let collision = item("b", "https://same", "hash-b", "ABC", 1_901);
        assert!(matches!(
            store.insert_versioned(collision),
            Ok(super::VersionInsert::Duplicate)
        ));
        assert!(store.contains_id("a"));
        assert!(!store.contains_id("b"));
        assert_eq!(store.versions("a").len(), 1);
    }

    #[test]
    fn detail_at_does_not_expose_late_corrections_or_related_articles() {
        let mut store = NewsStore::default();
        let original = item("a", "https://a", "ha", "ABC", 1_900);
        assert!(
            store
                .insert(original.clone())
                .is_ok_and(|inserted| inserted)
        );
        let mut correction = original;
        correction.content_hash = "ha-v2".into();
        correction.summary_text = Some("late correction".into());
        correction.received_at_ms = 2_100;
        assert!(matches!(
            store.insert_versioned(correction),
            Ok(VersionInsert::Correction)
        ));
        let mut related = item("b", "https://b", "hb", "ABC", 1_901);
        related.received_at_ms = 2_100;
        assert!(store.insert(related).is_ok_and(|inserted| inserted));
        let historical = store.detail_at("a", 2_000);
        assert_eq!(
            historical
                .as_ref()
                .map(|detail| detail.current.content_hash.as_str()),
            Some("ha")
        );
        assert_eq!(
            historical.as_ref().map(|detail| detail.versions.len()),
            Some(1)
        );
        assert!(
            historical
                .as_ref()
                .is_some_and(|detail| detail.related_item_ids.is_empty())
        );
        assert_eq!(
            store
                .detail_at("a", 2_100)
                .map(|detail| detail.current.content_hash),
            Some("ha-v2".into())
        );
    }

    struct Provider;
    impl NewsProvider for Provider {
        #[allow(clippy::unnecessary_literal_bound)]
        fn provider_id(&self) -> &str {
            "wire"
        }
        fn fetch(&self, _now_ms: i64) -> Result<Vec<NewsItem>, String> {
            Ok(vec![
                item("a", "https://a/", "ha", "abc", 1_900),
                item("b", "https://a", "hb", "ABC", 1_950),
            ])
        }
    }

    #[test]
    fn ingestion_normalizes_symbols_and_counts_duplicates() {
        let mut store = NewsStore::default();
        let report = ingest(&Provider, &mut store, 2_000);
        assert_eq!(
            report
                .ok()
                .map(|report| (report.accepted, report.duplicates)),
            Some((1, 1))
        );
        assert_eq!(store.relevant("ABC", 2_000, 10).len(), 1);
    }

    struct CursorWire;
    impl CursorProvider for CursorWire {
        #[allow(clippy::unnecessary_literal_bound)]
        fn provider_id(&self) -> &str {
            "wire"
        }

        fn fetch_page(&self, cursor: Option<&str>, _now_ms: i64) -> Result<ProviderBatch, String> {
            Ok(if cursor.is_none() {
                ProviderBatch {
                    items: vec![item("page-a", "https://page-a", "page-ha", "ABC", 1_900)],
                    next_cursor: Some("cursor-1".into()),
                }
            } else {
                ProviderBatch {
                    items: Vec::new(),
                    next_cursor: None,
                }
            })
        }
    }

    struct Committer {
        fail: bool,
        calls: usize,
    }

    impl CursorCommitter for Committer {
        fn commit_cursor(
            &mut self,
            _provider_id: &str,
            _expected_generation: u64,
            _next_cursor: Option<&str>,
        ) -> Result<(), String> {
            self.calls += 1;
            if self.fail {
                Err(String::from("journal unavailable"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn cursor_commit_happens_after_storage_and_failure_keeps_old_cursor() {
        let mut store = NewsStore::default();
        let mut state = CursorState::default();
        let mut committer = Committer {
            fail: true,
            calls: 0,
        };
        let result = ingest_cursor_with_commit(
            &CursorWire,
            &mut store,
            &mut state,
            &mut committer,
            2_000,
            10,
        );
        assert!(matches!(result, Err(super::IngestError::CursorCommit(_))));
        assert_eq!(committer.calls, 1);
        assert_eq!(state, CursorState::default());
        assert!(store.get("page-a").is_some());

        committer.fail = false;
        assert!(
            ingest_cursor_with_commit(
                &CursorWire,
                &mut store,
                &mut state,
                &mut committer,
                2_000,
                10,
            )
            .is_ok()
        );
        assert_eq!(state.cursor.as_deref(), Some("cursor-1"));
        assert_eq!(state.generation, 1);
    }

    #[test]
    fn provider_retry_and_rate_limits_are_bounded_and_deterministic() {
        let Some(policy) = super::RetryPolicy::new(2, 100, 500) else {
            return;
        };
        let mut retry = super::RetryState::default();
        assert_eq!(
            retry.on_failure(1_000, super::RetryClass::Transient, policy),
            super::RetryDecision::RetryAt(1_100)
        );
        assert_eq!(
            retry.on_failure(1_100, super::RetryClass::Transient, policy),
            super::RetryDecision::RetryAt(1_300)
        );
        assert_eq!(
            retry.on_failure(1_300, super::RetryClass::Transient, policy),
            super::RetryDecision::DeadLetter
        );
        let Some(mut limiter) = super::RequestLimiter::new(2, 1_000) else {
            return;
        };
        assert!(limiter.allow(5_000).is_ok());
        assert!(limiter.allow(5_001).is_ok());
        assert_eq!(limiter.allow(5_002), Err(6_000));
        assert!(limiter.allow(6_000).is_ok());
    }

    #[test]
    fn news_item_fields_are_bounded_before_storage() {
        let mut title = item("large", "https://large", "hash", "ABC", 1_900);
        title.title = "x".repeat(16_385);
        assert!(matches!(
            title.validate(),
            Err(super::NewsError::FieldTooLarge("title"))
        ));
        let mut summary = item("summary", "https://summary", "hash-2", "ABC", 1_900);
        summary.summary_text = Some("x".repeat(131_073));
        assert!(matches!(
            summary.validate(),
            Err(super::NewsError::FieldTooLarge("summary_text"))
        ));
        let mut symbols = item("symbols", "https://symbols", "hash-3", "ABC", 1_900);
        symbols.symbols = (0..257).map(|index| format!("S{index}")).collect();
        assert!(matches!(
            symbols.validate(),
            Err(super::NewsError::FieldTooLarge("symbols"))
        ));
        let mut unsafe_url = item("url", "http://example.test/article", "hash-4", "ABC", 1_900);
        assert!(matches!(
            unsafe_url.validate(),
            Err(super::NewsError::InvalidUrl)
        ));
        unsafe_url.canonical_url = "https://user:password@example.test/article".into();
        assert!(matches!(
            unsafe_url.validate(),
            Err(super::NewsError::InvalidUrl)
        ));
        unsafe_url.canonical_url = "https://example.test/article with-space".into();
        assert!(matches!(
            unsafe_url.validate(),
            Err(super::NewsError::InvalidUrl)
        ));
        unsafe_url.canonical_url = format!("https://example.test/{}", "a".repeat(2_040));
        assert!(matches!(
            unsafe_url.validate(),
            Err(super::NewsError::FieldTooLarge("canonical_url"))
        ));
    }

    #[test]
    fn news_store_capacity_cannot_exceed_hard_retention_bound() {
        let store = NewsStore::with_capacity(usize::MAX);
        assert_eq!(store.capacity, super::MAX_NEWS_ITEMS);
        let empty = NewsStore::with_capacity(0);
        assert_eq!(empty.capacity, 1);
    }
}
