//! Shared provider manifests and fail-closed discovery contracts.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

/// Stable subsystem identifier.
pub const SUBSYSTEM_ID: &str = "provider_core";
const MAX_FIELD_BYTES: usize = 256;
const MAX_PROVIDER_ID_BYTES: usize = 64;
const MAX_CAPABILITIES: usize = 64;

/// External system category.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProviderKind {
    /// Market quotes/trades/books/bars.
    Market,
    /// News and event feeds.
    News,
    /// LLM completion provider.
    Llm,
    /// Broker/order gateway.
    Broker,
}

/// Authentication mechanism declared by an adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthMethod {
    /// No credentials are required.
    None,
    /// API key supplied through secret storage.
    ApiKey,
    /// OAuth/token exchange through secret storage.
    OAuth,
    /// Broker session/login credentials.
    Session,
    /// Mutual TLS or equivalent certificate identity.
    MutualTls,
}

/// Bounded retry policy declared by a provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Maximum retries for one request.
    pub max_retries: u8,
    /// Initial backoff in milliseconds.
    pub initial_backoff_ms: u64,
    /// Maximum backoff in milliseconds.
    pub max_backoff_ms: u64,
    /// Whether server retry hints may be honored.
    pub honor_retry_after: bool,
}

/// Provider timeout and concurrency limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeoutPolicy {
    /// Connection deadline.
    pub connect_timeout_ms: u64,
    /// Request deadline.
    pub request_timeout_ms: u64,
    /// Maximum concurrent requests.
    pub max_parallel_requests: u16,
    /// Maximum requests in the configured rate window.
    pub max_requests: u32,
    /// Rate-limit window duration.
    pub window_ms: u64,
}

/// Versioned provider manifest shared by every adapter class.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderManifest {
    /// Stable provider identity.
    pub provider_id: String,
    /// Adapter category.
    pub kind: ProviderKind,
    /// Protocol/API schema version.
    pub schema_version: String,
    /// Base URL or local endpoint identifier.
    pub base_url: String,
    /// Credential mechanism.
    pub auth: AuthMethod,
    /// Declared bounded capabilities.
    pub capabilities: Vec<String>,
    /// Retry behavior.
    pub retry: RetryPolicy,
    /// Timeouts and rate limits.
    pub timeout: TimeoutPolicy,
    /// Optional health endpoint/path.
    pub health_probe: Option<String>,
    /// Whether the adapter supports streaming transport.
    pub streaming: bool,
}

impl ProviderManifest {
    /// Validates a manifest before registration.
    ///
    /// # Errors
    /// Returns [`ManifestError`] when identity, endpoint, capability, or policy
    /// fields are malformed or exceed declared bounds.
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.provider_id.trim().is_empty() || self.provider_id.len() > MAX_PROVIDER_ID_BYTES {
            return Err(ManifestError::InvalidField("provider_id"));
        }
        for (name, value) in [
            ("schema_version", self.schema_version.as_str()),
            ("base_url", self.base_url.as_str()),
        ] {
            if value.trim().is_empty() || value.len() > MAX_FIELD_BYTES {
                return Err(ManifestError::InvalidField(name));
            }
        }
        if self.capabilities.len() > MAX_CAPABILITIES
            || self.capabilities.windows(2).any(|pair| pair[0] >= pair[1])
            || self.capabilities.iter().any(|capability| {
                capability.trim().is_empty() || capability.len() > MAX_FIELD_BYTES
            })
        {
            return Err(ManifestError::InvalidField("capabilities"));
        }
        if self.retry.max_backoff_ms < self.retry.initial_backoff_ms
            || self.timeout.connect_timeout_ms == 0
            || self.timeout.request_timeout_ms == 0
            || self.timeout.max_parallel_requests == 0
            || self.timeout.max_requests == 0
            || self.timeout.window_ms == 0
        {
            return Err(ManifestError::InvalidPolicy);
        }
        if self
            .health_probe
            .as_ref()
            .is_some_and(|probe| probe.trim().is_empty() || probe.len() > MAX_FIELD_BYTES)
        {
            return Err(ManifestError::InvalidField("health_probe"));
        }
        Ok(())
    }
}

/// Manifest validation or registry failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManifestError {
    /// A named field is blank, oversized, or non-canonical.
    InvalidField(&'static str),
    /// Timeout/retry/rate policy is unsafe or zero.
    InvalidPolicy,
    /// Provider identity is already registered.
    Duplicate,
    /// Provider identity is not registered.
    NotFound,
}

/// Deterministic provider manifest registry.
#[derive(Clone, Default)]
pub struct ProviderRegistry {
    manifests: BTreeMap<String, ProviderManifest>,
}

impl ProviderRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a validated provider manifest exactly once.
    ///
    /// # Errors
    /// Returns [`ManifestError`] for invalid metadata or duplicate identity.
    pub fn register(&mut self, manifest: ProviderManifest) -> Result<(), ManifestError> {
        manifest.validate()?;
        if self.manifests.contains_key(&manifest.provider_id) {
            return Err(ManifestError::Duplicate);
        }
        self.manifests
            .insert(manifest.provider_id.clone(), manifest);
        Ok(())
    }

    /// Resolves an exact provider manifest.
    #[must_use]
    pub fn get(&self, provider_id: &str) -> Option<&ProviderManifest> {
        self.manifests.get(provider_id)
    }

    /// Returns manifests in deterministic provider-ID order.
    #[must_use]
    pub fn manifests(&self) -> Vec<ProviderManifest> {
        self.manifests.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuthMethod, ProviderKind, ProviderManifest, ProviderRegistry, RetryPolicy, TimeoutPolicy,
    };

    fn manifest(id: &str) -> ProviderManifest {
        ProviderManifest {
            provider_id: id.into(),
            kind: ProviderKind::Market,
            schema_version: "1".into(),
            base_url: "https://provider.test".into(),
            auth: AuthMethod::ApiKey,
            capabilities: vec!["quotes".into(), "trades".into()],
            retry: RetryPolicy {
                max_retries: 2,
                initial_backoff_ms: 10,
                max_backoff_ms: 100,
                honor_retry_after: true,
            },
            timeout: TimeoutPolicy {
                connect_timeout_ms: 100,
                request_timeout_ms: 1_000,
                max_parallel_requests: 4,
                max_requests: 20,
                window_ms: 1_000,
            },
            health_probe: Some("/health".into()),
            streaming: true,
        }
    }

    #[test]
    fn registry_is_bounded_and_duplicate_safe() {
        let mut registry = ProviderRegistry::new();
        assert!(registry.register(manifest("market-a")).is_ok());
        assert_eq!(
            registry.register(manifest("market-a")),
            Err(super::ManifestError::Duplicate)
        );
        assert_eq!(registry.manifests().len(), 1);
    }

    #[test]
    fn provider_identity_is_bounded_to_64_bytes() {
        assert!(manifest(&"p".repeat(64)).validate().is_ok());
        assert_eq!(
            manifest(&"p".repeat(65)).validate(),
            Err(super::ManifestError::InvalidField("provider_id"))
        );
        assert_eq!(
            manifest("   ").validate(),
            Err(super::ManifestError::InvalidField("provider_id"))
        );
        assert!(manifest(&"é".repeat(32)).validate().is_ok());
        assert_eq!(
            manifest(&"é".repeat(33)).validate(),
            Err(super::ManifestError::InvalidField("provider_id"))
        );
    }
}
