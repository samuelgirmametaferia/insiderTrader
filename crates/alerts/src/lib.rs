//! Bounded, deduplicated alert routing for workstation and operator channels.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

/// Stable subsystem identifier.
pub const SUBSYSTEM_ID: &str = "alerts";
/// Maximum persisted webhook destination length.
pub const MAX_WEBHOOK_URL_BYTES: usize = 2_048;

/// Operator-visible severity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Severity {
    /// Informational state change.
    Info,
    /// Degraded but non-blocking condition.
    Warning,
    /// Action or operator attention is required.
    Critical,
}

/// Delivery channel. Webhook URLs are accepted only through an allowlist.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Channel {
    /// Durable in-app alert center.
    InApp,
    /// Native desktop notification.
    Native,
    /// Local sound cue.
    Sound,
    /// External webhook endpoint.
    Webhook(String),
}

impl Channel {
    fn external(&self) -> bool {
        matches!(self, Self::Native | Self::Sound | Self::Webhook(_))
    }
}

/// Immutable alert event emitted by a runtime subsystem.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Alert {
    /// Stable event identity.
    pub alert_id: String,
    /// Dedupe identity shared by repeated observations of one condition.
    pub dedupe_key: String,
    /// Source subsystem/aggregate identity.
    pub source: String,
    /// Event timestamp in injected monotonic milliseconds.
    pub occurred_ms: i64,
    /// Severity shown to the operator.
    pub severity: Severity,
    /// Human-readable message; credentials must never be placed here.
    pub message: String,
    /// Whether the message contains sensitive fields and is restricted to `InApp`.
    pub sensitive: bool,
}

/// Alert routing failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlertError {
    /// Required identity/message field is blank.
    InvalidAlert,
    /// Event timestamp is in the future relative to the router clock.
    FutureTimestamp,
    /// An external channel was requested for sensitive content.
    SensitiveExternal,
    /// Webhook is not configured in the allowlist.
    WebhookNotAllowed,
    /// Pending alert bound is exhausted.
    Capacity,
}

/// Outcome of one route attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteOutcome {
    /// Alert was newly queued for delivery.
    Queued,
    /// Alert was suppressed by the dedupe cooldown.
    Suppressed,
}

/// Bounded alert router with deterministic cooldown and acknowledgement state.
#[derive(Clone, Debug)]
pub struct AlertRouter {
    cooldown_ms: i64,
    max_pending: usize,
    allowed_webhooks: BTreeSet<String>,
    last_routed: BTreeMap<(String, Channel), i64>,
    pending: BTreeMap<(String, Channel), (Alert, Channel)>,
    acknowledged: BTreeSet<String>,
}

impl AlertRouter {
    /// Creates a router with explicit queue and cooldown bounds.
    #[must_use]
    pub fn new(cooldown_ms: i64, max_pending: usize) -> Option<Self> {
        (cooldown_ms >= 0 && max_pending > 0).then(|| Self {
            cooldown_ms,
            max_pending,
            allowed_webhooks: BTreeSet::new(),
            last_routed: BTreeMap::new(),
            pending: BTreeMap::new(),
            acknowledged: BTreeSet::new(),
        })
    }

    /// Adds an exact webhook URL to the external delivery allowlist.
    pub fn allow_webhook(&mut self, url: impl Into<String>) -> bool {
        let url = url.into();
        if !is_allowed_url(&url) {
            return false;
        }
        self.allowed_webhooks.insert(url)
    }

    /// Updates cooldown and queue bounds without dropping pending deliveries.
    /// A smaller capacity is rejected while it would strand queued work.
    ///
    /// # Errors
    /// Returns [`AlertError::Capacity`] for invalid bounds or a capacity below
    /// the number of currently pending deliveries.
    pub fn validate_reconfigure(
        &self,
        cooldown_ms: i64,
        max_pending: usize,
    ) -> Result<(), AlertError> {
        if cooldown_ms < 0 || max_pending == 0 || self.pending.len() > max_pending {
            return Err(AlertError::Capacity);
        }
        Ok(())
    }

    /// Applies a previously validated routing-bound update.
    ///
    /// # Errors
    /// Returns [`AlertError::Capacity`] when the bounds are invalid or would
    /// strand pending deliveries.
    pub fn reconfigure(&mut self, cooldown_ms: i64, max_pending: usize) -> Result<(), AlertError> {
        self.validate_reconfigure(cooldown_ms, max_pending)?;
        self.cooldown_ms = cooldown_ms;
        self.max_pending = max_pending;
        Ok(())
    }

    /// Routes one alert after validation, deduplication, and capacity checks.
    ///
    /// # Errors
    /// Returns [`AlertError`] before a delivery adapter can observe an invalid,
    /// sensitive, disallowed, future, or over-capacity event.
    pub fn route(
        &mut self,
        alert: Alert,
        channel: Channel,
        now_ms: i64,
    ) -> Result<RouteOutcome, AlertError> {
        if alert.alert_id.trim().is_empty()
            || alert.dedupe_key.trim().is_empty()
            || alert.source.trim().is_empty()
            || alert.message.trim().is_empty()
        {
            return Err(AlertError::InvalidAlert);
        }
        if alert.occurred_ms > now_ms {
            return Err(AlertError::FutureTimestamp);
        }
        if alert.sensitive && channel.external() {
            return Err(AlertError::SensitiveExternal);
        }
        if let Channel::Webhook(url) = &channel
            && !self.allowed_webhooks.contains(url)
        {
            return Err(AlertError::WebhookNotAllowed);
        }
        let key = (alert.dedupe_key.clone(), channel.clone());
        if self
            .last_routed
            .get(&key)
            .is_some_and(|last| now_ms.saturating_sub(*last) < self.cooldown_ms)
        {
            return Ok(RouteOutcome::Suppressed);
        }
        if self.pending.len() >= self.max_pending {
            return Err(AlertError::Capacity);
        }
        self.last_routed.insert(key, now_ms);
        self.pending
            .insert((alert.alert_id.clone(), channel.clone()), (alert, channel));
        Ok(RouteOutcome::Queued)
    }

    /// Acknowledges and removes only the in-app delivery for one alert.
    /// External deliveries (for example a webhook) remain pending until their
    /// delivery adapter acknowledges that specific channel.
    pub fn acknowledge(&mut self, alert_id: &str) -> bool {
        let keys: Vec<(String, Channel)> = self
            .pending
            .keys()
            .filter(|(id, channel)| id == alert_id && matches!(channel, Channel::InApp))
            .cloned()
            .collect();
        let existed = !keys.is_empty();
        for key in keys {
            self.pending.remove(&key);
        }
        if existed {
            self.acknowledged.insert(alert_id.to_owned());
        }
        existed
    }

    /// Acknowledges delivery on one channel while retaining other channels.
    pub fn acknowledge_channel(&mut self, alert_id: &str, channel: &Channel) -> bool {
        let key = (alert_id.to_owned(), channel.clone());
        if self.pending.remove(&key).is_some() {
            if !self.pending.keys().any(|(id, _)| id == alert_id) {
                self.acknowledged.insert(alert_id.to_owned());
            }
            true
        } else {
            false
        }
    }

    /// Returns whether an alert was acknowledged.
    #[must_use]
    pub fn is_acknowledged(&self, alert_id: &str) -> bool {
        self.acknowledged.contains(alert_id)
    }

    /// Returns a deterministic snapshot of pending deliveries.
    #[must_use]
    pub fn pending(&self) -> Vec<(&Alert, &Channel)> {
        self.pending
            .values()
            .map(|(alert, channel)| (alert, channel))
            .collect()
    }
}

fn is_allowed_url(url: &str) -> bool {
    if url.len() > MAX_WEBHOOK_URL_BYTES {
        return false;
    }
    let Some((scheme, remainder)) = url.split_once("://") else {
        return false;
    };
    let authority = remainder.split(['/', '?', '#']).next().unwrap_or_default();
    matches!(scheme, "https")
        && !remainder.trim().is_empty()
        && !authority.is_empty()
        && !remainder.contains(char::is_whitespace)
        && !remainder.contains('@')
}

#[cfg(test)]
mod tests {
    use super::{
        Alert, AlertError, AlertRouter, Channel, MAX_WEBHOOK_URL_BYTES, RouteOutcome, SUBSYSTEM_ID,
        Severity,
    };

    fn alert(id: &str, sensitive: bool) -> Alert {
        Alert {
            alert_id: id.into(),
            dedupe_key: "risk:halt".into(),
            source: "risk-engine".into(),
            occurred_ms: 10,
            severity: Severity::Critical,
            message: "risk halted".into(),
            sensitive,
        }
    }

    #[test]
    fn router_deduplicates_acknowledges_and_guards_external_channels() {
        assert!(!SUBSYSTEM_ID.is_empty());
        let Some(mut router) = AlertRouter::new(1_000, 4) else {
            return;
        };
        assert_eq!(
            router.route(alert("a", false), Channel::InApp, 10),
            Ok(RouteOutcome::Queued)
        );
        assert_eq!(
            router.route(alert("b", false), Channel::InApp, 500),
            Ok(RouteOutcome::Suppressed)
        );
        assert_eq!(
            router.route(alert("c", true), Channel::Native, 2_000),
            Err(AlertError::SensitiveExternal)
        );
        assert!(router.acknowledge("a"));
        assert!(router.is_acknowledged("a"));
    }

    #[test]
    fn in_app_acknowledgement_does_not_drop_external_delivery() {
        let Some(mut router) = AlertRouter::new(0, 4) else {
            return;
        };
        let alert = alert("dual", false);
        let webhook = Channel::Webhook("https://ops.example/hook".into());
        assert!(router.allow_webhook("https://ops.example/hook"));
        assert_eq!(
            router.route(alert.clone(), Channel::InApp, 10),
            Ok(RouteOutcome::Queued)
        );
        assert_eq!(
            router.route(alert, webhook.clone(), 10),
            Ok(RouteOutcome::Queued)
        );
        assert!(router.acknowledge("dual"));
        assert_eq!(router.pending().len(), 1);
        assert!(matches!(router.pending()[0].1, Channel::Webhook(_)));
        assert!(router.acknowledge_channel("dual", &webhook));
    }

    #[test]
    fn webhook_delivery_requires_https_allowlist() {
        let Some(mut router) = AlertRouter::new(0, 2) else {
            return;
        };
        let alert = alert("webhook", false);
        assert_eq!(
            router.route(alert.clone(), Channel::Webhook("http://bad".into()), 10),
            Err(AlertError::WebhookNotAllowed)
        );
        assert!(router.allow_webhook("https://ops.example/hook"));
        assert_eq!(
            router.route(
                alert,
                Channel::Webhook("https://ops.example/hook".into()),
                10
            ),
            Ok(RouteOutcome::Queued)
        );
        assert!(router.reconfigure(5_000, 1).is_ok());
        assert_eq!(router.reconfigure(5_000, 0), Err(AlertError::Capacity));
        assert!(!router.allow_webhook(format!(
            "https://ops.example/{}",
            "x".repeat(MAX_WEBHOOK_URL_BYTES)
        )));
        assert!(!router.allow_webhook("https://user:password@ops.example/hook"));
        assert!(!router.allow_webhook("https:///missing-authority"));
    }
}
