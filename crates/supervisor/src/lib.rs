//! Failure-isolating component supervision and quarantine state machine.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

/// Lifecycle state of a supervised component.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum State {
    /// Component is eligible to run.
    Running,
    /// Component is waiting for its backoff deadline.
    Backoff,
    /// Component exceeded restart intensity and is isolated.
    Quarantined,
    /// Component is intentionally draining.
    Draining,
}

/// Health observed for a supervised dependency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Health {
    /// Dependency has not reported a state yet.
    Unknown,
    /// Dependency is serving normally.
    Healthy,
    /// Dependency is degraded but may still serve bounded work.
    Degraded,
    /// Dependency is unavailable or quarantined.
    Unavailable,
}

/// Restart intensity and backoff policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Policy {
    /// Maximum failures inside the rolling window before quarantine.
    pub max_failures: u32,
    /// Rolling failure window in nanoseconds.
    pub window_ns: u64,
    /// Initial backoff in nanoseconds.
    pub initial_backoff_ns: u64,
    /// Maximum backoff in nanoseconds.
    pub max_backoff_ns: u64,
    /// Maximum deterministic backoff jitter in basis points (`0..=10_000`).
    pub jitter_bps: u32,
}

/// Component state and failure history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Component {
    /// Stable component name.
    pub name: String,
    /// Current lifecycle state.
    pub state: State,
    /// Number of failures in the current window.
    pub failures: u32,
    /// Next time a restart may be attempted.
    pub retry_at_ns: u64,
    /// Current backoff duration.
    pub backoff_ns: u64,
    /// Most recent failure timestamp for rolling-window accounting.
    pub last_failure_ns: Option<u64>,
    /// Named dependencies that must be healthy before restart.
    pub dependencies: Vec<String>,
    /// Latest health published by this component.
    pub health: Health,
}

/// Bounded operational snapshot suitable for telemetry/read-model publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    /// Components in stable name order.
    pub components: Vec<Component>,
}

/// Failure while manually leaving quarantine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResumeError {
    /// Component is not registered.
    NotFound,
    /// Component is not currently quarantined.
    NotQuarantined,
    /// Operator identity is required for recovery.
    AuthorizationRequired,
}

/// Supervisor registry for independent component state.
pub struct Supervisor {
    policy: Policy,
    components: BTreeMap<String, Component>,
    dependency_health: BTreeMap<String, Health>,
}

impl Supervisor {
    /// Creates an empty supervisor with a bounded restart policy.
    #[must_use]
    pub const fn new(policy: Policy) -> Self {
        Self {
            policy,
            components: BTreeMap::new(),
            dependency_health: BTreeMap::new(),
        }
    }

    /// Registers a component as running.
    pub fn register(&mut self, name: impl Into<String>) -> bool {
        let name = name.into();
        if self.components.contains_key(&name) {
            return false;
        }
        self.components.insert(
            name.clone(),
            Component {
                name: name.clone(),
                state: State::Running,
                failures: 0,
                retry_at_ns: 0,
                backoff_ns: self.policy.initial_backoff_ns,
                last_failure_ns: None,
                dependencies: Vec::new(),
                health: Health::Healthy,
            },
        );
        self.dependency_health.insert(name, Health::Healthy);
        true
    }

    /// Registers a component with explicit dependency names.
    ///
    /// Dependencies are health-gated, not merely name-gated: a restart is
    /// withheld until each dependency reports [`Health::Healthy`].
    pub fn register_with_dependencies<I, S>(
        &mut self,
        name: impl Into<String>,
        dependencies: I,
    ) -> bool
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let name = name.into();
        if self.components.contains_key(&name) {
            return false;
        }
        let dependencies = dependencies.into_iter().map(Into::into).collect();
        self.components.insert(
            name.clone(),
            Component {
                name: name.clone(),
                state: State::Running,
                failures: 0,
                retry_at_ns: 0,
                backoff_ns: self.policy.initial_backoff_ns,
                last_failure_ns: None,
                dependencies,
                health: Health::Healthy,
            },
        );
        self.dependency_health.insert(name, Health::Healthy);
        true
    }

    /// Publishes one component's health for dependent restart gating.
    pub fn set_health(&mut self, name: &str, health: Health) -> bool {
        if !self.components.contains_key(name) {
            return false;
        }
        if let Some(component) = self.components.get_mut(name) {
            component.health = health;
        }
        self.dependency_health.insert(name.to_owned(), health);
        true
    }

    /// Returns whether all registered dependencies are healthy.
    #[must_use]
    pub fn dependencies_healthy(&self, name: &str) -> bool {
        let Some(component) = self.components.get(name) else {
            return false;
        };
        component.dependencies.iter().all(|dependency| {
            self.dependency_health
                .get(dependency)
                .copied()
                .unwrap_or(Health::Unknown)
                == Health::Healthy
        })
    }

    /// Records a failure and either schedules restart or quarantines the component.
    pub fn record_failure(&mut self, name: &str, now_ns: u64) -> Option<State> {
        let component = self.components.get_mut(name)?;
        if component
            .last_failure_ns
            .is_some_and(|previous| now_ns.saturating_sub(previous) > self.policy.window_ns)
        {
            component.failures = 0;
        }
        component.failures = component.failures.saturating_add(1);
        component.last_failure_ns = Some(now_ns);
        if component.failures >= self.policy.max_failures {
            component.state = State::Quarantined;
            return Some(component.state);
        }
        component.state = State::Backoff;
        let retry_backoff = jittered_backoff(
            self.policy.jitter_bps,
            component.backoff_ns,
            component.failures,
            name,
        );
        component.retry_at_ns = now_ns.saturating_add(retry_backoff);
        component.backoff_ns = component
            .backoff_ns
            .saturating_mul(2)
            .min(self.policy.max_backoff_ns);
        Some(component.state)
    }

    /// Returns a component to running when its backoff has elapsed.
    pub fn restart_if_ready(&mut self, name: &str, now_ns: u64) -> bool {
        if !self.dependencies_healthy(name) {
            return false;
        }
        let Some(component) = self.components.get_mut(name) else {
            return false;
        };
        if component.state != State::Backoff || now_ns < component.retry_at_ns {
            return false;
        }
        component.state = State::Running;
        true
    }

    /// Explicitly resets a quarantined component after operator authorization.
    ///
    /// # Errors
    /// Returns [`ResumeError`] when the component is absent, not quarantined,
    /// or the operator identity is blank.
    pub fn resume(&mut self, name: &str, authorization: &str) -> Result<(), ResumeError> {
        let Some(component) = self.components.get_mut(name) else {
            return Err(ResumeError::NotFound);
        };
        if component.state != State::Quarantined {
            return Err(ResumeError::NotQuarantined);
        }
        if authorization.trim().is_empty() {
            return Err(ResumeError::AuthorizationRequired);
        }
        component.state = State::Running;
        component.failures = 0;
        component.backoff_ns = self.policy.initial_backoff_ns;
        component.last_failure_ns = None;
        Ok(())
    }

    /// Begins graceful drain for all components.
    pub fn drain(&mut self) {
        for component in self.components.values_mut() {
            component.state = State::Draining;
        }
    }

    /// Reads a component state.
    #[must_use]
    pub fn component(&self, name: &str) -> Option<&Component> {
        self.components.get(name)
    }

    /// Returns a stable snapshot without exposing mutable supervisor state.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            components: self.components.values().cloned().collect(),
        }
    }
}

fn jittered_backoff(jitter_bps: u32, backoff_ns: u64, failures: u32, name: &str) -> u64 {
    if jitter_bps == 0 || backoff_ns == 0 {
        return backoff_ns;
    }
    let mut hash = u64::from(failures);
    for byte in name.as_bytes() {
        hash = hash
            .wrapping_mul(1_099_511_628_211)
            .wrapping_add(u64::from(*byte));
    }
    let jitter = hash % (u64::from(jitter_bps) + 1);
    backoff_ns.saturating_add(backoff_ns.saturating_mul(jitter) / 10_000)
}

#[cfg(test)]
mod tests {
    use super::{Health, Policy, State, Supervisor};

    fn policy() -> Policy {
        Policy {
            max_failures: 3,
            window_ns: 1_000,
            initial_backoff_ns: 10,
            max_backoff_ns: 100,
            jitter_bps: 0,
        }
    }

    #[test]
    fn failures_backoff_then_quarantine_independently() {
        let mut supervisor = Supervisor::new(policy());
        assert!(supervisor.register("market"));
        assert_eq!(supervisor.record_failure("market", 0), Some(State::Backoff));
        assert!(!supervisor.restart_if_ready("market", 9));
        assert!(supervisor.restart_if_ready("market", 10));
        assert_eq!(
            supervisor.record_failure("market", 20),
            Some(State::Backoff)
        );
        assert_eq!(
            supervisor.record_failure("market", 40),
            Some(State::Quarantined)
        );
        assert!(!supervisor.restart_if_ready("market", 1_000));
        assert!(supervisor.resume("market", "operator").is_ok());
        assert_eq!(
            supervisor
                .component("market")
                .map(|component| component.state),
            Some(State::Running)
        );
    }

    #[test]
    fn drain_affects_registered_components_only() {
        let mut supervisor = Supervisor::new(policy());
        assert!(supervisor.register("one"));
        supervisor.drain();
        assert_eq!(
            supervisor.component("one").map(|component| component.state),
            Some(State::Draining)
        );
        assert_eq!(supervisor.component("missing"), None);
    }

    #[test]
    fn failure_count_resets_after_rolling_window() {
        let mut supervisor = Supervisor::new(policy());
        assert!(supervisor.register("news"));
        assert_eq!(supervisor.record_failure("news", 0), Some(State::Backoff));
        assert!(supervisor.restart_if_ready("news", 10));
        assert_eq!(
            supervisor.record_failure("news", 2_000),
            Some(State::Backoff)
        );
        assert_eq!(
            supervisor
                .component("news")
                .map(|component| component.failures),
            Some(1)
        );
    }

    #[test]
    fn dependency_health_gates_restart_and_snapshot_is_stable() {
        let mut supervisor = Supervisor::new(Policy {
            max_failures: 3,
            window_ns: 1_000,
            initial_backoff_ns: 10,
            max_backoff_ns: 100,
            jitter_bps: 5_000,
        });
        assert!(supervisor.register("market"));
        assert!(supervisor.register_with_dependencies("strategy", ["market"]));
        assert_eq!(
            supervisor.record_failure("strategy", 0),
            Some(State::Backoff)
        );
        assert!(supervisor.set_health("market", Health::Degraded));
        assert!(!supervisor.restart_if_ready("strategy", 20));
        assert!(supervisor.set_health("market", Health::Healthy));
        assert!(supervisor.restart_if_ready("strategy", 20));
        let snapshot = supervisor.snapshot();
        assert_eq!(snapshot.components.len(), 2);
        assert_eq!(snapshot.components[0].name, "market");
        assert_eq!(snapshot.components[1].name, "strategy");
    }
}
