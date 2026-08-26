//! Policy-enforced execution of schema-validated autonomous plans.

#![forbid(unsafe_code)]

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "autonomy";

/// Hard upper bound for the typed live-enable challenge.
pub const LIVE_ENABLE_TTL_NS: u64 = 300_000_000_000;

/// Runtime environment enforced immediately before an order transport call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TradingEnvironment {
    /// Orders may only use a paper/simulated broker.
    Paper,
    /// Live order submission is enabled for the configured account.
    Live,
    /// An operator kill switch has disabled live submission.
    Killed,
}

/// Account and notional constraints for live order submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveLimits {
    /// Accounts permitted to enable live trading.
    pub allowed_accounts: std::collections::BTreeSet<String>,
    /// Maximum absolute estimated order notional in canonical ticks.
    pub max_notional_ticks: u64,
}

/// Failure returned by the live-enablement and pre-send guard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiveGuardError {
    /// The account is not in the explicit live allowlist.
    AccountNotAllowed,
    /// The required typed phrase was not supplied exactly.
    ConfirmationRequired,
    /// The challenge is absent, expired, or does not match.
    ChallengeInvalid,
    /// Live submission is disabled or killed.
    NotLive,
    /// A live order exceeded the configured hard cap.
    NotionalLimit,
    /// Market order notional could not be bounded before transport.
    NotionalUnknown,
}

#[derive(Clone, Debug)]
struct LiveChallenge {
    token: String,
    account: String,
    expires_at: MonoTime,
}

/// Fail-closed two-step live trading gate.
#[derive(Clone, Debug)]
pub struct LiveGuard {
    limits: LiveLimits,
    environment: TradingEnvironment,
    challenge: Option<LiveChallenge>,
    next_challenge: u64,
}

impl LiveGuard {
    /// Creates a paper-only guard. Live trading must be explicitly armed.
    #[must_use]
    pub fn paper(limits: LiveLimits) -> Self {
        Self {
            limits,
            environment: TradingEnvironment::Paper,
            challenge: None,
            next_challenge: 1,
        }
    }

    /// Returns the currently enforced environment.
    #[must_use]
    pub const fn environment(&self) -> TradingEnvironment {
        self.environment
    }

    /// Begins the first step of live enablement and returns a one-use token.
    ///
    /// The caller must still provide the returned token and the exact second
    /// phrase to [`Self::confirm_live`].
    ///
    /// # Errors
    /// Returns an account, phrase, or configuration error without arming live.
    pub fn arm_live(
        &mut self,
        account: &str,
        now: MonoTime,
        phrase: &str,
    ) -> Result<String, LiveGuardError> {
        if !self.limits.allowed_accounts.contains(account) {
            return Err(LiveGuardError::AccountNotAllowed);
        }
        if phrase != "ARM LIVE" {
            return Err(LiveGuardError::ConfirmationRequired);
        }
        let token = format!("LIVE-{}-{}", account, self.next_challenge);
        self.next_challenge = self.next_challenge.saturating_add(1);
        self.challenge = Some(LiveChallenge {
            token: token.clone(),
            account: account.to_owned(),
            expires_at: MonoTime::from_nanos(now.as_nanos().saturating_add(LIVE_ENABLE_TTL_NS)),
        });
        Ok(token)
    }

    /// Completes live enablement after checking the account, token, expiry,
    /// and exact second confirmation phrase.
    ///
    /// # Errors
    /// Returns an authentication/challenge error without enabling live.
    pub fn confirm_live(
        &mut self,
        account: &str,
        token: &str,
        now: MonoTime,
        phrase: &str,
    ) -> Result<(), LiveGuardError> {
        if phrase != "ENABLE LIVE" {
            return Err(LiveGuardError::ConfirmationRequired);
        }
        let Some(challenge) = self.challenge.take() else {
            return Err(LiveGuardError::ChallengeInvalid);
        };
        if now >= challenge.expires_at
            || challenge.account != account
            || challenge.token != token
            || !self.limits.allowed_accounts.contains(account)
        {
            return Err(LiveGuardError::ChallengeInvalid);
        }
        self.environment = TradingEnvironment::Live;
        Ok(())
    }

    /// Immediately disables live submission. Re-enabling requires both steps.
    pub fn kill_switch(&mut self) {
        self.environment = TradingEnvironment::Killed;
        self.challenge = None;
    }

    /// Returns to paper mode and clears any pending enablement challenge.
    pub fn disable_live(&mut self) {
        self.environment = TradingEnvironment::Paper;
        self.challenge = None;
    }

    /// Enforces the live account and hard notional boundary before transport.
    ///
    /// # Errors
    /// Returns an environment, allowlist, notional, or boundedness error.
    pub fn authorize(
        &self,
        account: &str,
        estimated_notional_ticks: Option<u128>,
    ) -> Result<(), LiveGuardError> {
        match self.environment {
            TradingEnvironment::Paper => return Ok(()),
            TradingEnvironment::Killed => return Err(LiveGuardError::NotLive),
            TradingEnvironment::Live => {}
        }
        if !self.limits.allowed_accounts.contains(account) {
            return Err(LiveGuardError::AccountNotAllowed);
        }
        let Some(notional) = estimated_notional_ticks else {
            return Err(LiveGuardError::NotionalUnknown);
        };
        if notional > u128::from(self.limits.max_notional_ticks) {
            return Err(LiveGuardError::NotionalLimit);
        }
        Ok(())
    }
}

use insider_common_types::MonoTime;
use insider_llm_core::{ActionType, AutonomousAction, LlmError};
use insider_strategy_sdk::Proposal;

/// Operator-selected automation mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    /// Plans are displayed but never executed.
    Manual,
    /// Only actions explicitly allowed by the policy may execute.
    Hybrid,
    /// Valid plan actions may execute through normal engine services.
    Autonomous,
}

/// Hybrid-mode constraints.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Policy {
    /// Current mode.
    pub mode: Mode,
    /// Whether opening/increasing exposure is permitted automatically.
    pub allow_entries: bool,
    /// Maximum absolute scale for one action.
    pub max_scale: f64,
}

/// A timestamped plan from the LLM layer.
#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    /// Stable plan identity.
    pub plan_id: String,
    /// Time at which this plan was generated.
    pub generated_at: MonoTime,
    /// Time after which actions must not execute.
    pub expires_at: MonoTime,
    /// Finite validated action list.
    pub actions: Vec<AutonomousAction>,
}

/// Durable lifecycle state for one autonomous plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanState {
    /// Received and awaiting policy/proposal evaluation.
    Pending,
    /// Policy evaluation completed and actions may be handed to the engine.
    Approved,
    /// Explicitly rejected by policy/operator.
    Rejected,
    /// TTL elapsed before execution.
    Expired,
    /// At least one approved action is being submitted through normal services.
    Executing,
    /// All approved actions completed.
    Completed,
    /// Execution failed or was interrupted and requires operator review.
    Failed,
}

/// Durable event emitted by the autonomy plan lifecycle.
#[derive(Clone, Debug, PartialEq)]
pub enum PlanEvent {
    /// A validated plan was accepted into the store.
    Submitted(Plan),
    /// A plan transitioned to a new lifecycle state.
    Transition {
        /// Stable plan identity.
        plan_id: String,
        /// New lifecycle state.
        state: PlanState,
    },
}

const PLAN_EVENT_MAGIC: &[u8] = b"IT_PLAN_EVENT_V1\0";
const MAX_PLAN_BYTES: usize = 4 * 1024 * 1024;

/// Encodes one autonomy lifecycle event for journal persistence.
#[must_use]
pub fn encode_plan_event(event: &PlanEvent) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(PLAN_EVENT_MAGIC);
    match event {
        PlanEvent::Submitted(plan) => {
            output.push(1);
            push_string(&mut output, &plan.plan_id);
            output.extend_from_slice(&plan.generated_at.as_nanos().to_le_bytes());
            output.extend_from_slice(&plan.expires_at.as_nanos().to_le_bytes());
            output.extend_from_slice(
                &u32::try_from(plan.actions.len())
                    .unwrap_or(u32::MAX)
                    .to_le_bytes(),
            );
            for action in &plan.actions {
                output.push(action_type_code(action.action_type));
                push_optional_string(&mut output, action.proposal_id.as_deref());
                output.push(u8::from(action.scale.is_some()));
                output.extend_from_slice(&action.scale.unwrap_or_default().to_bits().to_le_bytes());
                output.extend_from_slice(
                    &u16::try_from(action.reason_codes.len())
                        .unwrap_or(u16::MAX)
                        .to_le_bytes(),
                );
                for reason in &action.reason_codes {
                    push_string(&mut output, reason);
                }
            }
        }
        PlanEvent::Transition { plan_id, state } => {
            output.push(2);
            push_string(&mut output, plan_id);
            output.push(plan_state_code(*state));
        }
    }
    output
}

/// Decodes and validates one persisted autonomy lifecycle event.
///
/// # Errors
/// Returns [`LlmError::MalformedOutput`] for invalid framing, bounds, UTF-8,
/// enum values, or trailing bytes; schema-invalid plans are rejected before
/// they can enter a restored store.
pub fn decode_plan_event(payload: &[u8]) -> Result<PlanEvent, LlmError> {
    if payload.len() > MAX_PLAN_BYTES || !payload.starts_with(PLAN_EVENT_MAGIC) {
        return Err(LlmError::MalformedOutput(
            "invalid plan event framing".into(),
        ));
    }
    let mut cursor = PLAN_EVENT_MAGIC.len();
    let kind = read_u8(payload, &mut cursor)?;
    let event = match kind {
        1 => {
            let plan_id = read_string(payload, &mut cursor)?;
            let generated_at = MonoTime::from_nanos(read_u64(payload, &mut cursor)?);
            let expires_at = MonoTime::from_nanos(read_u64(payload, &mut cursor)?);
            let count = usize::try_from(read_u32(payload, &mut cursor)?)
                .map_err(|_| malformed("action count overflow"))?;
            if count > 4_096 {
                return Err(malformed("action count exceeds bound"));
            }
            let mut actions = Vec::with_capacity(count);
            for _ in 0..count {
                let action_type = decode_action_type(read_u8(payload, &mut cursor)?)?;
                let proposal_id = read_optional_string(payload, &mut cursor)?;
                let has_scale = read_u8(payload, &mut cursor)?;
                if has_scale > 1 {
                    return Err(malformed("invalid scale marker"));
                }
                let scale = f64::from_bits(read_u64(payload, &mut cursor)?);
                let reason_count = usize::from(read_u16(payload, &mut cursor)?);
                if reason_count > 256 {
                    return Err(malformed("reason count exceeds bound"));
                }
                let mut reason_codes = Vec::with_capacity(reason_count);
                for _ in 0..reason_count {
                    reason_codes.push(read_string(payload, &mut cursor)?);
                }
                actions.push(insider_llm_core::AutonomousAction {
                    action_type,
                    proposal_id,
                    scale: (has_scale == 1).then_some(scale),
                    reason_codes,
                });
            }
            if cursor != payload.len() {
                return Err(malformed("trailing plan event bytes"));
            }
            let plan = Plan {
                plan_id,
                generated_at,
                expires_at,
                actions,
            };
            plan.validate(generated_at).map_err(|error| {
                LlmError::SchemaViolation(format!("invalid persisted plan: {error:?}"))
            })?;
            PlanEvent::Submitted(plan)
        }
        2 => {
            let plan_id = read_string(payload, &mut cursor)?;
            let state = decode_plan_state(read_u8(payload, &mut cursor)?)?;
            if cursor != payload.len() {
                return Err(malformed("trailing transition bytes"));
            }
            PlanEvent::Transition { plan_id, state }
        }
        _ => return Err(malformed("unknown plan event kind")),
    };
    Ok(event)
}

fn malformed(message: &str) -> LlmError {
    LlmError::MalformedOutput(message.into())
}

fn push_string(output: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    output.extend_from_slice(&u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_le_bytes());
    output.extend_from_slice(bytes);
}

fn push_optional_string(output: &mut Vec<u8>, value: Option<&str>) {
    output.push(u8::from(value.is_some()));
    if let Some(value) = value {
        push_string(output, value);
    }
}

fn read_bytes<'a>(
    payload: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], LlmError> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| malformed("plan event cursor overflow"))?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or_else(|| malformed("truncated plan event"))?;
    *cursor = end;
    Ok(bytes)
}

fn read_u8(payload: &[u8], cursor: &mut usize) -> Result<u8, LlmError> {
    Ok(read_bytes(payload, cursor, 1)?[0])
}

fn read_u16(payload: &[u8], cursor: &mut usize) -> Result<u16, LlmError> {
    Ok(u16::from_le_bytes(
        read_bytes(payload, cursor, 2)?
            .try_into()
            .map_err(|_| malformed("invalid u16"))?,
    ))
}

fn read_u32(payload: &[u8], cursor: &mut usize) -> Result<u32, LlmError> {
    Ok(u32::from_le_bytes(
        read_bytes(payload, cursor, 4)?
            .try_into()
            .map_err(|_| malformed("invalid u32"))?,
    ))
}

fn read_u64(payload: &[u8], cursor: &mut usize) -> Result<u64, LlmError> {
    Ok(u64::from_le_bytes(
        read_bytes(payload, cursor, 8)?
            .try_into()
            .map_err(|_| malformed("invalid u64"))?,
    ))
}

fn read_string(payload: &[u8], cursor: &mut usize) -> Result<String, LlmError> {
    let length = usize::try_from(read_u32(payload, cursor)?)
        .map_err(|_| malformed("string length overflow"))?;
    if length > 1_048_576 {
        return Err(malformed("string exceeds bound"));
    }
    String::from_utf8(read_bytes(payload, cursor, length)?.to_vec())
        .map_err(|_| malformed("invalid UTF-8"))
}

fn read_optional_string(payload: &[u8], cursor: &mut usize) -> Result<Option<String>, LlmError> {
    match read_u8(payload, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(read_string(payload, cursor)?)),
        _ => Err(malformed("invalid optional string marker")),
    }
}

fn action_type_code(action_type: insider_llm_core::ActionType) -> u8 {
    use insider_llm_core::ActionType;
    match action_type {
        ActionType::ExecuteProposal => 1,
        ActionType::ExecuteProposalScaled => 2,
        ActionType::IgnoreProposal => 3,
        ActionType::PauseStrategy => 4,
        ActionType::ResumeStrategy => 5,
        ActionType::RequestReanalysis => 6,
        ActionType::AddToWatch => 7,
        ActionType::RemoveFromWatch => 8,
        ActionType::ReduceAutonomy => 9,
        ActionType::NoAction => 10,
    }
}

fn decode_action_type(code: u8) -> Result<insider_llm_core::ActionType, LlmError> {
    use insider_llm_core::ActionType;
    match code {
        1 => Ok(ActionType::ExecuteProposal),
        2 => Ok(ActionType::ExecuteProposalScaled),
        3 => Ok(ActionType::IgnoreProposal),
        4 => Ok(ActionType::PauseStrategy),
        5 => Ok(ActionType::ResumeStrategy),
        6 => Ok(ActionType::RequestReanalysis),
        7 => Ok(ActionType::AddToWatch),
        8 => Ok(ActionType::RemoveFromWatch),
        9 => Ok(ActionType::ReduceAutonomy),
        10 => Ok(ActionType::NoAction),
        _ => Err(malformed("unknown action type")),
    }
}

fn plan_state_code(state: PlanState) -> u8 {
    match state {
        PlanState::Pending => 1,
        PlanState::Approved => 2,
        PlanState::Rejected => 3,
        PlanState::Expired => 4,
        PlanState::Executing => 5,
        PlanState::Completed => 6,
        PlanState::Failed => 7,
    }
}

fn decode_plan_state(code: u8) -> Result<PlanState, LlmError> {
    match code {
        1 => Ok(PlanState::Pending),
        2 => Ok(PlanState::Approved),
        3 => Ok(PlanState::Rejected),
        4 => Ok(PlanState::Expired),
        5 => Ok(PlanState::Executing),
        6 => Ok(PlanState::Completed),
        7 => Ok(PlanState::Failed),
        _ => Err(malformed("unknown plan state")),
    }
}

/// Plan lifecycle mutation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanError {
    /// Plan failed schema/TTL validation.
    Invalid(LlmError),
    /// Plan ID already exists in the immutable store.
    Duplicate,
    /// Plan ID is absent.
    NotFound,
    /// Requested state transition is not legal.
    InvalidTransition {
        /// Current lifecycle state.
        from: PlanState,
        /// Requested lifecycle state.
        to: PlanState,
    },
    /// A terminal/expired plan cannot be executed.
    Expired,
}

/// Immutable plan plus its authoritative lifecycle state.
#[derive(Clone, Debug, PartialEq)]
pub struct PlanRecord {
    /// Original validated plan.
    pub plan: Plan,
    /// Current lifecycle state.
    pub state: PlanState,
}

/// In-memory projection of durable plan lifecycle events.
#[derive(Clone, Default)]
pub struct PlanStore {
    records: std::collections::BTreeMap<String, PlanRecord>,
    events: Vec<PlanEvent>,
}

impl PlanStore {
    /// Creates an empty plan store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a validated plan exactly once.
    ///
    /// # Errors
    /// Returns [`PlanError`] for invalid plans or duplicate IDs.
    pub fn submit(&mut self, plan: Plan, now: MonoTime) -> Result<(), PlanError> {
        plan.validate(now).map_err(PlanError::Invalid)?;
        if self.records.contains_key(&plan.plan_id) {
            return Err(PlanError::Duplicate);
        }
        let event_plan = plan.clone();
        self.records.insert(
            plan.plan_id.clone(),
            PlanRecord {
                plan,
                state: PlanState::Pending,
            },
        );
        self.events.push(PlanEvent::Submitted(event_plan));
        Ok(())
    }

    /// Returns a plan record by immutable plan ID.
    #[must_use]
    pub fn get(&self, plan_id: &str) -> Option<&PlanRecord> {
        self.records.get(plan_id)
    }

    /// Returns executing plan IDs in deterministic order for crash recovery.
    #[must_use]
    pub fn executing_plan_ids(&self) -> Vec<String> {
        self.records
            .iter()
            .filter(|(_, record)| record.state == PlanState::Executing)
            .map(|(plan_id, _)| plan_id.clone())
            .collect()
    }

    /// Returns the most recently generated plan using a deterministic tie-break.
    #[must_use]
    pub fn latest(&self) -> Option<&PlanRecord> {
        self.records.values().max_by(|left, right| {
            left.plan
                .generated_at
                .cmp(&right.plan.generated_at)
                .then_with(|| left.plan.plan_id.cmp(&right.plan.plan_id))
        })
    }

    /// Transitions a plan through the legal lifecycle using injected time.
    /// Repeating the current state is idempotent.
    ///
    /// # Errors
    /// Returns [`PlanError`] for missing, expired, or illegal transitions.
    pub fn transition(
        &mut self,
        plan_id: &str,
        next: PlanState,
        now: MonoTime,
    ) -> Result<(), PlanError> {
        let (emitted_state, expired_before_requested_transition) = {
            let record = self.records.get_mut(plan_id).ok_or(PlanError::NotFound)?;
            if record.state == next {
                return Ok(());
            }
            if now >= record.plan.expires_at
                && !matches!(record.state, PlanState::Completed | PlanState::Failed)
            {
                record.state = PlanState::Expired;
                (PlanState::Expired, next != PlanState::Expired)
            } else {
                if !legal_transition(record.state, next) {
                    return Err(PlanError::InvalidTransition {
                        from: record.state,
                        to: next,
                    });
                }
                record.state = next;
                (next, false)
            }
        };
        self.events.push(PlanEvent::Transition {
            plan_id: plan_id.to_owned(),
            state: emitted_state,
        });
        if expired_before_requested_transition {
            return Err(PlanError::Expired);
        }
        Ok(())
    }

    /// Returns lifecycle events that have not yet been persisted by the
    /// caller. Events are immutable and ordered by mutation sequence.
    #[must_use]
    pub fn pending_events(&self) -> &[PlanEvent] {
        &self.events
    }

    /// Removes and returns lifecycle events for durable journal append.
    pub fn drain_events(&mut self) -> Vec<PlanEvent> {
        std::mem::take(&mut self.events)
    }

    /// Replays one previously persisted event without emitting a duplicate
    /// event. This restores the in-memory projection only; it never submits a
    /// broker order or invokes an external provider.
    ///
    /// # Errors
    /// Returns [`PlanError`] when the event is malformed, duplicated, or would
    /// violate the lifecycle state machine.
    pub fn restore_event(&mut self, event: PlanEvent) -> Result<(), PlanError> {
        match event {
            PlanEvent::Submitted(plan) => {
                plan.validate(plan.generated_at)
                    .map_err(PlanError::Invalid)?;
                if self.records.contains_key(&plan.plan_id) {
                    return Err(PlanError::Duplicate);
                }
                self.records.insert(
                    plan.plan_id.clone(),
                    PlanRecord {
                        plan,
                        state: PlanState::Pending,
                    },
                );
                Ok(())
            }
            PlanEvent::Transition { plan_id, state } => {
                let record = self.records.get_mut(&plan_id).ok_or(PlanError::NotFound)?;
                if record.state == state {
                    return Ok(());
                }
                if !legal_transition(record.state, state) {
                    return Err(PlanError::InvalidTransition {
                        from: record.state,
                        to: state,
                    });
                }
                record.state = state;
                Ok(())
            }
        }
    }

    /// Evaluates policy and moves a pending plan to Approved.
    ///
    /// Rejected individual actions remain visible in the returned approval;
    /// the plan lifecycle itself is Approved only when schema/policy evaluation
    /// completes without a fatal error.
    ///
    /// # Errors
    /// Returns [`PlanError`] when the plan is missing, expired, or invalid.
    pub fn approve(
        &mut self,
        plan_id: &str,
        now: MonoTime,
        policy: Policy,
        proposals: &[Proposal],
    ) -> Result<Approval, PlanError> {
        let plan = self
            .records
            .get(plan_id)
            .ok_or(PlanError::NotFound)?
            .plan
            .clone();
        let approval = approve_plan(&plan, now, policy, proposals).map_err(PlanError::Invalid)?;
        self.transition(plan_id, PlanState::Approved, now)?;
        Ok(approval)
    }
}

fn legal_transition(from: PlanState, to: PlanState) -> bool {
    matches!(
        (from, to),
        (
            PlanState::Pending,
            PlanState::Approved | PlanState::Rejected | PlanState::Expired
        ) | (
            PlanState::Approved,
            PlanState::Executing | PlanState::Expired
        ) | (
            PlanState::Executing,
            PlanState::Completed | PlanState::Failed
        )
    )
}

impl Plan {
    /// Validates identity, TTL, action schemas, and duplicate action IDs.
    ///
    /// # Errors
    /// Returns [`LlmError::InvalidAction`] for malformed or expired plans.
    pub fn validate(&self, now: MonoTime) -> Result<(), LlmError> {
        if self.plan_id.trim().is_empty() || self.expires_at < self.generated_at {
            return Err(LlmError::InvalidAction(
                "invalid plan identity or interval".into(),
            ));
        }
        if now >= self.expires_at {
            return Err(LlmError::InvalidAction("plan expired".into()));
        }
        let mut ids = std::collections::BTreeSet::new();
        for action in &self.actions {
            action.validate()?;
            if let Some(id) = &action.proposal_id
                && !ids.insert(id)
            {
                return Err(LlmError::InvalidAction("duplicate proposal action".into()));
            }
        }
        Ok(())
    }
}

/// Action rejected by the operator policy or current proposal state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RejectReason {
    /// Manual mode never executes automatically.
    ManualMode,
    /// Hybrid policy disallows this action.
    Policy,
    /// Referenced proposal is absent or stale.
    ProposalUnavailable,
    /// Requested scale exceeds policy.
    ScaleLimit,
    /// Strategy is outside the allowlist or unhealthy.
    StrategyUnavailable,
    /// Instrument is outside the configured universe.
    InstrumentUnavailable,
    /// Estimated notional exceeds policy.
    NotionalLimit,
}

/// One action ready for the engine after policy checks.
#[derive(Clone, Debug, PartialEq)]
pub struct ApprovedAction {
    /// Original action.
    pub action: AutonomousAction,
    /// Effective scale after policy clamping.
    pub scale: f64,
}

/// Result of policy evaluation, separated for UI display and execution.
pub type Approval = (Vec<ApprovedAction>, Vec<(AutonomousAction, RejectReason)>);

/// Fine-grained permission policy for account/strategy/universe automation.
#[derive(Clone, Debug, PartialEq)]
pub struct PermissionPolicy {
    /// Account identity allowed to use this policy.
    pub account_id: String,
    /// Current operating mode.
    pub mode: Mode,
    /// Whether new/increased exposure may execute automatically.
    pub allow_entries: bool,
    /// Maximum action scale in `(0, 1]`.
    pub max_scale: f64,
    /// Optional strategy allowlist; `None` permits every healthy strategy.
    pub allowed_strategy_ids: Option<std::collections::BTreeSet<String>>,
    /// Optional instrument allowlist; `None` permits every instrument.
    pub allowed_instrument_ids: Option<std::collections::BTreeSet<String>>,
    /// Optional notional cap per proposal in canonical ticks.
    pub max_notional_ticks: Option<u64>,
    /// Optional inclusive automation window.
    pub active_window: Option<(MonoTime, MonoTime)>,
}

/// One immutable object supplied to an autonomy/LLM context packet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextEntry {
    /// Stable authoritative object ID.
    pub object_id: String,
    /// Finite object kind used for audit/display.
    pub kind: String,
    /// Monotonic generation time of the source object.
    pub generated_at: MonoTime,
    /// Monotonic expiry boundary; entries at or after this time are stale.
    pub expires_at: MonoTime,
    /// Serialized authoritative payload.
    pub payload: Vec<u8>,
}

/// Why an input was not included in a context packet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextOmissionReason {
    /// The object ID or kind was blank.
    InvalidIdentity,
    /// The object was already represented by an earlier entry.
    Duplicate,
    /// The object was stale at packet construction time.
    Stale,
    /// One object exceeded the packet byte budget.
    EntryTooLarge,
    /// Adding the object would exceed the aggregate byte budget.
    ByteBudget,
    /// Adding the object would exceed the estimated token budget.
    TokenBudget,
    /// The packet entry-count budget was exhausted.
    EntryBudget,
}

/// Explicit disclosure for data omitted from a context packet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextOmission {
    /// Object ID when it was syntactically available.
    pub object_id: String,
    /// Object kind when it was syntactically available.
    pub kind: String,
    /// Deterministic omission reason.
    pub reason: ContextOmissionReason,
}

/// Bounded, auditable context handed to an intelligence-plane consumer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextPacket {
    /// Injected packet construction time.
    pub as_of: MonoTime,
    /// Included entries in deterministic kind/ID order.
    pub entries: Vec<ContextEntry>,
    /// Explicitly omitted entries and reasons.
    pub omitted: Vec<ContextOmission>,
    /// Total included serialized bytes.
    pub total_bytes: usize,
    /// Conservative estimated token count (`ceil(bytes / 4)`).
    pub estimated_tokens: usize,
}

/// Limits used when constructing a context packet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContextBudget {
    /// Maximum included entries.
    pub max_entries: usize,
    /// Maximum serialized payload bytes.
    pub max_bytes: usize,
    /// Maximum estimated tokens.
    pub max_tokens: usize,
}

impl ContextBudget {
    /// Validates that all packet budgets are non-zero.
    ///
    /// # Errors
    /// Returns [`LlmError::InvalidAction`] when any budget is zero.
    pub fn validate(self) -> Result<(), LlmError> {
        if self.max_entries == 0 || self.max_bytes == 0 || self.max_tokens == 0 {
            return Err(LlmError::InvalidAction(
                "context budget must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

/// Deterministic context-packet assembler.
pub struct ContextPacketBuilder {
    budget: ContextBudget,
}

impl ContextPacketBuilder {
    /// Creates a builder after validating finite packet limits.
    ///
    /// # Errors
    /// Returns [`LlmError::InvalidAction`] when any budget is zero.
    pub fn new(budget: ContextBudget) -> Result<Self, LlmError> {
        budget.validate()?;
        Ok(Self { budget })
    }

    /// Builds a bounded packet, explicitly reporting every omitted object.
    /// Inputs are sorted before budgeting, so equivalent sets produce byte-
    /// identical inclusion and omission decisions.
    ///
    /// # Errors
    /// Returns [`LlmError::InvalidAction`] when an entry has an invalid expiry
    /// interval or the builder budget is invalid.
    pub fn build<I>(&self, as_of: MonoTime, entries: I) -> Result<ContextPacket, LlmError>
    where
        I: IntoIterator<Item = ContextEntry>,
    {
        self.budget.validate()?;
        let mut candidates = entries.into_iter().collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.kind
                .cmp(&right.kind)
                .then(left.object_id.cmp(&right.object_id))
        });
        let mut included = Vec::new();
        let mut omitted = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        let mut total_bytes = 0_usize;
        let mut estimated_tokens = 0_usize;
        for entry in candidates {
            let omission = |reason| ContextOmission {
                object_id: entry.object_id.clone(),
                kind: entry.kind.clone(),
                reason,
            };
            if entry.object_id.trim().is_empty() || entry.kind.trim().is_empty() {
                omitted.push(omission(ContextOmissionReason::InvalidIdentity));
                continue;
            }
            if !seen.insert(entry.object_id.clone()) {
                omitted.push(omission(ContextOmissionReason::Duplicate));
                continue;
            }
            if entry.expires_at <= entry.generated_at || as_of >= entry.expires_at {
                omitted.push(omission(ContextOmissionReason::Stale));
                continue;
            }
            if included.len() >= self.budget.max_entries {
                omitted.push(omission(ContextOmissionReason::EntryBudget));
                continue;
            }
            if entry.payload.len() > self.budget.max_bytes {
                omitted.push(omission(ContextOmissionReason::EntryTooLarge));
                continue;
            }
            let entry_tokens = entry.payload.len().saturating_add(3) / 4;
            if total_bytes.saturating_add(entry.payload.len()) > self.budget.max_bytes {
                omitted.push(omission(ContextOmissionReason::ByteBudget));
                continue;
            }
            if estimated_tokens.saturating_add(entry_tokens) > self.budget.max_tokens {
                omitted.push(omission(ContextOmissionReason::TokenBudget));
                continue;
            }
            total_bytes = total_bytes.saturating_add(entry.payload.len());
            estimated_tokens = estimated_tokens.saturating_add(entry_tokens);
            included.push(entry);
        }
        Ok(ContextPacket {
            as_of,
            entries: included,
            omitted,
            total_bytes,
            estimated_tokens,
        })
    }
}

/// Immutable versions and live facts captured immediately before approval.
#[derive(Clone, Debug, PartialEq)]
pub struct ApprovalContext {
    /// Authenticated account requesting approval.
    pub account_id: String,
    /// Version of the proposal snapshot used by the plan.
    pub expected_proposal_version: u64,
    /// Current proposal snapshot version.
    pub proposal_version: u64,
    /// Version of the risk budget snapshot used by the plan.
    pub expected_risk_version: u64,
    /// Current risk budget snapshot version.
    pub risk_version: u64,
    /// Current proposals, treated as authoritative immutable snapshot data.
    pub proposals: Vec<Proposal>,
    /// Per-proposal estimated notional in canonical ticks.
    pub notionals_ticks: std::collections::BTreeMap<String, u64>,
    /// Strategies currently unhealthy/quarantined.
    pub unhealthy_strategy_ids: std::collections::BTreeSet<String>,
}

/// Approval result with the context binding that must be rechecked at submit.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundApproval {
    /// Policy-approved actions.
    pub approved: Vec<ApprovedAction>,
    /// Actions rejected with operator-visible reasons.
    pub rejected: Vec<(AutonomousAction, RejectReason)>,
    /// Versions that execution must compare again immediately before target creation.
    pub proposal_version: u64,
    /// Risk budget version that execution must compare again.
    pub risk_version: u64,
    /// Digest of the exact proposal snapshot approved by policy.
    pub proposal_digest: [u8; 32],
}

/// Computes a deterministic digest for an authoritative proposal snapshot.
///
/// The digest is intentionally independent of vector ordering: proposal IDs
/// are sorted before hashing. It is used as an optimistic-concurrency guard in
/// addition to the monotonic snapshot version, catching accidental in-place
/// mutation or version-reuse bugs before an order target is created.
#[must_use]
pub fn proposal_snapshot_digest(proposals: &[Proposal]) -> [u8; 32] {
    let mut ordered = proposals.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|proposal| proposal.proposal_id.get());
    let mut lanes = [
        0xcbf2_9ce4_8422_2325_u64,
        0x8422_2325_cbf2_9ce4_u64,
        0x9e37_79b1_85eb_ca87_u64,
        0xd6e8_feb8_6659_fd93_u64,
    ];
    let mut feed = |bytes: &[u8]| {
        for byte in bytes {
            for (index, lane) in lanes.iter_mut().enumerate() {
                *lane ^= u64::from(*byte).wrapping_add((index as u64) << 8);
                *lane = lane.wrapping_mul(0x0100_0000_01b3_u64 ^ ((index as u64) << 32));
            }
        }
    };
    for proposal in ordered {
        feed(&proposal.proposal_id.get().to_le_bytes());
        feed(proposal.strategy_id.as_bytes());
        feed(&proposal.instrument_id.get().to_le_bytes());
        match proposal.action {
            insider_strategy_sdk::Action::NoAction => feed(&[0]),
            insider_strategy_sdk::Action::TargetQuantity { quantity_ticks } => {
                feed(&[1]);
                feed(&quantity_ticks.to_le_bytes());
            }
            insider_strategy_sdk::Action::TargetWeight { weight } => {
                feed(&[2]);
                feed(&weight.to_bits().to_le_bytes());
            }
            insider_strategy_sdk::Action::Increase { quantity_ticks } => {
                feed(&[3]);
                feed(&quantity_ticks.to_le_bytes());
            }
            insider_strategy_sdk::Action::Decrease { quantity_ticks } => {
                feed(&[4]);
                feed(&quantity_ticks.to_le_bytes());
            }
            insider_strategy_sdk::Action::Close => feed(&[5]),
        }
        feed(&proposal.confidence.to_bits().to_le_bytes());
        feed(&proposal.horizon_ns.to_le_bytes());
        feed(&proposal.ttl_ns.to_le_bytes());
        feed(&proposal.generated_mono.as_nanos().to_le_bytes());
        for evidence in &proposal.evidence {
            feed(&(evidence.len() as u64).to_le_bytes());
            feed(evidence.as_bytes());
        }
        feed(&[0xff]);
    }
    let mut digest = [0_u8; 32];
    for (index, lane) in lanes.into_iter().enumerate() {
        digest[index * 8..(index + 1) * 8].copy_from_slice(&lane.to_le_bytes());
    }
    digest
}

/// Approves a plan against account policy and an immutable live context.
///
/// This is deliberately separate from order submission. The returned versions
/// are optimistic-concurrency bindings; callers must re-read and compare them
/// immediately before creating targets or sending orders.
///
/// # Errors
/// Returns [`LlmError::InvalidAction`] when policy metadata or critical context
/// versions are invalid or changed.
pub fn approve_plan_bound(
    plan: &Plan,
    now: MonoTime,
    policy: &PermissionPolicy,
    context: &ApprovalContext,
) -> Result<BoundApproval, LlmError> {
    plan.validate(now)?;
    if policy.account_id.trim().is_empty()
        || context.account_id != policy.account_id
        || !policy.max_scale.is_finite()
        || policy.max_scale <= 0.0
        || policy.max_scale > 1.0
    {
        return Err(LlmError::InvalidAction("invalid account policy".into()));
    }
    if context.expected_proposal_version != context.proposal_version
        || context.expected_risk_version != context.risk_version
    {
        return Err(LlmError::InvalidAction("critical context changed".into()));
    }
    if let Some((start, end)) = policy.active_window
        && (now < start || now > end)
    {
        return Err(LlmError::InvalidAction(
            "outside autonomy policy window".into(),
        ));
    }
    let (approved, rejected) = approve_plan(
        plan,
        now,
        Policy {
            mode: policy.mode,
            allow_entries: policy.allow_entries,
            max_scale: policy.max_scale,
        },
        &context.proposals,
    )?;
    let mut final_approved = Vec::new();
    let mut final_rejected = rejected;
    for action in approved {
        let proposal = action.action.proposal_id.as_deref().and_then(|id| {
            context
                .proposals
                .iter()
                .find(|proposal| proposal.proposal_id.to_string() == id)
        });
        let Some(proposal) = proposal else {
            final_rejected.push((action.action, RejectReason::ProposalUnavailable));
            continue;
        };
        if policy
            .allowed_strategy_ids
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&proposal.strategy_id))
            || context
                .unhealthy_strategy_ids
                .contains(&proposal.strategy_id)
        {
            final_rejected.push((action.action, RejectReason::StrategyUnavailable));
            continue;
        }
        let instrument_id = proposal.instrument_id.to_string();
        if policy
            .allowed_instrument_ids
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&instrument_id))
        {
            final_rejected.push((action.action, RejectReason::InstrumentUnavailable));
            continue;
        }
        if let Some(max_notional) = policy.max_notional_ticks
            && context
                .notionals_ticks
                .get(&proposal.proposal_id.to_string())
                .is_some_and(|notional| *notional > max_notional)
        {
            final_rejected.push((action.action, RejectReason::NotionalLimit));
            continue;
        }
        final_approved.push(action);
    }
    Ok(BoundApproval {
        approved: final_approved,
        rejected: final_rejected,
        proposal_version: context.proposal_version,
        risk_version: context.risk_version,
        proposal_digest: proposal_snapshot_digest(&context.proposals),
    })
}

/// Revalidates an approved plan immediately before target creation.
///
/// No broker or portfolio mutation belongs before this check. It verifies TTL,
/// proposal/risk snapshot versions, and that every executable proposal still
/// exists and remains valid at the injected time.
///
/// # Errors
/// Returns [`LlmError::InvalidAction`] when any critical context changed or a
/// referenced proposal disappeared/expired.
pub fn revalidate_before_execution(
    approval: &BoundApproval,
    plan: &Plan,
    now: MonoTime,
    proposal_version: u64,
    risk_version: u64,
    proposals: &[Proposal],
) -> Result<(), LlmError> {
    plan.validate(now)?;
    if approval.proposal_version != proposal_version || approval.risk_version != risk_version {
        return Err(LlmError::InvalidAction(
            "critical context changed before execution".into(),
        ));
    }
    if approval.proposal_digest != proposal_snapshot_digest(proposals) {
        return Err(LlmError::InvalidAction(
            "proposal snapshot contents changed before execution".into(),
        ));
    }
    for action in &approval.approved {
        let Some(proposal_id) = action.action.proposal_id.as_deref() else {
            continue;
        };
        let Some(proposal) = proposals
            .iter()
            .find(|proposal| proposal.proposal_id.to_string() == proposal_id)
        else {
            return Err(LlmError::InvalidAction(
                "approved proposal is no longer available".into(),
            ));
        };
        proposal.validate(now).map_err(|error| {
            LlmError::InvalidAction(format!("proposal revalidation failed: {error:?}"))
        })?;
    }
    Ok(())
}

/// Validates a plan against current proposals without submitting broker orders.
/// The caller passes approved actions to the normal engine path.
///
/// # Errors
/// Returns [`LlmError`] if the plan schema or TTL is invalid.
pub fn approve_plan(
    plan: &Plan,
    now: MonoTime,
    policy: Policy,
    proposals: &[Proposal],
) -> Result<Approval, LlmError> {
    plan.validate(now)?;
    if !policy.max_scale.is_finite() || policy.max_scale <= 0.0 || policy.max_scale > 1.0 {
        return Err(LlmError::InvalidAction(
            "policy max_scale must be finite and in (0,1]".into(),
        ));
    }
    let mut approved = Vec::new();
    let mut rejected = Vec::new();
    for action in &plan.actions {
        if policy.mode == Mode::Manual {
            rejected.push((action.clone(), RejectReason::ManualMode));
            continue;
        }
        let proposal = action.proposal_id.as_deref().and_then(|id| {
            proposals
                .iter()
                .find(|proposal| proposal.proposal_id.to_string() == id)
        });
        if matches!(
            action.action_type,
            ActionType::ExecuteProposal | ActionType::ExecuteProposalScaled
        ) && proposal.is_none()
        {
            rejected.push((action.clone(), RejectReason::ProposalUnavailable));
            continue;
        }
        let scale = action.scale.unwrap_or(1.0);
        if !scale.is_finite() || scale <= 0.0 || scale > policy.max_scale {
            rejected.push((action.clone(), RejectReason::ScaleLimit));
            continue;
        }
        let is_entry = proposal.is_some_and(|proposal| match proposal.action {
            insider_strategy_sdk::Action::Increase { quantity_ticks }
            | insider_strategy_sdk::Action::TargetQuantity { quantity_ticks } => quantity_ticks > 0,
            _ => false,
        });
        if policy.mode == Mode::Hybrid && !policy.allow_entries && is_entry {
            rejected.push((action.clone(), RejectReason::Policy));
            continue;
        }
        approved.push(ApprovedAction {
            action: action.clone(),
            scale,
        });
    }
    Ok((approved, rejected))
}

#[cfg(test)]
mod tests {
    use super::{
        ApprovalContext, ContextBudget, ContextEntry, ContextOmissionReason, ContextPacketBuilder,
        LiveGuard, LiveGuardError, LiveLimits, Mode, PermissionPolicy, Plan, PlanState, PlanStore,
        Policy, SUBSYSTEM_ID, TradingEnvironment, approve_plan, approve_plan_bound,
        decode_plan_event, encode_plan_event, proposal_snapshot_digest,
    };
    use insider_common_types::InstrumentId;
    use insider_common_types::{MonoTime, ProposalId};
    use insider_llm_core::{ActionType, AutonomousAction};
    use insider_strategy_sdk::{Action, Proposal};

    #[test]
    fn subsystem_id_is_non_empty_and_ascii() {
        assert!(!SUBSYSTEM_ID.is_empty());
        assert!(SUBSYSTEM_ID.is_ascii());
    }

    #[test]
    fn live_guard_requires_two_phrases_and_enforces_cap() {
        let mut accounts = std::collections::BTreeSet::new();
        accounts.insert("account-1".to_owned());
        let mut guard = LiveGuard::paper(LiveLimits {
            allowed_accounts: accounts,
            max_notional_ticks: 1_000,
        });
        let now = MonoTime::from_nanos(10);
        assert_eq!(guard.environment(), TradingEnvironment::Paper);
        assert_eq!(
            guard.arm_live("account-1", now, "wrong"),
            Err(LiveGuardError::ConfirmationRequired)
        );
        let token = guard
            .arm_live("account-1", now, "ARM LIVE")
            .ok()
            .unwrap_or_default();
        assert!(
            guard
                .confirm_live("account-1", &token, now, "ENABLE LIVE")
                .is_ok()
        );
        assert_eq!(guard.environment(), TradingEnvironment::Live);
        assert_eq!(
            guard.authorize("account-1", Some(1_001)),
            Err(LiveGuardError::NotionalLimit)
        );
        assert_eq!(
            guard.authorize("account-1", None),
            Err(LiveGuardError::NotionalUnknown)
        );
        assert!(guard.authorize("account-1", Some(1_000)).is_ok());
    }

    #[test]
    fn kill_switch_blocks_until_explicit_reenable() {
        let mut accounts = std::collections::BTreeSet::new();
        accounts.insert("account-1".to_owned());
        let mut guard = LiveGuard::paper(LiveLimits {
            allowed_accounts: accounts,
            max_notional_ticks: 100,
        });
        let now = MonoTime::from_nanos(10);
        let token = guard
            .arm_live("account-1", now, "ARM LIVE")
            .ok()
            .unwrap_or_default();
        assert!(
            guard
                .confirm_live("account-1", &token, now, "ENABLE LIVE")
                .is_ok()
        );
        guard.kill_switch();
        assert_eq!(
            guard.authorize("account-1", Some(1)),
            Err(LiveGuardError::NotLive)
        );
        assert_eq!(guard.environment(), TradingEnvironment::Killed);
    }

    #[test]
    fn manual_mode_never_approves_and_expired_plan_is_rejected() {
        let action = AutonomousAction {
            action_type: ActionType::ExecuteProposal,
            proposal_id: Some("proposal_00000000000000000000000000000001".into()),
            scale: None,
            reason_codes: vec!["test".into()],
        };
        let plan = Plan {
            plan_id: "plan-1".into(),
            generated_at: MonoTime::from_nanos(1),
            expires_at: MonoTime::from_nanos(10),
            actions: vec![action.clone()],
        };
        let proposal = Proposal {
            proposal_id: ProposalId::new(1)
                .ok()
                .unwrap_or_else(|| std::process::abort()),
            strategy_id: "s".into(),
            instrument_id: InstrumentId::new(1)
                .ok()
                .unwrap_or_else(|| std::process::abort()),
            action: Action::Close,
            confidence: 1.0,
            horizon_ns: 10,
            ttl_ns: 10,
            evidence: Vec::new(),
            generated_mono: MonoTime::from_nanos(1),
        };
        let result = approve_plan(
            &plan,
            MonoTime::from_nanos(2),
            Policy {
                mode: Mode::Manual,
                allow_entries: false,
                max_scale: 1.0,
            },
            &[proposal],
        );
        assert!(
            result.is_ok_and(|(approved, rejected)| approved.is_empty() && rejected.len() == 1)
        );
        assert!(
            approve_plan(
                &plan,
                MonoTime::from_nanos(11),
                Policy {
                    mode: Mode::Autonomous,
                    allow_entries: true,
                    max_scale: 1.0
                },
                &[]
            )
            .is_err()
        );
    }

    #[test]
    fn plan_store_enforces_idempotent_lifecycle_and_expiry() {
        let plan = Plan {
            plan_id: "plan-lifecycle".into(),
            generated_at: MonoTime::from_nanos(1),
            expires_at: MonoTime::from_nanos(100),
            actions: vec![AutonomousAction {
                action_type: ActionType::NoAction,
                proposal_id: None,
                scale: None,
                reason_codes: vec!["no-signal".into()],
            }],
        };
        let mut store = PlanStore::new();
        assert!(store.submit(plan, MonoTime::from_nanos(2)).is_ok());
        assert!(
            store
                .transition(
                    "plan-lifecycle",
                    PlanState::Approved,
                    MonoTime::from_nanos(3)
                )
                .is_ok()
        );
        assert!(
            store
                .transition(
                    "plan-lifecycle",
                    PlanState::Approved,
                    MonoTime::from_nanos(3)
                )
                .is_ok()
        );
        assert!(
            store
                .transition(
                    "plan-lifecycle",
                    PlanState::Executing,
                    MonoTime::from_nanos(4)
                )
                .is_ok()
        );
        assert!(
            store
                .transition(
                    "plan-lifecycle",
                    PlanState::Completed,
                    MonoTime::from_nanos(5)
                )
                .is_ok()
        );
        assert!(
            store
                .transition(
                    "plan-lifecycle",
                    PlanState::Executing,
                    MonoTime::from_nanos(6)
                )
                .is_err()
        );

        let expired = Plan {
            plan_id: "plan-expired".into(),
            generated_at: MonoTime::from_nanos(1),
            expires_at: MonoTime::from_nanos(2),
            actions: Vec::new(),
        };
        assert!(store.submit(expired, MonoTime::from_nanos(1)).is_ok());
        assert!(
            store
                .transition("plan-expired", PlanState::Approved, MonoTime::from_nanos(3))
                .is_err()
        );
        assert_eq!(
            store.get("plan-expired").map(|record| record.state),
            Some(PlanState::Expired)
        );
        let events = store.drain_events();
        assert!(!events.is_empty());
        let mut restored = PlanStore::new();
        for event in events {
            let encoded = encode_plan_event(&event);
            let Ok(decoded) = decode_plan_event(&encoded) else {
                return;
            };
            assert!(restored.restore_event(decoded).is_ok());
        }
        assert_eq!(
            restored.get("plan-lifecycle").map(|record| record.state),
            Some(PlanState::Completed)
        );
        assert_eq!(
            restored.get("plan-expired").map(|record| record.state),
            Some(PlanState::Expired)
        );
    }

    #[test]
    fn bound_approval_rejects_context_changes_and_policy_out_of_universe() {
        let proposal_id = ProposalId::new(7)
            .ok()
            .unwrap_or_else(|| std::process::abort());
        let instrument_id = InstrumentId::new(8)
            .ok()
            .unwrap_or_else(|| std::process::abort());
        let proposal = Proposal {
            proposal_id,
            strategy_id: "strategy-a".into(),
            instrument_id,
            action: Action::Increase { quantity_ticks: 2 },
            confidence: 1.0,
            horizon_ns: 100,
            ttl_ns: 100,
            evidence: Vec::new(),
            generated_mono: MonoTime::from_nanos(1),
        };
        let plan = Plan {
            plan_id: "bound-plan".into(),
            generated_at: MonoTime::from_nanos(1),
            expires_at: MonoTime::from_nanos(100),
            actions: vec![AutonomousAction {
                action_type: ActionType::ExecuteProposal,
                proposal_id: Some(proposal_id.to_string()),
                scale: None,
                reason_codes: vec!["signal".into()],
            }],
        };
        let policy = PermissionPolicy {
            account_id: "account-a".into(),
            mode: Mode::Autonomous,
            allow_entries: true,
            max_scale: 1.0,
            allowed_strategy_ids: Some(std::collections::BTreeSet::from(["other".into()])),
            allowed_instrument_ids: None,
            max_notional_ticks: Some(10),
            active_window: None,
        };
        let context = ApprovalContext {
            account_id: "account-a".into(),
            expected_proposal_version: 2,
            proposal_version: 3,
            expected_risk_version: 1,
            risk_version: 1,
            proposals: vec![proposal],
            notionals_ticks: std::collections::BTreeMap::new(),
            unhealthy_strategy_ids: std::collections::BTreeSet::new(),
        };
        assert!(approve_plan_bound(&plan, MonoTime::from_nanos(2), &policy, &context).is_err());
        let mut stable_context = context;
        stable_context.expected_proposal_version = 3;
        let approval =
            approve_plan_bound(&plan, MonoTime::from_nanos(2), &policy, &stable_context).ok();
        assert!(approval.is_some_and(|approval| {
            approval.approved.is_empty() && approval.rejected.len() == 1
        }));
    }

    #[test]
    fn context_packets_are_sorted_bounded_and_disclose_omissions() {
        let builder = ContextPacketBuilder::new(ContextBudget {
            max_entries: 1,
            max_bytes: 4,
            max_tokens: 1,
        })
        .ok();
        let Some(builder) = builder else { return };
        let packet = builder
            .build(
                MonoTime::from_nanos(5),
                [
                    ContextEntry {
                        object_id: "z".into(),
                        kind: "quote".into(),
                        generated_at: MonoTime::from_nanos(1),
                        expires_at: MonoTime::from_nanos(10),
                        payload: vec![1, 2, 3, 4],
                    },
                    ContextEntry {
                        object_id: "a".into(),
                        kind: "quote".into(),
                        generated_at: MonoTime::from_nanos(1),
                        expires_at: MonoTime::from_nanos(4),
                        payload: vec![1],
                    },
                ],
            )
            .ok();
        let Some(packet) = packet else { return };
        assert_eq!(packet.entries.len(), 1);
        assert_eq!(packet.entries[0].object_id, "z");
        assert!(
            packet
                .omitted
                .iter()
                .any(|item| item.reason == ContextOmissionReason::Stale)
        );
    }

    #[test]
    fn proposal_digest_changes_when_authoritative_contents_change() {
        let proposal = Proposal {
            proposal_id: ProposalId::new(41)
                .ok()
                .unwrap_or_else(|| std::process::abort()),
            strategy_id: "digest-test".into(),
            instrument_id: InstrumentId::new(42)
                .ok()
                .unwrap_or_else(|| std::process::abort()),
            action: Action::Close,
            confidence: 0.5,
            horizon_ns: 100,
            ttl_ns: 10,
            evidence: vec!["evidence:v1".into()],
            generated_mono: MonoTime::from_nanos(1),
        };
        let original = proposal_snapshot_digest(std::slice::from_ref(&proposal));
        let mut changed = proposal;
        changed.confidence = 0.6;
        assert_ne!(
            original,
            proposal_snapshot_digest(std::slice::from_ref(&changed))
        );
    }
}
