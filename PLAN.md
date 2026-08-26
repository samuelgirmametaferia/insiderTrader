# InsiderTrader Production Implementation Plan

> Status: normative execution plan.
> Authority: this file operationalizes `AGENTS.md`. If they conflict, `AGENTS.md`
> defines architecture and safety; this file defines delivery order and proof.
> Initial release: Linux desktop, local service topology, Interactive Brokers,
> research + paper + live trading, React/Tauri UI, multi-asset domain model.

## 1. How this plan is used

This is not a feature wish list. It is a sequence of production gates. Work may be
developed in parallel only after its prerequisite gate contracts are merged. The
only Markdown checkboxes in this file are gate completion records. A box means the
capability is deployable, observable, recoverable, documented, and proven by the
evidence listed under that gate.

### 1.1 Rule for checking a gate

A gate may change from `[ ]` to `[x]` only in the same reviewed change that adds
`evidence/gates/GNN.yaml`. That evidence file MUST contain:

```yaml
gate: GNN
status: passed
source_revision: "<40-character commit id>"
completed_at: "<RFC3339 UTC>"
schema_bundle_sha256: "<sha256>"
release_artifacts:
  - path: "dist/<artifact>"
    sha256: "<sha256>"
verification:
  unit: { run_url: "...", passed: 0, failed: 0 }
  integration: { run_url: "...", passed: 0, failed: 0 }
  replay: { run_url: "...", final_state_sha256: "..." }
  performance: { run_url: "...", baseline_id: "...", regressions: [] }
  fault_injection: { run_url: "...", passed: 0, failed: 0 }
security_findings:
  critical_open: 0
  high_open: 0
approvals:
  - role: "engineering-owner"
    identity: "..."
    approved_at: "<RFC3339 UTC>"
known_limitations: []
```

`scripts/verify_gate.sh GNN` MUST fail if an evidence field is absent, a referenced
artifact/hash differs, a required CI job did not pass for the same revision, or a
critical/high defect is open. A regression invalidates the gate: remove the stale
evidence record or set `status: invalidated`, uncheck the box, and block releases
until recertification. Test-only adapters, skipped tests, manual database edits,
and undocumented operator intervention cannot satisfy a gate.

### 1.2 Fixed release profile and measurable budgets

All performance evidence names the machine profile. The initial reference profile
is x86-64 Linux, 8 physical cores, 32 GiB RAM, NVMe storage, and a 1920x1080 display.
CI may use slower hardware but must normalize against a stored reference baseline.

- Engine idle RSS: <= 750 MiB excluding configured data caches and model workers.
- UI idle RSS: <= 600 MiB with the Trading workspace open.
- Journal append: p99 <= 2 ms for 1 KiB events with durability policy enabled.
- In-process event handoff: p99 <= 250 us at 50,000 events/second.
- Quote-to-deterministic-strategy-proposal: p99 <= 25 ms for `FAST` workloads.
- Risk decision: p99 <= 5 ms for a single target and <= 25 ms for 500 targets.
- UI chart update: >= 55 FPS while ingesting 10,000 updates/second in batches.
- UI input response: p95 <= 100 ms; no task > 50 ms on the browser main thread.
- Recovery: engine authoritative state restored and reconciliation started within
  30 seconds after an unclean restart on the reference dataset.
- No unbounded queue is permitted. Every queue declares capacity, overflow policy,
  producer/consumer ownership, depth telemetry, and saturation alert.

Budgets may be tightened through an ADR. They may only be relaxed with a measured
capacity report, risk review, and corresponding `AGENTS.md` update.

## 2. Fixed technology and contract decisions

These are implementation decisions, not work left to the implementer:

1. Rust stable pinned by `rust-toolchain.toml`; Tokio for async control-plane work.
2. Python version pinned in `.python-version`; `uv` owns Python dependency locking.
3. React + TypeScript + Vite inside Tauri 2; Dockview for layouts; Lightweight
   Charts for financial charts; Zustand for low-rate UI state. Tick batches bypass
   Zustand and enter chart adapters directly.
4. Protobuf is the versioned service/event schema source where binary transport is
   needed; JSON Schema is generated for manifests/config/LLM structured output.
   `rust_decimal::Decimal` is used for money/quantity. Floating point is allowed
   for scores and statistical estimates, never authoritative money accounting.
5. SQLite in WAL mode is the initial local control/read-model database. The journal
   is a separate checksummed append-only segment store. Parquet + Arrow store
   historical columnar data. Database traits prevent domain code from depending on
   SQLite, enabling future PostgreSQL/object-store deployments.
6. Secrets use the OS keyring; configuration contains secret references only.
7. One `engine` executable supervises local components initially. Component traits,
   Protobuf contracts, and journal events are process-safe boundaries so market,
   research, and execution services can later be extracted without changing domain
   semantics. The Tauri UI is always a client and never authoritative.
8. Interactive Brokers is the first certified broker. The adapter wraps the IBKR
   API behind `BrokerGateway`; no other crate imports IBKR-specific types.
9. Multi-asset means the canonical model supports stocks/ETFs, options, futures,
   FX, and crypto. A product is live-enabled only when the IBKR capability matrix,
   account permissions, market-data subscriptions, order types, calendars, and
   reconciliation cases for that product are certified. Unsupported combinations
   return `UnsupportedCapability`; they are never approximated silently.

## 3. Cross-cutting public contracts

Create schemas before implementing consumers. Each schema has a numeric version,
unknown-field policy, compatibility fixture, and migration owner.

- `schemas/proto/identity.proto`: `InstrumentId`, `VenueId`, `AccountId`, `TraceId`,
  `EventId`, `ArtifactId`, `ConfigVersion`, `StrategyVersionId`.
- `schemas/proto/market.proto`: instrument definitions, sessions, corporate actions,
  quote/trade/book/bar events, source/knowledge/exchange timestamps, quality flags.
- `schemas/proto/decision.proto`: feature snapshots, `MetricOutput`, strategy input
  references, `StrategyProposal`, conflicts, virtual-book allocations, targets.
- `schemas/proto/risk.proto`: limits, utilization, `RiskDecision`, reason codes,
  state transitions and override authorization.
- `schemas/proto/execution.proto`: order intent, plan, child order, broker event,
  fill, reconciliation finding and TCA measurement.
- `schemas/proto/intelligence.proto`: news item/cluster, graph references, LLM trace,
  tool call and `AutonomousPlan`.
- `schemas/proto/ipc.proto`: snapshot queries, subscriptions, command envelope,
  optimistic concurrency token and typed error response.
- `schemas/json/{metric,strategy,provider,prompt,workspace}.schema.json`.

Every command carries `command_id`, `trace_id`, `actor`, `issued_at`, expected state
version, and idempotency key. Every event carries `event_id`, aggregate identity,
aggregate sequence, monotonic and wall time, schema version, causation ID, trace ID,
and payload hash. Commands are rejected on stale expected versions where concurrent
mutation could be unsafe.

## 4. Delivery gates

### [ ] G00 — Reproducible repository and CI foundation

**Prerequisites:** none.

**Create:** root `Cargo.toml`, `rust-toolchain.toml`, `deny.toml`, `pyproject.toml`,
`uv.lock`, `.python-version`, `ui/package.json`, `ui/src-tauri`, `schemas/`,
`scripts/`, `tests/fixtures/`, `evidence/`, `.github/workflows/` (or equivalent CI),
and every top-level directory required by `AGENTS.md`.

**Implementation:**

1. Create empty Rust crates named in the repository layout with workspace lint and
   dependency policy inherited from the root. Unsafe Rust is denied by default and
   exceptions require a crate-local safety document and tests.
2. Add schema lint/generation commands. Generated Rust/Python/TypeScript files are
   reproducible; CI regenerates them and fails on a diff.
3. Add `scripts/check.sh` running format checks, Clippy with warnings denied, Rust
   tests, Python lint/type/test, TypeScript lint/type/test, schema compatibility,
   dependency audit, license checks, secret scan, and documentation link checks.
4. Add deterministic fixture conventions: raw provider payload, normalized expected
   output, fixture metadata with capture time/license/sanitization/schema version.
5. Build a minimal signed Linux Tauri artifact and a headless engine artifact.
6. Add ADR template, threat-model template, incident template, release evidence
   schema, and `scripts/verify_gate.sh`.

**Objective proof:** clean checkout builds offline after dependency cache hydration;
two consecutive builds have identical schema/generated-file hashes; CI intentionally
fails for schema drift, a committed fake secret, a forbidden dependency, and a stale
gate evidence fixture. `cargo test --workspace`, the repository's Python `unittest`
suite, and
`npm --prefix ui test` all execute real smoke tests rather than zero-test success.

### [ ] G01 — Deterministic runtime foundation

**Prerequisites:** G00.

**Create:** `common-types`, `clock`, `cfg-core`, `journal`, `event-bus`, `ipc`,
`scheduler`, `supervisor`, `telemetry`, and `engine` crates.

**Implementation:**

1. Implement `Clock` with `SystemClock` and manually advanced `SimClock`. Domain
   code cannot call system time directly; a Clippy/import check enforces this.
2. Implement sortable IDs, checked timestamp arithmetic, UTC wall time plus monotonic
   deadlines, and deterministic seeded randomness passed through context.
3. Implement journal segments with header/version, length-delimited records, CRC32C
   per record, SHA-256 sealed segment hash, fsync policy, atomic segment rotation,
   sparse index, replay cursor, retention and read-only integrity scanner.
4. Implement aggregate sequence/idempotency checks. Duplicate event IDs with the
   same hash are ignored; same ID/different hash is corruption and halts mutation.
5. Implement CFG parse/validate/diff and transactional reload. Components prepare
   changes before a single version becomes visible; failed preparation aborts all.
6. Implement bounded typed bus channels and a scheduler using monotonic deadlines,
   priority classes, concurrency quotas, cancellation, backpressure and late-output
   disposition.
7. Implement the supervision tree, exponential backoff with deterministic bounded
   jitter, restart windows, quarantine, dependency-health gating, stable operational
   snapshots, and graceful drain/shutdown. A component in backoff cannot restart
   while any named dependency is unknown, degraded, or unavailable.
   `ServiceHost` owns the supervisor registry for market-data, news, metrics,
   strategies, LLM, autonomy, portfolio, risk, execution, journal, UI bridge, and
   telemetry components; authenticated runtime-status IPC returns a bounded,
   stable snapshot for System Health.
   Supervisor snapshot reads aggregate metric/strategy host quarantine counts:
   no registered workers is `Unknown`, partial quarantine is `Degraded`, total
   quarantine is `Unavailable`, and healthy registered workers report `Healthy`.
   It also aggregates registered market-feed quality: any stale/degraded stream
   makes the component `Degraded`, all unhealthy streams make it `Unavailable`,
   and no registered instruments remains `Unknown`.
   The broker-neutral gateway exposes session health without adapter-specific types;
   Paper is `Healthy`, IBKR maps authenticated/ready, degraded-reconciliation, and
   disconnected states explicitly, and the execution supervisor consumes that value.
8. Export OpenTelemetry traces/metrics and structured JSON logs. Redact fields marked
   secret and propagate TraceId/causation through queues and IPC.

**Objective proof:** property tests cover journal prefix recovery and timestamp/ID
round trips; fault tests kill the process during append/rotation/reload; golden
replay returns the same ordered event hashes on 100 runs; queue saturation follows
the declared policy; scheduler deadline tests run entirely on `SimClock`; benchmarks
meet section 1.2 with telemetry enabled.

### [ ] G02 — Canonical instruments and market data

**Prerequisites:** G01.

**Create:** `market-types`, `instrument-master`, `market-data`, providers under
`providers/market/{ibkr,files,yahoo}` and normalized Parquet layouts.

**Implementation:**

1. Model asset class, venue, listing, currency, contract multiplier, price/quantity
   increments, trading calendar, settlement, expiry, strike, option right, future
   month, continuous-contract mapping and provider identifiers separately.
2. Implement instrument resolution as an explicit result: exact, ambiguous, absent,
   stale, or unsupported. Never key authoritative state by display ticker alone.
   The desktop watchlist resolves every newly entered symbol across all six
   catalog-certified asset classes through the
   authenticated catalog command before persistence; unresolved, ambiguous,
   stale, or unsupported symbols are rejected with an operator-visible error.
3. Normalize quotes, trades, L1/L2 books and bars while retaining provider payload
   identity, exchange/source/receive/knowledge timestamps, sequence, correction,
   condition codes and quality flags.
4. Implement gap detection, bounded reorder windows, duplicate suppression, reconnect,
   snapshot+delta recovery, rate limits, subscription accounting and stale alarms.
5. Implement event-time bar aggregation with correction events; never overwrite an
   historical observation that was previously visible to a strategy.
6. Implement corporate-action/version history and point-in-time adjusted/unadjusted
   queries. Implement explicit FX conversion provenance.
7. Implement IBKR contract-detail and data adapters; Yahoo remains non-authoritative
   research/UI fallback and cannot feed live risk without explicit policy.
   Yahoo quote polling supports a bounded `IT_YAHOO_SYMBOLS` set of
   `SYMBOL=INSTRUMENT_ID` subscriptions (maximum 128 independently named workers)
   in addition to the single-instrument CLI mode. Startup registers those
   provider-qualified identities in the catalog before workers begin, every
   result enters the same canonical market hub, and malformed subscriptions are
   ignored safely.

**Objective proof:** fixture contracts cover at least one stock, ETF, call, put,
future, spot FX pair and supported crypto contract; DST/session/half-day/expiry and
corporate-action fixtures pass; forced disconnect and sequence-gap tests recover
without silent loss; 24-hour recorded-feed soak has zero unbounded growth; canonical
round-trip and bar aggregation property tests pass; data-quality dashboard reports
freshness, gaps, corrections and provider attribution.

### [ ] G03 — Feature, metric and model runtime

**Prerequisites:** G02.

**Create:** `feature-core`, `metric-sdk`, `metric-host`, `model-runtime`, `ensemble`,
`model-registry`, Rust/Python SDKs, example metrics and immutable artifact storage.

**Implementation:**

1. Validate manifests against JSON Schema and resolve declared streams/features.
   Undeclared input access is impossible because the host builds a capability-limited
   input view from the manifest.
2. Define warm-up, missing value, market correction, reset, TTL, confidence and
   uncertainty semantics. Outputs reference the exact feature snapshot and config.
3. Implement incremental state checkpoint/restore and batch reference functions.
   Require declared numeric tolerances between incremental and batch results.
4. Schedule by priority/deadline/budget; reject stale results from current snapshots
   while retaining them in diagnostics.
5. Run Python metrics out of process with framed IPC, time/memory quotas, restricted
   working directory, no broker secrets and network disabled unless declared.
6. Registry states are Research/Validated/Shadow/Canary/Production/Retired; immutable
   artifact hashes include code, features, parameters, training data and calibration.
7. Ship reference SMA, EWMA volatility, spread/liquidity and book-imbalance metrics
   in both batch tests and live incremental form.

**Objective proof:** malformed/undeclared/cyclic feature manifests fail with stable
error codes; incremental-versus-batch fixtures pass; checkpoint restore produces the
same subsequent output hashes; worker crash/timeout/OOM quarantines only that metric;
late results never reach the valid snapshot; benchmark includes 1,000 instruments,
configured telemetry and journal writes and meets declared per-metric budgets.

### [ ] G04 — Strategy runtime and coordinator

**Prerequisites:** G03.

**Create:** `strategy-sdk`, `strategy-host`, `strategy-coordinator`, `strategies/`,
strategy registry, virtual books and proposal read models.

**Implementation:**

1. Discover versioned packages without hard-coded registration. Validate universe,
   inputs, schedule, determinism, risk request, output kind and artifact hash.
2. Build one DAG over metric/context/strategy dependencies, reject cycles with the
   cycle path, and topologically schedule against immutable input snapshots.
3. Emit exactly one evaluation result per trigger: proposals or explicit `NoAction`
   with reason. Validate finite values, ranges, increments, horizon, TTL, evidence,
   trace, strategy version and supported instrument/action.
4. Store proposals immutably; transition live/expired/superseded/rejected states via
   events. TTL uses injected clock and has identical live/replay boundary behavior.
5. Implement isolated virtual books, weighted net, priority and optimizer handoff.
   Record every conflict, policy input, marginal allocation and attribution mapping.
6. Implement lifecycle and health state machines, risk-budget reservation and manual,
   hybrid, deterministic-auto eligibility without changing proposal representation.
7. Ship one deterministic cross-asset-safe example strategy that produces NoAction
   until liquidity/volatility/momentum evidence is fresh, then emits bounded targets.

**Objective proof:** discovery, invalid declaration, DAG cycle, TTL boundary, restart,
conflict and attribution tests pass; the same event tape yields byte-identical
proposal/state hashes; fuzzing never admits NaN/infinite/out-of-range proposals;
manual and autonomous test consumers deserialize the same proposal bytes; one worker
crash cannot stop unrelated strategies.

### [ ] G05 — Replay, simulator and research validation

**Prerequisites:** G04.

**Create:** `replay`, `exchange-sim`, `experiment-registry`, research CLI/notebooks,
dataset catalogue, backtest reports and comparison APIs.

**Implementation:**

1. Replay journal/Parquet streams by knowledge time through the same service traits
   as live operation. Record seeds and reject accidental system-clock/network use.
2. Preserve revisions instead of overwriting. News enters only at `received_at`;
   corporate-action knowledge and data corrections obey recorded availability.
3. Simulate venue calendars, latency, spread, fees, partial fills, rejects, cancels,
   replace races, halts, borrow, financing, option exercise/assignment, future expiry,
   FX conversion and supported crypto session behavior.
4. Persist every proposal including ignored/netted/rejected ones and calculate signal
   decay, realized return, turnover, capacity, conflicts, portfolio contribution,
   drawdown and net-of-cost performance.
5. Implement chronological walk-forward, purging/embargo, CPCV where applicable,
   final holdout locking, neighboring-parameter tests, cost/latency stress, Deflated
   Sharpe and PBO. Record every trial, including failures.
6. Produce an immutable experiment bundle with data/code/config/schema/model/prompt
   hashes, environment, commands, seeds, outputs and report.
   Persist a bounded `ExperimentProvenance` record on every run containing strategy
   ID/version, news dataset and clustering version, graph snapshot version, LLM
   provider/model, prompt/tool-schema versions, consumed LLM cache IDs, and autonomy
   configuration hash. Journal writes use `IT_EXPERIMENT_RUN_V2`; replay accepts V1
   records with empty provenance. Backtest creation populates strategy lineage and
   graph projection links the experiment to its strategy node. Authenticated IPC
   exposes a separate provenance-bearing create operation while retaining the V1
   create payload for older clients; its codec rejects invalid presence flags,
   unsorted/oversized cache IDs, and malformed UTF-8 before mutation.

   Runtime requirement: `ServiceHost::run_backtest` and
   `ServiceHost::run_strategy_backtest` MUST publish the deterministic report
   bundle before transitioning `backtest:<run_id>` to `Succeeded`. Publication
   MUST be content-addressed, verified after an existing-object retry, and
   journal the resulting `experiment_bundle` artifact path/hash. A successful
   backtest without that artifact is invalid research state.

**Objective proof:** 100 identical golden runs have the same final state hash; leak
sentinel fixtures fail if future data is exposed; simulator accounting balances after
every lifecycle case; research reports can be regenerated from their bundle; a fake
overfit strategy is rejected by the statistical-validation test; live and replay
strategy outputs match on the shared recorded tape; V1 and V2 experiment journal
fixtures both restore, malformed/oversized provenance is rejected, and a restored
backtest exposes its strategy lineage through the graph query API.

### [ ] G06 — Portfolio accounting and risk

**Prerequisites:** G05.

**Create:** `portfolio`, `risk-engine`, double-entry ledger, optimizer boundary,
risk configuration schemas and risk read models.

**Implementation:**

1. Maintain positions/lots, cash, accrued fees/interest, realized/unrealized PnL and
   FX translation using balanced immutable ledger entries. Handle corporate actions,
   exercise/assignment, expiry and futures variation margin.
   Corporate split and cash-dividend actions are journaled before portfolio mutation,
   replayed during startup, reject non-representable integer quantities, and preserve
   cost-basis/mark notional under the declared factor. Option exercise/assignment,
   option expiry, and futures variation-margin settlements are also atomic signed
   position/cash events with versioned journal tags and startup replay; a failed
   cash or quantity check leaves every affected instrument unchanged.
2. Reconcile aggregate broker positions while retaining strategy virtual-book lots
   and allocation provenance.
3. Convert proposals to targets with covariance/uncertainty/liquidity/turnover inputs;
   optimization is deterministic for fixed inputs and has a safe heuristic fallback.
   The current bounded allocator sorts candidates by net confidence score, applies
   per-instrument, liquidity, gross/net, and aggregate-turnover constraints, emits
   accept/resize/reject diagnostics for every candidate, and is exposed through the
   engine's resolved-proposal boundary before independent pre-trade risk checks.
4. Implement limits listed in section 35 of `AGENTS.md`, including scope hierarchy
   (system/account/strategy/asset/instrument), effective-time versioning and reasoned
   allow/resize/deny decisions.
   `ScopedRiskPolicy` resolves the most-specific revision at the injected monotonic
   decision timestamp and is used by proposal and manual-target planning; absent a
   policy, the existing system limits remain the fail-closed default.
   Policy replacement and explicit clearing are journaled with bounded revision
   counts and restored before the engine accepts new planning requests. Replacement
   is exposed only through authenticated `risk.policy.write` IPC; the command
   decoder enforces scope/revision bounds, identity validity, positive hard limits,
   duplicate rejection, and explicit clear semantics before the journal append.
   Contextual guardrails are configurable for predicted volatility, participation,
   message rate, and price deviation. Message rate is measured from planning
   boundaries in a bounded monotonic one-second window. Predicted volatility,
   participation, and price deviation fail closed with `StaleData` until their
   authoritative observation sources are wired; missing telemetry is never
   represented as a safe zero. Limit-order price deviation is measured
   deterministically against the reconciled mark before planning; market/proposal
   paths with no client price remain unavailable when this guard is enabled.
5. Implement RUNNING, REDUCE_ONLY, CANCEL_ONLY and HALTED as persistent state machines.
   Risk relaxation and halt reset require named authorization and are audit events.
6. Fail closed on stale price, missing FX, invalid instrument, unhealthy clock/session,
   unknown account state or inconsistent ledger. Cancel/reduce actions remain possible
   under explicitly tested degraded conditions. Pre-trade broker-session health now
   comes directly from the broker-neutral gateway (`Healthy` is the only approval
   state); unknown, degraded, and unavailable sessions deny before intent creation.

**Objective proof:** property-based ledger tests always balance; IBKR statement
fixtures reconcile cash/positions/PnL across all asset classes; optimizer constraints
hold within declared tolerance; every limit has allow/boundary/deny tests; restart
does not reset risk state; stale/missing/corrupt inputs never produce approval;
benchmarks meet section 1.2.

### [ ] G07 — Execution, IBKR, reconciliation and TCA

**Prerequisites:** G06.

**Create:** `execution`, `broker-api`, `reconciliation`, IBKR adapter, order journal,
capability matrix, operator controls and TCA pipeline.

**Implementation:**

1. Define broker-neutral order intent and complete state transitions. Invalid
   transitions are retained as anomalies and cannot mutate authoritative order state.
2. Generate deterministic ClientOrderId from account, durable intent ID and child
   sequence. Persist intent before send. Never generate a new ID for an uncertain send.
3. Map supported asset/order/time-in-force/session combinations through a versioned
   IBKR capability table. Preflight price/quantity increments and broker permissions.
4. Implement marketable-limit, passive-limit, TWAP, VWAP, POV, adaptive and
   implementation-shortfall planners as deterministic child-order state machines.
   Adaptive scheduling selects POV or front-loaded implementation shortfall from
   one bounded spread/volatility/volume snapshot; the selected schedule is encoded
   in the authenticated command and therefore has identical replay/live semantics.
5. Correlate IBKR callbacks, tolerate duplicates/out-of-order events, and model lost
   acknowledgement as Unknown. Reconcile open orders/executions/positions/account
   values before retry or declaring recovery complete.
6. Run reconciliation at startup, reconnect, periodic interval and detected anomaly;
   classify exact match, expected lag, external activity and dangerous divergence.
   Expose a read-only authenticated `broker.status` command that returns the
   adapter-neutral session health plus bounded broker order, position, and
   account-value counts. The desktop Broker Status panel polls it every five
   seconds, validates the versioned binary response, and falls back to local
   projections without blocking order entry. Credentials and raw broker
   payloads are never returned to the UI.
7. Record decision/arrival/send/ack/fill prices and times, spread, shortfall, latency,
   participation and adverse selection without LLM-derived measurements.
8. Separate paper/live configuration, credentials and visual state. Live enablement
   requires account allowlist, two-step typed confirmation, hard notional cap, kill
   switch and immutable Production strategy/model versions.

**Objective proof:** local fake-broker contract suite covers every transition plus
disconnect, duplicate callback, replace race, rejection, throttle and crash-after-send;
IBKR paper tests run those cases possible in its environment; restart at every send
boundary produces no duplicate order; broker statements reconcile; TCA golden values
match hand calculations; bounded live certification is recorded separately per asset
class before that class can be enabled in live config.

### [ ] G08 — Service assembly, persistence and secure IPC

**Prerequisites:** G07.

**Create:** engine composition root, SQLite migrations/read models, local RPC server,
binary market subscriptions, command authorization and backup/restore tooling.

**Implementation:**

1. Keep journal/order state authoritative; SQLite tables are rebuildable projections.
   Migrations are transactional, forward-tested and backed up before destructive steps.
2. Expose version handshake, snapshot+cursor subscriptions and resumption. Clients
   detect gaps and request a new snapshot rather than applying unknown deltas.
   The authenticated `supervisor.status` read command exposes at most 128 named
   component records with lifecycle, health, failure, retry, and backoff fields;
   malformed or oversized responses are rejected by the desktop decoder.
   The authenticated `risk.policy.status` read command exposes a bounded flattened
   view of installed system/account/strategy/asset/instrument revisions, including
   effective monotonic time and all hard limits; the Risk panel polls it without
   making the policy mutable from the UI.
3. Use length-prefixed Protobuf control messages over authenticated local sockets;
   use shared-memory/binary Arrow batches for high-rate chart streams with ownership,
   version and bounds checks.
4. Authorize commands by actor/session/capability; log command result without secrets.
   UI sessions cannot read credential material.
5. Start headlessly, acquire single-writer lock, restore journal, rebuild projections,
   reconcile broker and only then enter configured risk state.
6. Implement consistent backup manifest, checksum verification, restore into a new
   directory and rollback. Cache loss must not destroy trading state.

**Objective proof:** UI/IPC clients are killed during every command class; engine
continues safely; snapshot+delta reconstruction equals server state hash; malformed
and fuzzed messages cannot crash or allocate unbounded memory; database deletion is
recovered from journal; backup/restore hashes match; unclean reboot reaches reconcile
within 30 seconds on reference data; remote transport can be added by implementing
the transport trait without importing domain changes.

### [ ] G09 — React/Tauri workstation and manual trading

**Prerequisites:** G08.

**Create:** design tokens, Dockview shell, typed IPC client, chart worker/adapters,
command registry, persistence and core panels.

**Implementation:**

1. Implement versioned `WorkspaceLayout`, named presets, migration, monitor-boundary
   clamping and debounced persistence. Trading state is never serialized in layout.
2. Implement link groups with explicit symbol/timeframe/crosshair/watchlist/strategy
   properties and loop-prevention origin IDs.
3. Render candles/bars/line/area, volume and multiple panes; add metrics, strategy,
   positions, orders, fills and event marker primitives. Batch updates off the React
   render path and suspend hidden charts.
4. Build Chart, Watchlist, Order Ticket, Positions, Portfolio, Strategy Browser,
   Strategy Inspector/Comparison, Metrics/Inspector, Risk, Depth, Time & Sales,
   Broker Status, System Health and Trace panels against typed service APIs. The
   System Health panel polls the bounded authenticated supervisor snapshot every
   five seconds and renders each registered component's lifecycle state, health,
   failure count, retry deadline and backoff; a failed status read is isolated
   from order entry and chart rendering.
5. Proposal click creates a local ticket draft only. Preview sends a read-only risk
   request. Submission requires visible account, PAPER/LIVE state, quantity/notional,
   estimated cost, warnings and explicit confirmation; idempotency survives double-click.
6. Implement command palette/keyboard IDs, accessible labels/focus, non-color state
   cues, tabular numerals, virtualization and safe external-link handling.
   UI.md settings are implemented as a searchable modal with Appearance,
   Trading, Data, Notifications, Risk, and Connections categories. The
   colorblind-safe palette is persisted as a versioned preference and remaps
   semantic gain/loss colors without changing engine/trading state.
   Chart surfaces now maintain a bounded presentation-only candle window with
   cursor-anchored wheel zoom, pointer-drag pan, and double-click reset; canonical
   market data and strategy timestamps remain immutable.
   The top bar now exposes numbered, clickable workspace tabs for every named
   preset plus a `+` action that creates a Research-template workspace; tab
   selection is persisted through the same validated `WorkspaceLayout` store.
   Tabs are draggable, reorder only after a valid known-preset drop, and persist
   their bounded v1 order separately from trading/runtime state; malformed or
   incomplete tab-order storage deterministically falls back to the canonical order.
   The persistent status bar now includes an expandable message station with
   bounded pending-alert previews and a one-click route to the validated Alerts
   workspace panel; it never owns acknowledgement state or trading state.
   `Cmd/Ctrl+1` through `Cmd/Ctrl+7` switch directly to the corresponding
   validated workspace preset, ignore shortcuts while editing fields, and
   `Escape` closes the command palette or message station without mutating
   runtime/trading state.
   The Vite production build is a required frontend gate; it now covers the
   configuration reload handler that the source-presence check cannot parse.
   The Trading workspace uses a real right-side tabbed dock for Positions,
   Orders, Watchlist, and Alerts; those panels are removed from the central
   canvas while docked, and tab selection remains presentation-only. Other
   workspaces retain their full validated panel canvas.
   Workspace switches now save and restore a bounded per-workspace symbol and
   timeframe context, reloading the corresponding chart preferences and drawings;
   invalid context records fall back to the current canonical selection.
   Chart pointer movement now uses a bounded animation-frame update to expose a
   nearest-candle OHLC/time crosshair readout; pointer leave clears it, and the
   chart intercepts right-click with a local menu for horizontal drawing and
   zoom reset without invoking a global browser menu or mutating market data.
   The Trading right dock has a visible keyboard-focusable splitter. Pointer
   drag and Left/Right arrows resize it between 240px and 560px, update the
   layout live without a render loop, and persist only the clamped presentation
   width under a versioned local-storage key.
   Trading mode also exposes a compact left tools rail with an explicit expand
   toggle and focus actions for Chart, Order Ticket, Metrics, and Risk. Actions
   scroll existing validated panels into view; they do not create a second order
   submission path or duplicate broker state.
   The Settings modal now includes searchable Layout and Hotkeys categories,
   exposes the active workspace and an explicit template reset action, and lists
   only shortcuts that are actually wired. Reset delegates to the same validated
   workspace preset path and closes the modal after applying it.
   Message Station history now retains up to 256 session alert records, marks
   acknowledged records without deleting them, and keeps the engine's pending
   alert list authoritative for counts and acknowledgement requests.
   Chart drag release now applies bounded exponential momentum over a 350ms
   decay window, clamps the presentation window to available candles, and
   cancels safely on a new pointer-down; canonical candles remain untouched.
   Horizontal trackpad/two-finger wheel deltas now pan the bounded chart window;
   vertical and Ctrl+wheel gestures retain cursor-anchored zoom semantics.
   The desktop Configuration panel loads the authoritative snapshot, renders a
   deterministic `.cfg` editor/generator, and submits only versioned atomic reloads
   through authenticated IPC. The engine parser bounds keys/strings/input size,
   rejects duplicates/non-finite values, and preserves compare-and-swap conflicts.
   The desktop bridge also accepts `serve --config PATH`; the file is parsed by
   `cfg-core` before engine startup and supplies typed risk/alert settings, while
   legacy environment variables remain fallback values only when a key is absent.
   Startup cadence, market freshness, and LLM base URL/timeout now use the same
   typed file-first lookup with bounded ranges and environment fallback; focused
   bridge tests prove a valid `.cfg` value wins over the fallback path.
   README setup instructions now provide a reproducible `.cfg` bootstrap command,
   validation boundaries, UI reload behavior, and the explicit secret-manager
   boundary for credentials.
   The Configuration panel generator now covers all seven supported risk
   guardrails (`max_leverage`, drawdown, outstanding orders, predicted volatility,
   participation, message rate, and price deviation), rejects non-finite or
   non-integer inputs client-side, and emits deterministic keys for engine
   validation rather than silently omitting configured controls.
   UI contract tests now assert the required workstation navigation surfaces and
   every generator field/engine key marker, so a superficial panel removal or
   incomplete setup form fails the normal `npm test` gate.
   The command palette is now a filtered command registry rather than a static
   shortcut list: it searches bounded labels, exposes global search, alerts,
   strategy analysis, backtest, order ticket, TraceId inspection, workspace
   switching, and panel restoration, and routes each action through the existing
   validated workspace layout path.
   The required Metric Inspector is now a separate read-only panel. Selecting an
   installed metric from Metrics opens it through the validated layout path and
   shows the authoritative lifecycle/health, priority, period/deadline/budget,
   TTL, score bounds, and declared inputs from the typed metric registry response;
   lifecycle mutation remains available only through the existing authenticated
   engine command.
   The UI token layer now defines every token referenced by workstation surfaces,
   including display/body/monospace fonts, panel/control radii, glass blur, glass
   background, and focus borders. Primary panels and the tools rail consume those
   tokens, so `UI.md` styling cannot silently disappear because an undefined CSS
   custom property made a declaration invalid. Contract tests assert the token
   definitions are present.
   Appearance preference initialization now occurs only after the bounded storage
   adapter is constructed. This prevents a desktop startup `ReferenceError` from
   reading `layoutStorage` during module initialization; the UI contract test
   asserts the declaration order explicitly.
   The locked-in Trading top bar now displays a deterministic signed percentage
   change computed from the canonical resampled candle window. It shows an
   explicit plus/minus sign and an accessible gain/loss label, so color is not the
   only state cue; insufficient history renders a truthful unavailable state.
   Appearance settings now include a bounded persisted 90/100/110% font scale;
   it applies only to the document presentation layer and never enters engine or
   trading state. The setting is restored through the same validated local storage
   boundary and is covered by UI contract markers.
   The `MultiChart` preset now contains four validated chart panel IDs arranged in
   a deterministic 2×2 grid. Secondary charts reuse the canonical candle snapshot
   and shared presentation window/link-group state; they do not create duplicate
   market, order, or portfolio authorities. Existing layouts migrate through the
   normal completion path and the panel IDs are included in contract checks.
   Panel pop-outs now disclose their actual semantics: they are read-only,
   detached snapshots with all controls disabled and an explicit return-to-main
   workstation notice. The popup uses `noopener`, preventing a detached window
   from becoming an unauthorized action path or implying live state ownership.
   Glass surfaces now include an explicit opaque fallback under
   `@supports not (backdrop-filter: blur(1px))`, preserving contrast and panel
   hierarchy on WebViews/GPU paths without backdrop-filter support.
   News retention and presentation now support a hard-bounded 100,000-item
   virtualized feed: the runtime store retains at most 100,000 canonical items,
   the UI no longer truncates the loaded page set to 500, and only the viewport
   window is rendered into DOM nodes. This makes the 100,000-item scroll proof
   objectively testable without allowing unbounded client memory growth.
   Time & Sales retention is likewise hard-bounded to 100,000 prints per symbol
   at the immutable store boundary; its existing spacer/window virtualizer renders
   only the visible tape rows, so long-running sessions cannot grow DOM or memory
   without limit.
   The News panel now exposes the normative `Relevant`, `All`, `Watchlist`, and
   `Portfolio` views. Watchlist/Portfolio are presentation filters over canonical
   news symbols and request the engine's `all` page scope; they do not rewrite
   relevance scores or create a second news authority. Empty portfolio/watchlist
   results are rendered explicitly and remain virtualized.
   Configuration setup also provides copy/download actions for the exact text
   shown in the editor; the download is a plain `insidertrader.cfg` artifact and
   does not persist credentials or bypass authenticated engine reload.
   The setup generator reads the current authoritative `.cfg` text into its
   controls instead of presenting unrelated hard-coded values. It covers the risk
   assignments plus bounded Python worker paths and cadence, execution cadence,
   market freshness/transport/provider tuning, LLM timeout, and provider base URL
   settings. The URL generator
   requires HTTPS except for explicitly local `localhost`/`127.0.0.1` HTTP
   endpoints, which supports self-hosted inference without weakening remote
   transport safety. The desktop bridge repeats the same policy before installing
   an LLM provider, so raw editor/file input cannot bypass it; focused tests reject
   remote HTTP and lookalike localhost hostnames. Generation merges only those assignments,
   preserves all unrelated operational/provider settings and inline comments,
   removes duplicate target assignments, appends missing keys deterministically,
   and always emits one trailing newline. UI contract tests assert every generated
   field/key and the merge/read helpers so a future rewrite cannot silently replace
   the whole file.

   The AI Analyst now exposes an explicit removable context-chip set for symbol,
   timeframe, cursor, linked news, active strategy proposals, and reconciled
   positions. The analyze handler constructs the request from the chips that remain,
   appends bounded structured context to the user input, hashes exactly that selected
   context, and rejects the combined request before the bridge's 1 MiB limit. Removing
   a chip therefore changes both the provider input and audit context hash; selecting
   no chips sends a documented `manual-only` context rather than silently reusing live
   state. Contract tests require the chip controls and payload markers.
   The analyst evidence requirement is also implemented: each successful request
   snapshots selected context into typed evidence cards mapped to authoritative Chart,
   News, Strategy Inspector, or Positions panels. Cards expose an `Open source` action
   that promotes and scrolls to the validated panel. The response explicitly discloses
   that model prose is unverified unless supported by a source card; free-form text is
   never treated as authoritative execution state.
   Suggested analyst actions from AGENTS.md are now concrete bounded prompt presets
   (`Explain move`, `Summarize relevant news`, `Compare strategies`, `Why is risk high?`,
   `What changed since open?`, and `Analyze this region`). They only populate the normal
   input control, so analysis still requires the same explicit submit, selected-context
   hash, provider validation, TTL, and evidence-card path.
   The Autonomy Console now renders the authoritative policy mode and risk state,
   bounded runtime context snapshot, pending action count, model, plan validation
   timeline, reconsider deadline, and the exact selected proposal records with
   confidence, expiry, and rationale. It explicitly states that plan approval cannot
   bypass portfolio, risk, execution, or reconciliation, and renders an empty state
   when selected proposals are absent rather than inventing them.
   The command palette now covers the remaining normative command vocabulary for
   opening autonomy, news, watchlist, and metrics panels, plus explicit manual,
   hybrid, and autonomous mode commands. Mode commands call the authenticated
   `setTradingMode` boundary, require the same autonomous confirmation guard as the
   selector, and reconnect the typed runtime projection; they do not mutate UI-only
   state or submit orders directly.
   Workspace presets were reconciled with the normative workstation list: Research
   now includes Backtest, Experiment Registry, and Model Registry; Strategies includes
   Strategy Browser, Backtest, and Portfolio; Autonomy includes Strategy Inspector.
   A static preset contract test asserts these required panel memberships.
   AI Analyst context-chip selection is now persisted as a bounded version-1
   presentation envelope (`insidertrader.analyst.context.v1`). Restore accepts only
   the six known context IDs, ignores malformed or unknown values, rewrites a safe
   default envelope, and persists every removal immediately. This preserves user
   context preferences without persisting market, position, order, or broker state.
   High-frequency runtime patches no longer invoke a synchronous full-workspace render
   for every event. The store subscription coalesces updates through one
   `requestAnimationFrame` (with a bounded 16 ms fallback), while explicit user
   interactions retain immediate renders. This gives the UI a deterministic frame
   boundary without moving authoritative state into a second UI store.
   Message Station presentation now follows the UI.md severity contract: info/success
   entries remain visible for 4 seconds, warnings for 8 seconds, and critical entries
   remain visible until resolved. Expiry affects only the collapsed station summary;
   the authoritative Alerts panel and bounded session `messageHistory` retain every
   entry, so auto-dismiss never acknowledges, deletes, or hides an actionable record.
   Notifications now expose four persisted sound controls (Info, Success, Warning,
   Critical) behind the global sound switch. The delivery path checks the severity
   preference before creating audio, validates the four-element boolean envelope on
   restore, and leaves native/webhook routing and authoritative alert acknowledgement
   unchanged.
   The Message Station expiry cache is hard-bounded at 4096 IDs and prunes entries
   no longer present in the authoritative alert snapshot, preventing long-running
   sessions from accumulating presentation-only alert metadata.
   Settings categories now provide direct, validated navigation to the authoritative
   Order Ticket, CFG Generator, Alerts, Risk, and Broker Status panels. This removes
   dead-end descriptive placeholders while keeping operational values in `.cfg` and
   credentials outside UI persistence.
   The workspace `+` control now opens a bounded template menu for the UI.md-required
   Scalping, Swing, Research, and Backtest layouts. Each template is a validated
   `WorkspaceLayout` with an explicit panel set and link group; creation schedules the
   same asynchronous presentation persistence as normal workspace edits. The command
   palette exposes the new templates as well, and contract tests assert their required
   panel memberships.
   Graph/vector search hits are now typed interactive results rather than inert text.
   Selecting a hit snapshots its score/evidence path, shows the bounded detail card,
   routes recognized node kinds (instrument, news, strategy, metric, order, model,
   experiment, position) to the corresponding authoritative panel, and changes the
   linked symbol for instrument nodes only after strict identifier validation.
   Order Ticket edits now invalidate any existing preview on `change`. The current
   exact draft is retained for display, but status returns to `idle` and submission is
   disabled until the risk/execution preview is recomputed. This prevents a user from
   modifying quantity, side, type, or limit price and accidentally submitting stale
   preview assumptions.
   Order Ticket now displays the authoritative preview expiry timestamp and treats an
   expired preview as non-submit-able even if no new runtime patch has arrived. The UI
   shows `Expired — preview again`, while the engine remains the final validator.
   Analyst responses now carry a display timestamp and a five-minute UI TTL. Once
   expired, a bounded timer marks the response visibly stale and instructs the user to
   rerun it; the response remains available for review, but is not presented as current
   context. A new successful stream resets the timestamp and stale notice.
   The Trading tools rail now follows UI.md's collapsed-by-default behavior while
   expanding on hover or explicit pin/toggle. Labels remain in the accessible DOM but
   are CSS-collapsed at 38px and shown at the 112px expanded width, preserving icon
   affordances, keyboard activation, and the existing validated panel-focus actions.
   Chart touch interaction now supports two-pointer pinch zoom with midpoint anchoring,
   pointer cancellation cleanup, and browser gesture suppression via `touch-action:none`.
   Single-pointer touch remains compatible with the existing bounded drag/inertia path;
   pinch state is presentation-only and never changes canonical market data.
   During an active pinch, the chart keeps pointer listeners attached and applies a
   bounded transient `--chart-pinch-scale`; the canonical `ChartViewWindow` is
   committed on pointer release and the transient transform is removed on release or
   cancellation. This prevents gesture rerenders from dropping the second pointer.
   Single-pointer chart dragging now also applies a transient bounded SVG translation
   during pointer movement, giving immediate pan feedback without replacing the DOM or
   detaching pointer capture. Release converts the pixel delta into the canonical
   `ChartViewWindow` and clears the transient transform.
   Workspace tabs and keyboard navigation now include the expanded preset registry;
   `Cmd/Ctrl+1–9` resolves against the persisted validated tab order rather than a
   stale hard-coded seven-item list. MultiChart is also available as an explicit
   command-palette entry.
   Provider registration releases its registry lock before health projection,
   preventing recursive-lock deadlock during startup and making provider polling
   restart tests terminate deterministically.

**Objective proof:** Playwright/Tauri tests cover dock/tab/split/float/popout/restore,
multi-monitor clamping, link isolation, reconnect, stale state, proposal-to-preview-
confirm, double-click and live-mode warnings; visual regressions cover all states;
UI crash does not affect an active paper order; 8-hour UI soak and performance tests
meet section 1.2; manual trading remains complete with LLM/news disabled.

### [ ] G10 — News pipeline and news workstation

**Prerequisites:** G08; integrates with G09.

**Create:** `news-core`, provider adapters for NewsAPI/Yahoo/RSS/IBKR, content store,
dedupe/cluster/rank workers and Relevant/All/Detail panels.

**Implementation:**

1. Normalize immutable article versions with canonical URL, provider/source, title,
   summary, publish/receive times, entities, instruments, content hash and provenance.
2. Implement provider cursors, pagination, rate limits, exponential backoff, conditional
   requests and dead-letter diagnostics. Never advance a durable cursor before storage.
   NewsAPI exposes both `/v2/everything` and `/v2/top-headlines` through separate
   validated adapters. The top-headlines adapter requires at least one explicit
   country, category, or source filter, bounds each filter, preserves page cursors,
   and is selected only by `IT_NEWSAPI_ENDPOINT=top-headlines`; the default
   `everything` behavior remains unchanged.
   `ServiceHost::poll_news_fallback` accepts a bounded deterministic provider priority
   list, stops after newly ingested items, persists every attempted provider snapshot,
   and projects aggregate health once for the cycle.
   Every registered provider also exposes `Unknown`, `Healthy`, `CoolingDown`,
   `Degraded`, or `Failed` health with last success/failure timestamps and
   consecutive-failure count; fallback selection consumes this status rather than
   inferring health from an empty page.
   The status list is exposed through authenticated IPC with bounded binary
   decoding and is consumable by the desktop System Health/Broker Status views.
   Provider status transitions also project into the supervised `news` component:
   unknown or empty registries remain `Unknown`, transient retry/degraded states
   become `Degraded`, exhausted dead letters become `Unavailable`, and only an
   all-healthy registry becomes `Healthy`.
   Provider-state journal payloads are V2 and persist health, last-success,
   last-failure, and consecutive-failure fields; restart accepts legacy V1 cursor
   records with conservative `Unknown`/`CoolingDown` reconstruction.
   News cursor providers expose the shared typed manifest contract; registration
   validates provider identity/kind before accepting NewsAPI everything,
   top-headlines, Yahoo Finance, or RSS adapters.
3. Apply exact URL/content dedupe, normalized-title similarity and syndication rules;
   cluster events with a versioned algorithm and retain cluster membership history.
4. Resolve symbols through instrument master and entities with confidence/provenance.
   Low-confidence links do not become direct-symbol facts.
5. Rank deterministically using versioned weights for direct link, entity distance,
   lexical/semantic match, recency, event, strategy, position, timeframe and source.
   Relevant-page IPC responses use a versioned V2 envelope carrying the bounded
   deterministic score in basis points per item; the desktop decoder accepts V1
   during migration and renders the score without treating UI ranking as truth.
6. Implement virtualized Relevant/All/Watchlist/Portfolio feeds, filters, pinning,
   details, related strategies and exact chart-marker navigation.

**Objective proof:** recorded fixtures test pagination restart, duplicates, corrections,
missing dates, malformed HTML, 429/5xx and provider outage; labeled corpus publishes
precision/recall/NDCG baselines and blocks unexplained regression > 2%; disabling LLM
and graph still produces direct-symbol ranking; marker opens exact content version;
100,000-item feed scrolls within UI performance budgets.

### [ ] G11 — LLM core and AI Analyst

**Prerequisites:** G08, G10.

**Create:** `llm-core`, provider adapters, fake OpenAI server, prompt/tool registries,
cache, budget/rate limiter and AI Analyst panel.

**Implementation:**

1. Implement Responses and Chat Completions translation behind `LlmProvider`, custom
   base URL, capabilities, streaming, bounded retry/jitter, deadline and cancellation.
   Shared `provider-core` manifests now define provider kind, auth method, endpoint,
   capabilities, streaming, timeout/rate limits, retry policy, health probe, and
   schema version; invalid or duplicate manifests are rejected before registration.
   `LlmProvider` exposes the manifest boundary, and `ServiceHost::install_llm_provider`
   validates it before the provider can enter runtime state; the OpenAI-compatible
   adapter supplies its concrete manifest and capability list.
2. Distinguish transport, auth, rate-limit, timeout, refusal, malformed, schema,
   semantic and interrupted-stream failures. Retry only classified safe failures.
3. Compile JSON Schema validators for trading-relevant outputs; buffer/validate the
   complete action object before publishing it. Streamed text is display-only.
4. Register typed tools with permission, input/output schemas, maximum result size,
   freshness, deadline and audit. Tool results link to authoritative object IDs.
5. Version prompts and record provider/model/capabilities/parameters/prompt hash/tool
   schema/context hashes/usage/TTL/output. The runtime `PromptRegistry` stores
   immutable prompt ID/version records with purpose, input/output schemas, sorted
   allowlisted tools, required capabilities, fixture-suite ID and SHA-256 artifact
   hash; `latest` is rejected and duplicate versions cannot register. Cache by all
   semantic inputs and policy.
   Prompt registrations are persisted as `IT_PROMPT_RECORD_V1` journal events and
   restored before the engine exposes its LLM interface; invalid restored metadata
   halts recovery rather than silently accepting prompt drift.
6. Add per-task token/concurrency/cost budgets and metrics; secrets never enter traces.
   Completion and streaming failures publish `llm` component health to the engine
   supervisor; deterministic market, strategy, risk, and execution components stay
   available while the LLM component is degraded.
7. Analyst context chips enumerate exact objects and allow removal. Answers cite
   internal evidence cards; unsupported factual claims are visibly unverified.

**Objective proof:** fake-server contract suite covers endpoint fallback, SSE chunk
boundaries, UTF-8 splits, disconnect, 429 Retry-After, 5xx, timeout, refusal, malformed
JSON, extra fields, tool abuse and cancellation; no partial response creates an action;
cache keys change for every semantic input; prompt snapshots pass; provider outage
does not affect deterministic subsystems; replay uses pinned output artifacts.

### [ ] G12 — Context graph, embeddings and search

**Prerequisites:** G10, G11.

**Create:** `context-graph`, graph schema/migrations, embedding index, ingestion jobs,
hybrid retrieval API, global search and graph panel.

**Implementation:**

1. Implement every normative node/edge with stable identity, validity interval,
   knowledge interval, provenance, confidence and source artifact.
2. Idempotently project reference data, news, dependencies, portfolio, orders/fills,
   models and experiments. Durable order-intent and broker-fill events now project
   stable order/fill nodes and `HAS_FILL` causal edges. Model-registry and experiment lifecycle journal events
   now project stable model/experiment nodes with artifact provenance and
   point-in-time knowledge facts. Corrections close validity; they do not erase
   history. Broker reconciliation and fill application project the current
   portfolio aggregate, instrument positions, canonical instrument nodes, and
   `HOLDS`/`POSITION_OF` edges. News projection also creates normalized
   `NewsCluster` nodes and `IN_CLUSTER` edges using the same deterministic
   title normalization as the news store. Metric registration and lifecycle
   events project `Metric` nodes with lifecycle/evidence provenance. Strategy
   manifests project `USES_METRIC` and `DEPENDS_ON` edges before evaluation;
   strategy lifecycle transitions project state/evidence facts and are restored
   from the journal.
3. Store vectors with content hash, model/version, dimensions, normalization and
   created time. The graph projection now owns a single configured model/version
   index, validates and normalizes inserts atomically, and routes query vectors
   through the authoritative hybrid-search boundary. A versioned V2 context-search
   IPC payload carries bounded finite vectors while V1 clients remain compatible.
   Desktop deployments can configure the model tuple atomically with the
   all-or-none `IT_EMBEDDING_MODEL*` startup settings before reconciliation.
   Mixed-model retrieval is
   prohibited unless a calibrated bridge is explicitly versioned; re-embedding is
   resumable by building a complete validated generation and atomically replacing
   the rebuildable index projection; failed generations leave the active index
   untouched. Accepted generations are journaled and restored before the service
   accepts search or trading requests.
4. Combine exact symbol, lexical, filters, bounded graph traversal and vector scores
   through a versioned ranker. Return component scores and evidence paths.
5. Implement typed global results that open correct panels and a bounded graph view.

**Objective proof:** point-in-time graph fixtures return only known nodes/edges;
idempotent ingestion yields identical graph hash; traversal enforces depth/result/time
limits; embedding-version mixing is rejected; labeled retrieval corpus publishes
Recall@K/NDCG and blocks >2% unexplained regression; graph/vector outage falls back
to exact/lexical direct-symbol search without impacting trading. A malformed or
dimension-mismatched optional query vector is treated as a per-request vector
degradation and cannot suppress the deterministic fallback.

### [ ] G13 — Hybrid and autonomous coordination

**Prerequisites:** G07, G11, G12.

**Create:** `autonomy`, policy schema, context packet builder, semantic validator,
approval queue, shadow evaluator and Autonomy Console.

**Implementation:**

1. Model mode and permission by account/strategy/universe/action/time/notional. Default
   is manual; escalation requires authenticated explicit action and durable audit.
2. Build context packets from immutable IDs with freshness/size/token budgets and
   disclose omitted/truncated inputs. Snapshot proposal and risk-budget versions.
3. Accept only finite action vocabulary. Validate plan ID/time/TTL, proposal existence
   and live state, scale range, policy, strategy health and unchanged critical context.
4. Revalidate immediately before target creation. Portfolio/risk/execution remain
   authoritative; plan approval never equals an order approval.
5. Implement pending/approved/rejected/expired/executing/completed/failed plan states,
   reconsider timers on injected clock and idempotent recovery.
6. Implement shadow mode comparing suggested actions to actual deterministic/user
   decisions without submitting. Hybrid approval queues expire safely.
7. Console shows policy, exact context/evidence, provider/model/prompt, plan validation,
   selected proposals, targets, risk outcomes, approvals, orders and trace timeline.

**Objective proof:** property/fuzz tests reject unknown actions, stale IDs, invalid
scales, replayed plans and changed critical context; outage/interruption yields no
action; restart cannot resubmit completed intent; manual/hybrid/auto transition matrix
passes; shadow reports are reproducible; paper certification runs 30 consecutive
calendar days with zero action lacking a valid trace/policy/risk chain before live
autonomy can enter bounded canary certification.

### [ ] G14 — Remaining workstation and operational workflows

**Prerequisites:** G09-G13.

**Create:** remaining required panels/presets, alerts, notification router, drawing
store, backtest/registry views and operator runbooks.

**Implementation:**

1. Finish News Detail, AI Analyst, Autonomy, Screener, Heatmap, Correlation, Alerts,
   Backtest, Experiment Registry, Model Registry, TCA, Logs/Trace and all presets in
   sections 21 and 44 of `AGENTS.md`.
   News, watchlist, and time-and-sales views use bounded spacer/window virtualization
   so rendered DOM nodes remain proportional to the viewport rather than retained
   history length.
2. Persist watchlists, drawings, templates, filters and preferences with versioned
   migrations. Watchlists, chart preferences, drawings, and link groups use explicit
   v2 envelopes with bounded validation and v1 migration; references point to
   immutable domain objects and no UI record is broker truth.
   Successful migration rewrites the validated v2 value and removes the legacy key;
   drawing migration persists only the filtered drawing set, preventing repeated
   parsing of stale or malformed legacy records.
   Chart indicator/display templates are bounded to 32 named records and expose
   explicit save, apply, and delete actions; applying a template changes only
   chart presentation preferences.
   The Experiment Registry read path is V2: engine responses include bounded
   strategy/news/graph/LLM/prompt/tool/cache/autonomy provenance, while the Tauri
   decoder accepts legacy V1 records with empty provenance and renders the retained
   lineage without treating UI state as authoritative.
3. Route price/metric/strategy/news/risk/autonomy/order/provider alerts to in-app,
   native, sound and allowlisted webhook channels with dedupe, cooldown and acknowledgement.
4. Implement full TraceId reconstruction, raw-event permission checks and export with
   sensitive-field redaction. Accepted manual, direct-proposal, and scheduled-proposal
   submissions persist a typed trace-link event before the order intent; trace queries
   join that link to the immutable strategy-proposal record and subsequent broker
   events without treating UI state as evidence.
5. Publish installation, configuration, paper/live switching, backup/restore, upgrade,
   provider outage, reconciliation, halt/reduce-only, credential rotation and incident
   investigation runbooks.

**Objective proof:** a trace fixture reconstructs market/news through fills with no
missing causal link; each required panel has loading/empty/live/stale/degraded/error/
permission-denied tests; alert routing is idempotent and secret-safe; persisted state
migrates from every released version fixture; complete manual, research, hybrid and
autonomy journeys pass on a packaged Linux build, not the development server.

### [ ] G15 — Integrated production certification

**Prerequisites:** G00-G14.

**Implementation and proof:**

1. Freeze an RC with exact binary/config/schema/data hashes. Run all deterministic CI,
   performance, security and compatibility suites against that RC.
2. Run a seven-calendar-day full-load paper soak including market/news ingestion,
   metrics, strategies, UI sessions, research jobs and configured LLM workloads. Zero
   duplicate orders, unreconciled final positions, silent data gaps, journal corruption,
   critical alerts or unbounded resource growth are allowed.
3. Execute disaster drills: UI kill, engine kill -9, host reboot, disk-full warning,
   network partition, IBKR disconnect, market/news/LLM outage, corrupted cache, damaged
   journal tail, clock anomaly, database rebuild, risk halt and credential revocation.
4. Reconcile paper broker statements to ledger with zero unexplained cash/position/fill
   differences. Any explained tolerance is asset-specific and documented.
5. Certify live trading separately for stocks/ETFs, options, futures, FX and crypto.
   Each enabled class needs account permission proof, capability fixtures, shadow run,
   paper run, minimum-size live canary, statement reconciliation, kill-switch drill and
   risk/operations/account-owner approval. Non-certified classes remain hard-disabled.
6. Security review covers dependency provenance, secrets/keyring, local IPC, Python,
   external payloads, authorization, audit retention, update signing and backup data.
7. Demonstrate clean install, upgrade, rollback, backup/restore and incident response
   using published runbooks on the reference Linux profile.
8. Confirm production configs reference immutable strategy/model/prompt versions and
   that Research -> Validated -> Shadow -> Canary -> Production cannot be bypassed.

The executable certification procedure is `docs/runbooks/release-certification.md`.
It defines the RC hash manifest, seven-day soak observations, fail-closed disaster drills,
broker statement reconciliation, asset-class canary approvals, and immutable G15 evidence.
`scripts/check_runbook.py` validates that these sections and safety markers remain present.

**Final release condition:** all prior gate evidence verifies against the RC revision;
engineering, risk, security, operations and authorized account owner sign G15 evidence;
there are zero open critical/high defects. Profitability is not claimed by this gate;
it proves correctness, controls, and operational fitness of configured artifacts.

## 5. Requirement traceability

Create `evidence/requirements.csv` during G00 with columns:

```text
requirement_id,agents_section,normative_text,owner_gate,verification_id,status,evidence
```

Every `MUST`, every non-negotiable rule, every required panel and every A001-A050
acceptance item in `AGENTS.md` receives one row. `scripts/check_requirements.py` fails
when a normative line has no owner, when a verification ID has no test/runbook, or
when a release has any non-passed requirement owned by G00-G15. This matrix is the
mechanism that prevents broad gate language from hiding omitted specification work.
The checker additionally requires each verification ID to match `verify-...` and each
evidence field to resolve to an existing repository path, so rows cannot claim verification
against missing artifacts.

The traceability table now includes explicit planned rows for every A001-A050 acceptance
item. `check_requirements.py` derives the catalogue IDs directly from `AGENTS.md` and fails
if any `ACCEPT-A###` row is absent. These rows intentionally remain `planned` until the
required packaged, replay, outage, performance, and accessibility evidence exists; adding
the rows does not claim those gates have passed.

## 6. Implementation order and parallel work

The critical path is G00 -> G01 -> G02 -> G03 -> G04 -> G05 -> G06 -> G07 -> G08
-> G09 -> G14 -> G15. After G08, news/LLM/graph work proceeds as G10 -> G11 -> G12
while UI core proceeds through G09. G13 waits for execution, LLM and graph contracts.

Parallel branches may not duplicate domain types. Contract changes begin with schema
and compatibility fixtures, then regenerate bindings, then update producers before
consumers. Breaking changes require a migration/dual-read period and ADR; silently
changing serialized meaning is forbidden.

## 7. Explicit non-goals for the initial release

- No promise of strategy profitability or investment performance.
- No production broker other than IBKR, though broker-neutral contracts are required.
- No certified Windows/macOS package in G15.
- No remote/cloud deployment certification; local boundaries must remain extractable.
- No silent approximation of unsupported order types or asset capabilities.
- No LLM in the hot execution path and no direct LLM-to-broker access.

## 8. UI evidence additions (2026-08-26)

The screener must not silently hide canonical quote records behind a fixed cap. The
implementation derives the complete filtered/sorted result set from the runtime store,
renders only a bounded page of 100 rows, reports `rendered/total` counts, and exposes a
`Load next N` action until every matching record is visible. Changing the query or sort
resets the page to 100; each load-more action is wired to a fresh render and cannot exceed
the authoritative quote count. `ui/src/app/main.ts` contains the state and event contract,
and `ui/src/theme/tokens.css` defines the pagination layout. Verification requires
`npm test`, `npm run check`, and `npm run build`, plus a manual fixture with >100 quotes
that confirms counts advance 100 at a time and that filtering/sorting resets the count.

The chart interaction contract is also explicit: pointer movement snaps the crosshair to
the nearest candle index (`Math.round`, bounded to the active view), and a two-touch gesture
tracks both pinch distance and the touch centroid so horizontal two-finger translation is
applied to the canonical `ChartViewWindow`. The context menu routes only to existing
authoritative UI actions (open Alerts, draw/clear persisted levels, toggle persisted metric
overlays, reset view); it does not fabricate an alert or bypass the engine. The workstation
source contract asserts these action IDs and gesture state, while the repository gate builds
the TypeScript bundle and runs all Rust/UI tests.

Settings evidence now covers the high-frequency local presentation defaults from `UI.md`:
Appearance exposes the chart style selector, and Trading exposes validated default order type
and quantity controls persisted under a versioned local key. One-click trading is rendered as
explicitly disabled because the engine confirmation/risk gate remains mandatory; the UI cannot
turn that safety boundary into a client preference. Operational refresh, provider, and risk
limits continue to route through the `.cfg` generator and authenticated engine reload path.

Hotkeys are now an actual supported configuration surface rather than documentation-only:
the Settings modal exposes Command Palette and workspace-slot bindings, normalizes them to
`Mod+<key>`, rejects malformed values and duplicate assignments, persists a versioned map,
and applies the map in the global keyboard dispatcher. The editable set is deliberately
bounded to presentation/navigation actions; order, autonomy, and risk commands are not made
remappable from the UI. Corrupt stored bindings fall back to the documented defaults.
The loader additionally requires schema version `1` and rejects the entire persisted map if
any supported action is malformed or if two actions share a binding; partial or ambiguous
hotkey state is never activated.

Chart gridline density is now a real presentation preference with bounded values `none`,
`low`, and `high`. It is validated in chart-template migration, persisted per symbol and
timeframe, exposed in both Settings and chart controls, and passed to the deterministic SVG
renderer (0/4/8 horizontal gridlines). Invalid legacy values safely normalize to `low`.

Workspace switching now reads the validated persisted layout for the selected preset before
falling back to its template. This preserves panel order, hidden panels, link-group state,
and other presentation edits across switches; corrupt or schema-invalid layout data still
falls back through `loadWorkspaceLayout` to the canonical preset. The UI source contract
asserts this load boundary.

The switch boundary also flushes `WorkspacePersistence` before changing workspace keys. This
closes the debounce race where a fast tab switch could replace the previous workspace's
pending write with the next workspace layout. Verification covers the explicit flush call,
the validated load fallback, and the full UI/repository gates.

Workspace lifecycle is now bounded and presentation-only: up to eight custom workspaces may
be duplicated from the current validated layout, renamed, reordered, and deleted. Names are
validated against a 32-character allowlist and cannot collide with built-in presets; each
custom entry records its base template, layout, and symbol/timeframe context. Delete returns
to Trading and never touches journal, portfolio, order, or broker state. Custom workspace
metadata and tab order are versioned/persisted independently from runtime state.
Deletion also removes the workspace's persisted symbol/timeframe context before returning to
Trading, preventing orphaned presentation state from accumulating.

Workspace rename/delete also remove the obsolete versioned layout key through the
`WorkspacePersistence.remove` API. Rename flushes the old pending write before deleting
the old key, then schedules the validated layout under the new name; delete flushes and
removes the active key before switching to Trading. This prevents stale layouts from
resurfacing after a name is reused and keeps persisted presentation state bounded.
Rename also transfers the validated symbol/timeframe context and deletes the old
context key, so metadata, layout, and chart context remain one-to-one after repeated
rename cycles.

Strategy package discovery is now defensive at the filesystem boundary: recursive
walks canonicalize and de-duplicate directories (including symlink loops), enforce a
32-level depth limit and a 4,096-manifest limit, and retain deterministic sorted paths.
The host returns a typed bounds error instead of hanging or admitting an unbounded
package set. A Rust fixture covers recursive ordering and missing-root failure.
Metric package discovery applies the same canonical-path, depth, and count bounds and
has an equivalent filesystem fixture, preventing one package class from remaining an
unbounded startup resource even when strategy discovery is hardened.
Both discovery paths now reject duplicate immutable IDs before returning a catalog,
with typed `Invalid` errors naming the duplicate class; fixtures cover the rejection so
startup cannot proceed with ambiguous metric or strategy definitions.
Manifest files are read through a bounded 1 MiB stream before parsing. Oversized input
returns `BoundsExceeded { bound: "manifest_bytes" }` and is covered by both host fixtures,
so package discovery cannot allocate memory proportional to an untrusted file size.
Manifest parsers now reject duplicate keys with a stable `Invalid` discovery reason;
fixtures cover the duplicate-field path for both package types, preventing silent
last-write-wins changes to scheduling or identity.
The desktop bridge now reads `--config` through a bounded streaming reader before
calling `cfg-core`, and rejects files over 1 MiB. A bridge test covers the oversized
file path so startup cannot allocate memory proportional to an untrusted config file.
Canonical instrument insertion now indexes provider identities even when the canonical
definition already exists. A catalog fixture inserts one instrument through IBKR and
Yahoo and resolves both provider-qualified keys to the same ID, preserving safe fallback
routing without duplicating authoritative instrument state.
Provider identifiers are now validated before indexing (non-empty, maximum 64 bytes)
with `InvalidProvider`; a catalog fixture proves rejected identities leave the catalog
unchanged.
Order-book broker fill application now validates both quantity and execution price before
mutating state. A zero/negative-price fill returns `MismatchedOrder` and leaves the order's
filled quantity unchanged, with an execution-host regression test covering the boundary.
Market HTTP transport now checks `Content-Length` before reading and streams chunked/unknown
responses through an 8 MiB bounded reader. The provider fixture verifies oversized streams are
rejected without retaining the full payload, closing the previous post-buffer size check.
Read-model recovery now rejects projection files above a 256 MiB total bound before buffering;
the reader also enforces the bound during streaming. A sparse-file regression test confirms
oversized projections fail with `Bounds("projection bytes")` without touching journal state.
News HTTP transport now matches the market adapter: it rejects oversized `Content-Length`
values before buffering and streams unknown-length responses through the 8 MiB bound. A
provider fixture covers both the rejection and exact-bound success cases.
The LLM provider transport now applies the same preflight and bounded streaming pattern
to its 16 MiB response limit. A core fixture covers oversized and exact-bound streams,
preventing provider responses from allocating beyond the declared safety envelope.
Market and news `HttpRequest` debug implementations now expose method, URL, and header
names only; header values are redacted. Regression tests prove API-key-like values do not
appear in formatted diagnostics.
News core validation now bounds each canonical item field (including 16 KiB titles,
128 KiB summaries, and 256 symbols of at most 32 bytes) before insertion. Tests cover
oversized title, summary, and symbol collections, preventing a bounded item count from
becoming an unbounded memory surface.
Versioned news insertion now treats a canonical-URL collision with a different article
ID as an idempotent duplicate before mutating indexes. A regression fixture confirms the
existing article and URL mapping remain intact.
News validation now requires canonical links to use HTTPS with a non-empty authority and
rejects whitespace-containing targets. Tests cover HTTP and malformed HTTPS links before
they can reach storage or the desktop external-link action.
`NewsStore::with_capacity` now clamps requested retention to the hard 100,000-item maximum
while preserving smaller capacities. A regression test verifies both upper and lower bounds,
preventing configuration or caller mistakes from creating unbounded news retention.

The chart crosshair now tracks both axes: pointer movement updates a cursor-anchored
vertical candle snap and a horizontal price-plane guide, while the existing monospace
OHLC/time readout remains stable. Pointer-leave, double-click reset, and gesture cleanup
clear both coordinates, and the UI contract test requires the horizontal guide and state
marker so a future refactor cannot silently regress the second crosshair axis.

The workstation theme now uses the UI.md canonical near-black surfaces and semantic
accent values (`#16c784`, `#ea3943`, `#f0a63f`) through shared tokens. Global controls
use the documented 2px tactile radius, while chart and message surfaces remain square;
the contract test checks the exact semantic values and shared control-radius rule.

NewsAPI and Yahoo news poll cadences are now first-class `.cfg` settings
(`news.newsapi_poll_ms` and `news.yahoo_poll_ms`) with explicit runtime bounds of
1–300 seconds and 5–300 seconds respectively. The bridge resolves these settings
before environment fallbacks, and the CFG Generator exposes and validates both fields;
bridge and UI tests cover typed settings precedence and generated-key coverage.

Numeric environment fallbacks used by the desktop bridge no longer silently become
defaults when set to malformed text. An absent variable still selects the documented
default; a present but invalid value returns a startup error. The pure fallback parser
has regression coverage, and the full gate verifies the behavior across the bridge.

String fallbacks follow the same rule: an explicitly present but blank environment
value now fails with a named configuration error instead of silently selecting a
default URL. Missing variables alone select defaults; typed `.cfg` strings remain
authoritative. Bridge tests cover blank and absent fallback values, and the repository
gate passes with this startup contract enforced.

The IBKR Client Portal transport now performs a `Content-Length` preflight and uses a
4 MiB streaming reader for unknown-length responses. Oversized responses are rejected
before allocation or JSON parsing, while exact-bound responses remain accepted. The
adapter fixture covers both paths and the strict workspace gate passes with warnings
denied, so the certified broker path no longer differs from market/news/LLM limits.

IBKR request and Client Portal base URLs now require a bounded (2,048-byte),
whitespace-free HTTPS URL with a non-empty authority; embedded user-info is rejected,
and account identifiers are bounded to 128 bytes. Validation occurs before request
construction, with fixtures for HTTP, missing-authority, whitespace, and oversized URL
inputs. This prevents malformed endpoint configuration from reaching the broker client.

Chart pointer cancellation now clears both snapped candle and horizontal-guide state in
addition to removing transient pan/pinch transforms. A contract test locks this cleanup
path, preventing a canceled touch or lost pointer capture from leaving stale market
coordinates rendered over a live chart.

Journal recovery now enforces a 512 MiB total segment bound before buffering and again
while streaming. Oversized sparse segments return `JournalError::BoundsExceeded` and
leave the journal untouched; a regression fixture verifies the preflight size path.
This closes the last direct whole-file recovery read found by the workspace transport
and persistence audit.

Journal seal verification, backup, and restore now use the same bounded byte loader as
recovery instead of `fs::read`. A journal that is oversized at any operational entry
point therefore fails with the typed bounds error before hashing or temporary-backup
publication; existing atomic backup/restore tests continue to pass.

Experiment bundle publication and verification now enforce a 64 MiB immutable artifact
bound. Verification checks metadata size and streams through the bound before hashing;
oversized sparse bundles return `BundleError::TooLarge` without accepting their digest.
Publication rejects oversized canonical manifests before creating a temporary file, and
the registry fixture covers the oversized verification path.

Journal sealing now uses the bounded journal loader, and seal sidecars are streamed
through a 256-byte limit before digest parsing. Seal, backup, restore, and recovery
therefore share explicit input bounds; journal tests and the full gate pass with these
limits enforced.

Experiment bundle validation now applies field and collection bounds before canonical
serialization (4 KiB scalar fields, 2,048 map entries, 512 command arguments, and
4,096 artifacts). This prevents oversized metadata from allocating memory before the
64 MiB serialized-bundle guard; a regression fixture proves rejection occurs at
validation time.

Bundle digest sidecars are now read through a 256-byte bounded reader instead of an
unbounded text read. Oversized or invalid UTF-8 sidecars are treated as corruption
without changing the content-addressed bundle, and the immutability fixture covers the
oversized sidecar path.

Experiment bundle manifest fields now reject control characters before serialization,
in addition to their byte/count bounds. This prevents newline or carriage-return
injection from creating ambiguous line-oriented records; regression tests cover both
run identifiers and command arguments.

Portfolio and Positions P/L displays now encode direction textually (`+`, `−`, or
neutral) in addition to semantic color, with explicit accessible gain/loss labels.
This satisfies UI.md's colorblind rule for live P/L values; the workstation contract
test and production bundle verify the rendering path.

Alert cards and the expandable Message Station now render explicit severity labels
(`Info`, `Success`, `Warning`, `Critical`) alongside the semantic dot color. Severity
is therefore never color-only, including acknowledged history rows; the UI contract
test and production build cover the label path.

Candlestick SVG output now wraps each candle in an escaped, bounded Up/Down label and
OHLC `<title>`, while retaining semantic gain/loss colors. Screen readers and tooltips
can identify candle direction without hue, and the chart-source contract test covers
the renderer path.

Metric overlays now reject non-finite scores and invalid timestamps before calculating
SVG coordinates. This prevents `NaN`/`Infinity` geometry from corrupting a live chart
when an optional metric provider emits malformed data; the renderer contract test
requires both finite-score and timestamp guards.

Chart SVG rendering now caps each frame at a deterministic 4,096-candle window even
though the underlying bounded candle store retains up to 20,000 points for navigation
and recovery. The renderer keeps the requested window's newest points, preventing
pathological DOM/SVG work from violating the workstation frame budget; the chart-source
contract test locks the cap.

The canonical repository gate now invokes `npm --prefix ui run build` after UI tests
and source checks. A green `scripts/check.sh` therefore proves the production Vite
bundle compiles, not only that source-contract tests pass; the full gate passes with
the new build step enabled.

The workstation theme now provides a shared 2px `:focus-visible` ring for buttons,
inputs, selects, and textareas using the focus-border token. Keyboard navigation keeps
visible state against dark surfaces without changing pointer-only styling; the UI
contract test and production build cover the focus rule.

The collapsed Message Station status control now exposes a polite live-region label
(`Message station: …`) containing pending/critical/warning counts. Screen readers can
follow non-blocking status changes without opening the history panel; the workstation
contract test and production build cover the live-region semantics.

The requirements checker now enforces Appendix-B traceability in both directions: every
acceptance checkbox must have exactly one matrix row, stale rows fail validation, checked
items cannot remain `planned`, and completed items cannot cite `AGENTS.md` as their sole
evidence. This makes the completion ledger mechanically auditable and prevents a status
edit from masquerading as production verification. `scripts/check_requirements.py` and
the full repository gate pass with the current 50 planned acceptance rows.

Desktop control-plane cadence is now configuration-driven. Reconciliation polling and
webhook delivery timeout/poll intervals are read from bounded `.cfg` keys
(`reconciliation.poll_ms`, `alerts.webhook_timeout_ms`, and `alerts.webhook_poll_ms`)
with strict environment fallbacks and range checks; hard-coded sleeps remain only as
defaults. The CFG Generator exposes and validates all three values, preserving atomic
reload semantics. Desktop-bridge tests and UI checks pass for this path.

News provider transport timeout and retry policy are also now operational settings:
`news.http_timeout_ms`, `news.max_retries`, `news.retry_base_ms`, and
`news.retry_max_ms`. Rust validates retry ordering, attempt limits, and timeout bounds
before either NewsAPI or Yahoo workers start; the generator emits and validates the same
constraints. This removes another production tuning dependency on recompilation while
keeping provider request and retry limits finite. Desktop-bridge and UI checks pass.

Regression tests now exercise the configuration boundaries directly: retry attempts are
limited to the documented finite range, retry maximum delay must not precede its base
delay, and reconciliation/webhook settings reject values outside their operational
windows. These tests cover invalid `.cfg` values before any worker thread is started.

The checked-in `config/example.cfg` now documents every operational key consumed by the
desktop bridge, including provider retry/timeout and control-plane cadence settings;
credentials remain environment-injected. `cfg-core` regression coverage asserts the
example parses into the expected typed values, preventing documentation drift from
silently producing a different startup configuration.

Journal append now enforces the same 512 MiB segment bound used during recovery: the
writer computes the projected framed size under the file mutex and rejects an append
before writing when it would exceed the bound. This prevents a healthy process from
creating a segment that a subsequent restart cannot safely recover; overflow is reported
as the typed `JournalError::BoundsExceeded` failure.

Execution timing admission now rejects non-positive arrival/mid prices and negative
quoted spreads both when a decision is recorded and when timing is restored from the
journal. Timestamp ordering checks remain enforced, so TCA cannot publish mathematically
invalid source measurements after restart or from a malformed market reference.

Read-model rebuild and append paths now enforce the 256 MiB total projection bound before
writing each frame, not only when a projection is later read. The atomic temporary file
therefore cannot grow beyond the recovery limit during a large rebuild, and incremental
updates fail before appending an over-limit frame. Existing sparse-file and backup/restore
tests plus the complete repository gate pass.

The workstation contract test now explicitly covers every newly configurable provider and
control-plane key, ensuring the CFG Generator cannot silently drop those fields during a
frontend refactor. This complements the Rust typed-settings tests and keeps UI generation
and bridge consumption mechanically aligned.

Yahoo market adapters now consume bounded typed settings for base URL, history interval and
range, interval cadence, price scale, history/quote polling, and HTTP timeout. Invalid
values fail the adapter startup path instead of being silently coerced or clamped; the
existing environment variables remain compatibility fallbacks. The canonical example CFG
lists these keys for deployment-owned tuning.

The CFG Generator now exposes Yahoo base URL, interval, and range as editable string fields
with HTTPS, length, and token-shape validation, alongside the numeric Yahoo controls. The
generated values are merged as quoted CFG strings, so operators can tune the adapter from
the workstation without hand-editing configuration; UI contract tests cover the controls.

The checked-in CFG parser test also asserts the Yahoo price-scale value from the example,
so the market adapter’s integer conversion contract is covered at the configuration-file
boundary rather than only by adapter construction tests.

The workstation now has explicit responsive breakpoints: at 1200px the right dock moves
below the main grid, and at 760px the tools rail collapses, panels become a single-column
scrollable workspace, and the top bar wraps without clipping controls. Contract tests lock
both media queries so desktop layout changes remain verifiable against UI.md’s compact
workstation requirements.

The Python worker sandbox now parses CPU and address-space limits through a shared bounded
integer helper. Missing values use documented defaults; malformed, negative, or oversized
environment values raise before resource limits are applied, while unsupported platform
resource APIs remain a narrowly handled portability case. Python contract tests cover
default, malformed, and out-of-range inputs, and the full gate passes.

Desktop bridge string settings now share a 2,048-byte maximum across `.cfg` and environment
fallbacks. Oversized values fail before provider URLs, executable paths, or worker arguments
are constructed; empty values remain errors. Regression tests cover oversized environment
input, and the complete gate passes.

README setup guidance now accurately states that Python isolation and Yahoo adapter controls
are available in the CFG Generator, while still supporting direct file edits for deployment
automation. This removes stale operator guidance after the generator expansion.

The CFG Generator now renders Python CPU and memory budget inputs and merges them into the
same generated configuration payload consumed by the bridge. Values are validated against
the worker sandbox’s 1–86,400 second and 64 MiB–8 GiB bounds before generation, closing the
last setup-path gap for worker isolation controls.

The workstation contract test now pins the Python resource keys and both generator control
selectors, preventing a frontend refactor from silently removing worker-isolation setup
without failing CI.

Every rendered workstation panel now has a stable `panel-<id>` DOM identifier. This gives
accessible navigation controls a deterministic target and provides a stable anchor for
future Tauri end-to-end tests without coupling presentation IDs to journal or trading state.

README setup guidance now names the `.cfg` keys and exact Python CPU/memory bounds, while
retaining the environment fallback names for existing deployments. This removes ambiguity
between the documented setup path and the Rust launcher/sandbox behavior; the documentation
gate and full repository gate pass.

The UI now honors `prefers-reduced-motion: reduce`, disabling transitions/animations and
smooth scrolling while preserving layout and interaction. A workstation contract assertion
locks the media query, and the production bundle includes the accessibility fallback.

Right-dock tabs now implement an accessible roving-tabindex pattern: only the selected tab
is in the tab sequence, and Left/Right/Home/End move focus deterministically while selecting
the destination panel. This preserves pointer behavior and makes dock navigation usable
without a mouse; UI tests cover the keyboard handlers.

Primary workspace tabs now use the same roving-tabindex and arrow/Home/End keyboard model,
with an explicit `tablist`/`tab` semantic structure. Switching workspaces remains the same
validated presentation-only operation, while keyboard focus and selection stay synchronized.

Python package workers now receive their executable, work directory, discovery roots,
CPU budget, and memory budget through typed startup settings. CPU and memory values are
bounded before any worker command is registered; malformed or out-of-range values fail
startup rather than silently falling back. The deployment example documents these keys,
while secrets and process-environment compatibility remain unchanged.

Desktop bridge regression coverage now verifies that an oversized `.cfg` string value is
rejected before use, complementing the environment-fallback bound test. The bridge test
suite passes all nine tests, proving the shared 2,048-byte string cap is enforced on both
configuration ingress paths.

LLM provider contract tests now exercise HTTP classification for rate limits (including
`Retry-After` conversion to milliseconds), authentication rejection, and server failures.
The `insider-llm-core` suite passes all nine tests, providing executable evidence that
transient/provider errors remain typed and cannot be mistaken for valid model output.

The responsive workstation breakpoint now explicitly clears the desktop `body` minimum
width at 760px. This makes the documented single-column mobile layout reachable on narrow
windows instead of leaving a hidden horizontal overflow constraint; the UI contract suite
asserts both the breakpoint and the override.

Core risk sizing limits are now loaded from typed CFG settings (`risk.max_position_ticks`
and `risk.max_gross_notional_ticks`) before the engine is opened, with environment variables
retained only as compatibility fallbacks. The CFG Generator and example configuration expose
both bounded values, and bridge tests verify CFG precedence plus rejection of invalid types.

IBKR transport timeout and quote-poll cadence now follow bounded typed settings
(`broker.ibkr_timeout_ms` and `broker.ibkr_market_poll_ms`) while broker mode, account, and
credentials remain deployment-owned environment inputs. The CFG Generator and example file
expose these controls; bridge tests verify the configured values are selected and bounded
before an IBKR transport or poller is constructed.

The generator field registry now includes both IBKR keys, so configurations that do not yet
contain them receive deterministic defaults when operators merge setup values. This prevents
the controls from appearing editable while silently omitting them from generated `.cfg` files.

Broker selection is now also a typed setting (`broker.mode`) consumed before broker
construction. The CFG Generator exposes an explicit Paper/IBKR selector and emits the quoted
setting, while `IT_BROKER` remains a compatibility fallback; unsupported modes still fail
closed through the existing broker match. Bridge and UI contract tests cover the selector and
precedence path.

Reference-strategy enablement now uses a typed boolean setting (`strategy.reference_enabled`)
before registration, with strict boolean parsing and the legacy environment variable retained
as fallback. The CFG Generator exposes the switch and the example file documents its default;
bridge tests cover typed precedence and reject stringly-typed values instead of coercing them.

Reference strategy identity and behavior parameters now use typed CFG settings: strategy and
metric IDs, entry/exit thresholds, quantity, horizon, and TTL. Numeric values are finite and
bounded before registration; the generator exposes controls and emits all keys, while legacy
environment variables remain fallbacks. Bridge tests verify typed threshold precedence.

Reference metric registration now follows the same typed configuration boundary: EWMA ID,
lambda/TTL, SMA ID/window, shared metric TTL, spread ID, and imbalance ID are validated before
metric admission. The CFG Generator and example file expose the tunable numeric controls and
stable IDs; invalid windows, non-finite lambdas, and wrong scalar types fail closed rather than
silently using defaults.

Context-embedding setup now reads `embeddings.model`, `embeddings.model_version`, and bounded
`embeddings.dimensions` from typed CFG settings, with environment fallbacks retained. The
feature remains opt-in: no embedding keys means no provider setup, while partial or invalid
configuration fails before graph services are configured.

The CFG Generator now exposes an explicit context-embeddings opt-in, model, version, and
dimension controls. Disabling the option removes all persisted embedding keys before merge,
preventing stale configuration from silently re-enabling the provider; enabled values are
validated against the bridge bounds and emitted as typed CFG scalars.

The CFG merge primitive now resolves configuration keys to their field aliases correctly,
preserves existing values, quotes newly emitted string settings, and splits/rejoins real
lines. This fixes silent `undefined` writes and malformed generated strings; contract tests
pin the merge helpers and the full UI build verifies the corrected TypeScript source.

NewsAPI base URL and endpoint are now included in the checked-in example configuration and
the generator contract, matching the bridge’s typed settings path. Operators can select
`everything` or `top-headlines` without hand-editing provider setup, while credentials remain
outside CFG and injected through the deployment secret boundary.

The configuration merge implementation was corrected to resolve canonical keys through their
field aliases, preserve existing lines, quote new string values, and split on actual newline
characters. This removes a silent generator failure mode where edits could be ignored or
serialized as `undefined`; the UI source contract pins the helper path and the production
bundle compiles it.

CFG numeric parsing now accepts an optional leading minus sign, required for valid negative
reference thresholds. The generator no longer silently replaces those bounded values with
defaults; its signed-literal grammar is covered by the UI contract test.

All broker-mode consumers now resolve `broker.mode` through the same typed settings snapshot:
broker construction, demo instrument venue/provider identity, and Yahoo live-mark safety
checks no longer read `IT_BROKER` independently. This prevents CFG/environment disagreement
from creating mixed paper/IBKR state; the desktop bridge suite and full gate validate the
aligned startup path.

Typed configuration fallbacks now distinguish missing environment variables from malformed
non-UTF-8 values. Numeric, boolean, floating-point, and string helpers return a startup error
for invalid environment encoding instead of silently selecting defaults, preserving fail-closed
configuration semantics across supported deployment paths.

IBKR price normalization now uses bounded `broker.ibkr_price_scale` CFG input instead of
silently defaulting when `IT_IBKR_PRICE_SCALE` is malformed. The generator and example expose
the scale, while the compatibility environment path still participates through the same typed
validator.

IBKR’s base URL now uses the bounded typed `broker.ibkr_base_url` setting and is exposed in
the CFG Generator and example file. The adapter still requires HTTPS and keeps account IDs and
credentials outside configuration, so endpoint tuning is inspectable without weakening the
secret boundary.

Reference-threshold CFG parsing now accepts bounded integer literals (`0`/`1`/`-1`) as well as
floating-point values, matching generator output for exact boundary values. Conversion uses a
string round-trip to avoid precision-loss casts; a regression test proves an integer zero exits
the parser as `0.0` rather than failing startup.

IBKR conid parsing now distinguishes an absent optional poll configuration from malformed input:
an explicitly supplied non-integer or non-positive `IT_IBKR_CONID` fails broker startup instead
of silently disabling the market poller. This preserves visible failure semantics for missing
market data while allowing deployments that intentionally omit quote polling.

News provider queries now accept optional typed CFG keys (`news.newsapi_query` and
`news.yahoo_query`) with environment/CLI fallbacks. The generator removes stale query keys when
left blank, preventing an old symbol filter from silently persisting; provider credentials and
the required-query check remain unchanged.

Optional query keys are deliberately excluded from the generator’s required-field registry, so
blank optional inputs are not re-emitted as empty strings that would fail provider startup.
Nonblank values are still merged explicitly, and stale values are removed before merge when an
operator clears a field.

Bridge regression coverage now exercises optional query precedence, absent-query behavior, and
wrong scalar rejection. The test proves CFG query values are returned as authoritative inputs
without coercing malformed configuration into a provider request.

The absent-query regression uses a test-only environment variable name that is not part of the
production configuration surface, eliminating dependence on a developer machine's exported
provider variables while retaining explicit CFG-precedence and type-rejection assertions.

Verification after this change: `cargo test -p insider-desktop-bridge` passed all 12 bridge tests,
`npm test` passed all 3 UI contract tests, and `./scripts/check.sh` completed with exit code 0.

The CFG Generator now exposes the optional `alerts.webhook_url` endpoint used by the engine's
allowlisted webhook channel. The field is not part of the required/default registry: blank input
removes the persisted key, while nonblank input must parse as an HTTPS URL with a nonempty host
and a maximum of 2048 bytes. The generated CFG therefore cannot accidentally enable an empty or
insecure webhook destination, and operators can configure the complete alert route from the UI.
The UI contract test asserts the control, key, and URL validation path are present.

Alert routing capacity is now configuration-driven: `alerts.cooldown_ms` (0..86,400,000 ms)
controls dedupe suppression and `alerts.max_pending` (1..1,000,000) controls the bounded pending
delivery queue. The engine validates both as integer CFG values before constructing the router and
retains the safe defaults of 60,000 ms and 4,096 entries when omitted. The CFG Generator exposes
both values with matching bounds, and `config/example.cfg` documents the production keys. Engine
regression coverage verifies defaults, valid overrides, zero-capacity rejection, and wrong-type
rejection.

Configuration reload now validates alert routing bounds before publication and holds the alert
router lock through the atomic config update. It preflights the new capacity against currently
pending deliveries, then applies cooldown/capacity without dropping queued work; an undersized
capacity or poisoned router rejects the reload. This keeps live reload behavior consistent with
startup validation and prevents a configuration snapshot from claiming limits the runtime could
not safely enforce.

Supervisor restart policy is now explicitly configuration-backed: `supervisor.max_failures`,
`supervisor.window_ns`, `supervisor.initial_backoff_ns`, `supervisor.max_backoff_ns`, and
`supervisor.jitter_bps` are typed integer settings with bounded ranges and enforced
initial/max-backoff ordering. Startup constructs the supervisor from these values; reload validates
them but rejects policy changes as restart-required, preventing a published CFG snapshot from
silently disagreeing with the live failure-isolation policy. The CFG Generator and example config
expose the same controls, and engine tests cover defaults, valid overrides, ordering, and types.

The supervisor policy is intentionally restart-required on reload because the supervisor owns
component backoff state initialized from its policy; accepting a changed snapshot without
rebuilding that state would be misleading. Reload still validates every supplied policy value
before publishing, while startup applies the complete typed policy. This boundary is covered by
the same strict clippy/test/full-gate workflow used for execution-sensitive changes.

The required Logs/Trace workstation surface is now labeled `Logs / Trace` in the rendered panel
header and retains the existing TraceId query plus redacted-export actions. This makes the
operational surface discoverable without inventing a second non-authoritative log store; message
history remains in the Message Station and causal reconstruction remains journal-backed.

Webhook delivery now consumes the merged CFG `alerts.webhook_url` value directly. The desktop
worker no longer requires or re-reads the legacy environment variable, so Generator-created
configurations actually deliver pending alerts. Redirects are disabled on the HTTP client to keep
the exact HTTPS allowlist effective across requests; successful responses acknowledge only the
webhook channel, while in-app alerts remain operator-visible until acknowledged.

Bridge-level regression coverage validates the merged webhook setting independently of engine
construction: omission disables the worker, HTTPS localhost endpoints are accepted, and HTTP
endpoints are rejected. This closes the path where a malformed or CFG-only endpoint could be
silently ignored by the desktop delivery loop.

AI Analyst requests now use the configured `llm.model` value from the authoritative engine CFG
snapshot instead of a UI hard-coded model name. The CFG Generator exposes a bounded non-empty
model field, and `config/example.cfg` documents the default. This keeps request provenance aligned
with the deployment-selected provider model while preserving the existing prompt-version and
schema validation boundaries.

All production outbound HTTP transports now disable automatic redirects: market data, news,
LLM, IBKR, and alert webhook clients use `Policy::none()`. Requests therefore remain bound to
the explicitly configured HTTPS endpoint, avoiding cross-host redirect surprises and accidental
credential forwarding. Existing provider URL, body-bound, and redaction tests remain green under
the full repository gate.

Webhook endpoint validation now rejects embedded URL userinfo (for example,
`https://user:password@host/path`) in both the engine and desktop bridge. This prevents operators
from placing credentials in a URL that could be persisted, logged, or transmitted as request
metadata. HTTPS, non-empty authority, whitespace, and 2,048-byte bounds remain enforced; bridge
and engine regression tests cover the rejection consistently.

The Analyst prompt identifier is now configuration-backed through `llm.prompt_version` alongside
`llm.model`. The Generator validates both as bounded non-empty strings, the example CFG documents
their defaults, and the request builder uses the snapshot values. This prevents a deployment from
calling one prompt while journaling or displaying another version label.

The UI contract suite now guards this provenance boundary directly: it requires both Analyst
identity fields to be built with `configStringValue(configSnapshot.cfg_text, ...)` and rejects
literal model/prompt assignments. This turns the configuration requirement into a regression
check rather than relying solely on code review.

Desktop startup now validates explicitly present `llm.model` and `llm.prompt_version` settings
as bounded non-empty strings. Omitted keys retain the documented fallback behavior, but empty or
wrong-typed CFG values fail before service assembly instead of being hidden by UI defaults. Bridge
tests cover omission, valid values, whitespace-only prompt versions, and wrong scalar types.

The CFG Generator contract now verifies the risk-control descriptor itself: the nine normative
risk settings are present exactly once and duplicate descriptor keys fail `npm test`. This guards
against duplicate UI inputs producing ambiguous generated configuration and complements the
engine's duplicate-key rejection and atomic reload validation.

Chart rendering now includes a validated OHLC-bars mode in addition to candles, line, and area.
Bars render high/low stems with distinct open/close ticks, direction labels, escaped timestamps,
and the same bounded candle window and overlays as other modes. The renderer contract test checks
the mode union, bar direction markup, and dispatch branch; malformed persisted modes still fall
back to candles. Both the chart controls and searchable Settings chart-style control expose the
same bars mode and route it through the existing validated preference listener. The Settings
appearance control renders the OHLC-bars option directly in its initial markup, so persisted bar
preferences are selectable on first paint without depending on post-render DOM insertion.

The Tauri desktop bridge now stamps every IPC command with a bounded Unix-epoch nanosecond
`issued_wall_ns` value instead of the previous sentinel zero. Monotonic runtime time remains the
authority for deadlines, while the wall timestamp provides an auditable issuance time required by
the command envelope; pre-epoch clock results fail closed without panicking. A bridge unit test
guards the timestamp helper, and the repository gate remains green. Direct Tauri-target testing is
currently environment-limited because the build host lacks the `javascriptcoregtk-4.1` development
package; this is an infrastructure prerequisite for the desktop CI image, not a runtime fallback.
The shared IPC validator now rejects zero issuance timestamps, with codec-level regression coverage,
so clients other than the desktop bridge cannot silently omit command audit metadata.

Observational UI refresh cadence is now configuration-backed through bounded
`ui.status_poll_ms` (1,000–60,000 ms, default 5,000). The CFG Generator reads, validates, merges,
and documents the value. Provider, supervisor, broker, risk, and strategy diagnostic refreshes use
one self-rescheduling timer based on the authoritative snapshot; in-flight work is awaited before
the next schedule, preventing overlapping poll storms. Alert polling remains independently bounded
at its one-second safety cadence.
The UI contract suite also rejects a regression to fixed five-second diagnostic intervals and
requires the single awaited scheduling path.
README setup guidance now documents the key, bounds, default, and non-overlapping refresh
semantics so deployment operators can configure it without source inspection.

`evidence/requirements.csv` now carries a dedicated `REQ-UI-CONFIG-001` row for the bounded,
configuration-driven UI status scheduler, linked to the executable workstation contract test and
the `verify-ui-config-poll` verification identifier.

The requirements matrix also records `REQ-ALERT-001`, linking the bounded HTTPS/userinfo-safe
alert router and independent channel acknowledgement behavior to its executable crate tests under
`verify-alert-router`.

Alert acknowledgement semantics now preserve channel independence: an operator acknowledgement
removes only the in-app delivery, while queued webhook delivery remains pending until the webhook
worker records channel-specific success. This prevents a UI click from discarding an external
notification that has not yet been delivered. The alerts crate includes a dual-channel regression
test covering both pending retention and subsequent webhook acknowledgement.

The alert router now enforces the 2,048-byte webhook destination bound at its own allowlist
boundary, in addition to bridge and engine URL validation. Oversized HTTPS destinations are
rejected before insertion into the retained allowlist; an alerts-crate regression test covers the
limit directly.

The router-level HTTPS allowlist also rejects URL userinfo, matching the engine and desktop bridge
policy. Credentials embedded in a webhook destination are therefore refused even when callers use
the alerts crate directly; the webhook regression test covers the rejection.

The router-level validator also requires a non-empty URL authority, rejecting malformed forms such
as `https:///missing-authority` before allowlisting. This keeps direct crate callers subject to the
same syntactic endpoint boundary as the desktop and engine paths.

Strict workspace lint verification (`cargo clippy --workspace --all-targets -- -D warnings`) passes
after the IPC and alert-boundary changes, confirming no warning-level regression is being hidden by
the normal test profile.

The complete Rust workspace suite (`cargo test --workspace`) passes all crate unit tests and doc
tests, including engine restart/recovery, replay, risk, execution, provider bounds, IPC, alert,
strategy, autonomy, graph, and desktop-bridge configuration coverage.

CI now installs the Ubuntu WebKit/GTK prerequisites required by Tauri and explicitly runs
`cargo check --manifest-path ui/src-tauri/Cargo.toml --locked`. This moves the desktop compile
check from an undocumented local prerequisite into the reproducible CI contract; the local host
still lacks those packages, so the check is intentionally CI-only until the image is provisioned.
The workflow names both WebKitGTK and JavaScriptCoreGTK development packages explicitly so
pkg-config cannot resolve one half of the Tauri dependency graph by accident.
CI also pins Node.js 22.22.2 before activating npm 12, matching `ui/package.json` and its lockfile
instead of inheriting an undocumented runner runtime.
The repository now declares the same toolchain for local development: `ui/.node-version` contains
`22.22.2`, `ui/package.json` declares `engines.node >=22.22.2 <23`,
`engines.npm >=12.0.2 <13`, and `packageManager: npm@12.0.2`, and README setup commands use
`npm ci` from `ui/`. These are
machine-checkable inputs for release reproducibility rather than an informal documentation claim.
The UI workstation contract test also parses `package.json` and `.node-version` and fails if either
pin or the supported Node range drifts, making the toolchain requirement executable in every UI test
run.
CI now runs `npm ci --prefix ui` after provisioning the pinned runtime, and
`scripts/check_ci_contract.py` verifies the Node/npm pins, lockfile cache path, and locked install
step. A clean runner therefore cannot accidentally rely on a pre-existing `node_modules` tree.
`AGENTS.md` section 51 now makes this a normative CI invariant, so future workflow changes that
remove locked installation or silently inherit a runner toolchain violate the architecture contract.
The CI contract verifier additionally checks that the locked install appears before the repository
gate invocation, preventing a workflow from satisfying the check only textually after verification.

The engine guardrail parser now rejects negative integer values for drawdown, predicted volatility,
participation, and price-deviation limits at the authoritative `.cfg` boundary. This closes the
parity gap where the UI generator rejected invalid values but a hand-edited configuration could
otherwise reach risk evaluation. A regression test covers each affected key.

The UI `safeExternalUrl` boundary now rejects URLs over 2,048 bytes, whitespace, non-HTTPS schemes,
missing authorities, and username/password userinfo before rendering article links. The workstation
contract suite asserts each restriction so provider payloads cannot smuggle credentials into user
navigation.
The desktop LLM base-URL validator and CFG Generator apply the same no-userinfo, non-empty-authority,
whitespace, and size checks to configured LLM, Yahoo, and webhook endpoints; direct `.cfg` startup
and generated configuration therefore share one credential-safe provider boundary.
CFG generation now enforces the same 1 MiB input bound after merging preserved text and generated
settings, failing before the oversized value can be applied or submitted to the bridge.
Both URL and generated-configuration bounds are measured in UTF-8 bytes via `TextEncoder`, matching
Rust `str::len()` and the cfg-core/provider byte windows even for non-ASCII input.
The generator also applies cfg-core's 16 KiB UTF-8 string-value bound to every generated string
field, preventing a value that passes a per-control character limit from being rejected only after
submission.

Added `docs/runbooks/operator-guide.md` with executable installation, locked dependency validation,
paper startup, outage/halt, restart/reconciliation, backup/restore, paper-to-live certification,
and evidence-retention procedures. README now links operators to the runbook; it explicitly keeps
live mode disabled until the required certification evidence exists.
The runbook now creates the deployment `data/` directory explicitly and preflights config, journal,
and socket paths before startup, removing clean-install filesystem assumptions and preventing an
operator from accidentally targeting a non-socket or non-regular shared path.
Configuration bootstrap also refuses to overwrite an existing deployment-owned `.cfg`; replacement
requires an explicit backup and reviewed change, avoiding silent loss of live operational settings.
`scripts/check_runbook.py` now makes those operational requirements executable: it fails CI if any
required setup, paper-safety, outage, reconciliation, backup/restore, live-change, or evidence
retention section is removed from the runbook.
The incident template is now a structured operator record covering identity, TraceIds, immediate
risk action, diagnosis, journal/provider health, broker reconciliation, approval, and evidence
hashes. The runbook verifier checks that these closure-critical sections remain present.
The operator runbook now includes credential rotation: risk restriction, secret-manager-only
replacement, authenticated provider health verification, old-key revocation, and post-rotation
reconciliation. It explicitly forbids placing secrets in `.cfg` or validating revocation with a live
order.
`AGENTS.md` section 49 now records the URL boundary as a normative security invariant covering
storage, navigation, and request dispatch, with the desktop and UI tests serving as executable proof.
The engine webhook parser now additionally rejects an empty HTTPS authority (for example,
`https:///missing-authority`), with a regression test. Direct engine startup therefore cannot admit
the malformed endpoint that the bridge and UI already reject.
`evidence/requirements.csv` now traces this provider URL invariant as `REQ-PROVIDER-URL-001` with a
dedicated verification identifier, making the security boundary discoverable to automated release
audits.
The CFG Generator now rejects control characters across every generated string field before merge,
preventing pasted terminal/control bytes from producing parser-invalid configuration or ambiguous
operator-visible settings.
The desktop bridge CLI was smoke-tested from a clean build: incomplete invocations return explicit
usage errors, and a paper-mode `serve` command with the example `.cfg`, temporary journal, socket,
and account identity remains running through the startup window without crashing.
News provider constructors now enforce one shared HTTPS URL boundary for NewsAPI, Yahoo Finance,
and RSS adapters: 2,048-byte maximum, no whitespace, non-empty authority, no userinfo, and
successful URL parsing. This closes the gap where a malformed `.cfg` endpoint could pass a simple
`https://` prefix check. `insider-news-providers` regression tests cover malformed authority,
credential-bearing, non-HTTPS, and whitespace URLs; traceability is recorded as
`REQ-NEWS-URL-001`.
The market provider boundary received the same treatment: Yahoo chart/quote constructors and the
production market HTTP transport now reject oversized, whitespace-containing, authority-less, or
credential-bearing URLs instead of relying on a string prefix. Regression tests cover those cases
and traceability is recorded as `REQ-MARKET-URL-001`.
The provider URL regression fixtures now include explicit over-2,048-byte cases for both news and
market adapters, ensuring the byte ceiling is exercised rather than inferred from implementation.
Provider manifest validation now applies the normative 64-byte maximum to `provider_id` (while
retaining the broader bound for other manifest fields), rejecting empty or oversized identities
before registry indexing. Regression tests cover the exact 64/65-byte boundary and whitespace-only
IDs; traceability is recorded as `REQ-PROVIDER-ID-001`.
The identity tests also exercise UTF-8 byte semantics (`é` repeated 32 versus 33 times), proving
the 64-byte limit is not incorrectly implemented as a character-count limit.
Canonical `NewsItem` validation now rejects username/password userinfo in HTTPS authorities before
storage and indexing, matching provider URL policy. A regression fixture covers the credential
case and traceability is recorded as `REQ-NEWS-ITEM-URL-001`.
The same canonical-news fixture now covers an oversized article URL and asserts the typed
`FieldTooLarge("canonical_url")` error, proving the storage boundary enforces the byte limit.
The canonical `NewsItem` URL constant is now 2,048 bytes (previously 8,192), aligning storage
retention with provider/navigation security policy instead of relying on downstream checks.
Desktop startup now validates the final merged webhook setting after CFG and environment fallback
resolution, before the engine or delivery worker starts. This prevents an invalid environment URL
from surviving initial settings load; evidence is tracked as `REQ-WEBHOOK-STARTUP-001`.
UI control radii for keyboard hints, right-dock tabs, and panel-link selectors now consume the
shared `--radius-control` token (2 px), enforcing UI.md's sharp terminal geometry consistently.
Contract tests pin these selectors to the token so future CSS changes cannot silently reintroduce
larger rounded-card styling.
AI Analyst context chips now use the same 2 px control radius instead of a 999 px pill, keeping
tags consistent with UI.md's sharp terminal geometry while retaining their semantic grouping.
The CFG Generator now applies the same parsed HTTPS/authority/no-userinfo boundary to NewsAPI
and IBKR base URLs before merge, rather than relying on a prefix check. This keeps setup-time
feedback aligned with the backend provider constructors and prevents malformed endpoints from
being generated into `.cfg` files.
Yahoo multi-symbol subscriptions now use typed `market.yahoo_symbols` when present in `.cfg`, with
`IT_YAHOO_SYMBOLS` retained only as the fallback. The CFG Generator, example configuration, and
README expose the bounded `SYMBOL=INSTRUMENT_ID` format, removing another operational setting from
environment-only configuration.
`AGENTS.md` section 30 now makes this file-first provider-subscription behavior normative, including
the 128-entry bound and the environment-fallback rule.
Desktop-bridge regression coverage now verifies CFG precedence and the 2,048-byte bound for
`market.yahoo_symbols`, in addition to exercising the existing environment fallback path.
The CFG Generator now validates Yahoo subscription entries before merge: at most 128 non-empty
`SYMBOL=INSTRUMENT_ID` records, strict bounded symbol/positive-ID syntax, and uniqueness of both
symbols and canonical instrument IDs. Invalid entries produce an actionable generator error
instead of being silently discarded by the startup adapter; the engine remains the final authority.
The workstation contract suite asserts this validation path and its user-visible error messages.
Chart rendering now records a bounded presentation-only render duration using the browser
high-resolution clock and exposes it as an accessible diagnostic on the chart surface. This
provides an objective per-render measurement for the UI frame budget without moving market data
or trading state into telemetry, and the contract suite requires the instrumentation markers.
The UI contract also rejects any `backdrop-filter` declaration on `.chart-surface`, enforcing
UI.md's rule that rapidly updating price surfaces remain opaque while blur is reserved for
composited rails and overlays.
Candles and OHLC bars now include compact visible ▲/▼ direction glyphs in addition to semantic
gain/loss color, so chart direction remains discernible under the colorblind palette and when
color perception is unavailable. The glyphs are bounded to each rendered candle and hidden from
duplicate screen-reader announcements because the enclosing candle already carries its complete
accessible OHLC label.
Glyphs are emitted only when the computed candle width is at least 5 CSS pixels; dense windows
retain the semantic direction labels while avoiding thousands of unreadable text nodes and
preserving the SVG render budget.
The chart renderer now defensively filters snapshots through the same strict candle validator
used by the bounded series store before calculating scales. Malformed provider/UI snapshots can
therefore produce an empty/recovery surface but cannot inject non-finite coordinates into SVG.
Timeframe resampling applies that validation before bucketing as well, so an invalid candle cannot
contaminate a valid bucket's high/low/close/volume aggregates. The 1-minute path follows the same
boundary and returns only validated immutable records.
The validated 1-minute array is explicitly frozen before returning, preventing presentation
callers from mutating a snapshot after validation and keeping parity with aggregated snapshots.
The chart diagnostics now mark renders above 16.67 ms with an explicit “over 60 FPS budget”
warning and amber semantic styling, making frame-budget regressions visible without affecting
execution or throttling authoritative data.
Chart drag inertia now honors `prefers-reduced-motion: reduce`; the final bounded pan commits
immediately with no momentum animation when requested by the operating system, while normal users
retain the documented 350 ms decay behavior.
The live render path now computes the validated timeframe candle window once per render and shares
it with SVG generation, percentage-change display, crosshair lookup, and linked charts. This
removes duplicate resampling work from every market update without introducing another data store.
Added a runtime chart test that compiles the renderer module and verifies malformed candles are
filtered, SVG output contains no non-finite geometry, direction glyphs are emitted, output stays
bounded, and one-minute resampling returns a frozen validated snapshot. `esbuild` is now a direct
locked UI development dependency so this test does not rely on an incidental transitive package.
The runtime suite also renders the maximum 4,096-candle window and asserts SVG output below 2 MiB
and render completion below 500 ms on the test host. This is a deterministic ceiling check for
pathological output growth; the stricter 55 FPS target remains a packaged-reference-profile gate.
The render warning threshold is now named `CHART_FRAME_BUDGET_MS = 1000 / 60` in the application
source, and the UI contract test asserts both the constant and its use. This prevents a duplicated
rounded literal from silently diverging between diagnostics and the documented 60 FPS budget.
NewsAPI top-headlines `country`, `category`, and `sources` filters now use typed CFG keys first,
falling back to their environment variables only when absent. Wrong CFG types fail before adapter
construction, and the example deployment file documents all three bounded filter keys.
The CFG Generator now exposes IBKR account ID, conid, and instrument ID fields. It emits these
identities only when supplied, requires an account ID in IBKR mode, and requires conid/instrument
ID as a complete positive-integer pair for quote polling; partial or malformed values block reload.
IBKR account IDs are additionally constrained to 1–64 ASCII alphanumeric, period, underscore, or
hyphen bytes in both the bridge and generator, with whitespace and oversized identities rejected
before broker transport construction.
The generator also exposes NewsAPI top-headlines country/category/source filters and both explicit
mark-safety booleans. It validates the endpoint/filter contract, serializes the typed keys, and
removes stale optional keys when an operator clears a field; safety switches remain visibly
opt-in and default unchecked.
Python worker network access is now a typed `python.allow_network` CFG control, defaulting to
false and resolved once before metric/strategy worker registration. The resolved policy is injected
into every worker command, preventing per-process environment drift; the generator exposes the
same opt-in switch.
The Python sandbox now centralizes the exact opt-in check in `network_enabled()` and tests absent,
case-mismatched, and exact-`true` values. This verifies that a malformed or omitted injected policy
cannot accidentally enable worker networking.
The Orders panel now provides a bounded “Cancel all working orders” action. It filters only
cancellable lifecycle states, requires explicit confirmation, submits each request through the
existing authenticated/idempotent cancel command, continues after per-order failures, and reports
the completed/total count; it does not mutate local order state outside reconciliation.
The cancellation affordance excludes `cancel_pending` and `replace_pending` transitions, avoiding
duplicate requests while a prior lifecycle mutation is already in flight.
Bulk cancellation now reports both successful and failed request counts (`Cancel partial`), so
partial transport or authorization failures are visible immediately while reconciliation resolves
the final broker lifecycle state.
The bulk-cancel button's accessible name now includes the exact active-order count and is disabled
when that count is zero, exposing scope and availability to assistive technology before activation.
Its result label is also `aria-live="polite"`, ensuring completed and partial cancellation counts
are announced without introducing a competing toast or modal channel.
Individual Cancel and Replace controls now include explicit side/instrument/quantity accessible
names, so repeated actions in a dense order list remain distinguishable without visual row context.
The Positions panel now provides a one-motion `Close` draft action. It creates the exact opposite
market quantity for the current signed position, routes it into the existing Order Ticket, and
leaves preview, risk, confirmation, execution, and reconciliation gates unchanged.
Before drafting, the action now resolves the position symbol through the authenticated instrument
catalog and carries the returned canonical instrument ID; resolution failures leave the ticket
untouched and are surfaced on the action control.
The workstation contract test asserts that catalog resolution occurs textually before the ticket
mutation, preventing a future refactor from reintroducing ticker-as-identity drafts.
Close-position controls now include instrument-specific accessible labels (`Draft close order for
<symbol>`), so keyboard and screen-reader users can distinguish identical actions across positions
without relying on row context or color.
Position symbols are now HTML-escaped in the rendered row as well as the close-action attributes,
preventing provider-supplied display text from becoming executable markup; the workstation contract
test enforces the escaped rendering path.
The compact proposal summary now escapes strategy IDs, symbols, and proposal IDs before rendering,
closing the equivalent provider-data injection path outside the detailed Strategy Inspector.
The draft ticket summary and top-bar workspace/symbol/timeframe labels now escape runtime text as
well, so all newly touched live-data presentation paths use the same HTML safety boundary.
Strategy proposal cards now give each repeated Draft, TWAP, and implementation-shortfall control
an explicit accessible name containing the escaped strategy ID and symbol. The workstation contract
test checks all three template attributes, making the requirement mechanically reviewable even though
the controls are rendered from a dense repeated list.
The compact proposal summary uses the same context-bearing accessible name for its Preview control,
so repeated proposals remain distinguishable in the right-side/summary surfaces as well as the full
Strategy Inspector cards.
Command-palette command IDs and labels, including dynamically created custom-workspace entries, now
pass through `escapeHtml` for both attribute and text contexts. The workstation contract test asserts
the exact template boundary, preventing a custom name from becoming executable markup.
The news workstation now tracks page-load state explicitly: it announces loading, surfaces provider
errors while retaining cached articles, and marks data stale after the bounded freshness interval.
Pagination still passes the authoritative cursor through the wrapper, and the load-more control is
disabled during an in-flight request. Contract tests assert all three state branches and cursor use.
Provider errors additionally expose a Retry action. The failed request's scope, symbol, and cursor
are retained in bounded UI state, so recovery cannot silently restart at a different page or symbol;
the action still routes through the same authenticated provider command.
Initial-page requests now clear the prior feed and freshness timestamp before fetching a changed
symbol, timeframe, or scope; only cursor-bearing requests append. This prevents cross-symbol news
leakage during provider latency and keeps stale indicators tied to the active request context.
Rapid context changes no longer drop a newer initial request while an older page is in flight. The
wrapper retains the latest initial scope/symbol and replays it after completion; cursor requests are
never coalesced, preserving deterministic page order and preventing accidental pagination skips.
Virtualized news rows now give Pin/Unpin and Details controls article-specific accessible names,
including escaped titles. This keeps repeated actions distinguishable to keyboard and screen-reader
users without changing the feed's bounded rendering or provider semantics.
The feed container now exposes `aria-busy` during provider requests, and Retry identifies the active
symbol, making loading and recovery state available without visual inspection.
The News panel now displays a compact provider-health strip sourced from the authenticated status
snapshot (`Unknown`, `Healthy`, `CoolingDown`, `Degraded`, or `Failed`). IDs and health values are
escaped and bounded by the bridge decoder; credentials and raw provider responses are never rendered.
Provider chips follow the UI design tokens and include explicit glyphs alongside health text, so
healthy, degraded, cooling-down, and failed states remain distinguishable without color perception.
The health-class selectors are aligned with the bridge's canonical lowercase wire values, including
the `cooling_down` spelling; a CSS contract test now guards against silent styling drift.
News view controls now use an explicit tab/tabpanel relationship (`aria-controls` and
`aria-labelledby`) for Relevant, All, Watchlist, and Portfolio, preserving the selected view's
context for keyboard and assistive-technology navigation.
The News tablist now uses roving `tabindex` and handles ArrowLeft/ArrowRight/Home/End with focus
movement and the same validated view-change path as pointer activation.
The selected News filter now persists as the bounded presentation-only
`insidertrader.news-view.v1` value. Restore accepts only the four known views and rewrites a safe
default for malformed storage; changing the filter persists immediately without touching runtime
or trading state.
Workspace navigation tabs now identify the stable `workspace-main` controlled region with
`aria-controls`, while preserving the existing selected-state and roving-focus behavior.
The controlled workspace region is now a labelled `tabpanel`, with each validated workspace tab
using a stable escaped ID and the active tab referenced by `aria-labelledby`.
Trading right-dock tabs now identify a stable `right-dock-panel` tabpanel with matching active-tab
labels, preserving accessible context when the dock switches between Positions, Orders, Watchlist,
and Alerts.
The selected Trading right-dock tab now persists through `insidertrader.right-dock-tab.v1`; restore
accepts only Positions, Orders, Watchlist, or Alerts and rewrites a safe default otherwise.
Workspace tab-order restoration now rewrites the validated order immediately, including when the
stored value is malformed or incomplete, eliminating repeated invalid-state parsing on future starts.
The News stale threshold is now read from `ui.news_stale_after_ms` in the authoritative `.cfg`,
bounded to one through sixty minutes with a five-minute fallback. The CFG generator validates and
merges this setting while preserving unrelated configuration keys and comments.
The AI Analyst stale threshold is likewise read from `ui.analyst_stale_after_ms`, bounded to one
through sixty minutes with the same fallback, and generated through the CFG UI rather than a
hard-coded freshness policy.
Alert refresh cadence is now read from `ui.alert_poll_ms` and scheduled through a bounded
recursive timer, so the CFG generator controls alert load without overlapping polls.
Alert polling failures now set an explicit degraded state in the Alerts panel and message flow;
cached alerts remain visible but are labelled potentially stale, so provider failure cannot be
silently mistaken for an empty or safe alert stream.
The operator runbook also includes the exact Debian/Ubuntu WebKitGTK, JavaScriptCore, GTK,
protobuf, and support-library preflight used by CI, making local Tauri installation and release
verification reproducible instead of leaving the desktop dependency step implicit.
Native notification permission denial is now rendered as an explicit Alerts status with a
Message Station fallback; the UI never represents blocked desktop notifications as successfully
configured delivery.
The Alerts panel also derives blocked native-notification permission on startup, so a persisted
preference cannot hide an OS-level denial until the user toggles the control.
The degraded Alerts state now includes an explicit `Retry now` action in addition to the
configured polling retry, allowing incident operators to force an immediate authoritative refresh.
The Chart panel now surfaces non-ready market connection states as an explicit stale-price warning,
including a reminder that order freshness checks remain engine-authoritative; this state is tied to
the runtime snapshot rather than inferred from UI timers.
The CI contract now verifies that Tauri's locked desktop compile and its WebKitGTK/JavaScriptCore
system dependencies remain present after the repository gate, preventing a web-only green build
from being mistaken for desktop verification.
The repository gate now validates `ui/src-tauri/tauri.conf.json` directly, including the packaged
frontend path, product identifier, self-origin CSP directives, usable minimum window bounds, and
enabled bundling before any desktop release evidence is accepted.
The gate now compares active keys in `config/example.cfg` with the UI generator source, making
configuration coverage mechanically checkable and preventing new deployment settings from
silently becoming hand-edit-only.
Dense News and workspace tablists now expose bounded `aria-setsize`/`aria-posinset` metadata, making
their position and total count explicit to assistive technology without changing navigation state.
The workstation contract test now enumerates every required panel and published workspace preset,
ensuring a regression cannot silently remove a UI surface from the shared draggable panel shell.
Chart drawing migration is centralized across startup, symbol, timeframe, and workspace changes:
the bounded v2 envelope is written before each legacy key is removed. The CFG generator now also
round-trips market transport settings and Python executable/worker/package roots, so deployment
operators can change those values without hand-editing configuration.
UI theme tokens now expose the semantic aliases and motion tokens specified by `UI.md`
(`--accent-gain`, `--accent-loss`, `--accent-warn`, `--blur-panel`, and bounded easing values),
with a contract test preventing drift between the design guide and the shipped stylesheet.
Every draggable workstation panel now uses a stable visible-heading relationship via
`aria-labelledby`, eliminating duplicate labels and preserving semantic navigation for assistive
technology and automation.

Verification on 2026-08-26: `./scripts/check.sh` completed successfully, including Rust
format/lint/tests, schema/dependency/license/security/docs/CI/Tauri/CFG/runbook contracts,
Python tests, requirements traceability, UI contract tests, TypeScript checks, and the
production Vite bundle. A local `cargo check --manifest-path ui/src-tauri/Cargo.toml --locked`
remains an environment-bound check: this workstation does not have the CI-provisioned
`webkit2gtk-4.1`/`javascriptcoregtk-4.1` development packages, so no desktop compile result is
claimed from this host. The CI workflow retains the exact package installation and locked Tauri
compile as the authoritative desktop verification path.

The Settings and Command Palette modal paths now set the mounted workspace
`inert` and `aria-hidden` while open. This prevents keyboard, pointer, and assistive
technology interaction with background trading controls; the workstation contract
suite covers both attributes and the Escape dismissal path.

Modal keyboard navigation now also traps `Tab` and `Shift+Tab` within the active
Settings or Command Palette surface. The trap filters disabled and negative-tabindex
controls, wraps focus at both boundaries, and recovers focus into the modal if the
active element leaves it; the workstation contract suite checks the selector and
both wrap conditions.

Opening Settings now immediately focuses its search control after rendering, so
keyboard users never remain on the inert launch button while the modal is active.
The UI contract suite verifies this first-focus handoff.

Workspace duplicate, rename, and delete actions now use an in-app accessible modal
instead of browser `prompt`/`confirm` dialogs. Names are validated against the existing
bounded allowlist and case-insensitive uniqueness rule; deletion remains an explicit
confirmation and only removes presentation persistence before returning to Trading.
The modal shares inert background and focus-trap behavior, with contract coverage for
its role, submit path, delete confirmation, and stylesheet surface.

The workspace lifecycle modal is included in the no-backdrop-filter fallback surface,
ensuring opaque readable contrast on WebViews without composited blur support; the UI
contract suite guards this fallback selector.

Scheduled TWAP proposal execution now uses an in-app confirmation modal instead of a
browser prompt. The modal identifies the proposal, requires the exact `CONFIRM` phrase,
keeps the background inert, and submits only through `submitScheduledProposal` after
the existing engine validation path; invalid phrases remain local errors and cannot
trigger an order. UI contract tests cover the modal schema and phrase gate.

Implementation-Shortfall scheduling now shares the same confirmation boundary; the
modal identifies the selected schedule type and the submit path constructs the matching
typed schedule only after the exact phrase is entered. There is no direct UI path that
can submit IS child orders without confirmation.

The workspace lifecycle dialog now binds its explanatory text through
`aria-describedby="workspace-dialog-description"` in addition to the visible title,
so validation rules and destructive-impact text are announced before controls. The
workstation contract suite checks both the dialog relationship and paragraph ID.

UI interaction feedback now uses the documented short easing tokens: buttons/selects
and text controls transition only border/background/color properties at `--ease-fast`,
while tools-rail expansion uses `--ease-structural`. The existing reduced-motion media
rule overrides both durations, and the UI contract suite verifies all three boundaries.

Metric values and emphasized numeric outputs now explicitly use `--font-mono` with
tabular numerals, while labels retain the body font. This enforces UI.md's stable-width
data presentation rule for live prices, quantities, PnL, and status values; the UI
contract suite checks the selector.

Cancel-all-working-orders now uses an in-app accessible confirmation modal. The modal
states the exact bounded working-order count, requires the literal `CONFIRM` phrase,
keeps the workspace inert, and only then iterates cancellation requests through the
existing idempotent command/reconciliation path. Empty or changed working sets are
recomputed from the authoritative runtime snapshot; per-order failures remain visible
in the existing partial-result status. Contract tests cover the modal, phrase gate, and
keyboard modal boundary.

Order replacement now uses an in-app modal with bounded whole-number quantity and
optional limit-price fields. Invalid values are rejected before any command call, the
workspace is inert while editing, and valid replacements continue through the existing
broker-capability, risk, idempotency, and reconciliation path. The modal reports the
target client order ID and preserves the existing inline success/failure status.

Chart-template save and delete now use a single accessible in-app dialog. Save validates
the documented 1–64-character bounded name before replacing/persisting preferences;
delete requires an explicit confirmation and removes only presentation preferences.
Both operations keep the workspace inert and are covered by UI contract assertions,
eliminating the prior browser prompt and direct-delete path.

Metric and strategy lifecycle transitions now share an in-app confirmation dialog. The
dialog requires the exact `CONFIRM` phrase plus a bounded evidence reference before
calling the respective lifecycle command; the background is inert and failures remain
visible through the trace/message state. This preserves the engine's authorization and
version checks while removing browser-native confirmation/prompt behavior from
promotion workflows.

Model validate/canary operations now collect their required evidence reference in an
accessible in-app modal rather than a browser prompt. The reference is bounded and
validated before `mutateModel` receives it; cancellation and errors are explicit, and
the normal model registry refresh remains authoritative after success.

Autonomous-mode activation now uses an in-app safety modal instead of a browser
confirmation. It explains that policy, portfolio, risk, execution, broker, and
reconciliation gates remain mandatory, requires the exact `CONFIRM` phrase, and only
then calls the normal trading-mode/session path. Cancel and Escape leave the current
mode unchanged; contract tests cover the modal and phrase gate.

News point-in-time retrieval now filters both ranked and directly relevant paginated
queries by `received_at_ms <= as_of`. Pagination's continuation count uses the same
visible set, preventing future-known articles from leaking into historical contexts or
creating invalid cursors. A deterministic news-core regression test covers available
and not-yet-received articles.

The complete-news feed now also exposes `NewsStore::all_page_at(after, limit, as_of)`;
it applies the same receipt-time cutoff and computes cursor continuation from the
filtered set. The existing live `all_page` behavior remains unchanged by delegating
with an unbounded cutoff, while historical callers have an explicit safe API.

Historical article detail now exposes `NewsStore::detail_at(id, as_of)`, selecting the
latest version whose receipt timestamp is visible at the cutoff and filtering related
cluster members by the same rule. This prevents late corrections and future articles
from altering replay-era context; regression tests cover both original and corrected
versions.

Repository onboarding now includes `CONTRIBUTING.md` and `SECURITY.md`. These documents
make the pinned toolchain, full verification gate, CFG-first operational policy, secret
boundary, evidence rules, vulnerability-reporting process, and paper-only safety
expectation explicit for a fresh GitHub checkout.

README onboarding wording for CFG-controlled status, alert, news, and analyst polling
was corrected so each setting has an unambiguous owner and range; this keeps operator
setup instructions consistent with the desktop bridge and generator validation.

A checked-in `Makefile` now provides discoverable aliases for the canonical full gate,
Rust/Python/UI tests, UI production build, and formatting check. The aliases delegate
to existing scripts and cannot bypass the required verification policy; `make paper`
points operators to the explicit CFG/journal/socket paper-start command.

GitHub collaboration scaffolding now includes a pull-request checklist and bug/feature
issue templates. They require CFG ownership, secret hygiene, replay/risk/reconciliation
review, deterministic tests, and objective PLAN evidence before changes are merged.

The contributor shortcut now uses the repository's dependency-free Python `unittest`
command, matching `scripts/check.sh`; a fresh checkout no longer requires an
undeclared pytest installation to run the documented test path.

Repository hygiene is now GitHub-ready: nested Rust/Tauri targets, Python bytecode,
virtual environments, UI dependencies/build output, runtime journals/sockets/backups,
and logs are ignored. The real local baseline commit contains only source,
schemas, fixtures, scripts, and documentation; certification evidence remains
intentionally unclaimed until it can be tied to externally verifiable CI artifacts.

The chart runtime regression benchmark now uses a 2-second runaway-work ceiling for
the maximum 4,096-candle render and explicitly leaves frame-budget monitoring to the
renderer telemetry. This avoids machine-load-dependent false failures while retaining
an objective upper bound on pathological render regressions.

The desktop bridge now supports a bounded `serve --check` preflight. It executes the
same startup configuration, journal recovery, broker/catalog/risk composition, and
provider/package registration path as paper mode, then exits before starting worker
loops or binding the IPC socket. `make paper-check` runs this preflight with a copied
example configuration and private temporary paths, so deployment automation can
objectively verify startup without touching repository data or creating a live
execution endpoint. The target additionally asserts that a journal file exists and
the requested socket path remains absent after the check, making the no-worker/no-IPC
contract directly testable.

The contributor `make paper` target is now a real fail-closed launcher rather than an
instruction-only alias. It requires an existing deployment-owned CFG plus explicit
journal and socket paths (`IT_CONFIG`, `IT_JOURNAL`, `IT_SOCKET`; account defaults to
`IT_ACCOUNT=1`) and passes those values directly to the locked desktop-bridge binary.
An unset path fails before any process or filesystem mutation.

The Unix desktop transport now caps each accepted connection at 256 complete
request/response exchanges. Clients reconnect transparently per command, while a
held connection cannot monopolize the bounded accept loop indefinitely; payload,
authorization, optimistic-concurrency, and idempotency checks remain unchanged.
