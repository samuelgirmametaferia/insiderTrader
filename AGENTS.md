# AGENTS.md — InsiderTrader Technical Architecture and Agent Contract
> Status: normative engineering specification.
> Target: an institutional-grade, deadline-aware trading platform that supports manual, hybrid, and autonomous operation.
> Design rule: InsiderTrader is not a ChatGPT wrapper. LLMs extend a deterministic trading system; they do not replace metrics, strategies, portfolio logic, risk, execution, replay, or observability.
> Primary runtime: Rust. Research/ML: Rust + Python. Workstation: native Rust terminal UI.
## 0. Mission
InsiderTrader is a modular systematic-trading workstation and autonomous trading runtime.
Its architecture has four first-class computational layers: Market State, Metrics, Strategies, and Decision/Execution.
Metrics describe or predict the market.
Strategies decide what should be done using market state, metrics, news/context, or any declared subset of those inputs.
The Decision layer combines strategy proposals with portfolio state, user/autonomy preferences, and LLM analysis.
The deterministic execution stack converts an approved target into broker actions and reconciles the result.
The same application must serve a discretionary trader who wants excellent charts and intelligence and an autonomous operator who wants strategies and agents to act continuously.
The platform optimizes net risk-adjusted performance, research velocity, operational correctness, capital efficiency, execution quality, explainability, and resilience.
It MUST keep historical/replay semantics close to live semantics.
It MUST make every important decision reconstructible from versioned market data, metrics, strategies, context, model configuration, and order state.
## 1. Conceptual model
The canonical hierarchy is:
```text
Market Data + Reference Data + News + Account State
                    ↓
               Feature State
                    ↓
            Metrics / Predictors
                    ↓
              Strategy Modules
                    ↓
       Strategy Coordinator / LLM Layer
                    ↓
             Portfolio Targets
                    ↓
                Risk Engine
                    ↓
             Execution Planner
                    ↓
               Order Gateway
                    ↓
             Broker / Exchange
                    ↓
              Reconciliation
```
A metric is not a strategy.
A strategy is not an order.
An LLM response is not broker state.
The terminal is not the source of truth.
The journal and reconciled runtime state are authoritative for execution state.
## 2. Core definitions
**Metric**: a bounded computation that emits measurements, forecasts, probabilities, scores, uncertainty, or regime estimates.
**Strategy**: a versioned decision module that consumes market/context inputs and emits one or more trade/portfolio proposals.
**Strategy Proposal**: a structured recommendation such as increase/decrease/close exposure, target a position, or do nothing, with horizon, confidence, rationale codes, and requested risk.
**Strategy Coordinator**: central subsystem that collects strategy proposals, resolves conflicts, applies allocation rules, and exposes them to manual or autonomous operation.
**LLM Intelligence Layer**: provider-agnostic model layer for news understanding, chart/context explanation, strategy comparison, research, and autonomous orchestration.
**Autonomous Coordinator**: an LLM-capable control component that may choose, rank, combine, pause, or execute strategy proposals through typed actions.
**Manual Mode**: the system analyzes continuously but the user decides when to submit a trade.
**Autonomous Mode**: configured strategies and/or the Autonomous Coordinator may initiate actions automatically through normal portfolio/risk/execution interfaces.
**Hybrid Mode**: strategy-specific or action-specific automation; some actions are automatic and others require confirmation.
## 3. Non-negotiable architecture rules
- Metrics MUST be independently testable and schedulable.; Strategies MUST live in a first-class `strategies/` tree parallel to `metrics/`.
- Strategies MUST declare exactly which metrics, market streams, news/context streams, and account fields they consume.; Strategies MUST return typed proposals rather than directly writing broker orders.
- The Strategy Coordinator MUST centralize proposal collection, conflict resolution, and strategy-level allocation.; Manual and autonomous modes MUST use the same StrategyProposal objects.
- The LLM layer MUST be provider-agnostic and support OpenAI-style HTTP APIs through configurable base URLs.; The application MUST support both streaming and non-streaming LLM responses.
- LLM output that can affect trading MUST be schema-validated before it becomes an internal action.; Market data, metrics, strategies, risk, and execution MUST continue to function when the LLM provider is unavailable.
- News retrieval MUST be a provider layer, not hard-coded to one vendor.; Yahoo Finance SHOULD be supported as a convenient market/news adapter, but provider failure MUST degrade cleanly.
- The terminal MUST provide mnemonic function navigation, function keys, dense tables, keyboard-only operation, bounded scrolling, automatic refresh, and persisted operator preferences.; Charts MUST provide fast terminal-native price/history views plus metric, strategy, execution, and news annotations where supported. An optional loopback-only browser chart MAY provide a familiar TradingView-style presentation, but MUST remain a bounded projection with no independent trading state or broker connection.
- Manual traders MUST be able to inspect strategies, metrics, news, account state, and risk without invoking autonomous trading.; Autonomous trading MUST expose its current plan, selected strategies, model/provider, reasons, and pending actions in the terminal.
- Configuration reload MUST remain atomic.; Historical backtests MUST remain point-in-time correct.
- Order submission MUST remain idempotent and reconciled.
- Filesystem package discovery for metrics and strategies MUST be recursive but bounded,
  canonical-path de-duplicated, and deterministic. Discovery MUST report a typed error
  when depth or package-count limits are exceeded; symlink loops and malformed trees
  MUST NOT hang startup or admit an unbounded worker set.
- Discovery MUST reject duplicate immutable metric or strategy IDs before any package is
  admitted to a runtime catalog; startup may not continue with an ambiguous definition.
- Manifest payloads MUST be read through a bounded byte window (1 MiB maximum) before
  parsing; oversized manifests MUST produce a typed discovery error.
- Manifest keys MUST be unique within a package; duplicate keys MUST be rejected rather
  than resolved by last-write-wins semantics.
- The runtime and terminal MUST stream `.cfg` files through the same 1 MiB input bound before
  handing text to `cfg-core`; oversized files MUST fail before startup settings load.
- A canonical instrument MAY have multiple provider identities; inserting an already
  known canonical definition MUST index every provider/venue/symbol tuple so fallback
  feeds resolve to the same authoritative instrument rather than failing silently.
- Provider identity strings MUST be non-empty and bounded to 64 bytes before catalog
  indexing; invalid identities MUST return a typed resolution error.
- Safety overrides for non-authoritative or synthetic marks MUST be typed boolean CFG
  settings (`market.allow_yahoo_live_marks` and `broker.allow_ibkr_bootstrap_mark`),
  with environment variables used only as fallback when the CFG key is absent. Both
  defaults MUST remain false and invalid values MUST fail closed before use.
- Non-secret broker deployment identity such as the IBKR account identifier MUST be
  accepted from typed `.cfg` (`broker.ibkr_account_id`) with environment fallback;
  credentials and API keys MUST remain secret-manager references or secret environment
  inputs and MUST NOT be serialized into CFG snapshots.
- IBKR market-poll identifiers (`broker.ibkr_conid` and
  `broker.ibkr_instrument_id`) MUST follow the same file-first typed configuration
  path, reject non-positive/non-integer values before worker creation, and never be
  read afresh from process environment inside a polling worker.
- Python worker network access MUST use typed `python.allow_network` CFG with a
  fail-closed `false` default and environment fallback only when CFG is absent;
  the resolved value MUST be injected into every worker command before launch.
- Broker fill events MUST carry a strictly positive execution price and quantity;
  invalid fills MUST be retained as anomalies without changing filled quantity or
  lifecycle state.
- HTTP provider adapters MUST reject declared response bodies above their configured
  bound before allocation and MUST stream-read undeclared-length bodies through the same
  bound; provider payload size cannot be controlled only after full buffering.
- This response-size invariant applies equally to market and news adapters; provider
  implementations MUST share bounded transport behavior rather than relying on each
  parser to detect oversized payloads after allocation.
- The LLM HTTP transport MUST enforce its response bound before allocation using
  `Content-Length` and bounded streaming for unknown-length responses.
- Provider request debug/trace representations MUST redact header values and may expose
  only header names, method, and non-secret URL components; API keys must never appear
  in logs or diagnostics.
- Rebuildable read-model projections MUST enforce a total byte bound before recovery
  buffering; oversized or sparse files MUST fail safely while the journal remains intact.
- Canonical `NewsItem` records MUST enforce bounded identity, URL, source, title,
  summary, content-hash, and symbol fields before storage; item-count bounds alone are
  insufficient protection against oversized records.
- News versioning MUST reject canonical-URL collisions across different article IDs
  without overwriting the existing URL index or orphaning the original item.
- Canonical news links MUST use HTTPS and contain a non-empty authority; invalid URL
  forms MUST be rejected before storage or terminal navigation.
- News retention MUST enforce a hard 100,000-item maximum regardless of caller-supplied
  capacity; lower capacities remain allowed for constrained deployments and tests.
## 4. Repository layout
```text
insidertrader/
├── AGENTS.md
├── README.md
├── Cargo.toml
├── Cargo.lock
├── pyproject.toml
├── uv.lock
├── crates/
│   ├── cfg-core/
│   ├── common-types/
│   ├── clock/
│   ├── journal/
│   ├── event-bus/
│   ├── ipc/
│   ├── scheduler/
│   ├── supervisor/
│   ├── market-types/
│   ├── market-data/
│   ├── instrument-master/
│   ├── feature-core/
│   ├── metric-sdk/
│   ├── metric-host/
│   ├── strategy-sdk/
│   ├── strategy-host/
│   ├── strategy-coordinator/
│   ├── model-runtime/
│   ├── ensemble/
│   ├── portfolio/
│   ├── risk-engine/
│   ├── execution/
│   ├── broker-api/
│   ├── reconciliation/
│   ├── news-core/
│   ├── context-graph/
│   ├── llm-core/
│   ├── autonomy/
│   ├── replay/
│   ├── exchange-sim/
│   ├── experiment-registry/
│   ├── model-registry/
│   ├── telemetry/
│   └── engine/
├── metrics/
│   ├── rust/
│   └── python/
├── strategies/
│   ├── rust/
│   ├── python/
│   ├── graph/
│   ├── llm/
│   └── examples/
├── providers/
│   ├── market/
│   ├── news/
│   ├── llm/
│   └── brokers/
├── python/insidertrader/
│   ├── metric_sdk/
│   ├── strategy_sdk/
│   ├── research/
│   ├── features/
│   ├── models/
│   ├── validation/
│   ├── news/
│   └── agents/
├── crates/
│   ├── terminal/
│   └── desktop-bridge/  # headless runtime/control-plane package (`insider-runtime`)
├── schemas/
├── config/
├── data/
├── models/
├── simulation/
├── research/
├── benches/
├── tests/
└── infrastructure/
```
## 5. Runtime planes
InsiderTrader has three planes: Hot Execution, Intelligence/Decision, and Research.
### 5.1 Hot Execution
- market-data decode and normalization; incremental feature state
- deadline-sensitive metrics; deterministic strategies where applicable
- portfolio/risk evaluation; execution planning
- order gateway; reconciliation
The hot path SHOULD be Rust and MUST never wait on a remote LLM.
### 5.2 Intelligence and Decision
- news aggregation; news relevance ranking
- context graph updates; LLM summarization and reasoning
- strategy comparison; manual decision assistance
- autonomous strategy orchestration
This plane may operate asynchronously and attach fresh intelligence to deterministic state.
### 5.3 Research
- feature discovery; strategy discovery
- model training; LLM-assisted hypothesis generation
- graph research; backtesting
- statistical validation; transaction-cost analysis
- challenger generation
## 6. Metric system
Metrics remain the smallest independently scheduled predictive or descriptive computations.
Metrics MAY be pure technical indicators, statistical models, neural models, event scores, liquidity estimators, volatility models, regime classifiers, news sentiment outputs, or graph-derived scores.
Metrics MUST NOT know that a manual user or an autonomous agent exists.
### 6.1 Metric manifest
```yaml
metric:
  id: "microstructure.imbalance.v4"
  language: "rust"
  entrypoint: "imbalance_v4"
  inputs:
    market: ["book.l2", "trades"]
    features: ["spread", "microprice"]
  output:
    kind: "score"
    range: [-1.0, 1.0]
    horizon_ms: 3000
  scheduling:
    period_ms: 25
    deadline_ms: 2
    budget_ms: 1
    priority: FAST
```
### 6.2 Metric output
```rust
struct MetricOutput {
    metric_id: MetricId,
    instrument: InstrumentId,
    generated_mono_ns: u64,
    horizon_ns: u64,
    ttl_ns: u64,
    score: f64,
    confidence: f64,
    uncertainty: f64,
    feature_snapshot: FeatureSnapshotId,
}
```
## 7. Strategy system — first-class decision modules
Strategies are the central addition above metrics.
Each strategy is a package like a metric but operates at the decision level.
A strategy may consume raw/canonical market state, metrics, news, graph state, account state, positions, or other declared strategy outputs if an acyclic dependency graph permits it.
### 7.1 Strategy responsibilities
- declare its universe and horizon; declare required and optional inputs
- convert evidence into an actionable portfolio/trade proposal; express confidence and uncertainty
- express requested exposure and maximum acceptable execution cost; emit `NoAction` explicitly when evidence is insufficient
- remain deterministic when configured as a deterministic strategy
### 7.2 Strategy manifest
```yaml
strategy:
  id: "equity.intraday.breakout.v6"
  language: "rust"
  mode: "deterministic"
  universe:
    source: "watchlist.primary"
  inputs:
    market: ["quotes", "trades", "bars.1m"]
    metrics:
      - "momentum.fast.v3"
      - "volatility.ewma.v2"
      - "liquidity.spread.v5"
    news:
      optional: true
      max_age_minutes: 60
    account:
      positions: true
      buying_power: true
  output:
    horizon: "15m"
    proposal_kind: "target_position"
  scheduling:
    trigger: "bar_close:1m"
    deadline_ms: 10
```
### 7.3 Strategy input
```rust
struct StrategyContext {
    now: DecisionTime,
    market: MarketSnapshot,
    metrics: MetricSnapshotMap,
    news: Option<NewsContext>,
    graph: Option<GraphContext>,
    portfolio: PortfolioSnapshot,
    account: AccountSnapshot,
    config: Arc<ConfigSnapshot>,
}
```
### 7.4 Strategy proposal
```rust
enum StrategyAction {
    NoAction,
    TargetPosition { quantity: Decimal },
    TargetWeight { weight: f64 },
    Increase { quantity: Decimal },
    Decrease { quantity: Decimal },
    Close,
}
struct StrategyProposal {
    proposal_id: ProposalId,
    strategy_id: StrategyId,
    strategy_version: StrategyVersionId,
    instrument: InstrumentId,
    action: StrategyAction,
    confidence: f64,
    expected_horizon_ns: u64,
    ttl_ns: u64,
    expected_return: Option<f64>,
    expected_volatility: Option<f64>,
    max_expected_cost_bps: Option<f64>,
    rationale_codes: Vec<RationaleCode>,
    evidence_refs: Vec<EvidenceRef>,
    trace_id: TraceId,
}
```
### 7.5 Strategy dependency graph
Strategy dependencies MUST form a DAG.
A strategy MAY depend on metrics.
A strategy MAY depend on context graph outputs.
A strategy MAY depend on a separately produced LLM score.
A strategy SHOULD NOT synchronously invoke a remote LLM in a latency-critical callback.
Strategies that need LLM context should consume a cached, timestamped `LLMContextSnapshot`.
## 8. Strategy Coordinator
The Strategy Coordinator is the central system that answers: `Given all enabled strategies, what proposals exist and how should they be surfaced or combined?`
It owns:
- strategy discovery; strategy lifecycle
- strategy dependency DAG; proposal collection
- proposal expiration; conflict detection
- strategy weighting; strategy risk budgets
- manual presentation; autonomy handoff
- strategy-level performance accounting
### 8.1 Proposal conflicts
Conflicts include:
- two strategies requesting opposite exposure on the same instrument; multiple strategies consuming the same risk budget
- strategy proposals exceeding portfolio capacity in aggregate; short-horizon and long-horizon strategies fighting over the same position
Conflict policies are configurable:
- `ISOLATED_BOOKS`: maintain independent virtual strategy books and net only at execution.; `WEIGHTED_NET`: net proposals after strategy weights.
- `PRIORITY`: higher-priority strategy wins conflicting marginal exposure.; `PORTFOLIO_OPTIMIZER`: transform all proposals into expected-return/risk inputs for joint optimization.
- `LLM_COORDINATED`: allow Autonomous Coordinator to select among already-valid proposals.
The default institutional design SHOULD use virtual strategy books plus a joint portfolio optimizer so attribution remains possible.
## 9. Manual, hybrid, and autonomous modes
Mode changes who initiates the final action; they do not fork the entire architecture.
### 9.1 Manual mode
The system continuously calculates metrics, strategies, risk previews, news relevance, and LLM analysis.
Strategy proposals appear in the terminal with action, confidence, horizon, expected risk, historical statistics, current conditions, and evidence.
The user may click a proposal to prefill an order/target ticket.
Nothing is sent until the user confirms.
### 9.2 Hybrid mode
Automation is configured per strategy, instrument group, account, time window, or action type.
Examples:
- automatically rebalance a low-risk strategy but ask before opening a new event trade; automatically close positions on strategy exit signals but require confirmation for new entries
- automatically execute proposals below a notional threshold and queue larger proposals for approval
### 9.3 Autonomous mode
Autonomous mode allows the Strategy Coordinator and Autonomous Coordinator to initiate actions automatically.
The LLM can review market state, news, graph context, metrics, active strategies, positions, recent performance, and current risk before choosing actions.
The LLM MUST communicate through a typed `AutonomousPlan`.
The actual target sizing, risk evaluation, execution planning, order submission, and reconciliation remain normal system services.
## 10. Autonomous Coordinator
The Autonomous Coordinator is a central LLM-assisted decision engine.
It is not required for deterministic automated strategies.
It is valuable when the system needs to combine heterogeneous evidence, compare strategy narratives, understand news, or adapt which strategy is active.
### 10.1 Inputs
- current symbol/watchlist context; market regime
- latest metric snapshots; current strategy proposals
- strategy historical diagnostics; news clusters
- context graph neighborhood; portfolio positions
- risk-budget availability; TCA estimates
- recent fills/slippage; configured autonomy policy
### 10.2 Autonomous plan schema
```json
{
  "plan_id": "plan_...",
  "as_of": "2026-08-25T11:00:00Z",
  "actions": [
    {
      "type": "EXECUTE_PROPOSAL",
      "proposal_id": "proposal_...",
      "scale": 0.65,
      "urgency": "NORMAL",
      "reason_codes": ["NEWS_CONFIRMATION", "MULTI_STRATEGY_AGREEMENT"]
    }
  ],
  "watch": ["AAPL", "NVDA"],
  "reconsider_after_ms": 30000
}
```
### 10.3 Allowed action vocabulary
- `EXECUTE_PROPOSAL`; `EXECUTE_PROPOSAL_SCALED`
- `IGNORE_PROPOSAL`; `PAUSE_STRATEGY`
- `RESUME_STRATEGY`; `REQUEST_REANALYSIS`
- `ADD_TO_WATCH`; `REMOVE_FROM_WATCH`
- `REDUCE_AUTONOMY`; `NO_ACTION`
The vocabulary is intentionally finite so the engine can validate every autonomous action.
## 11. LLM provider architecture
InsiderTrader MUST support OpenAI-style APIs through a provider abstraction rather than compiling the application against a single model vendor.
The provider configuration MUST allow a custom `base_url`, model identifier, API-key reference, timeout, retry policy, streaming flag, and capability flags.
### 11.1 Protocol targets
- Prefer an OpenAI-style Responses API when the provider implements it.; Support OpenAI-style Chat Completions as a compatibility fallback.
- Support server-sent-event or equivalent streaming.; Support structured JSON/schema-constrained outputs where the provider offers them.
- Support tool/function calling where the provider offers it.; Support model listing/capability probing where available.
### 11.2 Provider trait
```rust
#[async_trait]
trait LlmProvider {
    async fn complete(&self, req: LlmRequest) -> Result<LlmResponse>;
    async fn stream(&self, req: LlmRequest) -> Result<LlmStream>;
    async fn health(&self) -> Result<LlmHealth>;
    fn capabilities(&self) -> LlmCapabilities;
}
```
### 11.3 Provider configuration
```cfg
llm_provider = {
    protocol: "openai_compatible",
    base_url: env_or("IT_LLM_BASE_URL", "https://api.openai.com/v1"),
    api_key_ref: "secret://llm/main",
    model: env_or("IT_LLM_MODEL", "configured-model"),
    preferred_endpoint: "responses",
    fallback_endpoint: "chat_completions",
    stream: true,
    connect_timeout_ms: 2000,
    request_timeout_ms: 30000,
    max_retries: 2,
    max_parallel_requests: 8
}
```
### 11.4 Provider capability record
```rust
struct LlmCapabilities {
    responses_api: bool,
    chat_completions: bool,
    streaming: bool,
    json_schema: bool,
    tools: bool,
    vision: bool,
    embeddings: bool,
    max_context_tokens: Option<u64>,
}
```
### 11.5 Local and alternate providers
Because `base_url` is configurable, InsiderTrader can connect to compatible gateways, self-hosted inference servers, local models, or cloud providers that implement the chosen protocol.
Provider-specific behavior MUST be isolated behind adapters.
A provider outage MUST not crash charts, metrics, deterministic strategies, or manual order entry.
## 12. LLM request discipline
- Every trading-relevant LLM request receives a `TraceId`.; Prompts MUST be versioned.
- System prompt, tool schema, model, temperature, and input context hashes MUST be journaled for reproducibility where practical.; Trading-relevant output MUST be schema-validated.
- The engine MUST distinguish transport failure, provider failure, refusal, malformed output, timeout, and semantic-validation failure.; LLM output MUST have a TTL.
- The terminal MUST show when displayed analysis is stale.; Remote LLM latency MUST never block the scheduler hot path.
- News summaries SHOULD be cached by article/content hash.; Repeated equivalent requests SHOULD reuse cached analysis when freshness permits.
- Token budgets MUST be explicit by task class.
Suggested task classes:
- `NEWS_TAGGING`: short, high-throughput extraction.; `NEWS_SUMMARY`: short per-cluster summary.
- `CHART_CONTEXT`: medium context, symbol-specific.; `STRATEGY_REVIEW`: strategy proposals + diagnostics.
- `AUTONOMOUS_PLAN`: broad but structured decision context.; `RESEARCH`: large asynchronous context and tools.
## 13. LLM tool layer
LLMs SHOULD receive data through typed tools rather than giant unstructured prompt dumps.
Core tools:
- `get_market_snapshot(symbol, timeframe)`; `get_chart_window(symbol, start, end, resolution)`
- `get_metric_snapshot(symbol, metric_ids)`; `get_strategy_proposals(symbol)`
- `get_strategy_report(strategy_id)`; `get_news(symbol, window, limit)`
- `get_news_cluster(cluster_id)`; `get_context_graph(entity_id, depth)`
- `get_portfolio()`; `get_position(symbol)`
- `preview_risk(proposal_id, scale)`; `get_tca(strategy_id, symbol, window)`
- `get_recent_fills(symbol)`; `submit_autonomous_plan(plan)`
Tool results are authoritative structured data.
The model should never be asked to remember precise price, quantity, or position data that can be retrieved by a tool.
## 14. News subsystem
News is a first-class data stream, not a text blob attached to a chat panel.
The news subsystem retrieves, normalizes, deduplicates, clusters, ranks, stores, and distributes articles/events.
### 14.1 Provider adapters
- `NewsApiProvider`: REST integration for broad article search and top headlines.; `YahooFinanceNewsProvider`: Yahoo Finance search/news adapter for symbol/company context.
- `RssProvider`: configurable RSS/Atom feeds.; `BrokerNewsProvider`: broker-specific news feeds when available.
- `PremiumNewsProvider`: generic interface for additional low-latency providers.; `FilingsProvider`: filing/event document streams kept in the same contextual event model.
Provider adapters MUST normalize into one `NewsItem` schema.
### 14.2 NewsAPI integration
The provider SHOULD support `/v2/everything` for article discovery and `/v2/top-headlines` for current headline streams.
Top-headlines country, category, and sources filters MUST be read from typed `.cfg`
keys (`news.newsapi_country`, `news.newsapi_category`, `news.newsapi_sources`) when
present, with environment variables used only as fallback when the corresponding key
is absent. Wrong CFG types and oversized values MUST fail before provider startup.
Search parameters SHOULD expose query, source/domain filters, time range, language, sort mode, and pagination.
The adapter MUST paginate in the background so the terminal can implement bounded scrolling.
API keys are secret references, not literals in CFG.
### 14.3 Yahoo Finance integration
Implement Yahoo Finance behind a replaceable provider module.
Useful current endpoints commonly used by community clients include:
- `/v8/finance/chart/{symbol}` for OHLCV/chart history.; `/v1/finance/search?q={query}&quotesCount=N&newsCount=N` for search plus news results.
- `/v7/finance/quote` for multi-symbol quote data where available.; `/v10/finance/quoteSummary/{symbol}` for richer summary data where available.
Because these endpoints are not a stability contract for InsiderTrader, the adapter MUST isolate cookie/crumb logic, throttling behavior, schema changes, and provider-specific failures.
Yahoo Finance is useful for research, terminal fallback, and convenient context; the rest of InsiderTrader MUST not depend structurally on it.
### 14.4 News item schema
```rust
struct NewsItem {
    id: NewsId,
    provider: ProviderId,
    canonical_url: String,
    source_name: String,
    title: String,
    summary_text: Option<String>,
    published_at: Option<i64>,
    received_at: i64,
    symbols: Vec<InstrumentId>,
    entities: Vec<EntityRef>,
    language: Option<String>,
    image_url: Option<String>,
    content_hash: Hash256,
}
```
### 14.5 Deduplication
- canonical URL match; content hash match
- normalized title similarity; same-event semantic clustering
- publisher syndication detection
Do not flood the terminal with twenty copies of the same wire story.
## 15. News intelligence and relevance
Every chart should have two news views: `Relevant` and `All`.
`Relevant` answers what matters most to the active chart and current timeframe.
`All` is a complete scrollable feed filtered only by the user-selected scope.
### 15.1 Relevance score
A default ranking score may combine:
```text
score =
    w_symbol      * direct_symbol_match
  + w_entity      * graph_entity_proximity
  + w_semantic    * embedding_similarity
  + w_recency     * recency_decay
  + w_event       * event_importance
  + w_strategy    * strategy_input_relevance
  + w_position    * portfolio_exposure_relevance
  + w_timeframe   * horizon_match
  + w_source      * source_quality_score
```
The ranking model is independent of the LLM.
An LLM may enrich tags and summaries, but deterministic ranking must have a non-LLM fallback.
### 15.2 Per-chart news context
When a chart changes symbol or timeframe:
1. resolve instrument and issuer nodes
2. query recent directly tagged news
3. expand related graph entities
4. retrieve semantically similar event clusters
5. score relevance for the chart timeframe
6. render top items immediately
7. asynchronously attach LLM summaries and implications
### 15.3 News terminal behavior
- scrolling feed with virtualization; Relevant / All / Watchlist / Portfolio tabs
- time filters; source filters
- event-type filters; LLM summary toggle
- expand article details; pin article
- open external article; drag article into an AI analysis panel
- show linked chart markers; show related strategies affected by the event
## 16. Context graph
The graph is the connective tissue between prices, news, metrics, strategies, and portfolio state.
It MUST remain a structured data system; the LLM is a consumer and annotator of the graph, not the graph itself.
### 16.1 Node types
- Instrument; Issuer
- Sector; Industry
- Index; ETF
- Currency; Commodity
- Country; MacroSeries
- Person; Event
- NewsItem; NewsCluster
- Metric; Strategy
- Model; Portfolio
- Position; Order
- Fill
### 16.2 Edge examples
- `ISSUED_BY`; `MEMBER_OF`
- `SUPPLIER_OF`; `CUSTOMER_OF`
- `COMPETES_WITH`; `TRACKS`
- `HOLDS`; `MENTIONS`
- `AFFECTS`; `GENERATED_BY`
- `USES_METRIC`; `USES_NEWS`
- `CORRELATED_WITH`; `HEDGES`
- `DERIVED_FROM`
### 16.3 Graph query use cases
- news relevant to a company through suppliers or competitors; strategies depending on a metric affected by a feed problem
- positions indirectly exposed to a macro event; find all charts/news connected to an issuer event
- build context packets for LLM strategy review; visualize why two instruments are treated as related
## 17. Embeddings and retrieval
Vector search complements the graph.
Embeddings SHOULD be generated for headlines, summaries, event descriptions, strategy descriptions, research notes, and optionally filings/documents.
Store embedding model/version with every vector.
A provider change requires either versioned mixed-index handling or re-embedding.
Retrieval should combine lexical search, graph traversal, metadata filters, and vector similarity.
## 18. Native terminal workstation architecture
The client MUST feel like a premium professional market terminal: mnemonic-first,
keyboard-only, information-dense, predictable, and fast under continuous updates.
The workstation is a Rust binary using a terminal renderer and the authenticated
Unix command transport. It MUST NOT embed a browser, WebView, Node runtime, or a
second copy of trading state. It MAY launch the operator's system browser for a
loopback-only chart projection that uses no remote UI assets.
### 18.1 Process boundaries
- The headless runtime owns providers, trading, journal, reconciliation, and credentials.
- The terminal owns bounded presentation state, current function, selection, scroll, and command input only.
- Closing or crashing the terminal MUST NOT stop autonomous execution.
- Every mutation uses the same versioned command payload, capability check, idempotency key, and journal path as unattended clients.
### 18.2 Optional local browser chart
- `TV` / `TRADINGVIEW` MAY expose the current canonical graph through an ephemeral
  loopback listener and open it in the system browser.
- The page MUST be local, chart-focused, dependency-free at runtime, and fed
  asynchronously from bounded terminal presentation snapshots.
- The sidebar coordination terminal MUST be read-only: it may change symbol,
  timeframe, chart style, overlays, zoom, and pan, but MUST reject order, risk,
  autonomy, broker, strategy, metric, and configuration mutations.
- Closing the browser MUST NOT affect the terminal or runtime. Closing the native
  terminal MUST stop its loopback chart listener without affecting autonomous execution.
## 19. Function model
Every major view is a named function reachable by mnemonic plus Enter/`GO` and,
for common functions, a function key. Required navigation includes `HOME`, `MARKET`,
`PORT`, `ORDERS`, `STRAT`, `METRICS`, `NEWS`, `RISK`, `AUTO`, `ALERTS`, `HEALTH`,
and `HELP`. Up/Down and PageUp/PageDown provide bounded scrolling. Escape clears
the command line and Ctrl-C/`QUIT` exits only the terminal.
## 20. Terminal visual system
- near-black surfaces, orange function accents, amber labels, and tabular numerals
- dense bordered regions and stable columns; no animation or layout shift on ticks
- gain/loss and health states MUST include text/sign/glyph in addition to color
- layout MUST adapt to terminal dimensions without writing outside the frame
- stale age, connection state, account, cursor, risk state, and autonomy mode remain visible
## 21. Required terminal functions
- market monitor and compact OHLCV history; portfolio and reconciled positions
- order blotter, two-stage order preview/confirmation, cancellation, and TCA
- strategy registry, proposal monitor, lifecycle controls, and coordinator state
- metric registry, live values/health/latency, and lifecycle controls
- Relevant/All news, news detail, provider health, and contextual analyst
- autonomy plan/actions, risk limits/utilization/state controls, and emergency halt
- alerts/acknowledgement, broker/supervisor health, configuration status/reload
- trace reconstruction, backtests, experiments, models, screener, depth/tape, and search
## 22. Terminal chart system
Terminal-native charts MUST provide bounded OHLCV history and fast redraws. Price,
volume, metrics, strategies, fills, orders, and news/event markers remain separate
typed series. Richer chart functions may use braille/block cells, but the underlying
data and navigation must remain usable on a basic color terminal.
## 23. Strategy terminal
`STRAT` shows identity, mode, state, lifecycle, priority, dependencies, proposal
count, confidence, horizon, risk usage, performance, and health. `AUTO` shows the
active typed proposals and plan. Lifecycle changes require explicit confirmation
and evidence references.
## 24. AI Analyst terminal
The analyst function is contextual, streams asynchronously, names its market/news/
strategy context, cites authoritative internal evidence, and never blocks runtime
refresh or execution. Trading-relevant actions remain schema validated commands.
## 25. Manual trader workflow
1. User opens a workspace with Chart, Relevant News, Strategy Browser, Order Ticket, Positions, and AI Analyst.
2. Selecting a symbol updates linked panels.
3. Market data and metrics update continuously.
4. Strategies emit proposals in the background.
5. News providers stream/fetch new items; dedupe and graph linking occur.
6. Relevant News ranks the strongest items for the chart.
7. LLM summaries arrive asynchronously and annotate news clusters.
8. Strategy cards update with risk, confidence, horizon, and evidence.
9. User can ask the AI Analyst to compare or explain.
10. Clicking a strategy proposal prefills a target/order ticket.
11. Risk preview calculates the resulting exposure before submission.
12. User confirms and the normal execution pipeline handles the trade.
## 26. Autonomous trader workflow
1. Market/news/account events update deterministic state.
2. Metrics evaluate on their configured schedules.
3. Strategies generate typed proposals.
4. The Strategy Coordinator filters expired/unhealthy proposals.
5. The context graph builds an autonomy context packet.
6. The Autonomous Coordinator receives strategy proposals plus relevant news and portfolio/risk context.
7. The LLM returns a schema-valid AutonomousPlan.
8. Semantic validation verifies proposal IDs, action vocabulary, TTL, and scaling bounds.
9. The coordinator converts selected actions into portfolio target requests.
10. The portfolio engine performs joint optimization where configured.
11. The risk engine allows, resizes, or denies resulting changes.
12. The execution planner selects execution style and child-order schedule.
13. The order gateway submits idempotent orders.
14. Reconciliation updates authoritative state.
15. TCA and realized outcomes update strategy/agent diagnostics.
16. The Autonomy Console shows each stage and next reconsideration time.
## 27. News-to-strategy interaction
News can affect trading through explicit channels.
Possible designs:
- a NewsSentiment metric that emits a calibrated score; an EventClassifier metric that emits event type/probability
- a strategy that directly consumes NewsContext; a graph strategy that reacts to multi-entity events
- an LLM strategy that produces StrategyProposal objects; the Autonomous Coordinator using news to choose among deterministic strategy proposals
Every strategy manifest must make its chosen path explicit.
The engine SHOULD avoid allowing the same news signal to be counted multiple times through correlated metrics without attribution.
## 28. LLM-based strategies
LLM strategies are allowed as a strategy class.
They live under `strategies/llm/` and follow the same manifest/proposal interfaces as every other strategy.
They are not special-cased into direct broker access.
Example manifest:
```yaml
strategy:
  id: "llm.event_reaction.v2"
  engine: "llm"
  llm:
    provider: "main"
    prompt_version: "event-reaction-12"
    response_schema: "StrategyProposalV3"
  inputs:
    market: ["bars.1m", "quotes"]
    metrics: ["volatility.fast", "liquidity.spread"]
    news:
      required: true
      max_age_minutes: 10
      max_clusters: 8
  output:
    ttl_ms: 15000
```
LLM-strategy output MUST pass the same proposal validator and downstream risk system as deterministic strategy output.
## 29. Market data providers
Market-data access is provider-based.
Support broker feeds, exchange/direct feeds, vendor APIs, historical data files, and convenient web providers behind the same canonical model.
Provider responsibilities:
- symbol resolution; timestamp normalization
- rate-limit handling; reconnect/backoff
- sequence/gap handling where applicable; corporate action/reference metadata
- provider-specific schema translation; health metrics
Yahoo Finance MUST be a named adapter rather than a special case in chart code.
Charts ask the MarketData service for canonical bars; the service decides which provider supplied them.
## 30. CFG extensions
CFG remains the authoritative declarative runtime configuration format.
It MUST support new domains for strategies, providers, LLMs, terminal defaults, and autonomy.
The terminal News stale threshold MUST be configurable as `terminal.news_stale_after_ms`, bounded to
60,000–3,600,000 milliseconds with a deterministic 300,000-millisecond fallback; the generator
MUST preserve unrelated keys and comments when merging it.
The terminal AI Analyst freshness threshold MUST be configurable as `terminal.analyst_stale_after_ms`
with the same 60,000–3,600,000 millisecond bounds and deterministic fallback.
The terminal alert refresh cadence MUST be configurable as `terminal.alert_poll_ms`, bounded to
500–60,000 milliseconds with a deterministic 1,000-millisecond fallback.
Example:
```cfg
mode = "hybrid"
providers = {
  market_primary: "broker",
  market_fallback: "yahoo_finance",
  news_primary: "newsapi",
  llm_primary: "main_llm"
}
autonomy = {
  enabled: true,
  coordinator: "llm",
  reconsider_ms: 30000,
  max_plan_age_ms: 60000
}
workspace = {
  default_layout: "trading-glass-v3",
  restore_last_layout: true,
  animations: true
}
```
Hot reload classes remain `HOT`, `DRAIN_AND_SWAP`, `RESTART_COMPONENT`, and `RESTART_NODE`.
Strategy model/version changes SHOULD normally be `DRAIN_AND_SWAP`.
Provider subscription sets such as `market.yahoo_symbols` MUST be accepted from CFG as
bounded comma-separated `SYMBOL=INSTRUMENT_ID` entries (maximum 128); environment variables
are fallback inputs only when the CFG key is absent.
## 31. Point-in-time correctness for news and LLMs
Historical strategy evaluation with news requires knowledge-time semantics.
A news article may only enter a replay at or after the recorded `received_at` or other configured historical availability timestamp.
Corrections and later article edits MUST NOT overwrite what the historical strategy saw.
LLM backtests need special treatment:
- best: store historical LLM outputs with prompt/model/input hashes and replay them; acceptable research mode: rerun a pinned model/provider and mark the run nondeterministic
- for production validation: prefer frozen extracted features/events over repeatedly asking a moving remote model
A backtest that asks today's model to reinterpret old news is a different experiment and must be labeled as such.
## 32. Strategy backtesting
Strategies are evaluated at the proposal level and portfolio level.
The replay engine should retain every emitted proposal, including proposals that were later rejected or netted away.
Report:
- proposal count; proposal hit rate
- proposal expected-vs-realized return; signal decay by horizon
- conflict rate with other strategies; net portfolio contribution
- turnover; execution cost
- capacity; drawdown
- regime performance; news-conditioned performance
- LLM-conditioned performance where applicable
## 33. Statistical validation
- Use chronological walk-forward evaluation.; Use purging/embargo when labels or feature windows overlap.
- Use combinatorial purged cross-validation where appropriate.; Track all material trials.
- Report Deflated Sharpe Ratio or another multiple-search adjustment.; Estimate Probability of Backtest Overfitting for broad strategy searches where feasible.
- Keep a final holdout that is not iteratively mined.; Stress transaction costs and latency.
- Test neighboring parameters, not only the optimum.
## 34. Portfolio engine
Strategy proposals can be converted to a common alpha/target representation and optimized jointly.
Default objective:
```text
maximize expected net return
- covariance risk; turnover
- liquidity penalty; estimated market impact
- model/strategy uncertainty
```
Typical constraints:
- gross exposure; net exposure
- symbol concentration; sector/factor concentration
- strategy risk budget; volatility target
- liquidity/ADV participation; turnover
- cash/buying power
## 35. Risk engine
The risk engine is independent of strategy optimism and LLM confidence.
Pre-trade checks include:
- max order notional; max position
- max strategy exposure; max portfolio gross/net
- max leverage; max drawdown
- max realized/predicted volatility; max participation rate
- max outstanding orders; max message rate
- price deviation guard; stale-data guard
- duplicate-intent guard; clock-health guard
- broker-session-health guard
Risk states: `RUNNING`, `REDUCE_ONLY`, `CANCEL_ONLY`, `HALTED`.
## 36. Execution and order gateway
Strategies never implement broker protocol code.
The execution layer supports passive limit, marketable limit, TWAP, VWAP, POV, implementation-shortfall, and adaptive styles.
Order state remains explicit: Created, RiskApproved, Queued, Sending, Sent, Acknowledged, PartiallyFilled, Filled, CancelPending, Cancelled, ReplacePending, Rejected, Expired, Unknown.
`Unknown` after a lost acknowledgement MUST trigger reconciliation before blind retry.
ClientOrderId MUST be deterministic/idempotent.
## 37. Transaction-cost analysis
Every fill records decision price, arrival price, mid at send, mid at acknowledgement, fill price, spread, implementation shortfall, latency, participation, and post-fill adverse selection.
TCA feeds back into strategy net returns, capacity, execution parameters, and Autonomous Coordinator context.
An LLM may explain TCA anomalies; it must not invent missing measurements.
## 38. Supervision and failure isolation
Supervision tree:
```text
RootSupervisor
├── MarketDataSupervisor
├── NewsSupervisor
├── MetricSupervisor
├── StrategySupervisor
├── LlmSupervisor
├── AutonomySupervisor
├── PortfolioSupervisor
├── RiskSupervisor
├── ExecutionSupervisor
├── JournalSupervisor
├── UiBridgeSupervisor
└── TelemetrySupervisor
```
Each child has restart intensity, backoff, jitter, and quarantine behavior.
An LLM outage should degrade AI summaries/autonomy without killing manual charts or deterministic strategies.
A NewsAPI outage should leave existing cached news and alternate providers usable.
A terminal crash should not corrupt the execution engine.
## 39. Scheduler and deadlines
Metric/strategy scheduling classes: `ULTRA`, `FAST`, `STANDARD`, `BATCH`.
Remote LLM calls belong to `STANDARD` or `BATCH`; never `ULTRA`.
The scheduler uses monotonic deadlines, bounded queues, per-worker concurrency caps, and explicit backpressure.
Late metric/strategy outputs are recorded but rejected when TTL/deadline semantics require.
## 40. IPC and event flow
- same-process hot queues: bounded SPSC/MPSC; same-host hot path: shared-memory SPSC rings + arenas
- same-host control: local RPC/domain sockets; cross-host control: authenticated RPC
- durable async: append-only event stream/log
Large chart batches and features SHOULD move through binary arrays/Arrow-like memory rather than per-object JSON.
LLM/news control messages may use normal structured serialization because they are not microsecond-path data.
The Unix desktop bridge MUST enforce bounded request payloads, owner-only socket
permissions, per-connection request counts, and read/write deadlines so an abandoned
or abusive client cannot monopolize the accept loop. `serve --check` MUST validate
startup composition, journal recovery, canonical paper fixture ingestion, and
provider/package registration without binding the IPC socket or starting workers.
## 41. Journal and trace model
Every trade decision should be reconstructible through a `TraceId`.
Trace chain:
```text
MarketEvent / NewsEvent
→ FeatureSnapshot
→ MetricOutput[]
→ StrategyProposal[]
→ AutonomousPlan or UserAction
→ PortfolioTarget
→ RiskDecision
→ ExecutionPlan
→ OrderEvent[]
→ Fill[]
→ Reconciliation
```
The journal also records strategy version, LLM provider/model, prompt version, news cluster IDs, and workspace/user action when relevant.
## 42. Observability
Required runtime dashboards:
- market feed health; news provider health
- LLM provider latency/error/token usage; metric latency and deadline misses
- strategy proposal rates; strategy conflict rates
- autonomy plan rate and invalid-plan rate; portfolio/risk state
- order/fill state; TCA
- system CPU/memory/queue depth
LLM metrics include p50/p95/p99 latency, stream-first-token latency, malformed output rate, schema validation failure rate, timeout rate, retries, cache hit rate, and tokens/cost if provider exposes usage.
## 43. Terminal performance
- redraws SHOULD remain responsive during live updates and avoid blocking input
- large news/watchlist/tape functions MUST retain bounded data and render only visible rows
- refresh batches MUST not overlap; terminal decoding MUST reject oversized collections and strings
- expensive analytics remain engine-side or asynchronous and never block keyboard handling
## 44. Terminal function presets
Ship polished functions for Trading, MultiChart, News, Strategies, Autonomy,
Execution, and Research. Presets are bounded presentation preferences only.
## 45. Command and keyboard model
The command line is always available. Mnemonics cover symbol search, functions,
strategy analysis, alerts, order preview/confirmation, backtests, TraceId inspection,
configuration, risk, and autonomy. Every core terminal action MUST map to a typed
command payload so keyboard shortcuts and automation invoke the same safe path.
## 46. Notifications and alerts
Alert sources:
- price levels; metric thresholds
- new strategy proposal; strategy state change
- relevant breaking news; portfolio/risk threshold
- autonomy action; order reject/fill
- provider/system failure
Alerts can route to in-app toast, panel stream, native notification, sound, or configured external webhook.
## 47. Search
Global search spans symbols, strategies, metrics, news, models, experiments, orders, and traces.
The search service combines exact symbol matching, lexical text search, graph entities, and vector semantic search.
Search results are typed so selecting a result opens the correct inspector/panel.
## 48. Persistence
Persist:
- workspace layouts; watchlists
- linked panel groups; chart indicator templates
- drawing objects; panel settings
- strategy display preferences; manual/autonomy preferences
- news filters; AI Analyst pinned threads/context references
Trading state MUST NOT be reconstructed from terminal persistence.
## 49. Security boundaries
Technical security rules:
- broker credentials only in broker/order-gateway boundary; LLM/API keys in secret storage
- terminal clients never receive raw secrets; production Python workers have restricted filesystem/network where practical
- provider outputs are parsed as untrusted external data; artifacts are hashed/versioned
- service identities and least privilege are preferred
- External provider and article URLs MUST be bounded to 2,048 bytes, use HTTPS (except
  explicitly allowlisted localhost HTTP for local inference), have a non-empty authority,
  and contain no username/password userinfo before storage, navigation, or request dispatch.
## 50. Testing the new architecture
Required strategy tests:
- manifest schema; input dependency resolution
- DAG cycle rejection; proposal schema
- TTL expiration; NoAction behavior
- conflict resolution; virtual-book attribution
- replay determinism for deterministic strategies
Required LLM tests:
- OpenAI-style base URL override; Responses endpoint happy path
- Chat Completions fallback; stream interruption
- timeout; 429/retry handling
- 5xx handling; malformed JSON
- schema mismatch; tool-call validation
- provider unavailable fallback; cache correctness
Required news tests:
- provider normalization; pagination
- deduplication; cluster stability
- symbol/entity mapping; relevance ordering
- stale timestamps; provider outage
Required terminal tests:
- snapshot and command wire decoding with strict bounds and trailing-byte rejection
- mnemonic routing, function keys, scrolling, reconnect, and stale-state retry
- large-news/tape bounded rendering and terminal resize behavior
- autonomy/proposal rendering and two-stage manual order preview/confirmation
- runtime continuation after terminal exit or crash
## 51. CI/CD additions
- Native terminal clippy/test/build is a merge gate.; Strategy SDK compatibility tests are a merge gate.
- Schema compatibility is a merge gate.; Golden replay is a merge gate.
- Hot-path benchmark regressions are a merge gate.; LLM provider contract tests run against a local fake OpenAI-compatible server.
- News provider tests use recorded fixtures to avoid network nondeterminism.; Native terminal build smoke tests run for target platforms.
- The release-candidate procedure in `docs/runbooks/release-certification.md` is normative
  for G15. It MUST be used to record immutable RC hashes, the seven-day paper soak,
  disaster drills, broker/ledger reconciliation, capability approvals, and canary evidence;
  `scripts/check_runbook.py` MUST fail if any required procedure section is removed.
- Linux CI MUST run `cargo check --locked -p insider-terminal` after the repository gate.
## 52. Development-agent ownership
- **Architect Agent** — cross-component contracts and invariants.; **Metric Agent** — metric SDK/runtime and metric implementations.
- **Strategy Agent** — strategy SDK/runtime, manifests, proposal semantics.; **Autonomy Agent** — strategy coordination and typed autonomous plans.
- **LLM Agent** — provider adapters, tools, prompt registry, structured output.; **News Agent** — provider ingestion, dedupe, clustering, ranking.
- **Graph Agent** — entity graph and hybrid retrieval.; **Terminal Agent** — native client, functions, command model, rendering.
- **Chart Agent** — renderer, panes, overlays, drawings, news/strategy markers.; **Portfolio/Risk Agent** — capital allocation and risk state.
- **Execution Agent** — orders, brokers, reconciliation, TCA.; **Simulation Agent** — point-in-time replay and fills.
- **Reliability Agent** — supervision, failover, chaos, capacity.
## 53. Coding-agent workflow
1. Read AGENTS.md and the nearest component README.
2. Read `PLAN.md`; name the owning gate and requirement IDs before editing.
3. Identify which invariant, public schema, journal event, and failure mode are affected.
4. Inspect adjacent tests, provider fixtures, migrations, and recovery paths before editing.
5. Prefer the smallest coherent vertical change that leaves no unsafe placeholder path.
6. Add or update deterministic tests, negative cases, and fault cases with implementation.
7. Run component and schema-compatibility tests; record the exact commands and results.
8. Run golden replay whenever serialized state, time, decisions, accounting, or orders change.
9. Run benchmarks with telemetry/journaling enabled whenever a hot path or terminal stream changes.
10. Update manifests, schemas, generated bindings, migrations, runbooks, and traceability rows.
11. Do not check a gate or acceptance item unless its same-revision evidence satisfies `PLAN.md`.
12. Record assumptions in an ADR when they alter a contract, safety property, or production limit.

Agents MUST NOT satisfy a requirement with a stub that returns success, an ignored test,
a mock unavailable in the packaged application, or a terminal function disconnected from an
authoritative service. `todo!`, `unimplemented!`, placeholder credentials, permissive
catch-all errors, silent fallback, and test-only production branches block completion
of the owning requirement. Partial work remains unchecked and must state what is absent.
## 54. Research-agent workflow
Research agents use structured experiments:
```text
Hypothesis
→ Data/feature plan
→ Baseline
→ Strategy/model candidate
→ Cost-aware replay
→ Walk-forward/CPCV
→ Robustness
→ Capacity/TCA
→ Shadow challenger
→ Canary
→ Production candidate
```
LLMs are especially useful for hypothesis generation, document/news extraction, graph queries, failure analysis, experiment explanation, and strategy ideation.
They MUST NOT erase failed experiment history.
## 55. Experiment registry extensions
Experiment records add:
- strategy ID/version; news dataset snapshot
- news clustering version; graph snapshot/version
- LLM provider/model if used; prompt version
- tool schema version; LLM output cache IDs
- autonomy configuration if used
## 56. Model and prompt registry
Treat prompts as versioned artifacts.
Prompt registry fields:
- prompt ID; version
- purpose; input schema
- output schema; allowed tools
- expected task class; recommended model capabilities
- hash; test fixture suite
A prompt change that affects autonomous plans is a versioned deployment change.
## 57. Strategy registry
Strategy states: `Research`, `Validated`, `Shadow`, `Canary`, `Production`, `Paused`, `Retired`, `Quarantined`.
Each record stores manifest, artifact hash, dependencies, expected latency, historical tests, risk budget, supported modes, and current health.
Production config references exact strategy versions; `latest` is forbidden.
## 58. Provider registry
All external providers are discovered through typed provider manifests.
A manifest declares:
- provider kind; base URL
- auth method; capabilities
- rate limits; timeout policy
- retry policy; streaming support
- health endpoint/probe; schema version
Provider replacement SHOULD require no strategy/terminal rewrites.
## 59. Graceful degradation
Examples:
- LLM down → show deterministic strategy proposals and raw/ranked news.; NewsAPI down → use Yahoo/RSS/broker providers and cached feed.
- Yahoo down → charts use broker/primary provider; Yahoo-specific context disappears.; Context graph down → direct symbol news still works.
- Strategy worker down → other strategies continue.; Autonomy coordinator down → deterministic autonomous strategies may continue if configured; manual mode remains available.
- Terminal down → execution service continues according to deployment mode.
## 60. Delivery roadmap
`PLAN.md` is the normative, dependency-ordered implementation and certification plan
for this specification. Its gates G00-G15 replace thematic phase completion. A gate
is complete only when its named artifacts, tests, thresholds, fault cases, runbooks,
and hashed evidence record exist and pass for the same source revision.

The roadmap is:
1. reproducible repository and deterministic runtime
2. canonical multi-asset data, metrics, strategies, replay, portfolio and risk
3. idempotent IBKR execution, reconciliation and TCA
4. independently restartable local services and secure terminal control plane
5. native Rust terminal workstation, news, LLM intelligence, graph and autonomy
6. packaged-system soak, disaster recovery, security and per-asset live certification

Implementation agents MUST use `PLAN.md` rather than interpreting this summary as a
task list. If a requirement in this file is absent from the plan traceability matrix,
the plan is incomplete and the relevant gate cannot be checked.
## 61. Definition of done for Strategy system
The Strategy system is done only when G04 evidence verifies package discovery,
manifest capability enforcement, DAG behavior, proposal validation, deterministic
TTL/replay, conflict policy, virtual-book attribution, lifecycle/quarantine and common
manual/autonomous schema consumption. A loaded package or rendered proposal alone is
not completion.
## 62. Definition of done for LLM system
The LLM system is done only when G11 contract fixtures verify Responses and Chat
Completions, custom base URL, streaming boundaries, failure taxonomy, bounded retry,
schema and semantic validation, typed tool permissions, cache identity, token/cost
budgets, trace completeness and full deterministic-system operation during outage.
## 63. Definition of done for News system
The News system is done only when G10 evidence verifies restart-safe pagination,
immutable normalization, deterministic dedupe/clustering, point-in-time corrections,
entity-link provenance, measured ranking quality, provider failure, virtualized feeds
and exact content-version navigation from chart markers.
## 64. Definition of done for terminal workstation
The terminal is done only when packaged-build evidence verifies every required
function and state, mnemonic/function-key navigation, chart and dense-table rendering,
manual preview/confirmation/idempotency, keyboard accessibility, reconnect and stale-state
behavior, bounded performance, resize handling, and crash isolation from engine state.
## 65. Final architecture invariant
InsiderTrader is healthy when market state, metrics, strategies, decision mode, portfolio, risk, execution, reconciliation, and terminal/intelligence state are separately observable and can fail independently without ambiguity.
The system should be understood as:
```text
a trading engine
+ a strategy runtime
+ a professional workstation
+ a news/context intelligence graph
+ a provider-agnostic LLM orchestration layer
+ a research and replay machine
```
The LLM layer is powerful because it sits on top of structured, tested trading primitives.
It must amplify InsiderTrader's capabilities without becoming the foundation on which prices, positions, orders, or strategy semantics depend.
## Appendix A — Current integration notes
As of the current design research:
- The official OpenAI Python client supports configurable `base_url`, making an OpenAI-compatible provider abstraction practical.; The OpenAI Python project describes the Responses API as its primary model interaction API, while Chat Completions remains available.
- NewsAPI documents `/v2/everything` for article discovery and `/v2/top-headlines` for live headline use cases.; Community Yahoo Finance clients currently use `/v8/finance/chart/{symbol}` for OHLCV and `/v1/finance/search` for quote/news search.
- Ratatui and Crossterm provide native terminal rendering and input without a browser/WebView runtime.
These details belong behind provider/terminal adapters so future API/library changes do not rewrite the trading core.
## Appendix B — Normative acceptance catalogue
These are atomic requirements, not implementation tasks. Checking one asserts that
its row in `evidence/requirements.csv` names an automated verification, that the
verification passed against a packaged system at the same source revision, and that
the result is referenced by the owning G00-G15 gate evidence in `PLAN.md`.

No item may be checked based only on code inspection, a mocked happy path, a terminal
screenshot, or the existence of a type/crate/panel. Provider-related items require
recorded contract fixtures plus outage/error cases. Trading-related items require
replay, restart and idempotency coverage. Terminal items require packaged native end-to-end
tests and applicable performance/accessibility measurements. When a regression is
confirmed, the item and its owning gate are invalidated until the same verification
passes again.

- [ ] A001 Strategy packages are discovered from `strategies/` without hard-coded registration.
- [ ] A002 Strategy manifests reject missing input declarations or invalid dependency references.
- [ ] A003 Strategy-to-strategy dependencies are rejected when they form a cycle.
- [ ] A004 Every strategy emits `StrategyProposal` or explicit `NoAction`.
- [ ] A005 Manual and autonomous modes consume the same proposal schema.
- [ ] A006 Strategy proposals retain attribution after portfolio netting.
- [ ] A007 Strategy TTL expiration is deterministic under live and simulated clocks.
- [ ] A008 The Strategy Coordinator exposes conflicts and the policy used to resolve them.
- [ ] A009 Autonomous plans may only reference valid, unexpired proposals or finite system actions.
- [ ] A010 Autonomous-plan scale factors and TTLs are range-checked before execution.
- [ ] A011 OpenAI-compatible provider configuration accepts a custom `base_url`.
- [ ] A012 Responses-style requests are supported when provider capability is enabled.
- [ ] A013 Chat-Completions-style requests are supported as a compatibility path.
- [ ] A014 Streaming interruption produces a typed recoverable failure, never partial action execution.
- [ ] A015 LLM structured output is schema-validated before becoming an internal action.
- [ ] A016 LLM provider failure leaves charts, metrics, deterministic strategies, and manual orders usable.
- [ ] A017 Trading-relevant LLM traces record provider, model, prompt version, and context hash.
- [ ] A018 LLM news summaries are cached by stable content identity and freshness policy.
- [ ] A019 NewsAPI provider supports broad discovery and current-headline query modes.
- [ ] A020 Yahoo Finance market/news logic is isolated behind a replaceable provider adapter.
- [ ] A021 News providers normalize into one canonical `NewsItem` schema.
- [ ] A022 News deduplication prevents syndication copies from flooding the feed.
- [ ] A023 News clustering groups multiple articles describing the same event.
- [ ] A024 Relevant News ranking works even when all LLM providers are disabled.
- [ ] A025 All News remains scrollable/paginated independently from Relevant News.
- [ ] A026 Every chart can request symbol/timeframe-specific news context.
- [ ] A027 Context graph queries can link issuer, instrument, event, metric, strategy, and portfolio nodes.
- [ ] A028 Hybrid graph/vector retrieval records the embedding model/version used.
- [ ] A029 Native terminal restores presentation preferences without changing trading state.
- [ ] A030 Every required function is reachable by mnemonic and keyboard-only navigation.
- [ ] A031 Symbol/timeframe context propagates only through explicit terminal selection state.
- [ ] A032 Terminal chart rendering supports price, volume, strategy, and news series.
- [ ] A033 Chart news markers resolve the exact linked news/event object.
- [ ] A034 Long news/watchlist/tape functions use bounded visible-row rendering.
- [ ] A035 Terminal redraw and input latency meet the documented performance targets.
- [ ] A036 Terminal crash cannot corrupt the order journal or broker state.
- [ ] A037 Manual proposals can prefill an order/target preview without submitting it.
- [ ] A038 Autonomy Console displays current plan, selected proposals, provider/model, and next reconsideration.
- [ ] A039 Point-in-time replay never exposes news before its recorded availability timestamp.
- [ ] A040 Historical LLM outputs are replayed from pinned/cache artifacts when deterministic validation requires it.
- [ ] A041 Golden replay produces the same final state hash with the same inputs and seeds.
- [ ] A042 Risk engine may resize or deny strategy/LLM requests and never depends on LLM confidence alone.
- [ ] A043 Unknown order acknowledgement state triggers reconciliation before any resend.
- [ ] A044 Client order IDs remain stable under retry semantics supported by the broker adapter.
- [ ] A045 TCA records arrival price, send/ack timing, fills, spread, and implementation shortfall.
- [ ] A046 Supervision can quarantine a crashing metric, strategy, news provider, or LLM provider independently.
- [ ] A047 CI includes provider contract fixtures so external APIs are not required for deterministic tests.
- [ ] A048 Hot-path performance tests run with telemetry, risk, and journaling enabled.
- [ ] A049 Production strategy and model references use immutable versions/hashes rather than `latest`.
- [ ] A050 A complete TraceId reconstructs market/news → metrics → strategies → decision → risk → orders → fills.
