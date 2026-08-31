use std::fs::File;
use std::io::Read;
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use insider_autonomy::Mode;
use insider_broker_api::OrderType;
use insider_cfg_core::Value as ConfigValue;
use insider_common_types::{InstrumentId, ProposalId, TraceId};
use insider_engine::command::{
    alert_ack_command_payload, alerts_get_command_payload, autonomy_mode_command_payload,
    backtest_list_command_payload, broker_status_command_payload, cancel_command_payload,
    config_reload_command_payload, config_status_command_payload, context_search_command_payload,
    experiment_list_command_payload, llm_complete_command_payload, llm_stream_command_payload,
    metric_lifecycle_transition_command_payload, metric_registry_list_command_payload,
    model_list_command_payload, news_detail_command_payload, news_page_command_payload,
    news_provider_status_command_payload, preview_command_payload_with_order,
    proposal_preview_command_payload, proposal_submit_command_payload,
    resolve_symbol_command_payload, risk_policy_status_command_payload,
    risk_state_transition_command_payload, snapshot_command_payload,
    strategy_execution_list_command_payload, strategy_lifecycle_transition_command_payload,
    strategy_registry_list_command_payload, strategy_resolution_list_command_payload,
    submit_preview_payload, supervisor_status_command_payload, trace_events_command_payload,
};
use insider_llm_core::{Endpoint, Request as LlmRequest};
use insider_metric_host::Lifecycle as MetricLifecycle;
use insider_strategy_host::Lifecycle as StrategyLifecycle;
use sha2::{Digest, Sha256};

use crate::chart::{CHART_WINDOWS, ChartInterval, ChartOverlays, ChartStyle, Overlay, zoom_window};
use crate::client::EngineClient;
use crate::command_line::{CommandLine, is_function};
use crate::model::{
    AlertView, AnalystView, BacktestView, ContextHitView, ExperimentView, MarketView, MetricView,
    ModelView, NewsDetailView, NewsView, PreviewView, ResolutionView, ResolvedInstrumentView,
    RuntimeView, StrategyExecutionView, StrategyView, TraceView, decode_alerts, decode_analyst,
    decode_backtests, decode_context_hits, decode_experiments, decode_metrics, decode_models,
    decode_news, decode_news_detail, decode_preview, decode_proposal_submit, decode_resolutions,
    decode_resolved_instrument, decode_snapshot, decode_strategies, decode_strategy_execution,
    decode_string, decode_trace,
};

const ALL_ASSET_CLASSES: u16 = 0b11_1111;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Page {
    Home,
    Market,
    Chart,
    Screener,
    Portfolio,
    Orders,
    Tca,
    Depth,
    Tape,
    Strategies,
    Metrics,
    News,
    Risk,
    Autonomy,
    Alerts,
    Health,
    Trace,
    Search,
    Analyst,
    LlmControl,
    Backtests,
    Models,
    Attribution,
    Experiments,
    Help,
}

impl Page {
    pub const fn title(self) -> &'static str {
        match self {
            Self::Home => "HOME",
            Self::Market => "MARKET MONITOR",
            Self::Chart => "OHLCV CHART",
            Self::Screener => "MARKET SCREENER",
            Self::Portfolio => "PORTFOLIO",
            Self::Orders => "ORDERS & EXECUTION",
            Self::Tca => "TRANSACTION COST ANALYSIS",
            Self::Depth => "MARKET DEPTH",
            Self::Tape => "TIME & SALES",
            Self::Strategies => "STRATEGY COORDINATOR",
            Self::Metrics => "METRIC REGISTRY",
            Self::News => "NEWS",
            Self::Risk => "RISK",
            Self::Autonomy => "AUTONOMY",
            Self::Alerts => "ALERTS",
            Self::Health => "SYSTEM HEALTH",
            Self::Trace => "DECISION TRACE",
            Self::Search => "CONTEXT SEARCH",
            Self::Analyst => "AI ANALYST",
            Self::LlmControl => "LLM CONTROL",
            Self::Backtests => "BACKTEST REGISTRY",
            Self::Models => "MODEL REGISTRY",
            Self::Attribution => "STRATEGY ATTRIBUTION",
            Self::Experiments => "EXPERIMENT REGISTRY",
            Self::Help => "FUNCTION DIRECTORY",
        }
    }

    pub const fn mnemonic(self) -> &'static str {
        match self {
            Self::Home => "HOME",
            Self::Market => "MARKET",
            Self::Chart => "CHART",
            Self::Screener => "SCREEN",
            Self::Portfolio => "PORT",
            Self::Orders => "ORDERS",
            Self::Tca => "TCA",
            Self::Depth => "DEPTH",
            Self::Tape => "TAPE",
            Self::Strategies => "STRAT",
            Self::Metrics => "METRICS",
            Self::News => "NEWS",
            Self::Risk => "RISK",
            Self::Autonomy => "AUTO",
            Self::Alerts => "ALERTS",
            Self::Health => "HEALTH",
            Self::Trace => "TRACE",
            Self::Search => "SEARCH",
            Self::Analyst => "ANALYST",
            Self::LlmControl => "LLM",
            Self::Backtests => "BACKTESTS",
            Self::Models => "MODELS",
            Self::Attribution => "ATTRIB",
            Self::Experiments => "EXPERIMENTS",
            Self::Help => "HELP",
        }
    }

    pub fn from_mnemonic(value: &str) -> Option<Self> {
        match value {
            "HOME" => Some(Self::Home),
            "MARKET" => Some(Self::Market),
            "CHART" => Some(Self::Chart),
            "SCREEN" => Some(Self::Screener),
            "PORT" => Some(Self::Portfolio),
            "ORDERS" => Some(Self::Orders),
            "TCA" => Some(Self::Tca),
            "DEPTH" => Some(Self::Depth),
            "TAPE" => Some(Self::Tape),
            "STRAT" => Some(Self::Strategies),
            "METRICS" => Some(Self::Metrics),
            "NEWS" => Some(Self::News),
            "RISK" => Some(Self::Risk),
            "AUTO" => Some(Self::Autonomy),
            "ALERTS" => Some(Self::Alerts),
            "HEALTH" => Some(Self::Health),
            "TRACE" => Some(Self::Trace),
            "SEARCH" => Some(Self::Search),
            "ANALYST" => Some(Self::Analyst),
            "LLM" | "LLMCONTROL" => Some(Self::LlmControl),
            "BACKTESTS" => Some(Self::Backtests),
            "MODELS" => Some(Self::Models),
            "ATTRIB" => Some(Self::Attribution),
            "EXPERIMENTS" => Some(Self::Experiments),
            "HELP" => Some(Self::Help),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScreenerMode {
    All,
    Movers,
    Gainers,
    Losers,
    Volume,
    Spread,
    Stale,
}

impl ScreenerMode {
    pub const fn name(self) -> &'static str {
        match self {
            Self::All => "ALL",
            Self::Movers => "MOVERS",
            Self::Gainers => "GAINERS",
            Self::Losers => "LOSERS",
            Self::Volume => "VOLUME",
            Self::Spread => "SPREAD",
            Self::Stale => "STALE",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_uppercase().as_str() {
            "ALL" => Some(Self::All),
            "MOVERS" | "MOVE" => Some(Self::Movers),
            "GAINERS" | "GAIN" => Some(Self::Gainers),
            "LOSERS" | "LOSE" => Some(Self::Losers),
            "VOLUME" | "VOL" => Some(Self::Volume),
            "SPREAD" | "WIDE" => Some(Self::Spread),
            "STALE" | "BAD" => Some(Self::Stale),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScreenerRow {
    pub instrument: u128,
    pub bid: Option<i64>,
    pub ask: Option<i64>,
    pub last: Option<i64>,
    pub change_bps: Option<i64>,
    pub spread_bps: Option<i64>,
    pub volume: i64,
    pub quote_quality: String,
    pub trade_quality: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SecurityFunction {
    Market,
    Chart,
    Depth,
    Tape,
    News,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SecurityFirst<'a> {
    symbol: &'a str,
    asset_mask: u16,
    function: SecurityFunction,
}

enum PendingPreview {
    Manual {
        bytes: Vec<u8>,
        preview: PreviewView,
    },
    Proposal {
        proposal_id: ProposalId,
        scale: f64,
        trace_id: TraceId,
        preview: PreviewView,
    },
}

#[allow(clippy::struct_excessive_bools)]
pub struct App {
    pub client: EngineClient,
    pub page: Page,
    pub command_line: CommandLine,
    pub status: String,
    pub status_is_error: bool,
    pub runtime_connected: bool,
    pub show_connection_help: bool,
    pub runtime: RuntimeView,
    pub strategies: Vec<StrategyView>,
    pub metrics: Vec<MetricView>,
    pub alerts: Vec<AlertView>,
    pub news: Vec<NewsView>,
    pub news_scope: String,
    pub news_sort: String,
    pub news_cursor: Option<String>,
    pub news_next_cursor: Option<String>,
    pub news_cursor_history: Vec<Option<String>>,
    pub news_selected: usize,
    pub news_detail: Option<NewsDetailView>,
    pub trace: Vec<TraceView>,
    pub context_hits: Vec<ContextHitView>,
    pub analyst: AnalystView,
    pub analyst_question: String,
    pub analyst_pending_since: Option<Instant>,
    pub analyst_completed_at: Option<Instant>,
    pub analyst_stale_after: Duration,
    pub backtests: Vec<BacktestView>,
    pub models: Vec<ModelView>,
    pub resolutions: Vec<ResolutionView>,
    pub strategy_execution: Vec<StrategyExecutionView>,
    pub experiments: Vec<ExperimentView>,
    pub selected_instrument: Option<u128>,
    pub market_selected: usize,
    pub chart_window: usize,
    pub chart_offset: usize,
    pub chart_interval: ChartInterval,
    pub chart_style: ChartStyle,
    pub chart_overlays: ChartOverlays,
    /// Selected interval bar counted back from the latest visible interval bar.
    /// `None` hides the chart crosshair; this is presentation state only.
    pub chart_cursor_from_latest: Option<usize>,
    pub screener_mode: ScreenerMode,
    pub screener_rows: Vec<ScreenerRow>,
    pub screener_selected: usize,
    pub selected_symbol: String,
    pub scroll: usize,
    pub should_quit: bool,
    pub last_refresh: Instant,
    pub refresh_interval: Duration,
    pub health_lines: Vec<String>,
    /// One-shot request consumed by the native event loop. The browser workspace
    /// is presentation-only and is deliberately not owned by application state.
    pub browser_chart_requested: Option<()>,
    /// Presentation-only palette selected by the operator. Runtime state is
    /// unaffected; values are intentionally bounded to the built-in themes.
    pub theme: String,
    pub llm_system_prompt: String,
    risk_drawdown_limit_bps: Option<i64>,
    drawdown_warning_sent: bool,
    pending_preview: Option<PendingPreview>,
    analyst_receiver: Option<Receiver<Result<Vec<u8>, String>>>,
}

impl App {
    pub fn new(client: EngineClient, refresh_interval: Duration) -> Self {
        Self {
            client,
            page: Page::Home,
            command_line: CommandLine::default(),
            status: "CONNECTED — type HELP and press Enter/GO".into(),
            status_is_error: false,
            runtime_connected: false,
            show_connection_help: false,
            runtime: RuntimeView::default(),
            strategies: Vec::new(),
            metrics: Vec::new(),
            alerts: Vec::new(),
            news: Vec::new(),
            news_scope: "relevant".into(),
            news_sort: "RELEVANCE".into(),
            news_cursor: None,
            news_next_cursor: None,
            news_cursor_history: Vec::new(),
            news_selected: 0,
            news_detail: None,
            trace: Vec::new(),
            context_hits: Vec::new(),
            analyst: AnalystView::default(),
            analyst_question: String::new(),
            analyst_pending_since: None,
            analyst_completed_at: None,
            analyst_stale_after: Duration::from_millis(300_000),
            backtests: Vec::new(),
            models: Vec::new(),
            resolutions: Vec::new(),
            strategy_execution: Vec::new(),
            experiments: Vec::new(),
            selected_instrument: None,
            market_selected: 0,
            chart_window: 120,
            chart_offset: 0,
            chart_interval: ChartInterval::default(),
            chart_style: ChartStyle::default(),
            chart_overlays: ChartOverlays::default(),
            chart_cursor_from_latest: Some(0),
            screener_mode: ScreenerMode::Movers,
            screener_rows: Vec::new(),
            screener_selected: 0,
            selected_symbol: String::new(),
            scroll: 0,
            should_quit: false,
            last_refresh: Instant::now()
                .checked_sub(refresh_interval)
                .unwrap_or_else(Instant::now),
            refresh_interval,
            health_lines: Vec::new(),
            browser_chart_requested: None,
            theme: "AMBER".into(),
            llm_system_prompt: String::new(),
            risk_drawdown_limit_bps: None,
            drawdown_warning_sent: false,
            pending_preview: None,
            analyst_receiver: None,
        }
    }

    pub fn refresh_if_due(&mut self) {
        if self.last_refresh.elapsed() >= self.refresh_interval
            && let Err(error) = self.refresh_runtime()
        {
            self.runtime_connected = false;
            self.status = format!("WAITING FOR RUNTIME — reconnecting ({error})");
            self.status_is_error = false;
        }
    }

    pub fn poll_background(&mut self) {
        let result = match self.analyst_receiver.as_ref().map(Receiver::try_recv) {
            Some(Ok(result)) => Some(result),
            Some(Err(TryRecvError::Disconnected)) => {
                Some(Err("analyst worker disconnected without a result".into()))
            }
            Some(Err(TryRecvError::Empty)) | None => None,
        };
        if let Some(result) = result {
            self.analyst_receiver = None;
            self.finish_analyst(result);
        }
    }

    pub fn wait_for_analyst(&mut self, timeout: Duration) -> Result<(), String> {
        let Some(receiver) = self.analyst_receiver.take() else {
            return Ok(());
        };
        match receiver.recv_timeout(timeout) {
            Ok(result) => {
                self.finish_analyst(result);
                if self.status_is_error {
                    Err(self.status.clone())
                } else {
                    Ok(())
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                self.analyst_pending_since = None;
                Err("analyst request did not finish within 60 seconds".into())
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.analyst_pending_since = None;
                Err("analyst worker disconnected without a result".into())
            }
        }
    }

    pub const fn analyst_is_pending(&self) -> bool {
        self.analyst_pending_since.is_some()
    }

    fn finish_analyst(&mut self, result: Result<Vec<u8>, String>) {
        self.analyst_pending_since = None;
        match result.and_then(|response| decode_analyst(&response)) {
            Ok(analyst) => {
                self.analyst = analyst;
                self.analyst_completed_at = Some(Instant::now());
                self.status = "ANALYST RESPONSE COMPLETE".into();
                self.status_is_error = false;
            }
            Err(error) => {
                self.analyst.finish_reason = "ERROR".into();
                self.analyst.content = format!("Analyst unavailable: {error}");
                self.analyst_completed_at = Some(Instant::now());
                self.status = format!("ANALYST UNAVAILABLE — {error}");
                self.status_is_error = true;
            }
        }
    }

    pub fn refresh_runtime(&mut self) -> Result<(), String> {
        let payload = self.client.request(snapshot_command_payload().to_vec())?;
        self.runtime = decode_snapshot(&payload)?;
        if !self.runtime_connected {
            self.show_connection_help = true;
        }
        self.runtime_connected = true;
        let previous_alerts = self
            .alerts
            .iter()
            .map(|alert| alert.id.clone())
            .collect::<std::collections::HashSet<_>>();
        if let Ok(()) = self.load_alerts() {
            self.notify_new_alerts(&previous_alerts);
        }
        if let Err(error) = self.load_terminal_settings() {
            self.status = format!("CONNECTED — SETTINGS UNAVAILABLE — {error}");
            self.status_is_error = false;
        }
        if let (Some(limit), Some(drawdown)) =
            (self.risk_drawdown_limit_bps, self.runtime.drawdown_bps)
            && limit > 0
            && drawdown >= limit.saturating_mul(80) / 100
            && !self.drawdown_warning_sent
        {
            self.drawdown_warning_sent = true;
            let message =
                format!("Drawdown is {drawdown} bps against configured {limit} bps limit");
            self.status = format!("RISK WARNING — {message}");
            self.status_is_error = true;
            let _ = std::process::Command::new("notify-send")
                .args(["InsiderTrader liquidation-risk warning", message.as_str()])
                .spawn();
        } else if self.runtime.drawdown_bps.is_some_and(|drawdown| {
            self.risk_drawdown_limit_bps
                .is_some_and(|limit| drawdown < limit.saturating_mul(70) / 100)
        }) {
            self.drawdown_warning_sent = false;
        }
        self.reconcile_market_selection();
        if self.page == Page::Screener {
            self.rebuild_screener(false);
        }
        self.last_refresh = Instant::now();
        Ok(())
    }

    fn notify_new_alerts(&mut self, previous: &std::collections::HashSet<String>) {
        let mut newest = None;
        for alert in &self.alerts {
            if previous.contains(&alert.id) {
                continue;
            }
            if newest
                .as_ref()
                .is_none_or(|current: &&AlertView| alert.occurred_ms > current.occurred_ms)
            {
                newest = Some(alert);
            }
            if alert.severity >= 3 {
                let message = format!("[{}] {}", alert.source, alert.message);
                let _ = std::process::Command::new("notify-send")
                    .args(["InsiderTrader critical alert", message.as_str()])
                    .spawn();
            }
        }
        if let Some(alert) = newest {
            self.status = format!("ALERT [{}] {}", alert.source, alert.message);
            self.status_is_error = alert.severity >= 4;
        }
    }

    pub fn load_terminal_settings(&mut self) -> Result<(), String> {
        let response = self
            .client
            .request(config_status_command_payload().to_vec())?;
        let (_, text) = decode_config(&response)?;
        let settings = insider_cfg_core::parse_cfg(&text)
            .map_err(|error| format!("terminal configuration: {error}"))?;
        self.analyst_stale_after = configured_analyst_stale_after(&settings)?;
        self.theme = configured_terminal_theme(&settings)?;
        self.news_sort = configured_news_sort(&settings)?;
        self.llm_system_prompt = configured_llm_prompt(&settings)?;
        self.risk_drawdown_limit_bps = configured_drawdown_limit(&settings)?;
        Ok(())
    }

    pub fn run_command(&mut self) {
        let Some(command) = self.command_line.submit() else {
            return;
        };
        let result = self.dispatch(&command);
        if let Err(error) = result {
            self.fail(error);
        }
    }

    pub fn run_shortcut(&mut self, command: &str) {
        if let Err(error) = self.command_line.set(command) {
            self.fail(error);
            return;
        }
        self.run_command();
    }

    pub fn execute_command(&mut self, command: &str) -> Result<(), String> {
        if command.trim().is_empty() {
            return Err("command is empty".into());
        }
        self.dispatch(command.trim())
    }

    pub fn restore_page(&mut self, page: Page) -> Result<(), String> {
        self.scroll = 0;
        self.reconcile_market_selection();
        match page {
            Page::Strategies => self.load_strategies()?,
            Page::Metrics => self.load_metrics()?,
            Page::Screener => self.rebuild_screener(true),
            Page::News => {
                self.load_news_page(None)?;
            }
            Page::Alerts => self.load_alerts()?,
            Page::Health => self.load_health(),
            Page::Backtests => {
                let response = self
                    .client
                    .request(backtest_list_command_payload().to_vec())?;
                self.backtests = decode_backtests(&response)?;
            }
            Page::Models => {
                let response = self.client.request(model_list_command_payload().to_vec())?;
                self.models = decode_models(&response)?;
            }
            Page::Attribution => {
                let response = self
                    .client
                    .request(strategy_resolution_list_command_payload().to_vec())?;
                self.resolutions = decode_resolutions(&response)?;
                let response = self
                    .client
                    .request(strategy_execution_list_command_payload().to_vec())?;
                self.strategy_execution = decode_strategy_execution(&response)?;
            }
            Page::Experiments => {
                let response = self
                    .client
                    .request(experiment_list_command_payload().to_vec())?;
                self.experiments = decode_experiments(&response)?;
            }
            Page::Home
            | Page::Market
            | Page::Chart
            | Page::Portfolio
            | Page::Orders
            | Page::Tca
            | Page::Depth
            | Page::Tape
            | Page::Risk
            | Page::Autonomy
            | Page::Trace
            | Page::Search
            | Page::Analyst
            | Page::LlmControl
            | Page::Help => {}
        }
        self.page = page;
        Ok(())
    }

    pub fn scroll_by(&mut self, delta: isize) {
        if self.page == Page::Market {
            self.move_market_selection(delta);
            return;
        }
        if self.page == Page::Screener {
            self.move_screener_selection(delta);
            return;
        }
        if self.page == Page::News && self.news_detail.is_none() {
            self.move_news_selection(delta);
            return;
        }
        self.scroll = if delta < 0 {
            self.scroll.saturating_sub(delta.unsigned_abs())
        } else {
            self.scroll
                .saturating_add(delta.unsigned_abs())
                .min(self.maximum_scroll())
        };
    }

    pub fn activate_selection(&mut self) -> Result<bool, String> {
        if self.page == Page::Market {
            self.select_instrument(None)?;
            self.chart_offset = 0;
            self.chart_cursor_from_latest = Some(0);
            self.page = Page::Chart;
            return Ok(true);
        }
        if self.page == Page::Screener {
            let instrument = self
                .screener_rows
                .get(self.screener_selected)
                .map(|row| row.instrument)
                .ok_or("the screener has no selected instrument")?;
            self.select_instrument(Some(&instrument.to_string()))?;
            self.chart_offset = 0;
            self.chart_cursor_from_latest = Some(0);
            self.page = Page::Chart;
            return Ok(true);
        }
        if self.page == Page::Autonomy {
            let proposal = self
                .runtime
                .proposals
                .get(self.scroll)
                .ok_or("the proposal list has no selected proposal")?;
            let proposal_id = proposal.id.to_string();
            self.preview_proposal(&proposal_id, None)?;
            return Ok(true);
        }
        if self.page != Page::News || self.news_detail.is_some() {
            return Ok(false);
        }
        self.open_news_detail(None)?;
        Ok(true)
    }

    pub fn pan_chart(&mut self, delta: isize) {
        let maximum = self
            .selected_instrument
            .and_then(|identity| {
                self.runtime
                    .markets
                    .iter()
                    .find(|market| market.instrument == identity)
            })
            .map_or(0, |market| market.bars.len().saturating_sub(1));
        self.chart_offset = if delta < 0 {
            self.chart_offset.saturating_sub(delta.unsigned_abs())
        } else {
            self.chart_offset
                .saturating_add(delta.unsigned_abs())
                .min(maximum)
        };
    }

    /// Pans in visible interval-bar units while retaining a source-bar offset.
    pub fn pan_chart_intervals(&mut self, delta: isize) {
        let factor = isize::try_from(self.chart_interval.factor()).unwrap_or(isize::MAX);
        self.pan_chart(delta.saturating_mul(factor));
        self.page = Page::Chart;
        self.status = format!(
            "CHART PAN — OFFSET {} SOURCE BARS / INTERVAL {}",
            self.chart_offset,
            self.chart_interval.name()
        );
        self.status_is_error = false;
    }

    pub fn zoom_chart(&mut self, inward: bool) {
        self.chart_window = zoom_window(self.chart_window, inward);
        self.chart_cursor_from_latest = Some(0);
        self.page = Page::Chart;
        self.status = format!("CHART WINDOW — {} SOURCE BARS", self.chart_window);
        self.status_is_error = false;
    }

    /// Moves the crosshair in interval-bar units. Positive values move older.
    pub fn move_chart_crosshair(&mut self, delta: isize) {
        let count = self.visible_interval_bar_count();
        if count == 0 {
            self.chart_cursor_from_latest = None;
            self.status = "CHART CROSSHAIR — NO VISIBLE BARS".into();
            self.status_is_error = false;
            return;
        }
        let current = self.chart_cursor_from_latest.unwrap_or(0).min(count - 1);
        let next = if delta < 0 {
            current.saturating_sub(delta.unsigned_abs())
        } else {
            current.saturating_add(delta.unsigned_abs()).min(count - 1)
        };
        self.chart_cursor_from_latest = Some(next);
        self.page = Page::Chart;
        self.status = format!("CHART CROSSHAIR — {next} INTERVAL BARS FROM LATEST");
        self.status_is_error = false;
    }

    pub fn reset_chart(&mut self) {
        self.chart_window = 120;
        self.chart_offset = 0;
        self.chart_interval = ChartInterval::default();
        self.chart_style = ChartStyle::default();
        self.chart_overlays = ChartOverlays::default();
        self.chart_cursor_from_latest = Some(0);
        self.page = Page::Chart;
        self.status = "CHART RESET — 120 BARS / 1x / CANDLE / SMA20,VWAP".into();
        self.status_is_error = false;
    }

    fn visible_interval_bar_count(&self) -> usize {
        let source_count = self
            .selected_instrument
            .and_then(|identity| {
                self.runtime
                    .markets
                    .iter()
                    .find(|market| market.instrument == identity)
            })
            .map_or(0, |market| {
                market
                    .bars
                    .len()
                    .saturating_sub(self.chart_offset)
                    .min(self.chart_window)
            });
        source_count.div_ceil(self.chart_interval.factor())
    }

    pub fn dismiss_overlay(&mut self) -> bool {
        if self.page == Page::News && self.news_detail.take().is_some() {
            self.scroll = 0;
            return true;
        }
        false
    }

    fn maximum_scroll(&self) -> usize {
        match self.page {
            Page::Market => self.runtime.markets.len().saturating_sub(1),
            Page::Screener => self.screener_rows.len().saturating_sub(1),
            Page::Portfolio => self.runtime.positions.len().saturating_sub(1),
            Page::Orders => self.runtime.orders.len().saturating_sub(1),
            Page::Tca => self.runtime.tca.len().saturating_sub(1),
            Page::Tape => self
                .selected_instrument
                .and_then(|identity| {
                    self.runtime
                        .markets
                        .iter()
                        .find(|market| market.instrument == identity)
                })
                .map_or(0, |market| market.trades.len().saturating_sub(1)),
            Page::Strategies => self.strategies.len().saturating_sub(1),
            Page::Metrics => self.metrics.len().saturating_sub(1),
            Page::News => self.news_detail.as_ref().map_or_else(
                || self.news.len().saturating_sub(1),
                |detail| {
                    detail
                        .current
                        .summary
                        .as_deref()
                        .unwrap_or_default()
                        .lines()
                        .count()
                        .saturating_sub(1)
                },
            ),
            Page::Autonomy => self.runtime.proposals.len().saturating_sub(1),
            Page::Alerts => self.alerts.len().saturating_sub(1),
            Page::Trace => self.trace.len().saturating_sub(1),
            Page::Search => self.context_hits.len().saturating_sub(1),
            Page::Analyst => self.analyst.content.lines().count().saturating_sub(1),
            Page::Backtests => self.backtests.len().saturating_sub(1),
            Page::Models => self.models.len().saturating_sub(1),
            Page::Attribution => self
                .resolutions
                .len()
                .max(self.strategy_execution.len())
                .saturating_sub(1),
            Page::Experiments => self.experiments.len().saturating_sub(1),
            Page::LlmControl
            | Page::Home
            | Page::Chart
            | Page::Depth
            | Page::Risk
            | Page::Health
            | Page::Help => 0,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn dispatch(&mut self, command: &str) -> Result<(), String> {
        let words = command.split_whitespace().collect::<Vec<_>>();
        if let Some(request) = parse_security_first(&words)? {
            return self.open_security_first(request);
        }
        let function = words[0].to_ascii_uppercase();
        let previous_status = self.status.clone();
        self.scroll = 0;
        match function.as_str() {
            "HOME" | "MENU" => self.page = Page::Home,
            "MARKET" | "MKTS" | "DES" => {
                self.page = Page::Market;
                if let Some(identity) = words.get(1) {
                    self.select_instrument(Some(identity))?;
                }
            }
            "GP" => {
                self.select_instrument(words.get(1).copied())?;
                self.chart_offset = 0;
                self.chart_cursor_from_latest = Some(0);
                self.page = Page::Chart;
            }
            "CHART" | "TV" | "TRADINGVIEW" => {
                self.select_instrument(words.get(1).copied())?;
                self.chart_offset = 0;
                self.chart_cursor_from_latest = Some(0);
                self.page = Page::Chart;
                self.browser_chart_requested = Some(());
                self.status = "LOCAL BROWSER CHART REQUESTED".into();
                self.status_is_error = false;
            }
            "SCREEN" | "SCREENER" | "SCAN" => {
                self.screener_mode = words.get(1).map_or(Ok(ScreenerMode::Movers), |value| {
                    ScreenerMode::parse(value)
                        .ok_or("usage: SCREEN <ALL|MOVERS|GAINERS|LOSERS|VOLUME|SPREAD|STALE>")
                })?;
                self.rebuild_screener(true);
                self.page = Page::Screener;
            }
            "ZOOM" => {
                self.set_chart_window(required(&words, 1, "usage: ZOOM <30|60|120|240|480|960>")?)?;
            }
            "INTERVAL" | "TIMEFRAME" | "TF" | "AGG" => {
                let raw = required(
                    &words,
                    1,
                    "usage: INTERVAL <1|5|15|30|60> (source-bar multiplier)",
                )?;
                self.chart_interval = ChartInterval::parse(raw)
                    .ok_or("chart interval must be a source-bar multiplier: 1, 5, 15, 30, or 60")?;
                self.chart_cursor_from_latest = Some(0);
                self.page = Page::Chart;
                self.status = format!(
                    "CHART INTERVAL — {} SOURCE BARS PER DISPLAY BAR",
                    self.chart_interval.factor()
                );
                self.status_is_error = false;
            }
            "STYLE" | "CHARTSTYLE" => {
                let raw = required(&words, 1, "usage: STYLE <CANDLE|OHLC|LINE>")?;
                self.chart_style =
                    ChartStyle::parse(raw).ok_or("chart style must be CANDLE, OHLC, or LINE")?;
                self.page = Page::Chart;
                self.status = format!("CHART STYLE — {}", self.chart_style.name());
                self.status_is_error = false;
            }
            "OVERLAY" => {
                let raw = required(
                    &words,
                    1,
                    "usage: OVERLAY <SMA20|SMA50|VWAP> [ON|OFF|TOGGLE] | OVERLAY <CLEAR|DEFAULT>",
                )?;
                match raw.to_ascii_uppercase().as_str() {
                    "CLEAR" | "NONE" => self.chart_overlays = ChartOverlays::none(),
                    "DEFAULT" => self.chart_overlays = ChartOverlays::default(),
                    _ => {
                        let overlay = Overlay::parse(raw)
                            .ok_or("chart overlay must be SMA20, SMA50, VWAP, CLEAR, or DEFAULT")?;
                        let action = words.get(2).map(|value| value.to_ascii_uppercase());
                        match action.as_deref() {
                            None | Some("TOGGLE") => {
                                let _ = self.chart_overlays.toggle(overlay);
                            }
                            Some("ON") => self.chart_overlays.set(overlay, true),
                            Some("OFF") => {
                                self.chart_overlays.set(overlay, false);
                            }
                            Some(_) => {
                                return Err("overlay action must be ON, OFF, or TOGGLE".into());
                            }
                        }
                    }
                }
                self.page = Page::Chart;
                self.status = format!("CHART OVERLAYS — {}", self.chart_overlays.legend());
                self.status_is_error = false;
            }
            "XHAIR" | "CROSSHAIR" => {
                match required(&words, 1, "usage: XHAIR <OLDER|NEWER|LATEST|OFF>")?
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "OLDER" | "LEFT" => self.move_chart_crosshair(1),
                    "NEWER" | "RIGHT" => self.move_chart_crosshair(-1),
                    "LATEST" | "ON" => {
                        self.chart_cursor_from_latest = Some(0);
                        self.status = "CHART CROSSHAIR — LATEST INTERVAL BAR".into();
                        self.status_is_error = false;
                    }
                    "OFF" => {
                        self.chart_cursor_from_latest = None;
                        self.status = "CHART CROSSHAIR — OFF".into();
                        self.status_is_error = false;
                    }
                    _ => {
                        return Err(
                            "crosshair direction must be OLDER, NEWER, LATEST, or OFF".into()
                        );
                    }
                }
                self.page = Page::Chart;
            }
            "CHARTRESET" => self.reset_chart(),
            "PAN" => {
                let direction = required(&words, 1, "usage: PAN <OLDER|NEWER> [bars]")?;
                let amount = words.get(2).map_or(Ok(10_usize), |value| {
                    value
                        .parse::<usize>()
                        .map_err(|_| "pan bars must be a positive integer")
                })?;
                if amount == 0 || amount > 4_096 {
                    return Err("pan bars must be 1..4096".into());
                }
                let amount = isize::try_from(amount).map_err(|_| "pan bars exceed bound")?;
                match direction.to_ascii_uppercase().as_str() {
                    "OLDER" | "LEFT" => self.pan_chart_intervals(amount),
                    "NEWER" | "RIGHT" => self.pan_chart_intervals(-amount),
                    _ => return Err("pan direction must be OLDER or NEWER".into()),
                }
            }
            "PORT" | "PORTFOLIO" | "POS" => self.page = Page::Portfolio,
            "ORD" | "ORDERS" | "EMSX" => self.page = Page::Orders,
            "TCA" => self.page = Page::Tca,
            "DEPTH" | "BOOK" => {
                self.select_instrument(words.get(1).copied())?;
                self.page = Page::Depth;
            }
            "TAPE" | "TS" => {
                self.select_instrument(words.get(1).copied())?;
                self.page = Page::Tape;
            }
            "STRAT" | "STRATEGIES" => {
                self.load_strategies()?;
                self.page = Page::Strategies;
            }
            "METRIC" | "METRICS" => {
                self.load_metrics()?;
                self.page = Page::Metrics;
            }
            "NEWS" | "N" => {
                if words
                    .get(1)
                    .is_some_and(|value| value.eq_ignore_ascii_case("SORT"))
                {
                    let sort = words
                        .get(2)
                        .ok_or("usage: NEWS SORT <RELEVANCE|RECENCY|SOURCE>")?
                        .to_ascii_uppercase();
                    if !matches!(sort.as_str(), "RELEVANCE" | "RECENCY" | "SOURCE") {
                        return Err("news sort must be RELEVANCE, RECENCY, or SOURCE".into());
                    }
                    self.news_sort = sort;
                    self.news_cursor = None;
                    self.news_cursor_history.clear();
                    self.load_news_page(None)?;
                    self.page = Page::News;
                    self.status = format!("NEWS SORT — {}", self.news_sort);
                    self.status_is_error = false;
                    return Ok(());
                }
                let (scope, symbol) = match words.get(1).map(|value| value.to_ascii_uppercase()) {
                    Some(value) if value == "ALL" || value == "RELEVANT" => {
                        (value.to_ascii_lowercase(), words.get(2).copied())
                    }
                    _ => ("relevant".into(), words.get(1).copied()),
                };
                self.news_scope = scope;
                if let Some(symbol) = symbol {
                    self.selected_symbol = symbol.to_ascii_uppercase();
                }
                self.news_cursor = None;
                self.news_cursor_history.clear();
                self.news_detail = None;
                self.load_news_page(None)?;
                self.page = Page::News;
            }
            "NEWSNEXT" | "NN" => self.next_news_page()?,
            "NEWSPREV" | "NP" => self.previous_news_page()?,
            "DETAIL" | "NEWSDTL" => self.open_news_detail(words.get(1).copied())?,
            "BACK" => {
                if !self.dismiss_overlay() {
                    return Err("no terminal detail view is open".into());
                }
            }
            "RISK" => self.page = Page::Risk,
            "AUTO" | "AUTONOMY" => self.page = Page::Autonomy,
            "LLM" | "LLMCONTROL" => self.page = Page::LlmControl,
            "ALERT" | "ALERTS" => {
                self.load_alerts()?;
                self.page = Page::Alerts;
            }
            "HEALTH" | "SYSTEM" => {
                self.load_health();
                self.page = Page::Health;
            }
            "TRACE" => {
                let raw = required(&words, 1, "usage: TRACE <numeric-trace-id>")?;
                let numeric = raw
                    .strip_prefix("terminal-trace-")
                    .or_else(|| raw.strip_prefix("trace-"))
                    .unwrap_or(raw)
                    .parse::<u128>()
                    .map_err(|_| "trace ID must be a positive integer")?;
                let trace = TraceId::new(numeric).map_err(|_| "trace ID must be positive")?;
                let response = self
                    .client
                    .request(trace_events_command_payload(trace).to_vec())?;
                self.trace = decode_trace(&response)?;
                self.page = Page::Trace;
            }
            "SEARCH" | "CTX" => {
                let query = command
                    .split_once(char::is_whitespace)
                    .map(|(_, value)| value.trim())
                    .filter(|value| !value.is_empty())
                    .ok_or("usage: SEARCH <text>")?;
                if query.len() > 16_384 {
                    return Err("search text exceeds bound".into());
                }
                let response = self
                    .client
                    .request(context_search_command_payload(query, None, 3, 100))?;
                self.context_hits = decode_context_hits(&response)?;
                self.page = Page::Search;
            }
            "ANALYZE" | "AI" => {
                if self.analyst_receiver.is_some() {
                    return Err("an analyst request is already running".into());
                }
                let input = command
                    .split_once(char::is_whitespace)
                    .map(|(_, value)| value.trim())
                    .filter(|value| !value.is_empty())
                    .ok_or("usage: ANALYZE <question>")?;
                if input.len() > 1_048_576 {
                    return Err("analyst input exceeds 1 MiB bound".into());
                }
                let identity = self.client.next_identity();
                let context = format!(
                    "cursor={};instrument={:?};risk={};mode={}",
                    self.runtime.cursor,
                    self.selected_instrument,
                    self.runtime.risk,
                    self.runtime.mode
                );
                let context_hash = format!("{:x}", Sha256::digest(context.as_bytes()));
                let request = LlmRequest {
                    trace_id: format!("terminal-analyst-{identity}"),
                    prompt_version: std::env::var("IT_LLM_PROMPT_VERSION")
                        .unwrap_or_else(|_| "terminal.analyst.v1".into()),
                    model: std::env::var("IT_LLM_MODEL")
                        .unwrap_or_else(|_| "configured-model".into()),
                    task: "CHART_CONTEXT".into(),
                    context_hash,
                    input: format!("Authoritative context: {context}\nOperator question: {input}"),
                    max_output_tokens: 2_048,
                    endpoint: Endpoint::Responses,
                };
                request
                    .validate()
                    .map_err(|error| format!("analyst request: {error:?}"))?;
                let stream = llm_stream_command_payload(&request);
                let fallback = llm_complete_command_payload(&request);
                self.analyst_receiver =
                    Some(self.client.request_background(stream, Some(fallback))?);
                self.analyst = AnalystView {
                    trace_id: request.trace_id,
                    finish_reason: "PENDING".into(),
                    content: String::new(),
                };
                input.clone_into(&mut self.analyst_question);
                self.analyst_pending_since = Some(Instant::now());
                self.analyst_completed_at = None;
                self.status = "ANALYST REQUEST RUNNING — market refresh remains active".into();
                self.status_is_error = false;
                self.page = Page::Analyst;
            }
            "BACKTESTS" | "BT" => {
                let response = self
                    .client
                    .request(backtest_list_command_payload().to_vec())?;
                self.backtests = decode_backtests(&response)?;
                self.page = Page::Backtests;
            }
            "MODELS" | "MODEL" => {
                let response = self.client.request(model_list_command_payload().to_vec())?;
                self.models = decode_models(&response)?;
                self.page = Page::Models;
            }
            "EXPERIMENTS" | "EXPERIMENT" | "EXP" => {
                let response = self
                    .client
                    .request(experiment_list_command_payload().to_vec())?;
                self.experiments = decode_experiments(&response)?;
                self.page = Page::Experiments;
            }
            "ATTRIB" | "RESOLUTIONS" => {
                let response = self
                    .client
                    .request(strategy_resolution_list_command_payload().to_vec())?;
                self.resolutions = decode_resolutions(&response)?;
                let response = self
                    .client
                    .request(strategy_execution_list_command_payload().to_vec())?;
                self.strategy_execution = decode_strategy_execution(&response)?;
                self.page = Page::Attribution;
            }
            "HELP" | "?" => self.page = Page::Help,
            "REFRESH" | "R" => self.refresh_runtime()?,
            "THEME" => {
                let value = required(&words, 1, "usage: THEME <AMBER|BLUE|GREEN|MONO>")?
                    .to_ascii_uppercase();
                if !matches!(value.as_str(), "AMBER" | "BLUE" | "GREEN" | "MONO") {
                    return Err("theme must be AMBER, BLUE, GREEN, GRAY, or MONO".into());
                }
                self.theme.clone_from(&value);
                self.status = format!("THEME SELECTED — {value}");
                self.status_is_error = false;
            }
            "MODE" => self.set_mode(words.get(1).copied())?,
            "ORDER" | "BUY" | "SELL" => self.preview_order(&words)?,
            "PREVIEW" | "PVIEW" => {
                let proposal = required(&words, 1, "usage: PREVIEW <proposal-id> [scale]")?;
                self.preview_proposal(proposal, words.get(2).copied())?;
            }
            "CONFIRM" => self.confirm_order()?,
            "CANCEL" => {
                let order = required(&words, 1, "usage: CANCEL <client-order-id>")?;
                let response = self.client.request_with_key(
                    &cancel_command_payload(order),
                    Some(format!("terminal-cancel-{order}")),
                )?;
                self.status = format!("CANCEL ACCEPTED — {} response bytes", response.len());
                self.status_is_error = false;
                self.refresh_runtime()?;
            }
            "ACK" => {
                let alert = required(&words, 1, "usage: ACK <alert-id>")?;
                let response = self.client.request_with_key(
                    &alert_ack_command_payload(alert),
                    Some(format!("terminal-alert-ack-{alert}")),
                )?;
                if !response.starts_with(b"IT_CMD_ALERT_ACK_RESPONSE_V1\0") {
                    return Err("invalid alert acknowledgement".into());
                }
                self.status = format!("ALERT {alert} ACKNOWLEDGED");
                self.status_is_error = false;
                self.load_alerts()?;
            }
            "HALT" => self.transition_risk(
                "halted",
                required(&words, 1, "usage: HALT <authorization>")?,
            )?,
            "RISKSTATE" => {
                let state = required(
                    &words,
                    1,
                    "usage: RISKSTATE <running|reduce|cancel|halted> <authorization>",
                )?;
                let authorization =
                    required(&words, 2, "risk state transition requires authorization")?;
                self.transition_risk(state, authorization)?;
            }
            "STRATSET" => self.transition_strategy(&words)?,
            "METRICSET" => self.transition_metric(&words)?,
            "CONFIG" => self.config_command(&words)?,
            "QUIT" | "EXIT" => self.should_quit = true,
            _ => return Err(format!("UNKNOWN FUNCTION {function} — type HELP")),
        }
        if self.status == previous_status {
            self.status = format!("{function} <GO>");
            self.status_is_error = false;
        }
        Ok(())
    }

    fn load_strategies(&mut self) -> Result<(), String> {
        let response = self
            .client
            .request(strategy_registry_list_command_payload().to_vec())?;
        self.strategies = decode_strategies(&response)?;
        Ok(())
    }

    fn select_instrument(&mut self, value: Option<&str>) -> Result<(), String> {
        if let Some(value) = value {
            let (identity, resolved) = self.resolve_instrument(value, ALL_ASSET_CLASSES)?;
            let index = self
                .runtime
                .markets
                .iter()
                .position(|market| market.instrument == identity)
                .ok_or("instrument is not present in the current market snapshot")?;
            self.selected_instrument = Some(identity);
            self.market_selected = index;
            self.selected_symbol = resolved.map_or_else(String::new, |value| value.symbol);
        }
        if self.selected_instrument.is_none() {
            return Err("no market instrument is available".into());
        }
        Ok(())
    }

    fn resolve_instrument(
        &mut self,
        value: &str,
        asset_mask: u16,
    ) -> Result<(u128, Option<ResolvedInstrumentView>), String> {
        if let Ok(identity) = value.parse::<u128>() {
            if identity == 0 {
                return Err("instrument ID must be positive".into());
            }
            return Ok((identity, None));
        }
        validate_display_symbol(value)?;
        let response = self.client.request(resolve_symbol_command_payload(
            value,
            current_utc_day()?,
            asset_mask,
        ))?;
        let resolved = decode_resolved_instrument(&response)?;
        Ok((resolved.instrument, Some(resolved)))
    }

    fn open_security_first(&mut self, request: SecurityFirst<'_>) -> Result<(), String> {
        let response = self.client.request(resolve_symbol_command_payload(
            request.symbol,
            current_utc_day()?,
            request.asset_mask,
        ))?;
        let resolved = decode_resolved_instrument(&response)?;
        self.selected_symbol.clone_from(&resolved.symbol);
        self.selected_instrument = Some(resolved.instrument);
        if request.function == SecurityFunction::News {
            self.news_scope = "relevant".into();
            self.news_cursor = None;
            self.news_cursor_history.clear();
            self.news_detail = None;
            self.load_news_page(None)?;
            self.page = Page::News;
        } else {
            self.market_selected = self
                .runtime
                .markets
                .iter()
                .position(|market| market.instrument == resolved.instrument)
                .ok_or("resolved security is not present in the current market snapshot")?;
            self.scroll = 0;
            self.chart_offset = 0;
            self.chart_cursor_from_latest = Some(0);
            self.page = match request.function {
                SecurityFunction::Market => Page::Market,
                SecurityFunction::Chart => Page::Chart,
                SecurityFunction::Depth => Page::Depth,
                SecurityFunction::Tape => Page::Tape,
                SecurityFunction::News => Page::News,
            };
        }
        self.status = format!(
            "{} {} {} — {} <GO>",
            resolved.symbol,
            resolved.asset_class,
            resolved.venue,
            self.page.mnemonic()
        );
        self.status_is_error = false;
        Ok(())
    }

    fn reconcile_market_selection(&mut self) {
        if let Some(index) = self.selected_instrument.and_then(|identity| {
            self.runtime
                .markets
                .iter()
                .position(|market| market.instrument == identity)
        }) {
            self.market_selected = index;
            return;
        }
        self.market_selected = 0;
        self.selected_instrument = self.runtime.markets.first().map(|market| market.instrument);
        self.chart_offset = 0;
        self.chart_cursor_from_latest = Some(0);
    }

    fn move_market_selection(&mut self, delta: isize) {
        const WINDOW_ROWS: usize = 12;
        let maximum = self.runtime.markets.len().saturating_sub(1);
        self.market_selected = if delta < 0 {
            self.market_selected.saturating_sub(delta.unsigned_abs())
        } else {
            self.market_selected
                .saturating_add(delta.unsigned_abs())
                .min(maximum)
        };
        self.selected_instrument = self
            .runtime
            .markets
            .get(self.market_selected)
            .map(|market| market.instrument);
        self.selected_symbol.clear();
        if self.market_selected < self.scroll {
            self.scroll = self.market_selected;
        } else if self.market_selected >= self.scroll.saturating_add(WINDOW_ROWS) {
            self.scroll = self.market_selected.saturating_sub(WINDOW_ROWS - 1);
        }
    }

    fn rebuild_screener(&mut self, reset: bool) {
        let previous = if reset {
            None
        } else {
            self.screener_rows
                .get(self.screener_selected)
                .map(|row| row.instrument)
        };
        self.screener_rows = build_screener_rows(&self.runtime.markets, self.screener_mode);
        self.screener_selected = previous
            .and_then(|instrument| {
                self.screener_rows
                    .iter()
                    .position(|row| row.instrument == instrument)
            })
            .unwrap_or(0);
        self.scroll = if reset {
            0
        } else {
            self.scroll.min(self.screener_rows.len().saturating_sub(1))
        };
        if self.screener_selected < self.scroll {
            self.scroll = self.screener_selected;
        }
        self.selected_instrument = self
            .screener_rows
            .first()
            .map(|row| row.instrument)
            .or(self.selected_instrument);
    }

    fn move_screener_selection(&mut self, delta: isize) {
        const WINDOW_ROWS: usize = 12;
        let maximum = self.screener_rows.len().saturating_sub(1);
        self.screener_selected = if delta < 0 {
            self.screener_selected.saturating_sub(delta.unsigned_abs())
        } else {
            self.screener_selected
                .saturating_add(delta.unsigned_abs())
                .min(maximum)
        };
        self.selected_instrument = self
            .screener_rows
            .get(self.screener_selected)
            .map(|row| row.instrument)
            .or(self.selected_instrument);
        self.selected_symbol.clear();
        if self.screener_selected < self.scroll {
            self.scroll = self.screener_selected;
        } else if self.screener_selected >= self.scroll.saturating_add(WINDOW_ROWS) {
            self.scroll = self.screener_selected.saturating_sub(WINDOW_ROWS - 1);
        }
    }

    fn set_chart_window(&mut self, value: &str) -> Result<(), String> {
        let window = value
            .parse::<usize>()
            .map_err(|_| "chart window must be an integer")?;
        if !CHART_WINDOWS.contains(&window) {
            return Err("chart window must be 30, 60, 120, 240, 480, or 960".into());
        }
        self.chart_window = window;
        self.chart_cursor_from_latest = Some(0);
        self.page = Page::Chart;
        self.status = format!("CHART WINDOW — {window} SOURCE BARS");
        self.status_is_error = false;
        Ok(())
    }

    fn load_metrics(&mut self) -> Result<(), String> {
        let response = self
            .client
            .request(metric_registry_list_command_payload().to_vec())?;
        self.metrics = decode_metrics(&response)?;
        Ok(())
    }

    fn load_alerts(&mut self) -> Result<(), String> {
        let response = self.client.request(alerts_get_command_payload().to_vec())?;
        self.alerts = decode_alerts(&response)?;
        Ok(())
    }

    fn load_news_page(&mut self, cursor: Option<String>) -> Result<(), String> {
        let response = self.client.request(news_page_command_payload(
            &self.news_scope,
            &self.selected_symbol,
            cursor.as_deref(),
        ))?;
        let (news, next) = decode_news(&response)?;
        self.news = news;
        sort_news(&mut self.news, &self.news_sort);
        self.news_cursor = cursor;
        self.news_next_cursor = next;
        self.news_selected = 0;
        self.scroll = 0;
        Ok(())
    }

    fn next_news_page(&mut self) -> Result<(), String> {
        if self.page != Page::News {
            return Err("NEWSNEXT is available from the NEWS function".into());
        }
        let next = self
            .news_next_cursor
            .clone()
            .ok_or("already at the end of the news feed")?;
        self.news_cursor_history.push(self.news_cursor.clone());
        if let Err(error) = self.load_news_page(Some(next)) {
            self.news_cursor_history.pop();
            return Err(error);
        }
        self.news_detail = None;
        Ok(())
    }

    fn previous_news_page(&mut self) -> Result<(), String> {
        if self.page != Page::News {
            return Err("NEWSPREV is available from the NEWS function".into());
        }
        let previous = self
            .news_cursor_history
            .pop()
            .ok_or("already at the start of the news feed")?;
        if let Err(error) = self.load_news_page(previous.clone()) {
            self.news_cursor_history.push(previous);
            return Err(error);
        }
        self.news_detail = None;
        Ok(())
    }

    fn move_news_selection(&mut self, delta: isize) {
        const WINDOW_ROWS: usize = 12;
        let maximum = self.news.len().saturating_sub(1);
        self.news_selected = if delta < 0 {
            self.news_selected.saturating_sub(delta.unsigned_abs())
        } else {
            self.news_selected
                .saturating_add(delta.unsigned_abs())
                .min(maximum)
        };
        if self.news_selected < self.scroll {
            self.scroll = self.news_selected;
        } else if self.news_selected >= self.scroll.saturating_add(WINDOW_ROWS) {
            self.scroll = self.news_selected.saturating_sub(WINDOW_ROWS - 1);
        }
    }

    fn open_news_detail(&mut self, item_id: Option<&str>) -> Result<(), String> {
        if self.page != Page::News {
            return Err("DETAIL is available from the NEWS function".into());
        }
        let item_id = item_id
            .map(str::to_owned)
            .or_else(|| {
                self.news
                    .get(self.news_selected)
                    .map(|item| item.id.clone())
            })
            .ok_or("the news page has no selected article")?;
        let response = self.client.request(news_detail_command_payload(&item_id))?;
        self.news_detail = decode_news_detail(&response)?;
        if self.news_detail.is_none() {
            return Err(format!("news item no longer exists: {item_id}"));
        }
        self.scroll = 0;
        Ok(())
    }

    fn load_health(&mut self) {
        self.health_lines.clear();
        for (label, payload) in [
            ("SUPERVISOR", supervisor_status_command_payload().to_vec()),
            ("BROKER", broker_status_command_payload().to_vec()),
            ("RISK POLICY", risk_policy_status_command_payload().to_vec()),
            (
                "NEWS PROVIDERS",
                news_provider_status_command_payload().to_vec(),
            ),
        ] {
            match self.client.request(payload) {
                Ok(response) => self
                    .health_lines
                    .push(format!("{label:<16} ONLINE  {} bytes", response.len())),
                Err(error) => self
                    .health_lines
                    .push(format!("{label:<16} DEGRADED  {error}")),
            }
        }
    }

    fn set_mode(&mut self, value: Option<&str>) -> Result<(), String> {
        let mode = match value.map(str::to_ascii_uppercase).as_deref() {
            Some("MANUAL") => Mode::Manual,
            Some("HYBRID") => Mode::Hybrid,
            Some("AUTO" | "AUTONOMOUS") => Mode::Autonomous,
            _ => return Err("usage: MODE <MANUAL|HYBRID|AUTO>".into()),
        };
        let response = self.client.request(autonomy_mode_command_payload(mode))?;
        if response != b"IT_CMD_AUTONOMY_MODE_OK_V1\0" {
            return Err("engine rejected autonomy mode".into());
        }
        self.refresh_runtime()?;
        self.page = Page::Autonomy;
        Ok(())
    }

    fn preview_order(&mut self, words: &[&str]) -> Result<(), String> {
        let offset = usize::from(words[0].eq_ignore_ascii_case("ORDER"));
        let side = required(
            words,
            offset,
            "usage: BUY <instrument-id> <quantity> [MKT|LMT <price>]",
        )?
        .to_ascii_uppercase();
        if side != "BUY" && side != "SELL" {
            return Err("order side must be BUY or SELL".into());
        }
        let instrument_value = required(words, offset + 1, "order instrument is required")?;
        let (instrument_value, resolved) =
            self.resolve_instrument(instrument_value, ALL_ASSET_CLASSES)?;
        let instrument =
            InstrumentId::new(instrument_value).map_err(|_| "instrument ID must be positive")?;
        if let Some(resolved) = resolved {
            self.selected_instrument = Some(resolved.instrument);
            self.selected_symbol = resolved.symbol;
        }
        let quantity = required(words, offset + 2, "order quantity is required")?
            .parse::<i64>()
            .map_err(|_| "quantity must be a positive integer")?;
        if quantity <= 0 {
            return Err("quantity must be positive".into());
        }
        let signed = if side == "SELL" {
            quantity.checked_neg().ok_or("quantity overflow")?
        } else {
            quantity
        };
        let kind = words
            .get(offset + 3)
            .map_or("MKT", |value| *value)
            .to_ascii_uppercase();
        let (order_type, limit) = match kind.as_str() {
            "MKT" | "MARKET" => (OrderType::Market, None),
            "LMT" | "LIMIT" => {
                let price = required(words, offset + 4, "limit order requires a positive price")?
                    .parse::<i64>()
                    .map_err(|_| "limit price must be a positive integer")?;
                if price <= 0 {
                    return Err("limit price must be positive".into());
                }
                (OrderType::Limit, Some(price))
            }
            _ => return Err("order type must be MKT or LMT".into()),
        };
        let identity = self.client.next_identity();
        let proposal = ProposalId::new(identity).map_err(|_| "proposal identity invalid")?;
        let trace = TraceId::new(identity).map_err(|_| "trace identity invalid")?;
        let payload = preview_command_payload_with_order(
            instrument,
            signed,
            proposal,
            self.client.now(),
            trace,
            30_000_000_000,
            order_type,
            limit,
        );
        let response = self.client.request(payload)?;
        let preview = decode_preview(&response)?;
        let warnings = if preview.warnings.is_empty() {
            "none".into()
        } else {
            preview.warnings.join("; ")
        };
        self.status = format!(
            "PREVIEW {} | state {} | notional {} | cost {} bps | warnings: {} | CONFIRM <GO>",
            preview.id,
            preview.expected_version,
            preview.estimated_notional,
            preview.estimated_cost_bps,
            warnings
        );
        self.status_is_error = false;
        self.pending_preview = Some(PendingPreview::Manual {
            bytes: response,
            preview,
        });
        self.page = Page::Orders;
        Ok(())
    }

    fn preview_proposal(&mut self, value: &str, scale: Option<&str>) -> Result<(), String> {
        let proposal_value = value
            .parse::<u128>()
            .map_err(|_| "proposal ID must be a positive integer")?;
        let proposal_id =
            ProposalId::new(proposal_value).map_err(|_| "proposal ID must be positive")?;
        let scale = parse_proposal_scale(scale)?;
        let identity = self.client.next_identity();
        let trace_id = TraceId::new(identity).map_err(|_| "trace identity invalid")?;
        let now = self.client.now();
        let response = self.client.request(proposal_preview_command_payload(
            proposal_id,
            scale,
            now,
            trace_id,
            30_000_000_000,
        ))?;
        let preview = decode_preview(&response)?;
        let warnings = if preview.warnings.is_empty() {
            "none".into()
        } else {
            preview.warnings.join("; ")
        };
        self.status = format!(
            "PROPOSAL {proposal_value} @ {scale:.2} | PREVIEW {} | notional {} | cost {} bps | warnings: {warnings} | CONFIRM <GO>",
            preview.id, preview.estimated_notional, preview.estimated_cost_bps
        );
        self.status_is_error = false;
        self.pending_preview = Some(PendingPreview::Proposal {
            proposal_id,
            scale,
            trace_id,
            preview,
        });
        self.page = Page::Autonomy;
        Ok(())
    }

    fn confirm_order(&mut self) -> Result<(), String> {
        let (payload, key, proposal_submission) = match self
            .pending_preview
            .as_ref()
            .ok_or("no pending preview; enter BUY, SELL, or PREVIEW first")?
        {
            PendingPreview::Manual { bytes, preview } => (
                submit_preview_payload(bytes, self.client.now(), "CONFIRM")?,
                format!("terminal-submit-{}", preview.id),
                false,
            ),
            PendingPreview::Proposal {
                proposal_id,
                scale,
                trace_id,
                preview,
            } => (
                proposal_submit_command_payload(*proposal_id, *scale, "CONFIRM", *trace_id),
                format!("terminal-proposal-submit-{}", preview.id),
                true,
            ),
        };
        let response = self.client.request_with_key(&payload, Some(key))?;
        let order_id = if proposal_submission {
            decode_proposal_submit(&response)?
        } else {
            decode_string(&response)?
        };
        self.pending_preview = None;
        self.status = format!("ORDER SUBMITTED — {order_id}");
        self.status_is_error = false;
        self.refresh_runtime()?;
        Ok(())
    }

    fn transition_risk(&mut self, state: &str, authorization: &str) -> Result<(), String> {
        let state = match state.to_ascii_uppercase().as_str() {
            "RUNNING" => insider_risk_engine::State::Running,
            "REDUCE" | "REDUCE_ONLY" => insider_risk_engine::State::ReduceOnly,
            "CANCEL" | "CANCEL_ONLY" => insider_risk_engine::State::CancelOnly,
            "HALT" | "HALTED" => insider_risk_engine::State::Halted,
            _ => return Err("risk state must be running, reduce, cancel, or halted".into()),
        };
        let response = self.client.request_with_key(
            &risk_state_transition_command_payload(state, authorization),
            None,
        )?;
        if response != b"IT_CMD_RISK_STATE_OK_V1\0" {
            return Err("engine rejected risk transition".into());
        }
        self.refresh_runtime()?;
        self.page = Page::Risk;
        Ok(())
    }

    fn transition_strategy(&mut self, words: &[&str]) -> Result<(), String> {
        let id = required(
            words,
            1,
            "usage: STRATSET <id> <lifecycle> <confirmation> <evidence>",
        )?;
        let lifecycle = strategy_lifecycle(required(words, 2, "strategy lifecycle required")?)?;
        let confirmation = required(words, 3, "strategy confirmation required")?;
        let evidence = required(words, 4, "strategy evidence reference required")?;
        let response = self
            .client
            .request(strategy_lifecycle_transition_command_payload(
                id,
                lifecycle,
                confirmation,
                evidence,
            ))?;
        if response != b"IT_CMD_STRATEGY_LIFECYCLE_TRANSITION_OK_V1\0" {
            return Err("strategy transition rejected".into());
        }
        self.load_strategies()?;
        self.page = Page::Strategies;
        Ok(())
    }

    fn transition_metric(&mut self, words: &[&str]) -> Result<(), String> {
        let id = required(
            words,
            1,
            "usage: METRICSET <id> <lifecycle> <confirmation> <evidence>",
        )?;
        let lifecycle = metric_lifecycle(required(words, 2, "metric lifecycle required")?)?;
        let confirmation = required(words, 3, "metric confirmation required")?;
        let evidence = required(words, 4, "metric evidence reference required")?;
        let response = self
            .client
            .request(metric_lifecycle_transition_command_payload(
                id,
                lifecycle,
                confirmation,
                evidence,
            ))?;
        if response != b"IT_CMD_METRIC_LIFECYCLE_TRANSITION_OK_V1\0" {
            return Err("metric transition rejected".into());
        }
        self.load_metrics()?;
        self.page = Page::Metrics;
        Ok(())
    }

    fn config_command(&mut self, words: &[&str]) -> Result<(), String> {
        match words.get(1).map(|value| value.to_ascii_uppercase()) {
            None => {
                let response = self
                    .client
                    .request(config_status_command_payload().to_vec())?;
                self.health_lines = vec![decode_config_summary(&response)?];
                self.page = Page::Health;
            }
            Some(value) if value == "SHOW" => {
                let response = self
                    .client
                    .request(config_status_command_payload().to_vec())?;
                self.health_lines = vec![decode_config_summary(&response)?];
                self.page = Page::Health;
            }
            Some(value) if value == "LOAD" => {
                let path = required(words, 2, "usage: CONFIG LOAD <path>")?;
                let text = read_bounded(path)?;
                let current = self
                    .client
                    .request(config_status_command_payload().to_vec())?;
                let (version, _) = decode_config(&current)?;
                let response = self.client.request_with_key(
                    &config_reload_command_payload(version, &text),
                    Some(format!("terminal-config-{version}")),
                )?;
                let (new_version, _) = decode_config(&response)?;
                self.status = format!("CONFIGURATION RELOADED — version {new_version}");
                self.status_is_error = false;
            }
            Some(value) if value == "PROMPT" => {
                let prompt = words.get(2..).unwrap_or_default().join(" ");
                if prompt.trim().is_empty() || prompt.len() > 16 * 1024 {
                    return Err("usage: CONFIG PROMPT <text> (1..16384 bytes)".into());
                }
                let current = self
                    .client
                    .request(config_status_command_payload().to_vec())?;
                let (version, text) = decode_config(&current)?;
                let updated = replace_cfg_setting(&text, "llm.system_prompt", &prompt);
                let response = self.client.request_with_key(
                    &config_reload_command_payload(version, &updated),
                    Some(format!("terminal-config-prompt-{version}")),
                )?;
                let (new_version, _) = decode_config(&response)?;
                self.llm_system_prompt = prompt;
                self.status = format!("LLM SYSTEM PROMPT UPDATED — version {new_version}");
                self.status_is_error = false;
            }
            _ => return Err("usage: CONFIG [SHOW|LOAD <path>|PROMPT <text>]".into()),
        }
        Ok(())
    }

    pub fn fail(&mut self, error: String) {
        self.status = error;
        self.status_is_error = true;
    }
}

fn required<'a>(words: &'a [&str], index: usize, message: &str) -> Result<&'a str, String> {
    words.get(index).copied().ok_or_else(|| message.into())
}

fn parse_proposal_scale(value: Option<&str>) -> Result<f64, String> {
    let scale = value.map_or(Ok(1.0), |value| {
        value
            .parse::<f64>()
            .map_err(|_| "proposal scale must be a number")
    })?;
    if !(scale.is_finite() && 0.0 < scale && scale <= 1.0) {
        return Err("proposal scale must be greater than 0 and at most 1".into());
    }
    Ok(scale)
}

fn parse_security_first<'a>(words: &'a [&str]) -> Result<Option<SecurityFirst<'a>>, String> {
    let Some(first) = words.first().copied() else {
        return Ok(None);
    };
    if is_function(first) {
        return Ok(None);
    }
    let Some(function) = words.last().and_then(|value| security_function(value)) else {
        return Ok(None);
    };
    if !(2..=3).contains(&words.len()) {
        return Err(
            "usage: <symbol> [EQUITY|ETF|OPTION|FUTURE|FX|CRYPTO] <GP|DES|DEPTH|TAPE|NEWS>".into(),
        );
    }
    validate_display_symbol(first)?;
    let asset_mask = words
        .get(1)
        .filter(|_| words.len() == 3)
        .map_or(Ok(ALL_ASSET_CLASSES), |value| asset_mask(value))?;
    Ok(Some(SecurityFirst {
        symbol: first,
        asset_mask,
        function,
    }))
}

fn security_function(value: &str) -> Option<SecurityFunction> {
    match value.to_ascii_uppercase().as_str() {
        "MARKET" | "DES" => Some(SecurityFunction::Market),
        "CHART" | "GP" => Some(SecurityFunction::Chart),
        "DEPTH" | "BOOK" => Some(SecurityFunction::Depth),
        "TAPE" | "TS" => Some(SecurityFunction::Tape),
        "NEWS" | "N" => Some(SecurityFunction::News),
        _ => None,
    }
}

fn asset_mask(value: &str) -> Result<u16, String> {
    match value.to_ascii_uppercase().as_str() {
        "EQUITY" => Ok(1 << 0),
        "ETF" => Ok(1 << 1),
        "OPTION" => Ok(1 << 2),
        "FUTURE" => Ok(1 << 3),
        "FX" => Ok(1 << 4),
        "CRYPTO" => Ok(1 << 5),
        _ => Err("security asset must be EQUITY, ETF, OPTION, FUTURE, FX, or CRYPTO".into()),
    }
}

fn validate_display_symbol(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 64
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'/' | b':')
        })
    {
        return Err("symbol must be 1..64 ASCII letters, digits, or .-_/: characters".into());
    }
    Ok(())
}

fn current_utc_day() -> Result<u32, String> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock predates the Unix epoch")?
        .as_secs();
    utc_day_from_unix_days(seconds / 86_400)
}

fn utc_day_from_unix_days(days: u64) -> Result<u32, String> {
    let days = i64::try_from(days).map_err(|_| "UTC day exceeds supported range")?;
    let shifted = days.checked_add(719_468).ok_or("UTC day overflow")?;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let value = year
        .checked_mul(10_000)
        .and_then(|value| value.checked_add(month * 100))
        .and_then(|value| value.checked_add(day))
        .ok_or("UTC date overflow")?;
    u32::try_from(value).map_err(|_| "UTC date exceeds supported range".into())
}

fn strategy_lifecycle(value: &str) -> Result<StrategyLifecycle, String> {
    match value.to_ascii_lowercase().as_str() {
        "research" => Ok(StrategyLifecycle::Research),
        "validated" => Ok(StrategyLifecycle::Validated),
        "shadow" => Ok(StrategyLifecycle::Shadow),
        "canary" => Ok(StrategyLifecycle::Canary),
        "production" => Ok(StrategyLifecycle::Production),
        "paused" => Ok(StrategyLifecycle::Paused),
        "retired" => Ok(StrategyLifecycle::Retired),
        _ => Err("unknown strategy lifecycle".into()),
    }
}
fn metric_lifecycle(value: &str) -> Result<MetricLifecycle, String> {
    match value.to_ascii_lowercase().as_str() {
        "research" => Ok(MetricLifecycle::Research),
        "validated" => Ok(MetricLifecycle::Validated),
        "shadow" => Ok(MetricLifecycle::Shadow),
        "canary" => Ok(MetricLifecycle::Canary),
        "production" => Ok(MetricLifecycle::Production),
        "paused" => Ok(MetricLifecycle::Paused),
        "retired" => Ok(MetricLifecycle::Retired),
        _ => Err("unknown metric lifecycle".into()),
    }
}

fn read_bounded(path: &str) -> Result<String, String> {
    const MAX: u64 = 1_048_576;
    let file = File::open(path).map_err(|error| format!("open {path}: {error}"))?;
    let mut text = String::new();
    file.take(MAX + 1)
        .read_to_string(&mut text)
        .map_err(|error| format!("read {path}: {error}"))?;
    if text.len() as u64 > MAX {
        return Err("configuration exceeds 1 MiB bound".into());
    }
    Ok(text)
}

fn replace_cfg_setting(text: &str, key: &str, value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    let replacement = format!("{key} = \"{escaped}\"");
    let mut lines = text.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut found = false;
    for line in &mut lines {
        let trimmed = line.trim_start();
        if trimmed.starts_with(&format!("{key} ")) || trimmed.starts_with(&format!("{key}=")) {
            line.clone_from(&replacement);
            found = true;
        }
    }
    if !found {
        lines.push(replacement);
    }
    lines.join("\n") + "\n"
}

fn decode_config(bytes: &[u8]) -> Result<(u64, String), String> {
    const MAGIC: &[u8] = b"IT_CMD_CONFIG_SNAPSHOT_V1\0";
    if !bytes.starts_with(MAGIC) || bytes.len() < MAGIC.len() + 10 {
        return Err("invalid configuration response".into());
    }
    let mut offset = MAGIC.len();
    let version = u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .map_err(|_| "invalid config version")?,
    );
    offset += 8;
    let length = usize::from(u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .map_err(|_| "invalid config length")?,
    ));
    offset += 2;
    let text = String::from_utf8(
        bytes
            .get(offset..offset + length)
            .ok_or("truncated configuration")?
            .to_vec(),
    )
    .map_err(|_| "configuration is not UTF-8")?;
    if offset + length != bytes.len() {
        return Err("trailing configuration bytes".into());
    }
    Ok((version, text))
}
fn decode_config_summary(bytes: &[u8]) -> Result<String, String> {
    let (version, text) = decode_config(bytes)?;
    Ok(format!("CONFIG VERSION {version}\n{text}"))
}

fn build_screener_rows(markets: &[MarketView], mode: ScreenerMode) -> Vec<ScreenerRow> {
    let mut rows = markets.iter().map(screener_row).collect::<Vec<_>>();
    rows.retain(|row| match mode {
        ScreenerMode::All | ScreenerMode::Volume => true,
        ScreenerMode::Movers => row.change_bps.is_some(),
        ScreenerMode::Gainers => row.change_bps.is_some_and(|value| value > 0),
        ScreenerMode::Losers => row.change_bps.is_some_and(|value| value < 0),
        ScreenerMode::Spread => row.spread_bps.is_some(),
        ScreenerMode::Stale => row.quote_quality != "GOOD" || row.trade_quality != "GOOD",
    });
    rows.sort_by(|left, right| {
        let primary = match mode {
            ScreenerMode::All | ScreenerMode::Stale => left.instrument.cmp(&right.instrument),
            ScreenerMode::Movers => right
                .change_bps
                .map(i64::unsigned_abs)
                .cmp(&left.change_bps.map(i64::unsigned_abs)),
            ScreenerMode::Gainers => right.change_bps.cmp(&left.change_bps),
            ScreenerMode::Losers => left.change_bps.cmp(&right.change_bps),
            ScreenerMode::Volume => right.volume.cmp(&left.volume),
            ScreenerMode::Spread => right.spread_bps.cmp(&left.spread_bps),
        };
        primary.then_with(|| left.instrument.cmp(&right.instrument))
    });
    rows
}

fn screener_row(market: &MarketView) -> ScreenerRow {
    let last = market
        .last
        .or_else(|| market.bars.last().map(|bar| bar.close));
    let change_bps = market
        .bars
        .get(market.bars.len().saturating_sub(2)..)
        .filter(|bars| bars.len() == 2)
        .and_then(|bars| {
            let previous = bars[0].close;
            (previous != 0).then(|| {
                bounded_i128(
                    (i128::from(bars[1].close) - i128::from(previous)) * 10_000
                        / i128::from(previous).abs(),
                )
            })
        });
    let spread_bps = market.bid.zip(market.ask).and_then(|(bid, ask)| {
        let midpoint = i128::from(bid) + i128::from(ask);
        (midpoint > 0)
            .then(|| bounded_i128((i128::from(ask) - i128::from(bid)) * 20_000 / midpoint))
    });
    ScreenerRow {
        instrument: market.instrument,
        bid: market.bid,
        ask: market.ask,
        last,
        change_bps,
        spread_bps,
        volume: market.bars.last().map_or(0, |bar| bar.volume.max(0)),
        quote_quality: market.quote_quality.clone(),
        trade_quality: market.trade_quality.clone(),
    }
}

fn bounded_i128(value: i128) -> i64 {
    i64::try_from(value).unwrap_or(if value < 0 { i64::MIN } else { i64::MAX })
}

fn configured_analyst_stale_after(
    settings: &insider_cfg_core::Settings,
) -> Result<Duration, String> {
    let Some(value) = settings.get("terminal.analyst_stale_after_ms") else {
        return Ok(Duration::from_millis(300_000));
    };
    let ConfigValue::Integer(value) = value else {
        return Err("terminal.analyst_stale_after_ms must be an integer".into());
    };
    if !(60_000..=3_600_000).contains(value) {
        return Err("terminal.analyst_stale_after_ms must be 60000..3600000".into());
    }
    Ok(Duration::from_millis(
        u64::try_from(*value).map_err(|_| "invalid analyst stale threshold")?,
    ))
}

fn configured_terminal_theme(settings: &insider_cfg_core::Settings) -> Result<String, String> {
    match settings.get("terminal.theme") {
        None => Ok("AMBER".into()),
        Some(insider_cfg_core::Value::String(value)) => {
            let value = value.trim().to_ascii_uppercase();
            if matches!(value.as_str(), "AMBER" | "BLUE" | "GREEN" | "GRAY" | "MONO") {
                Ok(value)
            } else {
                Err("terminal.theme must be AMBER, BLUE, GREEN, GRAY, or MONO".into())
            }
        }
        Some(_) => Err("terminal.theme must be a string".into()),
    }
}

fn configured_news_sort(settings: &insider_cfg_core::Settings) -> Result<String, String> {
    match settings.get("news.sort") {
        None => Ok("RELEVANCE".into()),
        Some(insider_cfg_core::Value::String(value)) => {
            let value = value.trim().to_ascii_uppercase();
            if matches!(value.as_str(), "RELEVANCE" | "RECENCY" | "SOURCE") {
                Ok(value)
            } else {
                Err("news.sort must be RELEVANCE, RECENCY, or SOURCE".into())
            }
        }
        Some(_) => Err("news.sort must be a string".into()),
    }
}

fn configured_llm_prompt(settings: &insider_cfg_core::Settings) -> Result<String, String> {
    match settings.get("llm.system_prompt") {
        None => Ok(String::new()),
        Some(insider_cfg_core::Value::String(value)) if value.len() <= 16 * 1024 => {
            Ok(value.clone())
        }
        Some(insider_cfg_core::Value::String(_)) => Err("llm.system_prompt exceeds 16 KiB".into()),
        Some(_) => Err("llm.system_prompt must be a string".into()),
    }
}

fn configured_drawdown_limit(settings: &insider_cfg_core::Settings) -> Result<Option<i64>, String> {
    match settings.get("risk.max_drawdown_bps") {
        None => Ok(None),
        Some(insider_cfg_core::Value::Integer(value)) if *value >= 0 => Ok(Some(*value)),
        Some(insider_cfg_core::Value::Integer(_)) => {
            Err("risk.max_drawdown_bps must be non-negative".into())
        }
        Some(_) => Err("risk.max_drawdown_bps must be an integer".into()),
    }
}

fn sort_news(news: &mut [NewsView], sort: &str) {
    news.sort_by(|left, right| {
        let order = match sort {
            "RECENCY" => right.received_ms.cmp(&left.received_ms),
            "SOURCE" => left
                .source
                .to_ascii_lowercase()
                .cmp(&right.source.to_ascii_lowercase())
                .then_with(|| right.received_ms.cmp(&left.received_ms)),
            _ => right
                .relevance
                .partial_cmp(&left.relevance)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.received_ms.cmp(&left.received_ms)),
        };
        order.then_with(|| left.id.cmp(&right.id))
    });
}

#[cfg(test)]
mod tests {
    use super::{
        App, Page, ScreenerMode, SecurityFirst, SecurityFunction, build_screener_rows,
        configured_analyst_stale_after, parse_proposal_scale, parse_security_first,
        utc_day_from_unix_days, validate_display_symbol,
    };
    use crate::chart::{ChartInterval, ChartOverlays, ChartStyle};
    use crate::client::EngineClient;
    use crate::model::{BarView, MarketView};

    use std::path::PathBuf;
    use std::time::Duration;

    #[test]
    fn analyst_freshness_is_typed_bounded_and_defaulted() {
        let default = insider_cfg_core::Settings::new();
        assert_eq!(
            configured_analyst_stale_after(&default).map(|value| value.as_millis()),
            Ok(300_000)
        );

        let valid = insider_cfg_core::parse_cfg("terminal.analyst_stale_after_ms = 60000\n")
            .unwrap_or_default();
        assert_eq!(
            configured_analyst_stale_after(&valid).map(|value| value.as_millis()),
            Ok(60_000)
        );

        let wrong_type = insider_cfg_core::parse_cfg("terminal.analyst_stale_after_ms = true\n")
            .unwrap_or_default();
        assert!(configured_analyst_stale_after(&wrong_type).is_err());

        let out_of_range = insider_cfg_core::parse_cfg("terminal.analyst_stale_after_ms = 59999\n")
            .unwrap_or_default();
        assert!(configured_analyst_stale_after(&out_of_range).is_err());
    }

    #[test]
    fn screener_filters_sorts_and_retains_the_complete_result() {
        let markets = vec![
            market(3, 100, 90, 50),
            market(1, 100, 110, 200),
            market(2, 100, 105, 100),
        ];
        let gainers = build_screener_rows(&markets, ScreenerMode::Gainers);
        assert_eq!(
            gainers.iter().map(|row| row.instrument).collect::<Vec<_>>(),
            [1, 2]
        );
        let movers = build_screener_rows(&markets, ScreenerMode::Movers);
        assert_eq!(movers.len(), markets.len());
        assert_eq!(movers[0].instrument, 1);
        let volume = build_screener_rows(&markets, ScreenerMode::Volume);
        assert_eq!(
            volume.iter().map(|row| row.instrument).collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    #[test]
    fn stale_screener_uses_explicit_quality_not_missing_prices() {
        let mut healthy = market(1, 100, 101, 10);
        healthy.last = None;
        let mut stale = market(2, 100, 101, 20);
        stale.quote_quality = "STALE".into();
        let rows = build_screener_rows(&[healthy, stale], ScreenerMode::Stale);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].instrument, 2);
    }

    #[test]
    fn parses_bloomberg_style_security_first_navigation() {
        assert_eq!(
            parse_security_first(&["AAPL", "GP"]),
            Ok(Some(SecurityFirst {
                symbol: "AAPL",
                asset_mask: 0b11_1111,
                function: SecurityFunction::Chart,
            }))
        );
        assert_eq!(
            parse_security_first(&["ESU6", "FUTURE", "DEPTH"]),
            Ok(Some(SecurityFirst {
                symbol: "ESU6",
                asset_mask: 1 << 3,
                function: SecurityFunction::Depth,
            }))
        );
        assert_eq!(parse_security_first(&["NEWS", "ALL"]), Ok(None));
        assert!(parse_security_first(&["AAPL", "BOND", "GP"]).is_err());
    }

    #[test]
    fn symbol_and_utc_day_conversion_are_bounded() {
        assert!(validate_display_symbol("BRK.B").is_ok());
        assert!(validate_display_symbol("BTC-USD").is_ok());
        assert!(validate_display_symbol("AAPL US").is_err());
        assert_eq!(utc_day_from_unix_days(0), Ok(19_700_101));
        assert_eq!(utc_day_from_unix_days(11_016), Ok(20_000_229));
    }

    #[test]
    fn proposal_scale_defaults_and_fails_closed() {
        assert_eq!(parse_proposal_scale(None), Ok(1.0));
        assert_eq!(parse_proposal_scale(Some("0.65")), Ok(0.65));
        for invalid in ["0", "-0.1", "1.01", "NaN", "inf", "not-a-number"] {
            assert!(parse_proposal_scale(Some(invalid)).is_err());
        }
    }

    #[test]
    fn chart_commands_mutate_only_validated_presentation_state() {
        let Ok(client) = EngineClient::connect(PathBuf::from(format!(
            "/tmp/insider-terminal-app-chart-test-{}.sock",
            std::process::id()
        ))) else {
            return;
        };
        let mut app = App::new(client, Duration::from_secs(1));

        assert!(app.execute_command("INTERVAL 15").is_ok());
        assert_eq!(app.chart_interval, ChartInterval::Fifteen);
        assert!(app.execute_command("STYLE LINE").is_ok());
        assert_eq!(app.chart_style, ChartStyle::Line);
        assert!(app.execute_command("OVERLAY SMA50 ON").is_ok());
        assert!(app.chart_overlays.sma50);
        assert!(app.execute_command("OVERLAY SMA20 OFF").is_ok());
        assert!(!app.chart_overlays.sma20);
        assert!(app.execute_command("XHAIR OFF").is_ok());
        assert_eq!(app.chart_cursor_from_latest, None);

        for invalid in [
            "INTERVAL 2",
            "STYLE AREA",
            "OVERLAY RSI ON",
            "OVERLAY SMA20 MAYBE",
            "XHAIR SIDEWAYS",
        ] {
            assert!(app.execute_command(invalid).is_err(), "accepted {invalid}");
        }

        assert!(app.execute_command("CHARTRESET").is_ok());
        assert_eq!(app.chart_interval, ChartInterval::One);
        assert_eq!(app.chart_style, ChartStyle::Candles);
        assert_eq!(app.chart_overlays, ChartOverlays::default());
        assert_eq!(app.chart_cursor_from_latest, Some(0));

        app.runtime.markets.push(market(7, 100, 105, 20));
        app.selected_instrument = Some(7);
        assert!(app.execute_command("TV").is_ok());
        assert_eq!(app.page, Page::Chart);
        assert_eq!(app.browser_chart_requested.take(), Some(()));
    }

    fn market(instrument: u128, previous: i64, close: i64, volume: i64) -> MarketView {
        MarketView {
            instrument,
            bid: Some(close.saturating_sub(1)),
            ask: Some(close.saturating_add(1)),
            last: Some(close),
            quote_quality: "GOOD".into(),
            trade_quality: "GOOD".into(),
            book_top: None,
            trades: Vec::new(),
            bars: vec![
                BarView {
                    start_time_ns: 0,
                    interval_ns: 60_000_000_000,
                    open: previous,
                    high: previous,
                    low: previous,
                    close: previous,
                    volume: 1,
                },
                BarView {
                    start_time_ns: 60_000_000_000,
                    interval_ns: 60_000_000_000,
                    open: previous,
                    high: close.max(previous),
                    low: close.min(previous),
                    close,
                    volume,
                },
            ],
        }
    }
}
