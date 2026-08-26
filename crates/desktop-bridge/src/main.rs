//! Headless paper-mode composition root for the desktop command bridge.

#![forbid(unsafe_code)]

use std::io::Read;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use insider_broker_api::BrokerGateway;
use insider_cfg_core::{Settings, Value, parse_cfg};
use insider_common_types::{AccountId, InstrumentId, WallTime};
use insider_engine::{EngineCommandService, ReconcileTrigger, ServiceHost};
use insider_exchange_sim::PaperBroker;
use insider_ibkr_adapter::{
    ClientPortalConfig, ClientPortalTransport, IbkrGateway,
    ReqwestHttpTransport as IbkrReqwestTransport,
};
use insider_instrument_master::Catalog;
use insider_ipc::CapabilityPolicy;
use insider_llm_core::{
    Capabilities as LlmCapabilities, OpenAiCompatibleProvider, ReqwestTransport,
};
use insider_market_data::{MarketEvent, Quote};
use insider_market_providers::{
    ReqwestHttpTransport as MarketReqwestTransport, YahooChartConfig, YahooChartProvider,
    YahooQuoteConfig, YahooQuoteProvider,
};
use insider_market_types::{AssetClass, Contract, Instrument, InstrumentSpec};
use insider_metric_host::discover_metric_packages;
use insider_metric_sdk::{BookImbalanceMetric, EwmaVolatility, SimpleMovingAverage, SpreadMetric};
use insider_news_core::{CursorProvider, RetryPolicy};
use insider_news_providers::{
    NewsApiProvider, NewsApiTopHeadlinesProvider, ReqwestHttpTransport, YahooFinanceNewsProvider,
    classify_provider_error,
};
use insider_portfolio::{Portfolio, Position};
use insider_risk_engine::{Limits, RiskEngine};
use insider_strategy_host::discover_strategy_packages;
use insider_strategy_sdk::ThresholdStrategy;
use reqwest::blocking::Client;

use insider_desktop_bridge::DesktopBridge;

fn usage() {
    eprintln!(
        "usage: insider-desktop-bridge serve --journal PATH --socket PATH [--config PATH] [--account ID] \\
         [--check] [--instrument ID --symbol SYMBOL --price TICKS]"
    );
}

fn value(args: &[String], name: &str) -> Result<String, String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .ok_or_else(|| format!("missing {name}"))
}

fn optional_value(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

fn configured_paper_quote(args: &[String]) -> Result<Option<(InstrumentId, i64)>, String> {
    let instrument_arg = optional_value(args, "--instrument");
    let price_arg = optional_value(args, "--price");
    match (instrument_arg, price_arg) {
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => {
            Err("--instrument and --price must be supplied together".to_owned())
        }
        (Some(instrument), Some(price)) => {
            let instrument = instrument.parse::<u128>().map_err(|error| {
                format!("--instrument must be a valid positive integer: {error}")
            })?;
            let instrument = InstrumentId::new(instrument)
                .map_err(|error| format!("market instrument: {error}"))?;
            let price = price
                .parse::<i64>()
                .map_err(|error| format!("--price must be a valid integer: {error}"))?;
            if price <= 0 {
                return Err("--price must be positive".to_owned());
            }
            Ok(Some((instrument, price)))
        }
    }
}

fn configured_cfg_settings(args: &[String]) -> Result<Settings, String> {
    let Some(path) = optional_value(args, "--config") else {
        return Ok(Settings::new());
    };
    let text = read_bounded_cfg_file(&path)?;
    parse_cfg(&text).map_err(|error| format!("parse configuration {path}: {error}"))
}

fn read_bounded_cfg_file(path: &str) -> Result<String, String> {
    const MAX_CFG_BYTES: u64 = 1_048_576;
    let file =
        std::fs::File::open(path).map_err(|error| format!("read configuration {path}: {error}"))?;
    let mut reader = file.take(MAX_CFG_BYTES.saturating_add(1));
    let mut text = String::new();
    reader
        .read_to_string(&mut text)
        .map_err(|error| format!("read configuration {path}: {error}"))?;
    if text.len() as u64 > MAX_CFG_BYTES {
        return Err(format!(
            "read configuration {path}: configuration exceeds 1 MiB bound"
        ));
    }
    Ok(text)
}

fn configured_news_retry_policy(settings: &Settings) -> Result<RetryPolicy, String> {
    let retries = configured_u64(
        settings,
        "news.max_retries",
        "IT_NEWS_MAX_RETRIES",
        4,
        0,
        16,
    )?;
    let base_ms = configured_u64(
        settings,
        "news.retry_base_ms",
        "IT_NEWS_RETRY_BASE_MS",
        1_000,
        1,
        60_000,
    )?;
    let max_ms = configured_u64(
        settings,
        "news.retry_max_ms",
        "IT_NEWS_RETRY_MAX_MS",
        60_000,
        base_ms,
        300_000,
    )?;
    RetryPolicy::new(
        u32::try_from(retries).map_err(|_| "news.max_retries is out of range")?,
        base_ms,
        max_ms,
    )
    .ok_or_else(|| "invalid news retry policy".to_owned())
}

/// Registers the configured `NewsAPI` adapter and starts its restart-safe poller.
/// The API key is injected through the process environment by the deployment
/// secret manager; it is never placed in settings, journal records, or URLs.
fn configure_news_polling(
    host: &Arc<ServiceHost>,
    args: &[String],
    settings: &Settings,
) -> Result<(), String> {
    let Some(api_key) = std::env::var_os("IT_NEWSAPI_KEY") else {
        return Ok(());
    };
    let api_key = api_key
        .into_string()
        .map_err(|_| "IT_NEWSAPI_KEY is not valid UTF-8".to_owned())?;
    let base_url = configured_string(
        settings,
        "news.newsapi_base_url",
        "IT_NEWSAPI_BASE_URL",
        "https://newsapi.org",
    )?;
    let transport_timeout_ms = configured_u64(
        settings,
        "news.http_timeout_ms",
        "IT_NEWS_HTTP_TIMEOUT_MS",
        30_000,
        1_000,
        120_000,
    )?;
    let transport = ReqwestHttpTransport::new(transport_timeout_ms)
        .map_err(|error| format!("NewsAPI transport: {error}"))?;
    let endpoint = configured_string(
        settings,
        "news.newsapi_endpoint",
        "IT_NEWSAPI_ENDPOINT",
        "everything",
    )?;
    let provider: Box<dyn CursorProvider> = match endpoint.as_str() {
        "everything" => {
            let query =
                configured_optional_string(settings, "news.newsapi_query", "IT_NEWSAPI_QUERY")?
                    .or_else(|| optional_value(args, "--symbol"))
                    .ok_or_else(|| {
                        "IT_NEWSAPI_QUERY or --symbol is required with IT_NEWSAPI_KEY".to_owned()
                    })?;
            Box::new(
                NewsApiProvider::from_secret(transport, base_url, api_key, query, 50)
                    .map_err(|error| format!("NewsAPI configuration: {error:?}"))?,
            )
        }
        "top-headlines" => {
            let country =
                configured_optional_string(settings, "news.newsapi_country", "IT_NEWSAPI_COUNTRY")?;
            let category = configured_optional_string(
                settings,
                "news.newsapi_category",
                "IT_NEWSAPI_CATEGORY",
            )?;
            let sources =
                configured_optional_string(settings, "news.newsapi_sources", "IT_NEWSAPI_SOURCES")?;
            Box::new(
                NewsApiTopHeadlinesProvider::from_secret(
                    transport, base_url, api_key, country, category, sources, 50,
                )
                .map_err(|error| format!("NewsAPI top-headlines configuration: {error:?}"))?,
            )
        }
        _ => return Err("IT_NEWSAPI_ENDPOINT must be everything or top-headlines".into()),
    };
    let retry_policy = configured_news_retry_policy(settings)?;
    host.register_news_provider(provider, retry_policy, 60, 60_000, 100, 2_048)
        .map_err(|error| format!("register NewsAPI provider: {error:?}"))?;

    let poll_host = Arc::clone(host);
    let interval_ms = configured_u64(
        settings,
        "news.newsapi_poll_ms",
        "IT_NEWSAPI_POLL_MS",
        30_000,
        1_000,
        300_000,
    )?;
    std::thread::Builder::new()
        .name("newsapi-poller".into())
        .spawn(move || {
            loop {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .and_then(|duration| i64::try_from(duration.as_millis()).ok())
                    .unwrap_or(i64::MAX);
                if let Err(error) = poll_host.poll_news_provider(
                    if endpoint == "top-headlines" {
                        "newsapi_top_headlines"
                    } else {
                        "newsapi"
                    },
                    now_ms,
                    classify_provider_error,
                ) {
                    eprintln!("insider-news: provider poll degraded: {error:?}");
                }
                std::thread::sleep(Duration::from_millis(interval_ms));
            }
        })
        .map_err(|error| format!("start NewsAPI poller: {error}"))?;
    Ok(())
}

/// Registers the optional Yahoo Finance search/news adapter. Yahoo is a
/// convenience/context provider: it uses the same normalized news journal and
/// retry path, but its failure never gates market data or order entry.
fn configure_yahoo_news_polling(
    host: &Arc<ServiceHost>,
    args: &[String],
    settings: &Settings,
) -> Result<(), String> {
    let query = configured_optional_string(settings, "news.yahoo_query", "IT_YAHOO_NEWS_QUERY")?
        .or_else(|| optional_value(args, "--symbol"));
    let Some(query) = query else {
        return Ok(());
    };
    let base_url = configured_string(
        settings,
        "market.yahoo_base_url",
        "IT_YAHOO_BASE_URL",
        "https://query1.finance.yahoo.com",
    )?;
    let transport_timeout_ms = configured_u64(
        settings,
        "news.http_timeout_ms",
        "IT_NEWS_HTTP_TIMEOUT_MS",
        30_000,
        1_000,
        120_000,
    )?;
    let transport = ReqwestHttpTransport::new(transport_timeout_ms)
        .map_err(|error| format!("Yahoo Finance transport: {error}"))?;
    let provider = YahooFinanceNewsProvider::new(transport, base_url, query)
        .map_err(|error| format!("Yahoo Finance configuration: {error:?}"))?;
    let retry_policy = configured_news_retry_policy(settings)?;
    host.register_news_provider(Box::new(provider), retry_policy, 60, 60_000, 100, 2_048)
        .map_err(|error| format!("register Yahoo Finance provider: {error:?}"))?;

    let poll_host = Arc::clone(host);
    let interval_ms = configured_u64(
        settings,
        "news.yahoo_poll_ms",
        "IT_YAHOO_NEWS_POLL_MS",
        60_000,
        5_000,
        300_000,
    )?;
    std::thread::Builder::new()
        .name("yahoo-news-poller".into())
        .spawn(move || {
            loop {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .and_then(|duration| i64::try_from(duration.as_millis()).ok())
                    .unwrap_or(i64::MAX);
                if let Err(error) =
                    poll_host.poll_news_provider("yahoo_finance", now_ms, classify_provider_error)
                {
                    eprintln!("insider-news: Yahoo provider degraded: {error:?}");
                }
                std::thread::sleep(Duration::from_millis(interval_ms));
            }
        })
        .map_err(|error| format!("start Yahoo Finance poller: {error}"))?;
    Ok(())
}

/// Discovers Python metric/strategy packages and starts one bounded worker per
/// package. Rust packages remain the responsibility of their compiled host;
/// malformed manifests fail startup instead of being silently skipped.
#[allow(clippy::too_many_lines)]
fn configure_python_packages(host: &Arc<ServiceHost>, settings: &Settings) -> Result<(), String> {
    let python = configured_string(settings, "python.executable", "IT_PYTHON", "python3")?;
    let worker_dir = configured_string(
        settings,
        "python.workdir",
        "IT_PYTHON_WORKDIR",
        "data/python-workers",
    )?;
    std::fs::create_dir_all(&worker_dir)
        .map_err(|error| format!("create Python worker directory: {error}"))?;
    let worker_cpu_seconds = configured_u64(
        settings,
        "python.cpu_seconds",
        "IT_PYTHON_CPU_SECONDS",
        3600,
        1,
        86_400,
    )?;
    let worker_memory_bytes = configured_u64(
        settings,
        "python.memory_bytes",
        "IT_PYTHON_MEMORY_BYTES",
        512 * 1024 * 1024,
        64 * 1024 * 1024,
        8 * 1024 * 1024 * 1024,
    )?;
    let allow_network = configured_bool(
        settings,
        "python.allow_network",
        "IT_PYTHON_ALLOW_NETWORK",
        false,
    )?;
    let worker_cpu_seconds = worker_cpu_seconds.to_string();
    let worker_memory_bytes = worker_memory_bytes.to_string();
    let allow_network = if allow_network { "true" } else { "false" };
    let python_path = std::env::var_os("PYTHONPATH").map_or_else(
        || "python".into(),
        |value| {
            std::env::join_paths(
                std::iter::once(PathBuf::from("python")).chain(std::env::split_paths(&value)),
            )
            .map_or_else(
                |_| "python".into(),
                |joined| joined.to_string_lossy().into_owned(),
            )
        },
    );
    let metric_root = configured_string(
        settings,
        "python.metrics_root",
        "IT_METRICS_ROOT",
        "metrics",
    )?;
    let strategy_root = configured_string(
        settings,
        "python.strategies_root",
        "IT_STRATEGIES_ROOT",
        "strategies",
    )?;

    for discovered in discover_metric_packages(&metric_root)
        .map_err(|error| format!("discover metrics: {error:?}"))?
        .into_iter()
        .filter(|package| package.language == "python")
    {
        let mut command = Command::new(&python);
        command
            .arg("-m")
            .arg("insidertrader.metric_sdk.worker")
            .env("PYTHONPATH", &python_path)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("PYTHONNOUSERSITE", "1")
            .env("IT_PYTHON_WORKDIR", &worker_dir)
            .env("IT_PYTHON_CPU_SECONDS", &worker_cpu_seconds)
            .env("IT_PYTHON_MEMORY_BYTES", &worker_memory_bytes)
            .env("IT_PYTHON_ALLOW_NETWORK", allow_network)
            .current_dir(&worker_dir);
        host.register_python_metric(&discovered, command)
            .map_err(|error| {
                format!(
                    "register Python metric {}: {error:?}",
                    discovered.manifest_path.display()
                )
            })?;
    }
    for discovered in discover_strategy_packages(&strategy_root)
        .map_err(|error| format!("discover strategies: {error:?}"))?
        .into_iter()
        .filter(|package| package.language == "python")
    {
        let mut command = Command::new(&python);
        command
            .arg("-m")
            .arg("insidertrader.strategy_sdk.worker")
            .env("PYTHONPATH", &python_path)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("PYTHONNOUSERSITE", "1")
            .env("IT_PYTHON_WORKDIR", &worker_dir)
            .env("IT_PYTHON_CPU_SECONDS", &worker_cpu_seconds)
            .env("IT_PYTHON_MEMORY_BYTES", &worker_memory_bytes)
            .env("IT_PYTHON_ALLOW_NETWORK", allow_network)
            .current_dir(&worker_dir);
        host.register_python_strategy(&discovered, command)
            .map_err(|error| {
                format!(
                    "register Python strategy {}: {error:?}",
                    discovered.manifest_path.display()
                )
            })?;
    }
    Ok(())
}

fn register_reference_metrics(host: &Arc<ServiceHost>, settings: &Settings) -> Result<(), String> {
    let ewma = EwmaVolatility::new(
        configured_string(
            settings,
            "metric.ewma_id",
            "IT_EWMA_METRIC_ID",
            "volatility.ewma.v1",
        )?,
        configured_f64(settings, "metric.ewma_lambda", "IT_EWMA_LAMBDA", 0.94)?,
        configured_u64(
            settings,
            "metric.ewma_ttl_ns",
            "IT_EWMA_TTL_NS",
            5_000_000_000,
            1,
            86_400_000_000_000,
        )?,
    )
    .map_err(|error| format!("EWMA metric configuration: {error:?}"))?;
    host.register_metric(Arc::new(ewma))
        .map_err(|error| format!("register EWMA metric: {error:?}"))?;
    let metric_ttl_ns = configured_u64(
        settings,
        "metric.reference_ttl_ns",
        "IT_REFERENCE_METRIC_TTL_NS",
        5_000_000_000,
        1,
        86_400_000_000_000,
    )?;
    let sma_window = usize::try_from(configured_u64(
        settings,
        "metric.sma_window",
        "IT_SMA_WINDOW",
        20,
        1,
        10_000,
    )?)
    .map_err(|_| "metric.sma_window exceeds platform bounds".to_owned())?;
    let sma = SimpleMovingAverage::new(
        configured_string(
            settings,
            "metric.sma_id",
            "IT_SMA_METRIC_ID",
            "trend.sma.v1",
        )?,
        sma_window,
        metric_ttl_ns,
    )
    .map_err(|error| format!("SMA metric configuration: {error:?}"))?;
    host.register_metric(Arc::new(sma))
        .map_err(|error| format!("register SMA metric: {error:?}"))?;
    let spread = SpreadMetric::new(
        configured_string(
            settings,
            "metric.spread_id",
            "IT_SPREAD_METRIC_ID",
            "liquidity.spread.v1",
        )?,
        metric_ttl_ns,
    )
    .map_err(|error| format!("spread metric configuration: {error:?}"))?;
    host.register_metric(Arc::new(spread))
        .map_err(|error| format!("register spread metric: {error:?}"))?;
    let imbalance = BookImbalanceMetric::new(
        configured_string(
            settings,
            "metric.imbalance_id",
            "IT_IMBALANCE_METRIC_ID",
            "microstructure.imbalance.v1",
        )?,
        metric_ttl_ns,
    )
    .map_err(|error| format!("book-imbalance metric configuration: {error:?}"))?;
    host.register_metric(Arc::new(imbalance))
        .map_err(|error| format!("register book-imbalance metric: {error:?}"))?;
    Ok(())
}

fn configure_context_embeddings(
    host: &Arc<ServiceHost>,
    settings: &Settings,
) -> Result<(), String> {
    let configured = [
        settings.contains_key("embeddings.model"),
        settings.contains_key("embeddings.model_version"),
        settings.contains_key("embeddings.dimensions"),
        std::env::var_os("IT_EMBEDDING_MODEL").is_some(),
        std::env::var_os("IT_EMBEDDING_MODEL_VERSION").is_some(),
        std::env::var_os("IT_EMBEDDING_DIMENSIONS").is_some(),
    ];
    if !configured.into_iter().any(|present| present) {
        return Ok(());
    }
    let model = configured_string(settings, "embeddings.model", "IT_EMBEDDING_MODEL", "")?;
    let version = configured_string(
        settings,
        "embeddings.model_version",
        "IT_EMBEDDING_MODEL_VERSION",
        "",
    )?;
    let dimensions = usize::try_from(configured_u64(
        settings,
        "embeddings.dimensions",
        "IT_EMBEDDING_DIMENSIONS",
        0,
        1,
        4_096,
    )?)
    .map_err(|_| "embeddings.dimensions exceeds platform bounds".to_owned())?;
    host.configure_context_embeddings(model, version, dimensions)
        .map_err(|error| format!("configure context embeddings: {error:?}"))
}

fn register_reference_strategy(host: &Arc<ServiceHost>, settings: &Settings) -> Result<(), String> {
    if !configured_bool(
        settings,
        "strategy.reference_enabled",
        "IT_ENABLE_REFERENCE_STRATEGY",
        false,
    )? {
        return Ok(());
    }
    let strategy = ThresholdStrategy::new(
        configured_string(
            settings,
            "strategy.reference_id",
            "IT_REFERENCE_STRATEGY_ID",
            "microstructure.imbalance.threshold.v1",
        )?,
        configured_string(
            settings,
            "strategy.reference_metric_id",
            "IT_IMBALANCE_METRIC_ID",
            "microstructure.imbalance.v1",
        )?,
        configured_f64(
            settings,
            "strategy.reference_entry_threshold",
            "IT_REFERENCE_ENTRY_THRESHOLD",
            0.5,
        )?,
        configured_f64(
            settings,
            "strategy.reference_exit_threshold",
            "IT_REFERENCE_EXIT_THRESHOLD",
            0.1,
        )?,
        configured_positive_i64_setting(
            settings,
            "strategy.reference_quantity_ticks",
            "IT_REFERENCE_QUANTITY_TICKS",
            1,
        )?,
        configured_u64(
            settings,
            "strategy.reference_horizon_ns",
            "IT_REFERENCE_HORIZON_NS",
            900_000_000_000,
            1,
            86_400_000_000_000,
        )?,
        configured_u64(
            settings,
            "strategy.reference_ttl_ns",
            "IT_REFERENCE_STRATEGY_TTL_NS",
            5_000_000_000,
            1,
            86_400_000_000_000,
        )?,
    )
    .ok_or_else(|| "reference strategy configuration is invalid".to_owned())?;
    host.register_strategy(Arc::new(strategy))
        .map_err(|error| format!("register reference strategy: {error:?}"))
}

/// Runs discovered workers on a bounded wall-clock cadence. The engine owns
/// monotonic decision timestamps and proposal admission; this thread only
/// wakes the cycle and reports worker degradation.
fn start_python_scheduler(host: &Arc<ServiceHost>, settings: &Settings) -> Result<(), String> {
    let interval_ms = configured_u64(
        settings,
        "scheduler.python_cycle_ms",
        "IT_PYTHON_CYCLE_MS",
        100,
        25,
        60_000,
    )?;
    let cycle_host = Arc::clone(host);
    let market_max_age_ns = configured_u64(
        settings,
        "market.max_age_ms",
        "IT_MARKET_MAX_AGE_MS",
        60_000,
        250,
        86_400_000,
    )?
    .saturating_mul(1_000_000);
    std::thread::Builder::new()
        .name("python-decision-cycle".into())
        .spawn(move || {
            loop {
                if let Err(error) =
                    cycle_host.mark_market_data_stale(cycle_host.monotonic_now(), market_max_age_ns)
                {
                    eprintln!("insider-market: freshness update degraded: {error:?}");
                }
                if let Err(error) = cycle_host.run_registered_python_cycle() {
                    eprintln!("insider-python: decision cycle degraded: {error:?}");
                }
                std::thread::sleep(Duration::from_millis(interval_ms));
            }
        })
        .map_err(|error| format!("start Python decision cycle: {error}"))?;
    Ok(())
}

/// Drives persisted child-order plans on an engine-monotonic cadence. The
/// planner claims each due child before transport, so wakeups and restarts
/// cannot duplicate a child send.
fn start_execution_scheduler(host: &Arc<ServiceHost>, settings: &Settings) -> Result<(), String> {
    let interval_ms = configured_u64(
        settings,
        "scheduler.execution_cycle_ms",
        "IT_EXECUTION_CYCLE_MS",
        25,
        5,
        60_000,
    )?;
    let execution_host = Arc::clone(host);
    std::thread::Builder::new()
        .name("scheduled-execution-cycle".into())
        .spawn(move || {
            loop {
                if let Err(error) =
                    execution_host.drive_scheduled_children(execution_host.monotonic_now())
                {
                    eprintln!("insider-execution: scheduled cycle degraded: {error:?}");
                }
                std::thread::sleep(Duration::from_millis(interval_ms));
            }
        })
        .map_err(|error| format!("start execution scheduler: {error}"))?;
    Ok(())
}

/// Installs the optional OpenAI-compatible provider for intelligence-plane
/// callers. Credentials are supplied only through the process environment.
fn configure_llm_provider(host: &Arc<ServiceHost>, settings: &Settings) -> Result<(), String> {
    let Some(api_key) = std::env::var_os("IT_LLM_API_KEY") else {
        return Ok(());
    };
    let api_key = api_key
        .into_string()
        .map_err(|_| "IT_LLM_API_KEY is not valid UTF-8".to_owned())?;
    let base_url = configured_string(
        settings,
        "llm.base_url",
        "IT_LLM_BASE_URL",
        "https://api.openai.com/v1",
    )?;
    let base_url = validate_llm_base_url(&base_url)?;
    let timeout_ms = configured_u64(
        settings,
        "llm.timeout_ms",
        "IT_LLM_TIMEOUT_MS",
        30_000,
        1_000,
        120_000,
    )?;
    let transport =
        ReqwestTransport::new(timeout_ms).map_err(|error| format!("LLM transport: {error:?}"))?;
    let provider = OpenAiCompatibleProvider::new(
        transport,
        base_url,
        api_key,
        LlmCapabilities {
            responses: true,
            chat_completions: true,
            streaming: true,
            json_schema: true,
            tools: true,
        },
    )
    .map_err(|error| format!("LLM configuration: {error:?}"))?;
    host.install_llm_provider(Arc::new(provider))
        .map_err(|error| format!("install LLM provider: {error:?}"))?;
    Ok(())
}

/// Runs periodic authoritative broker reconciliation on a control-plane
/// worker. The loop never submits orders; it only updates the journal-backed
/// runtime and lets reconciliation gate new work when divergence is found.
fn start_reconciliation_loop(host: &Arc<ServiceHost>, settings: &Settings) -> Result<(), String> {
    let interval_ms = configured_u64(
        settings,
        "reconciliation.poll_ms",
        "IT_RECONCILIATION_POLL_MS",
        30_000,
        1_000,
        300_000,
    )?;
    let reconcile_host = Arc::clone(host);
    std::thread::Builder::new()
        .name("broker-reconciliation".into())
        .spawn(move || {
            loop {
                match reconcile_host.reconcile_trigger(ReconcileTrigger::Periodic) {
                    Ok(summary)
                        if summary.still_unknown > 0
                            || summary.external_orders > 0
                            || summary.missing_at_broker > 0
                            || !summary.failed.is_empty() =>
                    {
                        eprintln!("insider-reconcile: runtime remains gated: {summary:?}");
                    }
                    Ok(_) => {}
                    Err(error) => eprintln!("insider-reconcile: sweep failed: {error:?}"),
                }
                std::thread::sleep(Duration::from_millis(interval_ms));
            }
        })
        .map_err(|error| format!("start reconciliation loop: {error}"))?;
    Ok(())
}

/// Delivers configured webhook alerts on a bounded control-plane worker.
/// Successful HTTP 2xx responses acknowledge only the webhook channel; the
/// in-app alert remains until the operator acknowledges it in the UI.
fn configured_alert_webhook_url(settings: &Settings) -> Result<Option<String>, String> {
    match settings.get("alerts.webhook_url") {
        None => Ok(None),
        Some(Value::String(url))
            if url.len() <= 2_048
                && !url.contains(char::is_whitespace)
                && reqwest::Url::parse(url).is_ok_and(|parsed| {
                    parsed.scheme() == "https"
                        && parsed.host_str().is_some()
                        && parsed.username().is_empty()
                        && parsed.password().is_none()
                }) =>
        {
            Ok(Some(url.clone()))
        }
        Some(_) => Err("alerts.webhook_url must be a valid HTTPS URL".into()),
    }
}

fn validate_llm_metadata(settings: &Settings) -> Result<(), String> {
    for key in ["llm.model", "llm.prompt_version"] {
        if let Some(value) = settings.get(key) {
            match value {
                Value::String(text) if !text.trim().is_empty() && text.len() <= 256 => {}
                _ => return Err(format!("{key} must be a non-empty string under 256 bytes")),
            }
        }
    }
    Ok(())
}

fn start_alert_webhook_loop(host: &Arc<ServiceHost>, settings: &Settings) -> Result<(), String> {
    let Some(webhook_url) = configured_alert_webhook_url(settings)? else {
        return Ok(());
    };
    let timeout_ms = configured_u64(
        settings,
        "alerts.webhook_timeout_ms",
        "IT_ALERT_WEBHOOK_TIMEOUT_MS",
        2_000,
        250,
        30_000,
    )?;
    let poll_ms = configured_u64(
        settings,
        "alerts.webhook_poll_ms",
        "IT_ALERT_WEBHOOK_POLL_MS",
        2_000,
        250,
        300_000,
    )?;
    let client = Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| format!("alert webhook client: {error}"))?;
    let alert_host = Arc::clone(host);
    std::thread::Builder::new()
        .name("alert-webhook-delivery".into())
        .spawn(move || {
            loop {
                for alert in alert_host.pending_webhook_alerts() {
                    let body = serde_json::json!({
                        "alert_id": alert.alert_id,
                        "dedupe_key": alert.dedupe_key,
                        "source": alert.source,
                        "occurred_ms": alert.occurred_ms,
                        "severity": alert.severity,
                        "message": alert.message,
                    });
                    match client.post(&webhook_url).json(&body).send() {
                        Ok(response) if response.status().is_success() => {
                            let _ = alert_host.acknowledge_webhook_alert(&alert.alert_id);
                        }
                        Ok(response) => eprintln!(
                            "insider-alerts: webhook returned HTTP {}",
                            response.status()
                        ),
                        Err(_) => eprintln!("insider-alerts: webhook delivery failed"),
                    }
                }
                std::thread::sleep(Duration::from_millis(poll_ms));
            }
        })
        .map_err(|error| format!("start alert webhook loop: {error}"))?;
    Ok(())
}

/// Loads optional Yahoo historical candles into chart state. This adapter is
/// deliberately non-authoritative: failures are reported and the runtime
/// continues with broker/other-provider data.
#[allow(clippy::too_many_lines)]
fn configure_yahoo_history(host: &Arc<ServiceHost>, args: &[String], settings: &Settings) {
    let Some(instrument_value) = optional_value(args, "--instrument") else {
        return;
    };
    let Some(symbol) = optional_value(args, "--symbol") else {
        return;
    };
    let Ok(instrument_value) = instrument_value.parse::<u128>() else {
        return;
    };
    let Ok(instrument_id) = InstrumentId::new(instrument_value) else {
        return;
    };
    let transport_timeout_ms = match configured_u64(
        settings,
        "market.http_timeout_ms",
        "IT_MARKET_HTTP_TIMEOUT_MS",
        30_000,
        1_000,
        120_000,
    ) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("insider-market: Yahoo transport timeout rejected: {error}");
            return;
        }
    };
    let transport = match MarketReqwestTransport::new(transport_timeout_ms) {
        Ok(transport) => transport,
        Err(error) => {
            eprintln!("insider-market: Yahoo transport unavailable: {error}");
            return;
        }
    };
    let base_url = match configured_string(
        settings,
        "market.yahoo_base_url",
        "IT_YAHOO_BASE_URL",
        "https://query1.finance.yahoo.com",
    ) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("insider-market: Yahoo base URL rejected: {error}");
            return;
        }
    };
    let interval =
        match configured_string(settings, "market.yahoo_interval", "IT_YAHOO_INTERVAL", "1m") {
            Ok(value) => value,
            Err(error) => {
                eprintln!("insider-market: Yahoo interval rejected: {error}");
                return;
            }
        };
    let range = match configured_string(settings, "market.yahoo_range", "IT_YAHOO_RANGE", "1d") {
        Ok(value) => value,
        Err(error) => {
            eprintln!("insider-market: Yahoo range rejected: {error}");
            return;
        }
    };
    let interval_ns = match configured_u64(
        settings,
        "market.yahoo_interval_ns",
        "IT_YAHOO_INTERVAL_NS",
        60_000_000_000,
        1_000_000_000,
        86_400_000_000_000,
    ) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("insider-market: Yahoo interval cadence rejected: {error}");
            return;
        }
    };
    let price_scale = match configured_u64(
        settings,
        "market.yahoo_price_scale",
        "IT_YAHOO_PRICE_SCALE",
        10_000,
        1,
        1_000_000_000,
    ) {
        Ok(value) => match i64::try_from(value) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("insider-market: Yahoo price scale rejected: {error}");
                return;
            }
        },
        Err(error) => {
            eprintln!("insider-market: Yahoo price scale rejected: {error}");
            return;
        }
    };
    let provider = match YahooChartProvider::new(
        transport,
        YahooChartConfig {
            base_url,
            symbol,
            instrument_id,
            interval,
            range,
            interval_ns,
            price_scale,
        },
    ) {
        Ok(provider) => provider,
        Err(error) => {
            eprintln!("insider-market: Yahoo configuration rejected: {error}");
            return;
        }
    };
    let poll_interval_ms = match configured_u64(
        settings,
        "market.yahoo_poll_ms",
        "IT_YAHOO_POLL_MS",
        60_000,
        5_000,
        900_000,
    ) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("insider-market: Yahoo history cadence rejected: {error}");
            return;
        }
    };
    let poll_host = Arc::clone(host);
    if let Err(error) = std::thread::Builder::new()
        .name("yahoo-chart-history".into())
        .spawn(move || {
            loop {
                match provider.fetch() {
                    Ok(bars) => {
                        for bar in bars {
                            if let Err(error) = poll_host.ingest_market_bar(bar.bar, bar.sequence) {
                                eprintln!("insider-market: Yahoo bar rejected: {error:?}");
                            }
                        }
                    }
                    Err(error) => eprintln!("insider-market: Yahoo history unavailable: {error}"),
                }
                std::thread::sleep(Duration::from_millis(poll_interval_ms));
            }
        })
    {
        eprintln!("insider-market: Yahoo refresh worker unavailable: {error}");
    }
}

/// Starts the optional Yahoo quote poller. Quotes enter the same canonical
/// market hub as broker/replay data and therefore update marks and freshness;
/// Yahoo remains explicitly non-authoritative for live execution.
#[allow(clippy::too_many_lines)]
fn configure_yahoo_quotes(host: &Arc<ServiceHost>, args: &[String], settings: &Settings) {
    let broker_mode = match configured_string(settings, "broker.mode", "IT_BROKER", "paper") {
        Ok(mode) => mode,
        Err(error) => {
            eprintln!("insider-market: broker mode rejected: {error}");
            return;
        }
    };
    let yahoo_symbols =
        match configured_string(settings, "market.yahoo_symbols", "IT_YAHOO_SYMBOLS", "") {
            Ok(value) if !value.trim().is_empty() => Some(value),
            Ok(_) => None,
            Err(error) => {
                eprintln!("insider-market: Yahoo symbol list rejected: {error}");
                None
            }
        };
    let allow_yahoo_live_marks = match configured_bool(
        settings,
        "market.allow_yahoo_live_marks",
        "IT_ALLOW_YAHOO_LIVE_MARKS",
        false,
    ) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("insider-market: Yahoo live-mark policy rejected: {error}");
            return;
        }
    };
    if broker_mode == "ibkr" && !allow_yahoo_live_marks {
        eprintln!(
            "insider-market: Yahoo quotes disabled for IBKR; set market.allow_yahoo_live_marks=true only with explicit policy"
        );
        return;
    }
    if let Some(raw_symbols) = yahoo_symbols.as_deref() {
        let mut configurations = Vec::new();
        for entry in raw_symbols
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .take(128)
        {
            let Some((symbol, instrument)) = entry.split_once('=') else {
                eprintln!("insider-market: ignoring malformed Yahoo symbol entry");
                continue;
            };
            let symbol = symbol.trim().to_ascii_uppercase();
            let instrument = instrument
                .trim()
                .parse::<u128>()
                .ok()
                .and_then(|value| InstrumentId::new(value).ok());
            if !symbol.is_empty()
                && symbol.len() <= 16
                && symbol
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b".-".contains(&byte))
                && let Some(instrument) = instrument
            {
                configurations.push((symbol, instrument));
            }
        }
        let poll_interval_ms = match configured_u64(
            settings,
            "market.yahoo_quote_poll_ms",
            "IT_YAHOO_QUOTE_POLL_MS",
            5_000,
            1_000,
            300_000,
        ) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("insider-market: Yahoo quote cadence rejected: {error}");
                return;
            }
        };
        let base_url = match configured_string(
            settings,
            "market.yahoo_base_url",
            "IT_YAHOO_BASE_URL",
            "https://query1.finance.yahoo.com",
        ) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("insider-market: Yahoo base URL rejected: {error}");
                return;
            }
        };
        let price_scale = match configured_u64(
            settings,
            "market.yahoo_price_scale",
            "IT_YAHOO_PRICE_SCALE",
            10_000,
            1,
            1_000_000_000,
        )
        .and_then(|value| i64::try_from(value).map_err(|_| "price scale exceeds i64".to_owned()))
        {
            Ok(value) => value,
            Err(error) => {
                eprintln!("insider-market: Yahoo price scale rejected: {error}");
                return;
            }
        };
        for (symbol, instrument_id) in configurations {
            let transport = match MarketReqwestTransport::new(30_000) {
                Ok(transport) => transport,
                Err(error) => {
                    eprintln!(
                        "insider-market: Yahoo quote transport unavailable for {symbol}: {error}"
                    );
                    continue;
                }
            };
            let provider = match YahooQuoteProvider::new(
                transport,
                YahooQuoteConfig {
                    base_url: base_url.clone(),
                    symbol: symbol.clone(),
                    instrument_id,
                    price_scale,
                },
            ) {
                Ok(provider) => provider,
                Err(error) => {
                    eprintln!(
                        "insider-market: Yahoo quote configuration rejected for {symbol}: {error}"
                    );
                    continue;
                }
            };
            let poll_host = Arc::clone(host);
            let worker_symbol = symbol.clone();
            let worker_name = format!("yahoo-quotes-{symbol}");
            if let Err(error) = std::thread::Builder::new().name(worker_name).spawn(move || loop {
                let fallback_wall = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
                    .map_or_else(|| WallTime::from_unix_nanos(0), WallTime::from_unix_nanos);
                match provider.fetch(poll_host.monotonic_now(), fallback_wall) {
                    Ok(quote) => {
                        if let Err(error) = poll_host.ingest_market_event(MarketEvent::Quote(quote), fallback_wall) {
                            eprintln!("insider-market: Yahoo quote rejected for {worker_symbol}: {error:?}");
                        }
                    }
                    Err(error) => eprintln!("insider-market: Yahoo quote unavailable for {worker_symbol}: {error}"),
                }
                std::thread::sleep(Duration::from_millis(poll_interval_ms));
            }) {
                eprintln!("insider-market: Yahoo quote worker unavailable for {symbol}: {error}");
            }
        }
        return;
    }
    let (Some(instrument_value), Some(symbol)) = (
        optional_value(args, "--instrument"),
        optional_value(args, "--symbol"),
    ) else {
        return;
    };
    let Ok(instrument_value) = instrument_value.parse::<u128>() else {
        return;
    };
    let Ok(instrument_id) = InstrumentId::new(instrument_value) else {
        return;
    };
    let transport_timeout_ms = match configured_u64(
        settings,
        "market.http_timeout_ms",
        "IT_MARKET_HTTP_TIMEOUT_MS",
        30_000,
        1_000,
        120_000,
    ) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("insider-market: Yahoo transport timeout rejected: {error}");
            return;
        }
    };
    let transport = match MarketReqwestTransport::new(transport_timeout_ms) {
        Ok(transport) => transport,
        Err(error) => {
            eprintln!("insider-market: Yahoo quote transport unavailable: {error}");
            return;
        }
    };
    let base_url = match configured_string(
        settings,
        "market.yahoo_base_url",
        "IT_YAHOO_BASE_URL",
        "https://query1.finance.yahoo.com",
    ) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("insider-market: Yahoo base URL rejected: {error}");
            return;
        }
    };
    let price_scale = match configured_u64(
        settings,
        "market.yahoo_price_scale",
        "IT_YAHOO_PRICE_SCALE",
        10_000,
        1,
        1_000_000_000,
    )
    .and_then(|value| i64::try_from(value).map_err(|_| "price scale exceeds i64".to_owned()))
    {
        Ok(value) => value,
        Err(error) => {
            eprintln!("insider-market: Yahoo price scale rejected: {error}");
            return;
        }
    };
    let provider = match YahooQuoteProvider::new(
        transport,
        YahooQuoteConfig {
            base_url,
            symbol,
            instrument_id,
            price_scale,
        },
    ) {
        Ok(provider) => provider,
        Err(error) => {
            eprintln!("insider-market: Yahoo quote configuration rejected: {error}");
            return;
        }
    };
    let poll_interval_ms = match configured_u64(
        settings,
        "market.yahoo_quote_poll_ms",
        "IT_YAHOO_QUOTE_POLL_MS",
        5_000,
        1_000,
        300_000,
    ) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("insider-market: Yahoo quote cadence rejected: {error}");
            return;
        }
    };
    let poll_host = Arc::clone(host);
    if let Err(error) = std::thread::Builder::new()
        .name("yahoo-quotes".into())
        .spawn(move || {
            loop {
                let fallback_wall = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
                    .map_or_else(|| WallTime::from_unix_nanos(0), WallTime::from_unix_nanos);
                match provider.fetch(poll_host.monotonic_now(), fallback_wall) {
                    Ok(quote) => {
                        if let Err(error) =
                            poll_host.ingest_market_event(MarketEvent::Quote(quote), fallback_wall)
                        {
                            eprintln!("insider-market: Yahoo quote rejected: {error:?}");
                        }
                    }
                    Err(error) => eprintln!("insider-market: Yahoo quote unavailable: {error}"),
                }
                std::thread::sleep(Duration::from_millis(poll_interval_ms));
            }
        })
    {
        eprintln!("insider-market: Yahoo quote worker unavailable: {error}");
    }
}

fn grant_desktop_capabilities() -> Result<CapabilityPolicy, String> {
    let mut policy = CapabilityPolicy::new();
    for capability in [
        "runtime.read",
        "llm.analyze",
        "strategy.evaluate",
        "instrument.read",
        "order.preview",
        "order.submit",
        "order.manage",
        "live.configure",
        "live.enable",
        "live.kill",
        "autonomy.plan.write",
        "autonomy.mode.write",
        "alerts.read",
        "alerts.ack",
        "read_model.backup.write",
        "journal.backup.write",
        "risk.state.write",
        "research.backtest",
    ] {
        policy
            .grant("desktop", capability)
            .map_err(|error| format!("grant {capability}: {error:?}"))?;
    }
    Ok(policy)
}

#[allow(clippy::too_many_lines)]
fn configure_demo(
    args: &[String],
    broker: &PaperBroker,
    settings: &Settings,
) -> Result<(Catalog, Portfolio, i64, i128), String> {
    let mut catalog = Catalog::new();
    let mut portfolio = Portfolio::new();
    let broker_mode = configured_string(settings, "broker.mode", "IT_BROKER", "paper")?;
    let broker_is_ibkr = broker_mode == "ibkr";
    let yahoo_symbols =
        configured_optional_string(settings, "market.yahoo_symbols", "IT_YAHOO_SYMBOLS")?;

    let instrument_config = [
        optional_value(args, "--instrument"),
        optional_value(args, "--symbol"),
        optional_value(args, "--price"),
    ];
    if instrument_config.iter().any(Option::is_some)
        && instrument_config.iter().any(Option::is_none)
    {
        return Err("--instrument, --symbol, and --price must be supplied together".into());
    }
    let (max_position, max_notional) = if let (Some(instrument), Some(symbol), Some(price)) = (
        instrument_config[0].clone(),
        instrument_config[1].clone(),
        instrument_config[2].clone(),
    ) {
        let allow_ibkr_bootstrap_mark = configured_bool(
            settings,
            "broker.allow_ibkr_bootstrap_mark",
            "IT_ALLOW_IBKR_BOOTSTRAP_MARK",
            false,
        )?;
        if broker_is_ibkr && !allow_ibkr_bootstrap_mark {
            return Err(
                "IBKR mode rejects synthetic --price marks; provide broker-authoritative market data or explicitly set broker.allow_ibkr_bootstrap_mark=true"
                    .into(),
            );
        }
        let instrument_id = InstrumentId::new(
            instrument
                .parse::<u128>()
                .map_err(|error| format!("instrument: {error}"))?,
        )
        .map_err(|error| format!("instrument: {error}"))?;
        let price_ticks = price
            .parse::<i64>()
            .map_err(|error| format!("price: {error}"))?;
        if price_ticks <= 0 || symbol.trim().is_empty() {
            return Err("instrument price must be positive and symbol non-empty".into());
        }
        let definition = Instrument::new(InstrumentSpec {
            id: instrument_id,
            symbol: symbol.to_ascii_uppercase(),
            asset_class: AssetClass::Equity,
            venue: if broker_is_ibkr {
                "IBKR".into()
            } else {
                "PAPER".into()
            },
            currency: "USD".into(),
            price_increment_ticks: 1,
            quantity_increment_ticks: 1,
            contract: Contract::Listing,
            provider_symbol: symbol.to_ascii_uppercase(),
        })
        .map_err(|error| format!("instrument definition: {error}"))?;
        catalog
            .insert(
                definition,
                if broker_is_ibkr {
                    "ibkr".into()
                } else {
                    "paper".into()
                },
            )
            .map_err(|error| format!("catalog: {error:?}"))?;
        broker
            .set_price(instrument_id, price_ticks)
            .map_err(|error| format!("paper price: {error:?}"))?;
        portfolio.set_position(
            instrument_id,
            Position {
                quantity_ticks: 0,
                mark_price_ticks: price_ticks,
            },
        );
        let max_position = 1_000_000_i64;
        let max_notional = i128::from(price_ticks).saturating_mul(i128::from(max_position));
        (max_position, max_notional)
    } else {
        (1_000_000_i64, 1_000_000_000_000_i128)
    };
    if let Some(raw_symbols) = yahoo_symbols.as_deref() {
        for entry in raw_symbols
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .take(128)
        {
            let Some((symbol, instrument)) = entry.split_once('=') else {
                continue;
            };
            let symbol = symbol.trim().to_ascii_uppercase();
            let Some(instrument_id) = instrument
                .trim()
                .parse::<u128>()
                .ok()
                .and_then(|value| InstrumentId::new(value).ok())
            else {
                continue;
            };
            if symbol.is_empty()
                || symbol.len() > 16
                || !symbol
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b".-".contains(&byte))
                || catalog.get(instrument_id).is_some()
            {
                continue;
            }
            let Ok(definition) = Instrument::new(InstrumentSpec {
                id: instrument_id,
                symbol: symbol.clone(),
                asset_class: AssetClass::Equity,
                venue: "YAHOO".into(),
                currency: "USD".into(),
                price_increment_ticks: 1,
                quantity_increment_ticks: 1,
                contract: Contract::Listing,
                provider_symbol: symbol,
            }) else {
                continue;
            };
            let _ = catalog.insert(definition, "yahoo".into());
        }
    }
    let max_position = configured_positive_i64_setting(
        settings,
        "risk.max_position_ticks",
        "IT_MAX_POSITION_TICKS",
        max_position,
    )?;
    let max_notional = configured_positive_i128_setting(
        settings,
        "risk.max_gross_notional_ticks",
        "IT_MAX_GROSS_NOTIONAL_TICKS",
        max_notional,
    )?;
    Ok((catalog, portfolio, max_position, max_notional))
}

fn configured_positive_i64_setting(
    settings: &Settings,
    key: &str,
    environment: &str,
    default: i64,
) -> Result<i64, String> {
    if let Some(value) = settings.get(key) {
        let Value::Integer(value) = value else {
            return Err(format!("{key} must be a positive integer"));
        };
        return (*value > 0)
            .then_some(*value)
            .ok_or_else(|| format!("{key} must be positive"));
    }
    configured_positive_i64(environment, default)
}

fn configured_positive_i128_setting(
    settings: &Settings,
    key: &str,
    environment: &str,
    default: i128,
) -> Result<i128, String> {
    if let Some(value) = settings.get(key) {
        let Value::Integer(value) = value else {
            return Err(format!("{key} must be a positive integer"));
        };
        return (*value > 0)
            .then_some(i128::from(*value))
            .ok_or_else(|| format!("{key} must be positive"));
    }
    configured_positive_i128(environment, default)
}

fn configured_positive_i64(name: &str, default: i64) -> Result<i64, String> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    let value = value
        .into_string()
        .map_err(|_| format!("{name} is not valid UTF-8"))?;
    let parsed = value
        .parse::<i64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    (parsed > 0)
        .then_some(parsed)
        .ok_or_else(|| format!("{name} must be positive"))
}

fn configured_positive_i128(name: &str, default: i128) -> Result<i128, String> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    let value = value
        .into_string()
        .map_err(|_| format!("{name} is not valid UTF-8"))?;
    let parsed = value
        .parse::<i128>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    (parsed > 0)
        .then_some(parsed)
        .ok_or_else(|| format!("{name} must be positive"))
}

fn configured_risk_settings(args: &[String]) -> Result<Settings, String> {
    let mut settings = configured_cfg_settings(args)?;
    if let Some(value) = std::env::var_os("IT_ALERT_WEBHOOK_URL") {
        let value = value
            .into_string()
            .map_err(|_| "IT_ALERT_WEBHOOK_URL is not valid UTF-8".to_owned())?;
        if value.len() > 2_048 || !value.starts_with("https://") {
            return Err("IT_ALERT_WEBHOOK_URL must be an HTTPS URL under 2048 bytes".into());
        }
        settings
            .entry("alerts.webhook_url".into())
            .or_insert_with(|| Value::String(value));
    }
    if let Some(value) = std::env::var_os("IT_MAX_LEVERAGE") {
        let value = value
            .into_string()
            .map_err(|_| "IT_MAX_LEVERAGE is not valid UTF-8".to_owned())?;
        let parsed = value
            .parse::<f64>()
            .map_err(|_| "IT_MAX_LEVERAGE must be a finite non-negative number".to_owned())?;
        if !parsed.is_finite() || parsed < 0.0 {
            return Err("IT_MAX_LEVERAGE must be a finite non-negative number".into());
        }
        settings
            .entry("risk.max_leverage".into())
            .or_insert_with(|| Value::Float(parsed));
    }
    if let Some(value) = std::env::var_os("IT_MAX_DRAWDOWN_BPS") {
        let value = value
            .into_string()
            .map_err(|_| "IT_MAX_DRAWDOWN_BPS is not valid UTF-8".to_owned())?;
        let parsed = value
            .parse::<i64>()
            .map_err(|_| "IT_MAX_DRAWDOWN_BPS must be a non-negative integer".to_owned())?;
        if parsed < 0 {
            return Err("IT_MAX_DRAWDOWN_BPS must be non-negative".into());
        }
        settings
            .entry("risk.max_drawdown_bps".into())
            .or_insert_with(|| Value::Integer(parsed));
    }
    if let Some(value) = std::env::var_os("IT_MAX_OUTSTANDING_ORDERS") {
        let value = value
            .into_string()
            .map_err(|_| "IT_MAX_OUTSTANDING_ORDERS is not valid UTF-8".to_owned())?;
        let parsed = value
            .parse::<i64>()
            .map_err(|_| "IT_MAX_OUTSTANDING_ORDERS must be a non-negative integer".to_owned())?;
        if parsed < 0 {
            return Err("IT_MAX_OUTSTANDING_ORDERS must be non-negative".into());
        }
        settings
            .entry("risk.max_outstanding_orders".into())
            .or_insert_with(|| Value::Integer(parsed));
    }
    for (environment, key) in [
        (
            "IT_MAX_PREDICTED_VOLATILITY_BPS",
            "risk.max_predicted_volatility_bps",
        ),
        ("IT_MAX_PARTICIPATION_BPS", "risk.max_participation_bps"),
        ("IT_MAX_MESSAGE_RATE", "risk.max_message_rate"),
        ("IT_MAX_PRICE_DEVIATION_BPS", "risk.max_price_deviation_bps"),
    ] {
        if let Some(value) = std::env::var_os(environment) {
            let value = value
                .into_string()
                .map_err(|_| format!("{environment} is not valid UTF-8"))?;
            let parsed = value
                .parse::<i64>()
                .map_err(|_| format!("{environment} must be a non-negative integer"))?;
            if parsed < 0 {
                return Err(format!("{environment} must be non-negative"));
            }
            settings
                .entry(key.into())
                .or_insert_with(|| Value::Integer(parsed));
        }
    }
    // Validate the final merged value, including environment fallback, before
    // handing settings to engine startup. This keeps malformed webhook URLs
    // from surviving until the first delivery attempt.
    configured_alert_webhook_url(&settings)?;
    Ok(settings)
}

fn configured_u64(
    settings: &Settings,
    key: &str,
    environment: &str,
    default: u64,
    minimum: u64,
    maximum: u64,
) -> Result<u64, String> {
    let value = match settings.get(key) {
        Some(Value::Integer(value)) if *value >= 0 => u64::try_from(*value)
            .map_err(|_| format!("{key} must be within unsigned integer bounds"))?,
        Some(_) => return Err(format!("{key} must be a non-negative integer")),
        None => configured_env_u64(environment, environment_value(environment)?, default)?,
    };
    if !(minimum..=maximum).contains(&value) {
        return Err(format!("{key} must be between {minimum} and {maximum}"));
    }
    Ok(value)
}

fn configured_bool(
    settings: &Settings,
    key: &str,
    environment: &str,
    default: bool,
) -> Result<bool, String> {
    if let Some(value) = settings.get(key) {
        return match value {
            Value::Boolean(value) => Ok(*value),
            _ => Err(format!("{key} must be boolean")),
        };
    }
    let Some(raw) = environment_value(environment)? else {
        return Ok(default);
    };
    match Some(raw.to_ascii_lowercase()).as_deref() {
        Some("true" | "1") => Ok(true),
        Some("false" | "0") => Ok(false),
        _ => Err(format!("{environment} must be true or false")),
    }
}

fn configured_f64(
    settings: &Settings,
    key: &str,
    environment: &str,
    default: f64,
) -> Result<f64, String> {
    let value = match settings.get(key) {
        Some(Value::Float(value)) => *value,
        Some(Value::Integer(value)) if (-1..=1).contains(value) => value
            .to_string()
            .parse::<f64>()
            .map_err(|_| format!("{key} must be a finite number"))?,
        Some(Value::Integer(_)) => return Err(format!("{key} must be between -1 and 1")),
        Some(_) => return Err(format!("{key} must be a finite number")),
        None => environment_value(environment)?
            .map(|raw| raw.parse::<f64>())
            .transpose()
            .map_err(|_| format!("{environment} must be a finite number"))?
            .unwrap_or(default),
    };
    value
        .is_finite()
        .then_some(value)
        .ok_or_else(|| format!("{key} must be finite"))
}

fn configured_env_u64(environment: &str, raw: Option<String>, default: u64) -> Result<u64, String> {
    let Some(raw) = raw else {
        return Ok(default);
    };
    raw.parse::<u64>()
        .map_err(|_| format!("{environment} must be a non-negative integer"))
}

fn configured_string(
    settings: &Settings,
    key: &str,
    environment: &str,
    default: &str,
) -> Result<String, String> {
    match settings.get(key) {
        Some(Value::String(value)) if !value.trim().is_empty() && value.len() <= 2_048 => {
            Ok(value.clone())
        }
        Some(Value::String(_)) => Err(format!("{key} must be non-empty and at most 2048 bytes")),
        Some(_) => Err(format!("{key} must be a non-empty string")),
        None => configured_env_string(environment, environment_value(environment)?, default),
    }
}

fn configured_optional_string(
    settings: &Settings,
    key: &str,
    environment: &str,
) -> Result<Option<String>, String> {
    if settings.contains_key(key) {
        return configured_string(settings, key, environment, "").map(Some);
    }
    environment_value(environment)
}

fn environment_value(environment: &str) -> Result<Option<String>, String> {
    match std::env::var(environment) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{environment} is not valid UTF-8")),
    }
}

fn configured_env_string(
    environment: &str,
    raw: Option<String>,
    default: &str,
) -> Result<String, String> {
    let Some(value) = raw else {
        return Ok(default.to_owned());
    };
    if value.trim().is_empty() || value.len() > 2_048 {
        return Err(format!(
            "{environment} must be non-empty and at most 2048 bytes"
        ));
    }
    Ok(value)
}

fn validate_llm_base_url(value: &str) -> Result<String, String> {
    let value = value.trim().trim_end_matches('/');
    if value.is_empty() || value.len() > 2_048 || value.chars().any(char::is_whitespace) {
        return Err("llm.base_url must be a non-empty URL under 2048 bytes".into());
    }
    let authority = value
        .strip_prefix("http://")
        .or_else(|| value.strip_prefix("https://"))
        .and_then(|rest| rest.split(['/', '?', '#']).next())
        .unwrap_or_default();
    if authority.is_empty() || authority.contains('@') {
        return Err("llm.base_url must have an authority without userinfo".into());
    }
    let host = authority
        .strip_prefix('[')
        .and_then(|rest| rest.split(']').next())
        .or_else(|| authority.split(':').next())
        .unwrap_or_default();
    let local_http =
        (host == "localhost" || host == "127.0.0.1" || host == "::1") && authority.len() <= 128;
    if !value.starts_with("https://") && !local_http {
        return Err("llm.base_url must use HTTPS except for localhost HTTP".into());
    }
    Ok(value.to_owned())
}

type MarketPollStarter = Box<dyn FnOnce(Arc<ServiceHost>) + Send + 'static>;

fn rounded_f64_to_i64(value: f64) -> Option<i64> {
    value
        .is_finite()
        .then(|| format!("{value:.0}"))?
        .parse()
        .ok()
}

fn valid_ibkr_account_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[allow(clippy::too_many_lines)]
fn configure_broker(
    broker: &Arc<PaperBroker>,
    settings: &Settings,
) -> Result<(Arc<dyn BrokerGateway>, Option<MarketPollStarter>), String> {
    let broker_mode = configured_string(settings, "broker.mode", "IT_BROKER", "paper")?;
    match broker_mode.as_str() {
        "paper" => Ok((broker.clone(), None)),
        "ibkr" => {
            let ibkr_account =
                configured_string(settings, "broker.ibkr_account_id", "IT_IBKR_ACCOUNT_ID", "")?;
            if ibkr_account.is_empty() {
                return Err("broker.ibkr_account_id is required when broker.mode=ibkr".to_owned());
            }
            if !valid_ibkr_account_id(&ibkr_account) {
                return Err("broker.ibkr_account_id must be 1-64 ASCII identifier bytes".to_owned());
            }
            let base_url = configured_string(
                settings,
                "broker.ibkr_base_url",
                "IT_IBKR_BASE_URL",
                "https://127.0.0.1:5000",
            )?;
            let timeout_ms = configured_u64(
                settings,
                "broker.ibkr_timeout_ms",
                "IT_IBKR_TIMEOUT_MS",
                10_000,
                1_000,
                120_000,
            )?;
            let transport = IbkrReqwestTransport::new(timeout_ms)
                .map_err(|error| format!("IBKR transport: {error}"))?;
            let transport = ClientPortalTransport::new(
                transport,
                ClientPortalConfig {
                    base_url,
                    account_id: ibkr_account,
                },
            )
            .map_err(|error| format!("IBKR configuration: {error}"))?;
            transport
                .verify_session()
                .map_err(|error| format!("IBKR session health: {error}"))?;
            let gateway = Arc::new(IbkrGateway::new(
                transport,
                insider_broker_api::Capabilities {
                    market: true,
                    limit: true,
                    fractional_quantity: false,
                    cancel_replace: true,
                },
            ));
            gateway
                .connect_ready()
                .map_err(|error| format!("IBKR session: {error}"))?;
            let conid =
                match configured_optional_string(settings, "broker.ibkr_conid", "IT_IBKR_CONID")? {
                    Some(value) => {
                        let parsed = value
                            .parse::<i64>()
                            .map_err(|_| "broker.ibkr_conid must be an integer".to_owned())?;
                        (parsed > 0)
                            .then_some(parsed)
                            .ok_or_else(|| "broker.ibkr_conid must be positive".to_owned())
                            .map(Some)?
                    }
                    None => None,
                };
            let instrument_id = configured_optional_string(
                settings,
                "broker.ibkr_instrument_id",
                "IT_IBKR_INSTRUMENT_ID",
            )?
            .map(|value| {
                value
                    .parse::<u128>()
                    .ok()
                    .and_then(|value| InstrumentId::new(value).ok())
                    .ok_or_else(|| {
                        "broker.ibkr_instrument_id must be a positive integer".to_owned()
                    })
            })
            .transpose()?;
            let scale = configured_positive_i64_setting(
                settings,
                "broker.ibkr_price_scale",
                "IT_IBKR_PRICE_SCALE",
                10_000,
            )?;
            let poll_interval_ms = configured_u64(
                settings,
                "broker.ibkr_market_poll_ms",
                "IT_IBKR_MARKET_POLL_MS",
                1_000,
                250,
                60_000,
            )?;
            let poll = conid.zip(instrument_id).map(|(conid, instrument)| {
                let interval_ms = poll_interval_ms;
                let gateway = Arc::clone(&gateway);
                Box::new(move |host: Arc<ServiceHost>| {
                    if scale <= 0 {
                        eprintln!("insider-market: invalid IT_IBKR_PRICE_SCALE");
                        return;
                    }
                    let _ = std::thread::Builder::new()
                        .name("ibkr-market-poller".into())
                        .spawn(move || {
                            let mut sequence = 0_u64;
                            loop {
                                match gateway.market_snapshot(conid) {
                                    Ok(snapshot) => {
                                        let Some(bid) = snapshot.bid else {
                                            std::thread::sleep(Duration::from_millis(interval_ms));
                                            continue;
                                        };
                                        let Some(ask) = snapshot.ask else {
                                            std::thread::sleep(Duration::from_millis(interval_ms));
                                            continue;
                                        };
                                        let Ok(scale_f64) = scale.to_string().parse::<f64>() else {
                                            eprintln!("insider-market: invalid IBKR price scale");
                                            std::thread::sleep(Duration::from_millis(interval_ms));
                                            continue;
                                        };
                                        let bid_ticks = (bid * scale_f64).round();
                                        let ask_ticks = (ask * scale_f64).round();
                                        let bid_quantity = snapshot.bid_size.unwrap_or(1.0).round();
                                        let ask_quantity = snapshot.ask_size.unwrap_or(1.0).round();
                                        if !bid_ticks.is_finite()
                                            || !ask_ticks.is_finite()
                                            || bid_ticks <= 0.0
                                            || ask_ticks < bid_ticks
                                            || bid_quantity < 1.0
                                            || ask_quantity < 1.0
                                        {
                                            eprintln!("insider-market: IBKR quote failed canonical validation");
                                        } else if let (Some(bid_ticks), Some(ask_ticks), Some(bid_quantity_ticks), Some(ask_quantity_ticks)) = (
                                            rounded_f64_to_i64(bid_ticks),
                                            rounded_f64_to_i64(ask_ticks),
                                            rounded_f64_to_i64(bid_quantity),
                                            rounded_f64_to_i64(ask_quantity),
                                        ) {
                                            sequence = sequence.saturating_add(1);
                                            let wall = SystemTime::now()
                                                .duration_since(UNIX_EPOCH)
                                                .ok()
                                                .and_then(|value| i64::try_from(value.as_nanos()).ok())
                                                .map(WallTime::from_unix_nanos);
                                            if let Some(wall) = wall {
                                                let _ = host.ingest_market_event(
                                                    MarketEvent::Quote(Quote {
                                                        instrument_id: instrument,
                                                        sequence,
                                                        exchange_time: wall,
                                                        received_mono: host.monotonic_now(),
                                                        bid_ticks,
                                                        ask_ticks,
                                                        bid_quantity_ticks,
                                                        ask_quantity_ticks,
                                                    }),
                                                    wall,
                                                );
                                            }
                                        }
                                    }
                                    Err(error) => eprintln!("insider-market: IBKR quote unavailable: {error}"),
                                }
                                std::thread::sleep(Duration::from_millis(interval_ms));
                            }
                        });
                }) as MarketPollStarter
            });
            Ok((gateway.clone(), poll))
        }
        other => Err(format!("unsupported IT_BROKER mode: {other}")),
    }
}

fn serve(args: &[String]) -> Result<(), String> {
    let journal = PathBuf::from(value(args, "--journal")?);
    let socket = PathBuf::from(value(args, "--socket")?);
    let account = AccountId::new(
        optional_value(args, "--account")
            .unwrap_or_else(|| "1".into())
            .parse::<u128>()
            .map_err(|error| format!("account: {error}"))?,
    )
    .map_err(|error| format!("account: {error}"))?;
    let settings = configured_risk_settings(args)?;
    validate_llm_metadata(&settings)?;
    let startup_settings = settings.clone();
    let broker = Arc::new(PaperBroker::new());
    let (broker_gateway, ibkr_market_poll) = configure_broker(&broker, &settings)?;
    let (catalog, portfolio, max_position, max_notional) =
        configure_demo(args, &broker, &settings)?;
    let host = Arc::new(
        ServiceHost::open(
            journal,
            account,
            broker_gateway,
            portfolio,
            RiskEngine::new(Limits {
                max_position_ticks: max_position,
                max_order_ticks: max_position,
                max_gross_notional_ticks: max_notional,
            }),
            settings,
        )
        .map_err(|error| format!("open engine: {error:?}"))?,
    );
    configure_context_embeddings(&host, &startup_settings)?;
    let summary = host
        .reconcile_trigger(ReconcileTrigger::Startup)
        .map_err(|error| format!("startup reconciliation: {error:?}"))?;
    if !host.is_ready() {
        return Err(format!("startup reconciliation incomplete: {summary:?}"));
    }
    register_reference_metrics(&host, &startup_settings)?;
    register_reference_strategy(&host, &startup_settings)?;
    configure_llm_provider(&host, &startup_settings)?;
    configure_news_polling(&host, args, &startup_settings)?;
    configure_yahoo_news_polling(&host, args, &startup_settings)?;
    configure_python_packages(&host, &startup_settings)?;

    // The paper composition root exposes its configured instrument through the
    // same canonical market hub used by live/replay providers. This initial
    // quote is deliberately a provider fixture, not a UI-side synthetic mark.
    // Run it before `--check` exits so preflight validates the complete fixture
    // path, including instrument identity and quote invariants.
    if let Some((instrument, price)) = configured_paper_quote(args)? {
        host.register_market_instrument(instrument)
            .map_err(|error| format!("register market instrument: {error:?}"))?;
        host.ingest_market_event(
            MarketEvent::Quote(Quote {
                instrument_id: instrument,
                sequence: 1,
                exchange_time: WallTime::from_unix_nanos(0),
                received_mono: insider_common_types::MonoTime::from_nanos(0),
                bid_ticks: price,
                ask_ticks: price,
                bid_quantity_ticks: 1,
                ask_quantity_ticks: 1,
            }),
            WallTime::from_unix_nanos(0),
        )
        .map_err(|error| format!("ingest paper quote: {error:?}"))?;
    }

    // `--check` intentionally stops before spawning background workers or
    // binding the IPC socket. This exercises the same bounded configuration,
    // journal recovery, broker, catalog, risk, provider, and package
    // composition path used by `serve`, while making startup certification
    // safe to run in CI and deployment preflight jobs.
    if args.iter().any(|arg| arg == "--check") {
        return Ok(());
    }

    start_python_scheduler(&host, &startup_settings)?;
    start_execution_scheduler(&host, &startup_settings)?;
    start_reconciliation_loop(&host, &startup_settings)?;
    start_alert_webhook_loop(&host, &startup_settings)?;

    configure_yahoo_history(&host, args, &startup_settings);
    configure_yahoo_quotes(&host, args, &startup_settings);
    if let Some(start) = ibkr_market_poll {
        start(Arc::clone(&host));
    }

    let catalog = Arc::new(catalog);
    let service = Arc::new(
        EngineCommandService::new(host, catalog, grant_desktop_capabilities()?, 4_096)
            .ok_or_else(|| "failed to create command service".to_owned())?,
    );
    let bridge = DesktopBridge::bind(socket, service, 16 * 1024 * 1024)
        .map_err(|error| format!("bind desktop bridge: {error:?}"))?;
    loop {
        bridge
            .serve_next()
            .map_err(|error| format!("desktop bridge: {error:?}"))?;
    }
}

fn run(args: &[String]) -> Result<(), String> {
    if let Some("serve") = args.get(1).map(String::as_str) {
        serve(args)
    } else {
        usage();
        Err("serve command required".into())
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if let Err(error) = run(&args) {
        eprintln!("insider-desktop-bridge: {error}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        configured_alert_webhook_url, configured_bool, configured_cfg_settings,
        configured_env_string, configured_env_u64, configured_f64, configured_news_retry_policy,
        configured_optional_string, configured_paper_quote, configured_positive_i64_setting,
        configured_positive_i128_setting, configured_string, configured_u64, valid_ibkr_account_id,
        validate_llm_base_url, validate_llm_metadata,
    };
    use insider_cfg_core::{Settings, Value};

    #[test]
    fn paper_quote_arguments_are_complete_bounded_and_positive() {
        let valid = vec![
            "serve".to_owned(),
            "--instrument".to_owned(),
            "7".to_owned(),
            "--price".to_owned(),
            "10000".to_owned(),
        ];
        let quote = configured_paper_quote(&valid).ok().flatten();
        assert_eq!(quote.map(|(_, price)| price), Some(10_000));

        let missing_price = vec![
            "serve".to_owned(),
            "--instrument".to_owned(),
            "7".to_owned(),
        ];
        assert!(configured_paper_quote(&missing_price).is_err());

        let malformed_price = vec![
            "serve".to_owned(),
            "--instrument".to_owned(),
            "7".to_owned(),
            "--price".to_owned(),
            "not-a-price".to_owned(),
        ];
        assert!(configured_paper_quote(&malformed_price).is_err());

        let non_positive = vec![
            "serve".to_owned(),
            "--instrument".to_owned(),
            "7".to_owned(),
            "--price".to_owned(),
            "0".to_owned(),
        ];
        assert!(configured_paper_quote(&non_positive).is_err());
    }

    #[test]
    fn config_argument_loads_typed_cfg_values() {
        let path = std::env::temp_dir().join(format!(
            "insidertrader-config-test-{}-{}.cfg",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let write_result = std::fs::write(
            &path,
            "risk.max_leverage = 2.5\nrisk.max_drawdown_bps = 400\nbroker.ibkr_timeout_ms = 5000\n",
        );
        assert!(write_result.is_ok(), "write test config: {write_result:?}");
        let args = vec![
            "serve".to_owned(),
            "--config".to_owned(),
            path.display().to_string(),
        ];
        let settings_result = configured_cfg_settings(&args);
        assert!(settings_result.is_ok(), "config should parse");
        let settings = settings_result.unwrap_or_default();
        assert_eq!(
            settings.get("risk.max_drawdown_bps"),
            Some(&Value::Integer(400))
        );
        assert_eq!(settings.get("risk.max_leverage"), Some(&Value::Float(2.5)));
        assert_eq!(
            settings.get("broker.ibkr_timeout_ms"),
            Some(&Value::Integer(5_000))
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn oversized_config_file_is_rejected_before_parse() {
        let path = std::env::temp_dir().join(format!(
            "insidertrader-config-oversized-{}.cfg",
            std::process::id()
        ));
        assert!(std::fs::write(&path, "x".repeat(1_048_577)).is_ok());
        let args = vec![
            "serve".to_owned(),
            "--config".to_owned(),
            path.display().to_string(),
        ];
        let result = configured_cfg_settings(&args);
        assert!(result.is_err_and(|error| error.contains("exceeds 1 MiB bound")));
        let _ = std::fs::remove_file(path);
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn typed_startup_settings_override_environment_fallbacks() {
        let settings = Settings::from([
            ("scheduler.python_cycle_ms".to_owned(), Value::Integer(250)),
            ("broker.ibkr_timeout_ms".to_owned(), Value::Integer(5_000)),
            ("broker.ibkr_market_poll_ms".to_owned(), Value::Integer(750)),
            (
                "broker.ibkr_account_id".to_owned(),
                Value::String("DU123456".to_owned()),
            ),
            (
                "broker.ibkr_conid".to_owned(),
                Value::String("265598".to_owned()),
            ),
            (
                "broker.ibkr_instrument_id".to_owned(),
                Value::String("1".to_owned()),
            ),
            ("broker.mode".to_owned(), Value::String("paper".to_owned())),
            (
                "strategy.reference_enabled".to_owned(),
                Value::Boolean(true),
            ),
            (
                "llm.base_url".to_owned(),
                Value::String("https://local.invalid/v1".to_owned()),
            ),
        ]);
        assert_eq!(
            configured_u64(
                &settings,
                "scheduler.python_cycle_ms",
                "IT_PYTHON_CYCLE_MS",
                100,
                25,
                60_000
            )
            .ok(),
            Some(250)
        );
        assert_eq!(
            configured_u64(
                &settings,
                "broker.ibkr_timeout_ms",
                "IT_IBKR_TIMEOUT_MS",
                10_000,
                1_000,
                120_000
            )
            .ok(),
            Some(5_000)
        );
        assert_eq!(
            configured_u64(
                &settings,
                "broker.ibkr_market_poll_ms",
                "IT_IBKR_MARKET_POLL_MS",
                1_000,
                250,
                60_000
            )
            .ok(),
            Some(750)
        );
        assert_eq!(
            configured_string(&settings, "broker.mode", "IT_BROKER", "paper")
                .ok()
                .as_deref(),
            Some("paper")
        );
        assert_eq!(
            configured_string(
                &settings,
                "broker.ibkr_account_id",
                "IT_IBKR_ACCOUNT_ID",
                ""
            )
            .ok()
            .as_deref(),
            Some("DU123456")
        );
        assert!(valid_ibkr_account_id("DU123456"));
        assert!(!valid_ibkr_account_id("DU 123456"));
        assert!(!valid_ibkr_account_id(&"D".repeat(65)));
        assert_eq!(
            configured_optional_string(&settings, "broker.ibkr_conid", "IT_IBKR_CONID")
                .ok()
                .flatten()
                .and_then(|value| value.parse::<i64>().ok()),
            Some(265_598)
        );
        assert_eq!(
            configured_optional_string(
                &settings,
                "broker.ibkr_instrument_id",
                "IT_IBKR_INSTRUMENT_ID",
            )
            .ok()
            .flatten()
            .and_then(|value| value.parse::<u128>().ok()),
            Some(1)
        );
        assert_eq!(
            configured_bool(
                &settings,
                "strategy.reference_enabled",
                "IT_ENABLE_REFERENCE_STRATEGY",
                false
            )
            .ok(),
            Some(true)
        );
        let invalid_bool = Settings::from([(
            "strategy.reference_enabled".to_owned(),
            Value::String("true".to_owned()),
        )]);
        assert!(
            configured_bool(
                &invalid_bool,
                "strategy.reference_enabled",
                "IT_ENABLE_REFERENCE_STRATEGY",
                false
            )
            .is_err()
        );
        assert_eq!(
            configured_bool(
                &Settings::new(),
                "market.allow_yahoo_live_marks",
                "IT_ALLOW_YAHOO_LIVE_MARKS_UNSET",
                false,
            )
            .ok(),
            Some(false)
        );
        let approved_marks = Settings::from([
            (
                "market.allow_yahoo_live_marks".to_owned(),
                Value::Boolean(true),
            ),
            (
                "broker.allow_ibkr_bootstrap_mark".to_owned(),
                Value::Boolean(true),
            ),
            ("python.allow_network".to_owned(), Value::Boolean(true)),
        ]);
        assert_eq!(
            configured_bool(
                &approved_marks,
                "market.allow_yahoo_live_marks",
                "IT_ALLOW_YAHOO_LIVE_MARKS",
                false,
            )
            .ok(),
            Some(true)
        );
        assert_eq!(
            configured_bool(
                &approved_marks,
                "broker.allow_ibkr_bootstrap_mark",
                "IT_ALLOW_IBKR_BOOTSTRAP_MARK",
                false,
            )
            .ok(),
            Some(true)
        );
        assert_eq!(
            configured_bool(
                &approved_marks,
                "python.allow_network",
                "IT_PYTHON_ALLOW_NETWORK",
                false,
            )
            .ok(),
            Some(true)
        );
        assert_eq!(
            configured_string(
                &settings,
                "llm.base_url",
                "IT_LLM_BASE_URL",
                "https://api.openai.com/v1"
            )
            .ok()
            .as_deref(),
            Some("https://local.invalid/v1")
        );
    }

    #[test]
    fn reference_threshold_uses_typed_cfg_value() {
        let settings = Settings::from([(
            "strategy.reference_entry_threshold".to_owned(),
            Value::Float(0.75),
        )]);
        assert_eq!(
            configured_f64(
                &settings,
                "strategy.reference_entry_threshold",
                "IT_REFERENCE_ENTRY_THRESHOLD",
                0.5
            )
            .ok(),
            Some(0.75)
        );
        let zero = Settings::from([(
            "strategy.reference_exit_threshold".to_owned(),
            Value::Integer(0),
        )]);
        assert_eq!(
            configured_f64(
                &zero,
                "strategy.reference_exit_threshold",
                "IT_REFERENCE_EXIT_THRESHOLD",
                0.1
            )
            .ok(),
            Some(0.0)
        );
    }

    #[test]
    fn optional_provider_query_prefers_cfg_and_rejects_wrong_types() {
        let settings = Settings::from([(
            "news.newsapi_query".to_owned(),
            Value::String("AAPL earnings".to_owned()),
        )]);
        assert_eq!(
            configured_optional_string(&settings, "news.newsapi_query", "IT_NEWSAPI_QUERY")
                .ok()
                .flatten()
                .as_deref(),
            Some("AAPL earnings")
        );
        let absent = Settings::new();
        assert!(
            configured_optional_string(
                &absent,
                "news.yahoo_query",
                "IT_INSIDERTRADER_TEST_OPTIONAL_QUERY",
            )
            .ok()
            .flatten()
            .is_none()
        );
        let invalid = Settings::from([("news.newsapi_query".to_owned(), Value::Integer(1))]);
        assert!(
            configured_optional_string(&invalid, "news.newsapi_query", "IT_NEWSAPI_QUERY").is_err()
        );
    }

    #[test]
    fn newsapi_headline_filters_prefer_cfg_and_validate_types() {
        let settings = Settings::from([
            (
                "news.newsapi_country".to_owned(),
                Value::String("us".to_owned()),
            ),
            (
                "news.newsapi_category".to_owned(),
                Value::String("business".to_owned()),
            ),
            (
                "news.newsapi_sources".to_owned(),
                Value::String("reuters,associated-press".to_owned()),
            ),
        ]);
        assert_eq!(
            configured_optional_string(&settings, "news.newsapi_country", "IT_NEWSAPI_COUNTRY",)
                .ok()
                .flatten()
                .as_deref(),
            Some("us")
        );
        assert_eq!(
            configured_optional_string(&settings, "news.newsapi_category", "IT_NEWSAPI_CATEGORY",)
                .ok()
                .flatten()
                .as_deref(),
            Some("business")
        );
        assert_eq!(
            configured_optional_string(&settings, "news.newsapi_sources", "IT_NEWSAPI_SOURCES",)
                .ok()
                .flatten()
                .as_deref(),
            Some("reuters,associated-press")
        );
        let invalid = Settings::from([("news.newsapi_country".to_owned(), Value::Integer(1))]);
        assert!(
            configured_optional_string(&invalid, "news.newsapi_country", "IT_NEWSAPI_COUNTRY",)
                .is_err()
        );
    }

    #[test]
    fn yahoo_symbol_list_is_file_first_and_bounded() {
        let settings = Settings::from([(
            "market.yahoo_symbols".to_owned(),
            Value::String("AAPL=1,MSFT=2".to_owned()),
        )]);
        assert_eq!(
            configured_optional_string(&settings, "market.yahoo_symbols", "IT_YAHOO_SYMBOLS")
                .ok()
                .flatten()
                .as_deref(),
            Some("AAPL=1,MSFT=2")
        );
        let oversized = Settings::from([(
            "market.yahoo_symbols".to_owned(),
            Value::String("x".repeat(2_049)),
        )]);
        assert!(
            configured_optional_string(&oversized, "market.yahoo_symbols", "IT_YAHOO_SYMBOLS")
                .is_err()
        );
    }

    #[test]
    fn cfg_risk_sizing_limits_override_environment_defaults_and_reject_invalid_types() {
        let settings = Settings::from([
            ("risk.max_position_ticks".to_owned(), Value::Integer(42)),
            (
                "risk.max_gross_notional_ticks".to_owned(),
                Value::Integer(9_999),
            ),
        ]);
        assert_eq!(
            configured_positive_i64_setting(
                &settings,
                "risk.max_position_ticks",
                "IT_MAX_POSITION_TICKS",
                100
            )
            .ok(),
            Some(42)
        );
        assert_eq!(
            configured_positive_i128_setting(
                &settings,
                "risk.max_gross_notional_ticks",
                "IT_MAX_GROSS_NOTIONAL_TICKS",
                100
            )
            .ok(),
            Some(9_999)
        );
        let invalid = Settings::from([("risk.max_position_ticks".to_owned(), Value::Float(1.0))]);
        assert!(
            configured_positive_i64_setting(
                &invalid,
                "risk.max_position_ticks",
                "IT_MAX_POSITION_TICKS",
                100
            )
            .is_err()
        );
    }

    #[test]
    fn malformed_environment_numeric_fallback_is_not_silently_defaulted() {
        let result = configured_env_u64("IT_TEST_INTERVAL", Some("not-a-number".to_owned()), 100);
        assert_eq!(
            result,
            Err("IT_TEST_INTERVAL must be a non-negative integer".to_owned())
        );
        assert_eq!(
            configured_env_u64("IT_TEST_INTERVAL", None, 100).ok(),
            Some(100)
        );
    }

    #[test]
    fn empty_environment_string_fallback_is_not_silently_defaulted() {
        assert_eq!(
            configured_env_string("IT_TEST_URL", Some("  ".to_owned()), "https://default.test"),
            Err("IT_TEST_URL must be non-empty and at most 2048 bytes".to_owned())
        );
        assert!(configured_env_string("IT_TEST_URL", Some("x".repeat(2_049)), "default").is_err());
        assert_eq!(
            configured_env_string("IT_TEST_URL", None, "https://default.test").ok(),
            Some("https://default.test".to_owned())
        );
    }

    #[test]
    fn oversized_cfg_string_setting_is_rejected_before_use() {
        let settings = Settings::from([(
            String::from("python.workdir"),
            Value::String("x".repeat(2_049)),
        )]);
        assert!(
            configured_string(
                &settings,
                "python.workdir",
                "IT_PYTHON_WORKDIR",
                "data/python-workers"
            )
            .is_err()
        );
    }

    #[test]
    fn llm_base_url_requires_https_or_strict_local_http() {
        assert!(validate_llm_base_url("https://provider.example/v1").is_ok());
        assert!(validate_llm_base_url("http://localhost:8080/v1").is_ok());
        assert!(validate_llm_base_url("http://127.0.0.1/v1").is_ok());
        assert!(validate_llm_base_url("http://provider.example/v1").is_err());
        assert!(validate_llm_base_url("http://localhost.evil/v1").is_err());
        assert!(validate_llm_base_url("https://user:password@provider.example/v1").is_err());
        assert!(validate_llm_base_url("https:///v1").is_err());
    }

    #[test]
    fn news_retry_settings_enforce_attempt_and_delay_relationships() {
        let valid = Settings::from([
            ("news.max_retries".to_owned(), Value::Integer(8)),
            ("news.retry_base_ms".to_owned(), Value::Integer(500)),
            ("news.retry_max_ms".to_owned(), Value::Integer(20_000)),
        ]);
        assert_eq!(
            configured_news_retry_policy(&valid).map(|policy| {
                (
                    policy.max_retries,
                    policy.base_delay_ms,
                    policy.max_delay_ms,
                )
            }),
            Ok((8, 500, 20_000))
        );

        let invalid_order = Settings::from([
            ("news.retry_base_ms".to_owned(), Value::Integer(20_001)),
            ("news.retry_max_ms".to_owned(), Value::Integer(20_000)),
        ]);
        assert!(configured_news_retry_policy(&invalid_order).is_err());

        let invalid_attempts =
            Settings::from([(String::from("news.max_retries"), Value::Integer(17))]);
        assert!(configured_news_retry_policy(&invalid_attempts).is_err());
    }

    #[test]
    fn control_plane_settings_reject_out_of_range_values() {
        let settings = Settings::from([
            ("reconciliation.poll_ms".to_owned(), Value::Integer(999)),
            (
                "alerts.webhook_timeout_ms".to_owned(),
                Value::Integer(30_001),
            ),
        ]);
        assert!(
            configured_u64(
                &settings,
                "reconciliation.poll_ms",
                "IT_RECONCILIATION_POLL_MS",
                30_000,
                1_000,
                300_000
            )
            .is_err()
        );
        assert!(
            configured_u64(
                &settings,
                "alerts.webhook_timeout_ms",
                "IT_ALERT_WEBHOOK_TIMEOUT_MS",
                2_000,
                250,
                30_000
            )
            .is_err()
        );
    }

    #[test]
    fn cfg_webhook_url_is_optional_bounded_and_https_only() {
        assert_eq!(
            configured_alert_webhook_url(&Settings::new())
                .ok()
                .flatten(),
            None
        );
        let valid = Settings::from([(
            "alerts.webhook_url".to_owned(),
            Value::String("https://localhost/alerts".into()),
        )]);
        assert_eq!(
            configured_alert_webhook_url(&valid)
                .ok()
                .flatten()
                .as_deref(),
            Some("https://localhost/alerts")
        );
        let invalid = Settings::from([(
            "alerts.webhook_url".to_owned(),
            Value::String("http://localhost/alerts".into()),
        )]);
        assert!(configured_alert_webhook_url(&invalid).is_err());
        let userinfo = Settings::from([(
            "alerts.webhook_url".to_owned(),
            Value::String("https://user:password@localhost/alerts".into()),
        )]);
        assert!(configured_alert_webhook_url(&userinfo).is_err());
    }

    #[test]
    fn llm_metadata_rejects_explicit_empty_or_wrong_types() {
        assert!(validate_llm_metadata(&Settings::new()).is_ok());
        let valid = Settings::from([("llm.model".to_owned(), Value::String("desk-model".into()))]);
        assert!(validate_llm_metadata(&valid).is_ok());
        let empty = Settings::from([("llm.prompt_version".to_owned(), Value::String(" ".into()))]);
        assert!(validate_llm_metadata(&empty).is_err());
        let wrong = Settings::from([("llm.model".to_owned(), Value::Integer(1))]);
        assert!(validate_llm_metadata(&wrong).is_err());
    }
}
