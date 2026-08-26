//! Headless `InsiderTrader` process for inspection and paper execution.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use insider_broker_api::BrokerGateway;
use insider_cfg_core::Settings;
use insider_common_types::{AccountId, InstrumentId, MonoTime, ProposalId, TraceId};
use insider_engine::{ReconcileTrigger, ServiceHost};
use insider_exchange_sim::PaperBroker;
use insider_journal::Journal;
use insider_portfolio::{Portfolio, Position};
use insider_risk_engine::{Limits, RiskEngine};
use insider_strategy_sdk::{Action, Proposal};

fn usage() {
    eprintln!("usage: insider-engine inspect --journal PATH");
    eprintln!("       insider-engine seal --journal PATH");
    eprintln!(
        "       insider-engine paper --journal PATH --instrument ID --price TICKS --quantity TICKS"
    );
}

fn value(args: &[String], name: &str) -> Result<String, String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .ok_or_else(|| format!("missing {name}"))
}

fn inspect(path: PathBuf) -> Result<(), String> {
    let journal = Journal::open(path).map_err(|error| format!("open journal: {error}"))?;
    let scan = journal
        .scan()
        .map_err(|error| format!("scan journal: {error}"))?;
    println!(
        "records={} valid_bytes={} invalid_tail={}",
        scan.records.len(),
        scan.valid_bytes,
        scan.has_invalid_tail
    );
    match journal.verify_seal() {
        Ok(digest) => println!("seal=verified sha256={digest:02x?}"),
        Err(error) => println!("seal=unverified reason={error}"),
    }
    Ok(())
}

fn seal(path: PathBuf) -> Result<(), String> {
    let journal = Journal::open(path).map_err(|error| format!("open journal: {error}"))?;
    let digest = journal
        .seal()
        .map_err(|error| format!("seal journal: {error}"))?;
    println!("seal=published sha256={digest:02x?}");
    Ok(())
}

fn paper(path: PathBuf, instrument_value: u128, price: i64, quantity: i64) -> Result<(), String> {
    let account = AccountId::new(1).map_err(|error| format!("account: {error}"))?;
    let instrument =
        InstrumentId::new(instrument_value).map_err(|error| format!("instrument: {error}"))?;
    let proposal_id = ProposalId::new(1).map_err(|error| format!("proposal: {error}"))?;
    let trace = TraceId::new(1).map_err(|error| format!("trace: {error}"))?;
    let broker = Arc::new(PaperBroker::new());
    broker
        .set_price(instrument, price)
        .map_err(|error| format!("paper price: {error:?}"))?;
    let broker_gateway: Arc<dyn BrokerGateway> = broker.clone();
    let quantity_limit = i64::try_from(quantity.unsigned_abs().max(1))
        .map_err(|error| format!("quantity limit: {error}"))?;
    let mut portfolio = Portfolio::new();
    portfolio.set_position(
        instrument,
        Position {
            quantity_ticks: 0,
            mark_price_ticks: price,
        },
    );
    let host = ServiceHost::open(
        path,
        account,
        broker_gateway,
        portfolio,
        RiskEngine::new(Limits {
            max_position_ticks: quantity_limit,
            max_order_ticks: quantity_limit,
            max_gross_notional_ticks: i128::from(price).saturating_mul(i128::from(quantity_limit)),
        }),
        Settings::from(BTreeMap::new()),
    )
    .map_err(|error| format!("open engine: {error:?}"))?;
    let startup = host
        .reconcile_trigger(ReconcileTrigger::Startup)
        .map_err(|error| format!("startup reconcile: {error:?}"))?;
    if !host.is_ready() {
        return Err(format!("startup reconciliation incomplete: {startup:?}"));
    }
    let proposal = Proposal {
        proposal_id,
        strategy_id: String::from("cli.paper.v1"),
        instrument_id: instrument,
        action: Action::TargetQuantity {
            quantity_ticks: quantity,
        },
        confidence: 1.0,
        horizon_ns: 1_000_000_000,
        ttl_ns: 10_000_000_000,
        evidence: Vec::new(),
        generated_mono: MonoTime::from_nanos(0),
    };
    let client = host
        .submit_proposal(&proposal, MonoTime::from_nanos(1), trace)
        .map_err(|error| format!("submit: {error:?}"))?;
    for event in broker.drain_events() {
        host.apply_broker_event(event)
            .map_err(|error| format!("broker event: {error:?}"))?;
    }
    let portfolio = host
        .runtime()
        .portfolio()
        .map_err(|error| format!("portfolio: {error:?}"))?;
    println!("client_order_id={client}");
    println!(
        "position_ticks={} cash_ticks={} realized_pnl_ticks={} fees_ticks={}",
        portfolio
            .position(instrument)
            .map_or(0, |position| position.quantity_ticks),
        portfolio.cash_ticks,
        portfolio.realized_pnl_ticks,
        portfolio.fees_ticks
    );
    Ok(())
}

fn run(args: &[String]) -> Result<(), String> {
    let Some(command) = args.get(1).map(String::as_str) else {
        usage();
        return Err(String::from("command required"));
    };
    let path = PathBuf::from(value(args, "--journal")?);
    match command {
        "inspect" => inspect(path),
        "seal" => seal(path),
        "paper" => {
            let instrument = value(args, "--instrument")?
                .parse::<u128>()
                .map_err(|error| format!("instrument: {error}"))?;
            let price = value(args, "--price")?
                .parse::<i64>()
                .map_err(|error| format!("price: {error}"))?;
            let quantity = value(args, "--quantity")?
                .parse::<i64>()
                .map_err(|error| format!("quantity: {error}"))?;
            if price <= 0 || quantity == 0 {
                return Err(String::from("price must be positive and quantity non-zero"));
            }
            paper(path, instrument, price, quantity)
        }
        _ => {
            usage();
            Err(format!("unknown command {command}"))
        }
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if let Err(error) = run(&args) {
        eprintln!("insider-engine: {error}");
        std::process::exit(2);
    }
}
