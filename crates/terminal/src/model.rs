use std::fmt::Write as _;

const SNAPSHOT_MAGIC: &[u8] = b"IT_RUNTIME_SNAPSHOT_V14\0";
const RESOLVED_INSTRUMENT_MAGIC: &[u8] = b"IT_RESOLVED_INSTRUMENT_V1\0";
const PROPOSAL_SUBMIT_MAGIC: &[u8] = b"IT_CMD_PROPOSAL_SUBMIT_RESPONSE_V1\0";

#[derive(Clone, Debug, Default)]
pub struct RuntimeView {
    pub account_id: u128,
    pub cursor: u64,
    pub risk: String,
    pub mode: String,
    pub plan: Option<AutonomyPlanView>,
    pub llm_provider_id: Option<String>,
    pub llm_model: Option<String>,
    pub cash_ticks: i64,
    pub realized_pnl_ticks: i64,
    pub fees_ticks: i64,
    pub gross_notional_ticks: i128,
    pub max_gross_notional_ticks: i128,
    pub gross_utilization_bps: i64,
    pub largest_position_notional_ticks: i128,
    pub drawdown_bps: Option<i64>,
    pub positions: Vec<PositionView>,
    pub orders: Vec<OrderView>,
    pub fills: Vec<FillView>,
    pub tca: Vec<TcaView>,
    pub proposals: Vec<ProposalView>,
    pub markets: Vec<MarketView>,
}

#[derive(Clone, Debug)]
pub struct AutonomyPlanView {
    pub id: String,
    pub state: String,
    pub generated_at_ns: u64,
    pub expires_at_ns: u64,
    pub actions: Vec<AutonomyActionView>,
}

#[derive(Clone, Debug)]
pub struct AutonomyActionView {
    pub action: String,
    pub proposal_id: Option<String>,
    pub scale: Option<f64>,
    pub reason_codes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct PositionView {
    pub instrument: u128,
    pub quantity: i64,
    pub mark: i64,
    pub average_cost: i64,
}

#[derive(Clone, Debug)]
pub struct OrderView {
    pub client_order_id: String,
    pub instrument: u128,
    pub side: String,
    pub quantity: i64,
    pub filled: i64,
    pub state: String,
}

#[derive(Clone, Debug)]
pub struct FillView {
    pub client_order_id: String,
    pub instrument: u128,
    pub signed_quantity: i64,
    pub price: i64,
}

#[derive(Clone, Debug)]
pub struct TcaView {
    pub client_order_id: String,
    pub filled_quantity: i64,
    pub notional: i128,
    pub average_price_numerator: i128,
    pub average_price_denominator: i64,
    pub arrival_price: Option<i64>,
    pub decision_ns: Option<u64>,
    pub send_ns: Option<u64>,
    pub ack_ns: Option<u64>,
    pub first_fill_ns: Option<u64>,
    pub shortfall: Option<i128>,
    pub spread: Option<i64>,
    pub adverse_selection: Option<i128>,
}

#[derive(Clone, Debug)]
pub struct ProposalView {
    pub id: u128,
    pub instrument: u128,
    pub strategy: String,
    pub action: String,
    pub confidence: f64,
    pub ttl_ns: u64,
    pub state: String,
}

#[derive(Clone, Debug)]
pub struct MarketView {
    pub instrument: u128,
    pub bid: Option<i64>,
    pub ask: Option<i64>,
    pub last: Option<i64>,
    pub quote_quality: String,
    pub trade_quality: String,
    pub book_top: Option<(i64, i64, i64, i64)>,
    pub trades: Vec<TradeView>,
    pub bars: Vec<BarView>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedInstrumentView {
    pub instrument: u128,
    pub asset_class: String,
    pub symbol: String,
    pub venue: String,
}

#[derive(Clone, Debug)]
pub struct TradeView {
    pub sequence: u64,
    pub exchange_time_ns: i64,
    pub received_mono_ns: u64,
    pub price: i64,
    pub quantity: i64,
}

#[derive(Clone, Debug)]
pub struct BarView {
    pub start_time_ns: i64,
    pub interval_ns: u64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: i64,
}

#[derive(Clone, Debug)]
pub struct StrategyView {
    pub id: String,
    pub mode: String,
    pub state: String,
    pub lifecycle: String,
    pub priority: String,
    pub metrics: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct MetricView {
    pub id: String,
    pub state: String,
    pub lifecycle: String,
    pub priority: String,
    pub period_ns: u64,
    pub deadline_ns: u64,
    pub inputs: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct AlertView {
    pub id: String,
    pub source: String,
    pub occurred_ms: i64,
    pub severity: u8,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct NewsView {
    pub id: String,
    pub title: String,
    pub source: String,
    pub url: String,
    pub received_ms: i64,
    pub symbols: Vec<String>,
    pub relevance: f64,
}

#[derive(Clone, Debug)]
pub struct NewsItemDetailView {
    pub id: String,
    pub provider: String,
    pub url: String,
    pub source: String,
    pub title: String,
    pub summary: Option<String>,
    pub published_ms: Option<i64>,
    pub received_ms: i64,
    pub symbols: Vec<String>,
    pub content_hash: String,
}

#[derive(Clone, Debug)]
pub struct NewsDetailView {
    pub current: NewsItemDetailView,
    pub versions: Vec<NewsItemDetailView>,
    pub cluster_id: String,
    pub related_item_ids: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct PreviewView {
    pub id: String,
    pub expected_version: u64,
    pub estimated_notional: i128,
    pub estimated_cost_bps: i64,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct TraceView {
    pub sequence: u64,
    pub kind: String,
    pub payload_bytes: usize,
    pub payload_preview: String,
}

#[derive(Clone, Debug)]
pub struct ContextHitView {
    pub node_id: String,
    pub score: f64,
    pub exact_score: f64,
    pub lexical_score: f64,
    pub vector_score: f64,
    pub evidence_path: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct AnalystView {
    pub trace_id: String,
    pub finish_reason: String,
    pub content: String,
}

#[derive(Clone, Debug)]
pub struct BacktestView {
    pub run_id: String,
    pub strategy_id: String,
    pub dataset_hash: String,
    pub config_hash: String,
    pub event_count: u64,
    pub max_drawdown: i128,
    pub fees: i128,
    pub final_equity: Option<i128>,
}

#[derive(Clone, Debug)]
pub struct ModelView {
    pub model_id: String,
    pub version: String,
    pub artifact_hash: String,
    pub input_width: u64,
    pub status: String,
    pub active: bool,
}

#[derive(Clone, Debug)]
pub struct ResolutionView {
    pub policy: String,
    pub now_ns: u64,
    pub accepted: u32,
    pub conflicts: u32,
    pub expired: u32,
    pub attributions: u32,
}

#[derive(Clone, Debug)]
pub struct StrategyExecutionView {
    pub strategy_id: String,
    pub fills: u64,
    pub quantity: i128,
    pub notional: i128,
}

#[derive(Clone, Debug)]
pub struct ExperimentView {
    pub run_id: String,
    pub code_hash: String,
    pub config_hash: String,
    pub dataset_hash: String,
    pub status: String,
    pub metrics: Vec<(String, f64)>,
    pub artifact_count: usize,
    pub provenance: Vec<String>,
}

#[allow(clippy::too_many_lines)]
pub fn decode_snapshot(bytes: &[u8]) -> Result<RuntimeView, String> {
    let mut reader = Reader::after_magic(bytes, SNAPSHOT_MAGIC)?;
    let account_id = reader.u128()?;
    let cursor = reader.u64()?;
    let risk = code(
        &["", "RUNNING", "REDUCE ONLY", "CANCEL ONLY", "HALTED"],
        reader.u8()?,
    )?;
    let mode = code(&["", "MANUAL", "HYBRID", "AUTONOMOUS"], reader.u8()?)?;
    let plan = match reader.u8()? {
        0 => None,
        1 => {
            let id = reader.string()?;
            let state = code(
                &[
                    "",
                    "PENDING",
                    "APPROVED",
                    "REJECTED",
                    "EXPIRED",
                    "EXECUTING",
                    "COMPLETED",
                    "FAILED",
                ],
                reader.u8()?,
            )?;
            let generated_at_ns = reader.u64()?;
            let expires_at_ns = reader.u64()?;
            let action_count = usize::from(reader.u16()?);
            if action_count > 4_096 {
                return Err("autonomous action collection exceeds bound".into());
            }
            let mut actions = Vec::with_capacity(action_count);
            for _ in 0..action_count {
                let action = code(
                    &[
                        "",
                        "EXECUTE PROPOSAL",
                        "EXECUTE SCALED",
                        "IGNORE PROPOSAL",
                        "PAUSE STRATEGY",
                        "RESUME STRATEGY",
                        "REQUEST REANALYSIS",
                        "ADD TO WATCH",
                        "REMOVE FROM WATCH",
                        "REDUCE AUTONOMY",
                        "NO ACTION",
                    ],
                    reader.u8()?,
                )?;
                let proposal = reader.string()?;
                let scale_marker = reader.u8()?;
                let scale = optional(scale_marker, reader.f64()?)?;
                let reason_count = usize::from(reader.u16()?);
                if reason_count > 256 {
                    return Err("autonomous reason collection exceeds bound".into());
                }
                let mut reason_codes = Vec::with_capacity(reason_count);
                for _ in 0..reason_count {
                    reason_codes.push(reader.string()?);
                }
                actions.push(AutonomyActionView {
                    action,
                    proposal_id: (!proposal.is_empty()).then_some(proposal),
                    scale,
                    reason_codes,
                });
            }
            Some(AutonomyPlanView {
                id,
                state,
                generated_at_ns,
                expires_at_ns,
                actions,
            })
        }
        _ => return Err("invalid autonomous plan marker".into()),
    };
    let llm_provider_id = reader.optional_string()?;
    let llm_model = reader.optional_string()?;
    let cash_ticks = reader.i64()?;
    let realized_pnl_ticks = reader.i64()?;
    let fees_ticks = reader.i64()?;
    let gross_notional_ticks = reader.i128()?;
    let max_gross_notional_ticks = reader.i128()?;
    let gross_utilization_bps = reader.i64()?;
    let largest_position_notional_ticks = reader.i128()?;
    let drawdown_present = reader.u8()?;
    let drawdown = reader.i64()?;
    let drawdown_bps = match drawdown_present {
        0 => None,
        1 => Some(drawdown),
        _ => return Err("invalid drawdown marker".into()),
    };

    let position_count = reader.count(16_384)?;
    let mut positions = Vec::with_capacity(position_count);
    for _ in 0..position_count {
        positions.push(PositionView {
            instrument: reader.u128()?,
            quantity: reader.i64()?,
            mark: reader.i64()?,
            average_cost: reader.i64()?,
        });
    }

    let order_count = reader.count(16_384)?;
    let mut orders = Vec::with_capacity(order_count);
    for _ in 0..order_count {
        let intent = decode_order_intent(reader.byte_vec(1024 * 1024)?)?;
        let _broker_id = reader.string()?;
        let filled = reader.i64()?;
        let state = code(
            &[
                "",
                "RISK APPROVED",
                "QUEUED",
                "SENDING",
                "SENT",
                "ACKNOWLEDGED",
                "PARTIAL",
                "FILLED",
                "REJECTED",
                "CANCEL PENDING",
                "CANCELLED",
                "REPLACE PENDING",
                "UNKNOWN",
                "CREATED",
                "EXPIRED",
            ],
            reader.u8()?,
        )?;
        orders.push(OrderView {
            client_order_id: intent.0,
            instrument: intent.1,
            side: intent.2,
            quantity: intent.3,
            filled,
            state,
        });
    }

    let fill_count = reader.count(10_000)?;
    let mut fills = Vec::with_capacity(fill_count);
    for _ in 0..fill_count {
        fills.push(FillView {
            client_order_id: reader.string()?,
            instrument: reader.u128()?,
            signed_quantity: reader.i64()?,
            price: reader.i64()?,
        });
    }
    let tca_count = reader.count(16_384)?;
    let mut tca = Vec::with_capacity(tca_count);
    for _ in 0..tca_count {
        let client_order_id = reader.string()?;
        let filled_quantity = reader.i64()?;
        let notional = reader.i128()?;
        let average_price_numerator = reader.i128()?;
        let average_price_denominator = reader.i64()?;
        let arrival_price = reader.optional_i64()?;
        let decision_ns = reader.optional_u64()?;
        let send_ns = reader.optional_u64()?;
        let ack_ns = reader.optional_u64()?;
        let first_fill_ns = reader.optional_u64()?;
        let shortfall = reader.optional_i128()?;
        let spread = reader.optional_i64()?;
        let adverse_selection = reader.optional_i128()?;
        tca.push(TcaView {
            client_order_id,
            filled_quantity,
            notional,
            average_price_numerator,
            average_price_denominator,
            arrival_price,
            decision_ns,
            send_ns,
            ack_ns,
            first_fill_ns,
            shortfall,
            spread,
            adverse_selection,
        });
    }

    let proposal_count = reader.count(4_096)?;
    let mut proposals = Vec::with_capacity(proposal_count);
    for _ in 0..proposal_count {
        let id = reader.u128()?;
        let instrument = reader.u128()?;
        let strategy = reader.string()?;
        let action = match reader.u8()? {
            0 => "NO ACTION".into(),
            1 => format!("TARGET {}", reader.i64()?),
            2 => format!("WEIGHT {:.2}%", reader.f64()? * 100.0),
            3 => format!("INCREASE {}", reader.i64()?),
            4 => format!("DECREASE {}", reader.i64()?),
            5 => "CLOSE".into(),
            _ => return Err("invalid proposal action".into()),
        };
        let confidence = reader.f64()?;
        let _generated_ns = reader.u64()?;
        let ttl_ns = reader.u64()?;
        let state = code(
            &[
                "",
                "ACCEPTED",
                "PENDING",
                "REJECTED",
                "SUPERSEDED",
                "EXPIRED",
            ],
            reader.u8()?,
        )?;
        proposals.push(ProposalView {
            id,
            instrument,
            strategy,
            action,
            confidence,
            ttl_ns,
            state,
        });
    }

    let market_count = reader.count(4_096)?;
    let mut markets = Vec::with_capacity(market_count);
    for _ in 0..market_count {
        let instrument = reader.u128()?;
        let (bid, ask) = match reader.u8()? {
            0 => (None, None),
            1 => {
                let _ = (reader.u64()?, reader.u64()?);
                let bid = reader.i64()?;
                let ask = reader.i64()?;
                let _ = (reader.i64()?, reader.i64()?);
                (Some(bid), Some(ask))
            }
            _ => return Err("invalid quote marker".into()),
        };
        let last = match reader.u8()? {
            0 => None,
            1 => {
                let _ = reader.u64()?;
                Some(reader.i64()?)
            }
            _ => return Err("invalid trade marker".into()),
        };
        let quote_quality = quality(reader.u8()?)?;
        let trade_quality = quality(reader.u8()?)?;
        let _book_quality = reader.u8()?;
        let book_top = match reader.u8()? {
            0 => None,
            1 => Some((reader.i64()?, reader.i64()?, reader.i64()?, reader.i64()?)),
            _ => return Err("invalid book marker".into()),
        };
        let trade_count = usize::from(reader.u16()?);
        if trade_count > 512 {
            return Err("too many trade prints".into());
        }
        let mut trades = Vec::with_capacity(trade_count);
        for _ in 0..trade_count {
            trades.push(TradeView {
                sequence: reader.u64()?,
                exchange_time_ns: reader.i64()?,
                received_mono_ns: reader.u64()?,
                price: reader.i64()?,
                quantity: reader.i64()?,
            });
        }
        let bar_count = reader.count(4_096)?;
        let mut bars = Vec::with_capacity(bar_count);
        for _ in 0..bar_count {
            let start_time_ns = reader.i64()?;
            let interval_ns = reader.u64()?;
            if interval_ns == 0 {
                return Err("chart bar interval must be positive".into());
            }
            bars.push(BarView {
                start_time_ns,
                interval_ns,
                open: reader.i64()?,
                high: reader.i64()?,
                low: reader.i64()?,
                close: reader.i64()?,
                volume: reader.i64()?,
            });
        }
        markets.push(MarketView {
            instrument,
            bid,
            ask,
            last,
            quote_quality,
            trade_quality,
            book_top,
            trades,
            bars,
        });
    }
    reader.finish()?;
    Ok(RuntimeView {
        account_id,
        cursor,
        risk,
        mode,
        plan,
        llm_provider_id,
        llm_model,
        cash_ticks,
        realized_pnl_ticks,
        fees_ticks,
        gross_notional_ticks,
        max_gross_notional_ticks,
        gross_utilization_bps,
        largest_position_notional_ticks,
        drawdown_bps,
        positions,
        orders,
        fills,
        tca,
        proposals,
        markets,
    })
}

pub fn decode_strategies(bytes: &[u8]) -> Result<Vec<StrategyView>, String> {
    let mut r = Reader::after_magic(bytes, b"IT_CMD_STRATEGY_REGISTRY_LIST_RESPONSE_V1\0")?;
    let mut values = Vec::new();
    for _ in 0..r.count(4_096)? {
        let id = r.string()?;
        let mode = r.string()?;
        let state = r.string()?;
        let lifecycle = r.string()?;
        let _evidence = r.string()?;
        let priority = r.string()?;
        let _ = (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
        let mut metrics = Vec::new();
        for _ in 0..r.count(4_096)? {
            metrics.push(r.string()?);
        }
        for _ in 0..r.count(4_096)? {
            let _ = r.string()?;
        }
        values.push(StrategyView {
            id,
            mode,
            state,
            lifecycle,
            priority,
            metrics,
        });
    }
    r.finish()?;
    Ok(values)
}

pub fn decode_metrics(bytes: &[u8]) -> Result<Vec<MetricView>, String> {
    let mut r = Reader::after_magic(bytes, b"IT_CMD_METRIC_REGISTRY_LIST_RESPONSE_V1\0")?;
    let mut values = Vec::new();
    for _ in 0..r.count(4_096)? {
        let id = r.string()?;
        let state = r.string()?;
        let lifecycle = r.string()?;
        let priority = r.string()?;
        let _ttl = r.u64()?;
        let period_ns = r.u64()?;
        let deadline_ns = r.u64()?;
        let _budget = r.u64()?;
        let _ = (r.u8()?, r.f64()?, r.u8()?, r.f64()?);
        let mut inputs = Vec::new();
        for _ in 0..r.count(4_096)? {
            inputs.push(r.string()?);
        }
        values.push(MetricView {
            id,
            state,
            lifecycle,
            priority,
            period_ns,
            deadline_ns,
            inputs,
        });
    }
    r.finish()?;
    Ok(values)
}

pub fn decode_alerts(bytes: &[u8]) -> Result<Vec<AlertView>, String> {
    let mut r = Reader::after_magic(bytes, b"IT_CMD_ALERTS_RESPONSE_V1\0")?;
    let mut values = Vec::new();
    for _ in 0..r.count(4_096)? {
        let id = r.string()?;
        let _dedupe = r.string()?;
        let source = r.string()?;
        let occurred_ms = r.i64()?;
        let severity = r.u8()?;
        let _sensitive = r.u8()?;
        let message = r.string()?;
        values.push(AlertView {
            id,
            source,
            occurred_ms,
            severity,
            message,
        });
    }
    r.finish()?;
    Ok(values)
}

pub fn decode_news(bytes: &[u8]) -> Result<(Vec<NewsView>, Option<String>), String> {
    let (mut r, v2) = if bytes.starts_with(b"IT_CMD_NEWS_PAGE_V2\0") {
        (Reader::after_magic(bytes, b"IT_CMD_NEWS_PAGE_V2\0")?, true)
    } else if bytes.starts_with(b"IT_CMD_NEWS_PAGE_RESPONSE_V2\0") {
        (
            Reader::after_magic(bytes, b"IT_CMD_NEWS_PAGE_RESPONSE_V2\0")?,
            true,
        )
    } else {
        (
            Reader::after_magic(bytes, b"IT_CMD_NEWS_PAGE_RESPONSE_V1\0")?,
            false,
        )
    };
    let mut values = Vec::new();
    for _ in 0..r.count(500)? {
        let id = r.string()?;
        let title = r.string()?;
        let source = r.string()?;
        let url = r.string()?;
        let _published = r.i64()?;
        let received_ms = r.i64()?;
        let count = usize::from(r.u16()?);
        if count > 1_024 {
            return Err("too many news symbols".into());
        }
        let mut symbols = Vec::new();
        for _ in 0..count {
            symbols.push(r.string()?);
        }
        let relevance = if v2 {
            f64::from(r.u16()?) / 10_000.0
        } else {
            0.0
        };
        values.push(NewsView {
            id,
            title,
            source,
            url,
            received_ms,
            symbols,
            relevance,
        });
    }
    let next = r.string()?;
    r.finish()?;
    Ok((values, (!next.is_empty()).then_some(next)))
}

pub fn decode_news_detail(bytes: &[u8]) -> Result<Option<NewsDetailView>, String> {
    let mut reader = Reader::after_magic(bytes, b"IT_CMD_NEWS_DETAIL_V1\0")?;
    let Some(current) = decode_optional_news_item_detail(&mut reader)? else {
        reader.finish()?;
        return Ok(None);
    };
    let version_count = usize::from(reader.u16()?);
    if version_count > 1_024 {
        return Err("too many retained news versions".into());
    }
    let mut versions = Vec::with_capacity(version_count);
    for _ in 0..version_count {
        versions.push(decode_news_item_detail(&mut reader)?);
    }
    let cluster_id = reader.string()?;
    let related_count = usize::from(reader.u16()?);
    if related_count > 1_024 {
        return Err("too many related news items".into());
    }
    let mut related_item_ids = Vec::with_capacity(related_count);
    for _ in 0..related_count {
        related_item_ids.push(reader.string()?);
    }
    reader.finish()?;
    Ok(Some(NewsDetailView {
        current,
        versions,
        cluster_id,
        related_item_ids,
    }))
}

fn decode_optional_news_item_detail(
    reader: &mut Reader<'_>,
) -> Result<Option<NewsItemDetailView>, String> {
    match reader.u8()? {
        0 => Ok(None),
        1 => decode_news_item_detail(reader).map(Some),
        _ => Err("invalid news detail marker".into()),
    }
}

fn decode_news_item_detail(reader: &mut Reader<'_>) -> Result<NewsItemDetailView, String> {
    let id = reader.string()?;
    let provider = reader.string()?;
    let url = reader.string()?;
    let source = reader.string()?;
    let title = reader.string()?;
    let summary = match reader.u8()? {
        0 => {
            let empty = reader.string()?;
            if !empty.is_empty() {
                return Err("absent news summary contains data".into());
            }
            None
        }
        1 => Some(reader.string()?),
        _ => return Err("invalid news summary marker".into()),
    };
    let published_marker = reader.u8()?;
    let published_value = reader.i64()?;
    let published_ms = optional(published_marker, published_value)?;
    let received_ms = reader.i64()?;
    let symbol_count = usize::from(reader.u16()?);
    if symbol_count > 1_024 {
        return Err("too many news detail symbols".into());
    }
    let mut symbols = Vec::with_capacity(symbol_count);
    for _ in 0..symbol_count {
        symbols.push(reader.string()?);
    }
    let content_hash = reader.string()?;
    Ok(NewsItemDetailView {
        id,
        provider,
        url,
        source,
        title,
        summary,
        published_ms,
        received_ms,
        symbols,
        content_hash,
    })
}

pub fn decode_preview(bytes: &[u8]) -> Result<PreviewView, String> {
    let mut r = Reader::after_magic(bytes, b"IT_CMD_PREVIEW_V1\0")?;
    let id = r.string()?;
    let expected_version = r.u64()?;
    let _expires = r.u64()?;
    let _target = r.i64()?;
    let _proposal = r.u128()?;
    let intent_len = r.u32()? as usize;
    let _intent = r.take(intent_len)?;
    let estimated_notional = r.i128()?;
    let estimated_cost_bps = r.i64()?;
    let count = usize::from(r.u16()?);
    if count > 128 {
        return Err("too many preview warnings".into());
    }
    let mut warnings = Vec::new();
    for _ in 0..count {
        warnings.push(r.string()?);
    }
    r.finish()?;
    Ok(PreviewView {
        id,
        expected_version,
        estimated_notional,
        estimated_cost_bps,
        warnings,
    })
}

pub fn decode_trace(bytes: &[u8]) -> Result<Vec<TraceView>, String> {
    let mut reader = Reader::after_magic(bytes, b"IT_CMD_TRACE_EVENTS_V1\0")?;
    let count = reader.count(4_096)?;
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        let sequence = reader.u64()?;
        let kind = reader.string()?;
        let payload = reader.byte_vec(1_048_576)?;
        let mut payload_preview = String::with_capacity(payload.len().min(24) * 2);
        for value in payload.iter().take(24) {
            let _ = write!(payload_preview, "{value:02x}");
        }
        events.push(TraceView {
            sequence,
            kind,
            payload_bytes: payload.len(),
            payload_preview,
        });
    }
    reader.finish()?;
    Ok(events)
}

pub fn decode_context_hits(bytes: &[u8]) -> Result<Vec<ContextHitView>, String> {
    let mut reader = Reader::after_magic(bytes, b"IT_CMD_CONTEXT_SEARCH_RESPONSE_V1\0")?;
    let count = usize::from(reader.u16()?);
    if count > 256 {
        return Err("too many context search hits".into());
    }
    let mut hits = Vec::with_capacity(count);
    for _ in 0..count {
        let node_id = reader.string()?;
        let score = reader.f64()?;
        let exact_score = reader.f64()?;
        let lexical_score = reader.f64()?;
        let vector_score = reader.f64()?;
        let path_count = usize::from(reader.u16()?);
        if path_count > 32 {
            return Err("context evidence path exceeds bound".into());
        }
        let mut evidence_path = Vec::with_capacity(path_count);
        for _ in 0..path_count {
            evidence_path.push(reader.string()?);
        }
        hits.push(ContextHitView {
            node_id,
            score,
            exact_score,
            lexical_score,
            vector_score,
            evidence_path,
        });
    }
    reader.finish()?;
    Ok(hits)
}

pub fn decode_analyst(bytes: &[u8]) -> Result<AnalystView, String> {
    if bytes.starts_with(b"IT_CMD_LLM_STREAM_RESPONSE_V1\0") {
        return decode_analyst_stream(bytes);
    }
    let mut reader = Reader::after_magic(bytes, b"IT_CMD_LLM_COMPLETE_RESPONSE_V1\0")?;
    let trace_id = reader.string()?;
    let finish_reason = reader.string()?;
    let content = reader.string()?;
    if trace_id.trim().is_empty() || content.trim().is_empty() {
        return Err("analyst response is empty or missing trace identity".into());
    }
    reader.finish()?;
    Ok(AnalystView {
        trace_id,
        finish_reason,
        content,
    })
}

fn decode_analyst_stream(bytes: &[u8]) -> Result<AnalystView, String> {
    const MAX_CONTENT_BYTES: usize = 1_048_576;
    let mut reader = Reader::after_magic(bytes, b"IT_CMD_LLM_STREAM_RESPONSE_V1\0")?;
    let trace_id = reader.string()?;
    if trace_id.trim().is_empty() {
        return Err("analyst stream is missing trace identity".into());
    }
    let count = reader.count(4_096)?;
    let mut content = String::new();
    let mut finish_reason = None;
    for _ in 0..count {
        match reader.u8()? {
            1 if finish_reason.is_none() => {
                let delta = reader.string()?;
                if content.len().saturating_add(delta.len()) > MAX_CONTENT_BYTES {
                    return Err("analyst stream content exceeds 1 MiB bound".into());
                }
                content.push_str(&delta);
            }
            2 if finish_reason.is_none() => finish_reason = Some(reader.string()?),
            1 | 2 => return Err("analyst stream contains data after completion".into()),
            _ => return Err("invalid analyst stream item".into()),
        }
    }
    reader.finish()?;
    let finish_reason = finish_reason.ok_or("analyst stream has no completion marker")?;
    if content.trim().is_empty() {
        return Err("analyst stream response is empty".into());
    }
    Ok(AnalystView {
        trace_id,
        finish_reason,
        content,
    })
}

pub fn decode_backtests(bytes: &[u8]) -> Result<Vec<BacktestView>, String> {
    let mut reader = Reader::after_magic(bytes, b"IT_CMD_BACKTEST_LIST_RESPONSE_V1\0")?;
    let count = reader.count(4_096)?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let run_id = reader.string()?;
        let strategy_id = reader.string()?;
        let dataset_hash = reader.string()?;
        let config_hash = reader.string()?;
        let event_count = reader.u64()?;
        let max_drawdown = reader.i128()?;
        let fees = reader.i128()?;
        let final_equity = match reader.u8()? {
            0 => None,
            1 => Some(reader.i128()?),
            _ => return Err("invalid backtest result marker".into()),
        };
        values.push(BacktestView {
            run_id,
            strategy_id,
            dataset_hash,
            config_hash,
            event_count,
            max_drawdown,
            fees,
            final_equity,
        });
    }
    reader.finish()?;
    Ok(values)
}

pub fn decode_models(bytes: &[u8]) -> Result<Vec<ModelView>, String> {
    let mut reader = Reader::after_magic(bytes, b"IT_CMD_MODEL_LIST_RESPONSE_V1\0")?;
    let count = reader.count(4_096)?;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let model_id = reader.string()?;
        let version = reader.string()?;
        let artifact_hash = reader.string()?;
        let _input_schema_hash = reader.string()?;
        let _output_schema_hash = reader.string()?;
        let input_width = reader.u64()?;
        let status = code(
            &[
                "",
                "RESEARCH",
                "VALIDATED",
                "SHADOW",
                "CANARY",
                "PRODUCTION",
                "RETIRED",
            ],
            reader.u8()?,
        )?;
        records.push(ModelView {
            model_id,
            version,
            artifact_hash,
            input_width,
            status,
            active: false,
        });
    }
    let active_count = reader.count(4_096)?;
    let mut active = std::collections::BTreeSet::new();
    for _ in 0..active_count {
        active.insert((reader.string()?, reader.string()?));
    }
    for record in &mut records {
        record.active = active.contains(&(record.model_id.clone(), record.version.clone()));
    }
    reader.finish()?;
    Ok(records)
}

pub fn decode_resolutions(bytes: &[u8]) -> Result<Vec<ResolutionView>, String> {
    let mut reader = Reader::after_magic(bytes, b"IT_CMD_STRATEGY_RESOLUTION_LIST_RESPONSE_V1\0")?;
    let count = reader.count(4_096)?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(ResolutionView {
            policy: reader.string()?,
            now_ns: reader.u64()?,
            accepted: reader.u32()?,
            conflicts: reader.u32()?,
            expired: reader.u32()?,
            attributions: reader.u32()?,
        });
    }
    reader.finish()?;
    Ok(values)
}

pub fn decode_strategy_execution(bytes: &[u8]) -> Result<Vec<StrategyExecutionView>, String> {
    let mut reader = Reader::after_magic(bytes, b"IT_CMD_STRATEGY_EXECUTION_LIST_RESPONSE_V1\0")?;
    let count = reader.count(4_096)?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(StrategyExecutionView {
            strategy_id: reader.string()?,
            fills: reader.u64()?,
            quantity: reader.i128()?,
            notional: reader.i128()?,
        });
    }
    reader.finish()?;
    Ok(values)
}

pub fn decode_experiments(bytes: &[u8]) -> Result<Vec<ExperimentView>, String> {
    let mut reader = Reader::after_magic(bytes, b"IT_CMD_EXPERIMENT_LIST_RESPONSE_V2\0")?;
    let count = reader.count(4_096)?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let run_id = reader.string()?;
        let code_hash = reader.string()?;
        let config_hash = reader.string()?;
        let dataset_hash = reader.string()?;
        let status = code(
            &["", "CREATED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"],
            reader.u8()?,
        )?;
        let metric_count = reader.count(4_096)?;
        let mut metrics = Vec::with_capacity(metric_count);
        for _ in 0..metric_count {
            metrics.push((reader.string()?, reader.f64()?));
        }
        let artifact_count = reader.count(4_096)?;
        for _ in 0..artifact_count {
            let _ = (reader.string()?, reader.string()?, reader.string()?);
        }
        let labels = [
            "strategy",
            "strategy-version",
            "news-dataset",
            "news-clustering",
            "graph",
            "llm-provider",
            "llm-model",
            "prompt",
            "tool-schema",
            "autonomy-config",
        ];
        let mut provenance = Vec::new();
        for label in labels {
            if let Some(value) = reader.optional_string()? {
                provenance.push(format!("{label}={value}"));
            }
        }
        let cache_count = reader.count(256)?;
        for _ in 0..cache_count {
            provenance.push(format!("cache={}", reader.string()?));
        }
        values.push(ExperimentView {
            run_id,
            code_hash,
            config_hash,
            dataset_hash,
            status,
            metrics,
            artifact_count,
            provenance,
        });
    }
    reader.finish()?;
    Ok(values)
}

pub fn decode_string(bytes: &[u8]) -> Result<String, String> {
    let mut r = Reader { bytes, offset: 0 };
    let value = r.string()?;
    r.finish()?;
    Ok(value)
}

pub fn decode_resolved_instrument(bytes: &[u8]) -> Result<ResolvedInstrumentView, String> {
    let mut reader = Reader::after_magic(bytes, RESOLVED_INSTRUMENT_MAGIC)?;
    let instrument = reader.u128()?;
    if instrument == 0 {
        return Err("resolved instrument identity must be positive".into());
    }
    let asset_class = match reader.u8()? {
        1 => "EQUITY",
        2 => "ETF",
        3 => "OPTION",
        4 => "FUTURE",
        5 => "FX",
        6 => "CRYPTO",
        _ => return Err("resolved instrument has an unknown asset class".into()),
    }
    .to_owned();
    let symbol = reader.string()?;
    let venue = reader.string()?;
    if symbol.is_empty() || symbol.len() > 64 || venue.is_empty() || venue.len() > 64 {
        return Err("resolved instrument identity exceeds display bounds".into());
    }
    reader.finish()?;
    Ok(ResolvedInstrumentView {
        instrument,
        asset_class,
        symbol,
        venue,
    })
}

pub fn decode_proposal_submit(bytes: &[u8]) -> Result<String, String> {
    let mut reader = Reader::after_magic(bytes, PROPOSAL_SUBMIT_MAGIC)?;
    let client_order_id = reader.string()?;
    if client_order_id.is_empty() || client_order_id.len() > 256 {
        return Err("proposal submission returned an invalid order identity".into());
    }
    reader.finish()?;
    Ok(client_order_id)
}

fn decode_order_intent(bytes: &[u8]) -> Result<(String, u128, String, i64), String> {
    let mut r = Reader::after_magic(bytes, b"IT_ORDER_INTENT_V1\0")?;
    let _account = r.u128()?;
    let instrument = r.u128()?;
    let side = code(&["", "BUY", "SELL"], r.u8()?)?;
    let quantity = r.i64()?;
    let _ = (r.u8()?, r.i64()?, r.u8()?, r.u128()?, r.string()?);
    let client_id = r.string()?;
    r.finish()?;
    Ok((client_id, instrument, side, quantity))
}

fn quality(value: u8) -> Result<String, String> {
    code(&["UNKNOWN", "GOOD", "DEGRADED", "STALE"], value)
}
fn code(values: &[&str], value: u8) -> Result<String, String> {
    values
        .get(usize::from(value))
        .filter(|v| !v.is_empty())
        .map(|v| (*v).into())
        .ok_or_else(|| format!("invalid wire enum {value}"))
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    fn after_magic(bytes: &'a [u8], magic: &[u8]) -> Result<Self, String> {
        if !bytes.starts_with(magic) {
            return Err("unexpected engine response".into());
        }
        Ok(Self {
            bytes,
            offset: magic.len(),
        })
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or("wire offset overflow")?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or("truncated engine response")?;
        self.offset = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(*self.take(1)?.first().ok_or("missing byte")?)
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().map_err(|_| "invalid u16")?,
        ))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().map_err(|_| "invalid u32")?,
        ))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| "invalid u64")?,
        ))
    }
    fn u128(&mut self) -> Result<u128, String> {
        Ok(u128::from_le_bytes(
            self.take(16)?.try_into().map_err(|_| "invalid u128")?,
        ))
    }
    fn i64(&mut self) -> Result<i64, String> {
        Ok(i64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| "invalid i64")?,
        ))
    }
    fn i128(&mut self) -> Result<i128, String> {
        Ok(i128::from_le_bytes(
            self.take(16)?.try_into().map_err(|_| "invalid i128")?,
        ))
    }
    fn optional_u64(&mut self) -> Result<Option<u64>, String> {
        let marker = self.u8()?;
        let value = self.u64()?;
        optional(marker, value)
    }
    fn optional_i64(&mut self) -> Result<Option<i64>, String> {
        let marker = self.u8()?;
        let value = self.i64()?;
        optional(marker, value)
    }
    fn optional_i128(&mut self) -> Result<Option<i128>, String> {
        let marker = self.u8()?;
        let value = self.i128()?;
        optional(marker, value)
    }
    fn optional_string(&mut self) -> Result<Option<String>, String> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.string().map(Some),
            _ => Err("invalid optional string marker".into()),
        }
    }
    fn f64(&mut self) -> Result<f64, String> {
        let value = f64::from_le_bytes(self.take(8)?.try_into().map_err(|_| "invalid f64")?);
        if !value.is_finite() {
            return Err("non-finite wire number".into());
        }
        Ok(value)
    }
    fn string(&mut self) -> Result<String, String> {
        let length = usize::from(self.u16()?);
        if length > 1_048_576 {
            return Err("wire string exceeds bound".into());
        }
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| "wire string is not UTF-8".into())
    }
    fn byte_vec(&mut self, max: usize) -> Result<&'a [u8], String> {
        let length = self.u32()? as usize;
        if length == 0 || length > max {
            return Err("wire byte field exceeds bound".into());
        }
        self.take(length)
    }
    fn count(&mut self, max: usize) -> Result<usize, String> {
        let value = self.u32()? as usize;
        if value > max {
            return Err("wire collection exceeds bound".into());
        }
        Ok(value)
    }
    fn finish(&self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing engine response bytes".into())
        }
    }
}

fn optional<T>(marker: u8, value: T) -> Result<Option<T>, String> {
    match marker {
        0 => Ok(None),
        1 => Ok(Some(value)),
        _ => Err("invalid optional field marker".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_analyst, decode_news, decode_news_detail, decode_proposal_submit,
        decode_resolved_instrument, decode_snapshot,
    };

    #[test]
    fn decodes_resolved_instrument_and_rejects_trailing_data() {
        let mut bytes = b"IT_RESOLVED_INSTRUMENT_V1\0".to_vec();
        bytes.extend_from_slice(&42_u128.to_le_bytes());
        bytes.push(1);
        push_string(&mut bytes, "AAPL");
        push_string(&mut bytes, "NASDAQ");
        assert_eq!(
            decode_resolved_instrument(&bytes).map(|value| (
                value.instrument,
                value.asset_class,
                value.symbol,
                value.venue
            )),
            Ok((42, "EQUITY".into(), "AAPL".into(), "NASDAQ".into()))
        );
        bytes.push(0);
        assert!(decode_resolved_instrument(&bytes).is_err());
    }

    #[test]
    fn decodes_proposal_submission_order_identity() {
        let mut bytes = b"IT_CMD_PROPOSAL_SUBMIT_RESPONSE_V1\0".to_vec();
        push_string(&mut bytes, "order-proposal-42");
        assert_eq!(
            decode_proposal_submit(&bytes),
            Ok("order-proposal-42".into())
        );
        bytes.push(0);
        assert!(decode_proposal_submit(&bytes).is_err());
    }

    #[test]
    fn decodes_autonomy_plan_actions_and_configured_llm_identity() {
        let mut bytes = b"IT_RUNTIME_SNAPSHOT_V14\0".to_vec();
        bytes.extend_from_slice(&7_u128.to_le_bytes());
        bytes.extend_from_slice(&11_u64.to_le_bytes());
        bytes.extend_from_slice(&[1, 3, 1]);
        push_string(&mut bytes, "plan-1");
        bytes.push(2);
        bytes.extend_from_slice(&100_u64.to_le_bytes());
        bytes.extend_from_slice(&1_100_u64.to_le_bytes());
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.push(2);
        push_string(&mut bytes, "42");
        bytes.push(1);
        bytes.extend_from_slice(&0.65_f64.to_le_bytes());
        bytes.extend_from_slice(&2_u16.to_le_bytes());
        push_string(&mut bytes, "NEWS_CONFIRMATION");
        push_string(&mut bytes, "MULTI_STRATEGY_AGREEMENT");
        bytes.push(1);
        push_string(&mut bytes, "openai-compatible");
        bytes.push(1);
        push_string(&mut bytes, "configured-model");
        bytes.extend_from_slice(&0_i64.to_le_bytes());
        bytes.extend_from_slice(&0_i64.to_le_bytes());
        bytes.extend_from_slice(&0_i64.to_le_bytes());
        bytes.extend_from_slice(&0_i128.to_le_bytes());
        bytes.extend_from_slice(&1_i128.to_le_bytes());
        bytes.extend_from_slice(&0_i64.to_le_bytes());
        bytes.extend_from_slice(&0_i128.to_le_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&0_i64.to_le_bytes());
        for _ in 0..5 {
            bytes.extend_from_slice(&0_u32.to_le_bytes());
        }
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&42_u128.to_le_bytes());
        bytes.extend_from_slice(&[0, 0, 1, 1, 0, 0]);
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&1_700_000_000_000_000_000_i64.to_le_bytes());
        let interval_offset = bytes.len();
        bytes.extend_from_slice(&60_000_000_000_u64.to_le_bytes());
        for value in [100_i64, 110, 90, 105, 1_000] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }

        let decoded = decode_snapshot(&bytes);
        assert!(decoded.is_ok());
        let Ok(view) = decoded else {
            return;
        };
        assert_eq!(view.llm_provider_id.as_deref(), Some("openai-compatible"));
        assert_eq!(view.llm_model.as_deref(), Some("configured-model"));
        assert!(view.plan.is_some());
        let Some(plan) = view.plan else {
            return;
        };
        assert_eq!(plan.id, "plan-1");
        assert_eq!(plan.generated_at_ns, 100);
        assert_eq!(plan.expires_at_ns, 1_100);
        assert_eq!(plan.actions[0].action, "EXECUTE SCALED");
        assert_eq!(plan.actions[0].proposal_id.as_deref(), Some("42"));
        assert_eq!(plan.actions[0].scale, Some(0.65));
        assert_eq!(plan.actions[0].reason_codes.len(), 2);
        assert_eq!(
            view.markets[0].bars[0].start_time_ns,
            1_700_000_000_000_000_000
        );
        assert_eq!(view.markets[0].bars[0].interval_ns, 60_000_000_000);

        let mut zero_interval = bytes.clone();
        zero_interval[interval_offset..interval_offset + 8].fill(0);
        assert!(decode_snapshot(&zero_interval).is_err());

        bytes.push(0);
        assert!(decode_snapshot(&bytes).is_err());
    }

    #[test]
    fn decodes_authoritative_news_page_magic_and_rejects_trailing_data() {
        let mut bytes = b"IT_CMD_NEWS_PAGE_V2\0".to_vec();
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        for value in [
            "article-1",
            "Headline",
            "Publisher",
            "https://example.com/article-1",
        ] {
            push_string(&mut bytes, value);
        }
        bytes.extend_from_slice(&10_i64.to_le_bytes());
        bytes.extend_from_slice(&20_i64.to_le_bytes());
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        push_string(&mut bytes, "AAPL");
        bytes.extend_from_slice(&8_750_u16.to_le_bytes());
        push_string(&mut bytes, "article-1");

        let (items, next) = decode_news(&bytes).unwrap_or_default();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, "article-1");
        assert!((items[0].relevance - 0.875).abs() < f64::EPSILON);
        assert_eq!(next.as_deref(), Some("article-1"));

        bytes.push(0);
        assert!(decode_news(&bytes).is_err());
    }

    #[test]
    fn decodes_news_detail_with_versions_cluster_and_related_items() {
        let mut bytes = b"IT_CMD_NEWS_DETAIL_V1\0".to_vec();
        bytes.push(1);
        push_item(&mut bytes, "current", true);
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        push_item(&mut bytes, "older", false);
        push_string(&mut bytes, "cluster-7");
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        push_string(&mut bytes, "related-2");

        let detail = decode_news_detail(&bytes).unwrap_or(None);
        assert!(detail.is_some());
        let Some(detail) = detail else {
            return;
        };
        assert_eq!(detail.current.id, "current");
        assert_eq!(detail.current.summary.as_deref(), Some("Summary"));
        assert_eq!(detail.versions.len(), 1);
        assert_eq!(detail.cluster_id, "cluster-7");
        assert_eq!(detail.related_item_ids, ["related-2"]);
    }

    #[test]
    fn decodes_absent_news_detail() {
        let mut bytes = b"IT_CMD_NEWS_DETAIL_V1\0".to_vec();
        bytes.push(0);
        assert!(matches!(decode_news_detail(&bytes), Ok(None)));
    }

    #[test]
    fn decodes_bounded_analyst_stream_and_requires_completion() {
        let mut bytes = b"IT_CMD_LLM_STREAM_RESPONSE_V1\0".to_vec();
        push_string(&mut bytes, "trace-1");
        bytes.extend_from_slice(&3_u32.to_le_bytes());
        bytes.push(1);
        push_string(&mut bytes, "market ");
        bytes.push(1);
        push_string(&mut bytes, "context");
        bytes.push(2);
        push_string(&mut bytes, "stop");
        let Ok(view) = decode_analyst(&bytes) else {
            return;
        };
        assert_eq!(view.trace_id, "trace-1");
        assert_eq!(view.content, "market context");
        assert_eq!(view.finish_reason, "stop");

        let mut incomplete = b"IT_CMD_LLM_STREAM_RESPONSE_V1\0".to_vec();
        push_string(&mut incomplete, "trace-2");
        incomplete.extend_from_slice(&1_u32.to_le_bytes());
        incomplete.push(1);
        push_string(&mut incomplete, "partial");
        assert!(decode_analyst(&incomplete).is_err());
    }

    fn push_item(bytes: &mut Vec<u8>, id: &str, summary: bool) {
        for value in [
            id,
            "provider",
            "https://example.com/article",
            "Publisher",
            "Headline",
        ] {
            push_string(bytes, value);
        }
        bytes.push(u8::from(summary));
        push_string(bytes, if summary { "Summary" } else { "" });
        bytes.push(1);
        bytes.extend_from_slice(&10_i64.to_le_bytes());
        bytes.extend_from_slice(&20_i64.to_le_bytes());
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        push_string(bytes, "AAPL");
        push_string(bytes, "hash");
    }

    fn push_string(bytes: &mut Vec<u8>, value: &str) {
        let length = u16::try_from(value.len()).unwrap_or(u16::MAX);
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
}
