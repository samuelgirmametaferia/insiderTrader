//! Native, command-driven `InsiderTrader` terminal workstation.

#![forbid(unsafe_code)]

mod app;
mod browser_chart;
mod chart;
mod client;
mod command_line;
mod model;
mod preferences;

use std::io::{self, IsTerminal};
use std::path::PathBuf;
use std::time::Duration;

use app::{App, Page, ScreenerMode};
use browser_chart::{BrowserChartSnapshot, BrowserChartWorkspace};
use chart::{
    ChartOverlays, ChartStyle, aggregate_interval, compress_for_width, cursor_index,
    simple_moving_average, window_vwap,
};
use client::EngineClient;
use command_line::Completion;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use model::BarView;
use preferences::Preferences;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table, Widget, Wrap};
use unicode_width::UnicodeWidthChar;

const ORANGE: Color = Color::Rgb(255, 140, 0);
const AMBER: Color = Color::Rgb(255, 190, 60);
const GREEN: Color = Color::Rgb(40, 210, 130);
const RED: Color = Color::Rgb(240, 70, 80);
const PANEL: Color = Color::Rgb(20, 22, 25);

fn main() {
    if let Err(error) = run() {
        eprintln!("insider-terminal: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let args = std::env::args().collect::<Vec<_>>();
    let socket = argument(&args, "--socket")
        .or_else(|| std::env::var("IT_ENGINE_SOCKET").ok())
        .map(PathBuf::from)
        .ok_or("usage: insider-terminal --socket PATH [--refresh-ms N] [--state-file PATH] [--snapshot | --command FUNCTION]")?;
    let refresh_ms = argument(&args, "--refresh-ms").map_or(Ok(1_000_u64), |value| {
        value
            .parse::<u64>()
            .map_err(|_| "--refresh-ms must be an integer")
    })?;
    if !(100..=60_000).contains(&refresh_ms) {
        return Err("--refresh-ms must be 100..60000".into());
    }
    let client = EngineClient::connect(socket)?;
    let mut app = App::new(client, Duration::from_millis(refresh_ms));
    // The renderer is useful before the headless runtime is ready (for example
    // while `insider server` is still starting). Keep the UI alive and let the
    // bounded refresh loop reconnect asynchronously.
    if let Err(error) = app.refresh_runtime() {
        app.runtime_connected = false;
        app.status = format!("WAITING FOR RUNTIME — {error}");
        app.status_is_error = false;
    } else if let Err(error) = app.load_terminal_settings() {
        app.status = format!("CONNECTED — SETTINGS UNAVAILABLE — {error}");
        app.status_is_error = false;
    }
    let state_path = argument(&args, "--state-file")
        .map(PathBuf::from)
        .or_else(preferences::default_path);
    if let Some(path) = &state_path {
        match Preferences::load(path) {
            Ok(preferences) => {
                app.selected_instrument =
                    preferences.selected_instrument.or(app.selected_instrument);
                app.selected_symbol = preferences.selected_symbol;
                app.news_scope = preferences.news_scope;
                app.chart_window = preferences.chart_window;
                app.chart_interval = preferences.chart_interval;
                app.chart_style = preferences.chart_style;
                app.chart_overlays = preferences.chart_overlays;
                app.screener_mode = ScreenerMode::parse(&preferences.screener_mode)
                    .ok_or("invalid persisted screener mode")?;
                if let Err(error) = app.restore_page(preferences.page) {
                    app.page = Page::Home;
                    app.status = format!("PREFERRED FUNCTION NOT RESTORED — {error}");
                    app.status_is_error = false;
                }
            }
            Err(error) => {
                app.status = format!("PREFERENCES NOT RESTORED — {error}");
                app.status_is_error = false;
            }
        }
    }
    if args.iter().any(|value| value == "--snapshot") {
        println!(
            "account={} cursor={} risk={} mode={} markets={} positions={} orders={} proposals={}",
            app.runtime.account_id,
            app.runtime.cursor,
            app.runtime.risk,
            app.runtime.mode,
            app.runtime.markets.len(),
            app.runtime.positions.len(),
            app.runtime.orders.len(),
            app.runtime.proposals.len()
        );
        return Ok(());
    }
    if let Some(command) = argument(&args, "--command") {
        app.execute_command(&command)?;
        if app.analyst_is_pending() {
            app.wait_for_analyst(Duration::from_secs(60))?;
        }
        print_plain(&app);
        return Ok(());
    }
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err("an interactive terminal is required (use --snapshot for automation)".into());
    }

    enable_raw_mode().map_err(|error| format!("enable raw mode: {error}"))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)
        .map_err(|error| format!("enter terminal screen: {error}"))?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal =
        Terminal::new(backend).map_err(|error| format!("terminal backend: {error}"))?;
    let result = event_loop(&mut terminal, &mut app, state_path.as_deref());
    let _ = disable_raw_mode();
    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
    let _ = terminal.show_cursor();
    result
}

fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    state_path: Option<&std::path::Path>,
) -> Result<(), String> {
    let mut browser_chart: Option<BrowserChartWorkspace> = None;
    while !app.should_quit {
        app.poll_background();
        app.refresh_if_due();
        if app.browser_chart_requested.take().is_some() {
            if browser_chart.is_none() {
                match BrowserChartWorkspace::start(BrowserChartSnapshot::from_app(app)) {
                    Ok(workspace) => browser_chart = Some(workspace),
                    Err(error) => app.fail(error),
                }
            }
            if let Some(workspace) = &browser_chart {
                match workspace.open_browser() {
                    Ok(()) => {
                        app.status = format!("LOCAL BROWSER CHART — {}", workspace.url());
                        app.status_is_error = false;
                    }
                    Err(error) => {
                        app.status = format!(
                            "BROWSER LAUNCH UNAVAILABLE — open {} ({error})",
                            workspace.url()
                        );
                        app.status_is_error = false;
                    }
                }
            }
        }
        if let Some(workspace) = &browser_chart {
            for _ in 0..8 {
                let Some(command) = workspace.try_command() else {
                    break;
                };
                if let Err(error) = app.execute_command(&command) {
                    app.fail(format!("BROWSER COMMAND — {error}"));
                }
            }
            workspace.publish(BrowserChartSnapshot::from_app(app));
        }
        terminal
            .draw(|frame| draw(frame, app))
            .map_err(|error| format!("draw terminal: {error}"))?;
        if event::poll(Duration::from_millis(50)).map_err(|error| format!("poll input: {error}"))?
            && let Event::Key(key) =
                event::read().map_err(|error| format!("read input: {error}"))?
            && matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat)
        {
            handle_key(app, key);
            let persist_presentation = matches!(
                key.code,
                KeyCode::Enter
                    | KeyCode::F(_)
                    | KeyCode::Up
                    | KeyCode::Down
                    | KeyCode::PageUp
                    | KeyCode::PageDown
                    | KeyCode::Left
                    | KeyCode::Right
                    | KeyCode::Char('+' | '-' | '0')
            );
            if persist_presentation
                && let Some(path) = state_path
                && let Err(error) = (Preferences {
                    page: app.page,
                    selected_instrument: app.selected_instrument,
                    selected_symbol: app.selected_symbol.clone(),
                    news_scope: app.news_scope.clone(),
                    chart_window: app.chart_window,
                    chart_interval: app.chart_interval,
                    chart_style: app.chart_style,
                    chart_overlays: app.chart_overlays,
                    screener_mode: app.screener_mode.name().into(),
                })
                .save(path)
            {
                app.status = format!("PREFERENCES NOT SAVED — {error}");
                app.status_is_error = false;
            }
        }
    }
    Ok(())
}

fn handle_key(app: &mut App, key: KeyEvent) {
    if app.show_connection_help {
        app.show_connection_help = false;
        app.status = "UNIX IPC CONNECTED — type HELP for the trading command guide".into();
        app.status_is_error = false;
        return;
    }
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.should_quit = true;
        }
        KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if !app.command_line.previous_history() {
                app.status = "COMMAND HISTORY IS EMPTY".into();
                app.status_is_error = false;
            }
        }
        KeyCode::Char('n') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let _ = app.command_line.next_history();
        }
        KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.command_line.move_home();
        }
        KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.command_line.move_end();
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.command_line.clear();
        }
        KeyCode::Left
            if key.modifiers.contains(KeyModifiers::SHIFT)
                && app.command_line.is_empty()
                && app.page == Page::Chart =>
        {
            app.move_chart_crosshair(1);
        }
        KeyCode::Right
            if key.modifiers.contains(KeyModifiers::SHIFT)
                && app.command_line.is_empty()
                && app.page == Page::Chart =>
        {
            app.move_chart_crosshair(-1);
        }
        KeyCode::Char('+') if app.command_line.is_empty() && app.page == Page::Chart => {
            app.zoom_chart(true);
        }
        KeyCode::Char('-') if app.command_line.is_empty() && app.page == Page::Chart => {
            app.zoom_chart(false);
        }
        KeyCode::Char('0') if app.command_line.is_empty() && app.page == Page::Chart => {
            app.reset_chart();
        }
        KeyCode::Char(character)
            if !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
        {
            if let Err(error) = app.command_line.insert(character) {
                app.fail(error);
            }
        }
        KeyCode::Backspace => app.command_line.backspace(),
        KeyCode::Delete => app.command_line.delete(),
        KeyCode::Enter => {
            if app.command_line.is_empty() {
                if let Err(error) = app.activate_selection() {
                    app.fail(error);
                }
            } else {
                app.run_command();
            }
        }
        KeyCode::Esc => {
            if !app.dismiss_overlay() {
                app.command_line.clear();
            }
        }
        KeyCode::Up => app.scroll_by(-1),
        KeyCode::Down => app.scroll_by(1),
        KeyCode::PageUp => app.scroll_by(-10),
        KeyCode::PageDown => app.scroll_by(10),
        KeyCode::Left => {
            if app.command_line.is_empty() && app.page == Page::Chart {
                app.pan_chart_intervals(10);
            } else if !app.command_line.is_empty() {
                let _ = app.command_line.move_left();
            }
        }
        KeyCode::Right => {
            if app.command_line.is_empty() && app.page == Page::Chart {
                app.pan_chart_intervals(-10);
            } else if !app.command_line.is_empty() {
                let _ = app.command_line.move_right();
            }
        }
        KeyCode::Home => app.command_line.move_home(),
        KeyCode::End => app.command_line.move_end(),
        KeyCode::Tab => complete_command(app),
        KeyCode::F(number) => run_function_key(app, number),
        _ => {}
    }
}

fn complete_command(app: &mut App) {
    match app.command_line.complete() {
        Completion::Applied(value) => {
            app.status = format!("{value} — COMPLETE");
            app.status_is_error = false;
        }
        Completion::Ambiguous(values) => {
            app.status = format!("MATCHES — {}", values.join("  "));
            app.status_is_error = false;
        }
        Completion::None => {
            app.status = "NO FUNCTION OR ARGUMENT MATCH".into();
            app.status_is_error = true;
        }
    }
}

fn run_function_key(app: &mut App, number: u8) {
    match number {
        1 => app.page = Page::Help,
        2 => app.page = Page::Market,
        3 => app.page = Page::Portfolio,
        4 => app.page = Page::Orders,
        5 => app.run_shortcut("STRAT"),
        6 => app.run_shortcut("NEWS"),
        7 => app.page = Page::Risk,
        8 => app.run_shortcut("HEALTH"),
        9 => app.page = Page::Tca,
        10 => app.run_shortcut("REFRESH"),
        _ => {}
    }
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &App) {
    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(8),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .split(frame.area());
    draw_header(frame, app, areas[0]);
    draw_page(frame, app, areas[1]);
    draw_command(frame, app, areas[2]);
    draw_functions(frame, areas[3]);
    if app.show_connection_help {
        draw_connection_help(frame);
    }
}

fn draw_connection_help(frame: &mut ratatui::Frame<'_>) {
    let area = centered_rect(70, 45, frame.area());
    let text = Paragraph::new(vec![
        Line::styled(
            "UNIX CONNECTION READY",
            Style::default().fg(AMBER).add_modifier(Modifier::BOLD),
        ),
        Line::from(""),
        Line::from("This terminal is attached to the authenticated local runtime."),
        Line::from("It is a presentation client: closing it will not stop server-side trading."),
        Line::from(""),
        Line::styled("TRADING SAFETY", Style::default().fg(AMBER)),
        Line::from(
            "  Preview orders before CONFIRM; risk and reconciliation remain authoritative.",
        ),
        Line::from(
            "  MODE MANUAL requires confirmation; HYBRID and AUTO follow configured policy.",
        ),
        Line::from("  Type HELP for navigation. Press any key to dismiss this guide."),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title("LOCAL IPC HELP"),
    )
    .wrap(Wrap { trim: false });
    frame.render_widget(ratatui::widgets::Clear, area);
    frame.render_widget(text, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}

fn draw_header(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let freshness = app.last_refresh.elapsed().as_secs_f32();
    let connection = if app.runtime_connected {
        "ONLINE"
    } else {
        "DISCONNECTED"
    };
    let color = if app.runtime_connected { GREEN } else { RED };
    let security = if app.selected_symbol.is_empty() {
        app.selected_instrument
            .map_or_else(|| "—".into(), |value| format!("#{value}"))
    } else {
        app.selected_symbol.clone()
    };
    let accent = theme_accent(&app.theme);
    let line = Line::from(vec![
        Span::styled(
            " INSIDERTRADER ",
            Style::default()
                .bg(accent)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("  {:<22}", app.page.title()),
            Style::default().fg(AMBER).add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "SEC {security:<10}  ACCT {}  CURSOR {}  ",
            app.runtime.account_id, app.runtime.cursor,
        )),
        Span::styled(
            format!("{connection} "),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{freshness:.1}s"),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(Color::Rgb(10, 11, 13))),
        area,
    );
}

fn draw_page(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    if !app.runtime_connected {
        draw_waiting_for_runtime(frame, area);
        return;
    }
    match app.page {
        Page::Home => draw_home(frame, app, area),
        Page::Market => draw_market(frame, app, area),
        Page::Chart => draw_chart(frame, app, area),
        Page::Screener => draw_screener(frame, app, area),
        Page::Portfolio => draw_portfolio(frame, app, area),
        Page::Orders => draw_orders(frame, app, area),
        Page::Tca => draw_tca(frame, app, area),
        Page::Depth => draw_depth(frame, app, area),
        Page::Tape => draw_tape(frame, app, area),
        Page::Strategies => draw_strategies(frame, app, area),
        Page::Metrics => draw_metrics(frame, app, area),
        Page::News => draw_news(frame, app, area),
        Page::Risk => draw_risk(frame, app, area),
        Page::Autonomy => draw_autonomy(frame, app, area),
        Page::Alerts => draw_alerts(frame, app, area),
        Page::Health => draw_health(frame, app, area),
        Page::Trace => draw_trace(frame, app, area),
        Page::Search => draw_search(frame, app, area),
        Page::Analyst => draw_analyst(frame, app, area),
        Page::LlmControl => draw_llm_control(frame, app, area),
        Page::Backtests => draw_backtests(frame, app, area),
        Page::Models => draw_models(frame, app, area),
        Page::Attribution => draw_attribution(frame, app, area),
        Page::Experiments => draw_experiments(frame, app, area),
        Page::Help => draw_help(frame, area),
    }
}

fn draw_llm_control(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let provider = app
        .runtime
        .llm_provider_id
        .as_deref()
        .unwrap_or("NOT CONFIGURED");
    let model = app.runtime.llm_model.as_deref().unwrap_or("—");
    let lines = vec![
        Line::styled("LLM DECISION CONTROL", amber()),
        Line::from(format!("PROVIDER       {provider}")),
        Line::from(format!("MODEL          {model}")),
        Line::from("TRADING MODES  MANUAL | HYBRID | AUTO"),
        Line::from(""),
        Line::from(
            "The LLM is asynchronous intelligence. Risk, sizing, execution, and reconciliation remain deterministic.",
        ),
        Line::from(""),
        Line::styled("COMMANDS", amber()),
        Line::from("  MODE MANUAL|HYBRID|AUTO"),
        Line::from("  ANALYZE <question>   request a background analysis"),
        Line::from("  AUTO                 inspect validated plans and proposals"),
        Line::from("  CONFIG SHOW | CONFIG PROMPT <text>   inspect/update system prompt"),
        Line::from(""),
        Line::styled("SYSTEM PROMPT", amber()),
        Line::from(if app.llm_system_prompt.is_empty() {
            "  (not configured)".to_owned()
        } else {
            format!(
                "  {}",
                app.llm_system_prompt.chars().take(240).collect::<String>()
            )
        }),
        Line::from(
            "Prompt changes are configuration mutations and are applied atomically by the runtime.",
        ),
        Line::from(
            "Provider outages never block charts, metrics, strategies, or manual order entry.",
        ),
    ];
    frame.render_widget(
        panel(
            "LLM CONTROL PLANE",
            Paragraph::new(lines).wrap(Wrap { trim: false }),
        ),
        area,
    );
}

fn draw_waiting_for_runtime(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let text = Paragraph::new(vec![
        Line::from(""),
        Line::styled("  WAITING FOR CONNECTION", amber()),
        Line::from(""),
        Line::from("  The terminal is ready. Start the headless runtime and it will reconnect automatically."),
        Line::from("  Expected command: insider server"),
        Line::from("  Press R or type REFRESH to retry now. Ctrl-C exits this terminal only."),
    ])
    .block(Block::default().borders(Borders::ALL).title("RUNTIME OFFLINE"))
    .wrap(Wrap { trim: false });
    frame.render_widget(text, area);
}

fn draw_home(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);
    let left = vec![
        Line::from(vec![
            Span::styled("MARKET", amber()),
            Span::raw(format!("   {} instruments", app.runtime.markets.len())),
        ]),
        Line::from(vec![
            Span::styled("PORT", amber()),
            Span::raw(format!(
                "     {} positions / {} orders",
                app.runtime.positions.len(),
                app.runtime.orders.len()
            )),
        ]),
        Line::from(vec![
            Span::styled("STRAT", amber()),
            Span::raw(format!(
                "    {} live proposals",
                app.runtime.proposals.len()
            )),
        ]),
        Line::from(vec![
            Span::styled("NEWS", amber()),
            Span::raw("     context and ranked headlines"),
        ]),
        Line::from(vec![
            Span::styled("HEALTH", amber()),
            Span::raw("   broker, providers, supervisors"),
        ]),
        Line::from(vec![
            Span::styled("TCA", amber()),
            Span::raw("      execution quality and latency"),
        ]),
        Line::from(vec![
            Span::styled("TRACE", amber()),
            Span::raw("    reconstruct a decision"),
        ]),
        Line::from(vec![
            Span::styled("ANALYZE", amber()),
            Span::raw("  contextual provider-agnostic analyst"),
        ]),
        Line::from(""),
        Line::from("Type a function mnemonic, then press Enter / <GO>."),
    ];
    frame.render_widget(panel("FUNCTIONS", Paragraph::new(left)), columns[0]);
    let risk_color = if app.runtime.risk == "RUNNING" {
        GREEN
    } else {
        RED
    };
    let right = vec![
        Line::from(vec![
            Span::raw("RISK STATE       "),
            Span::styled(
                &app.runtime.risk,
                Style::default().fg(risk_color).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(format!("AUTONOMY MODE    {}", app.runtime.mode)),
        Line::from(format!(
            "GROSS NOTIONAL  {}",
            app.runtime.gross_notional_ticks
        )),
        Line::from(format!(
            "UTILIZATION     {}",
            format_bps(app.runtime.gross_utilization_bps)
        )),
        Line::from(format!("CASH            {}", app.runtime.cash_ticks)),
        Line::from(format!(
            "REALIZED P&L    {:+}",
            app.runtime.realized_pnl_ticks
        )),
        Line::from(format!("FEES            {}", app.runtime.fees_ticks)),
        Line::from(format!("OPEN ALERTS     {}", app.alerts.len())),
    ];
    frame.render_widget(panel("OPERATING STATE", Paragraph::new(right)), columns[1]);
}

fn draw_market(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app
        .runtime
        .markets
        .iter()
        .enumerate()
        .skip(app.scroll)
        .map(|(index, market)| {
            let selected = index == app.market_selected;
            Row::new(vec![
                if selected { ">".into() } else { " ".into() },
                market.instrument.to_string(),
                format_optional(market.bid),
                format_optional(market.ask),
                format_optional(market.last),
                market.quote_quality.clone(),
                market.trade_quality.clone(),
                market.bars.len().to_string(),
            ])
            .style(if selected {
                Style::default().fg(Color::Black).bg(AMBER)
            } else {
                Style::default()
            })
        });
    let table = Table::new(
        rows,
        [
            Constraint::Length(2),
            Constraint::Length(18),
            Constraint::Length(14),
            Constraint::Length(14),
            Constraint::Length(14),
            Constraint::Length(11),
            Constraint::Length(11),
            Constraint::Length(6),
        ],
    )
    .header(header([
        "",
        "INSTRUMENT",
        "BID",
        "ASK",
        "LAST",
        "QUOTE",
        "TRADE",
        "BARS",
    ]));
    let splits = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(8), Constraint::Length(5)])
        .split(area);
    frame.render_widget(
        panel("CANONICAL MARKET STATE — Enter opens CHART", table),
        splits[0],
    );
    let selected = app.selected_instrument.and_then(|id| {
        app.runtime
            .markets
            .iter()
            .find(|market| market.instrument == id)
    });
    let closes = selected
        .map(|market| {
            market
                .bars
                .iter()
                .map(|bar| u64::try_from(bar.close.max(0)).unwrap_or(0))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let summary = selected.and_then(|market| market.bars.last()).map_or_else(
        || " PRICE HISTORY ".into(),
        |bar| {
            format!(
                " O {}  H {}  L {}  C {}  V {} ",
                bar.open, bar.high, bar.low, bar.close, bar.volume
            )
        },
    );
    frame.render_widget(
        Sparkline::default()
            .block(Block::bordered().title(summary))
            .data(&closes)
            .style(Style::default().fg(ORANGE)),
        splits[1],
    );
}

#[allow(clippy::too_many_lines)]
fn draw_chart(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let selected = app.selected_instrument.and_then(|identity| {
        app.runtime
            .markets
            .iter()
            .find(|market| market.instrument == identity)
    });
    let Some(market) = selected else {
        frame.render_widget(
            panel(
                "OHLCV",
                Paragraph::new("No selected canonical instrument. Use MARKET then Enter."),
            ),
            area,
        );
        return;
    };
    let end = market.bars.len().saturating_sub(app.chart_offset);
    let start = end.saturating_sub(app.chart_window);
    let source_bars = market.bars.get(start..end).unwrap_or_default();
    let bars = aggregate_interval(source_bars, app.chart_interval);
    let cursor = cursor_index(bars.len(), app.chart_cursor_from_latest);
    let open_orders = app
        .runtime
        .orders
        .iter()
        .filter(|order| order.instrument == market.instrument)
        .count();
    let proposals = app
        .runtime
        .proposals
        .iter()
        .filter(|proposal| proposal.instrument == market.instrument)
        .count();
    let position = app
        .runtime
        .positions
        .iter()
        .find(|position| position.instrument == market.instrument)
        .map_or(0, |position| position.quantity);
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(10),
            Constraint::Length(7),
        ])
        .split(area);
    frame.render_widget(
        panel(
            "LIVE CONTEXT",
            Paragraph::new(vec![
                Line::from(format!(
                    "SECURITY {}  INSTRUMENT {}  BID {}  ASK {}  LAST {}  QUOTE {}  TRADE {}",
                    if app.selected_symbol.is_empty() {
                        "—"
                    } else {
                        &app.selected_symbol
                    },
                    market.instrument,
                    format_optional(market.bid),
                    format_optional(market.ask),
                    format_optional(market.last),
                    market.quote_quality,
                    market.trade_quality
                )),
                Line::from(format!(
                    "POSITION {position:+}  ORDERS {open_orders}  PROPOSALS {proposals}  WINDOW {} SOURCE  AGG {}  STYLE {}  OFFSET {}",
                    app.chart_window,
                    app.chart_interval.name(),
                    app.chart_style.name(),
                    app.chart_offset
                )),
                chart_cursor_readout(&bars, cursor, app.chart_overlays),
            ]),
        ),
        regions[0],
    );
    let (low, high) = price_bounds(&bars).unwrap_or((0, 0));
    let timing = chart_timing(source_bars, app.chart_interval.factor());
    frame.render_widget(
        panel(
            &format!(
                "PRICE {low}..{high}  | {}  | {}  | {}",
                app.chart_style.name(),
                app.chart_overlays.legend(),
                timing.label
            ),
            PriceChart {
                bars: &bars,
                style: app.chart_style,
                overlays: app.chart_overlays,
                cursor,
                time_axis_valid: timing.axis_valid,
            },
        ),
        regions[1],
    );
    let maximum_volume = bars.iter().map(|bar| bar.volume.max(0)).max().unwrap_or(0);
    frame.render_widget(
        panel(
            &format!("VOLUME MAX {maximum_volume}  | +/- ZOOM  ←/→ PAN  SHIFT+←/→ XHAIR  0 RESET"),
            VolumeChart {
                bars: &bars,
                cursor,
            },
        ),
        regions[2],
    );
}

fn draw_screener(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(6)])
        .split(area);
    frame.render_widget(
        panel(
            "QUERY",
            Paragraph::new(format!(
                "MODE {:<8}  MATCHES {} / {} CANONICAL MARKETS  | SCREEN <ALL|MOVERS|GAINERS|LOSERS|VOLUME|SPREAD|STALE>  | Enter opens CHART",
                app.screener_mode.name(),
                app.screener_rows.len(),
                app.runtime.markets.len()
            )),
        ),
        regions[0],
    );
    let rows = app
        .screener_rows
        .iter()
        .enumerate()
        .skip(app.scroll)
        .map(|(index, row)| {
            let selected = index == app.screener_selected;
            let style = if selected {
                Style::default().fg(Color::Black).bg(AMBER)
            } else if row.quote_quality != "GOOD" || row.trade_quality != "GOOD" {
                Style::default().fg(AMBER)
            } else if row.change_bps.is_some_and(|value| value > 0) {
                Style::default().fg(GREEN)
            } else if row.change_bps.is_some_and(|value| value < 0) {
                Style::default().fg(RED)
            } else {
                Style::default()
            };
            Row::new(vec![
                if selected { ">".into() } else { " ".into() },
                index.saturating_add(1).to_string(),
                row.instrument.to_string(),
                format_optional(row.bid),
                format_optional(row.ask),
                format_optional(row.last),
                row.change_bps.map_or_else(|| "—".into(), format_bps),
                row.spread_bps
                    .map_or_else(|| "—".into(), |value| format!("{value} bp")),
                row.volume.to_string(),
                row.quote_quality.clone(),
                row.trade_quality.clone(),
            ])
            .style(style)
        });
    frame.render_widget(
        panel(
            "DETERMINISTIC SNAPSHOT RANKING",
            Table::new(
                rows,
                [
                    Constraint::Length(2),
                    Constraint::Length(6),
                    Constraint::Length(18),
                    Constraint::Length(14),
                    Constraint::Length(14),
                    Constraint::Length(14),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(14),
                    Constraint::Length(10),
                    Constraint::Length(10),
                ],
            )
            .header(header([
                "",
                "RANK",
                "INSTRUMENT",
                "BID",
                "ASK",
                "LAST",
                "CHANGE",
                "SPREAD",
                "VOLUME",
                "QUOTE",
                "TRADE",
            ])),
        ),
        regions[1],
    );
}

fn draw_portfolio(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app
        .runtime
        .positions
        .iter()
        .skip(app.scroll)
        .map(|position| {
            let pnl =
                i128::from(position.mark - position.average_cost) * i128::from(position.quantity);
            Row::new(vec![
                position.instrument.to_string(),
                position.quantity.to_string(),
                position.mark.to_string(),
                position.average_cost.to_string(),
                format!("{pnl:+}"),
            ])
            .style(Style::default().fg(if pnl < 0 { RED } else { GREEN }))
        });
    frame.render_widget(
        panel(
            "RECONCILED POSITIONS",
            Table::new(
                rows,
                [
                    Constraint::Length(20),
                    Constraint::Length(16),
                    Constraint::Length(16),
                    Constraint::Length(16),
                    Constraint::Length(20),
                ],
            )
            .header(header([
                "INSTRUMENT",
                "QUANTITY",
                "MARK",
                "AVG COST",
                "UNREALIZED P&L",
            ])),
        ),
        area,
    );
}

fn draw_orders(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Percentage(50),
            Constraint::Min(6),
        ])
        .split(area);
    let selected = app.runtime.orders.get(app.scroll);
    let details = selected_order_details(app, selected);
    frame.render_widget(
        panel(
            "SELECTED ORDER — AUTHORITATIVE STATE",
            Paragraph::new(details),
        ),
        regions[0],
    );
    let rows = app
        .runtime
        .orders
        .iter()
        .enumerate()
        .skip(app.scroll)
        .map(|(index, order)| {
            Row::new(vec![
                if index == app.scroll {
                    ">".into()
                } else {
                    " ".into()
                },
                order.client_order_id.clone(),
                order.instrument.to_string(),
                order.side.clone(),
                order.quantity.to_string(),
                order.filled.to_string(),
                order.state.clone(),
            ])
            .style(if index == app.scroll {
                Style::default().fg(Color::Black).bg(AMBER)
            } else {
                Style::default()
            })
        });
    frame.render_widget(
        panel(
            "ORDER BLOTTER — BUY/SELL <id> <qty> [MKT|LMT <px>]",
            Table::new(
                rows,
                [
                    Constraint::Length(2),
                    Constraint::Percentage(28),
                    Constraint::Length(18),
                    Constraint::Length(7),
                    Constraint::Length(12),
                    Constraint::Length(12),
                    Constraint::Length(18),
                ],
            )
            .header(header([
                "",
                "CLIENT ORDER",
                "INSTRUMENT",
                "SIDE",
                "QUANTITY",
                "FILLED",
                "STATE",
            ])),
        ),
        regions[1],
    );
    draw_selected_order_fills(frame, app, selected, regions[2]);
}

fn selected_order_details(app: &App, selected: Option<&model::OrderView>) -> Vec<Line<'static>> {
    selected.map_or_else(
        || vec![Line::from("NO ORDERS")],
        |order| {
            let remaining = order.quantity.saturating_sub(order.filled);
            let tca = app
                .runtime
                .tca
                .iter()
                .find(|value| value.client_order_id == order.client_order_id);
            vec![
                Line::from(format!(
                    "{}  {} {}  INSTRUMENT {}",
                    order.client_order_id, order.side, order.quantity, order.instrument
                )),
                Line::from(format!(
                    "STATE {}  FILLED {}  REMAINING {}",
                    order.state, order.filled, remaining
                )),
                Line::from(format!(
                    "TCA SHORTFALL {}  SPREAD {}  ADVERSE {}",
                    tca.and_then(|value| value.shortfall)
                        .map_or_else(|| "—".into(), |value| value.to_string()),
                    tca.and_then(|value| value.spread)
                        .map_or_else(|| "—".into(), |value| value.to_string()),
                    tca.and_then(|value| value.adverse_selection)
                        .map_or_else(|| "—".into(), |value| value.to_string())
                )),
                Line::from(
                    "Up/Down SELECT · CANCEL <client-order-id> · TCA opens full quality view",
                ),
            ]
        },
    )
}

fn draw_selected_order_fills(
    frame: &mut ratatui::Frame<'_>,
    app: &App,
    selected: Option<&model::OrderView>,
    area: Rect,
) {
    let selected_id = selected.map(|order| order.client_order_id.as_str());
    let rows = app
        .runtime
        .fills
        .iter()
        .filter(|fill| Some(fill.client_order_id.as_str()) == selected_id)
        .map(|fill| {
            Row::new(vec![
                fill.client_order_id.clone(),
                fill.instrument.to_string(),
                if fill.signed_quantity >= 0 {
                    "BUY"
                } else {
                    "SELL"
                }
                .into(),
                fill.signed_quantity.unsigned_abs().to_string(),
                fill.price.to_string(),
            ])
        });
    frame.render_widget(
        panel(
            "LINKED RECONCILED FILLS",
            Table::new(
                rows,
                [
                    Constraint::Percentage(35),
                    Constraint::Length(18),
                    Constraint::Length(7),
                    Constraint::Length(12),
                    Constraint::Length(15),
                ],
            )
            .header(header([
                "CLIENT ORDER",
                "INSTRUMENT",
                "SIDE",
                "QUANTITY",
                "PRICE",
            ])),
        ),
        area,
    );
}

fn draw_tca(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app.runtime.tca.iter().skip(app.scroll).map(|value| {
        let average = if value.average_price_denominator > 0 {
            format!(
                "{}/{}",
                value.average_price_numerator, value.average_price_denominator
            )
        } else {
            "—".into()
        };
        let decision_latency = elapsed_ns(value.decision_ns, value.send_ns);
        let ack_latency = elapsed_ns(value.send_ns, value.ack_ns);
        let fill_latency = elapsed_ns(value.send_ns, value.first_fill_ns);
        Row::new(vec![
            value.client_order_id.clone(),
            value.filled_quantity.to_string(),
            value.notional.to_string(),
            average,
            optional_number(value.arrival_price),
            optional_number(value.spread),
            optional_wide(value.shortfall),
            optional_wide(value.adverse_selection),
            decision_latency,
            ack_latency,
            fill_latency,
        ])
    });
    frame.render_widget(
        panel(
            "REALIZED EXECUTION QUALITY",
            Table::new(
                rows,
                [
                    Constraint::Percentage(22),
                    Constraint::Length(10),
                    Constraint::Length(15),
                    Constraint::Length(18),
                    Constraint::Length(12),
                    Constraint::Length(9),
                    Constraint::Length(14),
                    Constraint::Length(14),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(10),
                ],
            )
            .header(header([
                "ORDER",
                "FILLED",
                "NOTIONAL",
                "VWAP EXACT",
                "ARRIVAL",
                "SPREAD",
                "SHORTFALL",
                "ADVERSE",
                "SEND LAT",
                "ACK LAT",
                "FILL LAT",
            ])),
        ),
        area,
    );
}

fn draw_depth(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let selected = app.selected_instrument.and_then(|identity| {
        app.runtime
            .markets
            .iter()
            .find(|market| market.instrument == identity)
    });
    let lines = match selected {
        Some(market) => {
            let (bid, bid_quantity, ask, ask_quantity) = market.book_top.unwrap_or((0, 0, 0, 0));
            let total = i128::from(bid_quantity) + i128::from(ask_quantity);
            let imbalance_bps = if total > 0 {
                ((i128::from(bid_quantity) - i128::from(ask_quantity)) * 10_000) / total
            } else {
                0
            };
            vec![
                Line::from(format!("INSTRUMENT       {}", market.instrument)),
                Line::from(format!("ASK              {ask_quantity} x {ask}")),
                Line::styled(
                    "────────────────────────────────────────",
                    Style::default().fg(Color::DarkGray),
                ),
                Line::from(format!("BID              {bid_quantity} x {bid}")),
                Line::from(format!("SPREAD           {}", ask.saturating_sub(bid))),
                Line::from(format!("TOP IMBALANCE    {imbalance_bps:+} bps")),
                Line::from(format!("BOOK HEALTH      {}", market.quote_quality)),
            ]
        }
        None => vec![Line::from("No selected canonical instrument")],
    };
    frame.render_widget(
        panel("LEVEL 2 TOP — DEPTH <instrument-id>", Paragraph::new(lines)),
        area,
    );
}

fn draw_tape(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let trades = app.selected_instrument.and_then(|identity| {
        app.runtime
            .markets
            .iter()
            .find(|market| market.instrument == identity)
    });
    let rows = trades
        .into_iter()
        .flat_map(|market| market.trades.iter().rev())
        .skip(app.scroll)
        .map(|trade| {
            Row::new(vec![
                trade.sequence.to_string(),
                trade.exchange_time_ns.to_string(),
                trade.received_mono_ns.to_string(),
                trade.price.to_string(),
                trade.quantity.to_string(),
            ])
        });
    frame.render_widget(
        panel(
            "TIME & SALES — TAPE <instrument-id>",
            Table::new(
                rows,
                [
                    Constraint::Length(12),
                    Constraint::Length(22),
                    Constraint::Length(22),
                    Constraint::Length(16),
                    Constraint::Length(14),
                ],
            )
            .header(header([
                "SEQUENCE",
                "EXCHANGE NS",
                "RECEIVED NS",
                "PRICE",
                "QUANTITY",
            ])),
        ),
        area,
    );
}

fn draw_strategies(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let defaults = [
        (
            "starter.momentum.threshold.v1",
            "DETERMINISTIC",
            "READY",
            "ACTIVE",
            "NORMAL",
            "starter.momentum.v1",
        ),
        (
            "starter.mean.reversion.v1",
            "DETERMINISTIC",
            "READY",
            "ACTIVE",
            "NORMAL",
            "starter.rsi.v1, starter.volatility.v1",
        ),
        (
            "news.regime.guard.v1",
            "HYBRID",
            "READY",
            "PAUSED",
            "HIGH",
            "news.sentiment.v1, volatility.ewma.v2",
        ),
    ];
    let rows = if app.strategies.is_empty() {
        Box::new(defaults.into_iter().map(|value| {
            let (strategy, mode, state, lifecycle, priority, metrics) = value;
            Row::new(vec![
                strategy.to_owned(),
                mode.to_owned(),
                state.to_owned(),
                lifecycle.to_owned(),
                priority.to_owned(),
                metrics.to_owned(),
            ])
        })) as Box<dyn Iterator<Item = Row<'static>>>
    } else {
        Box::new(app.strategies.iter().skip(app.scroll).map(|value| {
            Row::new(vec![
                value.id.clone(),
                value.mode.clone(),
                value.state.clone(),
                value.lifecycle.clone(),
                value.priority.clone(),
                value.metrics.join(","),
            ])
        })) as Box<dyn Iterator<Item = Row<'static>>>
    };
    frame.render_widget(
        panel(
            "INSTALLED STRATEGIES",
            Table::new(
                rows,
                [
                    Constraint::Percentage(30),
                    Constraint::Length(14),
                    Constraint::Length(14),
                    Constraint::Length(14),
                    Constraint::Length(10),
                    Constraint::Percentage(30),
                ],
            )
            .header(header([
                "STRATEGY",
                "MODE",
                "STATE",
                "LIFECYCLE",
                "PRIORITY",
                "METRICS",
            ])),
        ),
        area,
    );
}

fn draw_metrics(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app.metrics.iter().skip(app.scroll).map(|value| {
        Row::new(vec![
            value.id.clone(),
            value.state.clone(),
            value.lifecycle.clone(),
            value.priority.clone(),
            format_duration(value.period_ns),
            format_duration(value.deadline_ns),
            value.inputs.join(","),
        ])
    });
    frame.render_widget(
        panel(
            "INSTALLED METRICS",
            Table::new(
                rows,
                [
                    Constraint::Percentage(28),
                    Constraint::Length(12),
                    Constraint::Length(14),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Percentage(30),
                ],
            )
            .header(header([
                "METRIC",
                "STATE",
                "LIFECYCLE",
                "PRIORITY",
                "PERIOD",
                "DEADLINE",
                "INPUTS",
            ])),
        ),
        area,
    );
}

fn draw_news(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    if let Some(detail) = &app.news_detail {
        draw_news_detail(frame, app, detail, area);
        return;
    }
    draw_news_feed(frame, app, area);
}

fn draw_news_feed(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(6)])
        .split(area);
    let page = app.news_cursor_history.len().saturating_add(1);
    let scope = app.news_scope.to_ascii_uppercase();
    frame.render_widget(
        panel(
            "FEED CONTROL",
            Paragraph::new(format!(
                "{scope:<8}  SYMBOL {:<12}  PAGE {page}  {}  | NEWS RELEVANT [symbol] · NEWS ALL · NN · NP · Enter/DETAIL",
                if app.news_next_cursor.is_some() { "MORE >" } else { "END" },
                if app.selected_symbol.is_empty() { "ALL" } else { &app.selected_symbol }
            )),
        ),
        regions[0],
    );
    let rows = app
        .news
        .iter()
        .enumerate()
        .skip(app.scroll)
        .map(|(index, value)| {
            let selected = index == app.news_selected;
            Row::new(vec![
                if selected { ">".into() } else { " ".into() },
                format!("{:.0}%", value.relevance * 100.0),
                value.source.clone(),
                value.title.clone(),
                value.symbols.join(","),
                value.received_ms.to_string(),
                format!("{} · {}", value.id, value.url),
            ])
            .style(if selected {
                Style::default().fg(Color::Black).bg(AMBER)
            } else {
                Style::default()
            })
        });
    frame.render_widget(
        panel(
            "BOUNDED CANONICAL NEWS",
            Table::new(
                rows,
                [
                    Constraint::Length(2),
                    Constraint::Length(7),
                    Constraint::Length(16),
                    Constraint::Percentage(45),
                    Constraint::Length(16),
                    Constraint::Length(15),
                    Constraint::Percentage(22),
                ],
            )
            .header(header([
                "",
                "SCORE",
                "SOURCE",
                "HEADLINE",
                "SYMBOLS",
                "RECEIVED",
                "ARTICLE ID / CANONICAL URL",
            ])),
        ),
        regions[1],
    );
}

fn draw_news_detail(
    frame: &mut ratatui::Frame<'_>,
    app: &App,
    detail: &model::NewsDetailView,
    area: Rect,
) {
    let current = &detail.current;
    let metadata = vec![
        Line::from(vec![
            Span::styled("SOURCE  ", amber()),
            Span::raw(format!("{} ({})", current.source, current.provider)),
        ]),
        Line::from(vec![
            Span::styled("SYMBOLS ", amber()),
            Span::raw(current.symbols.join(", ")),
        ]),
        Line::from(vec![
            Span::styled("TIME    ", amber()),
            Span::raw(format!(
                "published {}  received {}",
                current
                    .published_ms
                    .map_or_else(|| "unknown".into(), |value| value.to_string()),
                current.received_ms
            )),
        ]),
        Line::from(vec![
            Span::styled("CLUSTER ", amber()),
            Span::raw(format!(
                "{}  related={}  retained_versions={}",
                detail.cluster_id,
                detail.related_item_ids.len(),
                detail.versions.len()
            )),
        ]),
        Line::from(vec![
            Span::styled("HASH    ", amber()),
            Span::raw(&current.content_hash),
        ]),
    ];
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(7),
            Constraint::Min(5),
            Constraint::Length(3),
        ])
        .split(area);
    frame.render_widget(
        panel(
            "ARTICLE — ESC/BACK RETURNS TO FEED",
            Paragraph::new(current.title.as_str())
                .style(
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                )
                .wrap(Wrap { trim: false }),
        ),
        regions[0],
    );
    frame.render_widget(
        panel("AUTHORITATIVE METADATA", Paragraph::new(metadata)),
        regions[1],
    );
    frame.render_widget(
        panel(
            "NORMALIZED SUMMARY",
            Paragraph::new(current.summary.as_deref().unwrap_or("No summary supplied."))
                .wrap(Wrap { trim: false })
                .scroll((u16::try_from(app.scroll).unwrap_or(u16::MAX), 0)),
        ),
        regions[2],
    );
    frame.render_widget(
        panel(
            "CANONICAL HTTPS LINK",
            Paragraph::new(format!("{}  [{}]", current.url, current.id)),
        ),
        regions[3],
    );
}

fn draw_risk(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let lines = vec![
        Line::from(vec![
            Span::raw("CURRENT STATE                 "),
            Span::styled(
                &app.runtime.risk,
                Style::default()
                    .fg(if app.runtime.risk == "RUNNING" {
                        GREEN
                    } else {
                        RED
                    })
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(format!(
            "GROSS NOTIONAL               {}",
            app.runtime.gross_notional_ticks
        )),
        Line::from(format!(
            "MAX GROSS NOTIONAL           {}",
            app.runtime.max_gross_notional_ticks
        )),
        Line::from(format!(
            "UTILIZATION                  {}",
            format_bps(app.runtime.gross_utilization_bps)
        )),
        Line::from(format!(
            "LARGEST POSITION NOTIONAL    {}",
            app.runtime.largest_position_notional_ticks
        )),
        Line::from(format!(
            "DRAWDOWN                     {}",
            app.runtime
                .drawdown_bps
                .map_or_else(|| "N/A".into(), format_bps)
        )),
        Line::from(""),
        Line::styled(
            "Controls: RISKSTATE <running|reduce|cancel|halted> <authorization>",
            Style::default().fg(AMBER),
        ),
        Line::styled(
            "Emergency: HALT <authorization>",
            Style::default().fg(RED).add_modifier(Modifier::BOLD),
        ),
    ];
    frame.render_widget(
        panel("PRE-TRADE AND PORTFOLIO RISK", Paragraph::new(lines)),
        area,
    );
}

fn draw_autonomy(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let splits = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),
            Constraint::Percentage(42),
            Constraint::Min(6),
        ])
        .split(area);
    let provider = app.runtime.llm_provider_id.as_deref().unwrap_or("NONE");
    let model = app.runtime.llm_model.as_deref().unwrap_or("UNRECORDED");
    let mut lines = vec![
        Line::from(format!("MODE             {}", app.runtime.mode)),
        Line::from("MODE MANUAL | MODE HYBRID | MODE AUTO"),
        Line::from(format!("CONFIGURED LLM   {provider} / {model}")),
    ];
    if let Some(plan) = &app.runtime.plan {
        lines.push(Line::from(format!(
            "PLAN / STATE     {} / {}",
            plan.id, plan.state
        )));
        lines.push(Line::from(format!(
            "GENERATED / TTL  {} / {}",
            plan.generated_at_ns,
            format_duration(plan.expires_at_ns.saturating_sub(plan.generated_at_ns))
        )));
        lines.push(Line::from(format!(
            "HARD EXPIRY NS   {}",
            plan.expires_at_ns
        )));
        lines.push(Line::from("RECONSIDER       NOT RECORDED IN PLAN SCHEMA"));
    } else {
        lines.push(Line::from("PLAN             NONE"));
    }
    frame.render_widget(
        panel("AUTONOMOUS COORDINATOR", Paragraph::new(lines)),
        splits[0],
    );
    draw_autonomy_actions(frame, app, splits[1]);
    let rows = app
        .runtime
        .proposals
        .iter()
        .enumerate()
        .skip(app.scroll)
        .map(|(index, value)| {
            Row::new(vec![
                if index == app.scroll {
                    ">".into()
                } else {
                    " ".into()
                },
                value.id.to_string(),
                value.strategy.clone(),
                value.instrument.to_string(),
                value.action.clone(),
                format!("{:.1}%", value.confidence * 100.0),
                value.state.clone(),
                format_duration(value.ttl_ns),
            ])
            .style(if index == app.scroll {
                Style::default().fg(Color::Black).bg(AMBER)
            } else {
                Style::default()
            })
        });
    frame.render_widget(
        panel(
            "ACTIVE STRATEGY PROPOSALS — Enter previews selected proposal; CONFIRM submits",
            Table::new(
                rows,
                [
                    Constraint::Length(2),
                    Constraint::Length(20),
                    Constraint::Percentage(25),
                    Constraint::Length(18),
                    Constraint::Length(20),
                    Constraint::Length(10),
                    Constraint::Length(12),
                    Constraint::Length(10),
                ],
            )
            .header(header([
                "",
                "PROPOSAL",
                "STRATEGY",
                "INSTRUMENT",
                "ACTION",
                "CONF",
                "STATE",
                "TTL",
            ])),
        ),
        splits[2],
    );
}

fn draw_autonomy_actions(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let actions = app
        .runtime
        .plan
        .iter()
        .flat_map(|plan| plan.actions.iter())
        .map(|action| {
            let strategy = action
                .proposal_id
                .as_deref()
                .and_then(|id| id.parse::<u128>().ok())
                .and_then(|id| {
                    app.runtime
                        .proposals
                        .iter()
                        .find(|proposal| proposal.id == id)
                })
                .map_or("—", |proposal| proposal.strategy.as_str());
            Row::new(vec![
                action.action.clone(),
                action.proposal_id.clone().unwrap_or_else(|| "—".into()),
                strategy.to_owned(),
                action
                    .scale
                    .map_or_else(|| "—".into(), |scale| format!("{scale:.2}")),
                if action.reason_codes.is_empty() {
                    "—".into()
                } else {
                    action.reason_codes.join(", ")
                },
            ])
        });
    frame.render_widget(
        panel(
            "SELECTED PLAN ACTIONS — PROVIDER/MODEL ABOVE ARE CURRENT CONFIG, NOT PLAN PROVENANCE",
            Table::new(
                actions,
                [
                    Constraint::Length(20),
                    Constraint::Length(20),
                    Constraint::Percentage(22),
                    Constraint::Length(7),
                    Constraint::Min(18),
                ],
            )
            .header(header([
                "ACTION",
                "PROPOSAL",
                "STRATEGY",
                "SCALE",
                "REASON CODES",
            ])),
        ),
        area,
    );
}

fn draw_alerts(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app.alerts.iter().skip(app.scroll).map(|value| {
        Row::new(vec![
            severity(value.severity).into(),
            value.source.clone(),
            value.message.clone(),
            value.occurred_ms.to_string(),
            value.id.clone(),
        ])
        .style(Style::default().fg(if value.severity >= 3 { RED } else { AMBER }))
    });
    frame.render_widget(
        panel(
            "ACTIVE ALERTS — ACK <alert-id>",
            Table::new(
                rows,
                [
                    Constraint::Length(10),
                    Constraint::Length(18),
                    Constraint::Percentage(50),
                    Constraint::Length(16),
                    Constraint::Percentage(20),
                ],
            )
            .header(header(["SEVERITY", "SOURCE", "MESSAGE", "TIME", "ID"])),
        ),
        area,
    );
}

fn draw_health(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    frame.render_widget(
        panel(
            "ENGINE / BROKER / PROVIDERS / CONFIG",
            Paragraph::new(app.health_lines.join("\n")).wrap(Wrap { trim: false }),
        ),
        area,
    );
}

fn draw_trace(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app.trace.iter().skip(app.scroll).map(|event| {
        Row::new(vec![
            event.sequence.to_string(),
            event.kind.clone(),
            event.payload_bytes.to_string(),
            event.payload_preview.clone(),
        ])
    });
    frame.render_widget(
        panel(
            "JOURNAL TRACE EVENTS",
            Table::new(
                rows,
                [
                    Constraint::Length(14),
                    Constraint::Percentage(32),
                    Constraint::Length(12),
                    Constraint::Percentage(45),
                ],
            )
            .header(header([
                "SEQUENCE",
                "KIND",
                "BYTES",
                "PAYLOAD PREFIX (HEX)",
            ])),
        ),
        area,
    );
}

fn draw_search(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app.context_hits.iter().skip(app.scroll).map(|hit| {
        Row::new(vec![
            hit.node_id.clone(),
            format_score(hit.score),
            format_score(hit.exact_score),
            format_score(hit.lexical_score),
            format_score(hit.vector_score),
            hit.evidence_path.join(" → "),
        ])
    });
    frame.render_widget(
        panel(
            "HYBRID GRAPH / LEXICAL / VECTOR SEARCH",
            Table::new(
                rows,
                [
                    Constraint::Percentage(24),
                    Constraint::Length(9),
                    Constraint::Length(9),
                    Constraint::Length(9),
                    Constraint::Length(9),
                    Constraint::Percentage(40),
                ],
            )
            .header(header([
                "NODE",
                "SCORE",
                "EXACT",
                "LEXICAL",
                "VECTOR",
                "EVIDENCE PATH",
            ])),
        ),
        area,
    );
}

fn draw_analyst(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(5),
        ])
        .split(area);
    let state = app.analyst_pending_since.map_or_else(
        || {
            app.analyst_completed_at.map_or_else(
                || "IDLE".into(),
                |completed| {
                    let age = completed.elapsed();
                    format!(
                        "{}  age {:.1}s",
                        if age > app.analyst_stale_after {
                            "STALE"
                        } else {
                            "COMPLETE"
                        },
                        age.as_secs_f32()
                    )
                },
            )
        },
        |started| {
            format!(
                "RUNNING  elapsed {:.1}s  input and market refresh remain active",
                started.elapsed().as_secs_f32()
            )
        },
    );
    frame.render_widget(
        panel(
            "REPRODUCIBILITY",
            Paragraph::new(format!(
                "{state}  TRACE {}  FINISH {}",
                app.analyst.trace_id, app.analyst.finish_reason,
            )),
        ),
        sections[0],
    );
    frame.render_widget(
        panel(
            "AUTHORITATIVE CONTEXT QUESTION",
            Paragraph::new(app.analyst_question.as_str()).wrap(Wrap { trim: false }),
        ),
        sections[1],
    );
    let content = if app.analyst_is_pending() {
        "Waiting for the provider stream. Deterministic market, risk, execution, and terminal functions continue independently."
    } else if app.analyst.content.is_empty() {
        "Use ANALYZE <question> to start a bounded contextual request."
    } else {
        app.analyst.content.as_str()
    };
    frame.render_widget(
        panel(
            "ANSWER — ANALYZE <question>",
            Paragraph::new(content)
                .wrap(Wrap { trim: false })
                .scroll((u16::try_from(app.scroll).unwrap_or(u16::MAX), 0)),
        ),
        sections[2],
    );
}

fn draw_backtests(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app.backtests.iter().skip(app.scroll).map(|run| {
        Row::new(vec![
            run.run_id.clone(),
            run.strategy_id.clone(),
            run.event_count.to_string(),
            run.max_drawdown.to_string(),
            run.fees.to_string(),
            optional_wide(run.final_equity),
            run.dataset_hash.clone(),
            run.config_hash.clone(),
        ])
    });
    frame.render_widget(
        panel(
            "IMMUTABLE BACKTEST RUNS",
            Table::new(
                rows,
                [
                    Constraint::Percentage(18),
                    Constraint::Percentage(18),
                    Constraint::Length(10),
                    Constraint::Length(14),
                    Constraint::Length(12),
                    Constraint::Length(14),
                    Constraint::Percentage(14),
                    Constraint::Percentage(14),
                ],
            )
            .header(header([
                "RUN",
                "STRATEGY",
                "EVENTS",
                "MAX DD",
                "FEES",
                "FINAL EQUITY",
                "DATASET",
                "CONFIG",
            ])),
        ),
        area,
    );
}

fn draw_models(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app.models.iter().skip(app.scroll).map(|model| {
        Row::new(vec![
            if model.active {
                "● ACTIVE".into()
            } else {
                "○".into()
            },
            model.model_id.clone(),
            model.version.clone(),
            model.status.clone(),
            model.input_width.to_string(),
            model.artifact_hash.clone(),
        ])
        .style(Style::default().fg(if model.active { GREEN } else { Color::Gray }))
    });
    frame.render_widget(
        panel(
            "VERSIONED MODEL REGISTRY",
            Table::new(
                rows,
                [
                    Constraint::Length(10),
                    Constraint::Percentage(25),
                    Constraint::Length(16),
                    Constraint::Length(14),
                    Constraint::Length(10),
                    Constraint::Percentage(35),
                ],
            )
            .header(header([
                "ACTIVE",
                "MODEL",
                "VERSION",
                "STATUS",
                "WIDTH",
                "ARTIFACT HASH",
            ])),
        ),
        area,
    );
}

fn draw_attribution(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);
    let resolutions = app.resolutions.iter().skip(app.scroll).map(|value| {
        Row::new(vec![
            value.policy.clone(),
            value.now_ns.to_string(),
            value.accepted.to_string(),
            value.conflicts.to_string(),
            value.expired.to_string(),
            value.attributions.to_string(),
        ])
    });
    frame.render_widget(
        panel(
            "COORDINATOR RESOLUTIONS",
            Table::new(
                resolutions,
                [
                    Constraint::Percentage(30),
                    Constraint::Length(18),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(10),
                    Constraint::Length(12),
                ],
            )
            .header(header([
                "POLICY",
                "MONO NS",
                "ACCEPTED",
                "CONFLICTS",
                "EXPIRED",
                "ATTRIBUTED",
            ])),
        ),
        regions[0],
    );
    let execution = app.strategy_execution.iter().skip(app.scroll).map(|value| {
        Row::new(vec![
            value.strategy_id.clone(),
            value.fills.to_string(),
            value.quantity.to_string(),
            value.notional.to_string(),
        ])
    });
    frame.render_widget(
        panel(
            "REALIZED STRATEGY EXECUTION",
            Table::new(
                execution,
                [
                    Constraint::Percentage(40),
                    Constraint::Length(14),
                    Constraint::Length(20),
                    Constraint::Length(24),
                ],
            )
            .header(header(["STRATEGY", "FILLS", "QUANTITY", "NOTIONAL"])),
        ),
        regions[1],
    );
}

fn draw_experiments(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let rows = app.experiments.iter().skip(app.scroll).map(|run| {
        let metrics = run
            .metrics
            .iter()
            .map(|(name, value)| format!("{name}={value:.4}"))
            .collect::<Vec<_>>()
            .join(", ");
        Row::new(vec![
            run.run_id.clone(),
            run.status.clone(),
            metrics,
            run.artifact_count.to_string(),
            run.code_hash.clone(),
            run.dataset_hash.clone(),
            run.config_hash.clone(),
            run.provenance.join("; "),
        ])
        .style(Style::default().fg(if run.status == "SUCCEEDED" {
            GREEN
        } else {
            Color::Gray
        }))
    });
    frame.render_widget(
        panel(
            "REPRODUCIBLE RESEARCH RUNS",
            Table::new(
                rows,
                [
                    Constraint::Percentage(14),
                    Constraint::Length(11),
                    Constraint::Percentage(20),
                    Constraint::Length(9),
                    Constraint::Percentage(12),
                    Constraint::Percentage(12),
                    Constraint::Percentage(12),
                    Constraint::Percentage(22),
                ],
            )
            .header(header([
                "RUN",
                "STATUS",
                "METRICS",
                "ARTIFACTS",
                "CODE",
                "DATASET",
                "CONFIG",
                "PROVENANCE",
            ])),
        ),
        area,
    );
}

fn draw_help(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let text = "NAVIGATION\n  HOME  MARKET [id]  CHART/GP [id]  SCREEN [mode]  PORT  ORDERS  TCA\n  DEPTH [id]  TAPE [id]  STRAT  ATTRIB  METRICS  NEWS [scope] [symbol]\n  RISK  AUTO  ALERTS  HEALTH  TRACE <trace-id>  SEARCH <text>\n  ANALYZE <question>  BACKTESTS  EXPERIMENTS  MODELS\n\nSCREENER\n  SCREEN MOVERS|GAINERS|LOSERS|VOLUME|SPREAD|STALE|ALL\n  Complete canonical result set; Up/Down selects and Enter opens CHART\n\nCHART\n  MARKET + Up/Down + Enter      select and open canonical instrument\n  ZOOM <30|60|120|240|480|960> bounded source-bar window\n  INTERVAL <1|5|15|30|60>      source-bar aggregation (computed duration is shown)\n  STYLE <CANDLE|OHLC|LINE>      deterministic terminal-native renderer\n  OVERLAY <SMA20|SMA50|VWAP> [ON|OFF|TOGGLE] | OVERLAY CLEAR|DEFAULT\n  XHAIR <OLDER|NEWER|LATEST|OFF>  authoritative UTC OHLCV readout\n  PAN <OLDER|NEWER> [display-bars]  | CHARTRESET\n  Overlays are bounded display calculations and never enter metric/strategy state\n\nNEWS\n  NEWS AAPL | NEWS RELEVANT AAPL | NEWS ALL\n  NN / NEWSNEXT                 next bounded page\n  NP / NEWSPREV                 previous page\n  Enter / DETAIL [article-id]   authoritative article detail\n  Esc / BACK                    return to feed\n\nTRADING\n  BUY <instrument-id> <quantity> MKT\n  SELL <instrument-id> <quantity> LMT <price>\n  CONFIRM                       submit the displayed, risk-approved preview\n  CANCEL <client-order-id>\n\nCONTROL\n  MODE <MANUAL|HYBRID|AUTO>\n  RISKSTATE <running|reduce|cancel|halted> <authorization>\n  HALT <authorization>  ACK <alert-id>\n  STRATSET <id> <lifecycle> <confirmation> <evidence-ref>\n  METRICSET <id> <lifecycle> <confirmation> <evidence-ref>\n  CONFIG SHOW | CONFIG LOAD <path>  REFRESH  QUIT\n\nKEYS\n  F1 Help  F2 Market  F3 Portfolio  F4 Orders  F5 Strategies  F6 News\n  F7 Risk  F8 Health  F9 TCA  F10 Refresh\n  Chart: +/- zoom  Left/Right pan  Shift+Left/Right crosshair  0 reset\n  Up/Down select/scroll  Esc clears/backs out  Ctrl-C quits";
    let text = text
        .replace(
            "HOME  MARKET [id]  CHART/GP [id]  SCREEN [mode]",
            "HOME  MARKET [id]  CHART [id] (browser)  GP [id] (native)  SCREEN [mode]",
        )
        .replace(
            "PAN <OLDER|NEWER> [display-bars]  | CHARTRESET",
            "PAN <OLDER|NEWER> [display-bars]  | CHARTRESET\n  TV [instrument]                  local browser chart + read-only sidebar",
        );
    let text = format!(
        "{text}\n\nSECURITY-FIRST\n  AAPL GP | AAPL EQUITY GP | BTC-USD CRYPTO NEWS\n  Symbols resolve through the authoritative instrument master; BUY/SELL also accept symbols\n\nPROPOSALS\n  AUTO + Up/Down + Enter previews selection | PREVIEW <proposal-id> [scale]\n  CONFIRM is always required separately before proposal submission\n\nCOMMAND EDITING\n  Tab completes functions/arguments  Ctrl-P/Ctrl-N command history\n  Home/End or Ctrl-A/Ctrl-E move cursor  Delete/Backspace edit  Ctrl-U clears"
    );
    frame.render_widget(
        panel(
            "INSIDERTRADER FUNCTION DIRECTORY",
            Paragraph::new(text).wrap(Wrap { trim: false }),
        ),
        area,
    );
}

fn draw_command(frame: &mut ratatui::Frame<'_>, app: &App, area: Rect) {
    let style = if app.status_is_error {
        Style::default().fg(RED)
    } else {
        Style::default().fg(Color::Gray)
    };
    let command = app.command_line.text();
    let (before, after) = command_window(
        command,
        app.command_line.cursor(),
        usize::from(area.width.saturating_sub(3)),
    );
    let text = vec![
        Line::styled(&app.status, style),
        Line::from(vec![
            Span::styled(
                "> ",
                Style::default().fg(ORANGE).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                &before,
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                &after,
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(text)
            .block(
                Block::default()
                    .borders(Borders::TOP)
                    .border_style(Style::default().fg(Color::DarkGray)),
            )
            .style(Style::default().bg(Color::Rgb(8, 9, 10))),
        area,
    );
    if area.width >= 3 && area.height >= 3 {
        let cursor_column = u16::try_from(display_width(&before)).unwrap_or(u16::MAX);
        frame.set_cursor_position((
            area.x
                .saturating_add(2)
                .saturating_add(cursor_column)
                .min(area.right().saturating_sub(1)),
            area.y
                .saturating_add(2)
                .min(area.bottom().saturating_sub(1)),
        ));
    }
}

fn command_window(text: &str, cursor: usize, width: usize) -> (String, String) {
    if width == 0 {
        return (String::new(), String::new());
    }
    let (before, after) = text.split_at(cursor);
    let before_width = display_width(before);
    let after_width = display_width(after);
    if before_width.saturating_add(after_width) <= width {
        return (before.into(), after.into());
    }
    let (mut before_budget, mut after_budget) = if after_width == 0 {
        (width, 0)
    } else if before_width == 0 {
        (0, width)
    } else {
        let before = width.saturating_mul(2) / 3;
        (
            before.max(1).min(width),
            width.saturating_sub(before.max(1).min(width)),
        )
    };
    if before_width < before_budget {
        let unused = before_budget - before_width;
        before_budget = before_width;
        after_budget = after_budget.saturating_add(unused);
    }
    if after_width < after_budget {
        let unused = after_budget - after_width;
        after_budget = after_width;
        before_budget = before_budget.saturating_add(unused);
    }
    (
        visible_suffix(before, before_budget),
        visible_prefix(after, after_budget),
    )
}

fn visible_suffix(value: &str, width: usize) -> String {
    if display_width(value) <= width {
        return value.into();
    }
    if width <= 1 {
        return "‹".repeat(width);
    }
    let content_width = width - 1;
    let mut used = 0_usize;
    let mut start = value.len();
    for (index, character) in value.char_indices().rev() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if used.saturating_add(character_width) > content_width {
            break;
        }
        used = used.saturating_add(character_width);
        start = index;
    }
    format!("‹{}", &value[start..])
}

fn visible_prefix(value: &str, width: usize) -> String {
    if display_width(value) <= width {
        return value.into();
    }
    if width <= 1 {
        return "›".repeat(width);
    }
    let content_width = width - 1;
    let mut used = 0_usize;
    let mut end = 0;
    for (index, character) in value.char_indices() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if used.saturating_add(character_width) > content_width {
            break;
        }
        used = used.saturating_add(character_width);
        end = index.saturating_add(character.len_utf8());
    }
    format!("{}›", &value[..end])
}

fn display_width(value: &str) -> usize {
    value
        .chars()
        .map(|character| UnicodeWidthChar::width(character).unwrap_or(0))
        .sum()
}

fn draw_functions(frame: &mut ratatui::Frame<'_>, area: Rect) {
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(" F1 HELP ", key()),
            Span::styled(" F2 MARKET ", key()),
            Span::styled(" F3 PORT ", key()),
            Span::styled(" F4 ORDERS ", key()),
            Span::styled(" F5 STRAT ", key()),
            Span::styled(" F6 NEWS ", key()),
            Span::styled(" F7 RISK ", key()),
            Span::styled(" F8 HEALTH ", key()),
            Span::styled(" F9 TCA ", key()),
            Span::styled(" F10 REFRESH ", key()),
        ]))
        .style(Style::default().bg(Color::Rgb(16, 17, 19))),
        area,
    );
}

struct ChartTiming {
    label: String,
    axis_valid: bool,
}

fn chart_timing(source: &[BarView], factor: usize) -> ChartTiming {
    let Some(first) = source.first() else {
        return ChartTiming {
            label: "NO SOURCE BARS".into(),
            axis_valid: false,
        };
    };
    if first.interval_ns == 0
        || source
            .windows(2)
            .any(|pair| pair[1].interval_ns == 0 || pair[1].start_time_ns <= pair[0].start_time_ns)
    {
        return ChartTiming {
            label: "! NON-MONOTONIC SOURCE TIME".into(),
            axis_valid: false,
        };
    }
    if source
        .iter()
        .all(|bar| bar.interval_ns == first.interval_ns)
    {
        let display_interval = first
            .interval_ns
            .saturating_mul(u64::try_from(factor).unwrap_or(u64::MAX));
        ChartTiming {
            label: format!(
                "{} x {} = {}",
                factor,
                format_chart_duration(first.interval_ns),
                format_chart_duration(display_interval)
            ),
            axis_valid: true,
        }
    } else {
        ChartTiming {
            label: format!("{factor} SOURCE BARS / MIXED DURATIONS"),
            axis_valid: true,
        }
    }
}

fn chart_cursor_readout(
    bars: &[BarView],
    cursor: Option<usize>,
    overlays: ChartOverlays,
) -> Line<'static> {
    let Some(bar) = cursor.and_then(|cursor| bars.get(cursor)) else {
        return Line::from(format!(
            "XHAIR OFF  | DISPLAY-ONLY OVERLAYS {}",
            overlays.legend()
        ));
    };
    let direction = if bar.close >= bar.open {
        "+ UP"
    } else {
        "- DOWN"
    };
    let color = if bar.close >= bar.open { GREEN } else { RED };
    Line::from(vec![
        Span::raw(format!(
            "XHAIR {}  {}  O {}  H {}  L {}  C {}  V {}  ",
            format_full_utc_time(bar.start_time_ns),
            format_chart_duration(bar.interval_ns),
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume
        )),
        Span::styled(
            direction,
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("  | OVERLAYS {} DISPLAY ONLY", overlays.legend()),
            Style::default().fg(Color::Gray),
        ),
    ])
}

fn format_chart_duration(ns: u64) -> String {
    const SECOND: u64 = 1_000_000_000;
    const MINUTE: u64 = 60 * SECOND;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;
    if ns.is_multiple_of(DAY) {
        format!("{}d", ns / DAY)
    } else if ns.is_multiple_of(HOUR) {
        format!("{}h", ns / HOUR)
    } else if ns.is_multiple_of(MINUTE) {
        format!("{}m", ns / MINUTE)
    } else if ns.is_multiple_of(SECOND) {
        format!("{}s", ns / SECOND)
    } else if ns >= 1_000_000 {
        format!("{}ms", ns / 1_000_000)
    } else {
        format!("{ns}ns")
    }
}

fn format_axis_time(unix_ns: i64, interval_ns: u64) -> String {
    const MINUTE: u64 = 60_000_000_000;
    const DAY: u64 = 86_400_000_000_000;
    let (year, month, day, hour, minute, second) = utc_parts(unix_ns);
    if interval_ns >= DAY {
        format!("{year:04}-{month:02}-{day:02}")
    } else if interval_ns < MINUTE {
        format!("{hour:02}:{minute:02}:{second:02}")
    } else {
        format!("{hour:02}:{minute:02}")
    }
}

fn format_full_utc_time(unix_ns: i64) -> String {
    let (year, month, day, hour, minute, second) = utc_parts(unix_ns);
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}Z")
}

fn utc_parts(unix_ns: i64) -> (i64, i64, i64, i64, i64, i64) {
    const NS_PER_SECOND: i64 = 1_000_000_000;
    const SECONDS_PER_DAY: i64 = 86_400;
    let seconds = unix_ns.div_euclid(NS_PER_SECOND);
    let days = seconds.div_euclid(SECONDS_PER_DAY);
    let second_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    (
        year,
        month,
        day,
        second_of_day / 3_600,
        second_of_day % 3_600 / 60,
        second_of_day % 60,
    )
}

// Howard Hinnant's proleptic-Gregorian civil-from-days transform.
fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let shifted = days_since_epoch.saturating_add(719_468);
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted.saturating_sub(146_096)
    } / 146_097;
    let day_of_era = shifted.saturating_sub(era.saturating_mul(146_097));
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era.saturating_add(era.saturating_mul(400));
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

struct PriceChart<'a> {
    bars: &'a [BarView],
    style: ChartStyle,
    overlays: ChartOverlays,
    cursor: Option<usize>,
    time_axis_valid: bool,
}

impl Widget for PriceChart<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        if area.width == 0 || area.height == 0 || self.bars.is_empty() {
            return;
        }
        let axis_width = if area.width >= 24 { 13 } else { 0 };
        let time_axis_height = u16::from(area.height >= 4);
        let plot = Rect::new(
            area.x,
            area.y,
            area.width.saturating_sub(axis_width),
            area.height.saturating_sub(time_axis_height),
        );
        if plot.width == 0 || plot.height == 0 {
            return;
        }
        let bars = compress_for_width(self.bars, usize::from(plot.width));
        let sma20 = self
            .overlays
            .sma20
            .then(|| simple_moving_average(self.bars, 20));
        let sma50 = self
            .overlays
            .sma50
            .then(|| simple_moving_average(self.bars, 50));
        let vwap = self.overlays.vwap.then(|| window_vwap(self.bars));
        let Some((low, high)) = price_bounds_with_overlays(
            self.bars,
            [sma20.as_deref(), sma50.as_deref(), vwap.as_deref()],
        ) else {
            return;
        };

        draw_price_grid(buffer, plot, area, low, high);
        match self.style {
            ChartStyle::Candles | ChartStyle::Ohlc => {
                draw_price_bars(buffer, plot, &bars, low, high, self.style);
            }
            ChartStyle::Line => draw_close_line(buffer, plot, &bars, low, high),
        }
        if let Some(series) = sma20.as_deref() {
            draw_indicator_line(
                buffer,
                plot,
                &bars,
                series,
                low,
                high,
                Style::default().fg(Color::Cyan),
                '●',
            );
        }
        if let Some(series) = sma50.as_deref() {
            draw_indicator_line(
                buffer,
                plot,
                &bars,
                series,
                low,
                high,
                Style::default().fg(Color::Magenta),
                '◦',
            );
        }
        if let Some(series) = vwap.as_deref() {
            draw_indicator_line(
                buffer,
                plot,
                &bars,
                series,
                low,
                high,
                Style::default().fg(AMBER),
                '·',
            );
        }
        draw_time_axis(buffer, area, plot, &bars, self.time_axis_valid);
        draw_crosshair(buffer, plot, &bars, self.bars, self.cursor, low, high);
    }
}

struct VolumeChart<'a> {
    bars: &'a [BarView],
    cursor: Option<usize>,
}

impl Widget for VolumeChart<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        if area.width == 0 || area.height == 0 || self.bars.is_empty() {
            return;
        }
        let bars = compress_for_width(self.bars, usize::from(area.width));
        let maximum = bars
            .iter()
            .map(|bar| bar.bar.volume.max(0))
            .max()
            .unwrap_or(0);
        if maximum == 0 {
            write_text(
                buffer,
                area.x,
                area.y,
                usize::from(area.width),
                "NO POSITIVE VOLUME",
                Style::default().fg(Color::DarkGray),
            );
            return;
        }
        for (index, bar) in bars.iter().enumerate() {
            let x = chart_x(index, bars.len(), area);
            let volume = u128::from(u64::try_from(bar.bar.volume.max(0)).unwrap_or(0));
            let scaled = volume
                .saturating_mul(u128::from(area.height))
                .div_ceil(u128::from(u64::try_from(maximum).unwrap_or(u64::MAX)));
            let height = u16::try_from(scaled)
                .unwrap_or(area.height)
                .min(area.height);
            let selected = self
                .cursor
                .is_some_and(|cursor| bar.source_start <= cursor && cursor < bar.source_end);
            let style = Style::default().fg(if selected {
                Color::White
            } else if bar.bar.close >= bar.bar.open {
                GREEN
            } else {
                RED
            });
            for offset in 0..height {
                let y = area.bottom().saturating_sub(1).saturating_sub(offset);
                if let Some(cell) = buffer.cell_mut((x, y)) {
                    cell.set_char('█').set_style(style);
                }
            }
        }
    }
}

fn price_bounds(bars: &[BarView]) -> Option<(i64, i64)> {
    let low = bars.iter().map(|bar| bar.low).min()?;
    let high = bars.iter().map(|bar| bar.high).max()?;
    Some((low, high))
}

fn price_bounds_with_overlays(
    bars: &[BarView],
    overlays: [Option<&[Option<i64>]>; 3],
) -> Option<(i64, i64)> {
    let (mut low, mut high) = price_bounds(bars)?;
    for series in overlays.into_iter().flatten() {
        for value in series.iter().flatten() {
            low = low.min(*value);
            high = high.max(*value);
        }
    }
    Some((low, high))
}

fn price_y(price: i64, low: i64, high: i64, area: Rect) -> u16 {
    if area.height <= 1 || high <= low {
        return area.y.saturating_add(area.height / 2);
    }
    let range = i128::from(high) - i128::from(low);
    let distance = i128::from(high) - i128::from(price.clamp(low, high));
    let rows = i128::from(area.height.saturating_sub(1));
    let offset = u16::try_from(distance.saturating_mul(rows) / range).unwrap_or(0);
    area.y.saturating_add(offset)
}

fn draw_price_grid(buffer: &mut Buffer, plot: Rect, area: Rect, low: i64, high: i64) {
    let horizontal_levels = usize::from(plot.height.min(5));
    for level in 0..horizontal_levels {
        let divisor = horizontal_levels.saturating_sub(1).max(1);
        let y_offset = level.saturating_mul(usize::from(plot.height.saturating_sub(1))) / divisor;
        let y = plot.y.saturating_add(u16::try_from(y_offset).unwrap_or(0));
        for x in plot.x..plot.right() {
            if (x.saturating_sub(plot.x)) % 2 == 0
                && let Some(cell) = buffer.cell_mut((x, y))
            {
                cell.set_char('·')
                    .set_style(Style::default().fg(Color::Rgb(48, 52, 58)));
            }
        }
        if area.width > plot.width {
            let range = i128::from(high) - i128::from(low);
            let price = i128::from(high).saturating_sub(
                range.saturating_mul(i128::try_from(level).unwrap_or(0))
                    / i128::try_from(divisor).unwrap_or(1),
            );
            write_text(
                buffer,
                plot.right().saturating_add(1),
                y,
                usize::from(area.right().saturating_sub(plot.right().saturating_add(1))),
                &i64::try_from(price).unwrap_or_default().to_string(),
                Style::default().fg(Color::Gray),
            );
        }
    }
    if plot.width >= 16 {
        for quarter in 1..4_u16 {
            let x = plot
                .x
                .saturating_add(plot.width.saturating_sub(1).saturating_mul(quarter) / 4);
            for y in plot.y..plot.bottom() {
                if let Some(cell) = buffer.cell_mut((x, y))
                    && cell.symbol() == " "
                {
                    cell.set_char('┊')
                        .set_style(Style::default().fg(Color::Rgb(42, 46, 52)));
                }
            }
        }
    }
}

fn draw_price_bars(
    buffer: &mut Buffer,
    plot: Rect,
    bars: &[chart::RenderBar],
    low: i64,
    high: i64,
    chart_style: ChartStyle,
) {
    for (index, value) in bars.iter().enumerate() {
        let bar = &value.bar;
        let x = chart_x(index, bars.len(), plot);
        let high_y = price_y(bar.high, low, high, plot);
        let low_y = price_y(bar.low, low, high, plot);
        let open_y = price_y(bar.open, low, high, plot);
        let close_y = price_y(bar.close, low, high, plot);
        let style = Style::default().fg(if bar.close >= bar.open { GREEN } else { RED });
        for y in high_y..=low_y {
            if let Some(cell) = buffer.cell_mut((x, y)) {
                cell.set_char('│').set_style(style);
            }
        }
        match chart_style {
            ChartStyle::Candles => {
                for y in open_y.min(close_y)..=open_y.max(close_y) {
                    if let Some(cell) = buffer.cell_mut((x, y)) {
                        cell.set_char(if open_y == close_y { '━' } else { '█' })
                            .set_style(style);
                    }
                }
            }
            ChartStyle::Ohlc => {
                if let Some(cell) = buffer.cell_mut((x, open_y)) {
                    cell.set_char('┤').set_style(style);
                }
                if let Some(cell) = buffer.cell_mut((x, close_y)) {
                    cell.set_char('├').set_style(style);
                }
            }
            ChartStyle::Line => {}
        }
    }
}

fn draw_close_line(
    buffer: &mut Buffer,
    plot: Rect,
    bars: &[chart::RenderBar],
    low: i64,
    high: i64,
) {
    let mut previous = None;
    for (index, value) in bars.iter().enumerate() {
        let point = (
            chart_x(index, bars.len(), plot),
            price_y(value.bar.close, low, high, plot),
        );
        let style = Style::default().fg(if value.bar.close >= value.bar.open {
            GREEN
        } else {
            RED
        });
        if let Some(previous) = previous {
            draw_segment(buffer, previous, point, style, '•');
        }
        if let Some(cell) = buffer.cell_mut(point) {
            cell.set_char('●').set_style(style);
        }
        previous = Some(point);
    }
}

#[allow(clippy::too_many_arguments)]
fn draw_indicator_line(
    buffer: &mut Buffer,
    plot: Rect,
    bars: &[chart::RenderBar],
    series: &[Option<i64>],
    low: i64,
    high: i64,
    style: Style,
    symbol: char,
) {
    let mut previous = None;
    for (index, bar) in bars.iter().enumerate() {
        let Some(value) = bar
            .source_end
            .checked_sub(1)
            .and_then(|source| series.get(source))
            .copied()
            .flatten()
        else {
            previous = None;
            continue;
        };
        let point = (
            chart_x(index, bars.len(), plot),
            price_y(value, low, high, plot),
        );
        if let Some(previous) = previous {
            draw_segment(buffer, previous, point, style, symbol);
        }
        if let Some(cell) = buffer.cell_mut(point) {
            cell.set_char(symbol).set_style(style);
        }
        previous = Some(point);
    }
}

fn draw_segment(buffer: &mut Buffer, from: (u16, u16), to: (u16, u16), style: Style, symbol: char) {
    let width = to.0.saturating_sub(from.0);
    if width == 0 {
        return;
    }
    let from_y = i32::from(from.1);
    let delta_y = i32::from(to.1) - from_y;
    for offset in 0..=width {
        let y = from_y + delta_y.saturating_mul(i32::from(offset)) / i32::from(width.max(1));
        if let Ok(y) = u16::try_from(y)
            && let Some(cell) = buffer.cell_mut((from.0.saturating_add(offset), y))
        {
            cell.set_char(symbol).set_style(style);
        }
    }
}

fn draw_crosshair(
    buffer: &mut Buffer,
    plot: Rect,
    bars: &[chart::RenderBar],
    source: &[BarView],
    cursor: Option<usize>,
    low: i64,
    high: i64,
) {
    let Some(cursor) = cursor.filter(|cursor| *cursor < source.len()) else {
        return;
    };
    let Some((display_index, _)) = bars
        .iter()
        .enumerate()
        .find(|(_, bar)| bar.source_start <= cursor && cursor < bar.source_end)
    else {
        return;
    };
    let x = chart_x(display_index, bars.len(), plot);
    let y = price_y(source[cursor].close, low, high, plot);
    let style = Style::default()
        .fg(Color::White)
        .add_modifier(Modifier::BOLD);
    for row in plot.y..plot.bottom() {
        if let Some(cell) = buffer.cell_mut((x, row)) {
            cell.set_char('┊').set_style(style);
        }
    }
    for column in plot.x..plot.right() {
        if let Some(cell) = buffer.cell_mut((column, y)) {
            cell.set_char('┈').set_style(style);
        }
    }
    if let Some(cell) = buffer.cell_mut((x, y)) {
        cell.set_char('┼').set_style(style);
    }
}

fn draw_time_axis(
    buffer: &mut Buffer,
    area: Rect,
    plot: Rect,
    bars: &[chart::RenderBar],
    valid: bool,
) {
    if area.height == plot.height || bars.is_empty() {
        return;
    }
    let y = area.bottom().saturating_sub(1);
    if !valid {
        write_text(
            buffer,
            plot.x,
            y,
            usize::from(plot.width),
            "! NON-MONOTONIC SOURCE TIME",
            Style::default().fg(RED).add_modifier(Modifier::BOLD),
        );
        return;
    }
    let indices: Vec<usize> = if plot.width >= 42 && bars.len() >= 3 {
        vec![0, bars.len() / 2, bars.len() - 1]
    } else if plot.width >= 20 && bars.len() >= 2 {
        vec![0, bars.len() - 1]
    } else {
        vec![bars.len() - 1]
    };
    for index in indices {
        let text = format_axis_time(bars[index].bar.start_time_ns, bars[index].bar.interval_ns);
        let center = chart_x(index, bars.len(), plot);
        let half = u16::try_from(text.len() / 2).unwrap_or(0);
        let maximum_x = plot
            .right()
            .saturating_sub(u16::try_from(text.len()).unwrap_or(plot.width));
        let x = center
            .saturating_sub(half)
            .clamp(plot.x, maximum_x.max(plot.x));
        write_text(
            buffer,
            x,
            y,
            text.len().min(usize::from(plot.right().saturating_sub(x))),
            &text,
            Style::default().fg(Color::Gray),
        );
    }
}

fn chart_x(index: usize, length: usize, area: Rect) -> u16 {
    if length <= 1 || area.width <= 1 {
        return area.x.saturating_add(area.width.saturating_sub(1) / 2);
    }
    let offset =
        index.saturating_mul(usize::from(area.width.saturating_sub(1))) / length.saturating_sub(1);
    area.x.saturating_add(u16::try_from(offset).unwrap_or(0))
}

fn write_text(buffer: &mut Buffer, x: u16, y: u16, maximum: usize, text: &str, style: Style) {
    for (offset, character) in text.chars().take(maximum).enumerate() {
        let Some(column) = u16::try_from(offset)
            .ok()
            .and_then(|offset| x.checked_add(offset))
        else {
            break;
        };
        if let Some(cell) = buffer.cell_mut((column, y)) {
            cell.set_char(character).set_style(style);
        }
    }
}

struct Panel<W> {
    title: String,
    widget: W,
}
impl<W: Widget> Widget for Panel<W> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        let block = Block::default()
            .title(format!(" {} ", self.title))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .style(Style::default().bg(PANEL));
        let inner = block.inner(area);
        block.render(area, buffer);
        self.widget.render(inner, buffer);
    }
}
fn panel<W: Widget>(title: &str, widget: W) -> Panel<W> {
    Panel {
        title: title.into(),
        widget,
    }
}
fn header<const N: usize>(values: [&str; N]) -> Row<'static> {
    Row::new(values.map(|value| Cell::from(value.to_owned()))).style(
        Style::default()
            .fg(Color::Black)
            .bg(ORANGE)
            .add_modifier(Modifier::BOLD),
    )
}
fn amber() -> Style {
    Style::default().fg(AMBER).add_modifier(Modifier::BOLD)
}
fn theme_accent(theme: &str) -> Color {
    match theme {
        "BLUE" => Color::Rgb(70, 150, 255),
        "GREEN" => Color::Rgb(50, 200, 130),
        "GRAY" | "MONO" => Color::Rgb(180, 185, 190),
        _ => ORANGE,
    }
}
fn key() -> Style {
    Style::default()
        .fg(Color::Black)
        .bg(Color::Rgb(150, 155, 160))
        .add_modifier(Modifier::BOLD)
}
fn format_optional(value: Option<i64>) -> String {
    value.map_or_else(|| "—".into(), |value| value.to_string())
}
fn format_duration(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!(
            "{}.{:01}s",
            ns / 1_000_000_000,
            ns % 1_000_000_000 / 100_000_000
        )
    } else if ns >= 1_000_000 {
        format!("{}.{:01}ms", ns / 1_000_000, ns % 1_000_000 / 100_000)
    } else if ns >= 1_000 {
        format!("{}.{:01}µs", ns / 1_000, ns % 1_000 / 100)
    } else {
        format!("{ns}ns")
    }
}
fn format_bps(value: i64) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.unsigned_abs();
    format!("{sign}{}.{:02}%", magnitude / 100, magnitude % 100)
}
fn elapsed_ns(start: Option<u64>, end: Option<u64>) -> String {
    match (start, end) {
        (Some(start), Some(end)) if end >= start => format_duration(end - start),
        _ => "—".into(),
    }
}
fn optional_number(value: Option<i64>) -> String {
    value.map_or_else(|| "—".into(), |value| value.to_string())
}
fn optional_wide(value: Option<i128>) -> String {
    value.map_or_else(|| "—".into(), |value| value.to_string())
}
fn format_score(value: f64) -> String {
    format!("{value:.4}")
}
fn severity(value: u8) -> &'static str {
    match value {
        1 => "INFO",
        2 => "WARNING",
        3 => "CRITICAL",
        _ => "UNKNOWN",
    }
}
fn argument(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

#[allow(clippy::too_many_lines)]
fn print_plain(app: &App) {
    println!("function={:?} cursor={}", app.page, app.runtime.cursor);
    match app.page {
        Page::Home => println!(
            "account={} risk={} mode={} positions={} orders={} proposals={} markets={}",
            app.runtime.account_id,
            app.runtime.risk,
            app.runtime.mode,
            app.runtime.positions.len(),
            app.runtime.orders.len(),
            app.runtime.proposals.len(),
            app.runtime.markets.len()
        ),
        Page::Market => {
            for market in app.runtime.markets.iter().take(256) {
                println!(
                    "instrument={} bid={:?} ask={:?} last={:?} quote={} trade={} bars={}",
                    market.instrument,
                    market.bid,
                    market.ask,
                    market.last,
                    market.quote_quality,
                    market.trade_quality,
                    market.bars.len()
                );
            }
        }
        Page::Chart => print_chart_plain(app),
        Page::Screener => print_screener_plain(app),
        Page::Depth => {
            for market in app.runtime.markets.iter().take(256) {
                println!(
                    "instrument={} top={:?} quote={} trade={}",
                    market.instrument, market.book_top, market.quote_quality, market.trade_quality
                );
            }
        }
        Page::Tape => {
            for market in app.runtime.markets.iter().take(256) {
                println!(
                    "instrument={} trades={}",
                    market.instrument,
                    market.trades.len()
                );
                for trade in market.trades.iter().rev().take(200).rev() {
                    println!(
                        "  seq={} exchange_ns={} received_ns={} price={} quantity={}",
                        trade.sequence,
                        trade.exchange_time_ns,
                        trade.received_mono_ns,
                        trade.price,
                        trade.quantity
                    );
                }
            }
        }
        Page::Portfolio => println!("{:#?}", app.runtime.positions),
        Page::Orders => println!("{:#?}", app.runtime.orders),
        Page::Tca => println!("{:#?}", app.runtime.tca),
        Page::Strategies => println!("{:#?}", app.strategies),
        Page::Metrics => println!("{:#?}", app.metrics),
        Page::News => {
            if let Some(detail) = &app.news_detail {
                println!("{detail:#?}");
            } else {
                println!(
                    "scope={} symbol={} page={} next={:?}\n{:#?}",
                    app.news_scope,
                    app.selected_symbol,
                    app.news_cursor_history.len().saturating_add(1),
                    app.news_next_cursor,
                    app.news
                );
            }
        }
        Page::Risk => println!(
            "risk={} utilization_bps={}",
            app.runtime.risk, app.runtime.gross_utilization_bps
        ),
        Page::Autonomy => println!(
            "mode={} plan={:?}\n{:#?}",
            app.runtime.mode, app.runtime.plan, app.runtime.proposals
        ),
        Page::Alerts => println!("{:#?}", app.alerts),
        Page::Health => println!("{}", app.health_lines.join("\n")),
        Page::Trace => println!("{:#?}", app.trace),
        Page::Search => println!("{:#?}", app.context_hits),
        Page::Analyst => println!("{:#?}", app.analyst),
        Page::LlmControl => println!(
            "provider={:?} model={:?} mode={}",
            app.runtime.llm_provider_id, app.runtime.llm_model, app.runtime.mode
        ),
        Page::Backtests => println!("{:#?}", app.backtests),
        Page::Models => println!("{:#?}", app.models),
        Page::Attribution => println!(
            "resolutions={:#?}\nexecution={:#?}",
            app.resolutions, app.strategy_execution
        ),
        Page::Experiments => println!("{:#?}", app.experiments),
        Page::Help => {
            println!("Use --command HELP in the interactive terminal for the function directory.");
        }
    }
}

fn print_chart_plain(app: &App) {
    let bars = app
        .selected_instrument
        .and_then(|identity| {
            app.runtime
                .markets
                .iter()
                .find(|market| market.instrument == identity)
        })
        .map_or(0, |market| market.bars.len());
    println!(
        "instrument={:?} window={} offset={} interval={} style={} overlays={} available_bars={bars}",
        app.selected_instrument,
        app.chart_window,
        app.chart_offset,
        app.chart_interval.name(),
        app.chart_style.name(),
        app.chart_overlays.legend()
    );
}

fn print_screener_plain(app: &App) {
    println!(
        "mode={} matches={}/{}\n{:#?}",
        app.screener_mode.name(),
        app.screener_rows.len(),
        app.runtime.markets.len(),
        app.screener_rows
    );
}

#[cfg(test)]
mod chart_tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use ratatui::backend::TestBackend;

    use super::*;

    #[test]
    fn price_styles_overlays_and_volume_render_into_bounded_areas() {
        let bars = (0..60)
            .map(|index| {
                bar(
                    index,
                    100 + index,
                    104 + index,
                    98 + index,
                    102 + index,
                    100 + index,
                )
            })
            .collect::<Vec<_>>();
        let area = Rect::new(0, 0, 48, 12);
        for chart_style in [ChartStyle::Candles, ChartStyle::Ohlc, ChartStyle::Line] {
            let mut price_buffer = Buffer::empty(area);
            PriceChart {
                bars: &bars,
                style: chart_style,
                overlays: ChartOverlays {
                    sma20: true,
                    sma50: true,
                    vwap: true,
                },
                cursor: Some(30),
                time_axis_valid: true,
            }
            .render(area, &mut price_buffer);
            assert!(
                price_buffer
                    .content()
                    .iter()
                    .any(|cell| cell.symbol() != " ")
            );
        }

        let mut volume_buffer = Buffer::empty(area);
        VolumeChart {
            bars: &bars,
            cursor: Some(30),
        }
        .render(area, &mut volume_buffer);
        assert!(
            volume_buffer
                .content()
                .iter()
                .any(|cell| cell.symbol() == "█")
        );
    }

    #[test]
    fn tiny_areas_and_absent_volume_are_explicit_and_do_not_overflow() {
        let bars = [bar(0, 10, 10, 10, 10, 0)];
        let tiny = Rect::new(0, 0, 1, 1);
        let mut price = Buffer::empty(tiny);
        PriceChart {
            bars: &bars,
            style: ChartStyle::Candles,
            overlays: ChartOverlays::default(),
            cursor: Some(0),
            time_axis_valid: true,
        }
        .render(tiny, &mut price);

        let area = Rect::new(0, 0, 20, 2);
        let mut volume = Buffer::empty(area);
        VolumeChart {
            bars: &bars,
            cursor: Some(0),
        }
        .render(area, &mut volume);
        let text = buffer_text(&volume);
        assert!(text.contains("NO POSITIVE VOLUME"));
    }

    #[test]
    fn authoritative_time_axis_detects_disorder_and_formats_utc() {
        let regular = [bar(0, 10, 11, 9, 10, 1), bar(1, 10, 11, 9, 10, 1)];
        let timing = chart_timing(&regular, 5);
        assert!(timing.axis_valid);
        assert_eq!(timing.label, "5 x 1m = 5m");
        assert_eq!(format_full_utc_time(0), "1970-01-01 00:00:00Z");

        let mut disordered = regular;
        disordered[1].start_time_ns = disordered[0].start_time_ns;
        let timing = chart_timing(&disordered, 5);
        assert!(!timing.axis_valid);
        assert!(timing.label.contains("NON-MONOTONIC"));
    }

    #[test]
    fn full_chart_draws_with_test_backend_and_survives_compact_resize() {
        let Ok(client) = EngineClient::connect(PathBuf::from(format!(
            "/tmp/insider-terminal-chart-test-{}.sock",
            std::process::id()
        ))) else {
            return;
        };
        let mut app = App::new(client, Duration::from_secs(1));
        app.page = Page::Chart;
        app.runtime_connected = true;
        app.selected_instrument = Some(7);
        app.selected_symbol = "AAPL".into();
        app.runtime.markets.push(model::MarketView {
            instrument: 7,
            bid: Some(15_000),
            ask: Some(15_002),
            last: Some(15_001),
            quote_quality: "GOOD".into(),
            trade_quality: "GOOD".into(),
            book_top: None,
            trades: Vec::new(),
            bars: (0..120)
                .map(|index| {
                    bar(
                        index,
                        15_000 + index,
                        15_010 + index,
                        14_990 + index,
                        15_005 + index,
                        1_000 + index,
                    )
                })
                .collect(),
        });
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).expect("test backend initializes");
        assert!(terminal.draw(|frame| draw(frame, &app)).is_ok());
        let text = buffer_text(terminal.backend().buffer());
        assert!(text.contains("CANDLE"));
        assert!(text.contains("SMA20,VWAP"));
        assert!(text.contains("XHAIR"));

        let mut compact = Terminal::new(TestBackend::new(22, 9)).expect("test backend initializes");
        assert!(compact.draw(|frame| draw(frame, &app)).is_ok());
    }

    #[test]
    fn flat_price_maps_to_middle_row() {
        let area = Rect::new(4, 10, 20, 7);
        assert_eq!(price_y(50, 50, 50, area), 13);
    }

    #[test]
    fn command_window_keeps_the_cursor_visible_with_unicode_width() {
        let (before, after) = command_window("ANALYZE abcdefghijklmnop", 24, 10);
        assert!(before.starts_with('‹'));
        assert!(after.is_empty());
        assert!(display_width(&before) <= 10);

        let command = "ANALYZE 分析abcdefgh";
        let cursor = "ANALYZE 分析".len();
        let (before, after) = command_window(command, cursor, 10);
        assert!(before.starts_with('‹'));
        assert!(after.ends_with('›'));
        assert!(display_width(&before) + display_width(&after) <= 10);
    }

    fn buffer_text(buffer: &Buffer) -> String {
        buffer
            .content()
            .iter()
            .map(ratatui::buffer::Cell::symbol)
            .collect::<String>()
    }

    fn bar(index: i64, open: i64, high: i64, low: i64, close: i64, volume: i64) -> BarView {
        BarView {
            start_time_ns: index.saturating_mul(60_000_000_000),
            interval_ns: 60_000_000_000,
            open,
            high,
            low,
            close,
            volume,
        }
    }
}
