//! Optional loopback-only browser chart workspace.
//!
//! This module owns presentation state only. It receives bounded snapshots from
//! the native terminal and returns a small allowlist of read-only coordination
//! commands. It never connects to a broker or interprets order commands.

use std::fmt::Write as _;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::app::App;
use crate::chart::{aggregate_interval, simple_moving_average, window_vwap};
use crate::model::BarView;

const MAX_HTTP_BYTES: usize = 16_384;
const MAX_BROWSER_CLIENTS: usize = 8;
const MAX_COMMAND_BYTES: usize = 1_024;

/// Immutable, bounded chart projection sent to the browser.
pub(crate) struct BrowserChartSnapshot {
    json: String,
}

impl BrowserChartSnapshot {
    pub(crate) fn from_app(app: &App) -> Self {
        let market = app.selected_instrument.and_then(|identity| {
            app.runtime
                .markets
                .iter()
                .find(|candidate| candidate.instrument == identity)
        });
        let source = market.map_or(&[][..], |market| {
            let end = market.bars.len().saturating_sub(app.chart_offset);
            let start = end.saturating_sub(app.chart_window);
            market.bars.get(start..end).unwrap_or_default()
        });
        let bars = aggregate_interval(source, app.chart_interval);
        let sma20 = simple_moving_average(&bars, 20);
        let sma50 = simple_moving_average(&bars, 50);
        let vwap = window_vwap(&bars);
        let instrument = market.map_or(0, |value| value.instrument);
        let position = app
            .runtime
            .positions
            .iter()
            .find(|value| value.instrument == instrument)
            .map_or(0, |value| value.quantity);
        let orders = app
            .runtime
            .orders
            .iter()
            .filter(|value| value.instrument == instrument)
            .count();
        let proposals = app
            .runtime
            .proposals
            .iter()
            .filter(|value| value.instrument == instrument)
            .count();

        let mut json = String::with_capacity(bars.len().saturating_mul(160).saturating_add(512));
        json.push('{');
        json_field_string(&mut json, "symbol", symbol(app, instrument));
        json.push(',');
        json_field_u128(&mut json, "instrument", instrument);
        json.push(',');
        json_field_string(&mut json, "interval", app.chart_interval.name());
        json.push(',');
        json_field_string(&mut json, "style", app.chart_style.name());
        json.push(',');
        json_field_string(&mut json, "overlays", &app.chart_overlays.legend());
        json.push(',');
        json_field_string(&mut json, "risk", &app.runtime.risk);
        json.push(',');
        json_field_string(&mut json, "mode", &app.runtime.mode);
        json.push(',');
        json_field_string(&mut json, "status", &app.status);
        if let Some(market) = market {
            json.push(',');
            json_field_optional_i64(&mut json, "bid", market.bid);
            json.push(',');
            json_field_optional_i64(&mut json, "ask", market.ask);
            json.push(',');
            json_field_optional_i64(&mut json, "last", market.last);
        }
        if let Some(alert) = app.alerts.first() {
            json.push(',');
            json_field_string(&mut json, "alert_id", &alert.id);
            json.push(',');
            json.push_str("\"alert_severity\":");
            json.push_str(&alert.severity.to_string());
            json.push(',');
            json_field_string(&mut json, "alert_message", &alert.message);
        }
        let _ = write!(
            json,
            ",\"connected\":{},\"cursor\":{},\"position\":{},\"orders\":{},\"proposals\":{},\"bars\":[",
            app.runtime_connected, app.runtime.cursor, position, orders, proposals
        );
        for (index, bar) in bars.iter().enumerate() {
            if index != 0 {
                json.push(',');
            }
            push_bar(&mut json, bar, sma20[index], sma50[index], vwap[index]);
        }
        json.push_str("]}");
        Self { json }
    }
}

fn symbol(app: &App, instrument: u128) -> &str {
    if app.selected_symbol.is_empty() {
        if instrument == 0 {
            "NO MARKET"
        } else {
            "CANONICAL"
        }
    } else {
        &app.selected_symbol
    }
}

fn push_bar(
    output: &mut String,
    bar: &BarView,
    sma20: Option<i64>,
    sma50: Option<i64>,
    vwap: Option<i64>,
) {
    let _ = write!(
        output,
        "{{\"t\":{},\"dt\":{},\"o\":{},\"h\":{},\"l\":{},\"c\":{},\"v\":{},\"s20\":{},\"s50\":{},\"vw\":{}}}",
        bar.start_time_ns,
        bar.interval_ns,
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.volume,
        optional_i64(sma20),
        optional_i64(sma50),
        optional_i64(vwap)
    );
}

fn optional_i64(value: Option<i64>) -> String {
    value.map_or_else(|| "null".into(), |value| value.to_string())
}

fn json_field_string(output: &mut String, name: &str, value: &str) {
    output.push('"');
    output.push_str(name);
    output.push_str("\":\"");
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            value if value.is_control() => {
                let _ = write!(output, "\\u{:04x}", u32::from(value));
            }
            value => output.push(value),
        }
    }
    output.push('"');
}

fn json_field_u128(output: &mut String, name: &str, value: u128) {
    output.push('"');
    output.push_str(name);
    // Use a string because JavaScript numbers cannot exactly represent u128 IDs.
    output.push_str("\":\"");
    output.push_str(&value.to_string());
    output.push('"');
}

fn json_field_optional_i64(output: &mut String, name: &str, value: Option<i64>) {
    output.push('"');
    output.push_str(name);
    output.push_str("\":");
    output.push_str(&optional_i64(value));
}

/// Running local chart server and its bounded asynchronous command channel.
pub(crate) struct BrowserChartWorkspace {
    url: String,
    latest: Arc<Mutex<String>>,
    commands: Receiver<String>,
    stop: Arc<AtomicBool>,
    server: Option<JoinHandle<()>>,
}

impl BrowserChartWorkspace {
    pub(crate) fn start(initial: BrowserChartSnapshot) -> Result<Self, String> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .map_err(|error| format!("bind local browser chart: {error}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("configure local browser chart: {error}"))?;
        let address = listener
            .local_addr()
            .map_err(|error| format!("read local browser chart address: {error}"))?;
        let token = local_token(address.port());
        let url = format!("http://127.0.0.1:{}/?token={token}", address.port());
        let latest = Arc::new(Mutex::new(initial.json));
        let stop = Arc::new(AtomicBool::new(false));
        let clients = Arc::new(AtomicUsize::new(0));
        let (command_sender, commands) = mpsc::sync_channel(32);
        let server_latest = Arc::clone(&latest);
        let server_stop = Arc::clone(&stop);
        let server = thread::Builder::new()
            .name("insider-browser-chart".into())
            .spawn(move || {
                serve(
                    &listener,
                    &token,
                    &server_latest,
                    &command_sender,
                    &server_stop,
                    &clients,
                );
            })
            .map_err(|error| format!("start local browser chart: {error}"))?;
        Ok(Self {
            url,
            latest,
            commands,
            stop,
            server: Some(server),
        })
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn publish(&self, snapshot: BrowserChartSnapshot) {
        if let Ok(mut latest) = self.latest.try_lock()
            && *latest != snapshot.json
        {
            *latest = snapshot.json;
        }
    }

    pub(crate) fn try_command(&self) -> Option<String> {
        self.commands.try_recv().ok()
    }

    pub(crate) fn open_browser(&self) -> Result<(), String> {
        #[cfg(target_os = "macos")]
        let mut command = Command::new("open");
        #[cfg(target_os = "windows")]
        let mut command = {
            let mut command = Command::new("cmd");
            command.args(["/C", "start", ""]);
            command
        };
        #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
        let mut command = Command::new("xdg-open");
        command
            .arg(&self.url)
            .spawn()
            .map(|_| ())
            .map_err(|error| format!("launch system browser: {error}"))
    }
}

impl Drop for BrowserChartWorkspace {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

fn serve(
    listener: &TcpListener,
    token: &str,
    latest: &Arc<Mutex<String>>,
    commands: &SyncSender<String>,
    stop: &Arc<AtomicBool>,
    clients: &Arc<AtomicUsize>,
) {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                if clients.fetch_add(1, Ordering::AcqRel) >= MAX_BROWSER_CLIENTS {
                    clients.fetch_sub(1, Ordering::AcqRel);
                    let _ = reject_busy(stream);
                    continue;
                }
                let handler_token = token.to_owned();
                let handler_latest = Arc::clone(latest);
                let handler_commands = commands.clone();
                let handler_stop = Arc::clone(stop);
                let handler_clients = Arc::clone(clients);
                let spawned = thread::Builder::new()
                    .name("insider-browser-client".into())
                    .spawn(move || {
                        let _guard = ClientGuard(handler_clients);
                        let _ = handle_client(
                            stream,
                            &handler_token,
                            &handler_latest,
                            &handler_commands,
                            &handler_stop,
                        );
                    });
                if spawned.is_err() {
                    clients.fetch_sub(1, Ordering::AcqRel);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(_) => break,
        }
    }
}

struct ClientGuard(Arc<AtomicUsize>);

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn handle_client(
    mut stream: TcpStream,
    token: &str,
    latest: &Arc<Mutex<String>>,
    commands: &SyncSender<String>,
    stop: &AtomicBool,
) -> Result<(), String> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| format!("browser read timeout: {error}"))?;
    let request = read_request(&mut stream)?;
    let expected_token = format!("token={token}");
    if request
        .target
        .split_once('?')
        .is_none_or(|(_, query)| !query.split('&').any(|value| value == expected_token))
    {
        return response(&mut stream, "403 Forbidden", "text/plain", b"forbidden");
    }
    if request.method == "GET" && request.target.starts_with("/?") {
        let page = PAGE.replace("__TOKEN__", token);
        return response(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            page.as_bytes(),
        );
    }
    if request.method == "GET" && request.target.starts_with("/events?") {
        return stream_events(&mut stream, latest, stop);
    }
    if request.method == "POST" && request.target.starts_with("/command?") {
        let command = std::str::from_utf8(&request.body)
            .map_err(|_| "browser command is not UTF-8".to_owned())?
            .trim();
        if !allowed_browser_command(command) {
            return response(
                &mut stream,
                "403 Forbidden",
                "text/plain",
                b"read-only chart commands only",
            );
        }
        match commands.try_send(command.to_owned()) {
            Ok(()) => response(&mut stream, "202 Accepted", "text/plain", b"queued"),
            Err(TrySendError::Full(_)) => response(
                &mut stream,
                "429 Too Many Requests",
                "text/plain",
                b"command queue full",
            ),
            Err(TrySendError::Disconnected(_)) => response(
                &mut stream,
                "503 Service Unavailable",
                "text/plain",
                b"terminal unavailable",
            ),
        }
    } else {
        response(&mut stream, "404 Not Found", "text/plain", b"not found")
    }
}

struct HttpRequest {
    method: String,
    target: String,
    body: Vec<u8>,
}

fn read_request(stream: &mut TcpStream) -> Result<HttpRequest, String> {
    let mut bytes = Vec::with_capacity(1_024);
    let mut buffer = [0_u8; 1_024];
    let header_end = loop {
        let read = stream
            .read(&mut buffer)
            .map_err(|error| format!("read browser request: {error}"))?;
        if read == 0 {
            return Err("browser closed an incomplete request".into());
        }
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.len() > MAX_HTTP_BYTES {
            return Err("browser request exceeds bound".into());
        }
        if let Some(position) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let header = std::str::from_utf8(&bytes[..header_end])
        .map_err(|_| "browser request headers are not UTF-8".to_owned())?;
    let mut lines = header.lines();
    let mut request_line = lines.next().unwrap_or_default().split_whitespace();
    let method = request_line.next().unwrap_or_default().to_owned();
    let target = request_line.next().unwrap_or_default().to_owned();
    let content_length = lines
        .find_map(|line| {
            line.split_once(':').and_then(|(name, value)| {
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
        })
        .unwrap_or(0);
    if content_length > MAX_COMMAND_BYTES
        || header_end.saturating_add(content_length) > MAX_HTTP_BYTES
    {
        return Err("browser request body exceeds bound".into());
    }
    while bytes.len().saturating_sub(header_end) < content_length {
        let read = stream
            .read(&mut buffer)
            .map_err(|error| format!("read browser request body: {error}"))?;
        if read == 0 {
            return Err("browser closed an incomplete request body".into());
        }
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.len() > MAX_HTTP_BYTES {
            return Err("browser request exceeds bound".into());
        }
    }
    Ok(HttpRequest {
        method,
        target,
        body: bytes[header_end..header_end + content_length].to_vec(),
    })
}

fn response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> Result<(), String> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nContent-Security-Policy: default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'none'; frame-src 'none'\r\nReferrer-Policy: no-referrer\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(header.as_bytes())
        .and_then(|()| stream.write_all(body))
        .map_err(|error| format!("write browser response: {error}"))
}

fn reject_busy(mut stream: TcpStream) -> Result<(), String> {
    response(
        &mut stream,
        "503 Service Unavailable",
        "text/plain",
        b"browser client limit reached",
    )
}

fn stream_events(
    stream: &mut TcpStream,
    latest: &Arc<Mutex<String>>,
    stop: &AtomicBool,
) -> Result<(), String> {
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| format!("browser write timeout: {error}"))?;
    stream
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nX-Accel-Buffering: no\r\nConnection: keep-alive\r\n\r\n")
        .map_err(|error| format!("start browser event stream: {error}"))?;
    let mut previous = String::new();
    let mut heartbeat = 0_u8;
    while !stop.load(Ordering::Acquire) {
        let current = latest.lock().map(|value| value.clone()).unwrap_or_default();
        if current == previous {
            heartbeat = heartbeat.saturating_add(1);
            if heartbeat >= 50 {
                stream
                    .write_all(b": keepalive\n\n")
                    .map_err(|error| format!("stream browser heartbeat: {error}"))?;
                heartbeat = 0;
            }
        } else {
            stream
                .write_all(format!("event: snapshot\ndata: {current}\n\n").as_bytes())
                .map_err(|error| format!("stream browser snapshot: {error}"))?;
            previous = current;
            heartbeat = 0;
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

fn allowed_browser_command(command: &str) -> bool {
    if command.is_empty()
        || command.len() > MAX_COMMAND_BYTES
        || command.chars().any(char::is_control)
    {
        return false;
    }
    let function = command
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    matches!(
        function.as_str(),
        "CHART"
            | "GP"
            | "ZOOM"
            | "INTERVAL"
            | "TIMEFRAME"
            | "TF"
            | "AGG"
            | "STYLE"
            | "CHARTSTYLE"
            | "OVERLAY"
            | "PAN"
            | "CHARTRESET"
            | "REFRESH"
    )
}

fn local_token(port: u16) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::process::id().hash(&mut hasher);
    port.hash(&mut hasher);
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

const PAGE: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>InsiderTrader Local Chart</title>
<style>
:root{color-scheme:dark;--bg:#0d0f12;--panel:#13161b;--line:#2a2e37;--muted:#8b92a3;--text:#d9dde7;--orange:#ff8c00;--amber:#ffbe3c;--green:#26a69a;--red:#ef5350}
*{box-sizing:border-box}html,body{height:100%;margin:0;background:var(--bg);color:var(--text);font:13px ui-monospace,SFMono-Regular,Menlo,monospace;overflow:hidden}
#app{height:100%;display:grid;grid-template-rows:44px 36px 1fr 25px}.top,.tools{display:flex;align-items:center;border-bottom:1px solid var(--line);background:#101318;padding:0 10px;gap:8px}.tools{background:#0f1217;overflow:auto}.brand{font-weight:800;color:#050505;background:var(--orange);padding:7px 10px}.symbol{font:700 16px system-ui;color:#fff}.pill{border:1px solid var(--line);padding:5px 8px;color:var(--muted)}button{background:#171b22;color:var(--text);border:1px solid var(--line);padding:5px 8px;border-radius:3px;cursor:pointer;white-space:nowrap}button:hover,button.on{border-color:var(--orange);color:var(--amber)}.spacer{flex:1}.group{display:flex;align-items:center;gap:5px;padding-right:10px;border-right:1px solid var(--line)}.group label{color:var(--muted);font-size:11px}.body{min-height:0;display:grid;grid-template-columns:minmax(420px,1fr) 330px}.chart{position:relative;min-width:0;border-right:1px solid var(--line)}canvas{display:block;width:100%;height:100%}.legend{position:absolute;left:12px;top:10px;pointer-events:none;color:var(--muted);line-height:1.7}.legend b{color:var(--text)}.side{min-width:0;display:grid;grid-template-rows:auto 1fr auto;background:var(--panel)}.side h2{font:700 12px system-ui;letter-spacing:.08em;margin:0;padding:12px;border-bottom:1px solid var(--line);color:var(--amber)}#log{padding:12px;overflow:auto;white-space:pre-wrap;line-height:1.55}.hint{color:var(--muted)}form{display:flex;border-top:1px solid var(--line);padding:9px;gap:6px}input{min-width:0;flex:1;background:#090b0e;color:var(--text);border:1px solid var(--line);padding:8px;font:inherit;outline:none}input:focus{border-color:var(--orange)}.foot{display:flex;align-items:center;padding:0 10px;background:#090b0e;border-top:1px solid var(--line);color:var(--muted);gap:18px}.good{color:var(--green)}.bad{color:var(--red)}
@media(max-width:850px){#app{grid-template-rows:44px 36px 1fr 25px}.body{grid-template-columns:1fr;grid-template-rows:minmax(300px,2fr) minmax(180px,1fr)}.chart{border-right:0;border-bottom:1px solid var(--line)}.top .pill:nth-of-type(n+3){display:none}}
</style></head><body><div id="app">
<div class="top"><span class="brand">INSIDERTRADER</span><span id="symbol" class="symbol">—</span><span id="interval" class="pill">1x</span><span id="quote" class="pill">BID — · ASK — · LAST —</span><span class="spacer"></span><button data-cmd="CHARTRESET">Reset</button></div>
<div class="tools"><span class="group"><label>INTERVAL</label><button data-cmd="INTERVAL 1">1m</button><button data-cmd="INTERVAL 5">5m</button><button data-cmd="INTERVAL 15">15m</button><button data-cmd="INTERVAL 30">30m</button><button data-cmd="INTERVAL 60">60m</button></span><span class="group"><label>STYLE</label><button data-cmd="STYLE CANDLE">Candle</button><button data-cmd="STYLE OHLC">OHLC</button><button data-cmd="STYLE LINE">Line</button></span><span class="group"><label>OVERLAYS</label><button data-cmd="OVERLAY SMA20 TOGGLE">SMA20</button><button data-cmd="OVERLAY SMA50 TOGGLE">SMA50</button><button data-cmd="OVERLAY VWAP TOGGLE">VWAP</button></span><span class="group"><label>DRAW</label><button data-tool="cursor">Pointer</button><button data-tool="trend">Trend</button><button data-tool="fib">Fib</button><button data-tool="box">Box</button><button data-tool="hline">Horizontal</button><button data-tool="erase">Erase</button><button data-tool="clear">Clear</button></span><span class="group"><label>WINDOW</label><button data-cmd="ZOOM 30">30</button><button data-cmd="ZOOM 120">120</button><button data-cmd="ZOOM 240">240</button><button data-cmd="ZOOM 960">960</button></span></div>
<div class="body"><section class="chart"><canvas id="chart"></canvas><div id="legend" class="legend"></div></section>
<aside class="side"><h2>COORDINATION TERMINAL · READ ONLY</h2><div id="log"><span class="hint">Local asynchronous chart controls only. Try:
CHART AAPL
INTERVAL 5
ZOOM 240
OVERLAY SMA20 ON
PAN OLDER 10

Trading and risk mutations remain in the authenticated native terminal.</span></div>
<form id="form"><input id="command" maxlength="1024" autocomplete="off" spellcheck="false" placeholder="chart command"><button>GO</button></form></aside></div>
<div class="foot"><span id="connection">CONNECTING</span><span id="risk">RISK —</span><span id="mode">MODE —</span><span id="cursor">CURSOR —</span><span>LOCAL PRESENTATION ONLY</span></div></div>
<script>
const token='__TOKEN__',canvas=document.querySelector('#chart'),ctx=canvas.getContext('2d'),log=document.querySelector('#log');let state=null,mouse=null,tool='cursor',drawing=null,pan=null,drawings=[];
try{drawings=JSON.parse(localStorage.getItem('insidertrader.chart.drawings')||'[]').slice(-64)}catch(_){drawings=[]}
function send(cmd){fetch('/command?token='+token,{method:'POST',headers:{'Content-Type':'text/plain'},body:cmd}).then(async r=>{const t=await r.text();line((r.ok?'› ':'! ')+cmd+(r.ok?'':' — '+t));}).catch(e=>line('! '+e));}
function line(t){const d=document.createElement('div');d.textContent=t;log.append(d);log.scrollTop=log.scrollHeight;}
document.querySelectorAll('[data-cmd]').forEach(b=>b.onclick=()=>send(b.dataset.cmd));document.querySelector('#form').onsubmit=e=>{e.preventDefault();const i=document.querySelector('#command'),v=i.value.trim();if(v){send(v);i.value='';}};
document.querySelectorAll('[data-tool]').forEach(b=>b.onclick=()=>{const next=b.dataset.tool;if(next==='clear'){drawings=[];persistDrawings();draw();return;}tool=next;document.querySelectorAll('[data-tool]').forEach(x=>x.classList.toggle('on',x.dataset.tool===tool));canvas.style.cursor=tool==='cursor'?'grab':tool==='erase'?'not-allowed':'crosshair';});
let lastAlertId=null;function alertSurface(a){if(!a?.alert_id||a.alert_id===lastAlertId)return;lastAlertId=a.alert_id;line('! ALERT ['+a.alert_severity+'] '+a.alert_message);if(a.alert_severity>=3&&document.visibilityState!=='visible'&&'Notification'in window&&Notification.permission==='granted')new Notification('InsiderTrader critical alert',{body:a.alert_message});}const events=new EventSource('/events?token='+token);events.addEventListener('snapshot',e=>{state=JSON.parse(e.data);alertSurface(state);document.querySelector('#symbol').textContent=state.symbol+' · '+state.instrument;document.querySelector('#interval').textContent=state.interval;document.querySelector('#quote').textContent='BID '+(state.bid??'—')+' · ASK '+(state.ask??'—')+' · LAST '+(state.last??'—');document.querySelector('#risk').textContent='RISK '+state.risk;document.querySelector('#mode').textContent='MODE '+state.mode;document.querySelector('#cursor').textContent='CURSOR '+state.cursor;const c=document.querySelector('#connection');c.textContent=state.connected?'● LIVE':'● DISCONNECTED';c.className=state.connected?'good':'bad';document.querySelectorAll('[data-cmd^="INTERVAL"],[data-cmd^="STYLE"],[data-cmd^="ZOOM"]').forEach(b=>b.classList.toggle('on',b.dataset.cmd.endsWith(state.interval)||b.dataset.cmd.endsWith(state.style)||b.dataset.cmd.endsWith(String(state.bars.length))));draw();});events.onerror=()=>{const c=document.querySelector('#connection');c.textContent='● RECONNECTING';c.className='bad';};
function fit(){const r=canvas.getBoundingClientRect(),d=devicePixelRatio||1,w=Math.max(1,Math.floor(r.width*d)),h=Math.max(1,Math.floor(r.height*d));if(canvas.width!==w||canvas.height!==h){canvas.width=w;canvas.height=h;}ctx.setTransform(d,0,0,d,0,0);return r;}
function draw(){const r=fit(),w=r.width,h=r.height,b=state?.bars||[];ctx.fillStyle='#0d0f12';ctx.fillRect(0,0,w,h);if(!b.length){ctx.fillStyle='#8b92a3';ctx.fillText('No canonical bars in the selected window',20,40);return;}const pad={l:12,r:72,t:28,b:56},cw=w-pad.l-pad.r,ch=h-pad.t-pad.b,volH=Math.max(48,ch*.18),priceH=ch-volH-18;let lo=Math.min(...b.map(x=>x.l)),hi=Math.max(...b.map(x=>x.h));if(hi===lo){hi++;lo--;}const py=v=>pad.t+(hi-v)/(hi-lo)*priceH,step=cw/b.length,x=i=>pad.l+(i+.5)*step;
ctx.strokeStyle='#20242c';ctx.lineWidth=1;ctx.fillStyle='#7f8798';ctx.font='11px ui-monospace';for(let i=0;i<=5;i++){let y=pad.t+i*priceH/5,p=hi-(hi-lo)*i/5;ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();ctx.fillText(Math.round(p).toString(),w-pad.r+8,y+4);}for(let i=0;i<=6;i++){let xx=pad.l+i*cw/6;ctx.beginPath();ctx.moveTo(xx,pad.t);ctx.lineTo(xx,h-pad.b);ctx.stroke();if(b.length){const z=b[Math.min(b.length-1,Math.floor(i*b.length/6))];const d=new Date(z.t/1e6);ctx.fillText(d.toISOString().slice(0,16).replace('T',' '),Math.max(pad.l,Math.min(w-pad.r-90,xx-42)),h-pad.b+20);}}
const vmax=Math.max(1,...b.map(z=>Math.max(0,z.v)));b.forEach((z,i)=>{const xx=x(i),up=z.c>=z.o,col=up?'#26a69a':'#ef5350';ctx.strokeStyle=col;ctx.fillStyle=col;if(state.style==='LINE'){if(i){ctx.beginPath();ctx.moveTo(x(i-1),py(b[i-1].c));ctx.lineTo(xx,py(z.c));ctx.stroke();}}else{ctx.beginPath();ctx.moveTo(xx,py(z.h));ctx.lineTo(xx,py(z.l));ctx.stroke();const y1=py(Math.max(z.o,z.c)),y2=py(Math.min(z.o,z.c));if(up)ctx.strokeRect(xx-step*.3,y1,Math.max(1,step*.6),Math.max(1,y2-y1));else ctx.fillRect(xx-step*.3,y1,Math.max(1,step*.6),Math.max(1,y2-y1));}const vh=Math.max(1,z.v/vmax*volH);ctx.globalAlpha=.45;ctx.fillRect(xx-step*.32,h-pad.b-vh,Math.max(1,step*.64),vh);ctx.globalAlpha=1;});
function overlay(key,color){if(!state.overlays.includes(key))return;ctx.strokeStyle=color;ctx.lineWidth=1.5;ctx.beginPath();let started=false;b.forEach((z,i)=>{const field=key==='SMA20'?'s20':key==='SMA50'?'s50':'vw',v=z[field];if(v!==null){if(started)ctx.lineTo(x(i),py(v));else{ctx.moveTo(x(i),py(v));started=true;}}});ctx.stroke();}overlay('SMA20','#ffbe3c');overlay('SMA50','#9c7cff');overlay('VWAP','#4da3ff');drawAnnotations();
if(mouse&&mouse.x>=pad.l&&mouse.x<=w-pad.r){const i=Math.max(0,Math.min(b.length-1,Math.floor((mouse.x-pad.l)/step))),z=b[i],xx=x(i);ctx.strokeStyle='#8b92a3';ctx.setLineDash([4,4]);ctx.beginPath();ctx.moveTo(xx,pad.t);ctx.lineTo(xx,h-pad.b);ctx.stroke();ctx.setLineDash([]);document.querySelector('#legend').innerHTML='<b>'+state.symbol+'</b> '+new Date(z.t/1e6).toISOString()+'<br>O '+z.o+' H '+z.h+' L '+z.l+' C '+z.c+' V '+z.v;}else document.querySelector('#legend').innerHTML='<b>'+state.symbol+'</b> · '+state.style+' · '+state.overlays+'<br>POS '+state.position+' · ORDERS '+state.orders+' · PROPOSALS '+state.proposals+'<br>'+state.status;
}
function persistDrawings(){drawings=drawings.slice(-64);try{localStorage.setItem('insidertrader.chart.drawings',JSON.stringify(drawings))}catch(_){} }
function pointer(e){const r=canvas.getBoundingClientRect();return{x:e.clientX-r.left,y:e.clientY-r.top}}
function normalized(p){const r=canvas.getBoundingClientRect(),pad={l:12,r:72,t:28,b:56},cw=r.width-pad.l-pad.r,ph=(r.height-pad.t-pad.b)*.82;return{x:Math.max(0,Math.min(1,(p.x-pad.l)/cw)),y:Math.max(0,Math.min(1,(p.y-pad.t)/ph))}}
function drawAnnotations(){if(!state)return;const r=canvas.getBoundingClientRect(),pad={l:12,r:72,t:28,b:56},w=r.width,h=r.height,cw=w-pad.l-pad.r,ph=(h-pad.t-pad.b)*.82,sx=p=>pad.l+p*cw,sy=p=>pad.t+p*ph;const line=(a,b,color,dash=[])=>{ctx.strokeStyle=color;ctx.lineWidth=1.5;ctx.setLineDash(dash);ctx.beginPath();ctx.moveTo(sx(a.x),sy(a.y));ctx.lineTo(sx(b.x),sy(b.y));ctx.stroke();ctx.setLineDash([])};drawings.forEach(d=>{if(d.kind==='trend')line(d.a,d.b,'#ffbe3c');if(d.kind==='hline'){line({x:0,y:d.a.y},{x:1,y:d.a.y},'#ff8c00',[6,4])}if(d.kind==='box'){const x=sx(Math.min(d.a.x,d.b.x)),y=sy(Math.min(d.a.y,d.b.y)),ww=Math.abs(sx(d.a.x)-sx(d.b.x)),hh=Math.abs(sy(d.a.y)-sy(d.b.y));ctx.fillStyle='rgba(77,163,255,.10)';ctx.fillRect(x,y,ww,hh);ctx.strokeStyle='#4da3ff';ctx.strokeRect(x,y,ww,hh)}if(d.kind==='fib'){[0,.236,.382,.5,.618,.786,1].forEach((level,i)=>{const y=d.a.y+(d.b.y-d.a.y)*level;line({x:d.a.x,y},{x:d.b.x,y},'#9c7cff',[3,3]);ctx.fillStyle='#9c7cff';ctx.fillText(i===0?'0%':i===6?'100%':(level*100).toFixed(1)+'%',sx(d.b.x)+4,sy(y)+3)})}});if(drawing&&tool!=='erase')line(drawing.a,drawing.b,'#fff',[2,2])}
canvas.onmousemove=e=>{const r=canvas.getBoundingClientRect();mouse={x:e.clientX-r.left,y:e.clientY-r.top};if(pan&&Math.abs(mouse.x-pan.x)>14){send(mouse.x>pan.x?'PAN OLDER 5':'PAN NEWER 5');pan=mouse}if(drawing)drawing.b=normalized(mouse);draw();};canvas.onmousedown=e=>{if(tool==='cursor'){pan=pointer(e);canvas.style.cursor='grabbing';return}drawing={a:normalized(pointer(e)),b:normalized(pointer(e))};};canvas.onwheel=e=>{e.preventDefault();send(e.deltaY<0?'ZOOM 30':'ZOOM 960');};canvas.onmouseup=()=>{if(pan){pan=null;canvas.style.cursor='grab';return}if(!drawing)return;if(tool==='erase'){drawings.pop()}else if(['trend','fib','box','hline'].includes(tool))drawings.push({kind:tool,a:drawing.a,b:drawing.b});drawing=null;persistDrawings();draw();};canvas.onmouseleave=()=>{mouse=null;pan=null;drawing=null;canvas.style.cursor=tool==='cursor'?'grab':'crosshair';draw();};new ResizeObserver(draw).observe(canvas);window.addEventListener('keydown',e=>{if(e.target.tagName==='INPUT')return;if(e.key==='+')send('ZOOM 30');else if(e.key==='0')send('CHARTRESET');else if(e.key==='ArrowLeft')send('PAN OLDER 10');else if(e.key==='ArrowRight')send('PAN NEWER 10');});
</script></body></html>"#;

#[cfg(test)]
mod tests {
    use super::allowed_browser_command;

    #[test]
    fn browser_console_is_read_only() {
        assert!(allowed_browser_command("CHART AAPL"));
        assert!(allowed_browser_command("INTERVAL 5"));
        assert!(allowed_browser_command("OVERLAY SMA20 ON"));
        for command in [
            "BUY 1 10 MKT",
            "CONFIRM",
            "CANCEL order-1",
            "MODE AUTO",
            "HALT authorization",
            "STRATSET alpha PRODUCTION yes evidence",
        ] {
            assert!(!allowed_browser_command(command));
        }
    }

    #[test]
    fn browser_console_rejects_control_characters_and_oversize() {
        assert!(!allowed_browser_command("CHART\nAAPL"));
        assert!(!allowed_browser_command(&format!(
            "CHART {}",
            "A".repeat(1_024)
        )));
    }
}
