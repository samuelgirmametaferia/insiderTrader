# InsiderTrader terminal workstation

InsiderTrader uses a native Rust terminal interface inspired by professional
market terminals: the command line is the primary navigation surface, dense
tables are preferred over decorative cards, and every important workflow is
available without a mouse.

## Interaction contract

- Type a mnemonic and press Enter (`GO`): `MARKET`, `PORT`, `ORDERS`, `STRAT`,
  `METRICS`, `NEWS`, `RISK`, `AUTO`, `ALERTS`, `HEALTH`, or `HELP`.
- Security-first navigation accepts Bloomberg-style commands such as `AAPL GP`,
  `AAPL EQUITY DEPTH`, and `BTC-USD CRYPTO NEWS`. Symbols are resolved by the
  runtime's authoritative instrument master; the terminal does not guess through
  ambiguous, stale, or unsupported definitions. Function-first forms such as
  `GP AAPL` remain available.
- F1–F8 open the most common functions and F10 refreshes immediately.
- Up/Down and PageUp/PageDown scroll bounded result sets. Escape clears the
  command line. Ctrl-C or `QUIT` closes only the client.
- The command line supports insertion-point editing with Left/Right, Home/End,
  Delete, and Backspace. Tab completes both function mnemonics and bounded
  function arguments; ambiguous matches are shown without guessing. Ctrl-P and
  Ctrl-N traverse a bounded in-memory command history and preserve the current
  draft. Ctrl-A/Ctrl-E and Ctrl-U provide familiar fast editing controls. Long
  commands are display-width windowed around the real terminal cursor, including
  wide Unicode analyst text, without changing the submitted bytes.
- Function arguments retain their original case, so configuration paths and analyst
  questions are not rewritten while mnemonics remain case-insensitive.
- The header always shows account, journal cursor, connectivity, and data age.
- Orange identifies functions, amber identifies labels/warnings, and green/red
  always appear with explicit text or a sign so state is not color-only.

News is a first-class paged function. `NEWS AAPL` and `NEWS RELEVANT AAPL`
show deterministic relevance ranking, while `NEWS ALL` exposes the unranked feed.
`NN`/`NP` move through stable bounded cursor pages. Up/Down selects an article and
Enter opens its authoritative current version, normalized summary, audit-version
count, exact-title cluster, content hash, and canonical HTTPS link. Escape returns
to the feed.

`MARKET` is a selectable canonical-instrument monitor; Up/Down changes the
selection and Enter opens `CHART` (`GP`). The chart renders native OHLC candles
and a separate volume pane without requiring a browser or image surface. Price charts have
truthful UTC time labels and a right-side price scale sourced from the runtime's
canonical bar start time and interval. A non-monotonic source sequence is called
out explicitly instead of being drawn as a valid time axis.

Chart controls are operational function commands and bounded presentation state:

```text
ZOOM 240                         source-bar window: 30/60/120/240/480/960
INTERVAL 5                      aggregate five source bars per display bar
STYLE OHLC                      CANDLE, OHLC, or LINE
OVERLAY SMA20 ON                SMA20, SMA50, or visible-window VWAP
XHAIR OLDER                     OLDER, NEWER, LATEST, or OFF
PAN OLDER 10                    pan ten displayed interval bars
CHARTRESET                      deterministic workstation defaults
TV [instrument]                optional loopback browser chart workspace
```

The interval command is deliberately a source-bar multiplier. The chart shows the
computed duration from the authoritative source interval (for example,
`5 x 1m = 5m`) and never guesses that `5` means five minutes. `+`/`-` zoom,
Left/Right pan, Shift+Left/Right moves the crosshair, and `0` resets the chart.
The crosshair displays exact UTC start time, interval, OHLCV, and an explicit
`+ UP`/`- DOWN` cue. SMA and visible-window VWAP calculations are display-only;
they never enter metric, strategy, proposal, risk, or execution state. Chart
window, interval, style, and overlays persist in the versioned terminal preference
file, while cursor and authoritative bar data do not.

`CHART`, `TV`, or `TRADINGVIEW` opens the selected canonical graph in the system browser using
a familiar TradingView-style chart layout. The page is served only from an ephemeral
`127.0.0.1` port, uses no remote scripts, embeds, CDN, Node, or broker connection, and
stops with the native terminal. Server-sent events update the graph asynchronously,
so rendering never blocks market refresh. Its sidebar coordination terminal accepts
only bounded presentation commands (`CHART`, `INTERVAL`, `STYLE`, `OVERLAY`, `ZOOM`,
`PAN`, `CHARTRESET`, and `REFRESH`). The browser toolbar also supports local,
presentation-only Trend, Fib retracement, Box, and Horizontal level drawings;
drawings are capped and kept in browser local storage. Drag the chart to pan and
use the wheel to zoom. When the browser tab is hidden, critical in-app alerts are
surfaced through the browser notification API when permission has been granted.
Trading, risk, autonomy, broker, strategy, and
metric mutations remain available only through the authenticated native terminal.

Live quote/trade quality, position, open-order count, active proposal count,
source window, aggregation, style, and history offset remain visible above the
historical series.

`SCREEN <mode>` ranks the complete bounded canonical snapshot without silently
truncating matching instruments. Modes are `ALL`, `MOVERS`, `GAINERS`, `LOSERS`,
`VOLUME`, `SPREAD`, and `STALE`. The function reports matching and authoritative
totals, renders only visible rows, refreshes its cached ranking with each runtime
snapshot, and uses deterministic instrument-ID tie breaking. Up/Down selects a
result and Enter drills directly into its `CHART`.

`ANALYZE <question>` runs through a dedicated authenticated IPC session on one
bounded background worker. The terminal immediately opens a pending Analyst
function and continues keyboard handling, redraws, market snapshots, risk, and
execution monitoring while the provider streams. Only one analyst request may run
at a time. Streaming is preferred; a non-streaming completion is the compatibility
fallback. Provider or decoding failures remain isolated from deterministic trading.

`AUTO` is the autonomous-coordinator audit surface. It shows the durable mode,
latest plan state and timing, each selected finite action, proposal ID, resolved
strategy, scale, and reason codes. The displayed provider/model are explicitly the
current runtime configuration because the present plan journal schema does not bind
provider/model provenance or a reconsideration interval to a plan. Missing audit
fields are shown as unrecorded rather than inferred from TTL or terminal state.

## Trading controls

Manual orders use a mandatory two-stage flow:

```text
BUY AAPL 100 MKT <GO>
CONFIRM <GO>

SELL AAPL 100 LMT 205000 <GO>
CONFIRM <GO>
```

The preview is produced by the risk engine. `CONFIRM` sends the exact returned
preview bytes back through the authenticated command service; the terminal cannot
change quantity, price, intent identity, or warnings between stages. Cancellation,
risk state changes, autonomy mode changes, strategy/metric lifecycle transitions,
and configuration reloads use the same typed command path.

Active strategy proposals use the same two-stage operator discipline. Open `AUTO`,
move with Up/Down, and press Enter to request a full-scale risk preview, or use
`PREVIEW <proposal-id> [scale]` with a scale greater than zero and at most one.
The selected proposal, scale, notional, estimated cost, and warnings remain visible;
only a separate `CONFIRM` submits it through the strategy coordinator, risk, and
execution services. Enter never submits directly.

## Process model

`insider-runtime` is the authoritative headless process. `insider-terminal` is a
replaceable local client over an owner-only Unix socket. Closing the terminal does
not stop providers, strategies, autonomous execution, reconciliation, or journaling.
The terminal never receives broker credentials or provider API keys.

Presentation preferences are restored from
`$XDG_STATE_HOME/insidertrader/terminal.state` (or
`$HOME/.local/state/insidertrader/terminal.state`). The bounded file contains only
the last function, selected instrument/symbol, news scope, chart window, validated
source-bar interval, chart style, display-overlay switches, and screener mode.
Authoritative account, bar, metric, order, position, risk, and autonomy state always
comes from the runtime. Version-4 preferences migrate to safe chart defaults. Use
`--state-file PATH` to select a deployment-owned location.

## Performance rules

- Refresh work is serialized and bounded; requests do not overlap.
- Wire strings, collections, snapshots, manifests, and configuration input are
  rejected above their declared limits.
- Rendering touches visible rows and uses stable-width numeric columns.
- Remote LLM work never blocks input, market refresh, risk, or execution.
- A basic color terminal remains usable; advanced glyphs are optional enhancement.
