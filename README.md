# InsiderTrader

InsiderTrader is a deterministic, deadline-aware trading engine and professional
desktop workstation. Read `AGENTS.md` for normative architecture and `PLAN.md` for
the production implementation and certification gates.

The implementation is under active construction. No gate is complete until its
same-revision evidence exists under `evidence/gates/`.

Operators should use the [operator runbook](docs/runbooks/operator-guide.md) for
installation, paper startup, outages, reconciliation, backup/restore, and any
paper-to-live change. It keeps broker state and the journal authoritative and
does not permit live mode without certification evidence. Record incidents with
the [incident template](docs/runbooks/incident-template.md) so risk transitions,
reconciliation, approvals, and evidence hashes are retained consistently.
Release candidates must additionally follow the [release certification runbook](docs/runbooks/release-certification.md)
before any live canary.

## Local verification

```bash
./scripts/check.sh
```

For discoverable contributor shortcuts, `make test` runs the Rust, Python, and UI
tests; `make ui-build` runs the UI source check and production build; `make fmt` checks
Rust formatting. These aliases do not replace the full `make check` gate.

Before starting a long-running paper process, run `make paper-check`. It copies the
example configuration into a private temporary directory and executes the real
desktop-bridge composition root with `--check`; configuration bounds, journal
recovery, catalog, broker, risk, provider, and package registration must all pass.
The check does not bind a socket, spawn schedulers, or touch repository data.
For the long-running launcher, `make paper` is deliberately fail-closed and requires
`IT_CONFIG`, `IT_JOURNAL`, and `IT_SOCKET` deployment-owned paths; `IT_ACCOUNT`
defaults to `1`:

```bash
IT_CONFIG=data/insidertrader.cfg \
IT_JOURNAL=data/runtime.journal \
IT_SOCKET=data/runtime.sock \
IT_ACCOUNT=1 make paper
```

The UI toolchain is pinned to Node `22.22.2` and npm `12.0.2`. Use a version
manager that reads [`ui/.node-version`](ui/.node-version), then install and test
from the UI package directory (the root has no npm project):

```bash
cd ui
npm ci
npm test
npm run build
```

`ui/package.json` declares both the supported Node and npm ranges plus the npm
`packageManager`; CI uses the same versions so dependency resolution and build
behavior cannot silently drift between developer and release environments.

## Typed `.cfg` setup

Start from [`config/example.cfg`](config/example.cfg) and pass the deployment-owned
copy to the desktop bridge:

```bash
mkdir -p data
cp config/example.cfg data/insidertrader.cfg
insider-desktop-bridge serve \
  --config data/insidertrader.cfg \
  --journal data/runtime.journal \
  --socket data/runtime.sock \
  --account 1
```

The file uses bounded `key = value` syntax. `cfg-core` rejects duplicate keys,
invalid scalars, non-finite numbers, oversized input, and malformed strings before
the engine starts. Values in the file are authoritative for typed risk and alert
settings; matching legacy environment variables are fallback values only when a
key is absent. The Configuration panel generates and atomically reloads the same
syntax using an authenticated compare-and-swap version, so a stale UI cannot
silently overwrite a newer policy. API keys and other credentials do not belong in
`.cfg`; keep them in the deployment secret manager/environment boundary.
Configured provider and article URLs are bounded to 2,048 UTF-8 bytes, require HTTPS with a
non-empty authority, and reject whitespace and username/password credentials. Local HTTP is
reserved for explicitly allowlisted local LLM inference endpoints; NewsAPI, Yahoo, RSS, and IBKR
configuration remains HTTPS-only. The engine and provider adapters enforce this again at runtime.
The example also contains bounded operational keys for Python worker isolation,
execution cadence, market freshness/provider tuning, and LLM base URL/timeout; these
use the same file-first, environment-fallback rule at startup. Python worker
`cpu_seconds` is bounded to 1–86,400 and `memory_bytes` to 64 MiB–8 GiB before a
worker is registered. The Configuration panel exposes the supported generator fields,
including Python isolation and Yahoo adapter settings; direct `.cfg` editing remains
available for deployment automation and settings not represented by the compact form.
For bounded Yahoo multi-symbol polling, set `market.yahoo_symbols` to a comma-separated
`SYMBOL=INSTRUMENT_ID` list (up to 128 subscriptions); `IT_YAHOO_SYMBOLS` remains the
fallback when the CFG key is absent.
For NewsAPI top-headlines, the Configuration panel can set `news.newsapi_country`,
`news.newsapi_category`, or `news.newsapi_sources`; at least one filter is required
for that endpoint and each value is validated before startup.
`ui.status_poll_ms` controls the desktop's observational provider, broker, risk, supervisor,
and strategy status refresh cadence. It accepts 1,000–60,000 ms (default 5,000).
`ui.alert_poll_ms` controls alert refresh cadence (500–60,000 ms, default 1,000), while
`ui.news_stale_after_ms` and `ui.analyst_stale_after_ms` control freshness warnings (each
60,000–3,600,000 ms, default 300,000). All three are generated and validated by the Configuration
panel and remain presentation/observability settings rather than broker state.
Each bounded refresh batch completes before the next one is scheduled, so lowering the values
cannot create overlapping diagnostic requests.
`ui.news_stale_after_ms` controls when the News panel marks its last successful page stale;
the CFG generator enforces 60,000–3,600,000 ms (default 300,000) and preserves unrelated keys.

Python research workers use the same bounded contracts as the Rust runtime:

```python
from insidertrader.metric_sdk import MetricDescriptor, MetricOutput, validate_output
from insidertrader.strategy_sdk import Action, Proposal, validate_proposal
```

`MetricOutput.to_wire()` and `Proposal.to_wire()` emit JSON-safe objects with
canonical IDs and explicit freshness/TTL fields. These SDKs produce proposals
only; broker credentials and order submission remain outside the worker process.

Live trading is disabled until the per-asset G07 and integrated G15 certifications
are complete.

Python metric and strategy workers run out of process with framed IPC, a bounded
working directory (`python.workdir` or `IT_PYTHON_WORKDIR`, default
`data/python-workers`), CPU and address-space limits (`python.cpu_seconds` or
`IT_PYTHON_CPU_SECONDS`, default 3600 seconds per worker; `python.memory_bytes` or
`IT_PYTHON_MEMORY_BYTES`, default 512 MiB), no user-site imports, and network
sockets disabled by default.
Network access requires the explicit `python.allow_network = true` CFG setting
(with `IT_PYTHON_ALLOW_NETWORK` retained only as fallback).

The desktop bridge accepts `--instrument ID --symbol SYMBOL --price TICKS` for a
canonical paper instrument. When those arguments are present, Yahoo quote polling
is enabled by default through the non-authoritative `/v7/finance/quote` adapter;
`IT_YAHOO_QUOTE_POLL_MS` controls its bounded interval (1–300 seconds). Quote
failure only marks the provider degraded and never bypasses broker/risk state.
Yahoo quote marks are disabled automatically when `IT_BROKER=ibkr`; enabling
`market.allow_yahoo_live_marks = true` in CFG (or its fallback environment variable)
is an explicit operator policy override.
For paper/research multi-symbol polling, set `IT_YAHOO_SYMBOLS` to a bounded
comma-separated list of `SYMBOL=INSTRUMENT_ID` entries (maximum 128 workers).
Those entries also bootstrap provider-qualified canonical catalog records, so
watchlist resolution and quote ingestion use the same instrument identity. Each
worker emits the same canonical quote event and uses the same interval and
price-scale controls; malformed entries are ignored rather than guessed.
In IBKR mode, the desktop bridge also rejects synthetic `--price` bootstrap
marks by default; `broker.allow_ibkr_bootstrap_mark = true` in CFG (or its fallback
environment variable) is required for a deliberate non-authoritative test override.
Authoritative IBKR quote polling can be enabled with `broker.ibkr_conid` and
`broker.ibkr_instrument_id` (also exposed by the CFG Generator); `IT_IBKR_MARKET_POLL_MS` is bounded to 250–60,000 ms
and `IT_IBKR_PRICE_SCALE` defines the integer-tick conversion (default 10,000).
Snapshots are accepted only when bid/ask ordering and finite positive canonical
values are valid; provider failures leave the existing stream state unchanged.
The decision scheduler marks quote, trade, and book streams stale when no
accepted event arrives within `IT_MARKET_MAX_AGE_MS` (default 60,000 ms,
bounded to 250–86,400,000 ms). The transition is exposed in runtime health and
never fabricates a replacement price.
Risk caps can be overridden explicitly with positive `IT_MAX_POSITION_TICKS` and
`IT_MAX_GROSS_NOTIONAL_TICKS` values; malformed or non-positive values fail startup.
Contextual risk guardrails are configured in the atomic engine settings snapshot
using `risk.max_leverage` (finite non-negative ratio), `risk.max_drawdown_bps`
(non-negative basis points), `risk.max_outstanding_orders`,
`risk.max_predicted_volatility_bps`, `risk.max_participation_bps`,
`risk.max_message_rate`, and `risk.max_price_deviation_bps` (all non-negative
integers). They are evaluated immediately before every proposal or manual target
becomes an order intent, using reconciled marks/equity and the local working-order
projection. Missing marks, non-positive equity for leverage, or missing high-water
drawdown state deny the order. Guardrail fields whose authoritative observation
is not yet wired are rejected during configuration rather than interpreted as
zero; config reload updates the active policy only after validation succeeds.
The desktop bridge maps these settings from matching `IT_MAX_*` environment
variables only as a backward-compatible fallback when the corresponding `.cfg`
key is absent; malformed or negative values fail startup before the engine accepts
orders. Predicted-volatility, participation, and price-deviation limits fail
closed until their authoritative observation is available; they are never
interpreted as zero. Message rate is measured from planning boundaries in a
bounded monotonic one-second window and is therefore enforceable locally.
For limit orders, price-deviation guardrails use the reconciled mark and the
requested limit price; market orders and proposals are denied when that guard is
enabled because no client price is available at planning time.
Set `IT_ALERT_WEBHOOK_URL` to an HTTPS endpoint to enable bounded webhook alert
delivery. Delivery runs off the trading path with a two-second request timeout;
only HTTP 2xx acknowledges the webhook copy, while the in-app alert remains
until manually acknowledged. Webhook configuration changes require restart.
Semantic context retrieval can be enabled explicitly with
`IT_EMBEDDING_MODEL`, `IT_EMBEDDING_MODEL_VERSION`, and
`IT_EMBEDDING_DIMENSIONS` (1–4096). These settings are all-or-none and are
validated before the engine enters reconciliation; without them, exact,
lexical, and graph retrieval remains the deterministic fallback.
The desktop composition root also registers the compiled `volatility.ewma.v1`
metric through the normal metric host. Its ID, lambda, and TTL are configurable
with `IT_EWMA_METRIC_ID`, `IT_EWMA_LAMBDA`, and `IT_EWMA_TTL_NS`.
The same composition root registers the compiled reference `trend.sma.v1`,
`liquidity.spread.v1`, and `microstructure.imbalance.v1` metrics. Their IDs and
shared TTL are configurable with `IT_SMA_METRIC_ID`, `IT_SPREAD_METRIC_ID`,
`IT_IMBALANCE_METRIC_ID`, and `IT_REFERENCE_METRIC_TTL_NS`; `IT_SMA_WINDOW`
controls the bounded SMA warm-up window. These metrics consume canonical quote,
bar, and book-quantity features and are evaluated through the same validated host
as Python metrics.
The deterministic threshold reference strategy is available through the same
coordinator when `IT_ENABLE_REFERENCE_STRATEGY=true` (or `1`). It consumes the
book-imbalance metric and remains disabled by default; its strategy ID, thresholds,
quantity, horizon, and TTL are controlled by `IT_REFERENCE_STRATEGY_ID`,
`IT_REFERENCE_ENTRY_THRESHOLD`, `IT_REFERENCE_EXIT_THRESHOLD`,
`IT_REFERENCE_QUANTITY_TICKS`, `IT_REFERENCE_HORIZON_NS`, and
`IT_REFERENCE_STRATEGY_TTL_NS`.
