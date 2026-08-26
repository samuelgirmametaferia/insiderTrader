# insiderTrader — UI/UX Design Specification

Design language: dark-first, sharp, high-signal, low-noise. Built for someone who has the app open for hours and needs to act in under a second when the market moves. Every principle below is grounded in how professional trading terminals (Bloomberg, TradingView, dYdX, Hyperliquid-ecosystem terminals) and modern dark-mode design systems actually solve this problem — not generic dashboard advice.

---

## 1. Core Philosophy

Dark mode here isn't an aesthetic choice, it's a functional requirement: it reduces glare and eye fatigue over long monitoring sessions and keeps chart colors (candles, lines, heatmaps) legible without competing with a bright background. The two dominant terminal archetypes are opposite ends of a spectrum — Bloomberg-style (max density, dense tables, monospace numerics, sacrifice whitespace for information) vs. Robinhood-style (progressive disclosure, generous whitespace, few numbers at once). insiderTrader sits closer to Bloomberg's density but borrows Robinhood's calm — dense when you look for it, quiet when you don't.

Navigation friction is a trust problem, not a polish problem. In a fast market, hesitation caused by hunting for a button is a financial loss. Every primary action (execute, cancel, flatten) must be reachable in one motion from the locked-in view — never buried in a menu.

## 2. Color System

Base palette: layered near-blacks (never pure `#000000` — pure black flattens depth and crushes shadow detail on OLED). Elevation is communicated by *lightness*, not drop-shadow, since shadows barely read on a black canvas.

| Token | Value | Use |
|---|---|---|
| `--bg-void` | `#0a0a0c` | App background, outermost layer |
| `--bg-panel` | `#111114` | Panel surface (non-glass fallback) |
| `--bg-elevated` | `#18181c` | Modals, popovers, raised surfaces |
| `--border-hairline` | `rgba(255,255,255,0.08)` | Panel edges, dividers |
| `--border-focus` | `rgba(255,255,255,0.18)` | Active/focused panel edge |
| `--text-primary` | `#f2f2f0` | Primary text (off-white, not pure white — pure white on near-black is harsh over long sessions) |
| `--text-secondary` | `#8a8a92` | Labels, timestamps, muted metadata |
| `--accent-green` | `#16c784` | Gains, buy, long, positive P/L |
| `--accent-green-dim` | `#0e7a52` | Secondary green (subtle fills, gridlines, hover states) |
| `--accent-red` | `#ea3943` | Losses, sell, short, negative P/L |
| `--accent-red-dim` | `#8f1f26` | Secondary red (subtle fills, gridlines, hover states) |
| `--accent-amber` | `#f0a63f` | Warnings, pending states, risk flags |

Why these specific greens/reds: fully saturated neon (`#00ff00`/`#ff0000`) vibrates against black and causes fatigue; pulling saturation down to ~65-75% and lightness to ~45-55% keeps the "pop" against dark backgrounds while staying starable for hours. This is the same reasoning dark-terminal dashboards use — a near-black base with a restrained neon-adjacent accent for "healthy/unhealthy" states.

**Colorblind rule (non-negotiable):** ~8% of men have red-green color vision deficiency and cannot separate profit from loss by hue alone. Every green/red signal must carry a second channel: a `+`/`−` sign, an up/down triangle glyph, or bold vs. normal weight. Never ship a P/L number, order-book side, or chart candle that relies on color as the *only* signal. Bloomberg's own accessibility work added alternate CVD-safe palettes (blue/orange) as a settings-level swap — do the same: a "colorblind mode" toggle in Appearance settings that remaps green/red to blue/amber without changing any other token.

**Market convention:** Western markets = green up / red down. If insiderTrader ever serves East Asian markets (China, Japan, Korea, Taiwan), that convention inverts (red = up). Keep the green/red mapping as a semantic token (`--accent-gain` / `--accent-loss`), never hardcoded, so it can flip per locale later without a redesign.

## 3. Typography

Three-font system, each doing one job:

- **Display / headers — Space Grotesk.** Geometric, slightly technical, has personality without sacrificing legibility. This is the "fun to look at" font — used for workspace titles, section headers, the wordmark.
- **Data / numbers — JetBrains Mono or IBM Plex Mono, tabular figures on.** All prices, P/L, order sizes, timestamps go in monospace so digits align in columns and don't jitter width when a live-updating number ticks over (e.g. 199 → 200 shouldn't visibly shift). JetBrains Mono has the widest character disambiguation (0 vs O, 1 vs l vs I), which matters when someone is fat-fingering an order size at 2am.
- **UI / body — Inter.** Labels, buttons, settings copy, tooltips. Designed for screens, high x-height, reads clean at small sizes.

Rule of thumb: if it's a number that changes, it's mono. If it's a label that doesn't, it's Inter. If it's a section title, it's Space Grotesk.

## 4. Shape & Elevation

- `border-radius: 0` everywhere except: small controls that need a tactile affordance (toggle pills, tags) get 2px max. No default 8–16px rounded-card look — that reads as "consumer SaaS," not "terminal."
- Depth comes from three things, in order of preference: (1) lightness step (`--bg-void` → `--bg-panel` → `--bg-elevated`), (2) a 1px hairline border, (3) blur (see §5). Avoid box-shadow as a primary depth cue — it barely reads against black and adds visual noise.
- Panel corners can carry a small HUD-style bracket accent (a short corner tick in `--border-focus`) instead of a full border-radius, to reinforce the "instrument panel" feel without softening the edges.

## 5. Transparency & Glass Panels

Use glass sparingly — on floating/overlay surfaces (the tools rail, the tab dock, toast notifications), not on the primary chart canvas, which should stay opaque so price action never fights a moving background through it.

```css
.panel-glass {
  background: rgba(17, 17, 20, 0.62);
  backdrop-filter: blur(14px);
  -webkit-backdrop-filter: blur(14px);
  border: 1px solid rgba(255, 255, 255, 0.08);
}
/* Fallback for browsers without backdrop-filter support */
@supports not (backdrop-filter: blur(1px)) {
  .panel-glass { background: rgba(17, 17, 20, 0.92); }
}
```

Working ranges pulled from current glass-UI practice: background opacity 5–30%, blur 10–20px, border opacity 10–20% white. Push opacity toward the high end (55–70%) for panels that sit directly over the live chart, since dense candle data underneath will otherwise fight your text for contrast — text must stay `--text-primary` regardless of what's behind the glass, never a color that could vanish over a bright candle.

## 6. Default View — "Locked-In" Trading Mode

This is what the app opens into. One chart, minimal chrome, everything else one keystroke away.

```
┌──────────────────────────────────────────────────────────────────┐
│ ▎Tools   insiderTrader              BTC-PERP   +4.82%  ▎ Workspace│
│ ─────────────────────────────────────────────────────────┤ tabs  │
│  ⌂                                                        │───────│
│  ⌇  chart                                                 │Positions│
│  ⌗                                                        │Orders  │
│  ⚙                                                        │Watchlist│
│                        (price chart, full bleed)          │───────│
│                                                             │       │
│                                                             │       │
│                                                             │       │
├──────────────────────────────────────────────────────────────────┤
│ ▎ message station — informational · warning · error, expandable   │
└──────────────────────────────────────────────────────────────────┘
```

- **Top bar:** icon-only tools rail toggle (left), instrument + live % change in mono type with color + sign (center-left), workspace tab strip (right). Nothing here is decorative — every element is either a live number or a navigation control.
- **Tools rail (left):** collapsed to icons by default, glass panel, expands on hover/pin. Chart tools, order entry, indicators.
- **Chart (center):** full-bleed, opaque background, takes whatever space isn't claimed by rail/side panel. This is the only surface that should never have a modal or glass panel drawn permanently over it.
- **Right dock:** tabbed panel (Positions / Orders / Watchlist / Alerts) — one tab visible at a time, not stacked accordions. Glass panel, resizable via drag handle.
- **Message station (bottom):** thin strip, collapsed to a single line by default (last message + severity dot), expands upward into a scrollable log on click. This is the only place errors, fills, disconnects, and system messages appear — never as a blocking modal unless the action needs a decision.

## 7. Workspaces

Workspaces are saved layouts, not just view filters — each remembers its panel arrangement, chart symbol/timeframe, and indicator set independently, the way an IDE remembers per-project window layout.

- Tab strip along the top-right, browser-tab-like: click to switch, drag to reorder, `+` to create from a template (Scalping, Swing, Research, Backtest).
- Hotkeys `Cmd/Ctrl + 1-9` jump directly to a workspace slot.
- Closing a workspace prompts nothing (it's just a view, not unsaved work) — but editing a workspace's panel layout auto-saves silently, no "save layout?" dialog.

## 8. Popups vs. Persistent Surfaces

Rule: if it's touched every session, it lives in the main HUD. If it's touched to set something up once and revisited rarely, it's a modal.

**Always modal (centered, dimmed backdrop, blocks input until dismissed):**
- Exchange/API key connection & account linking
- Risk parameter configuration (max leverage, max position size, daily loss limit)
- Workspace creation/rename/delete
- Full settings panel (see §10)
- Anything destructive (close all positions, delete API key) — always requires explicit confirm, never a timed auto-dismiss

**Never modal — lives in the flow:**
- Order entry and confirmation (inline in the tools rail or a slide-out drawer, not a dialog — this is the highest-frequency action in the app and must never cost a click to "close")
- Symbol search / switching
- Chart indicator add/remove

## 9. Message Station

A single, consistent channel for all system communication — no competing toast systems, no silent failures.

Severity model, four levels, each with its own color, placement behavior, and persistence rule:

| Severity | Color | Behavior |
|---|---|---|
| Info | `--text-secondary` | Auto-dismiss ~4s, logs silently to history |
| Success | `--accent-green-dim` | Auto-dismiss ~4s (order filled, connection restored) |
| Warning | `--accent-amber` | Auto-dismiss ~8s, stays in expanded log until acknowledged |
| Error / Critical | `--accent-red` | Does not auto-dismiss — order rejections, disconnects, and liquidation warnings persist until the user dismisses or resolves them |

Never auto-dismiss anything the user needs to act on — a timed toast for a failed order is how people lose money silently. The expandable log (click the strip to open) keeps full history for the session so nothing is ever truly lost, just collapsed.

## 10. Interaction Physics

If a chart looks like it should be draggable, it must be draggable at the same responsiveness as every other charting tool the user has touched — TradingView and Bloomberg have set that expectation industry-wide, and falling short of it reads as broken, not minimal.

Required chart interactions:
- **Scroll wheel = zoom**, anchored at the cursor position (not chart center) — zooming should keep whatever price/time is under the cursor fixed in place.
- **Click + drag = pan**, with light momentum/inertia on release (not infinite — decay over ~300-400ms) so it feels physical, not sticky-then-dead.
- **Trackpad pinch = zoom**, two-finger pan as an alternative to click-drag.
- **Crosshair** follows the cursor continuously and magnet-snaps to the nearest candle/data point; displays OHLC + time in a small mono readout pinned to the axis, not a floating tooltip that can drift over price action.
- **Double-click / double-tap** resets zoom to the default range.
- **Right-click** opens a context menu for chart-specific actions (add alert, draw tool, remove indicator) rather than a global menu bar.
- All transitions (zoom, pan settle, panel resize, hover states) target 60fps and use short, snappy easing — 100–150ms ease-out for hover/press feedback, nothing longer than ~250ms for structural transitions (panel open/close). Long animations on a trading surface feel like they're getting between the user and the market.
- Panel splitters are draggable with a visible grab cursor and a live-resize preview, not a "drag then release to see the result" delay.

## 11. Settings

One settings modal, categorized, searchable (a single search box at the top filters across all categories — don't make someone hunt through six tabs for one toggle):

- **Appearance** — theme (dark only at launch; colorblind-safe palette toggle), accent intensity, font size scale, chart style defaults (candle/line/bar), gridline density
- **Trading** — default order type, confirmation-dialog thresholds (e.g. always confirm above $X or above Nx leverage), one-click trading on/off, default position size
- **Layout** — workspace management (rename/duplicate/delete/reorder), default panel arrangement for new workspaces
- **Data** — refresh rate, data source/exchange selection, timezone, decimal precision
- **Notifications** — which events hit the message station vs. are silent, sound on/off per severity, desktop notification permissions
- **Risk** — max leverage warning threshold, daily loss circuit-breaker, position size warnings
- **Hotkeys** — full remap table, searchable, conflict detection
- **Connections / API** — exchange accounts, key management, connection status per account

## 12. Design Tokens (drop-in)

```css
:root {
  /* surfaces */
  --bg-void: #0a0a0c;
  --bg-panel: #111114;
  --bg-elevated: #18181c;
  --border-hairline: rgba(255,255,255,0.08);
  --border-focus: rgba(255,255,255,0.18);

  /* text */
  --text-primary: #f2f2f0;
  --text-secondary: #8a8a92;

  /* semantic accents */
  --accent-gain: #16c784;
  --accent-gain-dim: #0e7a52;
  --accent-loss: #ea3943;
  --accent-loss-dim: #8f1f26;
  --accent-warn: #f0a63f;

  /* type */
  --font-display: "Space Grotesk", sans-serif;
  --font-mono: "JetBrains Mono", monospace;
  --font-body: "Inter", sans-serif;

  /* shape */
  --radius: 0px;
  --radius-control: 2px;

  /* glass */
  --blur-panel: 14px;
  --glass-bg: rgba(17, 17, 20, 0.62);

  /* motion */
  --ease-fast: 120ms ease-out;
  --ease-structural: 220ms ease-out;
}
```

## 13. Research References

- Lollypop Design Studio, *Trading App Design: The Complete Guide to UI, UX & System Architecture* — dark-mode-as-requirement, data density by audience
- Bloomberg UX, *Designing the Terminal for Color Accessibility* — CVD-safe palette swaps for red/green
- ColorArchive, *Color in Financial UI* — red/green convention and required secondary encoding
- Visily, red color usage in dark-surface UI and badge/alert semantics
- Figr / DEV Community / IxDF / UX Pilot — glassmorphism CSS recipes and opacity/blur ranges
- DiverseKit / DesignMonks — Space Grotesk, JetBrains Mono, Inter font selection for technical/dashboard UI
- TradingView `lightweight-charts` docs (DeepWiki) — crosshair and mouse-interaction model
- MUI X Charts / Grafana Candlestick docs — scroll-to-zoom, drag-to-pan, double-click-to-reset conventions
- SaaSUI / Carbon Design System / GitLab Pajamas — toast severity models and persistence rules for critical vs. informational messages
- Hyperliquid design-pattern writeups — three-column trading canvas (watchlist / chart+order / positions), collapsible advanced panels for progressive disclosure