#!/usr/bin/env bash
# Build the lightweight launch-media loop used by README.md.
# Replace this illustrative loop with a real paper-session capture before a
# public launch; never record credentials, account IDs, or live orders.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MEDIA_DIR="$ROOT_DIR/docs/media"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT
mkdir -p "$MEDIA_DIR"

frames=(
  "INSIDERTRADER|DETERMINISTIC TRADING WORKSTATION|One journal. Multiple clients."
  "CHART AAPL|LOCAL BROWSER CHART|Candles  SMA20  VWAP  Fib  Trend  Box"
  "METRICS + STRATEGIES|PROPOSALS WITH EVIDENCE|Freshness  Confidence  Risk budget"
  "RISK GATE|PREVIEW BEFORE CONFIRM|Manual  Hybrid  Autonomous"
)

index=0
for frame in "${frames[@]}"; do
  IFS='|' read -r title subtitle detail <<< "$frame"
  convert -size 1280x720 xc:'#0d0f12' \
    -fill '#ff8c00' -draw 'rectangle 0,0 1280,18' \
    -fill '#ffbe3c' -pointsize 52 -font DejaVu-Sans-Mono-Bold \
    -annotate +72+210 "$title" \
    -fill '#d9dde7' -pointsize 30 -font DejaVu-Sans-Mono \
    -annotate +72+285 "$subtitle" \
    -fill '#8b92a3' -pointsize 22 \
    -annotate +72+350 "$detail" \
    -fill '#26a69a' -pointsize 18 \
    -annotate +72+650 'PAPER-SAFE DEMO  |  LOCAL PRESENTATION  |  OPEN SOURCE' \
    "$WORK_DIR/frame-${index}.png"
  index=$((index + 1))
done

convert -delay 100 -loop 0 "$WORK_DIR"/frame-*.png "$MEDIA_DIR/insidertrader-demo.gif"
ffmpeg -y -loglevel error -framerate 10 -i "$WORK_DIR/frame-%d.png" \
  -pix_fmt yuv420p -movflags +faststart "$MEDIA_DIR/insidertrader-demo.mp4"
printf 'Wrote %s and %s\n' "$MEDIA_DIR/insidertrader-demo.gif" "$MEDIA_DIR/insidertrader-demo.mp4"
