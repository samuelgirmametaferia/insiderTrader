#!/usr/bin/env python3
"""Verify the packaged Tauri shell retains production-safe desktop settings."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "ui" / "src-tauri" / "tauri.conf.json"


def main() -> int:
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    if config.get("productName") != "InsiderTrader":
        raise SystemExit("Tauri productName must remain InsiderTrader")
    if config.get("identifier") != "com.insidertrader.desktop":
        raise SystemExit("Tauri identifier changed unexpectedly")
    build = config.get("build", {})
    if build.get("frontendDist") != "../dist":
        raise SystemExit("Tauri frontendDist must point at the locked UI build")
    app = config.get("app", {})
    security = app.get("security", {})
    csp = security.get("csp", "")
    for directive in ("default-src 'self'", "connect-src 'self'", "script-src 'self'"):
        if directive not in csp:
            raise SystemExit(f"Tauri CSP missing required directive: {directive}")
    windows = app.get("windows", [])
    if len(windows) != 1 or windows[0].get("minWidth", 0) < 960 or windows[0].get("minHeight", 0) < 600:
        raise SystemExit("Tauri window minimum bounds are missing or unsafe")
    if config.get("bundle", {}).get("active") is not True:
        raise SystemExit("Tauri packaging must remain enabled")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
