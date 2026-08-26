#!/usr/bin/env python3
"""Ensure every active example CFG key is represented by the setup generator."""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CFG = ROOT / "config" / "example.cfg"
UI = ROOT / "ui" / "src" / "app" / "main.ts"
KEY = re.compile(r"^\s*([A-Za-z][A-Za-z0-9_.-]*)\s*=\s*(?!#)", re.MULTILINE)


def main() -> int:
    cfg_keys = sorted(set(KEY.findall(CFG.read_text(encoding="utf-8"))))
    ui = UI.read_text(encoding="utf-8")
    missing = [key for key in cfg_keys if key not in ui]
    if missing:
        raise SystemExit("example CFG keys missing from generator: " + ", ".join(missing))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
