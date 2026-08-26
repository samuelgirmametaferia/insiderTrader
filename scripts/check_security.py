#!/usr/bin/env python3
"""Fail closed on accidentally committed credentials or unsafe secret literals."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKIP = {".git", "target", "node_modules", ".venv", "dist", "__pycache__"}
PATTERNS = (
    (re.compile(r"AKIA[0-9A-Z]{16}"), "AWS access key"),
    (re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"), "private key"),
    (re.compile(r"(?i)(?:api[_-]?key|access[_-]?token|client[_-]?secret)\s*[:=]\s*['\"][A-Za-z0-9_+/=-]{16,}['\"]"), "credential literal"),
)


def files() -> list[Path]:
    return [path for path in ROOT.rglob("*") if path.is_file() and not any(part in SKIP for part in path.relative_to(ROOT).parts)]


def main() -> int:
    failures: list[str] = []
    for path in files():
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for pattern, label in PATTERNS:
            if pattern.search(text):
                failures.append(f"{path.relative_to(ROOT)}: possible {label}")
    if failures:
        print("secret scan failed:", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
