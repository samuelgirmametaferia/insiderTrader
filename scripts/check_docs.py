#!/usr/bin/env python3
"""Validate local Markdown links without requiring network access."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent.parent
LINK = re.compile(r"\[[^\]]+\]\(([^)\s]+)")


def main() -> int:
    failures: list[str] = []
    for markdown in ROOT.rglob("*.md"):
        if any(part in {".git", "target", "node_modules", ".venv", "dist"} for part in markdown.relative_to(ROOT).parts):
            continue
        for target in LINK.findall(markdown.read_text(encoding="utf-8")):
            parsed = urlsplit(target)
            if parsed.scheme or target.startswith("#"):
                continue
            relative = (markdown.parent / parsed.path).resolve()
            try:
                relative.relative_to(ROOT.resolve())
            except ValueError:
                failures.append(f"{markdown.relative_to(ROOT)}: link escapes repository: {target}")
                continue
            if not relative.exists():
                failures.append(f"{markdown.relative_to(ROOT)}: missing link target: {target}")
    if failures:
        print("documentation link check failed:", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
