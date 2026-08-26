#!/usr/bin/env python3
"""Check that locked dependency metadata is present and reproducible."""

from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    lock = ROOT / "Cargo.lock"
    if not lock.is_file() or lock.stat().st_size == 0:
        print("Cargo.lock is required", file=sys.stderr)
        return 1
    try:
        first = subprocess.check_output(["cargo", "metadata", "--locked", "--format-version", "1"], cwd=ROOT)
        second = subprocess.check_output(["cargo", "metadata", "--locked", "--format-version", "1"], cwd=ROOT)
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"locked cargo metadata failed: {error}", file=sys.stderr)
        return 1
    if hashlib.sha256(first).digest() != hashlib.sha256(second).digest():
        print("cargo metadata is not reproducible", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
