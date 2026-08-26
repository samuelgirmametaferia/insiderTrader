#!/usr/bin/env python3
"""Require SPDX/license metadata for every resolved Cargo package."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    try:
        raw = subprocess.check_output(["cargo", "metadata", "--locked", "--format-version", "1"], cwd=ROOT)
        packages = json.loads(raw)["packages"]
    except (OSError, subprocess.CalledProcessError, KeyError, json.JSONDecodeError) as error:
        print(f"license metadata inspection failed: {error}", file=sys.stderr)
        return 1
    missing = [package["name"] for package in packages if not package.get("license") and not package.get("license_file")]
    if missing:
        print("packages missing license metadata: " + ", ".join(sorted(missing)), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
