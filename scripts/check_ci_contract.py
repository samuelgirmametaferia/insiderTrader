#!/usr/bin/env python3
"""Fail fast when CI stops provisioning the pinned build toolchain."""

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def main() -> int:
    text = WORKFLOW.read_text(encoding="utf-8")
    required = (
        "cargo check --locked -p insider-terminal",
        "protobuf-compiler",
        "python-version-file: .python-version",
        "set -o pipefail",
        "./scripts/check.sh 2>&1 | tee gate.log",
        "actions/upload-artifact@v4",
        "run: make doctor",
    )
    missing = [marker for marker in required if marker not in text]
    if missing:
        raise SystemExit("CI contract missing: " + ", ".join(missing))
    verify_at = text.index("./scripts/check.sh")
    if text.index("cargo check --locked -p insider-terminal") < verify_at:
        raise SystemExit("terminal compile must run after repository verification")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
