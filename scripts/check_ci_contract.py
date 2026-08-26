#!/usr/bin/env python3
"""Fail fast when CI stops provisioning the pinned build toolchain."""

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def main() -> int:
    text = WORKFLOW.read_text(encoding="utf-8")
    required = (
        "node-version: 22.22.2",
        "corepack prepare npm@12.0.2 --activate",
        "npm ci --prefix ui",
        "cache-dependency-path: ui/package-lock.json",
        "libwebkit2gtk-4.1-dev",
        "libjavascriptcoregtk-4.1-dev",
        "cargo check --manifest-path ui/src-tauri/Cargo.toml --locked",
        "python-version-file: .python-version",
        "set -o pipefail",
        "./scripts/check.sh 2>&1 | tee gate.log",
        "actions/upload-artifact@v4",
    )
    missing = [marker for marker in required if marker not in text]
    if missing:
        raise SystemExit("CI contract missing: " + ", ".join(missing))
    install_at = text.index("npm ci --prefix ui")
    verify_at = text.index("./scripts/check.sh")
    if install_at > verify_at:
        raise SystemExit("locked UI install must precede repository verification")
    if text.index("cargo check --manifest-path ui/src-tauri/Cargo.toml --locked") < verify_at:
        raise SystemExit("Tauri compile must run after repository verification")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
