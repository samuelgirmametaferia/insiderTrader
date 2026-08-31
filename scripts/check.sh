#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

# The repository pins 1.98.0. `stable` is explicitly selected here so an already
# installed pinned compiler can be used in read-only/offline build environments
# without rustup attempting to update channel metadata.
export RUSTUP_TOOLCHAIN="${IT_RUSTUP_TOOLCHAIN:-stable}"
actual_rust_version="$(rustc --version | awk '{print $2}')"
if [[ "$actual_rust_version" != "1.98.0" ]]; then
  echo "Rust 1.98.0 is required; found $actual_rust_version" >&2
  exit 1
fi

cargo fmt --all -- --check
./scripts/check_schemas.sh
python3 scripts/check_dependencies.py
python3 scripts/check_licenses.py
python3 scripts/check_security.py
python3 scripts/check_docs.py
python3 scripts/check_ci_contract.py
python3 scripts/check_runbook.py
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
PYTHONPATH=python python3 -m unittest discover -s tests/python -p 'test_*.py'
python3 scripts/check_requirements.py
