.PHONY: check test rust-test python-test ui-test ui-build fmt paper paper-check

# The shell gate remains authoritative; these targets are discoverable aliases
# for contributors and never weaken the required verification command.
check:
	./scripts/check.sh

test: rust-test python-test ui-test

rust-test:
	cargo test --workspace

python-test:
	PYTHONPATH=python python3 -m unittest discover -s tests/python -p 'test_*.py'

ui-test:
	npm --prefix ui test

ui-build:
	npm --prefix ui run check
	npm --prefix ui run build

fmt:
	cargo fmt --all -- --check

paper:
	@set -eu; \
	: "$${IT_CONFIG:?set IT_CONFIG to a deployment-owned .cfg path}"; \
	: "$${IT_JOURNAL:?set IT_JOURNAL to a deployment-owned journal path}"; \
	: "$${IT_SOCKET:?set IT_SOCKET to a deployment-owned Unix socket path}"; \
	account="$${IT_ACCOUNT:-1}"; \
	test -f "$$IT_CONFIG"; \
	cargo run --locked -p insider-desktop-bridge -- serve \
		--config "$$IT_CONFIG" --journal "$$IT_JOURNAL" \
		--socket "$$IT_SOCKET" --account "$$account"

# Safe preflight: uses a private temporary directory and never binds an IPC
# socket or starts background workers. This is the composition-root check used
# by deployment automation before invoking the long-running `paper` command.
paper-check:
	@set -eu; tmp_dir=$$(mktemp -d "$${TMPDIR:-/tmp}/insidertrader-paper-check.XXXXXX"); trap 'rm -rf "$$tmp_dir"' EXIT INT TERM; cp config/example.cfg "$$tmp_dir/example.cfg"; cargo run --locked -p insider-desktop-bridge -- serve --check --config "$$tmp_dir/example.cfg" --journal "$$tmp_dir/runtime.journal" --socket "$$tmp_dir/runtime.sock" --account 1 --instrument 1 --symbol AAPL --price 100000; test -f "$$tmp_dir/runtime.journal"; test ! -e "$$tmp_dir/runtime.sock"
