.PHONY: check test rust-test python-test ui-test ui-build fmt paper

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
	@echo "Use README.md's paper-start command with an explicit .cfg, journal, socket, and account."
