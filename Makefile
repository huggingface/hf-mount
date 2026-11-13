# Same commands as the CI lint and unit-test jobs (.github/workflows/ci.yml).
.PHONY: all build check clean doc fmt lint release test

# rustfmt toolchain of the CI format check: bump both together.
NIGHTLY := nightly-2026-04-22
FEATURES := fuse,nfs

all: fmt build

build:
	cargo build --features $(FEATURES)

check:
	cargo check --features $(FEATURES)

clean:
	cargo clean

doc:
	cargo doc --no-deps --features $(FEATURES)

fmt:
	cargo +$(NIGHTLY) fmt

lint:
	cargo +$(NIGHTLY) fmt --check
	cargo clippy --features $(FEATURES) -- -D warnings

release:
	cargo build --release --features $(FEATURES)

test:
	cargo test --lib --features $(FEATURES)
