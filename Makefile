CARGO_DIR := project/Cargo.toml

.PHONY: all test format run lint

format:
	cargo fmt  --quiet --manifest-path $(CARGO_DIR)

lint:
	cargo clippy --quiet --manifest-path $(CARGO_DIR)

test:
	cargo test --quiet --manifest-path $(CARGO_DIR)
run:
	cargo run --quiet --manifest-path $(CARGO_DIR)

run-release:
	cargo run --release --bin my_binary

all: format lint test run