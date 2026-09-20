# Spatter

## Workflow

- Never commit directly to `main`.
- Do development on a separate branch (`git checkout -b <topic>`), push it, and open a PR for user review.
- Only merge after the user approves.

## Commands

- Build/test: `cargo test` (workspace). Lint: `cargo clippy --all-targets -- -D warnings`. Format: `cargo fmt`.
- Rust toolchain via `source "$HOME/.cargo/env"`.
- Integration test: `cargo run --release --example wordcount -- [--cluster N] /tmp/gospark-wc-big.txt`.