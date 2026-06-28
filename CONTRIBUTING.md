# Contributing to rift-client

Thanks for your interest in contributing! This guide will help you get started.

## Development Setup

1. Install Rust (edition 2024 requires nightly or a recent stable):
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. Clone and build:
   ```bash
   git clone https://github.com/rift-proto/rift-client-rs.git
   cd rift-client
   cargo build
   ```

3. Run tests:
   ```bash
   cargo test
   ```

## Code Style

- Run `cargo fmt` before committing.
- Run `cargo clippy -- -D warnings` and fix all warnings.
- All public items must have doc comments (`#![deny(missing_docs)]` is enforced).
- Use `tracing` for logging, not `println!`.
- Avoid `unwrap()` in library code — use `?` or explicit error handling.

## Pull Request Process

1. Fork the repo and create a branch from `main`.
2. Make your changes, adding tests where appropriate.
3. Ensure `cargo test`, `cargo fmt --check`, and `cargo clippy` all pass.
4. Open a PR with a clear description of what changed and why.

## Reporting Issues

Open an issue on GitHub with:
- A clear title and description
- Steps to reproduce (if a bug)
- Expected vs actual behavior
- Your Rust version (`rustc --version`)

## License

By contributing, you agree that your contributions will be dual-licensed under
[MIT](LICENSE-MIT) and [Apache 2.0](LICENSE-APACHE).
