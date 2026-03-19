# rd-rs — Antigravity Project Rules

## File Length Limit (300 lines)

- **Rule**: No source file in `src/` or `tests/` may exceed **300 lines**.
- **When exceeded**: Split the file into submodules or sibling modules (e.g. extract helpers, a group of trait impls, or a coherent set of types into a separate file). Re-export via `mod`/`pub use` so the crate's public API stays unchanged.
- **Why**: Smaller files are easier to navigate, review, and test; they also keep compile times and IDE responsiveness better.

## General Rust Style

- Run `cargo clippy --all-targets --all-features -- -D warnings` before committing; all warnings must be resolved (no `#[allow(...)]` silencing unless genuinely unavoidable, with a comment explaining why).
- Prefer `thiserror` for library error types, `anyhow` for binary/top-level error propagation.
- Use `tracing` (not `println!` / `eprintln!`) for all runtime output.
- Crate edition is **2024**; use let-chain syntax (`if let ... && ...`) where clippy suggests it.
