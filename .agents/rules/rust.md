# rd-rs — Antigravity Project Rules

## Tests in Separate Files

- **Rule**: Do **not** use inline `#[cfg(test)]` modules inside `src/` files. All test code goes in the `tests/` directory as separate integration-test files (e.g. `tests/cache_tests.rs`, `tests/bitmap_tests.rs`).
- **Why**: Keeps production source files clean and under the 300-line limit; test files are independently navigable and compilable.
- **How**: Create `tests/<module>_tests.rs`, import with `use rd_rs::<module>::...`, and add the file to the workspace so `cargo test` picks it up automatically.

## File Length Limit (300 lines)

- **Rule**: No source file in `src/` or `tests/` may exceed **300 lines**.
- **When exceeded**: Split the file into submodules or sibling modules (e.g. extract helpers, a group of trait impls, or a coherent set of types into a separate file). Re-export via `mod`/`pub use` so the crate's public API stays unchanged.
- **Why**: Smaller files are easier to navigate, review, and test; they also keep compile times and IDE responsiveness better.

## General Rust Style

- Run `cargo clippy --all-targets --all-features -- -D warnings` before committing; all warnings must be resolved (no `#[allow(...)]` silencing unless genuinely unavoidable, with a comment explaining why).
- Prefer `thiserror` for library error types, `anyhow` for binary/top-level error propagation.
- Use `tracing` (not `println!` / `eprintln!`) for all runtime output.
- Crate edition is **2024**; use let-chain syntax (`if let ... && ...`) where clippy suggests it.

## Keep `config.toml` in Sync

- **Rule**: Whenever you add, rename, or change the default value of any field in `src/config/structs.rs` (or `src/config/defaults.rs`), you **must** also update `config.toml` to reflect that field.
- **How**: Add the field with its default value and a comment explaining what it controls. Use the same section headers (`[vfs]`, `[api]`, `[repair]`, etc.) as the structs use.
- **Why**: `config.toml` is the canonical example/reference config. If it drifts from the actual config struct, users get confusing behaviour (silent ignored keys or missing documentation).
