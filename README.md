isearch
========
full text file search
based on: https://cursor.com/blog/fast-regex-search

under construction. dont ask timelines.

## Testing

Run the complete local suite (unit + integration + CLI scenarios):

```bash
cargo test --all-targets
```

## Coverage

Coverage is enforced in CI via `cargo-llvm-cov`.

Local run:

```bash
cargo install cargo-llvm-cov
MIN_COVERAGE=60 ./scripts/coverage.sh
```
