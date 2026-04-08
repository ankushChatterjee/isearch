#!/usr/bin/env bash
set -euo pipefail

min_coverage="${MIN_COVERAGE:-60}"

if ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "cargo-llvm-cov is required. Install with:"
  echo "  cargo install cargo-llvm-cov"
  exit 1
fi

if [[ -z "${LLVM_COV:-}" ]] && command -v xcrun >/dev/null 2>&1; then
  LLVM_COV="$(xcrun --find llvm-cov 2>/dev/null || true)"
  export LLVM_COV
fi

if [[ -z "${LLVM_PROFDATA:-}" ]] && command -v xcrun >/dev/null 2>&1; then
  LLVM_PROFDATA="$(xcrun --find llvm-profdata 2>/dev/null || true)"
  export LLVM_PROFDATA
fi

echo "Running coverage with minimum threshold: ${min_coverage}%"
cargo llvm-cov \
  --all-features \
  --all-targets \
  --workspace \
  --fail-under-lines "${min_coverage}" \
  --summary-only
