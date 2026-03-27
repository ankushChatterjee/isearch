#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

if ! command -v rg >/dev/null 2>&1; then
  echo "error: ripgrep (rg) not found on PATH" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 required for timing" >&2
  exit 1
fi

"$SCRIPT_DIR/ensure_containerd.sh"

CORPUS="$ROOT/fixtures/containerd"
if [ ! -d "$CORPUS" ]; then
  echo "error: corpus missing at $CORPUS" >&2
  exit 1
fi

cargo build --release -q

ISEARCH="$ROOT/target/release/isearch"

run_compare() {
  local pat="$1"
  python3 - "$ROOT" "$CORPUS" "$ISEARCH" "$pat" <<'PY'
import subprocess
import sys
import time
from pathlib import Path

root = Path(sys.argv[1])
corpus = Path(sys.argv[2])
isearch = Path(sys.argv[3])
pat = sys.argv[4]
# -F on rg only: fixed strings (literal), matching isearch's substring search (not regex).
rg_flags = ["--no-heading", "--line-number", "--color", "never", "-F"]
isearch_flags = ["--no-heading", "--line-number", "--color", "never"]

def sort_lines(data: bytes) -> bytes:
    lines = data.splitlines(True)
    return b"".join(sorted(lines, key=lambda b: b))

# ripgrep: search corpus via cwd + "." (same tree as isearch, which only uses CWD).
t0 = time.perf_counter()
rg_out = subprocess.run(
    ["rg", *rg_flags, pat, "."],
    cwd=str(corpus),
    stdout=subprocess.PIPE,
    check=True,
).stdout
rg_ms = int((time.perf_counter() - t0) * 1000)
rg_sorted = sort_lines(rg_out)

# isearch: always searches current directory only.
t0 = time.perf_counter()
is_out = subprocess.run(
    [str(isearch), *isearch_flags, pat],
    cwd=str(corpus),
    stdout=subprocess.PIPE,
    check=True,
).stdout
is_ms = int((time.perf_counter() - t0) * 1000)
is_sorted = sort_lines(is_out)

if rg_sorted != is_sorted:
    print("MISMATCH for pattern:", pat, file=sys.stderr)
    (root / ".out_rg.txt").write_bytes(rg_sorted)
    (root / ".out_isearch.txt").write_bytes(is_sorted)
    sys.exit(1)

print(f'[OK] "{pat}"  rg={rg_ms}ms  isearch={is_ms}ms')
PY
}

echo "=== comparing isearch vs ripgrep on $CORPUS ==="

while IFS= read -r pat || [ -n "${pat:-}" ]; do
  [[ -z "${pat// }" ]] && continue
  [[ "$pat" == \#* ]] && continue
  echo "=== pattern: $pat ==="
  run_compare "$pat"
done < "$SCRIPT_DIR/search_candidates.txt"

echo "All patterns matched. Harness complete."

if command -v hyperfine >/dev/null 2>&1; then
  echo ""
  echo "=== hyperfine (high-frequency patterns) ==="
  for hf in "Copyright The containerd Authors" "context.Context" "fmt.Errorf"; do
    hyperfine --warmup 1 --min-runs 2 \
      "cd \"$CORPUS\" && rg --no-heading --line-number --color never -F \"$hf\" ." \
      "cd \"$CORPUS\" && \"$ISEARCH\" --no-heading --line-number --color never \"$hf\"" \
      || true
  done
else
  echo "(install hyperfine for detailed benchmarks: https://github.com/sharkdp/hyperfine)"
fi
