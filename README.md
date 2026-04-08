# isearch

`isearch` is a fast local code/text search CLI built around a sparse n-gram inverted index, regex-aware prefiltering, and parallel verification.

It supports:
- `index`: build a persistent on-disk index bundle
- `query`: run regex search with n-gram candidate pruning
- `watch`: keep a live sidecar index up to date from filesystem changes
- `live`: interactive TUI search with embedded watcher status

Inspired by the Cursor post on fast regex search: <https://cursor.com/blog/fast-regex-search>

## Latest Features

- Sharded index bundles with configurable shard target size (`--shard-target-postings-bytes`)
- Spill-to-disk indexing path for large repositories (`--spill-*` knobs)
- Regex query planning using `regex-syntax` HIR literal extraction
- Candidate prefilter modes:
  - `NeverMatches` for unsatisfiable patterns
  - `AllDocs` fallback when no safe literals can be extracted
  - OR-of-AND literal hash groups for selective prefiltering
- Parallel candidate verification using Rayon
- Result format compatible with common grep workflows:
  - path header per file
  - `line_number:line_text` hits
  - one hit per matching line (deduplicates multiple matches on the same line)
- Query behavior parity checks against `rg` in test suite
- Incremental watch mode:
  - coalesced/debounced filesystem events
  - append-only delta log (`delta.bin`)
  - persisted watch state snapshot (`watch_state.bin`)
  - periodic compaction back into the base sharded bundle
- Single-process watch lock (`.watch.lock`) to avoid concurrent writers
- Live TUI mode:
  - interactive query input
  - scrollable results
  - watcher phase/status bar (`indexing`, `idle`, `updating`, `compacting`)
  - backend indicator (`watch-state` vs `mmap`)
  - bounded verification per query for responsiveness

## Install

```bash
cargo build --release
```

Binary will be available at `target/release/isearch`.

## Usage

### Build an index

```bash
isearch index .
```

Useful flags:
- `--spill-min-paths`
- `--spill-max-pairs-in-mem`
- `--spill-temp-dir`
- `--shard-target-postings-bytes`

### Run a query

```bash
isearch query "foo|bar" --path .
```

### Run incremental watcher

```bash
isearch watch . --debounce-ms 100 --compact-interval-secs 60
```

### Start live TUI

```bash
isearch live . --max-results 400
```

## How Does It Work

`isearch` is a two-stage search engine:

1. Build (offline or incremental): create/update an inverted index of n-gram hashes to document IDs.
2. Query (online): use regex-derived literals to prune candidate docs, then verify matches in file contents.

### 1) Index Build Pipeline

- Files are discovered recursively (respecting ignore rules via `ignore::WalkBuilder`).
- Each file is tokenized into n-grams (`src/ngram.rs`) and hashed.
- The index stores `(ngram_hash -> postings)` where postings are doc IDs.
- Postings are encoded compactly with varint + delta encoding.
- Single-doc postings can be inlined in lookup entries to avoid postings reads.
- For large datasets, builder can spill sorted `(hash, doc_id)` runs to disk and k-way merge.
- The final output is a sharded bundle under:
  - `~/.isearch/indexes/<pwd_hash>/index/`

### 2) On-Disk Layout

Top-level bundle files:
- `manifest.isearch`: shard metadata and doc ranges
- `paths.txt`: doc ID to absolute path mapping
- `meta.txt`: basic metadata

Per-shard files:
- `lookup.isearch`: sorted `(hash -> value)` table for binary search
- `postings.isearch`: postings payload blob

Both lookup and postings files have a fixed binary header with:
- magic
- format version
- flags
- payload size
- entry count

### 3) Query Execution

Given `isearch query <regex>`:

- Compile regex with `regex`.
- Build prefilter plan from regex HIR literals (`regex-syntax`):
  - If unsatisfiable: return empty immediately.
  - If no reliable literals: scan all docs.
  - Otherwise: create OR branches of ANDed n-gram hashes.
- Fetch candidate docs by intersecting postings lists per branch, then unioning branches.
- Verify candidates by reading files and executing the full regex.
- Emit matching lines with line numbers.

### 4) Watch / Incremental Updates

`isearch watch` runs a long-lived filesystem watcher:

- Coalesces noisy file events with debounce.
- Applies per-file add/update/delete as delta operations.
- Appends ops to `delta.bin`.
- Maintains persistent document/hash state in `watch_state.bin`.
- Periodically compacts:
  - rebuilds a fresh sharded base bundle from live docs
  - resets delta log
  - rewrites state with compacted doc IDs

When querying, if watch state is available, candidates can be sourced from replayed watch state + delta without requiring full mmap postings traversal.

### 5) Live TUI Mode

`isearch live` embeds watcher + search UI:

- Starts watcher in background thread.
- Runs debounced searches as user types.
- Uses watch-state backend when available, falls back to mmap bundle otherwise.
- Renders results and watcher/search status continuously.

## Testing

Run full local suite (unit + integration + CLI):

```bash
cargo test --all-targets
```

Notable coverage includes:
- CLI behavior and help output
- regex planning behavior
- end-to-end regex search cases
- parity checks against `rg`
- watch state/delta replay and compaction behavior

## Coverage

Coverage is enforced in CI via `cargo-llvm-cov`.

Local run:

```bash
cargo install cargo-llvm-cov
MIN_COVERAGE=60 ./scripts/coverage.sh
```
