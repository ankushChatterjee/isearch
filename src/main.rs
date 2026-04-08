//! `isearch index` — build an on-disk inverted index under `~/.isearch/indexes/`.
//! `isearch query` — load that index, intersect sparse n-gram postings, verify with regex.

mod index;
mod live;
mod ngram;
mod regex_plan;
mod verify;
mod watch;

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, Subcommand};
use dirs::home_dir;
use ignore::WalkBuilder;
use index::{
    build_sharded_bundle, index_bundle_path, pwd_hash, DocId, PostingsReadTimings, ShardedBundle,
    SpillOptions, DEFAULT_TARGET_POSTINGS_BYTES,
};
use regex_plan::PrefilterPlan;
/// `./relative/path` under `root`, or the path as given with `/` separators.
fn query_result_path_display(file_path: &str, root: &Path) -> String {
    let p = Path::new(file_path);
    if let Ok(rel) = p.strip_prefix(root) {
        let s = rel.to_string_lossy();
        if s.is_empty() {
            "./".to_string()
        } else {
            format!("./{}", s.replace('\\', "/"))
        }
    } else {
        p.to_string_lossy().replace('\\', "/")
    }
}

#[derive(Parser, Debug)]
#[command(name = "isearch")]
#[command(about = "Sparse n-gram index with regex search (`index` / `query` / `live`).")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Recursively index files under PATH (default `.`) and write
    /// `~/.isearch/indexes/<pwd_hash>/index/`.
    Index {
        /// Directory to crawl (respects .gitignore when present).
        #[arg(default_value = ".")]
        path: PathBuf,
        /// Enable spill mode automatically only for projects with at least this many paths.
        #[arg(long, default_value_t = 100_000)]
        spill_min_paths: usize,
        /// Max in-memory pair buffer size before flushing a spill run.
        #[arg(long, default_value_t = 20_000_000)]
        spill_max_pairs_in_mem: usize,
        /// Directory for spill run files. Default is bundle-local temp staging.
        #[arg(long)]
        spill_temp_dir: Option<PathBuf>,
        /// Max postings payload bytes per shard.
        #[arg(long, default_value_t = DEFAULT_TARGET_POSTINGS_BYTES)]
        shard_target_postings_bytes: u64,
    },
    /// Search the indexed corpus with a Rust regex (sparse n-gram intersection when literals are extracted, then verifies).
    Query {
        /// Regular expression (Rust `regex` syntax).
        text: String,
        /// Indexed project root (must match the tree passed to `index`).
        #[arg(short, long, default_value = ".")]
        path: PathBuf,
    },
    /// Watch for filesystem changes and maintain incremental live index sidecars.
    Watch {
        /// Directory to watch recursively.
        #[arg(default_value = ".")]
        path: PathBuf,
        /// Debounce interval for coalescing noisy editor events.
        #[arg(long, default_value_t = 100)]
        debounce_ms: u64,
        /// Timer-based compaction interval in seconds.
        #[arg(long, default_value_t = 60)]
        compact_interval_secs: u64,
        /// Max number of coalesced paths applied per event-loop iteration.
        #[arg(long, default_value_t = 256)]
        max_batch_files: usize,
        /// Emit verbose watcher lifecycle logs to stderr.
        #[arg(long, default_value_t = false)]
        verbose: bool,
    },
    /// Launch interactive live-search TUI with embedded watcher + status bar.
    Live {
        /// Directory to watch/search recursively.
        #[arg(default_value = ".")]
        path: PathBuf,
        /// Debounce interval for coalescing noisy editor events.
        #[arg(long, default_value_t = 100)]
        debounce_ms: u64,
        /// Timer-based compaction interval in seconds.
        #[arg(long, default_value_t = 60)]
        compact_interval_secs: u64,
        /// Max number of coalesced paths applied per event-loop iteration.
        #[arg(long, default_value_t = 256)]
        max_batch_files: usize,
        /// Maximum result rows shown in the TUI list.
        #[arg(long, default_value_t = 400)]
        max_results: usize,
    },
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Index {
            path,
            spill_min_paths,
            spill_max_pairs_in_mem,
            spill_temp_dir,
            shard_target_postings_bytes,
        } => run_index(
            path,
            spill_min_paths,
            spill_max_pairs_in_mem,
            spill_temp_dir,
            shard_target_postings_bytes,
        ),
        Commands::Query { text, path } => run_query(text, path),
        Commands::Watch {
            path,
            debounce_ms,
            compact_interval_secs,
            max_batch_files,
            verbose,
        } => run_watch(
            path,
            debounce_ms,
            compact_interval_secs,
            max_batch_files,
            verbose,
        ),
        Commands::Live {
            path,
            debounce_ms,
            compact_interval_secs,
            max_batch_files,
            max_results,
        } => run_live(
            path,
            debounce_ms,
            compact_interval_secs,
            max_batch_files,
            max_results,
        ),
    }
}

fn run_index(
    path: PathBuf,
    spill_min_paths: usize,
    spill_max_pairs_in_mem: usize,
    spill_temp_dir: Option<PathBuf>,
    shard_target_postings_bytes: u64,
) -> io::Result<()> {
    let root = std::fs::canonicalize(&path)
        .map_err(|e| io::Error::other(format!("canonicalize {}: {e}", path.display())))?;
    let hash = pwd_hash(&root);
    let home = home_dir().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "could not resolve home directory")
    })?;
    let out_dir = index_bundle_path(&home, &hash);

    eprintln!("── index ─────────────────────────────────────────────");
    eprintln!("  root     : {}", root.display());
    eprintln!("  output   : {}", out_dir.display());

    eprint!("  scanning directory...");
    let _ = io::stderr().flush();
    let mut paths: Vec<String> = WalkBuilder::new(&root)
        .sort_by_file_path(std::cmp::Ord::cmp)
        .build()
        .filter_map(|entry| entry.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .map(|e| e.path().to_string_lossy().into_owned())
        .collect();
    paths.sort();
    eprintln!("\r  scanning directory → {} paths", paths.len());

    let t_total = Instant::now();
    let spill_options = SpillOptions {
        spill_min_paths,
        spill_max_pairs_in_mem,
        spill_temp_dir,
    };
    build_sharded_bundle(
        &root,
        &paths,
        &spill_options,
        &out_dir,
        shard_target_postings_bytes,
    )?;
    eprintln!("  build total: {:.2}s", t_total.elapsed().as_secs_f64());
    eprintln!("  wrote bundle under {}", out_dir.display());

    Ok(())
}

fn run_query(pattern: String, path: PathBuf) -> io::Result<()> {
    if pattern.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty search text",
        ));
    }

    let plan = regex_plan::build_regex_plan(&pattern)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid regex: {e}")))?;

    let t_query_total = Instant::now();

    let root = std::fs::canonicalize(&path)
        .map_err(|e| io::Error::other(format!("canonicalize {}: {e}", path.display())))?;
    let hash = pwd_hash(&root);
    let home = home_dir().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "could not resolve home directory")
    })?;
    let bundle_dir = index_bundle_path(&home, &hash);

    if !bundle_dir.is_dir() {
        return Err(io::Error::other(format!(
            "no index at {} — run `isearch index` for {}",
            bundle_dir.display(),
            root.display()
        )));
    }

    let t_load = Instant::now();
    let (bundle, paths) = ShardedBundle::open(&bundle_dir)?;
    let load_ms = t_load.elapsed().as_secs_f64() * 1000.0;
    eprintln!("opened bundle in {:.3}ms ({} docs)", load_ms, paths.len());

    let mut effective_doc_paths: Option<HashMap<u32, String>> = None;
    let t_query = Instant::now();
    let (candidates, postings_reads) = if let Some(docs) = watch::load_query_docs(&bundle_dir)? {
        let candidate_doc_ids = match &plan.prefilter {
            PrefilterPlan::NeverMatches => Vec::new(),
            PrefilterPlan::AllDocs => docs.iter().map(|(doc_id, _, _)| DocId(*doc_id)).collect(),
            PrefilterPlan::Union(_) => {
                regex_plan::filter_watch_docs_by_prefilter(&docs, &plan.prefilter)
            }
        };
        let mut map = HashMap::with_capacity(docs.len());
        for (doc_id, path, _) in docs {
            map.insert(doc_id, path);
        }
        effective_doc_paths = Some(map);
        (candidate_doc_ids, PostingsReadTimings::default())
    } else {
        regex_plan::sharded_candidates(&bundle, paths.len(), &plan.prefilter)?
    };
    let query_ms = t_query.elapsed().as_secs_f64() * 1000.0;
    if effective_doc_paths.is_some() {
        eprintln!(
            "candidates after n-gram AND: {} doc(s)  ({:.3}ms)  [source: watch_state+delta replay]",
            candidates.len(),
            query_ms,
        );
    } else {
        eprintln!(
            "candidates after n-gram AND: {} doc(s)  ({:.3}ms)  [postings.isearch read: {:.3}ms, {} list(s)]",
            candidates.len(),
            query_ms,
            postings_reads.ms,
            postings_reads.postings_lists_read,
        );
    }

    let stdout = io::stdout();
    let mut out = stdout.lock();

    let t_verify = Instant::now();
    let verify_results = if matches!(plan.prefilter, PrefilterPlan::NeverMatches) {
        Vec::new()
    } else if let Some(path_map) = &effective_doc_paths {
        let candidate_pairs: Vec<(DocId, String)> = candidates
            .iter()
            .filter_map(|doc| path_map.get(&doc.0).map(|p| (*doc, p.clone())))
            .collect();
        verify::verify_doc_paths_parallel_regex(&candidate_pairs, &plan.regex)
    } else {
        verify::verify_candidates_parallel_regex(&candidates, &paths, &plan.regex)
    };

    let verify_read_ms: f64 = verify_results.iter().map(|v| v.read_ms).sum();
    let mut result_count = 0usize;
    for (idx, v) in verify_results.iter().enumerate() {
        if idx > 0 {
            writeln!(out)?;
        }
        writeln!(out, "{}", query_result_path_display(&v.rel_path, &root))?;
        for hit in &v.hits {
            writeln!(out, "{}:{}", hit.line_no, hit.line)?;
            result_count += 1;
        }
    }
    let verify_ms = t_verify.elapsed().as_secs_f64() * 1000.0;
    eprintln!(
        "verify: {} file(s) scanned  ({:.3}ms)  [candidate file read I/O: {:.3}ms]",
        candidates.len(),
        verify_ms,
        verify_read_ms
    );

    let total_s = t_query_total.elapsed().as_secs_f64();
    let total_ms = total_s * 1000.0;
    if total_s >= 1.0 {
        eprintln!("Found {} result(s) in {:.2}s", result_count, total_s);
    } else {
        eprintln!("Found {} result(s) in {:.3}ms", result_count, total_ms);
    }

    Ok(())
}

fn run_watch(
    path: PathBuf,
    debounce_ms: u64,
    compact_interval_secs: u64,
    max_batch_files: usize,
    verbose: bool,
) -> io::Result<()> {
    let root = std::fs::canonicalize(&path)
        .map_err(|e| io::Error::other(format!("canonicalize {}: {e}", path.display())))?;
    let hash = pwd_hash(&root);
    let home = home_dir().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "could not resolve home directory")
    })?;
    let bundle_dir = index_bundle_path(&home, &hash);

    if !watch::has_base_bundle(&bundle_dir) {
        eprintln!("no baseline bundle found; building initial index first...");
        run_index(
            path,
            100_000,
            20_000_000,
            None,
            DEFAULT_TARGET_POSTINGS_BYTES,
        )?;
    }

    watch::run(watch::WatchConfig {
        root,
        bundle_dir,
        debounce_ms,
        compact_interval_secs,
        max_batch_files,
        verbose,
        log_to_stderr: true,
        status_tx: None,
    })
}

fn run_live(
    path: PathBuf,
    debounce_ms: u64,
    compact_interval_secs: u64,
    max_batch_files: usize,
    max_results: usize,
) -> io::Result<()> {
    let root = std::fs::canonicalize(&path)
        .map_err(|e| io::Error::other(format!("canonicalize {}: {e}", path.display())))?;
    let hash = pwd_hash(&root);
    let home = home_dir().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "could not resolve home directory")
    })?;
    let bundle_dir = index_bundle_path(&home, &hash);

    if !watch::has_base_bundle(&bundle_dir) {
        eprintln!("no baseline bundle found; building initial index first...");
        run_index(
            path,
            100_000,
            20_000_000,
            None,
            DEFAULT_TARGET_POSTINGS_BYTES,
        )?;
    }

    live::run(live::LiveConfig {
        root,
        bundle_dir,
        debounce_ms,
        compact_interval_secs,
        max_batch_files,
        max_results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_result_path_display_is_relative_for_files_under_root() {
        let root = PathBuf::from("/tmp/project");
        let p = root.join("src/lib.rs");
        assert_eq!(
            query_result_path_display(p.to_string_lossy().as_ref(), &root),
            "./src/lib.rs"
        );
    }

    #[test]
    fn query_result_path_display_keeps_original_when_outside_root() {
        let root = PathBuf::from("/tmp/project");
        let p = PathBuf::from("/tmp/other/file.rs");
        assert_eq!(
            query_result_path_display(p.to_string_lossy().as_ref(), &root),
            "/tmp/other/file.rs"
        );
    }
}
