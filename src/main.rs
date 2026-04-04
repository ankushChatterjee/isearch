//! `isearch index` — build an on-disk inverted index under `~/.isearch/indexes/`.
//! `isearch query` — load that index, intersect sparse n-gram postings, verify literally.

mod index;
mod ngram;
mod verify;
mod watch;

use std::io::{self, Write};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, Subcommand};
use dirs::home_dir;
use ignore::WalkBuilder;
use index::{
    write_bundle, write_paths_and_meta, BuildOutput, DocId, Index, MmapBundle, PostingsReadTimings,
    SpillOptions, index_bundle_path, pwd_hash,
};
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
#[command(about = "Sparse n-gram index (`index` / `query`).")]
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
    },
    /// Search for literal TEXT in the indexed corpus (uses sparse n-gram intersection, then verifies).
    Query {
        /// Literal string to find (not a regular expression).
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
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Index {
            path,
            spill_min_paths,
            spill_max_pairs_in_mem,
            spill_temp_dir,
        } => run_index(path, spill_min_paths, spill_max_pairs_in_mem, spill_temp_dir),
        Commands::Query { text, path } => run_query(text, path),
        Commands::Watch {
            path,
            debounce_ms,
            compact_interval_secs,
            max_batch_files,
            verbose,
        } => run_watch(path, debounce_ms, compact_interval_secs, max_batch_files, verbose),
    }
}

fn run_index(
    path: PathBuf,
    spill_min_paths: usize,
    spill_max_pairs_in_mem: usize,
    spill_temp_dir: Option<PathBuf>,
) -> io::Result<()> {
    let root = std::fs::canonicalize(&path).map_err(|e| {
        io::Error::other(format!("canonicalize {}: {e}", path.display()))
    })?;
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
    let (store, build) = Index::ingest_files_with_spill_options(&paths, &spill_options, &out_dir)?;
    eprintln!("  build total: {:.2}s", t_total.elapsed().as_secs_f64());

    match build {
        BuildOutput::InMemory(index) => {
            write_bundle(&out_dir, &index, &store, &root)?;
        }
        BuildOutput::SpilledToDisk => {
            write_paths_and_meta(&out_dir, &store, &root)?;
        }
    }
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

    let t_query_total = Instant::now();

    let root = std::fs::canonicalize(&path).map_err(|e| {
        io::Error::other(format!("canonicalize {}: {e}", path.display()))
    })?;
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
    let (bundle, paths, open_reads) = MmapBundle::open(&bundle_dir)?;
    let load_ms = t_load.elapsed().as_secs_f64() * 1000.0;
    eprintln!(
        "opened bundle in {:.3}ms ({} docs)  [file reads: lookup open+mmap {:.3}ms, postings open+header {:.3}ms, paths.txt {:.3}ms]",
        load_ms,
        paths.len(),
        open_reads.lookup_open_and_mmap_ms,
        open_reads.postings_open_and_header_ms,
        open_reads.paths_file_read_ms,
    );

    let query_bytes = pattern.as_bytes();

    let mut effective_doc_paths: Option<HashMap<u32, String>> = None;
    let t_query = Instant::now();
    // Sparse n-gram extraction needs length ≥ 2; shorter queries fall back to scanning all docs.
    let (candidates, postings_reads) = if let Some(docs) = watch::load_query_docs(&bundle_dir)? {
        let candidate_doc_ids = if query_bytes.len() < 2 {
            docs.iter().map(|(doc_id, _, _)| DocId(*doc_id)).collect()
        } else {
            let covering = ngram::covering_ngrams(query_bytes);
            let hashes: Vec<u32> = covering.iter().map(|ng| ngram::hash_ngram(ng)).collect();
            docs.iter()
                .filter(|(_, _, doc_hashes)| {
                    hashes
                        .iter()
                        .all(|h| doc_hashes.binary_search(h).is_ok())
                })
                .map(|(doc_id, _, _)| DocId(*doc_id))
                .collect()
        };
        let mut map = HashMap::with_capacity(docs.len());
        for (doc_id, path, _) in docs {
            map.insert(doc_id, path);
        }
        effective_doc_paths = Some(map);
        (candidate_doc_ids, PostingsReadTimings::default())
    } else if query_bytes.len() < 2 {
        (
            (0..paths.len())
                .map(|i| DocId(i as u32))
                .collect::<Vec<_>>(),
            PostingsReadTimings::default(),
        )
    } else {
        let covering = ngram::covering_ngrams(query_bytes);
        let hashes: Vec<u32> = covering.iter().map(|ng| ngram::hash_ngram(ng)).collect();
        bundle.candidates(&hashes)?
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
    let verify_results = if let Some(path_map) = &effective_doc_paths {
        let candidate_pairs: Vec<(DocId, String)> = candidates
            .iter()
            .filter_map(|doc| path_map.get(&doc.0).map(|p| (*doc, p.clone())))
            .collect();
        verify::verify_doc_paths_parallel(&candidate_pairs, query_bytes)
    } else {
        verify::verify_candidates_parallel(&candidates, &paths, query_bytes)
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
        eprintln!(
            "Found {} result(s) in {:.2}s",
            result_count, total_s
        );
    } else {
        eprintln!(
            "Found {} result(s) in {:.3}ms",
            result_count, total_ms
        );
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
    let root = std::fs::canonicalize(&path).map_err(|e| {
        io::Error::other(format!("canonicalize {}: {e}", path.display()))
    })?;
    let hash = pwd_hash(&root);
    let home = home_dir().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "could not resolve home directory")
    })?;
    let bundle_dir = index_bundle_path(&home, &hash);

    if !watch::has_base_bundle(&bundle_dir) {
        eprintln!("no baseline bundle found; building initial index first...");
        run_index(path, 100_000, 20_000_000, None)?;
    }

    watch::run(watch::WatchConfig {
        root,
        bundle_dir,
        debounce_ms,
        compact_interval_secs,
        max_batch_files,
        verbose,
    })
}
