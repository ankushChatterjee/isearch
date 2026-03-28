//! `isearch index` — build an on-disk inverted index under `~/.isearch/indexes/`.
//! `isearch query` — load that index, intersect sparse n-gram postings, verify literally.

mod index;
mod ngram;

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, Subcommand};
use dirs::home_dir;
use ignore::WalkBuilder;
use index::{
    write_bundle, DocId, Index, MmapBundle, PostingsReadTimings, index_bundle_path, pwd_hash,
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
    },
    /// Search for literal TEXT in the indexed corpus (uses sparse n-gram intersection, then verifies).
    Query {
        /// Literal string to find (not a regular expression).
        text: String,
        /// Indexed project root (must match the tree passed to `index`).
        #[arg(short, long, default_value = ".")]
        path: PathBuf,
    },
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Index { path } => run_index(path),
        Commands::Query { text, path } => run_query(text, path),
    }
}

fn run_index(path: PathBuf) -> io::Result<()> {
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
    let (store, index) = Index::ingest_files(&paths)?;
    eprintln!("  build total: {:.2}s", t_total.elapsed().as_secs_f64());

    write_bundle(&out_dir, &index, &store, &root)?;
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
    let (mut bundle, paths, open_reads) = MmapBundle::open(&bundle_dir)?;
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

    let t_query = Instant::now();
    // Sparse n-gram extraction needs length ≥ 2; shorter queries fall back to scanning all paths.
    let (candidates, postings_reads) = if query_bytes.len() < 2 {
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
    eprintln!(
        "candidates after n-gram AND: {} doc(s)  ({:.3}ms)  [postings.isearch read: {:.3}ms, {} list(s)]",
        candidates.len(),
        query_ms,
        postings_reads.ms,
        postings_reads.postings_lists_read,
    );

    let stdout = io::stdout();
    let mut out = stdout.lock();

    let t_verify = Instant::now();
    let mut verify_read_ms = 0.0f64;
    let mut result_count = 0usize;
    let mut first_file_block = true;
    for doc_id in &candidates {
        let Some(rel_path) = paths.get(doc_id.0 as usize) else {
            continue;
        };
        let t_read = Instant::now();
        let content = match fs::read_to_string(rel_path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{}: read error: {e}", rel_path);
                continue;
            }
        };
        verify_read_ms += t_read.elapsed().as_secs_f64() * 1000.0;

        let hits: Vec<(usize, &str)> = content
            .lines()
            .enumerate()
            .filter(|(_, line)| line.contains(&pattern))
            .map(|(i, line)| (i + 1, line))
            .collect();
        if hits.is_empty() {
            continue;
        }

        if !first_file_block {
            writeln!(out)?;
        }
        first_file_block = false;
        writeln!(out, "{}", query_result_path_display(rel_path, &root))?;
        for (line_no, line) in hits {
            writeln!(out, "{}:{}", line_no, line)?;
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
