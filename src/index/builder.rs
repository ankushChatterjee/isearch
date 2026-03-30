//! Index construction: parallel ingest, sort, dedup, group, serialize.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use rayon::prelude::*;

use crate::ngram;

use super::spill;
use super::types::{DocId, DocStore, Index, LookupTable, Posting, PostingsBlob};

#[derive(Debug, Clone)]
pub struct SpillOptions {
    pub spill_min_paths: usize,
    pub spill_max_pairs_in_mem: usize,
    pub spill_temp_dir: Option<PathBuf>,
}

impl Default for SpillOptions {
    fn default() -> Self {
        Self {
            spill_min_paths: 100_000,
            spill_max_pairs_in_mem: 20_000_000,
            spill_temp_dir: None,
        }
    }
}

pub enum BuildOutput {
    InMemory(Index),
    SpilledToDisk,
}

/// Skip files that look binary (NUL byte anywhere in the first 8 KB).
fn is_binary(bytes: &[u8]) -> bool {
    bytes[..bytes.len().min(8 * 1024)].contains(&0)
}

impl PostingsBlob {
    pub(crate) fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Append one posting list; returns the byte offset written.
    pub(crate) fn append(&mut self, posting: &Posting) -> u64 {
        let offset = self.data.len() as u64;
        self.data.extend_from_slice(&(posting.doc_ids.len() as u32).to_le_bytes());
        for &DocId(id) in &posting.doc_ids {
            self.data.extend_from_slice(&id.to_le_bytes());
        }
        offset
    }

    /// Build blob + lookup table from postings sorted by hash.
    pub(crate) fn from_postings(postings: &[Posting]) -> (Self, LookupTable) {
        let mut blob   = Self::new();
        let mut lookup = LookupTable::new();
        for posting in postings {
            let offset = blob.append(posting);
            lookup.push(posting.hash, offset);
        }
        (blob, lookup)
    }
}

impl LookupTable {
    pub(crate) fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub(crate) fn push(&mut self, hash: u32, offset: u64) {
        self.entries.push(super::types::LookupEntry { hash, offset });
    }
}

impl Index {
    /// Read paths in parallel, extract n-gram hashes per file, then assign
    /// [`DocId`]s in path order and finish the index pipeline.
    #[allow(dead_code)] // legacy entrypoint retained for callers/tests
    pub fn ingest_files(paths: &[String]) -> io::Result<(DocStore, Self)> {
        // Keep legacy entrypoint behavior for existing callers.
        const MAX_FILE_BYTES: usize = 2 * 1024 * 1024 * 1024;
        const BATCH_SIZE: usize = 512;
        let total = paths.len();

        let done = AtomicUsize::new(0);
        eprint!("  [1/5] read + extract  {:>5} / {}", 0, total);
        let _ = io::stderr().flush();
        let t = Instant::now();

        let mut store = DocStore::new();
        let mut pairs: Vec<(u32, DocId)> = Vec::new();
        let mut skipped = 0usize;

        for chunk in paths.chunks(BATCH_SIZE) {
            let extracted: Vec<io::Result<Option<Vec<u32>>>> = chunk
                .par_iter()
                .map(|path| {
                    let bytes = fs::read(path)
                        .map_err(|e| io::Error::other(format!("read {path}: {e}")))?;
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if n % 256 == 0 || n == total {
                        eprint!("\r  [1/5] read + extract  {:>5} / {}", n, total);
                        let _ = io::stderr().flush();
                    }
                    if is_binary(&bytes) || bytes.len() > MAX_FILE_BYTES {
                        return Ok(None);
                    }
                    let mut hashes: Vec<u32> = ngram::extract_all_ngrams(&bytes)
                        .map(ngram::hash_ngram)
                        .collect();
                    hashes.sort_unstable();
                    hashes.dedup();
                    Ok(Some(hashes))
                })
                .collect();

            for (path, result) in chunk.iter().zip(extracted) {
                match result? {
                    None => skipped += 1,
                    Some(hashes) => {
                        let doc_id = store.add_path(path);
                        pairs.extend(hashes.into_iter().map(|h| (h, doc_id)));
                    }
                }
            }
        }

        eprintln!(
            "\r  [1/5] read + extract  {} docs → {} pairs, {} skipped  ({:.2}s)",
            store.len(),
            pairs.len(),
            skipped,
            t.elapsed().as_secs_f64()
        );

        let index = Self::build_from_pairs(pairs);
        Ok((store, index))
    }

    /// Ingest with automatic external spilling for large corpora.
    pub fn ingest_files_with_spill_options(
        paths: &[String],
        options: &SpillOptions,
        bundle_dir: &Path,
    ) -> io::Result<(DocStore, BuildOutput)> {
        const MAX_FILE_BYTES: usize = 2 * 1024 * 1024 * 1024;
        const BATCH_SIZE: usize = 512;
        let total = paths.len();

        let done = AtomicUsize::new(0);
        eprint!("  [1/5] read + extract  {:>5} / {}", 0, total);
        let _ = io::stderr().flush();
        let t = Instant::now();

        let mut store = DocStore::new();
        let mut pairs: Vec<(u32, DocId)> = Vec::new();
        let mut skipped = 0usize;

        let start_spill = total >= options.spill_min_paths;
        let mut spill_state = if start_spill {
            Some(SpillState::new(options, bundle_dir)?)
        } else {
            None
        };

        for chunk in paths.chunks(BATCH_SIZE) {
            let extracted: Vec<io::Result<Option<Vec<u32>>>> = chunk
                .par_iter()
                .map(|path| {
                    let bytes = fs::read(path)
                        .map_err(|e| io::Error::other(format!("read {path}: {e}")))?;
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if n % 256 == 0 || n == total {
                        eprint!("\r  [1/5] read + extract  {:>5} / {}", n, total);
                        let _ = io::stderr().flush();
                    }
                    if is_binary(&bytes) || bytes.len() > MAX_FILE_BYTES {
                        return Ok(None);
                    }
                    let mut hashes: Vec<u32> = ngram::extract_all_ngrams(&bytes)
                        .map(ngram::hash_ngram)
                        .collect();
                    hashes.sort_unstable();
                    hashes.dedup();
                    Ok(Some(hashes))
                })
                .collect();

            for (path, result) in chunk.iter().zip(extracted) {
                match result? {
                    None => skipped += 1,
                    Some(hashes) => {
                        let doc_id = store.add_path(path);
                        pairs.extend(hashes.into_iter().map(|h| (h, doc_id)));
                        if spill_state.is_none() && pairs.len() >= options.spill_max_pairs_in_mem {
                            eprintln!(
                                "\n  [spill] activating spill mode at {} docs ({} pairs)",
                                store.len(),
                                pairs.len()
                            );
                            spill_state = Some(SpillState::new(options, bundle_dir)?);
                        }
                        if let Some(state) = spill_state.as_mut() {
                            if pairs.len() >= options.spill_max_pairs_in_mem {
                                state.flush(&mut pairs)?;
                            }
                        }
                    }
                }
            }
        }

        eprintln!(
            "\r  [1/5] read + extract  {} docs → {} buffered pairs, {} skipped  ({:.2}s)",
            store.len(),
            pairs.len(),
            skipped,
            t.elapsed().as_secs_f64()
        );

        if let Some(mut state) = spill_state {
            if !pairs.is_empty() {
                state.flush(&mut pairs)?;
            }
            eprint!("  [2/5] merge {} spill run(s)...", state.runs.len());
            let _ = io::stderr().flush();
            let t = Instant::now();
            let stats = spill::merge_runs_to_index_files(&state.runs, bundle_dir)?;
            eprintln!(
                "\r  [2/5] merged {} run(s) → {} lookup row(s), {} unique / {} merged pairs, postings {} bytes  ({:.2}s)",
                stats.run_count,
                stats.lookup_rows,
                stats.unique_pairs,
                stats.merged_pairs,
                stats.postings_payload_bytes,
                t.elapsed().as_secs_f64()
            );
            state.cleanup_success();
            return Ok((store, BuildOutput::SpilledToDisk));
        }

        let index = Self::build_from_pairs(pairs);
        Ok((store, BuildOutput::InMemory(index)))
    }

    fn build_from_pairs(mut pairs: Vec<(u32, DocId)>) -> Self {
        eprint!("  [2/5] sorting {} pairs...", pairs.len());
        let _ = io::stderr().flush();
        let t = Instant::now();
        pairs.par_sort_unstable_by_key(|&(h, DocId(d))| (h, d));
        eprintln!("\r  [2/5] sorted {} pairs  ({:.2}s)", pairs.len(), t.elapsed().as_secs_f64());

        eprint!("  [3/5] dedup {} pairs...", pairs.len());
        let _ = io::stderr().flush();
        let t = Instant::now();
        pairs.dedup();
        eprintln!(
            "\r  [3/5] dedup → {} unique pairs  ({:.2}s)",
            pairs.len(),
            t.elapsed().as_secs_f64()
        );

        eprint!("  [4/5] grouping postings...");
        let _ = io::stderr().flush();
        let t = Instant::now();
        let mut postings_list: Vec<Posting> = Vec::new();
        let mut i = 0;
        while i < pairs.len() {
            let hash = pairs[i].0;
            let j = i + pairs[i..].partition_point(|&(h, _)| h == hash);
            let doc_ids = pairs[i..j].iter().map(|&(_, d)| d).collect();
            postings_list.push(Posting { hash, doc_ids });
            i = j;
        }
        eprintln!(
            "\r  [4/5] grouping → {} postings  ({:.2}s)",
            postings_list.len(),
            t.elapsed().as_secs_f64()
        );

        eprint!("  [5/5] serializing postings blob...");
        let _ = io::stderr().flush();
        let t = Instant::now();
        let (postings, lookup) = PostingsBlob::from_postings(&postings_list);
        eprintln!(
            "\r  [5/5] serialized → lookup {} entries ({} bytes), postings {} bytes  ({:.2}s)",
            lookup.len(),
            lookup.byte_size(),
            postings.byte_size(),
            t.elapsed().as_secs_f64()
        );
        Self { lookup, postings }
    }
}

struct SpillState {
    temp_dir: PathBuf,
    runs: Vec<PathBuf>,
    next_run_idx: usize,
}

impl SpillState {
    fn new(options: &SpillOptions, bundle_dir: &Path) -> io::Result<Self> {
        let temp_dir = match &options.spill_temp_dir {
            Some(p) => p.clone(),
            None => {
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis();
                bundle_dir.join(format!(".spill-{}-{ts}", process::id()))
            }
        };
        fs::create_dir_all(&temp_dir)?;
        Ok(Self {
            temp_dir,
            runs: Vec::new(),
            next_run_idx: 0,
        })
    }

    fn flush(&mut self, pairs: &mut Vec<(u32, DocId)>) -> io::Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let run = spill::flush_run(&self.temp_dir, self.next_run_idx, pairs)?;
        self.next_run_idx += 1;
        self.runs.push(run);
        Ok(())
    }

    fn cleanup_success(&mut self) {
        let _ = fs::remove_dir_all(&self.temp_dir);
    }
}
