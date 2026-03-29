//! Index construction: parallel ingest, sort, dedup, group, serialize.

use std::fs;
use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use rayon::prelude::*;

use crate::ngram;

use super::types::{DocId, DocStore, Index, LookupTable, Posting, PostingsBlob};

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
    pub fn ingest_files(paths: &[String]) -> io::Result<(DocStore, Self)> {
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
