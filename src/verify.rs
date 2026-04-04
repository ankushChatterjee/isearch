//! Literal substring verification after n-gram candidate filtering.

use std::fs;
use std::io;

use memchr::{memchr, memchr_iter, memmem, memrchr};
use rayon::prelude::*;

use crate::index::DocId;

#[derive(Debug)]
pub struct VerifyLine {
    pub line_no: usize,
    pub line: String,
}

#[derive(Debug)]
pub struct VerifyFileResult {
    pub doc_id: DocId,
    pub rel_path: String,
    pub read_ms: f64,
    pub hits: Vec<VerifyLine>,
}

/// Read one file and confirm literal `pattern` occurrences; map each match to its line text.
pub fn verify_candidate(
    path: &str,
    pattern: &[u8],
    doc_id: DocId,
) -> io::Result<Option<VerifyFileResult>> {
    let t_read = std::time::Instant::now();
    let bytes = fs::read(path)?;
    let read_ms = t_read.elapsed().as_secs_f64() * 1000.0;

    let matches: Vec<usize> = memmem::find_iter(&bytes, pattern).collect();
    if matches.is_empty() {
        return Ok(None);
    }

    // Preserve prior behavior: skip files that are not UTF-8.
    if let Err(e) = std::str::from_utf8(&bytes) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, e));
    }

    let mut hits = Vec::new();
    let mut last_line_start = None::<usize>;
    for &off in &matches {
        let line_start = memrchr(b'\n', &bytes[..off]).map_or(0usize, |i| i + 1);
        if last_line_start == Some(line_start) {
            continue;
        }
        last_line_start = Some(line_start);

        let rel_end = memchr(b'\n', &bytes[off..]).unwrap_or(bytes.len() - off);
        let line_end = off + rel_end;
        let line_no = memchr_iter(b'\n', &bytes[..line_start]).count() + 1;

        let line_bytes = if line_end > line_start && bytes[line_end - 1] == b'\r' {
            &bytes[line_start..line_end - 1]
        } else {
            &bytes[line_start..line_end]
        };
        let line = std::str::from_utf8(line_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .to_owned();
        hits.push(VerifyLine { line_no, line });
    }

    if hits.is_empty() {
        return Ok(None);
    }

    Ok(Some(VerifyFileResult {
        doc_id,
        rel_path: path.to_owned(),
        read_ms,
        hits,
    }))
}

/// Parallel verification over candidate doc ids (paths resolved from `paths`).
pub fn verify_candidates_parallel(
    candidates: &[DocId],
    paths: &[String],
    pattern: &[u8],
) -> Vec<VerifyFileResult> {
    let mut verify_results: Vec<VerifyFileResult> = candidates
        .par_iter()
        .filter_map(|&doc_id| {
            let rel_path = paths.get(doc_id.0 as usize)?;
            match verify_candidate(rel_path, pattern, doc_id) {
                Ok(Some(v)) => Some(v),
                Ok(None) => None,
                Err(e) => {
                    eprintln!("{}: read error: {e}", rel_path);
                    None
                }
            }
        })
        .collect();

    verify_results.sort_unstable_by_key(|v| v.doc_id);
    verify_results
}

/// Parallel verification over explicit `(DocId, path)` pairs.
pub fn verify_doc_paths_parallel(
    candidates: &[(DocId, String)],
    pattern: &[u8],
) -> Vec<VerifyFileResult> {
    let mut verify_results: Vec<VerifyFileResult> = candidates
        .par_iter()
        .filter_map(|(doc_id, rel_path)| match verify_candidate(rel_path, pattern, *doc_id) {
            Ok(Some(v)) => Some(v),
            Ok(None) => None,
            Err(e) => {
                eprintln!("{}: read error: {e}", rel_path);
                None
            }
        })
        .collect();
    verify_results.sort_unstable_by_key(|v| v.doc_id);
    verify_results
}
