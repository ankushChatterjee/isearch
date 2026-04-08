//! Substring / regex verification after n-gram candidate filtering.

use std::fs;
use std::io;
use std::sync::{Arc, Mutex};

use memchr::{memchr, memchr_iter, memmem, memrchr};
use rayon::prelude::*;
use regex::Regex;

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
#[allow(dead_code)] // Kept for literal / `-F`-style search if reintroduced.
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

/// Read one file and confirm regex matches; map each match start to its line text (one line per distinct line start).
pub fn verify_candidate_regex(
    path: &str,
    re: &Regex,
    doc_id: DocId,
) -> io::Result<Option<VerifyFileResult>> {
    let t_read = std::time::Instant::now();
    let bytes = fs::read(path)?;
    let read_ms = t_read.elapsed().as_secs_f64() * 1000.0;

    let s =
        std::str::from_utf8(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let matches: Vec<usize> = re.find_iter(s).map(|m| m.start()).collect();
    if matches.is_empty() {
        return Ok(None);
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
#[allow(dead_code)]
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
#[allow(dead_code)]
pub fn verify_doc_paths_parallel(
    candidates: &[(DocId, String)],
    pattern: &[u8],
) -> Vec<VerifyFileResult> {
    let mut verify_results: Vec<VerifyFileResult> = candidates
        .par_iter()
        .filter_map(
            |(doc_id, rel_path)| match verify_candidate(rel_path, pattern, *doc_id) {
                Ok(Some(v)) => Some(v),
                Ok(None) => None,
                Err(e) => {
                    eprintln!("{}: read error: {e}", rel_path);
                    None
                }
            },
        )
        .collect();
    verify_results.sort_unstable_by_key(|v| v.doc_id);
    verify_results
}

/// Parallel regex verification over candidate doc ids (paths resolved from `paths`).
pub fn verify_candidates_parallel_regex(
    candidates: &[DocId],
    paths: &[String],
    re: &Regex,
) -> Vec<VerifyFileResult> {
    let (verify_results, errors) =
        verify_candidates_parallel_regex_collect_errors(candidates, paths, re);
    for err in errors {
        eprintln!("{err}");
    }
    verify_results
}

/// Parallel regex verification over candidate doc ids (paths resolved from `paths`),
/// collecting read/decode errors for caller-managed logging.
pub fn verify_candidates_parallel_regex_collect_errors(
    candidates: &[DocId],
    paths: &[String],
    re: &Regex,
) -> (Vec<VerifyFileResult>, Vec<String>) {
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut verify_results: Vec<VerifyFileResult> = candidates
        .par_iter()
        .filter_map(|&doc_id| {
            let rel_path = paths.get(doc_id.0 as usize)?;
            match verify_candidate_regex(rel_path, re, doc_id) {
                Ok(Some(v)) => Some(v),
                Ok(None) => None,
                Err(e) => {
                    if let Ok(mut out) = errors.lock() {
                        out.push(format!("{rel_path}: read error: {e}"));
                    }
                    None
                }
            }
        })
        .collect();

    verify_results.sort_unstable_by_key(|v| v.doc_id);
    let collected = errors.lock().map(|v| v.clone()).unwrap_or_default();
    (verify_results, collected)
}

/// Parallel regex verification over explicit `(DocId, path)` pairs.
pub fn verify_doc_paths_parallel_regex(
    candidates: &[(DocId, String)],
    re: &Regex,
) -> Vec<VerifyFileResult> {
    let (verify_results, errors) = verify_doc_paths_parallel_regex_collect_errors(candidates, re);
    for err in errors {
        eprintln!("{err}");
    }
    verify_results
}

/// Parallel regex verification over explicit `(DocId, path)` pairs,
/// collecting read/decode errors for caller-managed logging.
pub fn verify_doc_paths_parallel_regex_collect_errors(
    candidates: &[(DocId, String)],
    re: &Regex,
) -> (Vec<VerifyFileResult>, Vec<String>) {
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut verify_results: Vec<VerifyFileResult> = candidates
        .par_iter()
        .filter_map(
            |(doc_id, rel_path)| match verify_candidate_regex(rel_path, re, *doc_id) {
                Ok(Some(v)) => Some(v),
                Ok(None) => None,
                Err(e) => {
                    if let Ok(mut out) = errors.lock() {
                        out.push(format!("{rel_path}: read error: {e}"));
                    }
                    None
                }
            },
        )
        .collect();
    verify_results.sort_unstable_by_key(|v| v.doc_id);
    let collected = errors.lock().map(|v| v.clone()).unwrap_or_default();
    (verify_results, collected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "isearch-{prefix}-{}-{nanos}.txt",
            std::process::id()
        ))
    }

    #[test]
    fn verify_candidate_regex_reports_one_hit_per_matching_line() {
        let file = temp_path("verify-regex");
        fs::write(&file, "foo foo\r\nbar\nfoo\n").unwrap();
        let re = Regex::new("foo").unwrap();

        let got = verify_candidate_regex(file.to_string_lossy().as_ref(), &re, DocId(7))
            .unwrap()
            .expect("expected regex hit");

        assert_eq!(got.doc_id, DocId(7));
        assert_eq!(got.hits.len(), 2);
        assert_eq!(got.hits[0].line_no, 1);
        assert_eq!(got.hits[0].line, "foo foo");
        assert_eq!(got.hits[1].line_no, 3);
        assert_eq!(got.hits[1].line, "foo");
        let _ = fs::remove_file(file);
    }

    #[test]
    fn verify_candidate_regex_rejects_non_utf8() {
        let file = temp_path("verify-non-utf8");
        fs::write(&file, [0xffu8, 0xfe, 0xfd]).unwrap();
        let re = Regex::new("foo").unwrap();

        let err = verify_candidate_regex(file.to_string_lossy().as_ref(), &re, DocId(1))
            .expect_err("expected UTF-8 decode error");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let _ = fs::remove_file(file);
    }

    #[test]
    fn verify_candidates_parallel_regex_collect_errors_sorts_and_collects() {
        let hit = temp_path("verify-hit");
        let miss = temp_path("verify-miss");
        fs::write(&hit, "alpha\n").unwrap();
        fs::write(&miss, "zzz\n").unwrap();

        let missing = temp_path("verify-missing");
        let paths = vec![
            hit.to_string_lossy().into_owned(),
            missing.to_string_lossy().into_owned(),
            miss.to_string_lossy().into_owned(),
        ];
        let candidates = vec![DocId(2), DocId(0), DocId(1)];
        let re = Regex::new("alpha").unwrap();

        let (results, errors) =
            verify_candidates_parallel_regex_collect_errors(&candidates, &paths, &re);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, DocId(0));
        assert_eq!(results[0].hits[0].line, "alpha");
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("read error"));

        let _ = fs::remove_file(hit);
        let _ = fs::remove_file(miss);
    }
}
