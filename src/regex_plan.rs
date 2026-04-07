//! Regex compilation and HIR-based literal prefilter for sparse n-gram candidate selection.

use std::io;

use regex::Regex;
use regex_syntax::hir::literal::Extractor;
use regex_syntax::parse;

use crate::index::{DocId, MmapBundle, PostingsReadTimings, ShardedBundle};
use crate::ngram;

/// How to narrow candidate documents before regex verification.
#[derive(Debug, Clone)]
pub enum PrefilterPlan {
    /// Literal extraction found no finite prefix set (e.g. unbounded class); scan all docs.
    AllDocs,
    /// Extractor returned an empty literal sequence (e.g. unsatisfiable pattern).
    NeverMatches,
    /// OR of AND-hash groups: each inner vec is `covering_ngrams(literal)` → hash AND; union across literals.
    Union(Vec<Vec<u32>>),
}

/// Compiled regex plus prefilter derived from HIR prefix literals (`regex_syntax::hir::literal`).
#[derive(Debug)]
pub struct RegexPlan {
    pub regex: Regex,
    pub prefilter: PrefilterPlan,
}

/// Build a [`RegexPlan`]: compile with the `regex` crate and extract prefix literals for n-gram prefiltering.
pub fn build_regex_plan(pattern: &str) -> Result<RegexPlan, String> {
    let regex = Regex::new(pattern).map_err(|e| e.to_string())?;
    let hir = parse(pattern).map_err(|e| e.to_string())?;
    let seq = Extractor::new().extract(&hir);
    let prefilter = seq_to_prefilter(&seq);
    Ok(RegexPlan { regex, prefilter })
}

fn seq_to_prefilter(seq: &regex_syntax::hir::literal::Seq) -> PrefilterPlan {
    let Some(literals) = seq.literals() else {
        return PrefilterPlan::AllDocs;
    };
    if literals.is_empty() {
        return PrefilterPlan::NeverMatches;
    }

    let mut union_groups: Vec<Vec<u32>> = Vec::new();
    for lit in literals {
        let b = lit.as_bytes();
        if b.is_empty() {
            return PrefilterPlan::AllDocs;
        }
        if b.len() < 2 {
            return PrefilterPlan::AllDocs;
        }
        let covering = ngram::covering_ngrams(b);
        // Index/postings only contain n-grams of length >= 2; single-byte
        // fragments in query covering must not participate in AND prefiltering.
        let hashes: Vec<u32> = covering
            .iter()
            .filter(|ng| ng.len() >= 2)
            .map(|ng| ngram::hash_ngram(ng))
            .collect();
        if hashes.is_empty() {
            return PrefilterPlan::AllDocs;
        }
        union_groups.push(hashes);
    }
    PrefilterPlan::Union(union_groups)
}

/// Whether a watch-side doc hash set satisfies the prefilter (OR of AND hash groups).
pub fn doc_matches_prefilter(doc_hashes: &[u32], pref: &PrefilterPlan) -> bool {
    match pref {
        PrefilterPlan::NeverMatches => false,
        PrefilterPlan::AllDocs => true,
        PrefilterPlan::Union(groups) => groups
            .iter()
            .any(|g| g.iter().all(|h| doc_hashes.binary_search(h).is_ok())),
    }
}

/// Filter watch `docs` to doc ids matching the prefilter.
pub fn filter_watch_docs_by_prefilter(
    docs: &[(u32, String, Vec<u32>)],
    pref: &PrefilterPlan,
) -> Vec<DocId> {
    docs.iter()
        .filter(|(_, _, dh)| doc_matches_prefilter(dh, pref))
        .map(|(id, _, _)| DocId(*id))
        .collect()
}

/// Resolve mmap posting candidates from a prefilter plan.
pub fn mmap_candidates(
    bundle: &MmapBundle,
    paths_len: usize,
    pref: &PrefilterPlan,
) -> io::Result<(Vec<DocId>, PostingsReadTimings)> {
    match pref {
        PrefilterPlan::NeverMatches => Ok((Vec::new(), PostingsReadTimings::default())),
        PrefilterPlan::AllDocs => Ok((
            (0..paths_len).map(|i| DocId(i as u32)).collect(),
            PostingsReadTimings::default(),
        )),
        PrefilterPlan::Union(groups) => bundle.candidates_union(groups),
    }
}

pub fn sharded_candidates(
    bundle: &ShardedBundle,
    paths_len: usize,
    pref: &PrefilterPlan,
) -> io::Result<(Vec<DocId>, PostingsReadTimings)> {
    match pref {
        PrefilterPlan::NeverMatches => Ok((Vec::new(), PostingsReadTimings::default())),
        PrefilterPlan::AllDocs => Ok((
            (0..paths_len).map(|i| DocId(i as u32)).collect(),
            PostingsReadTimings::default(),
        )),
        PrefilterPlan::Union(groups) => bundle.candidates_union(groups),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_pattern_yields_union_of_one_group() {
        let p = build_regex_plan("fn main").expect("plan");
        match p.prefilter {
            PrefilterPlan::Union(v) => assert_eq!(v.len(), 1),
            other => panic!("expected Union, got {other:?}"),
        }
    }

    #[test]
    fn alternation_yields_multiple_groups() {
        let p = build_regex_plan("foo|bar").expect("plan");
        match p.prefilter {
            PrefilterPlan::Union(v) => assert_eq!(v.len(), 2),
            other => panic!("expected Union, got {other:?}"),
        }
    }

    #[test]
    fn large_char_class_falls_back_to_all_docs() {
        let p = build_regex_plan("[a-z]+").expect("plan");
        assert!(
            matches!(p.prefilter, PrefilterPlan::AllDocs),
            "expected AllDocs for unbounded class, got {:?}",
            p.prefilter
        );
    }

    #[test]
    fn prefilter_drops_single_byte_covering_fragments() {
        let p = build_regex_plan("(?m)^func\\s+New[A-Za-z0-9_]*").expect("plan");
        match p.prefilter {
            PrefilterPlan::Union(groups) => {
                assert!(!groups.is_empty(), "expected at least one OR branch");
                assert!(
                    groups.iter().all(|g| !g.is_empty()),
                    "expected each branch to keep at least one >=2-byte n-gram hash"
                );
            }
            other => panic!("expected Union, got {other:?}"),
        }
    }

    #[test]
    fn doc_matches_union_prefilter() {
        let pref = PrefilterPlan::Union(vec![vec![11, 22], vec![33]]);
        assert!(doc_matches_prefilter(&[11, 22, 44], &pref));
        assert!(doc_matches_prefilter(&[10, 33, 99], &pref));
        assert!(!doc_matches_prefilter(&[11, 44], &pref));
    }
}
