//! Lookup and search over a built [`super::types::Index`].

use super::format::{decode_lookup_value, read_u32_varint_from_slice, LookupValue};
use super::types::{DocId, Index, LookupEntry, LookupTable, PostingsBlob};

/// Sorted union of two sorted doc-id lists (deduplicated).
pub(crate) fn union_sorted(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

pub(crate) fn intersect_sorted(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

impl LookupTable {
    /// Sorted rows for serialization or inspection.
    pub fn entries(&self) -> &[LookupEntry] {
        &self.entries
    }

    /// Binary-search for `hash`; returns packed lookup value if found.
    pub fn lookup(&self, hash: u32) -> Option<LookupValue> {
        self.entries
            .binary_search_by_key(&hash, |e| e.hash)
            .ok()
            .map(|idx| decode_lookup_value(self.entries[idx].value))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// On-disk size: 4 bytes hash + 4 bytes value per entry.
    pub fn byte_size(&self) -> usize {
        self.entries.len() * 8
    }

    pub fn inline_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| matches!(decode_lookup_value(e.value), LookupValue::InlineDocId(_)))
            .count()
    }
}

impl PostingsBlob {
    /// Raw posting lists (length-prefixed doc-id runs), contiguous.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Decode the postings list at payload offset `offset` into sorted doc ids.
    pub fn read(&self, offset: u32) -> Vec<DocId> {
        let o = offset as usize;
        let mut cursor = o;
        let count = read_u32_varint_from_slice(&self.data, &mut cursor)
            .expect("invalid postings varint count") as usize;
        let mut docs = Vec::with_capacity(count);
        let mut prev = 0u32;
        for _ in 0..count {
            let delta = read_u32_varint_from_slice(&self.data, &mut cursor)
                .expect("invalid postings varint delta");
            let doc = prev
                .checked_add(delta)
                .expect("postings doc_id overflow while decoding");
            docs.push(DocId(doc));
            prev = doc;
        }
        docs
    }

    pub fn byte_size(&self) -> usize {
        self.data.len()
    }
}

impl Index {
    /// `(lookup_value, posting_list)` for `hash`, or `None` if absent.
    pub fn posting_list(&self, hash: u32) -> Option<(LookupValue, Vec<DocId>)> {
        self.lookup.lookup(hash).map(|value| {
            let docs = match value {
                LookupValue::InlineDocId(doc_id) => vec![DocId(doc_id)],
                LookupValue::PostingsOffset(offset) => self.postings.read(offset),
            };
            (value, docs)
        })
    }

    /// Intersect posting lists for all `hashes` (AND over n-grams).
    pub fn candidates(&self, hashes: &[u32]) -> Vec<DocId> {
        let mut result: Option<Vec<DocId>> = None;
        for &hash in hashes {
            match self.lookup.lookup(hash) {
                None => return Vec::new(),
                Some(value) => {
                    let docs = match value {
                        LookupValue::InlineDocId(doc_id) => vec![DocId(doc_id)],
                        LookupValue::PostingsOffset(offset) => self.postings.read(offset),
                    };
                    result = Some(match result {
                        None => docs,
                        Some(c) => intersect_sorted(&c, &docs),
                    });
                }
            }
        }
        result.unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::{intersect_sorted, union_sorted, DocId};

    #[test]
    fn union_sorted_merges_sorted_lists() {
        assert_eq!(union_sorted(&[], &[]), Vec::<DocId>::new());
        assert_eq!(
            union_sorted(&[DocId(1), DocId(3)], &[DocId(2), DocId(4)]),
            vec![DocId(1), DocId(2), DocId(3), DocId(4)]
        );
        assert_eq!(
            union_sorted(&[DocId(1), DocId(2)], &[DocId(2), DocId(3)]),
            vec![DocId(1), DocId(2), DocId(3)]
        );
    }

    #[test]
    fn intersect_sorted_handles_common_shapes() {
        assert_eq!(intersect_sorted(&[], &[]), Vec::<DocId>::new());
        assert_eq!(
            intersect_sorted(&[DocId(1), DocId(2)], &[DocId(3), DocId(4)]),
            Vec::<DocId>::new()
        );
        assert_eq!(
            intersect_sorted(&[DocId(1), DocId(2), DocId(3)], &[DocId(2), DocId(3)]),
            vec![DocId(2), DocId(3)]
        );
        assert_eq!(
            intersect_sorted(
                &[DocId(1), DocId(3), DocId(5), DocId(7)],
                &[DocId(1), DocId(2), DocId(5), DocId(8)]
            ),
            vec![DocId(1), DocId(5)]
        );
    }
}
