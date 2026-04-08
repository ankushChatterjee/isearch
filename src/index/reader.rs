//! Lookup and search over a built [`super::types::Index`].

use super::types::{DocId, LookupEntry, LookupTable, PostingsBlob};

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

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// On-disk size: 4 bytes hash + 4 bytes value per entry.
    pub fn byte_size(&self) -> usize {
        self.entries.len() * 8
    }
}

impl PostingsBlob {
    /// Raw posting lists (length-prefixed doc-id runs), contiguous.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn byte_size(&self) -> usize {
        self.data.len()
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
