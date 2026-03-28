//! Lookup and search over a built [`super::types::Index`].

use super::types::{DocId, Index, LookupEntry, LookupTable, PostingsBlob};

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

    /// Binary-search for `hash`; returns the postings byte offset if found.
    pub fn lookup(&self, hash: u32) -> Option<u64> {
        self.entries
            .binary_search_by_key(&hash, |e| e.hash)
            .ok()
            .map(|idx| self.entries[idx].offset)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// On-disk size: 4 bytes hash + 8 bytes offset per entry.
    pub fn byte_size(&self) -> usize {
        self.entries.len() * 12
    }
}

impl PostingsBlob {
    /// Raw posting lists (length-prefixed doc-id runs), contiguous.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Decode the posting list at `offset` into sorted document ids.
    pub fn read(&self, offset: u64) -> Vec<DocId> {
        let o = offset as usize;
        let count = u32::from_le_bytes(self.data[o..o + 4].try_into().unwrap()) as usize;
        (0..count)
            .map(|k| {
                let pos = o + 4 + k * 4;
                DocId(u32::from_le_bytes(self.data[pos..pos + 4].try_into().unwrap()))
            })
            .collect()
    }

    pub fn byte_size(&self) -> usize {
        self.data.len()
    }
}

impl Index {
    /// `(byte_offset, posting_list)` for `hash`, or `None` if absent.
    pub fn posting_list(&self, hash: u32) -> Option<(u64, Vec<DocId>)> {
        self.lookup
            .lookup(hash)
            .map(|offset| (offset, self.postings.read(offset)))
    }

    /// Intersect posting lists for all `hashes` (AND over n-grams).
    pub fn candidates(&self, hashes: &[u32]) -> Vec<DocId> {
        let mut result: Option<Vec<DocId>> = None;
        for &hash in hashes {
            match self.lookup.lookup(hash) {
                None => return Vec::new(),
                Some(offset) => {
                    let docs = self.postings.read(offset);
                    result = Some(match result {
                        None    => docs,
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
    use super::{DocId, intersect_sorted};

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
