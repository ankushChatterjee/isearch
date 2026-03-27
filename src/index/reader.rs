//! Lookup and search over a built [`super::types::Index`].

use std::collections::HashSet;

use super::types::{DocId, Index, LookupEntry, LookupTable, PostingsBlob};

impl LookupTable {
    /// Sorted rows for serialization or inspection.
    pub fn entries(&self) -> &[LookupEntry] {
        &self.entries
    }

    /// Binary-search for `hash`; returns the postings byte offset if found.
    pub fn lookup(&self, hash: u64) -> Option<u64> {
        self.entries
            .binary_search_by_key(&hash, |e| e.hash)
            .ok()
            .map(|idx| self.entries[idx].offset)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// On-disk size: 8 bytes hash + 8 bytes offset per entry.
    pub fn byte_size(&self) -> usize {
        self.entries.len() * 16
    }
}

impl PostingsBlob {
    /// Raw posting lists (length-prefixed doc-id runs), contiguous.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Decode the posting list at `offset` into a set of document ids.
    pub fn read(&self, offset: u64) -> HashSet<DocId> {
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
    pub fn posting_list(&self, hash: u64) -> Option<(u64, HashSet<DocId>)> {
        self.lookup
            .lookup(hash)
            .map(|offset| (offset, self.postings.read(offset)))
    }

    /// Intersect posting lists for all `hashes` (AND over n-grams).
    pub fn candidates(&self, hashes: &[u64]) -> HashSet<DocId> {
        let mut result: Option<HashSet<DocId>> = None;
        for &hash in hashes {
            match self.lookup.lookup(hash) {
                None => return HashSet::new(),
                Some(offset) => {
                    let docs = self.postings.read(offset);
                    result = Some(match result {
                        None    => docs,
                        Some(c) => c.intersection(&docs).copied().collect(),
                    });
                }
            }
        }
        result.unwrap_or_default()
    }
}
