//! Shared index data structures. Build logic lives in [`super::builder`];
//! lookup and search in [`super::reader`].

// ── Document identity & storage ───────────────────────────────────────────────

/// Identifies a document in the corpus; indexes into [`DocStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocId(pub u32);

/// Holds document paths for indexed documents.
/// `DocId(n)` is the index into the inner vec.
pub struct DocStore {
    pub(crate) docs: Vec<String>,
}

impl DocStore {
    pub fn new() -> Self {
        Self { docs: Vec::new() }
    }

    /// Add a path and return its assigned `DocId`.
    pub fn add_path(&mut self, path: &str) -> DocId {
        let id = DocId(self.docs.len() as u32);
        self.docs.push(path.to_owned());
        id
    }

    #[allow(dead_code)] // retained for API completeness
    pub fn path(&self, id: DocId) -> &str {
        &self.docs[id.0 as usize]
    }

    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn iter_paths(&self) -> impl Iterator<Item = (DocId, &str)> {
        self.docs
            .iter()
            .enumerate()
            .map(|(i, path)| (DocId(i as u32), path.as_str()))
    }
}

// ── Lookup table ──────────────────────────────────────────────────────────────

/// One row: maps an n-gram hash to a byte offset in [`PostingsBlob`].
#[derive(Debug, Clone, Copy)]
pub struct LookupEntry {
    pub hash:   u32,
    pub offset: u64,
}

/// Sorted array of [`LookupEntry`]; binary-searched at query time.
pub struct LookupTable {
    pub(crate) entries: Vec<LookupEntry>,
}

// ── Posting ───────────────────────────────────────────────────────────────────

/// One distinct n-gram hash and the documents that contain it, before serialization.
#[derive(Debug)]
pub struct Posting {
    pub hash:    u32,
    pub doc_ids: Vec<DocId>,
}

// ── Postings blob ─────────────────────────────────────────────────────────────

/// Posting lists written back-to-back: `[count: u32 LE][doc_id: u32 LE] × count`.
pub struct PostingsBlob {
    pub(crate) data: Vec<u8>,
}

// ── Index ─────────────────────────────────────────────────────────────────────

/// In-memory inverted index: lookup table + postings blob.
pub struct Index {
    pub lookup:   LookupTable,
    pub postings: PostingsBlob,
}
