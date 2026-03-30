//! On-disk layouts for the isearch inverted index.
//!
//! # Design (summary)
//!
//! These types describe **logical** and **serialized** forms. Conventions follow
//! common practice for binary formats ([magic][magic], [versioning][ver], fixed
//! headers, explicit endianness):
//!
//! - **Magic (8 bytes)** — Identifies the file kind and rejects mistaken text
//!   tools. Distinct values for the lookup table vs postings blob.
//! - **Format version (`u32`)** — Readers reject or migrate unknown versions.
//! - **Flags (`u32`)** — Reserved for optional features (compression, sorted
//!   doc-id lists, etc.) without changing the base layout.
//! - **Little-endian** — All multi-byte integers use LE (`to_le_bytes` /
//!   `from_le_bytes`), matching typical desktop/server CPUs.
//! - **Fixed header size** — Enables `mmap` and bounds checks without scanning
//!   the payload.
//!
//! [magic]: https://fadden.com/tech/file-formats.html
//! [ver]: https://stackoverflow.com/questions/323604/what-are-important-points-when-designing-a-binary-file-format
//!
//! # File split
//!
//! The index is stored as **two** files (same idea as the Cursor blog: lookup
//! table + postings region):
//!
//! 1. **Lookup** — Sorted `(ngram_hash → byte_offset)` rows; binary search.
//! 2. **Postings** — Concatenated posting lists referenced by those offsets.
//!
//! A future **manifest** (paths, corpus root, build time) can be a third file;
//! it is not part of the on-disk layout yet.

use std::fs;
use std::io::{self, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::types::{DocStore, Index, LookupEntry};

// ── Version & magic ───────────────────────────────────────────────────────────

/// Current format version number in file headers.
pub const FORMAT_VERSION: u32 = 1;

/// Lookup-table file: `ISEARCH` + `L` + padding to 8 bytes.
pub const LOOKUP_MAGIC: [u8; 8] = *b"ISEARCHL";

/// Postings blob file: `ISEARCH` + `P` + padding to 8 bytes.
pub const POSTINGS_MAGIC: [u8; 8] = *b"ISEARCHP";

/// Lookup table file name inside an index bundle directory.
pub const LOOKUP_FILENAME: &str = "lookup.isearch";

/// Postings blob file name inside an index bundle directory.
pub const POSTINGS_FILENAME: &str = "postings.isearch";

/// One path per line (UTF-8), `DocId` order — line *i* is document *i*.
pub const PATHS_FILENAME: &str = "paths.txt";

/// Key/value metadata (`root=`, `format_version=`, `doc_count=`).
pub const META_FILENAME: &str = "meta.txt";

/// Bit flags in [`IsearchIndexFileHeader::flags`]. None defined yet.
pub mod flags {
    pub type Flags = u32;
    pub const NONE: Flags = 0;
    // Example future flags:
    // pub const POSTING_LISTS_SORTED: Flags = 1 << 0;
    // pub const ZSTD_POSTINGS: Flags = 1 << 1;
}

// ── Shared file header ────────────────────────────────────────────────────────

/// 32-byte header at offset 0 of both lookup and postings files.
///
/// Layout (all **little-endian**):
///
/// | Offset | Size | Field            |
/// |--------|------|------------------|
/// | 0      | 8    | `magic`          |
/// | 8      | 4    | `format_version` |
/// | 12     | 4    | `flags`          |
/// | 16     | 8    | `payload_size`   |
/// | 24     | 8    | `entry_count`    |
///
/// **Lookup file:** `payload_size` must equal `entry_count * 12` (see
/// [`LookupEntryRecord`]). `entry_count` is the number of hash→offset rows.
///
/// **Postings file:** `payload_size` is the byte length of the raw postings
/// region. `entry_count` is **unused** (set to `0`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct IsearchIndexFileHeader {
    pub magic:          [u8; 8],
    pub format_version: u32,
    pub flags:          u32,
    /// Bytes of payload following this header (the body).
    pub payload_size:   u64,
    /// Lookup: number of [`LookupEntryRecord`] rows. Postings: `0` (unused).
    pub entry_count:    u64,
}

impl IsearchIndexFileHeader {
    #[inline]
    pub fn lookup_new(entry_count: u64, flags: u32) -> Self {
        let payload_size = entry_count
            .checked_mul(LookupEntryRecord::SIZE as u64)
            .expect("lookup payload_size overflow");
        Self {
            magic: LOOKUP_MAGIC,
            format_version: FORMAT_VERSION,
            flags,
            payload_size,
            entry_count,
        }
    }

    #[inline]
    pub fn postings_new(payload_size: u64, flags: u32) -> Self {
        Self {
            magic: POSTINGS_MAGIC,
            format_version: FORMAT_VERSION,
            flags,
            payload_size,
            entry_count: 0,
        }
    }

    /// Append 32-byte LE header to a buffer (batched disk writes).
    pub fn extend_le_to(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.magic);
        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.extend_from_slice(&self.flags.to_le_bytes());
        buf.extend_from_slice(&self.payload_size.to_le_bytes());
        buf.extend_from_slice(&self.entry_count.to_le_bytes());
    }
}

// ── Lookup table body ─────────────────────────────────────────────────────────

/// One row in the lookup file body: maps an n-gram hash to a byte offset in
/// the postings file.
///
/// | Offset | Size | Field    |
/// |--------|------|----------|
/// | 0      | 4    | `hash`   |
/// | 4      | 8    | `offset` |
///
/// Rows are stored **sorted by `hash`** ascending for binary search.
/// Packed so the on-disk row is 12 bytes (no padding between `u32` and `u64`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct LookupEntryRecord {
    pub hash:   u32,
    pub offset: u64,
}

impl LookupEntryRecord {
    pub const SIZE: usize = size_of::<Self>();
}

impl From<LookupEntry> for LookupEntryRecord {
    fn from(e: LookupEntry) -> Self {
        Self {
            hash:   e.hash,
            offset: e.offset,
        }
    }
}

/// Logical view of a complete lookup-table file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupTableFile {
    pub header: IsearchIndexFileHeader,
    pub entries: Vec<LookupEntryRecord>,
}

// ── Postings blob body ────────────────────────────────────────────────────────

/// One posting list: a length-prefixed run of document ids.
///
/// | Part   | Size     | Field        |
/// |--------|----------|--------------|
/// | header | 4        | `doc_count`  |
/// | body   | 4×count  | `doc_id` each|
///
/// Lists are concatenated back-to-back with no padding between them.
/// Offsets in [`LookupEntryRecord`] point to the `doc_count` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingListRecord {
    pub doc_ids: Vec<u32>,
}

impl PostingListRecord {
    /// Serialized size in bytes: `4 + 4 * doc_ids.len()`.
    #[allow(dead_code)]
    #[inline]
    pub fn serialized_size(&self) -> usize {
        4usize.saturating_add(self.doc_ids.len().saturating_mul(4))
    }
}

/// Logical view of the postings file: header + opaque blob matching today's
/// in-memory [`crate::types::PostingsBlob`](super::types::PostingsBlob).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingsBlobFile {
    pub header: IsearchIndexFileHeader,
    /// Raw bytes: concatenated [`PostingListRecord`] encodings.
    pub payload: Vec<u8>,
}

// ── Storage layout under ~/.isearch/indexes ───────────────────────────────────

/// Directory name for the index bundle under `~/.isearch/indexes/<pwd_hash>/`.
pub const INDEX_BUNDLE_DIR: &str = "index";

/// Stable hex id for a canonical project root (FNV-1a 64 → 16 hex chars).
pub fn pwd_hash(canonical_root: &Path) -> String {
    let s = canonical_root.to_string_lossy();
    fnv1a64_hex(s.as_bytes())
}

fn fnv1a64_hex(bytes: &[u8]) -> String {
    let mut h = 0xcbf29ce484222325u64;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", h)
}

/// `~/.isearch/indexes/<pwd_hash>/index/`
pub fn index_bundle_path(home: &Path, pwd_hash: &str) -> PathBuf {
    home.join(".isearch")
        .join("indexes")
        .join(pwd_hash)
        .join(INDEX_BUNDLE_DIR)
}

/// Write lookup + postings + `paths.txt` + `meta.txt` into `out_dir` (created if missing).
///
/// Prints **wall-clock time for actual file I/O** (and per-file breakdown) to stderr.
pub fn write_bundle(out_dir: &Path, index: &Index, store: &DocStore, root: &Path) -> io::Result<()> {
    let t_wall = Instant::now();
    fs::create_dir_all(out_dir)?;

    let t = Instant::now();
    write_lookup_file(&out_dir.join(LOOKUP_FILENAME), index)?;
    let ms_lookup = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    write_postings_file(&out_dir.join(POSTINGS_FILENAME), index)?;
    let ms_postings = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    write_paths_file(&out_dir.join(PATHS_FILENAME), store)?;
    let ms_paths = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    write_meta_file(&out_dir.join(META_FILENAME), root, store.len())?;
    let ms_meta = t.elapsed().as_secs_f64() * 1000.0;

    let ms_total = t_wall.elapsed().as_secs_f64() * 1000.0;
    eprintln!(
        "  write to disk: {:.3}ms total  (lookup {:.3}ms, postings {:.3}ms, paths {:.3}ms, meta {:.3}ms)",
        ms_total, ms_lookup, ms_postings, ms_paths, ms_meta
    );

    Ok(())
}

/// Write `paths.txt` + `meta.txt` into `out_dir` (created if missing).
pub fn write_paths_and_meta(out_dir: &Path, store: &DocStore, root: &Path) -> io::Result<()> {
    fs::create_dir_all(out_dir)?;
    write_paths_file(&out_dir.join(PATHS_FILENAME), store)?;
    write_meta_file(&out_dir.join(META_FILENAME), root, store.len())
}

/// **Pt 7:** One contiguous buffer + [`fs::write`] — avoids per-row syscalls.
fn write_lookup_file(path: &Path, index: &Index) -> io::Result<()> {
    let entries = index.lookup.entries();
    let n = entries.len() as u64;
    let header = IsearchIndexFileHeader::lookup_new(n, flags::NONE);
    let row_bytes = entries
        .len()
        .checked_mul(LookupEntryRecord::SIZE)
        .expect("lookup row bytes overflow");
    let mut buf = Vec::with_capacity(size_of::<IsearchIndexFileHeader>() + row_bytes);
    header.extend_le_to(&mut buf);
    for e in entries {
        buf.extend_from_slice(&e.hash.to_le_bytes());
        buf.extend_from_slice(&e.offset.to_le_bytes());
    }
    fs::write(path, buf)
}

/// **Pt 8:** Header + postings payload in one buffer, single [`fs::write`].
fn write_postings_file(path: &Path, index: &Index) -> io::Result<()> {
    let payload = index.postings.as_bytes();
    let header =
        IsearchIndexFileHeader::postings_new(payload.len() as u64, flags::NONE);
    let mut buf = Vec::with_capacity(size_of::<IsearchIndexFileHeader>() + payload.len());
    header.extend_le_to(&mut buf);
    buf.extend_from_slice(payload);
    fs::write(path, buf)
}

/// **Pt 9:** Pre-sized buffer, one [`fs::write`] (no per-line syscalls).
pub(crate) fn write_paths_file(path: &Path, store: &DocStore) -> io::Result<()> {
    let mut nbytes = 0usize;
    for (_, p) in store.iter_paths() {
        nbytes = nbytes.saturating_add(p.len()).saturating_add(1);
    }
    let mut buf = Vec::with_capacity(nbytes);
    for (_, p) in store.iter_paths() {
        buf.extend_from_slice(p.as_bytes());
        buf.push(b'\n');
    }
    fs::write(path, buf)
}

pub(crate) fn write_meta_file(path: &Path, root: &Path, doc_count: usize) -> io::Result<()> {
    let mut f = fs::File::create(path)?;
    writeln!(f, "root={}", root.display())?;
    writeln!(f, "format_version={}", FORMAT_VERSION)?;
    writeln!(f, "doc_count={doc_count}")?;
    Ok(())
}

// ── Read bundle (mmap-friendly layout: header + body) ─────────────────────────

/// Decode the 32-byte file header at the start of `lookup.isearch` / `postings.isearch`.
pub(crate) fn decode_file_header(
    file_bytes: &[u8],
    expected_magic: [u8; 8],
) -> io::Result<IsearchIndexFileHeader> {
    if file_bytes.len() < 32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file too short for 32-byte header",
        ));
    }
    let magic: [u8; 8] = file_bytes[0..8].try_into().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "invalid magic length")
    })?;
    if magic != expected_magic {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected file magic (not an isearch lookup/postings file?)",
        ));
    }
    let format_version = u32::from_le_bytes(file_bytes[8..12].try_into().unwrap());
    if format_version != FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported format version {format_version} (expected {FORMAT_VERSION})"),
        ));
    }
    let flags = u32::from_le_bytes(file_bytes[12..16].try_into().unwrap());
    let payload_size = u64::from_le_bytes(file_bytes[16..24].try_into().unwrap());
    let entry_count = u64::from_le_bytes(file_bytes[24..32].try_into().unwrap());
    Ok(IsearchIndexFileHeader {
        magic,
        format_version,
        flags,
        payload_size,
        entry_count,
    })
}

pub(crate) fn read_paths_lines(path: &Path) -> io::Result<Vec<String>> {
    let text = fs::read_to_string(path)?;
    Ok(text.lines().map(String::from).collect())
}

// ── Tests: header size and magic uniqueness ───────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_is_32_bytes() {
        assert_eq!(size_of::<IsearchIndexFileHeader>(), 32);
    }

    #[test]
    fn lookup_magic_distinct_from_postings() {
        assert_ne!(LOOKUP_MAGIC, POSTINGS_MAGIC);
    }

    #[test]
    fn lookup_header_payload_matches_entries() {
        let h = IsearchIndexFileHeader::lookup_new(10, 0);
        assert_eq!(h.entry_count, 10);
        assert_eq!(h.payload_size, 10 * LookupEntryRecord::SIZE as u64);
    }

    #[test]
    fn lookup_entry_record_is_packed_12_bytes() {
        assert_eq!(LookupEntryRecord::SIZE, 12);
    }
}
