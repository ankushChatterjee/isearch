//! [`MmapBundle`]: mmap the lookup table only; read posting lists from the postings file at offsets.
//!
//! Matches the client-side layout in
//! [Fast regex search](https://cursor.com/blog/fast-regex-search):
//! mmap the sorted hash→offset table for binary search; do **not** mmap the full postings blob.

use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::Instant;

use memmap2::MmapOptions;

use super::format::{
    decode_file_header, decode_lookup_value, read_paths_lines, LookupEntryRecord, LookupValue,
    LOOKUP_FILENAME, LOOKUP_MAGIC, PATHS_FILENAME, POSTINGS_FILENAME, POSTINGS_MAGIC,
};
use super::reader::intersect_sorted;
use super::types::DocId;

const HEADER_LEN: usize = 32;

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

/// Wall-clock time for file-backed steps during [`MmapBundle::open`].
#[derive(Debug, Clone, Copy, Default)]
pub struct BundleOpenTimings {
    /// Open lookup file + `mmap` the lookup table.
    pub lookup_open_and_mmap_ms: f64,
    /// Open postings file + read 32-byte header.
    pub postings_open_and_header_ms: f64,
    /// Read `paths.txt` into memory.
    pub paths_file_read_ms: f64,
}

/// Time spent reading posting lists from `postings.isearch` during [`MmapBundle::candidates`].
#[derive(Debug, Clone, Copy, Default)]
pub struct PostingsReadTimings {
    pub ms: f64,
    /// Number of posting lists read (one per n-gram hash after lookup hit).
    pub postings_lists_read: u32,
}

/// Lookup table is mmap’d; postings stay on disk and are read at offsets (see blog).
pub struct MmapBundle {
    /// Kept open so the lookup [`memmap2::Mmap`] stays valid (platform requirement).
    _lookup_file: File,
    lookup: memmap2::Mmap,
    postings: File,
    /// Always `HEADER_LEN`: posting list offsets in the lookup table are relative to the postings **payload**.
    postings_payload_base: u64,
}

impl MmapBundle {
    /// Open bundle: mmap `lookup.isearch`, open `postings.isearch` for reads at offset, load `paths.txt`.
    pub fn open(dir: &Path) -> io::Result<(Self, Vec<String>, BundleOpenTimings)> {
        let mut timings = BundleOpenTimings::default();

        let lookup_path = dir.join(LOOKUP_FILENAME);
        let postings_path = dir.join(POSTINGS_FILENAME);
        let paths_path = dir.join(PATHS_FILENAME);

        let t = Instant::now();
        let lookup_file = OpenOptions::new()
            .read(true)
            .open(&lookup_path)
            .map_err(|e| io::Error::other(format!("open {}: {e}", lookup_path.display())))?;
        // SAFETY: `lookup_file` is read-only and kept open in `_lookup_file` for the lifetime of
        // this struct; we do not mutate the file while the mapping exists.
        let lookup = unsafe {
            MmapOptions::new()
                .map(&lookup_file)
                .map_err(|e| io::Error::other(format!("mmap {}: {e}", lookup_path.display())))?
        };
        timings.lookup_open_and_mmap_ms = ms(t);

        let lookup_hdr = decode_file_header(&lookup, LOOKUP_MAGIC)?;
        let body_len = lookup.len().saturating_sub(HEADER_LEN);
        if body_len != lookup_hdr.payload_size as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "lookup file size does not match header payload_size",
            ));
        }
        let expected_rows = lookup_hdr.entry_count as usize;
        let expected_bytes = expected_rows
            .checked_mul(LookupEntryRecord::SIZE)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "lookup row count overflow")
            })?;
        if body_len != expected_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "lookup payload: expected {} bytes ({} rows), got {}",
                    expected_bytes, expected_rows, body_len
                ),
            ));
        }

        let t = Instant::now();
        let mut postings = OpenOptions::new()
            .read(true)
            .open(&postings_path)
            .map_err(|e| io::Error::other(format!("open {}: {e}", postings_path.display())))?;
        let file_len = postings.metadata()?.len();
        let mut hdr_buf = [0u8; HEADER_LEN];
        if file_len < HEADER_LEN as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "postings file too small",
            ));
        }
        postings.read_exact(&mut hdr_buf)?;
        timings.postings_open_and_header_ms = ms(t);
        let postings_hdr = decode_file_header(&hdr_buf, POSTINGS_MAGIC)?;
        if postings_hdr.entry_count != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "postings file: expected entry_count 0 in header",
            ));
        }
        let expected_len = HEADER_LEN as u64 + postings_hdr.payload_size;
        if file_len != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "postings file size: expected {} bytes, got {}",
                    expected_len, file_len
                ),
            ));
        }

        let t = Instant::now();
        let paths = read_paths_lines(&paths_path)
            .map_err(|e| io::Error::other(format!("read {}: {e}", paths_path.display())))?;
        timings.paths_file_read_ms = ms(t);

        Ok((
            Self {
                _lookup_file: lookup_file,
                lookup,
                postings,
                postings_payload_base: HEADER_LEN as u64,
            },
            paths,
            timings,
        ))
    }

    /// Binary search on the mmap’d lookup body; returns decoded lookup value.
    fn lookup_hash(&self, hash: u32) -> Option<LookupValue> {
        let body = &self.lookup[HEADER_LEN..];
        let n = body.len() / LookupEntryRecord::SIZE;
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let base = mid * LookupEntryRecord::SIZE;
            let row = &body[base..base + LookupEntryRecord::SIZE];
            let h = u32::from_le_bytes(row[0..4].try_into().ok()?);
            match h.cmp(&hash) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => {
                    return Some(decode_lookup_value(u32::from_le_bytes(
                        row[4..8].try_into().ok()?,
                    )));
                }
            }
        }
        None
    }

    /// Read `[count: varint][delta(doc): varint]...` at payload offset `offset`.
    fn read_posting_list(&self, offset: u32) -> io::Result<Vec<DocId>> {
        let abs = self
            .postings_payload_base
            .checked_add(u64::from(offset))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "posting offset overflow"))?;
        let (count_u32, mut cur) = read_u32_varint_at(&self.postings, abs)?;
        let count = usize::try_from(count_u32).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "posting count does not fit usize",
            )
        })?;

        let mut docs = Vec::with_capacity(count);
        let mut prev = 0u32;
        for _ in 0..count {
            let (delta, next) = read_u32_varint_at(&self.postings, cur)?;
            cur = next;
            let doc = prev
                .checked_add(delta)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "doc_id overflow"))?;
            docs.push(DocId(doc));
            prev = doc;
        }
        Ok(docs)
    }

    /// Intersect posting lists for all `hashes` (AND), same semantics as [`super::Index::candidates`].
    pub fn candidates(&self, hashes: &[u32]) -> io::Result<(Vec<DocId>, PostingsReadTimings)> {
        let mut postings_read_ms = 0.0f64;
        let mut postings_lists_read = 0u32;
        let mut result: Option<Vec<DocId>> = None;
        for &hash in hashes {
            let Some(value) = self.lookup_hash(hash) else {
                return Ok((
                    Vec::new(),
                    PostingsReadTimings {
                        ms: postings_read_ms,
                        postings_lists_read,
                    },
                ));
            };
            let t = Instant::now();
            let docs = match value {
                LookupValue::InlineDocId(doc_id) => vec![DocId(doc_id)],
                LookupValue::PostingsOffset(off) => {
                    let docs = self.read_posting_list(off)?;
                    postings_read_ms += ms(t);
                    postings_lists_read += 1;
                    docs
                }
            };
            result = Some(match result {
                None => docs,
                Some(c) => intersect_sorted(&c, &docs),
            });
        }
        Ok((
            result.unwrap_or_default(),
            PostingsReadTimings {
                ms: postings_read_ms,
                postings_lists_read,
            },
        ))
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        let n = file.read_at(buf, offset)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer with pread",
            ));
        }
        offset = offset.saturating_add(n as u64);
        let (_, rest) = buf.split_at_mut(n);
        buf = rest;
    }
    Ok(())
}

#[cfg(unix)]
fn read_u32_varint_at(file: &File, mut offset: u64) -> io::Result<(u32, u64)> {
    let mut shift = 0u32;
    let mut out = 0u32;
    loop {
        let mut b = [0u8; 1];
        read_exact_at(file, &mut b, offset)?;
        offset = offset.saturating_add(1);
        out |= u32::from(b[0] & 0x7f) << shift;
        if b[0] & 0x80 == 0 {
            return Ok((out, offset));
        }
        shift += 7;
        if shift >= 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint overflow",
            ));
        }
    }
}

#[cfg(not(unix))]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};

    let mut f = file.try_clone()?;
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(buf)
}

#[cfg(not(unix))]
fn read_u32_varint_at(file: &File, mut offset: u64) -> io::Result<(u32, u64)> {
    let mut shift = 0u32;
    let mut out = 0u32;
    loop {
        let mut b = [0u8; 1];
        read_exact_at(file, &mut b, offset)?;
        offset = offset.saturating_add(1);
        out |= u32::from(b[0] & 0x7f) << shift;
        if b[0] & 0x80 == 0 {
            return Ok((out, offset));
        }
        shift += 7;
        if shift >= 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint overflow",
            ));
        }
    }
}
