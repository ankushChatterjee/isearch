//! External-memory helpers: spill sorted runs and k-way merge into final index files.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use rayon::prelude::*;

use super::format::{
    encode_inline_doc_id, encode_postings_offset, flags, push_u32_varint, IsearchIndexFileHeader,
    LookupEntryRecord, LOOKUP_FILENAME, LOOKUP_VALUE_MASK, POSTINGS_FILENAME,
};
use super::types::DocId;

#[derive(Debug, Clone, Copy, Default)]
pub struct MergeStats {
    pub run_count: usize,
    pub merged_pairs: u64,
    pub unique_pairs: u64,
    pub lookup_rows: u64,
    pub postings_payload_bytes: u64,
    pub inline_singletons: u64,
}

pub fn flush_run(dir: &Path, run_idx: usize, pairs: &mut Vec<(u32, DocId)>) -> io::Result<PathBuf> {
    pairs.par_sort_unstable_by_key(|&(h, DocId(d))| (h, d));
    pairs.dedup();

    let run_path = dir.join(format!("run-{run_idx:06}.bin"));
    let mut w = BufWriter::new(File::create(&run_path)?);
    for &(hash, DocId(doc_id)) in pairs.iter() {
        w.write_all(&hash.to_le_bytes())?;
        w.write_all(&doc_id.to_le_bytes())?;
    }
    w.flush()?;
    pairs.clear();
    Ok(run_path)
}

pub fn merge_runs_to_index_files(runs: &[PathBuf], out_dir: &Path) -> io::Result<MergeStats> {
    fs::create_dir_all(out_dir)?;
    let lookup_tmp = out_dir.join(format!("{LOOKUP_FILENAME}.tmp"));
    let postings_tmp = out_dir.join(format!("{POSTINGS_FILENAME}.tmp"));

    let mut lookup_w = BufWriter::new(File::create(&lookup_tmp)?);
    let mut postings_w = BufWriter::new(File::create(&postings_tmp)?);
    lookup_w.write_all(&[0u8; 32])?;
    postings_w.write_all(&[0u8; 32])?;

    let mut readers: Vec<RunReader> = runs
        .iter()
        .map(|p| RunReader::open(p))
        .collect::<io::Result<Vec<_>>>()?;
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::new();
    for (idx, reader) in readers.iter_mut().enumerate() {
        if let Some((hash, doc_id)) = reader.next_record()? {
            heap.push(HeapItem {
                hash,
                doc_id,
                run_idx: idx,
            });
        }
    }

    let mut merged_pairs = 0u64;
    let mut unique_pairs = 0u64;
    let mut lookup_rows = 0u64;
    let mut postings_payload_bytes = 0u64;
    let mut inline_singletons = 0u64;
    let mut last_pair: Option<(u32, u32)> = None;

    let mut current_hash: Option<u32> = None;
    let mut current_docs: Vec<u32> = Vec::new();

    while let Some(item) = heap.pop() {
        merged_pairs += 1;
        let pair = (item.hash, item.doc_id);
        if last_pair != Some(pair) {
            unique_pairs += 1;
            last_pair = Some(pair);
            match current_hash {
                None => {
                    current_hash = Some(item.hash);
                    current_docs.push(item.doc_id);
                }
                Some(h) if h == item.hash => current_docs.push(item.doc_id),
                Some(h) => {
                    flush_posting(
                        h,
                        &current_docs,
                        &mut lookup_w,
                        &mut postings_w,
                        &mut postings_payload_bytes,
                        &mut lookup_rows,
                        &mut inline_singletons,
                    )?;
                    current_docs.clear();
                    current_hash = Some(item.hash);
                    current_docs.push(item.doc_id);
                }
            }
        }

        if let Some((hash, doc_id)) = readers[item.run_idx].next_record()? {
            heap.push(HeapItem {
                hash,
                doc_id,
                run_idx: item.run_idx,
            });
        }
    }

    if let Some(h) = current_hash {
        flush_posting(
            h,
            &current_docs,
            &mut lookup_w,
            &mut postings_w,
            &mut postings_payload_bytes,
            &mut lookup_rows,
            &mut inline_singletons,
        )?;
    }

    let _lookup_payload_bytes = lookup_rows
        .checked_mul(LookupEntryRecord::SIZE as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "lookup payload overflow"))?;

    write_header(
        &mut lookup_w,
        IsearchIndexFileHeader::lookup_new(lookup_rows, flags::NONE),
    )?;
    write_header(
        &mut postings_w,
        IsearchIndexFileHeader::postings_new(postings_payload_bytes, flags::NONE),
    )?;

    lookup_w.flush()?;
    postings_w.flush()?;
    drop(lookup_w);
    drop(postings_w);

    fs::rename(&lookup_tmp, out_dir.join(LOOKUP_FILENAME))?;
    fs::rename(&postings_tmp, out_dir.join(POSTINGS_FILENAME))?;

    Ok(MergeStats {
        run_count: runs.len(),
        merged_pairs,
        unique_pairs,
        lookup_rows,
        postings_payload_bytes,
        inline_singletons,
    })
}

fn write_header(w: &mut BufWriter<File>, header: IsearchIndexFileHeader) -> io::Result<()> {
    w.seek(SeekFrom::Start(0))?;
    let mut buf = Vec::with_capacity(32);
    header.extend_le_to(&mut buf);
    w.write_all(&buf)
}

fn flush_posting(
    hash: u32,
    docs: &[u32],
    lookup_w: &mut BufWriter<File>,
    postings_w: &mut BufWriter<File>,
    postings_payload_bytes: &mut u64,
    lookup_rows: &mut u64,
    inline_singletons: &mut u64,
) -> io::Result<()> {
    if docs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty posting list",
        ));
    }

    let value = if docs.len() == 1 {
        let doc = docs[0];
        *inline_singletons += 1;
        encode_inline_doc_id(doc)?
    } else {
        let offset = *postings_payload_bytes;
        if offset > LOOKUP_VALUE_MASK as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "postings payload exceeds 31-bit offset limit",
            ));
        }

        let mut buf = Vec::new();
        let count = u32::try_from(docs.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "posting list too long"))?;
        push_u32_varint(&mut buf, count);
        let mut prev = 0u32;
        for (i, &doc) in docs.iter().enumerate() {
            if doc > LOOKUP_VALUE_MASK {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "doc_id exceeds 31-bit limit",
                ));
            }
            if i > 0 && doc < prev {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "posting doc_ids must be sorted ascending",
                ));
            }
            let delta = doc - prev;
            push_u32_varint(&mut buf, delta);
            prev = doc;
        }
        postings_w.write_all(&buf)?;
        *postings_payload_bytes = postings_payload_bytes
            .checked_add(buf.len() as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "postings payload overflow")
            })?;
        encode_postings_offset(offset as u32)?
    };

    lookup_w.write_all(&hash.to_le_bytes())?;
    lookup_w.write_all(&value.to_le_bytes())?;
    *lookup_rows += 1;
    Ok(())
}

#[derive(Debug)]
struct RunReader {
    reader: BufReader<File>,
}

impl RunReader {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            reader: BufReader::new(File::open(path)?),
        })
    }

    fn next_record(&mut self) -> io::Result<Option<(u32, u32)>> {
        let mut word = [0u8; 4];
        match self.reader.read_exact(&mut word) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let hash = u32::from_le_bytes(word);
        self.reader.read_exact(&mut word)?;
        let doc_id = u32::from_le_bytes(word);
        Ok(Some((hash, doc_id)))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct HeapItem {
    hash: u32,
    doc_id: u32,
    run_idx: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap behavior in BinaryHeap.
        (other.hash, other.doc_id, other.run_idx).cmp(&(self.hash, self.doc_id, self.run_idx))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(prefix: &str) -> PathBuf {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("{prefix}-{t}"));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn run_flush_and_merge_are_sorted_and_unique() {
        let dir = temp_dir("isearch-spill-test");
        let mut p1 = vec![
            (3u32, DocId(9)),
            (1, DocId(2)),
            (1, DocId(2)),
            (1, DocId(3)),
        ];
        let mut p2 = vec![(2u32, DocId(4)), (3, DocId(9)), (3, DocId(10))];

        let r1 = flush_run(&dir, 0, &mut p1).unwrap();
        let r2 = flush_run(&dir, 1, &mut p2).unwrap();
        let out = dir.join("out");
        let stats = merge_runs_to_index_files(&[r1, r2], &out).unwrap();

        assert_eq!(stats.run_count, 2);
        assert_eq!(stats.unique_pairs, 5);
        assert!(out.join(LOOKUP_FILENAME).is_file());
        assert!(out.join(POSTINGS_FILENAME).is_file());

        let _ = fs::remove_dir_all(&dir);
    }
}
