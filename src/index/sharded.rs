use std::fs;
use std::io;
use std::path::Path;

use rayon::prelude::*;

use super::format::{
    read_paths_lines, read_sharded_manifest, validate_sharded_manifest, MANIFEST_FILENAME,
    META_FILENAME, PATHS_FILENAME,
};
use super::mmap::MmapBundle;
use super::reader::union_sorted;
use super::{DocId, PostingsReadTimings};

pub struct ShardedBundle {
    shards: Vec<ShardReader>,
}

struct ShardReader {
    doc_base: u32,
    _doc_count: u32,
    bundle: MmapBundle,
}

impl ShardedBundle {
    pub fn open(bundle_dir: &Path) -> io::Result<(Self, Vec<String>)> {
        let manifest_path = bundle_dir.join(MANIFEST_FILENAME);
        let manifest = read_sharded_manifest(&manifest_path)?;
        validate_sharded_manifest(&manifest)?;

        let paths = read_paths_lines(&bundle_dir.join(PATHS_FILENAME))?;
        if paths.len() != manifest.doc_count as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "paths.txt length does not match manifest doc_count",
            ));
        }
        if !bundle_dir.join(META_FILENAME).is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "missing meta.txt in bundle",
            ));
        }

        let mut shards = Vec::with_capacity(manifest.shards.len());
        for s in manifest.shards {
            let lookup_path = bundle_dir.join(&s.lookup_relpath);
            let postings_path = bundle_dir.join(&s.postings_relpath);
            if !lookup_path.is_file() || !postings_path.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "missing shard files: {} or {}",
                        lookup_path.display(),
                        postings_path.display()
                    ),
                ));
            }
            let bundle = MmapBundle::open_lookup_postings(&lookup_path, &postings_path)?;
            shards.push(ShardReader {
                doc_base: s.doc_base,
                _doc_count: s.doc_count,
                bundle,
            });
        }

        Ok((Self { shards }, paths))
    }

    pub fn has_valid_layout(bundle_dir: &Path) -> bool {
        let manifest_path = bundle_dir.join(MANIFEST_FILENAME);
        let Ok(m) = read_sharded_manifest(&manifest_path) else {
            return false;
        };
        if validate_sharded_manifest(&m).is_err() {
            return false;
        }
        if !bundle_dir.join(PATHS_FILENAME).is_file() || !bundle_dir.join(META_FILENAME).is_file() {
            return false;
        }
        m.shards.iter().all(|s| {
            let lookup = bundle_dir.join(&s.lookup_relpath);
            let postings = bundle_dir.join(&s.postings_relpath);
            lookup.is_file() && postings.is_file()
        })
    }

    pub fn candidates_union(
        &self,
        alternatives: &[Vec<u32>],
    ) -> io::Result<(Vec<DocId>, PostingsReadTimings)> {
        let shard_results: Vec<io::Result<(Vec<DocId>, PostingsReadTimings)>> = self
            .shards
            .par_iter()
            .map(|shard| {
                let (local, timings) = shard.bundle.candidates_union(alternatives)?;
                let mut global = Vec::with_capacity(local.len());
                for DocId(local_id) in local {
                    let doc = shard.doc_base.checked_add(local_id).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "global doc id overflow")
                    })?;
                    global.push(DocId(doc));
                }
                Ok((global, timings))
            })
            .collect();

        let mut out = Vec::<DocId>::new();
        let mut total_timings = PostingsReadTimings::default();
        for sr in shard_results {
            let (docs, t) = sr?;
            total_timings.ms += t.ms;
            total_timings.postings_lists_read = total_timings
                .postings_lists_read
                .saturating_add(t.postings_lists_read);
            out = union_sorted(&out, &docs);
        }
        Ok((out, total_timings))
    }
}

pub fn replace_dir_atomically(dst: &Path, staging: &Path) -> io::Result<()> {
    if dst.exists() {
        fs::remove_dir_all(dst)?;
    }
    fs::rename(staging, dst)
}
