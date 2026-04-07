use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use super::builder::{BuildOutput, SpillOptions};
use super::format::{
    decode_file_header, write_meta_file, write_paths_lines, write_sharded_manifest,
    IsearchIndexFileHeader, ShardManifestEntry, ShardedManifest, FORMAT_VERSION, LOOKUP_FILENAME,
    MANIFEST_FILENAME, POSTINGS_FILENAME, POSTINGS_MAGIC,
};
use super::sharded::replace_dir_atomically;
use super::Index;

pub const DEFAULT_TARGET_POSTINGS_BYTES: u64 = 1_073_741_824;
const PRE_SPLIT_MAX_PATHS: usize = 200_000;

struct AcceptedShard {
    temp_dir: PathBuf,
    paths: Vec<String>,
}

pub fn build_sharded_bundle(
    root: &Path,
    paths: &[String],
    options: &SpillOptions,
    out_dir: &Path,
    target_postings_bytes: u64,
) -> io::Result<()> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let parent = out_dir
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid output dir"))?;
    let staging = parent.join(format!(".index-staging-{}-{ts}", process::id()));
    let attempts_dir = staging.join(".tmp-shards");
    let shards_root = staging.join("shards");
    fs::create_dir_all(&attempts_dir)?;
    fs::create_dir_all(&shards_root)?;

    let mut accepted = Vec::<AcceptedShard>::new();
    let mut next_attempt_id = 0usize;
    split_build(
        root,
        paths,
        options,
        &attempts_dir,
        target_postings_bytes,
        &mut next_attempt_id,
        &mut accepted,
    )?;

    if accepted.is_empty() {
        let empty_tmp = attempts_dir.join("empty-000000");
        fs::create_dir_all(&empty_tmp)?;
        let (store, index) = Index::build_from_doc_hashes(&[])?;
        super::format::write_bundle(&empty_tmp, &index, &store, root)?;
        accepted.push(AcceptedShard {
            temp_dir: empty_tmp,
            paths: Vec::new(),
        });
    }

    let mut global_paths = Vec::<String>::new();
    let mut manifest_shards = Vec::<ShardManifestEntry>::new();
    let mut doc_base = 0u32;

    for (sid, shard) in accepted.into_iter().enumerate() {
        let shard_dir_rel = format!("shards/{sid:06}");
        let shard_dir = staging.join(&shard_dir_rel);
        fs::rename(&shard.temp_dir, &shard_dir)?;
        let doc_count = u32::try_from(shard.paths.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "too many docs in shard"))?;
        global_paths.extend(shard.paths.iter().cloned());
        manifest_shards.push(ShardManifestEntry {
            shard_id: sid as u32,
            doc_base,
            doc_count,
            lookup_relpath: format!("{shard_dir_rel}/{LOOKUP_FILENAME}"),
            postings_relpath: format!("{shard_dir_rel}/{POSTINGS_FILENAME}"),
        });
        doc_base = doc_base
            .checked_add(doc_count)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "global doc overflow"))?;
    }

    write_paths_lines(&staging.join(super::format::PATHS_FILENAME), &global_paths)?;
    write_meta_file(
        &staging.join(super::format::META_FILENAME),
        root,
        global_paths.len(),
    )?;
    let manifest = ShardedManifest {
        format_version: FORMAT_VERSION,
        root: root.to_string_lossy().into_owned(),
        doc_count: u32::try_from(global_paths.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "too many docs"))?,
        shard_count: u32::try_from(manifest_shards.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "too many shards"))?,
        target_postings_bytes,
        shards: manifest_shards,
    };
    write_sharded_manifest(&staging.join(MANIFEST_FILENAME), &manifest)?;

    let _ = fs::remove_dir_all(&attempts_dir);
    replace_dir_atomically(out_dir, &staging)?;
    Ok(())
}

fn split_build(
    root: &Path,
    paths: &[String],
    options: &SpillOptions,
    attempts_dir: &Path,
    target_postings_bytes: u64,
    next_attempt_id: &mut usize,
    accepted: &mut Vec<AcceptedShard>,
) -> io::Result<()> {
    if paths.is_empty() {
        return Ok(());
    }

    // Keep small/medium repos on the existing fast path. For very large ranges we
    // pre-split by contiguous path order to avoid spending a full failing pass
    // that can overflow the 31-bit postings-offset limit during spill merge.
    if paths.len() > PRE_SPLIT_MAX_PATHS {
        let mid = paths.len() / 2;
        split_build(
            root,
            &paths[..mid],
            options,
            attempts_dir,
            target_postings_bytes,
            next_attempt_id,
            accepted,
        )?;
        split_build(
            root,
            &paths[mid..],
            options,
            attempts_dir,
            target_postings_bytes,
            next_attempt_id,
            accepted,
        )?;
        return Ok(());
    }

    let attempt_dir = attempts_dir.join(format!("attempt-{:06}", *next_attempt_id));
    *next_attempt_id += 1;
    fs::create_dir_all(&attempt_dir)?;

    let (store, build) = match Index::ingest_files_with_spill_options(paths, options, &attempt_dir)
    {
        Ok(v) => v,
        Err(e) if should_split_after_overflow(&e, paths.len()) => {
            let _ = fs::remove_dir_all(&attempt_dir);
            let mid = paths.len() / 2;
            split_build(
                root,
                &paths[..mid],
                options,
                attempts_dir,
                target_postings_bytes,
                next_attempt_id,
                accepted,
            )?;
            split_build(
                root,
                &paths[mid..],
                options,
                attempts_dir,
                target_postings_bytes,
                next_attempt_id,
                accepted,
            )?;
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    match build {
        BuildOutput::InMemory(index) => {
            super::format::write_bundle(&attempt_dir, &index, &store, root)?;
        }
        BuildOutput::SpilledToDisk => {
            super::format::write_paths_and_meta(&attempt_dir, &store, root)?;
        }
    }

    let postings_payload = postings_payload_bytes(&attempt_dir.join(POSTINGS_FILENAME))?;
    let indexed_doc_count = store.len();
    let should_split =
        postings_payload > target_postings_bytes && paths.len() > 1 && indexed_doc_count > 1;
    if should_split {
        fs::remove_dir_all(&attempt_dir)?;
        let mid = paths.len() / 2;
        split_build(
            root,
            &paths[..mid],
            options,
            attempts_dir,
            target_postings_bytes,
            next_attempt_id,
            accepted,
        )?;
        split_build(
            root,
            &paths[mid..],
            options,
            attempts_dir,
            target_postings_bytes,
            next_attempt_id,
            accepted,
        )?;
        return Ok(());
    }

    let mut shard_paths = Vec::with_capacity(indexed_doc_count);
    for (_, p) in store.iter_paths() {
        shard_paths.push(p.to_owned());
    }
    if shard_paths.is_empty() {
        fs::remove_dir_all(&attempt_dir)?;
        return Ok(());
    }
    accepted.push(AcceptedShard {
        temp_dir: attempt_dir,
        paths: shard_paths,
    });
    Ok(())
}

fn postings_payload_bytes(path: &Path) -> io::Result<u64> {
    let bytes = fs::read(path)?;
    let header: IsearchIndexFileHeader = decode_file_header(&bytes, POSTINGS_MAGIC)?;
    Ok(header.payload_size)
}

fn should_split_after_overflow(e: &io::Error, path_count: usize) -> bool {
    path_count > 1
        && e.kind() == io::ErrorKind::InvalidData
        && e.to_string()
            .contains("postings payload exceeds 31-bit offset limit")
}
