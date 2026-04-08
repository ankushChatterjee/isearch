use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::index::format::{MANIFEST_FILENAME, META_FILENAME, PATHS_FILENAME};
use crate::index::{build_sharded_bundle, SpillOptions, DEFAULT_TARGET_POSTINGS_BYTES};

use super::delta;
use super::state::{now_unix_secs, DocMeta, WatchState};

pub fn compact(
    bundle_dir: &Path,
    root: &Path,
    state_path: &Path,
    delta_path: &Path,
    state: &mut WatchState,
) -> io::Result<()> {
    let mut alive: Vec<DocMeta> = state
        .docs
        .values()
        .filter(|d| !d.tombstone)
        .cloned()
        .collect();
    alive.sort_unstable_by(|a, b| a.path.cmp(&b.path));

    let paths: Vec<String> = alive.iter().map(|d| d.path.clone()).collect();
    let tmp_bundle = bundle_dir.join(format!(
        ".compact-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    fs::create_dir_all(&tmp_bundle)?;
    let spill = SpillOptions::default();
    build_sharded_bundle(
        root,
        &paths,
        &spill,
        &tmp_bundle,
        DEFAULT_TARGET_POSTINGS_BYTES,
    )?;
    for name in [MANIFEST_FILENAME, PATHS_FILENAME, META_FILENAME, "shards"] {
        if bundle_dir.join(name).exists() {
            if bundle_dir.join(name).is_dir() {
                fs::remove_dir_all(bundle_dir.join(name))?;
            } else {
                fs::remove_file(bundle_dir.join(name))?;
            }
        }
        fs::rename(tmp_bundle.join(name), bundle_dir.join(name))?;
    }
    let _ = fs::remove_dir_all(&tmp_bundle);

    let mut new_docs = BTreeMap::new();
    let mut new_path_to_doc = HashMap::new();
    for (i, doc) in alive.into_iter().enumerate() {
        let doc_id = i as u32;
        new_path_to_doc.insert(doc.path.clone(), doc_id);
        new_docs.insert(doc_id, doc);
    }
    state.docs = new_docs;
    state.path_to_doc = new_path_to_doc;
    state.next_doc_id = u32::try_from(state.docs.len()).unwrap_or(u32::MAX);
    state.last_compaction_unix_secs = now_unix_secs();
    delta::reset(delta_path)?;
    state.last_delta_offset = delta::header_len();
    state.persist(state_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watch::delta::{header_len, replay, DeltaOp, DeltaWriter, DELTA_FILENAME};
    use std::path::PathBuf;

    fn temp_dir(prefix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "isearch-watch-compact-{prefix}-{}-{}",
            std::process::id(),
            now_unix_secs()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn compact_rebuilds_alive_docs_and_resets_delta() {
        let root = temp_dir("root");
        let bundle_dir = temp_dir("bundle");
        fs::write(root.join("a.txt"), "alpha\n").unwrap();
        fs::write(root.join("b.txt"), "beta\n").unwrap();

        let mut state = WatchState::new(&root);
        let a = state.ensure_doc_for_path(root.join("a.txt").to_string_lossy().as_ref());
        let b = state.ensure_doc_for_path(root.join("b.txt").to_string_lossy().as_ref());
        state.docs.get_mut(&a).unwrap().hashes = vec![1, 2];
        state.docs.get_mut(&b).unwrap().hashes = vec![3, 4];
        state.docs.get_mut(&b).unwrap().tombstone = true;

        let state_path = bundle_dir.join("watch_state.bin");
        let delta_path = bundle_dir.join(DELTA_FILENAME);
        state.last_delta_offset = header_len();
        state.persist(&state_path).unwrap();

        let mut writer = DeltaWriter::open(&delta_path).unwrap();
        writer
            .append_batch(&[DeltaOp::AddHash {
                doc_id: a,
                hash: 999,
            }])
            .unwrap();

        compact(&bundle_dir, &root, &state_path, &delta_path, &mut state).unwrap();

        assert_eq!(state.docs.len(), 1, "tombstoned doc should be dropped");
        assert_eq!(state.next_doc_id, 1);
        assert_eq!(state.last_delta_offset, header_len());
        assert!(bundle_dir.join(MANIFEST_FILENAME).is_file());
        assert!(bundle_dir.join(PATHS_FILENAME).is_file());
        assert!(bundle_dir.join(META_FILENAME).is_file());
        let (ops, off) = replay(&delta_path, header_len()).unwrap();
        assert!(ops.is_empty(), "delta should be reset after compaction");
        assert_eq!(off, header_len());

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(bundle_dir);
    }
}
