use std::fs;
use std::io;
use std::path::Path;
use std::time::UNIX_EPOCH;

use crate::ngram;

use super::delta::DeltaOp;
use super::events::FileAction;
use super::state::{Fingerprint, WatchState};

const MAX_FILE_BYTES: usize = 2 * 1024 * 1024 * 1024;

pub fn apply_actions(state: &mut WatchState, actions: &[FileAction]) -> io::Result<Vec<DeltaOp>> {
    let mut ops = Vec::new();
    for action in actions {
        match action {
            FileAction::Upsert(path) => apply_upsert(state, path, &mut ops)?,
            FileAction::Delete(path) => apply_delete(state, path, &mut ops),
        }
    }
    Ok(ops)
}

fn apply_upsert(state: &mut WatchState, path: &Path, ops: &mut Vec<DeltaOp>) -> io::Result<()> {
    if !path.is_file() {
        apply_delete(state, path, ops);
        return Ok(());
    }
    let path_s = path.to_string_lossy().into_owned();
    let preexisting_doc = state.path_to_doc.get(&path_s).copied();
    let new_hashes = read_unique_hashes(path)?;
    let Some(new_hashes) = new_hashes else {
        apply_delete(state, path, ops);
        return Ok(());
    };
    let new_fp = fingerprint(path)?;

    let doc_id = state.ensure_doc_for_path(&path_s);
    if preexisting_doc.is_none() {
        ops.push(DeltaOp::UpsertPath {
            doc_id,
            path: path_s.clone(),
        });
    }
    let path_changed = state
        .docs
        .get(&doc_id)
        .map(|d| d.path != path_s)
        .unwrap_or(false);
    if path_changed {
        state.set_doc_path(doc_id, path_s.clone());
        ops.push(DeltaOp::UpsertPath {
            doc_id,
            path: path_s.clone(),
        });
    }
    let was_tombstone = state
        .docs
        .get(&doc_id)
        .map(|d| d.tombstone)
        .unwrap_or(false);
    if was_tombstone {
        if let Some(doc) = state.docs.get_mut(&doc_id) {
            doc.tombstone = false;
        }
        ops.push(DeltaOp::UpsertPath {
            doc_id,
            path: path_s.clone(),
        });
    }

    let doc = state.docs.get_mut(&doc_id).expect("doc just inserted");
    if doc.fingerprint.mtime_ns == new_fp.mtime_ns && doc.fingerprint.size == new_fp.size {
        return Ok(());
    }

    let (removes, adds) = sorted_diff(&doc.hashes, &new_hashes);
    for h in removes {
        ops.push(DeltaOp::RemoveHash { doc_id, hash: h });
    }
    for h in adds {
        ops.push(DeltaOp::AddHash { doc_id, hash: h });
    }

    doc.hashes = new_hashes;
    doc.fingerprint = new_fp;
    state.dirty = true;
    Ok(())
}

fn apply_delete(state: &mut WatchState, path: &Path, ops: &mut Vec<DeltaOp>) {
    let path_s = path.to_string_lossy().into_owned();
    let Some(&doc_id) = state.path_to_doc.get(&path_s) else {
        return;
    };
    let Some(doc) = state.docs.get_mut(&doc_id) else {
        return;
    };
    if doc.tombstone {
        return;
    }
    for h in &doc.hashes {
        ops.push(DeltaOp::RemoveHash { doc_id, hash: *h });
    }
    ops.push(DeltaOp::TombstoneDoc { doc_id });
    doc.tombstone = true;
    doc.hashes.clear();
    doc.fingerprint = Fingerprint::default();
    state.dirty = true;
}

fn read_unique_hashes(path: &Path) -> io::Result<Option<Vec<u32>>> {
    let bytes = fs::read(path)?;
    if is_binary(&bytes) || bytes.len() > MAX_FILE_BYTES {
        return Ok(None);
    }
    let mut hashes: Vec<u32> = ngram::extract_all_ngrams(&bytes)
        .map(ngram::hash_ngram)
        .collect();
    hashes.sort_unstable();
    hashes.dedup();
    Ok(Some(hashes))
}

fn is_binary(bytes: &[u8]) -> bool {
    bytes[..bytes.len().min(8 * 1024)].contains(&0)
}

pub fn fingerprint(path: &Path) -> io::Result<Fingerprint> {
    let md = fs::metadata(path)?;
    let mtime_ns = md
        .modified()
        .ok()
        .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    Ok(Fingerprint {
        mtime_ns,
        size: md.len(),
    })
}

fn sorted_diff(old: &[u32], new: &[u32]) -> (Vec<u32>, Vec<u32>) {
    let mut removes = Vec::new();
    let mut adds = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < old.len() && j < new.len() {
        if old[i] == new[j] {
            i += 1;
            j += 1;
        } else if old[i] < new[j] {
            removes.push(old[i]);
            i += 1;
        } else {
            adds.push(new[j]);
            j += 1;
        }
    }
    while i < old.len() {
        removes.push(old[i]);
        i += 1;
    }
    while j < new.len() {
        adds.push(new[j]);
        j += 1;
    }
    (removes, adds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn sorted_diff_is_correct() {
        let (r, a) = sorted_diff(&[1, 2, 5], &[2, 3, 5, 9]);
        assert_eq!(r, vec![1]);
        assert_eq!(a, vec![3, 9]);
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "isearch-watch-apply-{prefix}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn apply_actions_upsert_then_delete_updates_state_and_ops() {
        let root = temp_dir("upsert-delete");
        let file = root.join("a.txt");
        fs::write(&file, "alpha beta\n").unwrap();
        let mut state = WatchState::new(&root);

        let upsert_ops = apply_actions(&mut state, &[FileAction::Upsert(file.clone())]).unwrap();
        assert!(upsert_ops
            .iter()
            .any(|op| matches!(op, DeltaOp::UpsertPath { .. })));
        let doc_id = *state
            .path_to_doc
            .get(file.to_string_lossy().as_ref())
            .unwrap();
        let hashes_after_upsert = state.docs.get(&doc_id).unwrap().hashes.clone();
        assert!(!hashes_after_upsert.is_empty());

        fs::remove_file(&file).unwrap();
        let delete_ops = apply_actions(&mut state, &[FileAction::Delete(file.clone())]).unwrap();
        assert!(delete_ops
            .iter()
            .any(|op| matches!(op, DeltaOp::TombstoneDoc { .. })));
        assert!(state.docs.get(&doc_id).unwrap().tombstone);
        assert!(state.docs.get(&doc_id).unwrap().hashes.is_empty());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_actions_binary_file_becomes_tombstone() {
        let root = temp_dir("binary");
        let file = root.join("bin.dat");
        fs::write(&file, [0, 1, 2, 3]).unwrap();
        let mut state = WatchState::new(&root);

        let ops = apply_actions(&mut state, &[FileAction::Upsert(file.clone())]).unwrap();
        assert!(ops.is_empty(), "binary file should not produce index ops");
        let path_key = file.to_string_lossy().to_string();
        assert!(
            !state.path_to_doc.contains_key(&path_key),
            "binary file should not remain indexed"
        );
        let _ = fs::remove_dir_all(root);
    }
}
