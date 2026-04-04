use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::index::format::{LOOKUP_FILENAME, META_FILENAME, PATHS_FILENAME, POSTINGS_FILENAME};
use crate::index::{write_bundle, Index};

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

    let docs: Vec<(String, Vec<u32>)> = alive
        .iter()
        .map(|d| (d.path.clone(), d.hashes.clone()))
        .collect();
    let (store, index) = Index::build_from_doc_hashes(&docs)?;
    let tmp_bundle = bundle_dir.join(format!(
        ".compact-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    fs::create_dir_all(&tmp_bundle)?;
    write_bundle(&tmp_bundle, &index, &store, root)?;
    for name in [LOOKUP_FILENAME, POSTINGS_FILENAME, PATHS_FILENAME, META_FILENAME] {
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
