pub mod apply;
pub mod compact;
pub mod delta;
pub mod events;
pub mod state;

use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use notify::{recommended_watcher, RecursiveMode, Watcher};

use crate::index::format::{read_paths_lines, LOOKUP_FILENAME, META_FILENAME, PATHS_FILENAME, POSTINGS_FILENAME};

use self::apply::{apply_actions, fingerprint};
use self::delta::{DeltaOp, DeltaWriter, DELTA_FILENAME};
use self::events::{actions_from_notify_event, Coalescer, FileAction};
use self::state::WatchState;

const WATCH_STATE_FILENAME: &str = "watch_state.bin";
const WATCH_LOCK_FILENAME: &str = ".watch.lock";
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct WatchConfig {
    pub root: PathBuf,
    pub bundle_dir: PathBuf,
    pub debounce_ms: u64,
    pub compact_interval_secs: u64,
    pub max_batch_files: usize,
    pub verbose: bool,
}

pub fn run(cfg: WatchConfig) -> io::Result<()> {
    let _lock = WatchLock::acquire(&cfg.bundle_dir.join(WATCH_LOCK_FILENAME))?;
    log_watch(&format!(
        "lock acquired at {}",
        cfg.bundle_dir.join(WATCH_LOCK_FILENAME).display()
    ));
    fs::create_dir_all(&cfg.bundle_dir)?;
    let state_path = cfg.bundle_dir.join(WATCH_STATE_FILENAME);
    let delta_path = cfg.bundle_dir.join(DELTA_FILENAME);

    let mut state = if let Some(s) = WatchState::load(&state_path)? {
        log_watch(&format!("loaded watch state from {}", state_path.display()));
        s
    } else {
        log_watch("watch state not found; bootstrapping from baseline bundle");
        bootstrap_state(&cfg.root, &cfg.bundle_dir)?
    };
    let (replayed, new_off) = delta::replay(&delta_path, state.last_delta_offset)?;
    if !replayed.is_empty() {
        log_watch(&format!(
            "replayed {} delta op(s) from offset {}",
            replayed.len(),
            state.last_delta_offset
        ));
        apply_replayed_ops(&mut state, &replayed);
        state.dirty = true;
    } else {
        log_watch("no delta ops to replay");
    }
    state.last_delta_offset = new_off;

    let mut delta_writer = DeltaWriter::open(&delta_path)?;
    if state.last_delta_offset < delta::header_len() {
        state.last_delta_offset = delta::header_len();
    }
    state.persist(&state_path)?;
    log_watch(&format!(
        "state checkpointed at startup -> {}",
        state_path.display()
    ));

    let (tx, rx) = mpsc::channel();
    let mut watcher = recommended_watcher(move |res| {
        let _ = tx.send(res);
    })
    .map_err(io::Error::other)?;
    watcher
        .watch(&cfg.root, RecursiveMode::Recursive)
        .map_err(io::Error::other)?;
    let should_stop = Arc::new(AtomicBool::new(false));
    let stop_for_handler = Arc::clone(&should_stop);
    ctrlc::set_handler(move || {
        stop_for_handler.store(true, Ordering::SeqCst);
    })
    .map_err(io::Error::other)?;

    let mut coalescer = Coalescer::new(Duration::from_millis(cfg.debounce_ms));
    let mut last_compact = Instant::now();
    let mut last_checkpoint = Instant::now();

    log_watch(&format!(
        "watching {} (debounce={}ms, compact={}s, max_batch_files={})",
        cfg.root.display(),
        cfg.debounce_ms,
        cfg.compact_interval_secs,
        cfg.max_batch_files
    ));

    while !should_stop.load(Ordering::SeqCst) {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(Ok(event)) => {
                for action in actions_from_notify_event(&event) {
                    if cfg.verbose {
                        log_watch(&format!("raw fs event -> {}", action_display(&action)));
                    }
                    coalescer.push(action, Instant::now());
                }
            }
            Ok(Err(err)) => {
                eprintln!("watch event error: {err}");
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(io::Error::other("watch channel disconnected"));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }

        let ready = coalescer.drain_ready(Instant::now(), cfg.max_batch_files);
        if !ready.is_empty() {
            for action in &ready {
                log_watch(&format!("change -> {}", action_display(action)));
            }
            let ops = apply_actions(&mut state, &ready)?;
            if !ops.is_empty() {
                let new_offset = delta_writer.append_batch(&ops)?;
                state.last_delta_offset = new_offset;
                state.dirty = true;
                log_watch(&format!(
                    "applied {} path change(s) -> {} delta op(s), delta_offset={}",
                    ready.len(),
                    ops.len(),
                    state.last_delta_offset
                ));
            } else {
                log_watch(&format!(
                    "applied {} path change(s) -> 0 delta op(s) (no-op after diff)",
                    ready.len()
                ));
            }
        }

        if last_compact.elapsed() >= Duration::from_secs(cfg.compact_interval_secs.max(1)) {
            log_watch("compaction started");
            compact::compact(&cfg.bundle_dir, &cfg.root, &state_path, &delta_path, &mut state)?;
            log_watch("compaction finished");
            last_compact = Instant::now();
            last_checkpoint = Instant::now();
            continue;
        }

        if state.dirty && last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL {
            state.persist(&state_path)?;
            log_watch(&format!("periodic checkpoint -> {}", state_path.display()));
            last_checkpoint = Instant::now();
        }
    }
    log_watch("shutdown signal received");
    if state.dirty {
        state.persist(&state_path)?;
        log_watch(&format!("final checkpoint -> {}", state_path.display()));
    }
    log_watch("watch loop exited cleanly");
    Ok(())
}

fn bootstrap_state(root: &Path, bundle_dir: &Path) -> io::Result<WatchState> {
    let mut state = WatchState::new(root);
    let paths = read_paths_lines(&bundle_dir.join(PATHS_FILENAME))?;
    for path in paths {
        let p = PathBuf::from(&path);
        if !p.is_file() {
            continue;
        }
        let doc_id = state.ensure_doc_for_path(&path);
        let bytes = fs::read(&p)?;
        if is_binary(&bytes) {
            continue;
        }
        let mut hashes: Vec<u32> = crate::ngram::extract_all_ngrams(&bytes)
            .map(crate::ngram::hash_ngram)
            .collect();
        hashes.sort_unstable();
        hashes.dedup();
        if let Some(doc) = state.docs.get_mut(&doc_id) {
            doc.hashes = hashes;
            doc.fingerprint = fingerprint(&p)?;
            doc.tombstone = false;
        }
    }
    Ok(state)
}

fn is_binary(bytes: &[u8]) -> bool {
    bytes[..bytes.len().min(8 * 1024)].contains(&0)
}

pub fn has_base_bundle(bundle_dir: &Path) -> bool {
    bundle_dir.join(LOOKUP_FILENAME).is_file()
        && bundle_dir.join(POSTINGS_FILENAME).is_file()
        && bundle_dir.join(PATHS_FILENAME).is_file()
        && bundle_dir.join(META_FILENAME).is_file()
}

pub fn load_query_docs(bundle_dir: &Path) -> io::Result<Option<Vec<(u32, String, Vec<u32>)>>> {
    let state_path = bundle_dir.join(WATCH_STATE_FILENAME);
    let delta_path = bundle_dir.join(DELTA_FILENAME);
    let Some(mut state) = WatchState::load(&state_path)? else {
        return Ok(None);
    };
    let (ops, _) = delta::replay(&delta_path, state.last_delta_offset)?;
    if !ops.is_empty() {
        apply_replayed_ops(&mut state, &ops);
    }
    let mut docs = Vec::new();
    for (doc_id, doc) in state.docs {
        if doc.tombstone {
            continue;
        }
        docs.push((doc_id, doc.path, doc.hashes));
    }
    docs.sort_unstable_by_key(|(doc_id, _, _)| *doc_id);
    Ok(Some(docs))
}

fn apply_replayed_ops(state: &mut WatchState, ops: &[DeltaOp]) {
    for op in ops {
        match op {
            DeltaOp::AddHash { doc_id, hash } => {
                if let Some(doc) = state.docs.get_mut(doc_id) {
                    if doc.hashes.binary_search(hash).is_err() {
                        doc.hashes.push(*hash);
                        doc.hashes.sort_unstable();
                    }
                }
            }
            DeltaOp::RemoveHash { doc_id, hash } => {
                if let Some(doc) = state.docs.get_mut(doc_id) {
                    if let Ok(idx) = doc.hashes.binary_search(hash) {
                        doc.hashes.remove(idx);
                    }
                }
            }
            DeltaOp::TombstoneDoc { doc_id } => {
                if let Some(doc) = state.docs.get_mut(doc_id) {
                    doc.tombstone = true;
                }
            }
            DeltaOp::UpsertPath { doc_id, path } => {
                if !state.docs.contains_key(doc_id) {
                    state.docs.insert(
                        *doc_id,
                        state::DocMeta {
                            path: path.clone(),
                            fingerprint: state::Fingerprint::default(),
                            hashes: Vec::new(),
                            tombstone: false,
                        },
                    );
                } else {
                    state.set_doc_path(*doc_id, path.clone());
                }
                state.path_to_doc.insert(path.clone(), *doc_id);
                state.next_doc_id = state.next_doc_id.max(doc_id.saturating_add(1));
            }
        }
    }
}

fn action_display(action: &FileAction) -> String {
    match action {
        FileAction::Upsert(path) => format!("UPSERT {}", path.display()),
        FileAction::Delete(path) => format!("DELETE {}", path.display()),
    }
}

fn log_watch(msg: &str) {
    eprintln!("[watch] {msg}");
}

struct WatchLock {
    path: PathBuf,
}

impl WatchLock {
    fn acquire(path: &Path) -> io::Result<Self> {
        let _ = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }
}

impl Drop for WatchLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
