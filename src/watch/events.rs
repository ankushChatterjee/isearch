use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use notify::{event::ModifyKind, event::RenameMode, Event, EventKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileAction {
    Upsert(PathBuf),
    Delete(PathBuf),
}

#[derive(Debug)]
pub struct Coalescer {
    debounce: Duration,
    pending: HashMap<PathBuf, Pending>,
}

#[derive(Debug, Clone, Copy)]
struct Pending {
    last_seen: Instant,
    kind: PendingKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingKind {
    Upsert,
    Delete,
}

impl Coalescer {
    pub fn new(debounce: Duration) -> Self {
        Self {
            debounce,
            pending: HashMap::new(),
        }
    }

    pub fn push(&mut self, action: FileAction, now: Instant) {
        match action {
            FileAction::Upsert(path) => self.insert(path, PendingKind::Upsert, now),
            FileAction::Delete(path) => self.insert(path, PendingKind::Delete, now),
        }
    }

    fn insert(&mut self, path: PathBuf, kind: PendingKind, now: Instant) {
        let prev = self.pending.get(&path).map(|p| p.kind);
        let merged = match (prev, kind) {
            (Some(PendingKind::Delete), PendingKind::Upsert) => PendingKind::Upsert,
            (Some(PendingKind::Upsert), PendingKind::Delete) => PendingKind::Delete,
            (_, next) => next,
        };
        self.pending.insert(
            path,
            Pending {
                last_seen: now,
                kind: merged,
            },
        );
    }

    pub fn drain_ready(&mut self, now: Instant, max: usize) -> Vec<FileAction> {
        let mut ready = Vec::new();
        let mut done = Vec::new();
        for (path, pending) in &self.pending {
            if now.duration_since(pending.last_seen) >= self.debounce {
                done.push(path.clone());
            }
        }
        done.sort();
        for path in done.into_iter().take(max) {
            if let Some(pending) = self.pending.remove(&path) {
                ready.push(match pending.kind {
                    PendingKind::Upsert => FileAction::Upsert(path),
                    PendingKind::Delete => FileAction::Delete(path),
                });
            }
        }
        ready
    }
}

pub fn actions_from_notify_event(event: &Event) -> Vec<FileAction> {
    match &event.kind {
        EventKind::Remove(_) => event
            .paths
            .iter()
            .cloned()
            .map(FileAction::Delete)
            .collect(),
        EventKind::Create(_) => event
            .paths
            .iter()
            .cloned()
            .map(FileAction::Upsert)
            .collect(),
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)) if event.paths.len() >= 2 => {
            vec![
                FileAction::Delete(event.paths[0].clone()),
                FileAction::Upsert(event.paths[1].clone()),
            ]
        }
        EventKind::Modify(ModifyKind::Name(RenameMode::To)) => event
            .paths
            .iter()
            .cloned()
            .map(FileAction::Upsert)
            .collect(),
        EventKind::Modify(ModifyKind::Name(RenameMode::From)) => event
            .paths
            .iter()
            .cloned()
            .map(FileAction::Delete)
            .collect(),
        EventKind::Modify(_) => event
            .paths
            .iter()
            .cloned()
            .map(FileAction::Upsert)
            .collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalescer_debounces_and_merges() {
        let mut c = Coalescer::new(Duration::from_millis(100));
        let now = Instant::now();
        let p = PathBuf::from("/tmp/file.txt");
        c.push(FileAction::Upsert(p.clone()), now);
        c.push(
            FileAction::Upsert(p.clone()),
            now + Duration::from_millis(20),
        );
        assert!(c
            .drain_ready(now + Duration::from_millis(60), 100)
            .is_empty());
        let ready = c.drain_ready(now + Duration::from_millis(150), 100);
        assert_eq!(ready, vec![FileAction::Upsert(p)]);
    }
}
