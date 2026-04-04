use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const STATE_MAGIC: [u8; 8] = *b"ISWSTATE";
const STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Default)]
pub struct Fingerprint {
    pub mtime_ns: u64,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct DocMeta {
    pub path: String,
    pub fingerprint: Fingerprint,
    pub hashes: Vec<u32>,
    pub tombstone: bool,
}

#[derive(Debug, Clone)]
pub struct WatchState {
    pub root: String,
    pub next_doc_id: u32,
    pub docs: BTreeMap<u32, DocMeta>,
    pub path_to_doc: HashMap<String, u32>,
    pub last_delta_offset: u64,
    pub last_compaction_unix_secs: u64,
    pub dirty: bool,
}

impl WatchState {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_string_lossy().into_owned(),
            next_doc_id: 0,
            docs: BTreeMap::new(),
            path_to_doc: HashMap::new(),
            last_delta_offset: 8,
            last_compaction_unix_secs: now_unix_secs(),
            dirty: false,
        }
    }

    pub fn load(path: &Path) -> io::Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        fs::File::open(path)?.read_to_end(&mut bytes)?;
        let mut cur = 0usize;

        let magic = read_exact_array::<8>(&bytes, &mut cur)?;
        if magic != STATE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid watch_state magic",
            ));
        }
        let version = read_u32(&bytes, &mut cur)?;
        if version != STATE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported watch_state version {version}"),
            ));
        }

        let root = read_string(&bytes, &mut cur)?;
        let next_doc_id = read_u32(&bytes, &mut cur)?;
        let last_delta_offset = read_u64(&bytes, &mut cur)?;
        let last_compaction_unix_secs = read_u64(&bytes, &mut cur)?;
        let doc_count = read_u32(&bytes, &mut cur)? as usize;

        let mut docs = BTreeMap::new();
        let mut path_to_doc = HashMap::new();
        for _ in 0..doc_count {
            let doc_id = read_u32(&bytes, &mut cur)?;
            let tombstone = read_u8(&bytes, &mut cur)? != 0;
            let mtime_ns = read_u64(&bytes, &mut cur)?;
            let size = read_u64(&bytes, &mut cur)?;
            let path = read_string(&bytes, &mut cur)?;
            let hash_count = read_u32(&bytes, &mut cur)? as usize;
            let mut hashes = Vec::with_capacity(hash_count);
            for _ in 0..hash_count {
                hashes.push(read_u32(&bytes, &mut cur)?);
            }
            hashes.sort_unstable();
            hashes.dedup();

            path_to_doc.insert(path.clone(), doc_id);
            docs.insert(
                doc_id,
                DocMeta {
                    path,
                    fingerprint: Fingerprint { mtime_ns, size },
                    hashes,
                    tombstone,
                },
            );
        }

        Ok(Some(Self {
            root,
            next_doc_id,
            docs,
            path_to_doc,
            last_delta_offset,
            last_compaction_unix_secs,
            dirty: false,
        }))
    }

    pub fn persist(&mut self, path: &Path) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&STATE_MAGIC);
        buf.extend_from_slice(&STATE_VERSION.to_le_bytes());
        write_string(&mut buf, &self.root)?;
        buf.extend_from_slice(&self.next_doc_id.to_le_bytes());
        buf.extend_from_slice(&self.last_delta_offset.to_le_bytes());
        buf.extend_from_slice(&self.last_compaction_unix_secs.to_le_bytes());
        let doc_count = u32::try_from(self.docs.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "too many docs for watch state")
        })?;
        buf.extend_from_slice(&doc_count.to_le_bytes());

        for (doc_id, doc) in &self.docs {
            buf.extend_from_slice(&doc_id.to_le_bytes());
            buf.push(u8::from(doc.tombstone));
            buf.extend_from_slice(&doc.fingerprint.mtime_ns.to_le_bytes());
            buf.extend_from_slice(&doc.fingerprint.size.to_le_bytes());
            write_string(&mut buf, &doc.path)?;
            let hash_count = u32::try_from(doc.hashes.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "too many hashes in doc")
            })?;
            buf.extend_from_slice(&hash_count.to_le_bytes());
            for h in &doc.hashes {
                buf.extend_from_slice(&h.to_le_bytes());
            }
        }

        let mut f = fs::File::create(path)?;
        f.write_all(&buf)?;
        self.dirty = false;
        Ok(())
    }

    pub fn ensure_doc_for_path(&mut self, path: &str) -> u32 {
        if let Some(&doc_id) = self.path_to_doc.get(path) {
            return doc_id;
        }
        let doc_id = self.next_doc_id;
        self.next_doc_id = self.next_doc_id.saturating_add(1);
        self.docs.insert(
            doc_id,
            DocMeta {
                path: path.to_owned(),
                fingerprint: Fingerprint::default(),
                hashes: Vec::new(),
                tombstone: false,
            },
        );
        self.path_to_doc.insert(path.to_owned(), doc_id);
        self.dirty = true;
        doc_id
    }

    pub fn set_doc_path(&mut self, doc_id: u32, new_path: String) {
        if let Some(doc) = self.docs.get_mut(&doc_id) {
            self.path_to_doc.remove(&doc.path);
            doc.path = new_path.clone();
            self.path_to_doc.insert(new_path, doc_id);
            self.dirty = true;
        }
    }
}

pub fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn write_string(buf: &mut Vec<u8>, s: &str) -> io::Result<()> {
    let n = u32::try_from(s.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "string too large"))?;
    buf.extend_from_slice(&n.to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], cur: &mut usize) -> io::Result<String> {
    let n = read_u32(bytes, cur)? as usize;
    if bytes.len().saturating_sub(*cur) < n {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short string"));
    }
    let out = std::str::from_utf8(&bytes[*cur..*cur + n])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .to_owned();
    *cur += n;
    Ok(out)
}

fn read_exact_array<const N: usize>(bytes: &[u8], cur: &mut usize) -> io::Result<[u8; N]> {
    if bytes.len().saturating_sub(*cur) < N {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short array"));
    }
    let mut out = [0u8; N];
    out.copy_from_slice(&bytes[*cur..*cur + N]);
    *cur += N;
    Ok(out)
}

fn read_u8(bytes: &[u8], cur: &mut usize) -> io::Result<u8> {
    Ok(read_exact_array::<1>(bytes, cur)?[0])
}

fn read_u32(bytes: &[u8], cur: &mut usize) -> io::Result<u32> {
    Ok(u32::from_le_bytes(read_exact_array::<4>(bytes, cur)?))
}

fn read_u64(bytes: &[u8], cur: &mut usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(read_exact_array::<8>(bytes, cur)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_roundtrip() {
        let root = std::env::temp_dir().join("isearch-watch-state-test");
        let mut state = WatchState::new(&root);
        let doc = state.ensure_doc_for_path("/tmp/a.txt");
        state.set_doc_path(doc, "/tmp/b.txt".to_string());
        {
            let meta = state.docs.get_mut(&doc).unwrap();
            meta.hashes = vec![1, 2, 2, 3];
            meta.hashes.sort_unstable();
            meta.hashes.dedup();
        }
        let file = std::env::temp_dir().join(format!("watch-state-{}.bin", now_unix_secs()));
        state.persist(&file).unwrap();

        let loaded = WatchState::load(&file).unwrap().unwrap();
        assert_eq!(loaded.path_to_doc.get("/tmp/b.txt"), Some(&doc));
        assert_eq!(loaded.docs.get(&doc).unwrap().hashes, vec![1, 2, 3]);
        let _ = fs::remove_file(file);
    }
}
