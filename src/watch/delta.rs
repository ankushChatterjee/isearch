use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use crate::index::format::{push_u32_varint, read_u32_varint_from_slice};

pub const DELTA_FILENAME: &str = "delta.bin";
const DELTA_MAGIC: [u8; 8] = *b"ISDELTA1";
const DELTA_HEADER_LEN: u64 = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaOp {
    AddHash { doc_id: u32, hash: u32 },
    RemoveHash { doc_id: u32, hash: u32 },
    TombstoneDoc { doc_id: u32 },
    UpsertPath { doc_id: u32, path: String },
}

pub struct DeltaWriter {
    file: File,
    last_sync: Instant,
    sync_interval: Duration,
}

impl DeltaWriter {
    pub fn open(path: &Path) -> io::Result<Self> {
        if !path.exists() {
            let mut f = File::create(path)?;
            f.write_all(&DELTA_MAGIC)?;
            f.flush()?;
        }
        let mut f = OpenOptions::new().read(true).append(true).open(path)?;
        let mut magic = [0u8; 8];
        f.seek(SeekFrom::Start(0))?;
        f.read_exact(&mut magic)?;
        if magic != DELTA_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid delta magic",
            ));
        }
        f.seek(SeekFrom::End(0))?;
        Ok(Self {
            file: f,
            last_sync: Instant::now(),
            sync_interval: Duration::from_secs(2),
        })
    }

    pub fn append_batch(&mut self, ops: &[DeltaOp]) -> io::Result<u64> {
        if ops.is_empty() {
            return self.file.stream_position();
        }
        let payload = encode_ops(ops)?;
        let len = u32::try_from(payload.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "delta frame too large"))?;
        let crc = crc32fast::hash(&payload);
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&payload)?;
        if self.last_sync.elapsed() >= self.sync_interval {
            self.file.sync_data()?;
            self.last_sync = Instant::now();
        }
        self.file.stream_position()
    }
}

pub fn replay(path: &Path, start_offset: u64) -> io::Result<(Vec<DeltaOp>, u64)> {
    if !path.exists() {
        return Ok((Vec::new(), DELTA_HEADER_LEN));
    }
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() < DELTA_HEADER_LEN as usize {
        return Ok((Vec::new(), DELTA_HEADER_LEN));
    }
    if bytes[0..8] != DELTA_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid delta magic",
        ));
    }

    let mut cur = start_offset.max(DELTA_HEADER_LEN) as usize;
    let mut ops = Vec::new();
    while bytes.len().saturating_sub(cur) >= 8 {
        let len = u32::from_le_bytes(bytes[cur..cur + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[cur + 4..cur + 8].try_into().unwrap());
        cur += 8;
        if bytes.len().saturating_sub(cur) < len {
            break;
        }
        let frame = &bytes[cur..cur + len];
        if crc32fast::hash(frame) != crc {
            break;
        }
        let decoded = decode_ops(frame)?;
        ops.extend(decoded);
        cur += len;
    }
    Ok((ops, cur as u64))
}

pub fn reset(path: &Path) -> io::Result<()> {
    let mut f = fs::File::create(path)?;
    f.write_all(&DELTA_MAGIC)?;
    f.flush()
}

pub fn header_len() -> u64 {
    DELTA_HEADER_LEN
}

fn encode_ops(ops: &[DeltaOp]) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    push_u32_varint(
        &mut out,
        u32::try_from(ops.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "too many ops"))?,
    );
    for op in ops {
        match op {
            DeltaOp::AddHash { doc_id, hash } => {
                out.push(1);
                push_u32_varint(&mut out, *doc_id);
                push_u32_varint(&mut out, *hash);
            }
            DeltaOp::RemoveHash { doc_id, hash } => {
                out.push(2);
                push_u32_varint(&mut out, *doc_id);
                push_u32_varint(&mut out, *hash);
            }
            DeltaOp::TombstoneDoc { doc_id } => {
                out.push(3);
                push_u32_varint(&mut out, *doc_id);
            }
            DeltaOp::UpsertPath { doc_id, path } => {
                out.push(4);
                push_u32_varint(&mut out, *doc_id);
                push_u32_varint(
                    &mut out,
                    u32::try_from(path.len())
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "path too long"))?,
                );
                out.extend_from_slice(path.as_bytes());
            }
        }
    }
    Ok(out)
}

fn decode_ops(payload: &[u8]) -> io::Result<Vec<DeltaOp>> {
    let mut cur = 0usize;
    let n = read_u32_varint_from_slice(payload, &mut cur)? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        if cur >= payload.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "delta op tag"));
        }
        let tag = payload[cur];
        cur += 1;
        match tag {
            1 => {
                let doc_id = read_u32_varint_from_slice(payload, &mut cur)?;
                let hash = read_u32_varint_from_slice(payload, &mut cur)?;
                out.push(DeltaOp::AddHash { doc_id, hash });
            }
            2 => {
                let doc_id = read_u32_varint_from_slice(payload, &mut cur)?;
                let hash = read_u32_varint_from_slice(payload, &mut cur)?;
                out.push(DeltaOp::RemoveHash { doc_id, hash });
            }
            3 => {
                let doc_id = read_u32_varint_from_slice(payload, &mut cur)?;
                out.push(DeltaOp::TombstoneDoc { doc_id });
            }
            4 => {
                let doc_id = read_u32_varint_from_slice(payload, &mut cur)?;
                let path_len = read_u32_varint_from_slice(payload, &mut cur)? as usize;
                if payload.len().saturating_sub(cur) < path_len {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "delta path"));
                }
                let path = std::str::from_utf8(&payload[cur..cur + path_len])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .to_owned();
                cur += path_len;
                out.push(DeltaOp::UpsertPath { doc_id, path });
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown delta tag {tag}"),
                ));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_file(name: &str) -> std::path::PathBuf {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("{name}-{t}.bin"))
    }

    #[test]
    fn replay_ignores_trailing_partial_frame() {
        let file = temp_file("isearch-delta");
        let mut w = DeltaWriter::open(&file).unwrap();
        let ops = vec![
            DeltaOp::AddHash {
                doc_id: 1,
                hash: 10,
            },
            DeltaOp::UpsertPath {
                doc_id: 1,
                path: "/tmp/a".to_string(),
            },
        ];
        let _ = w.append_batch(&ops).unwrap();
        drop(w);

        let mut f = OpenOptions::new().append(true).open(&file).unwrap();
        f.write_all(&[1, 2, 3]).unwrap();
        drop(f);

        let (got, offset) = replay(&file, header_len()).unwrap();
        assert_eq!(got, ops);
        assert!(offset > header_len());
        let _ = fs::remove_file(file);
    }
}
