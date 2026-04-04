//! Inverted index: [`types`] for shared structs, [`format`] for on-disk layouts,
//! [`builder`] for construction, [`reader`] for lookup and search, [`mmap`] for query-time I/O.

pub mod builder;
pub mod format;
pub mod mmap;
pub mod reader;
pub mod spill;
pub mod types;

pub use builder::{BuildOutput, SpillOptions};
pub use format::write_paths_and_meta;
pub use format::{index_bundle_path, pwd_hash, write_bundle};
pub use mmap::{MmapBundle, PostingsReadTimings};
pub use types::{DocId, Index};
