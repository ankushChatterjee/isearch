//! Inverted index: [`types`] for shared structs, [`format`] for on-disk layouts,
//! [`builder`] for construction, [`reader`] for lookup and search, [`mmap`] for query-time I/O.

pub mod builder;
pub mod format;
pub mod mmap;
pub mod reader;
pub mod sharded;
pub mod sharded_build;
pub mod spill;
pub mod types;

pub use builder::SpillOptions;
pub use format::{index_bundle_path, pwd_hash};
pub use mmap::PostingsReadTimings;
pub use sharded::ShardedBundle;
pub use sharded_build::{build_sharded_bundle, DEFAULT_TARGET_POSTINGS_BYTES};
pub use types::{DocId, Index};
