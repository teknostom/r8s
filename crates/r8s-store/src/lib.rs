//! Embedded resource store backing the API server, built on [`redb`].
//!
//! Objects are kept as JSON in a `resources` table keyed by
//! `group/version/resource[/namespace]/name`, alongside a `revisions` table
//! that records a bounded window of recent mutations for durable watch
//! replay. Every mutation allocates a monotonic `resourceVersion`;
//! reads take a consistent snapshot under a single lock so that a list's
//! version and the events delivered to a concurrently-opened watch never
//! overlap or skip. All fallible operations return [`StoreError`].

pub mod backend;
pub mod error;
pub mod index;
pub mod revision;
pub mod watch;

pub use backend::{Store, StoreStats};
pub use error::{Result, StoreError};

use backend::{RESOURCES, REVISIONS};

/// Read store statistics directly from a database file, without opening a live
/// [`Store`]. Intended for offline inspection.
pub fn stats(path: &std::path::Path) -> Result<StoreStats> {
    use redb::{Database, ReadableTable, ReadableTableMetadata};

    let db = Database::create(path)?;
    let txn = db.begin_read()?;
    let revisions = txn.open_table(REVISIONS)?;
    let resources = txn.open_table(RESOURCES)?;
    Ok(StoreStats {
        current_revision: revisions.last()?.map(|(k, _)| k.value()).unwrap_or(0),
        revision_entries: revisions.len()?,
        resource_count: resources.len()?,
    })
}
