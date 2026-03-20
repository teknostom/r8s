pub mod backend;
pub mod error;
pub mod index;
pub mod revision;
pub mod watch;

pub use backend::Store;

pub fn stats(path: &std::path::Path) -> anyhow::Result<(u64, u64, u64)> {
    use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
    const RESOURCES: TableDefinition<&str, &[u8]> = TableDefinition::new("resources");
    const REVISIONS: TableDefinition<u64, &[u8]> = TableDefinition::new("revisions");

    let db = Database::create(path)?;
    let tx = db.begin_read()?;
    let rev_table = tx.open_table(REVISIONS)?;
    let res_table = tx.open_table(RESOURCES)?;
    let current_rev = rev_table.last()?.map(|(k, _)| k.value()).unwrap_or(0);
    let rev_entries = rev_table.len()?;
    let res_count = res_table.len()?;
    Ok((current_rev, rev_entries, res_count))
}
