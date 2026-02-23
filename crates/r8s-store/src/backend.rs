use std::{path::Path, sync::Arc, time::Duration};

use base64::prelude::*;
use r8s_types::{GroupVersionResource, ObjectMeta};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use tokio::sync::broadcast;

use crate::{
    error::StoreError,
    index::{FieldSelector, LabelIndex, LabelSelector},
    revision::RevisionCounter,
    watch::{WatchEvent, WatchEventType, WatchHub},
};

const RESOURCES: TableDefinition<&str, &[u8]> = TableDefinition::new("resources");
const REVISIONS: TableDefinition<u64, &[u8]> = TableDefinition::new("revisions");

/// The main storage backend, wrapping redb with K8s-compatible semantics.
#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
    revision: RevisionCounter,
    watches: WatchHub,
    index: LabelIndex,
}

pub struct ListResult {
    pub items: Vec<serde_json::Value>,
    pub resource_version: u64,
    pub continue_token: Option<String>,
}

pub struct ResourceRef<'a> {
    pub gvr: &'a GroupVersionResource,
    pub namespace: Option<&'a str>,
    pub name: &'a str,
}

impl ResourceRef<'_> {
    pub fn key(&self) -> String {
        match self.namespace {
            Some(ns) => format!("{}/{}/{}", self.gvr.key_prefix(), ns, self.name),
            None => format!("{}/{}", self.gvr.key_prefix(), self.name),
        }
    }
}

impl Store {
    /// Open or create a store at the given path.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let db = Database::create(path)?;
        // Opening tables in transaction, creates if not exists.
        let w_transaction = db.begin_write()?;
        // Scoped to drop before the commit
        {
            let _table = w_transaction.open_table(RESOURCES)?;
            let _table = w_transaction.open_table(REVISIONS)?;
        }
        w_transaction.commit()?;

        let r_transaction = db.begin_read()?;
        let table = r_transaction.open_table(REVISIONS)?;

        let max_revision = table.last()?.map(|(k, _)| k.value()).unwrap_or(0);

        let store = Self {
            db: Arc::new(db),
            revision: RevisionCounter::new(max_revision),
            watches: WatchHub::default(),
            index: LabelIndex::default(),
        };

        store.start_compaction(1000, 300);

        Ok(store)
    }

    pub fn create(
        &self,
        resource: ResourceRef,
        object: &serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let key = resource.key();
        let w_transaction = self.db.begin_write()?;
        let mut obj = object.clone();
        let rev = self.revision.next();
        let metadata = obj["metadata"]
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("metadata must be an object"))?;

        metadata.insert(
            "resourceVersion".to_string(),
            serde_json::json!(rev.to_string()),
        );
        metadata.insert(
            "uid".to_string(),
            serde_json::json!(uuid::Uuid::new_v4().to_string()),
        );
        metadata.insert(
            "creationTimestamp".to_string(),
            serde_json::json!(chrono::Utc::now().to_rfc3339()),
        );

        {
            let mut table = w_transaction.open_table(RESOURCES)?;

            if table.get(key.as_str())?.is_some() {
                return Err(StoreError::AlreadyExists {
                    gvr: resource.gvr.key_prefix(),
                    name: resource.name.to_string(),
                }
                .into());
            }

            table.insert(key.as_str(), serde_json::to_vec(&obj)?.as_slice())?;
        }

        {
            let mut table = w_transaction.open_table(REVISIONS)?;
            let rev_entry = serde_json::json!({
                "type": "ADDED",
                "key": key,
                "object": obj,
            });
            table.insert(rev, serde_json::to_vec(&rev_entry)?.as_slice())?;
        }

        w_transaction.commit()?;

        let meta: ObjectMeta =
            serde_json::from_value(obj["metadata"].clone()).unwrap_or_default();
        let empty_labels = std::collections::BTreeMap::new();
        self.index
            .update(&resource.gvr.key_prefix(), &key, meta.labels.as_ref().unwrap_or(&empty_labels));

        self.watches.notify(
            &resource.gvr.key_prefix(),
            WatchEvent {
                event_type: WatchEventType::Added,
                resource_version: rev,
                object: obj.clone(),
            },
        );

        Ok(obj)
    }

    pub fn get(&self, resource: &ResourceRef) -> anyhow::Result<Option<serde_json::Value>> {
        let r_transaction = self.db.begin_read()?;
        let table = r_transaction.open_table(RESOURCES)?;
        match table.get(resource.key().as_str())? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    pub fn update(
        &self,
        resource: &ResourceRef,
        object: &serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let w_transaction = self.db.begin_write()?;
        let key = resource.key();
        let existing: serde_json::Value = {
            let table = w_transaction.open_table(RESOURCES)?;

            match table.get(key.as_str())? {
                Some(guard) => serde_json::from_slice(guard.value())?,
                None => {
                    return Err(StoreError::NotFound {
                        gvr: resource.gvr.key_prefix(),
                        name: resource.name.to_string(),
                    }
                    .into());
                }
            }
        };

        let incoming_meta: ObjectMeta =
            serde_json::from_value(object["metadata"].clone()).unwrap_or_default();
        let stored_meta: ObjectMeta =
            serde_json::from_value(existing["metadata"].clone()).unwrap_or_default();
        let incoming_rv = incoming_meta.resource_version.as_deref().unwrap_or("");
        let stored_rv = stored_meta.resource_version.as_deref().unwrap_or("");
        if incoming_rv != stored_rv {
            return Err(StoreError::Conflict {
                gvr: resource.gvr.key_prefix(),
                name: resource.name.to_string(),
                message: format!(
                    "resourceVersion mismatch: stored={stored_rv}, incoming={incoming_rv}"
                ),
            }
            .into());
        }

        let rev = self.revision.next();

        let mut obj = object.clone();
        let metadata = obj["metadata"]
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("metadata must be an object"))?;

        metadata.insert(
            "resourceVersion".to_string(),
            serde_json::json!(rev.to_string()),
        );

        {
            let mut table = w_transaction.open_table(RESOURCES)?;
            table.insert(key.as_str(), &serde_json::to_vec(&obj)?.as_slice())?;
        }

        {
            let mut table = w_transaction.open_table(REVISIONS)?;
            let rev_entry = serde_json::json!({
                "type": "MODIFIED",
                "key": key,
                "object": obj,
            });
            table.insert(rev, serde_json::to_vec(&rev_entry)?.as_slice())?;
        }

        w_transaction.commit()?;

        let meta: ObjectMeta =
            serde_json::from_value(obj["metadata"].clone()).unwrap_or_default();
        let empty_labels = std::collections::BTreeMap::new();
        self.index
            .update(&resource.gvr.key_prefix(), &key, meta.labels.as_ref().unwrap_or(&empty_labels));

        self.watches.notify(
            &resource.gvr.key_prefix(),
            WatchEvent {
                event_type: WatchEventType::Modified,
                resource_version: rev,
                object: obj.clone(),
            },
        );

        Ok(obj)
    }

    pub fn delete(&self, resource: &ResourceRef) -> anyhow::Result<Option<serde_json::Value>> {
        let key = resource.key();
        let w_transaction = self.db.begin_write()?;

        let obj: serde_json::Value = {
            let mut table = w_transaction.open_table(RESOURCES)?;
            match table.remove(key.as_str())? {
                Some(guard) => serde_json::from_slice(guard.value())?,
                None => return Ok(None),
            }
        };

        let rev = self.revision.next();

        {
            let mut table = w_transaction.open_table(REVISIONS)?;
            let rev_entry = serde_json::json!({
                "type": "DELETED",
                "key": key,
                "object": obj,
            });
            table.insert(rev, serde_json::to_vec(&rev_entry)?.as_slice())?;
        }

        w_transaction.commit()?;

        self.index.remove(&resource.gvr.key_prefix(), &key);

        self.watches.notify(
            &resource.gvr.key_prefix(),
            WatchEvent {
                event_type: WatchEventType::Deleted,
                resource_version: rev,
                object: obj.clone(),
            },
        );

        Ok(Some(obj))
    }

    pub fn list(
        &self,
        gvr: &GroupVersionResource,
        namespace: Option<&str>,
        label_selector: Option<&LabelSelector>,
        field_selector: Option<&FieldSelector>,
        limit: Option<usize>,
        continue_token: Option<&str>,
    ) -> anyhow::Result<ListResult> {
        let scan_prefix = match namespace {
            Some(ns) => format!("{}/{}/", gvr.key_prefix(), ns),
            None => format!("{}/", gvr.key_prefix()),
        };

        let resource_version = self.revision.current();

        let r_transaction = self.db.begin_read()?;

        let table = r_transaction.open_table(RESOURCES)?;

        let mut items = Vec::new();
        let mut last_key: Option<String> = None;

        let candidates = label_selector.and_then(|s| self.index.matches(&gvr.key_prefix(), s));

        // If continue_token is set, decode it and start scanning after that key
        let range_start = match continue_token {
            Some(token) => String::from_utf8(
                BASE64_STANDARD
                    .decode(token)
                    .map_err(|e| anyhow::anyhow!("invalid continue token: {e}"))?,
            )
            .map_err(|e| anyhow::anyhow!("invalid continue token: {e}"))?,
            None => scan_prefix.clone(),
        };

        let mut skipped_first = continue_token.is_none(); // skip the continue key itself

        for entry in table.range(range_start.as_str()..)? {
            let (key, value) = entry?;
            if !key.value().starts_with(scan_prefix.as_str()) {
                break;
            }

            // Skip the continue_token key itself (exclusive start)
            if !skipped_first {
                skipped_first = true;
                continue;
            }

            if let Some(ref candidates) = candidates
                && !candidates.contains(key.value())
            {
                continue;
            }
            let item: serde_json::Value = serde_json::from_slice(value.value())?;
            if let Some(ls) = label_selector
                && !ls.matches(&item)
            {
                continue;
            }
            if let Some(fs) = field_selector
                && !fs.matches(&item)
            {
                continue;
            }
            last_key = Some(key.value().to_string());
            items.push(item);

            if let Some(limit) = limit
                && items.len() >= limit
            {
                break;
            }
        }

        // If we hit the limit, encode the last key as continue token
        let continue_token = if let Some(limit) = limit {
            if items.len() >= limit {
                last_key.map(|k| BASE64_STANDARD.encode(k))
            } else {
                None
            }
        } else {
            None
        };

        Ok(ListResult {
            items,
            resource_version,
            continue_token,
        })
    }

    /// Returns (current_revision, revision_table_entries, resource_count).
    pub fn stats(&self) -> anyhow::Result<(u64, u64, u64)> {
        let current_rev = self.revision.current();
        let r_transaction = self.db.begin_read()?;
        let rev_table = r_transaction.open_table(REVISIONS)?;
        let rev_count = rev_table.len()?;
        let res_table = r_transaction.open_table(RESOURCES)?;
        let res_count = res_table.len()?;
        Ok((current_rev, rev_count, res_count))
    }

    pub fn watch(&self, gvr: &GroupVersionResource) -> broadcast::Receiver<WatchEvent> {
        self.watches.subscribe(&gvr.key_prefix())
    }

    pub fn start_compaction(&self, keep: u64, interval_secs: u64) {
        let db = self.db.clone();
        let revision = self.revision.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                if let Err(e) = compact(&db, &revision, keep) {
                    tracing::warn!("compaction failed: {e}")
                }
            }
        });
    }
}

fn compact(db: &Database, revision: &RevisionCounter, keep: u64) -> anyhow::Result<()> {
    let current = revision.current();
    if current <= keep {
        return Ok(());
    }
    let cutoff = current - keep;

    let w_transaction = db.begin_write()?;
    {
        let mut table = w_transaction.open_table(REVISIONS)?;
        // Drain all entries with key <= cutoff
        let to_remove: Vec<u64> = table
            .range(..=cutoff)?
            .map(|entry| entry.map(|(k, _)| k.value()))
            .collect::<Result<_, _>>()?;
        for key in to_remove {
            table.remove(key)?;
        }
    }
    w_transaction.commit()?;
    Ok(())
}
