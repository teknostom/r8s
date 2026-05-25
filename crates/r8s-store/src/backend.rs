use std::{
    path::Path,
    sync::{Arc, RwLock},
};

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

// History window for the REVISIONS table. Each write prunes the entry
// `KEEP_REVISIONS` behind the new one, so the table stays bounded without
// a background task.
//
// TODO(410-gone): nothing reads REVISIONS yet. When watch-from-resourceVersion
// and resourceVersionMatch=Exact list reads are wired up, requests referencing
// any RV older than `current - KEEP_REVISIONS` must return HTTP 410 Gone
// (status reason "Expired"). There is a single threshold — the oldest
// retained RV — checked per request; clients respond by relisting.
const KEEP_REVISIONS: u64 = 1000;

#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
    revision: RevisionCounter,
    watches: WatchHub,
    index: LabelIndex,
    view_lock: Arc<RwLock<()>>,
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
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let db = Database::create(path)?;
        let w_transaction = db.begin_write()?;
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
            view_lock: Arc::new(RwLock::new(())),
        };

        Ok(store)
    }

    pub fn create(
        &self,
        resource: ResourceRef,
        object: &serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let key = resource.key();
        let _view_guard = self.view_lock.write().expect("view_lock poisoned");
        let w_transaction = self.db.begin_write()?;
        let mut obj = object.clone();
        let rev = self.revision.next();
        let metadata = obj
            .get_mut("metadata")
            .and_then(|v| v.as_object_mut())
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
            prune_revision(&mut table, rev)?;
        }

        w_transaction.commit()?;

        let meta: ObjectMeta =
            serde_json::from_value(obj.get("metadata").cloned().unwrap_or_default())
                .unwrap_or_default();
        let labels = meta.labels.unwrap_or_default();
        self.index.update(&resource.gvr.key_prefix(), &key, &labels);

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
        let key = resource.key();
        let _view_guard = self.view_lock.write().expect("view_lock poisoned");
        let w_transaction = self.db.begin_write()?;
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
            serde_json::from_value(object.get("metadata").cloned().unwrap_or_default())
                .unwrap_or_default();
        let stored_meta: ObjectMeta =
            serde_json::from_value(existing.get("metadata").cloned().unwrap_or_default())
                .unwrap_or_default();
        let incoming_rv = incoming_meta.resource_version.as_deref().unwrap_or("");
        let stored_rv = stored_meta.resource_version.as_deref().unwrap_or("");
        // An empty incoming resourceVersion means "blind update" — accept the
        // current stored version. K8s semantics: only enforce the version check
        // when the client explicitly sent one.
        if !incoming_rv.is_empty() && incoming_rv != stored_rv {
            return Err(StoreError::Conflict {
                gvr: resource.gvr.key_prefix(),
                name: resource.name.to_string(),
                message: format!(
                    "resourceVersion mismatch: stored={stored_rv}, incoming={incoming_rv}"
                ),
            }
            .into());
        }

        // No-op update: if the incoming object is identical to what's stored
        // (ignoring the server-managed resourceVersion), don't bump the
        // revision or emit a watch event — matching apiserver semantics.
        // Without this, a controller that re-Updates an unchanged object
        // (e.g. cert-manager's cainjector rewriting the same caBundle) gets a
        // MODIFIED event back, re-reconciles, and spins in a tight loop.
        {
            let strip_rv = |v: &serde_json::Value| -> serde_json::Value {
                let mut v = v.clone();
                if let Some(m) = v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                    m.remove("resourceVersion");
                }
                v
            };
            if strip_rv(object) == strip_rv(&existing) {
                return Ok(existing);
            }
        }

        let rev = self.revision.next();

        let mut obj = object.clone();
        let metadata = obj
            .get_mut("metadata")
            .and_then(|v| v.as_object_mut())
            .ok_or_else(|| anyhow::anyhow!("metadata must be an object"))?;

        metadata.insert(
            "resourceVersion".to_string(),
            serde_json::json!(rev.to_string()),
        );

        {
            let mut table = w_transaction.open_table(RESOURCES)?;
            table.insert(key.as_str(), serde_json::to_vec(&obj)?.as_slice())?;
        }

        {
            let mut table = w_transaction.open_table(REVISIONS)?;
            let rev_entry = serde_json::json!({
                "type": "MODIFIED",
                "key": key,
                "object": obj,
            });
            table.insert(rev, serde_json::to_vec(&rev_entry)?.as_slice())?;
            prune_revision(&mut table, rev)?;
        }

        w_transaction.commit()?;

        let meta: ObjectMeta =
            serde_json::from_value(obj.get("metadata").cloned().unwrap_or_default())
                .unwrap_or_default();
        let labels = meta.labels.unwrap_or_default();
        self.index.update(&resource.gvr.key_prefix(), &key, &labels);

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
        let _view_guard = self.view_lock.write().expect("view_lock poisoned");
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
            prune_revision(&mut table, rev)?;
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

        let _view_guard = self.view_lock.write().expect("view_lock poisoned");
        let resource_version = self.revision.current();

        let r_transaction = self.db.begin_read()?;

        let table = r_transaction.open_table(RESOURCES)?;

        let mut items = Vec::new();
        let mut last_key: Option<String> = None;

        let candidates = label_selector.and_then(|s| self.index.matches(&gvr.key_prefix(), s));
        drop(_view_guard);

        let range_start = match continue_token {
            Some(token) => String::from_utf8(
                BASE64_STANDARD
                    .decode(token)
                    .map_err(|e| anyhow::anyhow!("invalid continue token: {e}"))?,
            )
            .map_err(|e| anyhow::anyhow!("invalid continue token: {e}"))?,
            None => scan_prefix.clone(),
        };

        let mut skipped_first = continue_token.is_none();

        for entry in table.range(range_start.as_str()..)? {
            let (key, value) = entry?;
            if !key.value().starts_with(scan_prefix.as_str()) {
                break;
            }

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
                && !fs.matches(&item, &gvr.key_prefix())
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

    pub fn stats(&self) -> anyhow::Result<(u64, u64, u64)> {
        let current_rev = self.revision.current();
        let r_transaction = self.db.begin_read()?;
        let rev_table = r_transaction.open_table(REVISIONS)?;
        let rev_count = rev_table.len()?;
        let res_table = r_transaction.open_table(RESOURCES)?;
        let res_count = res_table.len()?;
        Ok((current_rev, rev_count, res_count))
    }

    pub fn get_as<T: serde::de::DeserializeOwned>(
        &self,
        resource: &ResourceRef,
    ) -> anyhow::Result<Option<T>> {
        match self.get(resource)? {
            Some(v) => Ok(Some(serde_json::from_value(v)?)),
            None => Ok(None),
        }
    }

    pub fn list_as<T: serde::de::DeserializeOwned>(
        &self,
        gvr: &GroupVersionResource,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<T>> {
        let result = self.list(gvr, namespace, None, None, None, None)?;
        Ok(result
            .items
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect())
    }

    pub fn watch(&self, gvr: &GroupVersionResource) -> broadcast::Receiver<WatchEvent> {
        self.watches.subscribe(&gvr.key_prefix())
    }

    /// Open a watch with history: returns every recently-buffered event with
    /// `resource_version > since_rv` followed by a live receiver. Pass 0 for
    /// `since_rv` to request no history.
    pub fn watch_from(
        &self,
        gvr: &GroupVersionResource,
        since_rv: u64,
    ) -> Result<(Vec<WatchEvent>, broadcast::Receiver<WatchEvent>), crate::watch::TooOld> {
        self.watches.subscribe_with_history(&gvr.key_prefix(), since_rv)
    }
}

/// Remove the revision entry that just fell out of the keep window. Called
/// inside the same write transaction that inserts `new_rev`, so history stays
/// bounded at `KEEP_REVISIONS` entries without a separate compaction pass.
fn prune_revision(
    table: &mut redb::Table<'_, u64, &[u8]>,
    new_rev: u64,
) -> anyhow::Result<()> {
    if let Some(stale) = new_rev.checked_sub(KEEP_REVISIONS) {
        if stale > 0 {
            table.remove(stale)?;
        }
    }
    Ok(())
}
