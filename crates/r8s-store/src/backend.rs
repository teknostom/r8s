use std::{ops::Bound, path::Path, sync::Arc};

use base64::prelude::*;
use parking_lot::RwLock;
use r8s_types::GroupVersionResource;
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use crate::{
    error::{Result, StoreError},
    index::{FieldSelector, LabelSelector},
    revision::RevisionCounter,
    watch::{WatchEvent, WatchEventType, WatchHub},
};

pub(crate) const RESOURCES: TableDefinition<&str, &[u8]> = TableDefinition::new("resources");
pub(crate) const REVISIONS: TableDefinition<u64, &[u8]> = TableDefinition::new("revisions");

/// Number of trailing revisions retained for watch replay. A watch resuming
/// from a `resourceVersion` older than this window is answered with
/// [`StoreError::TooOld`].
const KEEP_REVISIONS: u64 = 1000;

/// How long a paginated list's `continue` token stays consistent. Mirrors the
/// etcd compaction interval the apiserver is bounded by; after this a resumed
/// list is answered with [`StoreError::ContinueExpired`].
const CONTINUE_TOKEN_TTL_MS: i64 = 5 * 60 * 1000;

/// `metav1.ListMeta.continue` as the apiserver encodes it: base64url (no
/// padding) of `{"v":"meta.k8s.io/v1","rv":<rv>,"start":"<key>"}`. `rv > 0`
/// is the revision the first page was served at; `-1` resumes inconsistently
/// at the latest revision; `0` is invalid.
#[derive(Serialize, serde::Deserialize)]
struct ContinueToken {
    v: String,
    rv: i64,
    start: String,
}

const CONTINUE_TOKEN_VERSION: &str = "meta.k8s.io/v1";

#[derive(Clone)]
pub struct Store {
    db: Arc<Database>,
    revision: RevisionCounter,
    watches: WatchHub,
    /// Serializes writes against the snapshot taken by reads, so that a list's
    /// `resourceVersion` and the events delivered to a concurrently-opened
    /// watch never overlap or skip.
    view_lock: Arc<RwLock<()>>,
}

pub struct ListResult {
    pub items: Vec<serde_json::Value>,
    pub resource_version: u64,
    pub continue_token: Option<String>,
    /// Items left after this page; `None` on the last page or when a selector
    /// makes the count unknowable without scanning.
    pub remaining_item_count: Option<u64>,
}

pub struct StoreStats {
    pub current_revision: u64,
    pub revision_entries: u64,
    pub resource_count: u64,
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

/// One entry in the durable revision log, as written to the `REVISIONS` table.
#[derive(Serialize)]
struct RevisionEntryRef<'a> {
    #[serde(rename = "type")]
    event_type: WatchEventType,
    key: &'a str,
    object: &'a serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev: Option<&'a serde_json::Value>,
    /// Unix milliseconds the revision was committed; bounds continue tokens.
    ts: i64,
}

#[derive(serde::Deserialize)]
struct RevisionEntry {
    #[serde(rename = "type")]
    event_type: WatchEventType,
    key: String,
    object: serde_json::Value,
    #[serde(default)]
    prev: Option<serde_json::Value>,
    #[serde(default)]
    ts: i64,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path)?;
        {
            let txn = db.begin_write()?;
            txn.open_table(RESOURCES)?;
            txn.open_table(REVISIONS)?;
            txn.commit()?;
        }

        let max_revision = {
            let txn = db.begin_read()?;
            let table = txn.open_table(REVISIONS)?;
            table.last()?.map(|(k, _)| k.value()).unwrap_or(0)
        };

        Ok(Self {
            db: Arc::new(db),
            revision: RevisionCounter::new(max_revision),
            watches: WatchHub::default(),
            view_lock: Arc::new(RwLock::new(())),
        })
    }

    pub fn create(
        &self,
        resource: ResourceRef,
        object: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let key = resource.key();
        let gvr_prefix = resource.gvr.key_prefix();

        let mut obj = object.clone();
        let metadata = obj
            .get_mut("metadata")
            .and_then(|v| v.as_object_mut())
            .ok_or(StoreError::InvalidMetadata)?;
        if metadata.get("uid").and_then(|v| v.as_str()).is_none() {
            metadata.insert("uid".into(), uuid::Uuid::new_v4().to_string().into());
        }
        if metadata
            .get("creationTimestamp")
            .and_then(|v| v.as_str())
            .is_none()
        {
            metadata.insert(
                "creationTimestamp".into(),
                chrono::Utc::now().to_rfc3339().into(),
            );
        }

        let _guard = self.view_lock.write();
        let txn = self.db.begin_write()?;

        let rev = {
            let mut table = txn.open_table(RESOURCES)?;
            if table.get(key.as_str())?.is_some() {
                return Err(StoreError::AlreadyExists {
                    gvr: gvr_prefix,
                    name: resource.name.to_string(),
                });
            }
            let rev = self.revision.next();
            set_resource_version(&mut obj, rev);
            table.insert(key.as_str(), serde_json::to_vec(&obj)?.as_slice())?;
            rev
        };

        self.record(
            txn,
            &gvr_prefix,
            &key,
            rev,
            WatchEventType::Added,
            obj,
            None,
        )
    }

    pub fn get(&self, resource: &ResourceRef) -> Result<Option<serde_json::Value>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(RESOURCES)?;
        match table.get(resource.key().as_str())? {
            Some(guard) => Ok(Some(serde_json::from_slice(guard.value())?)),
            None => Ok(None),
        }
    }

    pub fn update(
        &self,
        resource: &ResourceRef,
        object: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let key = resource.key();
        let gvr_prefix = resource.gvr.key_prefix();

        let _guard = self.view_lock.write();
        let txn = self.db.begin_write()?;

        let existing: serde_json::Value = {
            let table = txn.open_table(RESOURCES)?;
            match table.get(key.as_str())? {
                Some(guard) => serde_json::from_slice(guard.value())?,
                None => {
                    return Err(StoreError::NotFound {
                        gvr: gvr_prefix,
                        name: resource.name.to_string(),
                    });
                }
            }
        };

        let stored_rv = resource_version(&existing);
        let incoming_rv = resource_version(object);
        if !incoming_rv.is_empty() && incoming_rv != stored_rv {
            return Err(StoreError::Conflict {
                gvr: gvr_prefix,
                name: resource.name.to_string(),
                message: format!(
                    "resourceVersion mismatch: stored={stored_rv}, incoming={incoming_rv}"
                ),
            });
        }

        if equal_ignoring_revision(object, &existing) {
            return Ok(existing);
        }

        let rev = self.revision.next();
        let mut obj = object.clone();
        set_resource_version(&mut obj, rev);
        {
            let mut table = txn.open_table(RESOURCES)?;
            table.insert(key.as_str(), serde_json::to_vec(&obj)?.as_slice())?;
        }

        self.record(
            txn,
            &gvr_prefix,
            &key,
            rev,
            WatchEventType::Modified,
            obj,
            Some(existing),
        )
    }

    pub fn delete(&self, resource: &ResourceRef) -> Result<Option<serde_json::Value>> {
        let key = resource.key();
        let gvr_prefix = resource.gvr.key_prefix();

        let _guard = self.view_lock.write();
        let txn = self.db.begin_write()?;

        let object: serde_json::Value = {
            let mut table = txn.open_table(RESOURCES)?;
            match table.remove(key.as_str())? {
                Some(guard) => serde_json::from_slice(guard.value())?,
                None => return Ok(None),
            }
        };

        let rev = self.revision.next();
        // The DELETED event carries the deletion revision, as upstream does.
        let mut object = object;
        set_resource_version(&mut object, rev);
        let object = self.record(
            txn,
            &gvr_prefix,
            &key,
            rev,
            WatchEventType::Deleted,
            object,
            None,
        )?;
        Ok(Some(object))
    }

    /// Commits the revision-log entry for an already-staged resource mutation,
    /// then publishes the event to live watchers. Runs under the write guard
    /// held by the calling mutation.
    #[allow(clippy::too_many_arguments)]
    fn record(
        &self,
        txn: redb::WriteTransaction,
        gvr_prefix: &str,
        key: &str,
        rev: u64,
        event_type: WatchEventType,
        object: serde_json::Value,
        prev: Option<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        {
            let mut table = txn.open_table(REVISIONS)?;
            let entry = serde_json::to_vec(&RevisionEntryRef {
                event_type,
                key,
                object: &object,
                prev: prev.as_ref(),
                ts: chrono::Utc::now().timestamp_millis(),
            })?;
            table.insert(rev, entry.as_slice())?;
            prune_revisions(&mut table, rev)?;
        }
        txn.commit()?;

        let object = Arc::new(object);
        self.watches.notify(
            gvr_prefix,
            WatchEvent {
                event_type,
                resource_version: rev,
                object: object.clone(),
                prev_object: prev.map(Arc::new),
            },
        );
        Ok(Arc::unwrap_or_clone(object))
    }

    pub fn list(
        &self,
        gvr: &GroupVersionResource,
        namespace: Option<&str>,
        label_selector: Option<&LabelSelector>,
        field_selector: Option<&FieldSelector>,
        limit: Option<usize>,
        continue_token: Option<&str>,
    ) -> Result<ListResult> {
        let prefix = scan_prefix(gvr, namespace);
        // A resumed page reports the revision its first page was served at,
        // so a client sees one resourceVersion across the whole list. The
        // store keeps no point-in-time snapshots, so the page content is the
        // current state; once the token outlives the consistency window the
        // client is told to resume inconsistently (rv = -1) instead.
        let (resume_after, reported_rv) = match continue_token {
            None => (None, None),
            Some(token) => {
                let (rv, start) = decode_continue(token)?;
                let resume = format!("{prefix}{start}");
                if rv > 0 {
                    if self.revision_expired(rv as u64)? {
                        return Err(StoreError::ContinueExpired {
                            inconsistent_token: encode_continue(-1, &start),
                        });
                    }
                    (Some(resume), Some(rv as u64))
                } else {
                    (Some(resume), None)
                }
            }
        };
        let (txn, current_rv) = self.snapshot()?;
        let scan = list_scan(
            &txn,
            &prefix,
            &gvr.key_prefix(),
            label_selector,
            field_selector,
            limit.filter(|l| *l > 0),
            resume_after.as_deref(),
        )?;
        let resource_version = reported_rv.unwrap_or(current_rv);
        let continue_token = match (&scan.last_key, scan.has_more) {
            (Some(key), true) => Some(encode_continue(
                resource_version as i64,
                &key[prefix.len()..],
            )),
            _ => None,
        };
        Ok(ListResult {
            items: scan.items,
            resource_version,
            continue_token,
            remaining_item_count: scan.remaining,
        })
    }

    /// True once `rv` has been pruned from the revision log or is older than
    /// [`CONTINUE_TOKEN_TTL_MS`]; a revision not yet written counts as live.
    fn revision_expired(&self, rv: u64) -> Result<bool> {
        if rv > self.revision.current() {
            return Ok(false);
        }
        let txn = self.db.begin_read()?;
        let table = txn.open_table(REVISIONS)?;
        let Some(bytes) = table.get(rv)? else {
            return Ok(true);
        };
        let entry: RevisionEntry = serde_json::from_slice(bytes.value())?;
        let age = chrono::Utc::now().timestamp_millis() - entry.ts;
        Ok(entry.ts == 0 || age > CONTINUE_TOKEN_TTL_MS)
    }

    pub fn stats(&self) -> Result<StoreStats> {
        let current_revision = self.revision.current();
        let txn = self.db.begin_read()?;
        let revision_entries = txn.open_table(REVISIONS)?.len()?;
        let resource_count = txn.open_table(RESOURCES)?.len()?;
        Ok(StoreStats {
            current_revision,
            revision_entries,
            resource_count,
        })
    }

    pub fn get_as<T: DeserializeOwned>(&self, resource: &ResourceRef) -> Result<Option<T>> {
        match self.get(resource)? {
            Some(v) => Ok(Some(serde_json::from_value(v)?)),
            None => Ok(None),
        }
    }

    pub fn list_as<T: DeserializeOwned>(
        &self,
        gvr: &GroupVersionResource,
        namespace: Option<&str>,
    ) -> Result<Vec<T>> {
        let result = self.list(gvr, namespace, None, None, None, None)?;
        result
            .items
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(StoreError::from))
            .collect()
    }

    /// Subscribe to live events for `gvr`, starting now.
    pub fn watch(&self, gvr: &GroupVersionResource) -> broadcast::Receiver<WatchEvent> {
        self.watches.subscribe(&gvr.key_prefix())
    }

    /// Atomically snapshot the current state of `gvr` and subscribe to its live
    /// stream. The receiver delivers exactly the events following the returned
    /// snapshot, with no overlap or gap.
    pub fn list_and_watch(
        &self,
        gvr: &GroupVersionResource,
        namespace: Option<&str>,
        label_selector: Option<&LabelSelector>,
        field_selector: Option<&FieldSelector>,
    ) -> Result<(ListResult, broadcast::Receiver<WatchEvent>)> {
        let (txn, resource_version, receiver) = {
            let _guard = self.view_lock.read();
            let receiver = self.watches.subscribe(&gvr.key_prefix());
            let resource_version = self.revision.current();
            let txn = self.db.begin_read()?;
            (txn, resource_version, receiver)
        };
        let scan = list_scan(
            &txn,
            &scan_prefix(gvr, namespace),
            &gvr.key_prefix(),
            label_selector,
            field_selector,
            None,
            None,
        )?;
        Ok((
            ListResult {
                items: scan.items,
                resource_version,
                continue_token: None,
                remaining_item_count: None,
            },
            receiver,
        ))
    }

    /// Replay the events for `gvr` with `resourceVersion` greater than
    /// `since_rv`, then subscribe to the live stream continuing from there.
    /// Returns [`StoreError::TooOld`] when `since_rv` predates the retained
    /// revision window.
    pub fn watch_from(
        &self,
        gvr: &GroupVersionResource,
        since_rv: u64,
    ) -> Result<(Vec<WatchEvent>, broadcast::Receiver<WatchEvent>)> {
        let gvr_prefix = gvr.key_prefix();

        let (txn, current, receiver) = {
            let _guard = self.view_lock.read();
            let receiver = self.watches.subscribe(&gvr_prefix);
            let current = self.revision.current();
            let txn = self.db.begin_read()?;
            (txn, current, receiver)
        };

        if since_rv >= current {
            return Ok((Vec::new(), receiver));
        }

        let watermark = current.saturating_sub(KEEP_REVISIONS);
        if since_rv > 0 && since_rv < watermark {
            return Err(StoreError::TooOld {
                requested: since_rv,
                oldest_available: watermark + 1,
            });
        }

        let events = replay_since(&txn, &gvr_prefix, since_rv)?;
        Ok((events, receiver))
    }

    /// Acquire a consistent read snapshot together with the matching
    /// `resourceVersion`. The snapshot is pinned before the guard is released,
    /// so the long scan that follows runs lock-free.
    fn snapshot(&self) -> Result<(redb::ReadTransaction, u64)> {
        let _guard = self.view_lock.read();
        let resource_version = self.revision.current();
        let txn = self.db.begin_read()?;
        Ok((txn, resource_version))
    }
}

/// Key prefix every stored object of `gvr` (in `namespace`, when given) shares.
fn scan_prefix(gvr: &GroupVersionResource, namespace: Option<&str>) -> String {
    let gvr_prefix = gvr.key_prefix();
    match namespace {
        Some(ns) => format!("{gvr_prefix}/{ns}/"),
        None => format!("{gvr_prefix}/"),
    }
}

struct Scan {
    items: Vec<serde_json::Value>,
    /// Key of the last item returned, the resume point for the next page.
    last_key: Option<String>,
    /// More keys follow under the prefix after a page filled to `limit`.
    has_more: bool,
    /// Keys left after the page, counted only when no selector filters; a
    /// selector would make the count a lie, so upstream omits it too.
    remaining: Option<u64>,
}

fn list_scan(
    txn: &redb::ReadTransaction,
    scan_prefix: &str,
    gvr_prefix: &str,
    label_selector: Option<&LabelSelector>,
    field_selector: Option<&FieldSelector>,
    limit: Option<usize>,
    resume_after: Option<&str>,
) -> Result<Scan> {
    // Resume strictly after the last returned key: if it still exists it was
    // already returned, and if it was deleted between pages the next key is
    // correctly included rather than skipped.
    let lower = match resume_after {
        Some(key) => Bound::Excluded(key),
        None => Bound::Included(scan_prefix),
    };

    let table = txn.open_table(RESOURCES)?;
    let mut scan = Scan {
        items: Vec::new(),
        last_key: None,
        has_more: false,
        remaining: None,
    };
    let mut iter = table.range::<&str>((lower, Bound::Unbounded))?;
    while let Some(entry) = iter.next() {
        let (key, value) = entry?;
        let key = key.value();
        if !key.starts_with(scan_prefix) {
            break;
        }
        if let Some(limit) = limit
            && scan.items.len() >= limit
        {
            scan.has_more = true;
            if label_selector.is_none() && field_selector.is_none() {
                let mut rest = 1u64;
                for entry in iter.by_ref() {
                    let (key, _) = entry?;
                    if !key.value().starts_with(scan_prefix) {
                        break;
                    }
                    rest += 1;
                }
                scan.remaining = Some(rest);
            }
            break;
        }

        let item: serde_json::Value = serde_json::from_slice(value.value())?;
        if let Some(selector) = label_selector
            && !selector.matches(&item)
        {
            continue;
        }
        if let Some(selector) = field_selector
            && !selector.matches(&item, gvr_prefix)
        {
            continue;
        }

        scan.last_key = Some(key.to_string());
        scan.items.push(item);
    }
    Ok(scan)
}

fn replay_since(
    txn: &redb::ReadTransaction,
    gvr_prefix: &str,
    since_rv: u64,
) -> Result<Vec<WatchEvent>> {
    let key_prefix = format!("{gvr_prefix}/");
    let table = txn.open_table(REVISIONS)?;
    let mut events = Vec::new();
    for entry in table.range(since_rv.saturating_add(1)..)? {
        let (rev, bytes) = entry?;
        let stored: RevisionEntry = serde_json::from_slice(bytes.value())?;
        if !stored.key.starts_with(&key_prefix) {
            continue;
        }
        events.push(WatchEvent {
            event_type: stored.event_type,
            resource_version: rev.value(),
            object: Arc::new(stored.object),
            prev_object: stored.prev.map(Arc::new),
        });
    }
    Ok(events)
}

fn prune_revisions(table: &mut redb::Table<'_, u64, &[u8]>, current_rev: u64) -> Result<()> {
    let watermark = current_rev.saturating_sub(KEEP_REVISIONS);
    if watermark == 0 {
        return Ok(());
    }
    let stale: Vec<u64> = table
        .range(..=watermark)?
        .map(|entry| entry.map(|(key, _)| key.value()))
        .collect::<std::result::Result<_, redb::StorageError>>()?;
    for key in stale {
        table.remove(key)?;
    }
    Ok(())
}

fn set_resource_version(obj: &mut serde_json::Value, rev: u64) {
    if let Some(metadata) = obj.get_mut("metadata").and_then(|v| v.as_object_mut()) {
        metadata.insert("resourceVersion".into(), rev.to_string().into());
    }
}

fn resource_version(obj: &serde_json::Value) -> &str {
    obj.get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
}

/// Whether two objects are equal once `metadata.resourceVersion` is disregarded
/// on both sides. Compares structurally without cloning, since this runs on
/// every update to detect no-op writes.
fn equal_ignoring_revision(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    let (serde_json::Value::Object(a), serde_json::Value::Object(b)) = (a, b) else {
        return a == b;
    };
    a.len() == b.len()
        && a.iter().all(|(key, value)| match b.get(key) {
            Some(other) if key == "metadata" => equal_metadata_ignoring_revision(value, other),
            Some(other) => value == other,
            None => false,
        })
}

fn equal_metadata_ignoring_revision(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    let (serde_json::Value::Object(a), serde_json::Value::Object(b)) = (a, b) else {
        return a == b;
    };
    let significant = |m: &serde_json::Map<String, serde_json::Value>| {
        m.len() - usize::from(m.contains_key("resourceVersion"))
    };
    significant(a) == significant(b)
        && a.iter()
            .filter(|(key, _)| key.as_str() != "resourceVersion")
            .all(|(key, value)| b.get(key) == Some(value))
}

fn encode_continue(rv: i64, start: &str) -> String {
    let token = ContinueToken {
        v: CONTINUE_TOKEN_VERSION.to_string(),
        rv,
        start: start.to_string(),
    };
    BASE64_URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token).unwrap_or_default())
}

/// `(rv, start)` of a client-supplied token. Rejects foreign versions, an rv
/// of 0 and an empty start key, as the apiserver does.
fn decode_continue(token: &str) -> Result<(i64, String)> {
    let bytes = BASE64_URL_SAFE_NO_PAD
        .decode(token)
        .or_else(|_| BASE64_STANDARD.decode(token))
        .map_err(|_| StoreError::InvalidContinueToken)?;
    let parsed: ContinueToken =
        serde_json::from_slice(&bytes).map_err(|_| StoreError::InvalidContinueToken)?;
    if parsed.v != CONTINUE_TOKEN_VERSION || parsed.rv == 0 || parsed.start.is_empty() {
        return Err(StoreError::InvalidContinueToken);
    }
    Ok((parsed.rv, parsed.start))
}
