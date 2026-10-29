//! Event recording for the in-process kubelet and scheduler, as client-go's
//! recorder does for upstream's: one `v1.Event` per (object, reason, message),
//! repeats bumping `count` and `lastTimestamp`; events expire after an hour.

use crate::{Store, backend::ResourceRef};
use parking_lot::Mutex;
use r8s_types::GroupVersionResource;
use rustc_hash::FxHashMap;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const NORMAL: &str = "Normal";
pub const WARNING: &str = "Warning";

/// How long an event stays in the store.
pub const TTL: Duration = Duration::from_secs(60 * 60);

/// The object an event is about.
#[derive(Debug, Clone)]
pub struct ObjectRef {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub uid: String,
    pub field_path: Option<String>,
}

impl ObjectRef {
    /// A stored object's reference, from its apiVersion/kind/metadata.
    pub fn of(object: &Value) -> Option<Self> {
        let meta = object.get("metadata")?;
        let str_of = |v: &Value, key: &str| v.get(key).and_then(|s| s.as_str()).map(String::from);
        Some(Self {
            api_version: str_of(object, "apiVersion").unwrap_or_else(|| "v1".into()),
            kind: str_of(object, "kind")?,
            namespace: str_of(meta, "namespace"),
            name: str_of(meta, "name")?,
            uid: str_of(meta, "uid").unwrap_or_default(),
            field_path: None,
        })
    }

    pub fn pod(namespace: Option<&str>, name: &str, uid: &str) -> Self {
        Self {
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: namespace.map(String::from),
            name: name.into(),
            uid: uid.into(),
            field_path: None,
        }
    }

    /// The same object, narrowed to one of its containers.
    pub fn container(mut self, name: &str) -> Self {
        self.field_path = Some(format!("spec.containers{{{name}}}"));
        self
    }
}

/// A recorded event, remembered so a repeat updates it instead of adding one.
struct Seen {
    name: String,
    count: i64,
    at: Instant,
}

/// (involved uid, reason, message).
type SeenKey = (String, String, String);

/// Records events on behalf of one component.
#[derive(Clone)]
pub struct Recorder {
    store: Store,
    component: String,
    host: String,
    seen: Arc<Mutex<FxHashMap<SeenKey, Seen>>>,
}

impl Recorder {
    pub fn new(store: Store, component: &str, host: &str) -> Self {
        Self {
            store,
            component: component.into(),
            host: host.into(),
            seen: Arc::default(),
        }
    }

    /// Record `reason`/`message` about `object`; `type_` is `NORMAL` or
    /// `WARNING`.
    pub fn event(&self, object: &ObjectRef, type_: &str, reason: &str, message: &str) {
        let namespace = object.namespace.clone().unwrap_or_else(|| "default".into());
        let key = (object.uid.clone(), reason.to_string(), message.to_string());
        let now = chrono::Utc::now();
        let gvr = GroupVersionResource::events();

        let mut seen = self.seen.lock();
        if seen.len() > 4096 {
            seen.retain(|_, s| s.at.elapsed() < TTL);
        }
        if let Some(prev) = seen.get_mut(&key) {
            let rref = ResourceRef {
                gvr: &gvr,
                namespace: Some(&namespace),
                name: &prev.name,
            };
            if let Ok(Some(mut existing)) = self.store.get(&rref) {
                prev.count += 1;
                prev.at = Instant::now();
                existing["count"] = json!(prev.count);
                existing["lastTimestamp"] = json!(now.to_rfc3339());
                if let Err(e) = self.store.update(&rref, &existing) {
                    tracing::debug!("event {}/{}: update failed: {e}", namespace, prev.name);
                }
                return;
            }
        }

        let name = format!(
            "{}.{:x}",
            object.name,
            now.timestamp_nanos_opt().unwrap_or(0)
        );
        let mut involved = json!({
            "apiVersion": object.api_version,
            "kind": object.kind,
            "name": object.name,
            "uid": object.uid,
        });
        if let Some(ns) = &object.namespace {
            involved["namespace"] = json!(ns);
        }
        if let Some(path) = &object.field_path {
            involved["fieldPath"] = json!(path);
        }
        let event = json!({
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": {"name": name, "namespace": namespace},
            "involvedObject": involved,
            "reason": reason,
            "message": message,
            "source": {"component": self.component, "host": self.host},
            "firstTimestamp": now.to_rfc3339(),
            "lastTimestamp": now.to_rfc3339(),
            "count": 1,
            "type": type_,
            "reportingComponent": self.component,
            "reportingInstance": self.host,
        });
        let rref = ResourceRef {
            gvr: &gvr,
            namespace: Some(&namespace),
            name: &name,
        };
        match self.store.create(rref, &event) {
            Ok(_) => {
                seen.insert(
                    key,
                    Seen {
                        name,
                        count: 1,
                        at: Instant::now(),
                    },
                );
            }
            Err(e) => tracing::debug!("event {namespace}/{name}: create failed: {e}"),
        }
    }
}

/// Deletes events older than `TTL`, checking every `interval`.
pub async fn expire(
    store: Store,
    interval: Duration,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => expire_once(&store),
        }
    }
}

fn expire_once(store: &Store) {
    let gvr = GroupVersionResource::events();
    let cutoff = chrono::Utc::now() - chrono::Duration::from_std(TTL).unwrap_or_default();
    let Ok(result) = store.list(&gvr, None, None, None, None, None) else {
        return;
    };
    for event in result.items {
        let meta = &event["metadata"];
        let stamp = ["lastTimestamp", "eventTime"]
            .iter()
            .find_map(|k| event.get(k).and_then(|v| v.as_str()))
            .or_else(|| meta.get("creationTimestamp").and_then(|v| v.as_str()));
        let expired = stamp
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .is_some_and(|t| t < cutoff);
        if !expired {
            continue;
        }
        let (Some(name), namespace) = (
            meta.get("name").and_then(|v| v.as_str()),
            meta.get("namespace").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        let rref = ResourceRef {
            gvr: &gvr,
            namespace,
            name,
        };
        let _ = store.delete(&rref);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("t.db")).unwrap();
        (dir, s)
    }

    fn events(store: &Store) -> Vec<Value> {
        store
            .list(
                &GroupVersionResource::events(),
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap()
            .items
    }

    #[test]
    fn repeats_bump_the_count() {
        let (_d, store) = store();
        let rec = Recorder::new(store.clone(), "kubelet", "node-1");
        let pod = ObjectRef::pod(Some("ns"), "web", "uid-1").container("main");
        rec.event(&pod, NORMAL, "Started", "Started container main");
        rec.event(&pod, NORMAL, "Started", "Started container main");
        rec.event(
            &pod,
            WARNING,
            "BackOff",
            "Back-off restarting failed container",
        );

        let all = events(&store);
        assert_eq!(all.len(), 2);
        let started = all.iter().find(|e| e["reason"] == "Started").unwrap();
        assert_eq!(started["count"], 2);
        assert_eq!(started["metadata"]["namespace"], "ns");
        assert_eq!(started["involvedObject"]["uid"], "uid-1");
        assert_eq!(
            started["involvedObject"]["fieldPath"],
            "spec.containers{main}"
        );
        assert_eq!(started["source"]["component"], "kubelet");
        assert!(
            started["metadata"]["name"]
                .as_str()
                .unwrap()
                .starts_with("web.")
        );
    }

    #[test]
    fn expiry_removes_old_events_only() {
        let (_d, store) = store();
        let rec = Recorder::new(store.clone(), "kubelet", "node-1");
        let pod = ObjectRef::pod(Some("ns"), "web", "uid-1");
        rec.event(&pod, NORMAL, "Started", "fresh");
        let old = chrono::Utc::now() - chrono::Duration::hours(2);
        store
            .create(
                ResourceRef {
                    gvr: &GroupVersionResource::events(),
                    namespace: Some("ns"),
                    name: "web.old",
                },
                &json!({"apiVersion":"v1","kind":"Event",
                        "metadata":{"name":"web.old","namespace":"ns"},
                        "lastTimestamp": old.to_rfc3339()}),
            )
            .unwrap();
        expire_once(&store);
        let all = events(&store);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0]["message"], "fresh");
    }
}
