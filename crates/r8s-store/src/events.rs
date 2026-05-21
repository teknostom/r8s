//! Event recorder: writes Kubernetes-style `v1/Event` objects to the store
//! and aggregates repeat events (same target + reason + source) by bumping
//! `count` and `lastTimestamp` instead of inserting a new row.

use crate::Store;
use crate::backend::ResourceRef;
use r8s_types::GroupVersionResource;

#[derive(Debug, Clone)]
pub struct EventTarget {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub uid: String,
}

impl EventTarget {
    pub fn from_object(api_version: &str, kind: &str, object: &serde_json::Value) -> Option<Self> {
        let meta = object.get("metadata")?;
        let name = meta.get("name")?.as_str()?.to_string();
        let uid = meta.get("uid")?.as_str()?.to_string();
        let namespace = meta
            .get("namespace")
            .and_then(|v| v.as_str())
            .map(String::from);
        Some(Self {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace,
            name,
            uid,
        })
    }
}

pub const TYPE_NORMAL: &str = "Normal";
pub const TYPE_WARNING: &str = "Warning";

/// Record an Event against `target`. If an Event already exists for the same
/// (uid, reason, source.component) tuple in the same namespace, bump its
/// `count` and `lastTimestamp` and refresh the message; otherwise insert a new
/// Event. Failures are logged at warn level — event recording must never fail
/// the caller's reconcile.
pub fn record_event(
    store: &Store,
    target: &EventTarget,
    component: &str,
    event_type: &str,
    reason: &str,
    message: &str,
) {
    let gvr = GroupVersionResource::events();
    let ns = target.namespace.as_deref().unwrap_or("default");
    // Canonical Kubernetes timestamp format: RFC3339 with Z suffix and second
    // precision for metav1.Time fields. Some Go clients (kubectl describe via
    // strict decode) reject offset-form timestamps with nanosecond fractions.
    let now_time = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let now_micro = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);

    if let Ok(list) = store.list(&gvr, Some(ns), None, None, None, None) {
        for item in list.items {
            if item
                .get("involvedObject")
                .and_then(|o| o.get("uid"))
                .and_then(|v| v.as_str())
                == Some(&target.uid)
                && item.get("reason").and_then(|v| v.as_str()) == Some(reason)
                && item
                    .get("source")
                    .and_then(|o| o.get("component"))
                    .and_then(|v| v.as_str())
                    == Some(component)
            {
                let name = match item
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|v| v.as_str())
                {
                    Some(n) => n.to_string(),
                    None => break,
                };
                let count = item.get("count").and_then(|v| v.as_i64()).unwrap_or(1) + 1;
                let mut updated = item.clone();
                if let Some(o) = updated.as_object_mut() {
                    o.insert("count".into(), serde_json::json!(count));
                    o.insert("lastTimestamp".into(), serde_json::json!(now_time));
                    o.insert("message".into(), serde_json::json!(message));
                }
                let resource_ref = ResourceRef {
                    gvr: &gvr,
                    namespace: Some(ns),
                    name: &name,
                };
                if let Err(e) = store.update(&resource_ref, &updated) {
                    tracing::warn!("event: update failed: {e}");
                }
                return;
            }
        }
    }

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let event_name = format!("{}.{}", target.name, &suffix[..16]);
    let event = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {
            "name": event_name,
            "namespace": ns,
        },
        "involvedObject": {
            "apiVersion": target.api_version,
            "kind": target.kind,
            "namespace": target.namespace,
            "name": target.name,
            "uid": target.uid,
        },
        "reason": reason,
        "message": message,
        "type": event_type,
        "source": {"component": component},
        "firstTimestamp": now_time,
        "lastTimestamp": now_time,
        "eventTime": now_micro,
        "count": 1,
        "reportingComponent": component,
    });

    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: Some(ns),
        name: &event_name,
    };
    if let Err(e) = store.create(resource_ref, &event) {
        tracing::warn!("event: create failed: {e}");
    }
}
