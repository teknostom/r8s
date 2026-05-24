//! Foreground deletion reconciler. When a client deletes an object with
//! `propagationPolicy=Foreground`, the API server marks it
//! `metadata.deletionTimestamp + finalizers: [foregroundDeletion]` instead of
//! removing it. This reconciler then:
//!
//!   1. Walks every registered GVR.
//!   2. For each object that's marked-for-foreground-deletion, finds
//!      dependents (any object whose `ownerReferences` carries the UID).
//!   3. Deletes each dependent.
//!   4. When no dependents remain, strips the `foregroundDeletion`
//!      finalizer and finally removes the parent.
//!
//! The pass repeats on a short interval so cascades of any depth converge.
//! It's intentionally simple — full per-event wiring would be more efficient
//! but conformance only cares about correctness within ~30s, well within
//! reach of a 250ms reconcile.

use std::time::Duration;

use r8s_store::{Store, backend::ResourceRef};
use r8s_types::{GroupVersionResource, registry::ResourceRegistry};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

const FOREGROUND_FINALIZER: &str = "foregroundDeletion";

pub async fn run(
    store: Store,
    shutdown: CancellationToken,
    registry: ResourceRegistry,
) -> anyhow::Result<()> {
    tracing::info!("foreground-gc controller started");
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("foreground-gc controller shutting down");
                return Ok(());
            }
            _ = tick.tick() => {
                reconcile(&store, &registry);
            }
        }
    }
}

fn reconcile(store: &Store, registry: &ResourceRegistry) {
    let gvrs: Vec<GroupVersionResource> = registry
        .iter()
        .iter()
        .map(|rt| rt.gvr.clone())
        .collect();
    // Build a fresh UID set: every object that currently exists in any
    // registered GVR. Used by both passes — foreground (to know which other
    // owners are still alive) and orphan-by-absence (to find dependents
    // whose owner has vanished, e.g. a closed dependency cycle).
    let mut alive_uids: std::collections::HashSet<String> = Default::default();
    for gvr in &gvrs {
        if let Ok(r) = store.list(gvr, None, None, None, None, None) {
            for item in &r.items {
                if let Some(u) = item
                    .get("metadata")
                    .and_then(|m| m.get("uid"))
                    .and_then(|v| v.as_str())
                {
                    alive_uids.insert(u.to_string());
                }
            }
        }
    }

    // Pass 1: foreground deletions in progress.
    for gvr in &gvrs {
        let items = match store.list(gvr, None, None, None, None, None) {
            Ok(r) => r.items,
            Err(_) => continue,
        };
        for item in items {
            if !is_foreground_pending(&item) {
                continue;
            }
            reconcile_one(store, gvr, &gvrs, &item);
        }
    }

    // Pass 2: absent-owner GC. Walk every object; if `metadata.ownerReferences`
    // is non-empty and EVERY listed owner is missing, the object is no
    // longer owned by anything and should be reaped — this is the path
    // that breaks dependency cycles once the seed has been deleted.
    for gvr in &gvrs {
        let items = match store.list(gvr, None, None, None, None, None) {
            Ok(r) => r.items,
            Err(_) => continue,
        };
        for item in items {
            let refs: Vec<Value> = item
                .get("metadata")
                .and_then(|m| m.get("ownerReferences"))
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if refs.is_empty() {
                continue;
            }
            let any_alive = refs.iter().any(|r| {
                r.get("uid")
                    .and_then(|v| v.as_str())
                    .is_some_and(|u| alive_uids.contains(u))
            });
            if any_alive {
                continue;
            }
            let name = item
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ns = item
                .get("metadata")
                .and_then(|m| m.get("namespace"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let rref = ResourceRef {
                gvr,
                namespace: ns.as_deref(),
                name: &name,
            };
            let _ = store.delete(&rref);
        }
    }
}

fn is_foreground_pending(obj: &Value) -> bool {
    let meta = match obj.get("metadata") {
        Some(m) => m,
        None => return false,
    };
    if meta
        .get("deletionTimestamp")
        .map(|v| v.is_null())
        .unwrap_or(true)
    {
        return false;
    }
    meta.get("finalizers")
        .and_then(|v| v.as_array())
        .is_some_and(|arr| arr.iter().any(|v| v.as_str() == Some(FOREGROUND_FINALIZER)))
}

fn reconcile_one(
    store: &Store,
    parent_gvr: &GroupVersionResource,
    all_gvrs: &[GroupVersionResource],
    parent: &Value,
) {
    let uid = match parent
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|v| v.as_str())
    {
        Some(u) => u.to_string(),
        None => return,
    };
    let parent_name = parent
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let parent_ns = parent
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Build a set of UIDs of objects currently undergoing foreground
    // deletion. When a dependent has multiple owners, we only delete it if
    // EVERY owner is in this set (or missing entirely); otherwise we just
    // strip the foreground parent's ref so the surviving owner keeps it.
    let mut deleting_uids: std::collections::HashSet<String> = Default::default();
    deleting_uids.insert(uid.clone());
    for owner_gvr in all_gvrs {
        let items = match store.list(owner_gvr, None, None, None, None, None) {
            Ok(r) => r.items,
            Err(_) => continue,
        };
        for owner in items {
            if !is_foreground_pending(&owner) {
                continue;
            }
            if let Some(u) = owner
                .get("metadata")
                .and_then(|m| m.get("uid"))
                .and_then(|v| v.as_str())
            {
                deleting_uids.insert(u.to_string());
            }
        }
    }
    // For each potential dependent decide: delete (no other surviving
    // owner), or strip-only (at least one ref points to something not
    // being deleted). The latter is "blocking owner": the dependent
    // outlives this cascade.
    let mut still_has_dependents = false;
    for child_gvr in all_gvrs {
        let items = match store.list(child_gvr, None, None, None, None, None) {
            Ok(r) => r.items,
            Err(_) => continue,
        };
        for item in items {
            let refs: Vec<Value> = item
                .get("metadata")
                .and_then(|m| m.get("ownerReferences"))
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let mentions_parent = refs
                .iter()
                .any(|r| r.get("uid").and_then(|u| u.as_str()) == Some(uid.as_str()));
            if !mentions_parent {
                continue;
            }
            let has_surviving_owner = refs.iter().any(|r| {
                let u = r.get("uid").and_then(|x| x.as_str()).unwrap_or("");
                !u.is_empty() && u != uid && !deleting_uids.contains(u)
            });
            let name = item
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ns = item
                .get("metadata")
                .and_then(|m| m.get("namespace"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let rref = ResourceRef {
                gvr: child_gvr,
                namespace: ns.as_deref(),
                name: &name,
            };
            if has_surviving_owner {
                // Just strip our ref. Use a blind write (clear rv) so we
                // don't lose the strip to a racing controller update.
                let new_refs: Vec<Value> = refs
                    .into_iter()
                    .filter(|r| r.get("uid").and_then(|u| u.as_str()) != Some(uid.as_str()))
                    .collect();
                let mut updated = item.clone();
                if let Some(meta) = updated.get_mut("metadata").and_then(|v| v.as_object_mut()) {
                    if new_refs.is_empty() {
                        meta.remove("ownerReferences");
                    } else {
                        meta.insert(
                            "ownerReferences".to_string(),
                            Value::Array(new_refs),
                        );
                    }
                    meta.remove("resourceVersion");
                }
                let _ = store.update(&rref, &updated);
                // Stripping is bookkeeping, not unfinished cascade work, so
                // we don't flag still_has_dependents here.
            } else {
                still_has_dependents = true;
                let _ = store.delete(&rref);
            }
        }
    }

    if still_has_dependents {
        return;
    }

    // No dependents left — strip the foreground finalizer and let the
    // object's natural deletion proceed. Some callers ALSO need the actual
    // store.delete to happen (otherwise the object lingers with a
    // deletionTimestamp but no finalizers); upstream issues that delete from
    // the GC, mirror that.
    let mut updated = parent.clone();
    let cleared = if let Some(meta) = updated.get_mut("metadata").and_then(|v| v.as_object_mut()) {
        let mut changed = false;
        if let Some(arr) = meta.get_mut("finalizers").and_then(|v| v.as_array_mut()) {
            let before = arr.len();
            arr.retain(|v| v.as_str() != Some(FOREGROUND_FINALIZER));
            if arr.len() != before {
                changed = true;
            }
            if arr.is_empty() {
                meta.remove("finalizers");
            }
        }
        changed
    } else {
        false
    };
    let rref = ResourceRef {
        gvr: parent_gvr,
        namespace: parent_ns.as_deref(),
        name: &parent_name,
    };
    if cleared {
        let _ = store.update(&rref, &updated);
    }
    // Now actually delete.
    let _ = store.delete(&rref);
}
