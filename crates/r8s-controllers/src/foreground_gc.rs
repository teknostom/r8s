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
    // Absent-owner GC (pass 2 of `reconcile`) only reaps an object once it has
    // been seen orphaned in two consecutive passes. During an install storm a
    // child (RS/pod) can be listed a beat before its just-created owner
    // (Deployment/RS) lands in our `alive_uids` snapshot — which would
    // otherwise make us reap a perfectly healthy object as a false orphan,
    // cascading into deleted pods and wiped volumes. Requiring two consecutive
    // sightings (~250ms apart) lets that transient creation race settle first.
    let mut prev_orphans: std::collections::HashSet<String> = Default::default();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("foreground-gc controller shutting down");
                return Ok(());
            }
            _ = tick.tick() => {
                prev_orphans = reconcile(&store, &registry, &prev_orphans);
            }
        }
    }
}

fn reconcile(
    store: &Store,
    registry: &ResourceRegistry,
    prev_orphans: &std::collections::HashSet<String>,
) -> std::collections::HashSet<String> {
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
    //
    // We DON'T reap on first sight: `alive_uids` above is a non-atomic snapshot
    // stitched from one `list` per GVR, so a freshly-created owner can be
    // absent from it while its child is already present (the child is always
    // created after its owner). Reaping then would delete a healthy object.
    // Instead we collect this pass's candidates and only delete the ones that
    // were ALSO candidates last pass — a real orphan stays absent-owner across
    // passes, a creation-race artifact resolves within one tick.
    let mut orphans: std::collections::HashSet<String> = Default::default();
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
            // No UID means we can't debounce safely across passes — skip rather
            // than risk reaping on a single racy sighting.
            let uid = match item
                .get("metadata")
                .and_then(|m| m.get("uid"))
                .and_then(|v| v.as_str())
            {
                Some(u) => u.to_string(),
                None => continue,
            };
            if !prev_orphans.contains(&uid) {
                // First sighting — remember it so a still-orphaned object is
                // reaped next pass, but don't delete yet.
                orphans.insert(uid);
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
            if matches!(store.delete(&rref), Ok(Some(_))) {
                tracing::info!(
                    "foreground-gc: reaped orphan {}/{} (all {} owner(s) absent for 2 passes)",
                    gvr.resource,
                    name,
                    refs.len(),
                );
            }
        }
    }
    orphans
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

#[cfg(test)]
mod tests {
    use super::reconcile;
    use r8s_store::{Store, backend::ResourceRef};
    use r8s_types::{GroupVersionResource, registry::ResourceRegistry};
    use std::collections::HashSet;
    use tempfile::TempDir;

    fn rs_with_owner(store: &Store, name: &str, owner_uid: &str) -> String {
        let gvr = GroupVersionResource::replica_sets();
        let rref = ResourceRef {
            gvr: &gvr,
            namespace: Some("argocd"),
            name,
        };
        let obj = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": name,
                "namespace": "argocd",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "argocd-redis",
                    "uid": owner_uid,
                    "controller": true,
                }],
            },
            "spec": { "replicas": 1 },
        });
        let created = store.create(rref, &obj).unwrap();
        created["metadata"]["uid"].as_str().unwrap().to_string()
    }

    fn exists(store: &Store, name: &str) -> bool {
        let gvr = GroupVersionResource::replica_sets();
        store
            .get(&ResourceRef { gvr: &gvr, namespace: Some("argocd"), name })
            .unwrap()
            .is_some()
    }

    // The regression: during an install storm the owner Deployment can be
    // missing from the `alive_uids` snapshot while its RS is already present.
    // The RS must survive the first pass (it's a race artifact, not a real
    // orphan) and only be reaped if it's STILL orphaned the next pass.
    #[test]
    fn absent_owner_child_is_not_reaped_on_first_sighting() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let registry = ResourceRegistry::default_mvp();

        let rs_uid = rs_with_owner(&store, "argocd-redis-abc", "owner-not-yet-visible");

        // First pass: flagged but not deleted.
        let orphans = reconcile(&store, &registry, &HashSet::new());
        assert!(exists(&store, "argocd-redis-abc"), "must survive first pass");
        assert!(orphans.contains(&rs_uid), "should be flagged as a candidate");

        // Second pass, still orphaned: now it's reaped.
        let orphans2 = reconcile(&store, &registry, &orphans);
        assert!(!exists(&store, "argocd-redis-abc"), "confirmed orphan is reaped");
        assert!(!orphans2.contains(&rs_uid), "deleted object drops out of the set");
    }

    // If the owner becomes visible before the second pass, the child is never
    // reaped — exactly what saves a healthy RS whose Deployment landed a beat
    // late in the snapshot.
    #[test]
    fn child_survives_when_owner_appears_next_pass() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let registry = ResourceRegistry::default_mvp();

        // Create the owner Deployment first so it's in `alive_uids`.
        let dep_gvr = GroupVersionResource::deployments();
        let dep = store
            .create(
                ResourceRef { gvr: &dep_gvr, namespace: Some("argocd"), name: "argocd-redis" },
                &serde_json::json!({
                    "apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": { "name": "argocd-redis", "namespace": "argocd" },
                    "spec": {},
                }),
            )
            .unwrap();
        let dep_uid = dep["metadata"]["uid"].as_str().unwrap().to_string();

        rs_with_owner(&store, "argocd-redis-def", &dep_uid);

        let orphans = reconcile(&store, &registry, &HashSet::new());
        let orphans2 = reconcile(&store, &registry, &orphans);
        assert!(exists(&store, "argocd-redis-def"), "owned RS is never reaped");
        assert!(orphans.is_empty() && orphans2.is_empty());
    }
}
