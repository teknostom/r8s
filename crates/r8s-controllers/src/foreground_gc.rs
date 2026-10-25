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

use rustc_hash::FxHashSet;
use std::time::Duration;

use r8s_store::{Store, backend::ResourceRef};
use r8s_types::{FOREGROUND_FINALIZER, GroupVersionResource, registry::ResourceRegistry};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

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
    let mut prev_orphans: FxHashSet<String> = Default::default();
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
    prev_orphans: &FxHashSet<String>,
) -> FxHashSet<String> {
    let gvrs: Vec<GroupVersionResource> = registry.iter().iter().map(|rt| rt.gvr.clone()).collect();
    // Build a fresh UID set: every object that currently exists in any
    // registered GVR. Used by both passes — foreground (to know which other
    // owners are still alive) and orphan-by-absence (to find dependents
    // whose owner has vanished, e.g. a closed dependency cycle).
    let mut alive_uids: FxHashSet<String> = Default::default();
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

    // Pass 1: objects mid-deletion. A foreground-pending one drives its cascade.
    // One whose finalizers have all cleared is released and gets reaped here —
    // a crash between `reconcile_one`'s finalizer strip and its delete, or a
    // non-foreground finalizer that its own owner removed directly, would
    // otherwise strand it with a `deletionTimestamp` and nothing left to block
    // it (a real API server deletes such an object the instant its last
    // finalizer clears; our low-level store does not).
    for gvr in &gvrs {
        let items = match store.list(gvr, None, None, None, None, None) {
            Ok(r) => r.items,
            Err(_) => continue,
        };
        for item in items {
            if is_foreground_pending(&item) {
                reconcile_one(store, gvr, &gvrs, &item);
            } else if is_deletion_finalized(&item) {
                finalize_released(store, gvr, &item);
            }
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
    let mut orphans: FxHashSet<String> = Default::default();
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

/// An object marked for deletion with no finalizer left to hold it. Nothing is
/// blocking its removal, so it should already be gone; the store still has it,
/// so reap it.
fn is_deletion_finalized(obj: &Value) -> bool {
    let meta = match obj.get("metadata") {
        Some(m) => m,
        None => return false,
    };
    let marked = meta
        .get("deletionTimestamp")
        .is_some_and(|v| !v.is_null());
    if !marked {
        return false;
    }
    let has_finalizers = meta
        .get("finalizers")
        .and_then(|v| v.as_array())
        .is_some_and(|arr| !arr.is_empty());
    !has_finalizers
}

fn finalize_released(store: &Store, gvr: &GroupVersionResource, obj: &Value) {
    let name = obj
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if name.is_empty() {
        return;
    }
    let ns = obj
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
            "foreground-gc: finalized {}/{} (marked for deletion, no finalizers left)",
            gvr.resource,
            name,
        );
    }
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
    let mut deleting_uids: FxHashSet<String> = Default::default();
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
                        meta.insert("ownerReferences".to_string(), Value::Array(new_refs));
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

    let rref = ResourceRef {
        gvr: parent_gvr,
        namespace: parent_ns.as_deref(),
        name: &parent_name,
    };

    // Every dependent is gone. Drop our foreground finalizer. If it was the last
    // finalizer the object is fully released, so delete it in a SINGLE write —
    // no strip-then-delete window where a crash strands it with a
    // deletionTimestamp and no finalizer (see `is_deletion_finalized`). If other
    // finalizers remain, only persist the strip and leave the object for their
    // owners to release; the released-object sweep reaps it once they clear.
    let remaining: Vec<Value> = parent
        .get("metadata")
        .and_then(|m| m.get("finalizers"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter(|v| v.as_str() != Some(FOREGROUND_FINALIZER))
                .cloned()
                .collect()
        })
        .unwrap_or_default();

    if remaining.is_empty() {
        let _ = store.delete(&rref);
        return;
    }

    let mut updated = parent.clone();
    if let Some(meta) = updated.get_mut("metadata").and_then(|v| v.as_object_mut()) {
        meta.insert("finalizers".to_string(), Value::Array(remaining));
        // Blind write: foreground-gc owns terminal deletion, so don't lose the
        // strip to a racing controller's optimistic-concurrency bump.
        meta.remove("resourceVersion");
    }
    let _ = store.update(&rref, &updated);
}

#[cfg(test)]
mod tests {
    use super::reconcile;
    use r8s_store::{Store, backend::ResourceRef};
    use r8s_types::{GroupVersionResource, registry::ResourceRegistry};
    use rustc_hash::FxHashSet;
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
            .get(&ResourceRef {
                gvr: &gvr,
                namespace: Some("argocd"),
                name,
            })
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
        let orphans = reconcile(&store, &registry, &FxHashSet::default());
        assert!(
            exists(&store, "argocd-redis-abc"),
            "must survive first pass"
        );
        assert!(
            orphans.contains(&rs_uid),
            "should be flagged as a candidate"
        );

        // Second pass, still orphaned: now it's reaped.
        let orphans2 = reconcile(&store, &registry, &orphans);
        assert!(
            !exists(&store, "argocd-redis-abc"),
            "confirmed orphan is reaped"
        );
        assert!(
            !orphans2.contains(&rs_uid),
            "deleted object drops out of the set"
        );
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
                ResourceRef {
                    gvr: &dep_gvr,
                    namespace: Some("argocd"),
                    name: "argocd-redis",
                },
                &serde_json::json!({
                    "apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": { "name": "argocd-redis", "namespace": "argocd" },
                    "spec": {},
                }),
            )
            .unwrap();
        let dep_uid = dep["metadata"]["uid"].as_str().unwrap().to_string();

        rs_with_owner(&store, "argocd-redis-def", &dep_uid);

        let orphans = reconcile(&store, &registry, &FxHashSet::default());
        let orphans2 = reconcile(&store, &registry, &orphans);
        assert!(
            exists(&store, "argocd-redis-def"),
            "owned RS is never reaped"
        );
        assert!(orphans.is_empty() && orphans2.is_empty());
    }

    // The strand the self-heal exists for: a crash between `reconcile_one`'s
    // finalizer strip and its delete leaves an object with a deletionTimestamp
    // and no finalizer. Nothing blocks its removal, so it must be reaped.
    #[test]
    fn released_object_marked_for_deletion_is_finalized() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let registry = ResourceRegistry::default_mvp();

        let gvr = GroupVersionResource::replica_sets();
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: Some("argocd"),
                    name: "stuck-rs",
                },
                &serde_json::json!({
                    "apiVersion": "apps/v1", "kind": "ReplicaSet",
                    "metadata": {
                        "name": "stuck-rs", "namespace": "argocd",
                        "deletionTimestamp": "2026-01-01T00:00:00Z",
                    },
                    "spec": { "replicas": 1 },
                }),
            )
            .unwrap();

        assert!(exists(&store, "stuck-rs"));
        reconcile(&store, &registry, &FxHashSet::default());
        assert!(
            !exists(&store, "stuck-rs"),
            "released object with no finalizer must be reaped"
        );
    }

    // A parent carrying a non-foreground finalizer alongside ours must survive
    // the cascade: we strip only `foregroundDeletion` and leave the object for
    // the other finalizer's owner. Deleting it outright would violate that
    // finalizer's contract.
    #[test]
    fn foreground_parent_keeps_non_foreground_finalizers() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let registry = ResourceRegistry::default_mvp();

        let gvr = GroupVersionResource::deployments();
        let rref = ResourceRef {
            gvr: &gvr,
            namespace: Some("argocd"),
            name: "dep",
        };
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: Some("argocd"),
                    name: "dep",
                },
                &serde_json::json!({
                    "apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {
                        "name": "dep", "namespace": "argocd",
                        "deletionTimestamp": "2026-01-01T00:00:00Z",
                        "finalizers": ["foregroundDeletion", "example.com/keep"],
                    },
                    "spec": {},
                }),
            )
            .unwrap();

        // No dependents, so the cascade completes in one pass.
        reconcile(&store, &registry, &FxHashSet::default());

        let obj = store
            .get(&rref)
            .unwrap()
            .expect("object with a surviving finalizer must not be deleted");
        let fins: Vec<&str> = obj["metadata"]["finalizers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(
            fins, ["example.com/keep"],
            "only the foreground finalizer is stripped"
        );
    }

    // The common path: a foreground parent whose only finalizer is ours, with no
    // dependents left, is deleted outright.
    #[test]
    fn foreground_parent_with_no_other_finalizer_is_deleted() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let registry = ResourceRegistry::default_mvp();

        let gvr = GroupVersionResource::deployments();
        let rref = ResourceRef {
            gvr: &gvr,
            namespace: Some("argocd"),
            name: "dep",
        };
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: Some("argocd"),
                    name: "dep",
                },
                &serde_json::json!({
                    "apiVersion": "apps/v1", "kind": "Deployment",
                    "metadata": {
                        "name": "dep", "namespace": "argocd",
                        "deletionTimestamp": "2026-01-01T00:00:00Z",
                        "finalizers": ["foregroundDeletion"],
                    },
                    "spec": {},
                }),
            )
            .unwrap();

        reconcile(&store, &registry, &FxHashSet::default());
        assert!(
            store.get(&rref).unwrap().is_none(),
            "fully released foreground parent is deleted"
        );
    }
}
