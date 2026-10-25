//! Local dynamic-volume provisioner.
//!
//! Watches PersistentVolumeClaims and, for each unbound claim, carves out a
//! hostPath-backed PersistentVolume under `<data_dir>/volumes/<pv>` and binds
//! the two together. When a bound PVC is deleted and its PV's reclaim policy is
//! `Delete` (the default here), the PV object and its backing directory are
//! reclaimed.
//!
//! This is the single-node analogue of a CSI provisioner: every PV is just a
//! directory on the host the kubelet bind-mounts into the pod. Deliberate
//! single-node simplifications: no StorageClass topology; no access-mode
//! enforcement (RWO/RWX all map to the same local dir); and `capacity` is
//! advisory — the hostPath directory isn't quota-limited to the requested size.
//! All correct for a one-node cluster.

use std::path::{Path, PathBuf};

use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::GroupVersionResource;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

pub async fn run(
    store: Store,
    shutdown: CancellationToken,
    data_dir: PathBuf,
) -> anyhow::Result<()> {
    tracing::info!("provisioner controller started");
    let gvr = GroupVersionResource::persistent_volume_claims();

    reconcile_all(&store, &data_dir);

    let mut pvc_rx = store.watch(&gvr);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("provisioner controller shutting down");
                return Ok(());
            }
            event = pvc_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = handle_pvc_deleted(&store, &data_dir, &event.object) {
                            tracing::warn!("pvc reclaim error: {e}");
                        }
                    }
                    Ok(event) => {
                        if let Err(e) = reconcile_pvc(&store, &data_dir, &event.object) {
                            tracing::warn!("pvc provision error: {e}");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store, &data_dir),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
}

fn reconcile_all(store: &Store, data_dir: &Path) {
    let gvr = GroupVersionResource::persistent_volume_claims();
    let result = match store.list(&gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("provisioner list error: {e}");
            return;
        }
    };
    for pvc in &result.items {
        if let Err(e) = reconcile_pvc(store, data_dir, pvc) {
            tracing::warn!("pvc provision error: {e}");
        }
    }
}

fn reconcile_pvc(store: &Store, data_dir: &Path, pvc: &serde_json::Value) -> anyhow::Result<()> {
    let name = pvc
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("PVC has no name"))?;
    let namespace = pvc.pointer("/metadata/namespace").and_then(|v| v.as_str());
    let uid = pvc
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if uid.is_empty() {
        // The PV name and claimRef are keyed by uid; an empty one would collide
        // across every uid-less PVC. Wait for the stamped object.
        tracing::warn!("pvc '{name}': missing uid; skipping provision");
        return Ok(());
    }

    // Already bound (or being bound by a prior pass)? Nothing to do.
    let phase = pvc.pointer("/status/phase").and_then(|v| v.as_str());
    let bound_volume = pvc
        .pointer("/spec/volumeName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    if phase == Some("Bound") || bound_volume.is_some() {
        return Ok(());
    }

    // We provision for the default class: an explicit class we don't know
    // about belongs to someone else. Empty/missing class == default.
    let class = pvc
        .pointer("/spec/storageClassName")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if !class.is_empty() && class != "standard" {
        return Ok(());
    }

    let requested = pvc
        .pointer("/spec/resources/requests/storage")
        .and_then(|v| v.as_str())
        .unwrap_or("1Gi")
        .to_string();
    let access_modes = pvc
        .pointer("/spec/accessModes")
        .cloned()
        .unwrap_or_else(|| serde_json::json!(["ReadWriteOnce"]));

    let pv_name = format!("pvc-{uid}");
    let host_path = data_dir.join("volumes").join(&pv_name);
    std::fs::create_dir_all(&host_path)?;
    let host_path = host_path.to_string_lossy().to_string();

    // Create the backing PV (idempotent — skip if a prior pass made it).
    let pv_gvr = GroupVersionResource::persistent_volumes();
    let pv_ref = ResourceRef {
        gvr: &pv_gvr,
        namespace: None,
        name: &pv_name,
    };
    if store.get(&pv_ref)?.is_none() {
        let pv = serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": { "name": pv_name },
            "spec": {
                "capacity": { "storage": requested },
                "accessModes": access_modes,
                "persistentVolumeReclaimPolicy": "Delete",
                "storageClassName": "standard",
                "hostPath": { "path": host_path },
                "claimRef": {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "namespace": namespace,
                    "name": name,
                    "uid": uid,
                },
            },
            "status": { "phase": "Bound" },
        });
        store.create(pv_ref, &pv)?;
        tracing::info!("provisioned PV '{pv_name}' for pvc '{name}' ({requested})");
    }

    // Bind the PVC: point it at the PV and flip it to Bound.
    let pvc_gvr = GroupVersionResource::persistent_volume_claims();
    let pvc_ref = ResourceRef {
        gvr: &pvc_gvr,
        namespace,
        name,
    };
    let mut current = match store.get(&pvc_ref)? {
        Some(v) => v,
        None => return Ok(()),
    };
    if let Some(spec) = current.pointer_mut("/spec").and_then(|v| v.as_object_mut()) {
        spec.insert("volumeName".to_string(), serde_json::json!(pv_name));
    }
    let status = serde_json::json!({
        "phase": "Bound",
        "accessModes": access_modes,
        "capacity": { "storage": requested },
    });
    if let Some(obj) = current.as_object_mut() {
        obj.insert("status".to_string(), status);
    }
    store.update(&pvc_ref, &current)?;
    tracing::info!("bound pvc '{name}' -> PV '{pv_name}'");

    Ok(())
}

/// On PVC deletion, honor the bound PV's reclaim policy. `Delete` (our default)
/// removes the PV object and its backing directory; `Retain` leaves both. The
/// directory is only removed when it lives under `<data_dir>/volumes`, so a
/// hand-authored PV pointing hostPath elsewhere can't make us `rm -rf` a path we
/// don't own.
fn handle_pvc_deleted(store: &Store, data_dir: &Path, pvc: &serde_json::Value) -> anyhow::Result<()> {
    let pv_name = pvc
        .pointer("/spec/volumeName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| {
            pvc.pointer("/metadata/uid")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|u| format!("pvc-{u}"))
        });
    let Some(pv_name) = pv_name else {
        return Ok(());
    };

    let pv_gvr = GroupVersionResource::persistent_volumes();
    let pv_ref = ResourceRef {
        gvr: &pv_gvr,
        namespace: None,
        name: &pv_name,
    };
    let Some(pv) = store.get(&pv_ref)? else {
        return Ok(());
    };

    let reclaim = pv
        .pointer("/spec/persistentVolumeReclaimPolicy")
        .and_then(|v| v.as_str())
        .unwrap_or("Retain");
    if reclaim != "Delete" {
        return Ok(());
    }

    if let Some(path) = pv.pointer("/spec/hostPath/path").and_then(|v| v.as_str()) {
        let volumes_root = data_dir.join("volumes");
        if Path::new(path).starts_with(&volumes_root) {
            match std::fs::remove_dir_all(path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!("failed to remove volume dir '{path}': {e}"),
            }
        } else {
            tracing::warn!(
                "PV '{pv_name}' hostPath '{path}' is outside {}; not removing",
                volumes_root.display()
            );
        }
    }

    store.delete(&pv_ref)?;
    tracing::info!("reclaimed PV '{pv_name}' (PVC deleted, reclaimPolicy=Delete)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pv_ref<'a>(gvr: &'a GroupVersionResource, name: &'a str) -> ResourceRef<'a> {
        ResourceRef {
            gvr,
            namespace: None,
            name,
        }
    }

    #[test]
    fn reclaim_deletes_pv_and_dir_on_pvc_delete() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let store = Store::open(&data_dir.join("s.db")).unwrap();
        let pvc_gvr = GroupVersionResource::persistent_volume_claims();

        let created = store
            .create(
                ResourceRef {
                    gvr: &pvc_gvr,
                    namespace: Some("default"),
                    name: "pvc",
                },
                &json!({
                    "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                    "metadata": {"name": "pvc", "namespace": "default"},
                    "spec": {"resources": {"requests": {"storage": "1Gi"}}}
                }),
            )
            .unwrap();
        reconcile_pvc(&store, &data_dir, &created).unwrap();

        let uid = created["metadata"]["uid"].as_str().unwrap();
        let pv_name = format!("pvc-{uid}");
        let pv_dir = data_dir.join("volumes").join(&pv_name);
        let pv_gvr = GroupVersionResource::persistent_volumes();
        assert!(pv_dir.exists());
        assert!(store.get(&pv_ref(&pv_gvr, &pv_name)).unwrap().is_some());

        // Fetch the now-bound PVC (has spec.volumeName) and delete it.
        let bound = store
            .get(&ResourceRef {
                gvr: &pvc_gvr,
                namespace: Some("default"),
                name: "pvc",
            })
            .unwrap()
            .unwrap();
        handle_pvc_deleted(&store, &data_dir, &bound).unwrap();

        assert!(!pv_dir.exists(), "backing directory reclaimed");
        assert!(
            store.get(&pv_ref(&pv_gvr, &pv_name)).unwrap().is_none(),
            "PV object reclaimed"
        );
    }

    #[test]
    fn retain_policy_keeps_pv() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let store = Store::open(&data_dir.join("s.db")).unwrap();
        let pv_gvr = GroupVersionResource::persistent_volumes();

        store
            .create(
                pv_ref(&pv_gvr, "pv-retained"),
                &json!({
                    "apiVersion": "v1", "kind": "PersistentVolume",
                    "metadata": {"name": "pv-retained"},
                    "spec": {
                        "persistentVolumeReclaimPolicy": "Retain",
                        "hostPath": {"path": data_dir.join("volumes/pv-retained").to_string_lossy()}
                    }
                }),
            )
            .unwrap();

        let pvc = json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": "pvc", "namespace": "default", "uid": "u"},
            "spec": {"volumeName": "pv-retained"}
        });
        handle_pvc_deleted(&store, &data_dir, &pvc).unwrap();

        assert!(
            store.get(&pv_ref(&pv_gvr, "pv-retained")).unwrap().is_some(),
            "Retain policy must keep the PV"
        );
    }
}
