//! Local dynamic-volume provisioner.
//!
//! Watches PersistentVolumeClaims and, for each unbound claim, carves out a
//! hostPath-backed PersistentVolume under `<data_dir>/volumes/<pv>` and binds
//! the two together. This is the single-node analogue of a CSI provisioner:
//! every PV is just a directory on the host the kubelet bind-mounts into the
//! pod. No StorageClass topology, no access-mode enforcement — RWO/RWX all map
//! to the same local dir, which is correct for a one-node cluster.

use std::path::PathBuf;

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
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = reconcile_pvc(&store, &data_dir, &event.object) {
                            tracing::warn!("pvc provision error: {e}");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store, &data_dir),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

fn reconcile_all(store: &Store, data_dir: &PathBuf) {
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

fn reconcile_pvc(
    store: &Store,
    data_dir: &PathBuf,
    pvc: &serde_json::Value,
) -> anyhow::Result<()> {
    let name = pvc
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("PVC has no name"))?;
    let namespace = pvc.pointer("/metadata/namespace").and_then(|v| v.as_str());
    let uid = pvc
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .unwrap_or("");

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
