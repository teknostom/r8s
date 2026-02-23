use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{GroupVersionResource, ObjectMeta};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::is_owned_by;

/// Minimal deserializable wrapper -- we only need metadata for GC.
#[derive(serde::Deserialize)]
struct MetadataOnly {
    metadata: ObjectMeta,
}

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("gc controller started");
    let deploy_gvr = GroupVersionResource::deployments();
    let rs_gvr = GroupVersionResource::replica_sets();
    let sts_gvr = GroupVersionResource::stateful_sets();
    let pods_gvr = GroupVersionResource::pods();

    let mut deploy_rx = store.watch(&deploy_gvr);
    let mut rs_rx = store.watch(&rs_gvr);
    let mut sts_rx = store.watch(&sts_gvr);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("gc controller shutting down");
                return Ok(());
            }
            event = deploy_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted) => {
                        let meta: MetadataOnly = match serde_json::from_value(event.object) {
                            Ok(m) => m,
                            Err(_) => continue,
                        };
                        let uid = meta.metadata.uid.as_deref().unwrap_or("");
                        delete_owned(&store, &rs_gvr, uid);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        gc_orphans(&store, &deploy_gvr, &rs_gvr);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
            event = rs_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted) => {
                        let meta: MetadataOnly = match serde_json::from_value(event.object) {
                            Ok(m) => m,
                            Err(_) => continue,
                        };
                        let uid = meta.metadata.uid.as_deref().unwrap_or("");
                        delete_owned(&store, &pods_gvr, uid);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        gc_orphans(&store, &rs_gvr, &pods_gvr);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
            event = sts_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted) => {
                        let meta: MetadataOnly = match serde_json::from_value(event.object) {
                            Ok(m) => m,
                            Err(_) => continue,
                        };
                        let uid = meta.metadata.uid.as_deref().unwrap_or("");
                        delete_owned(&store, &pods_gvr, uid);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        gc_orphans(&store, &sts_gvr, &pods_gvr);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

/// Full reconciliation after lag -- find children whose owners no longer exist.
fn gc_orphans(store: &Store, owner_gvr: &GroupVersionResource, child_gvr: &GroupVersionResource) {
    let owners = match store.list_as::<MetadataOnly>(owner_gvr, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("gc orphan scan: failed to list owners: {e}");
            return;
        }
    };
    let owner_uids: std::collections::HashSet<String> = owners
        .into_iter()
        .filter_map(|m| m.metadata.uid)
        .collect();

    let children = match store.list_as::<MetadataOnly>(child_gvr, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("gc orphan scan: failed to list children: {e}");
            return;
        }
    };

    for meta in &children {
        for oref in meta
            .metadata
            .owner_references
            .as_deref()
            .unwrap_or_default()
        {
            if !owner_uids.contains(&oref.uid) {
                let name = meta.metadata.name.as_deref().unwrap_or("");
                let ns = meta.metadata.namespace.as_deref();
                let rref = ResourceRef {
                    gvr: child_gvr,
                    namespace: ns,
                    name,
                };
                let _ = store.delete(&rref);
                tracing::info!(
                    "gc: orphan {}/{} deleted (owner uid={} gone)",
                    child_gvr.resource,
                    name,
                    oref.uid
                );
            }
        }
    }
}

fn delete_owned(store: &Store, child_gvr: &GroupVersionResource, owner_uid: &str) {
    let children = match store.list_as::<MetadataOnly>(child_gvr, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("gc list error: {e}");
            return;
        }
    };
    for meta in &children {
        if is_owned_by(&meta.metadata, owner_uid) {
            let name = meta.metadata.name.as_deref().unwrap_or("");
            let namespace = meta.metadata.namespace.as_deref();
            let resource_ref = ResourceRef {
                gvr: child_gvr,
                namespace,
                name,
            };
            match store.delete(&resource_ref) {
                Ok(_) => tracing::info!(
                    "gc: deleted {}/{} (owner uid={})",
                    child_gvr.resource,
                    name,
                    owner_uid
                ),
                Err(e) => {
                    tracing::warn!("gc: failed to delete {}/{}: {e}", child_gvr.resource, name)
                }
            }
        }
    }
}
