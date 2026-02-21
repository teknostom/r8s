use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::GroupVersionResource;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

fn is_owned_by(object: &serde_json::Value, owner_uid: &str) -> bool {
    object["metadata"]["ownerReferences"]
        .as_array()
        .map(|refs| refs.iter().any(|r| r["uid"].as_str() == Some(owner_uid)))
        .unwrap_or(false)
}

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("gc controller started");
    let deploy_gvr = GroupVersionResource::new("apps", "v1", "deployments");
    let rs_gvr = GroupVersionResource::new("apps", "v1", "replicasets");
    let pods_gvr = GroupVersionResource::new("", "v1", "pods");

    let mut deploy_rx = store.watch(&deploy_gvr);
    let mut rs_rx = store.watch(&rs_gvr);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("gc controller shutting down");
                return Ok(());
            }
            event = deploy_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted) => {
                        let uid = event.object["metadata"]["uid"].as_str().unwrap_or("");
                        delete_owned(&store, &rs_gvr, uid);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
            event = rs_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted) => {
                        let uid = event.object["metadata"]["uid"].as_str().unwrap_or("");
                        delete_owned(&store, &pods_gvr, uid);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

fn delete_owned(store: &Store, child_gvr: &GroupVersionResource, owner_uid: &str) {
    let result = match store.list(child_gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("gc list error: {e}");
            return;
        }
    };
    for item in &result.items {
        if is_owned_by(item, owner_uid) {
            let name = item["metadata"]["name"].as_str().unwrap_or("");
            let namespace = item["metadata"]["namespace"].as_str();
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
