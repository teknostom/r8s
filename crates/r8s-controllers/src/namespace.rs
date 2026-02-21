use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::GroupVersionResource;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("namespace controller started");
    let ns_gvr = GroupVersionResource::new("", "v1", "namespaces");
    let sa_gvr = GroupVersionResource::new("", "v1", "serviceaccounts");

    reconcile_all(&store, &ns_gvr, &sa_gvr);

    let mut rx = store.watch(&ns_gvr);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("namespace controller shutting down");
                return Ok(());
            }
            event = rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Added) => {
                        if let Some(name) = event.object["metadata"]["name"].as_str() {
                            ensure_default_sa(&store, &sa_gvr, name);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("namespace controller lagged {n} events, re-syncing");
                        reconcile_all(&store, &ns_gvr, &sa_gvr);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    Ok(_) => {}
                }
            }
        }
    }
}

fn reconcile_all(store: &Store, ns_gvr: &GroupVersionResource, sa_gvr: &GroupVersionResource) {
    let result = match store.list(ns_gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("namespace controller list error: {e}");
            return;
        }
    };
    for ns in &result.items {
        if let Some(name) = ns["metadata"]["name"].as_str() {
            ensure_default_sa(store, sa_gvr, name);
        }
    }
}

fn ensure_default_sa(store: &Store, sa_gvr: &GroupVersionResource, namespace: &str) {
    let resource_ref = ResourceRef {
        gvr: sa_gvr,
        namespace: Some(namespace),
        name: "default",
    };
    match store.get(&resource_ref) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let sa = serde_json::json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"name": "default", "namespace": namespace},
            });
            match store.create(resource_ref, &sa) {
                Ok(_) => tracing::info!("created default ServiceAccount in '{namespace}'"),
                Err(e) => tracing::warn!("failed to create default SA in '{namespace}': {e}"),
            }
        }
        Err(e) => tracing::warn!("failed to check SA in '{namespace}': {e}"),
    }
}
