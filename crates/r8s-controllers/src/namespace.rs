use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{GroupVersionResource, ObjectMeta, ServiceAccount};
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
                        let ns: Result<r8s_types::Namespace, _> = serde_json::from_value(event.object);
                        if let Ok(ns) = ns {
                            if let Some(name) = ns.metadata.name.as_deref() {
                                ensure_default_sa(&store, &sa_gvr, name);
                            }
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
    for item in &result.items {
        let ns: Result<r8s_types::Namespace, _> = serde_json::from_value(item.clone());
        if let Ok(ns) = ns {
            if let Some(name) = ns.metadata.name.as_deref() {
                ensure_default_sa(store, sa_gvr, name);
            }
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
            let sa = ServiceAccount {
                metadata: ObjectMeta {
                    name: Some("default".into()),
                    namespace: Some(namespace.into()),
                    ..Default::default()
                },
                ..Default::default()
            };
            let value = match serde_json::to_value(&sa) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("failed to serialize SA: {e}");
                    return;
                }
            };
            match store.create(resource_ref, &value) {
                Ok(_) => tracing::info!("created default ServiceAccount in '{namespace}'"),
                Err(e) => tracing::warn!("failed to create default SA in '{namespace}': {e}"),
            }
        }
        Err(e) => tracing::warn!("failed to check SA in '{namespace}': {e}"),
    }
}
