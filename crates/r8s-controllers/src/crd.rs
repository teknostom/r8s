use std::sync::{Arc, RwLock};

use r8s_store::{Store, watch::WatchEventType};
use r8s_types::{GroupVersionResource, ResourceType, registry::ResourceRegistry};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// CRD controller: watches CustomResourceDefinition objects and dynamically
/// registers the corresponding resource types in the shared registry so that
/// the API server can serve their endpoints.
pub async fn run(
    store: Store,
    registry: Arc<RwLock<ResourceRegistry>>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!("crd controller started");
    let crd_gvr =
        GroupVersionResource::new("apiextensions.k8s.io", "v1", "customresourcedefinitions");

    // Register any CRDs that were persisted in a previous run.
    reconcile_all(&store, &crd_gvr, &registry);

    let mut rx = store.watch(&crd_gvr);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("crd controller shutting down");
                return Ok(());
            }
            event = rx.recv() => {
                match event {
                    Ok(event) => match event.event_type {
                        WatchEventType::Added | WatchEventType::Modified => {
                            if let Some(rt) = resource_type_from_crd(&event.object) {
                                let key = rt.gvr.key_prefix();
                                registry.write().expect("registry lock poisoned").register(rt);
                                tracing::info!("registered CRD resource: {key}");
                            } else {
                                tracing::warn!(
                                    "crd controller: could not parse CRD spec from {:?}",
                                    event.object["metadata"]["name"]
                                );
                            }
                        }
                        WatchEventType::Deleted => {
                            // Resource-type un-registration is not supported: the dynamic
                            // routes baked into Axum at startup cannot be removed at
                            // runtime.  The registry entry stays; requests for deleted
                            // CRD resources will simply return empty lists / 404s once
                            // the stored objects are gone.
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("crd controller lagged {n} events, re-syncing");
                        reconcile_all(&store, &crd_gvr, &registry);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
}

fn reconcile_all(
    store: &Store,
    crd_gvr: &GroupVersionResource,
    registry: &Arc<RwLock<ResourceRegistry>>,
) {
    let result = match store.list(crd_gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("crd controller list error: {e}");
            return;
        }
    };
    for crd in &result.items {
        if let Some(rt) = resource_type_from_crd(crd) {
            let key = rt.gvr.key_prefix();
            registry.write().expect("registry lock poisoned").register(rt);
            tracing::info!("registered CRD resource: {key}");
        }
    }
}

/// Build a [`ResourceType`] from a stored CRD object following the
/// `apiextensions.k8s.io/v1` schema.
fn resource_type_from_crd(crd: &serde_json::Value) -> Option<ResourceType> {
    let spec = &crd["spec"];
    let group = spec["group"].as_str()?;
    let names = &spec["names"];
    let plural = names["plural"].as_str()?;
    let kind = names["kind"].as_str()?;
    let singular = names["singular"].as_str().unwrap_or(plural);
    let namespaced = spec["scope"].as_str() == Some("Namespaced");
    let short_names: Vec<String> = names["shortNames"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // Prefer the version marked as storage=true; fall back to the first entry.
    let versions = spec["versions"].as_array()?;
    let version = versions
        .iter()
        .find(|v| v["storage"].as_bool() == Some(true))
        .or_else(|| versions.first())
        .and_then(|v| v["name"].as_str())?;

    Some(ResourceType {
        gvr: GroupVersionResource::new(group, version, plural),
        kind: kind.to_string(),
        namespaced,
        singular: singular.to_string(),
        short_names,
        subresources: vec![],
    })
}
