use r8s_store::{Store, watch::WatchEventType};
use r8s_types::{
    CustomResourceDefinition, GroupVersionResource, ResourceType, registry::ResourceRegistry,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

pub async fn run(
    store: Store,
    shutdown: CancellationToken,
    registry: ResourceRegistry,
) -> anyhow::Result<()> {
    tracing::info!("crd controller started");
    let crd_gvr = GroupVersionResource::crds();

    // Bootstrap: register any CRDs already in the store.
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
                    Ok(event) => {
                        let crd: Result<CustomResourceDefinition, _> =
                            serde_json::from_value(event.object);
                        let Ok(crd) = crd else { continue };
                        match event.event_type {
                            WatchEventType::Added | WatchEventType::Modified => {
                                register_crd(&registry, &crd);
                            }
                            WatchEventType::Deleted => {
                                unregister_crd(&registry, &crd);
                            }
                        }
                    }
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

fn reconcile_all(store: &Store, crd_gvr: &GroupVersionResource, registry: &ResourceRegistry) {
    let crds = match store.list_as::<CustomResourceDefinition>(crd_gvr, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("crd controller list error: {e}");
            return;
        }
    };
    for crd in &crds {
        register_crd(registry, crd);
    }
}

fn register_crd(registry: &ResourceRegistry, crd: &CustomResourceDefinition) {
    let spec = &crd.spec;
    let namespaced = spec.scope.eq_ignore_ascii_case("namespaced");

    for ver in &spec.versions {
        if !ver.served {
            continue;
        }
        let gvr = GroupVersionResource::new(&spec.group, &ver.name, &spec.names.plural);
        let schema = ver
            .schema
            .as_ref()
            .and_then(|v| v.open_api_v3_schema.as_ref())
            .and_then(|s| serde_json::to_value(s).ok());
        let rt = ResourceType {
            gvr,
            kind: spec.names.kind.clone(),
            namespaced,
            singular: spec.names.singular.clone().unwrap_or_default(),
            short_names: spec.names.short_names.clone().unwrap_or_default(),
            subresources: vec![],
            schema,
        };
        tracing::info!(
            "registered CRD resource {}/{}/{}",
            spec.group,
            ver.name,
            spec.names.plural,
        );
        registry.register(rt);
    }
}

fn unregister_crd(registry: &ResourceRegistry, crd: &CustomResourceDefinition) {
    let spec = &crd.spec;
    for ver in &spec.versions {
        let gvr = GroupVersionResource::new(&spec.group, &ver.name, &spec.names.plural);
        tracing::info!(
            "unregistered CRD resource {}/{}/{}",
            spec.group,
            ver.name,
            spec.names.plural,
        );
        registry.unregister(&gvr);
    }
}
