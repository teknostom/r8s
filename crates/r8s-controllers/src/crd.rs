use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
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
                            serde_json::from_value(event.object.as_ref().clone());
                        let Ok(crd) = crd else { continue };
                        match event.event_type {
                            WatchEventType::Added | WatchEventType::Modified => {
                                // Unregister first so versions that flipped
                                // to served=false (or were renamed) drop out
                                // of the registry; register_crd only adds
                                // served versions, never removes stale ones.
                                unregister_crd(&registry, &crd);
                                register_crd(&registry, &crd);
                                mark_established(&store, &crd);
                            }
                            WatchEventType::Deleted => {
                                delete_instances(&store, &crd);
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
        mark_established(store, crd);
    }
}

/// Write back `status.conditions = [Established=True, NamesAccepted=True]`
/// and `status.acceptedNames`/`status.storedVersions` so Helm's kstatus
/// readiness check treats the CRD as ready. r8s has no real establishment
/// process — registration is synchronous and never fails — so we always
/// mark True. The store's no-op detection means re-applying the same
/// status doesn't re-fire a watch event (which would loop us back here).
fn mark_established(store: &Store, crd: &CustomResourceDefinition) {
    let crd_gvr = GroupVersionResource::crds();
    let name = crd.metadata.name.as_deref().unwrap_or("");
    if name.is_empty() {
        return;
    }
    let rref = ResourceRef {
        gvr: &crd_gvr,
        namespace: None,
        name,
    };
    let mut obj = match store.get(&rref) {
        Ok(Some(v)) => v,
        _ => return,
    };
    let stored_versions: Vec<serde_json::Value> = crd
        .spec
        .versions
        .iter()
        .filter(|v| v.storage)
        .map(|v| serde_json::Value::String(v.name.clone()))
        .collect();
    let accepted_names = serde_json::to_value(&crd.spec.names).unwrap_or(serde_json::json!({}));
    let new_status = serde_json::json!({
        "conditions": [
            {
                "type": "NamesAccepted",
                "status": "True",
                "reason": "NoConflicts",
                "message": "no conflicts found",
            },
            {
                "type": "Established",
                "status": "True",
                "reason": "InitialNamesAccepted",
                "message": "the initial names have been accepted",
            },
        ],
        "acceptedNames": accepted_names,
        "storedVersions": stored_versions,
    });
    if obj.get("status") == Some(&new_status) {
        return;
    }
    if let Some(map) = obj.as_object_mut() {
        map.insert("status".into(), new_status);
    }
    if let Err(e) = store.update(&rref, &obj) {
        tracing::debug!("crd status update for '{name}' failed: {e}");
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
            subresources: ver
                .subresources
                .as_ref()
                .map(|s| {
                    let mut out = Vec::new();
                    if s.status.is_some() {
                        out.push("status".to_string());
                    }
                    if s.scale.is_some() {
                        out.push("scale".to_string());
                    }
                    out
                })
                .unwrap_or_default(),
            schema,
            storage_gvr: Some(GroupVersionResource::storage(
                &spec.group,
                &spec.names.plural,
            )),
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

/// Delete every stored custom resource of a CRD being removed. Upstream GCs a
/// CRD's instances when the definition goes away; without this they'd linger in
/// the store under a GVR nothing can serve or reach.
fn delete_instances(store: &Store, crd: &CustomResourceDefinition) {
    let spec = &crd.spec;
    let mut deleted = 0u32;
    let gvr = GroupVersionResource::storage(&spec.group, &spec.names.plural);
    let items = store
        .list(&gvr, None, None, None, None, None)
        .map(|r| r.items)
        .unwrap_or_default();
    for item in &items {
        let meta = item.get("metadata");
        let Some(name) = meta.and_then(|m| m.get("name")).and_then(|v| v.as_str()) else {
            continue;
        };
        let namespace = meta
            .and_then(|m| m.get("namespace"))
            .and_then(|v| v.as_str());
        if matches!(
            store.delete(&ResourceRef {
                gvr: &gvr,
                namespace,
                name,
            }),
            Ok(Some(_))
        ) {
            deleted += 1;
        }
    }
    if deleted > 0 {
        tracing::info!(
            "crd '{}' deleted: removed {deleted} custom resource(s)",
            crd.metadata.name.as_deref().unwrap_or("")
        );
    }
}

fn unregister_crd(registry: &ResourceRegistry, crd: &CustomResourceDefinition) {
    let spec = &crd.spec;
    // Sweep every GVR matching this CRD's (group, plural) — not just the
    // versions listed in the current spec — so renames and served-flips
    // drop stale entries instead of leaving them in the registry.
    let stale: Vec<GroupVersionResource> = registry
        .iter()
        .into_iter()
        .filter(|rt| rt.gvr.group == spec.group && rt.gvr.resource == spec.names.plural)
        .map(|rt| rt.gvr.clone())
        .collect();
    for gvr in stale {
        tracing::info!(
            "unregistered CRD resource {}/{}/{}",
            gvr.group,
            gvr.version,
            gvr.resource,
        );
        registry.unregister(&gvr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn crd_deletion_reaps_instances() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();

        let crd: CustomResourceDefinition = serde_json::from_value(json!({
            "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.example.com"},
            "spec": {
                "group": "example.com",
                "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
                "scope": "Namespaced",
                "versions": [{"name": "v1", "served": true, "storage": true}]
            }
        }))
        .unwrap();

        let gvr = GroupVersionResource::storage("example.com", "widgets");
        for name in ["w1", "w2"] {
            store
                .create(
                    ResourceRef {
                        gvr: &gvr,
                        namespace: Some("default"),
                        name,
                    },
                    &json!({
                        "apiVersion": "example.com/v1", "kind": "Widget",
                        "metadata": {"name": name, "namespace": "default"}
                    }),
                )
                .unwrap();
        }

        delete_instances(&store, &crd);

        assert_eq!(
            store
                .list(&gvr, None, None, None, None, None)
                .unwrap()
                .items
                .len(),
            0,
            "all custom resources reaped on CRD deletion"
        );
    }

    #[test]
    fn served_versions_share_one_storage_gvr() {
        let crd: CustomResourceDefinition = serde_json::from_value(json!({
            "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.example.com"},
            "spec": {
                "group": "example.com",
                "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
                "scope": "Cluster",
                "versions": [
                    {"name": "v1beta1", "served": true, "storage": false},
                    {"name": "v1", "served": true, "storage": true},
                    {"name": "v2alpha1", "served": false, "storage": false}
                ]
            }
        }))
        .unwrap();
        let registry = ResourceRegistry::new();
        register_crd(&registry, &crd);

        let storage = GroupVersionResource::storage("example.com", "widgets");
        for version in ["v1beta1", "v1"] {
            let rt = registry
                .get_by_gvr(&GroupVersionResource::new(
                    "example.com",
                    version,
                    "widgets",
                ))
                .unwrap_or_else(|| panic!("{version} registered"));
            assert_eq!(rt.storage_gvr(), &storage);
        }
        assert!(
            registry
                .get_by_gvr(&GroupVersionResource::new(
                    "example.com",
                    "v2alpha1",
                    "widgets"
                ))
                .is_none(),
            "unserved versions are not registered"
        );
    }
}
