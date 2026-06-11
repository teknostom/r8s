use std::collections::BTreeMap;

use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{GroupVersionResource, ObjectMeta, ServiceAccount, registry::ResourceRegistry};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Name of the ConfigMap the upstream `root-ca-cert-publisher` controller
/// writes into every namespace. The e2e framework blocks each test's
/// BeforeEach on this object showing up (see `WaitForKubeRootCAInNamespace`).
const KUBE_ROOT_CA_CONFIGMAP: &str = "kube-root-ca.crt";

pub async fn run(
    store: Store,
    shutdown: CancellationToken,
    ca_pem: String,
    registry: ResourceRegistry,
) -> anyhow::Result<()> {
    tracing::info!("namespace controller started");
    let ns_gvr = GroupVersionResource::namespaces();
    let sa_gvr = GroupVersionResource::service_accounts();
    let cm_gvr = GroupVersionResource::configmaps();

    reconcile_all(&store, &ns_gvr, &sa_gvr, &cm_gvr, &ca_pem);

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
                        if let Ok(ns) = ns
                            && let Some(name) = ns.metadata.name.as_deref()
                        {
                            ensure_default_sa(&store, &sa_gvr, name);
                            ensure_kube_root_ca(&store, &cm_gvr, name, &ca_pem);
                        }
                    }
                    Ok(event) if matches!(event.event_type, WatchEventType::Deleted) => {
                        let ns: Result<r8s_types::Namespace, _> =
                            serde_json::from_value(event.object);
                        if let Ok(ns) = ns
                            && let Some(name) = ns.metadata.name.as_deref()
                        {
                            cascade_delete(&store, &registry, name);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("namespace controller lagged {n} events, re-syncing");
                        reconcile_all(&store, &ns_gvr, &sa_gvr, &cm_gvr, &ca_pem);
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    Ok(_) => {}
                }
            }
        }
    }
}

fn reconcile_all(
    store: &Store,
    ns_gvr: &GroupVersionResource,
    sa_gvr: &GroupVersionResource,
    cm_gvr: &GroupVersionResource,
    ca_pem: &str,
) {
    let namespaces = match store.list_as::<r8s_types::Namespace>(ns_gvr, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("namespace controller list error: {e}");
            return;
        }
    };
    for ns in &namespaces {
        if let Some(name) = ns.metadata.name.as_deref() {
            ensure_default_sa(store, sa_gvr, name);
            ensure_kube_root_ca(store, cm_gvr, name, ca_pem);
        }
    }
}

/// Delete every namespaced object remaining in `namespace`. Mirrors the
/// kube-controller-manager namespace controller: when a namespace goes away,
/// everything inside it must too. Without this, objects in a deleted namespace
/// are orphaned — the API forgets the namespace but pods keep their containers
/// running on the kubelet, and `kubectl delete namespace` silently leaks. We
/// delete pods directly (not just their controllers) so the kubelet sees the
/// Deleted event and reaps promptly; absent-owner GC mops up anything missed.
fn cascade_delete(store: &Store, registry: &ResourceRegistry, namespace: &str) {
    let mut deleted = 0u32;
    for rt in registry.iter() {
        if !rt.namespaced {
            continue;
        }
        let items = match store.list(&rt.gvr, Some(namespace), None, None, None, None) {
            Ok(r) => r.items,
            Err(_) => continue,
        };
        for item in &items {
            let Some(name) = item
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|v| v.as_str())
            else {
                continue;
            };
            let rref = ResourceRef {
                gvr: &rt.gvr,
                namespace: Some(namespace),
                name,
            };
            if matches!(store.delete(&rref), Ok(Some(_))) {
                deleted += 1;
            }
        }
    }
    if deleted > 0 {
        tracing::info!("namespace '{namespace}' deleted: cascaded {deleted} objects");
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

/// Publish the `kube-root-ca.crt` ConfigMap holding the API server's CA bundle
/// into `namespace`, mirroring kube-controller-manager's `root-ca-cert-publisher`.
/// Conformance BeforeEach polls for this; without it every spec stalls 2 min.
fn ensure_kube_root_ca(
    store: &Store,
    cm_gvr: &GroupVersionResource,
    namespace: &str,
    ca_pem: &str,
) {
    let resource_ref = ResourceRef {
        gvr: cm_gvr,
        namespace: Some(namespace),
        name: KUBE_ROOT_CA_CONFIGMAP,
    };
    match store.get(&resource_ref) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let mut data = BTreeMap::new();
            data.insert("ca.crt".to_string(), serde_json::Value::String(ca_pem.into()));
            let cm = serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": KUBE_ROOT_CA_CONFIGMAP,
                    "namespace": namespace,
                },
                "data": data,
            });
            match store.create(resource_ref, &cm) {
                Ok(_) => {
                    tracing::info!("published {KUBE_ROOT_CA_CONFIGMAP} into namespace '{namespace}'");
                }
                Err(e) => tracing::warn!(
                    "failed to publish {KUBE_ROOT_CA_CONFIGMAP} into '{namespace}': {e}"
                ),
            }
        }
        Err(e) => tracing::warn!("failed to check {KUBE_ROOT_CA_CONFIGMAP} in '{namespace}': {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::cascade_delete;
    use r8s_store::{Store, backend::ResourceRef};
    use r8s_types::{GroupVersionResource, registry::ResourceRegistry};
    use tempfile::TempDir;

    fn pod(name: &str, namespace: &str) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": name, "namespace": namespace },
            "spec": { "containers": [{ "name": "main", "image": "nginx" }] }
        })
    }

    #[test]
    fn cascade_delete_removes_only_target_namespace() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("test.db")).unwrap();
        let registry = ResourceRegistry::default_mvp();
        let pods = GroupVersionResource::new("", "v1", "pods");

        let mk = |ns: &str, name: &str| {
            store
                .create(
                    ResourceRef { gvr: &pods, namespace: Some(ns), name },
                    &pod(name, ns),
                )
                .unwrap();
        };
        mk("doomed", "a");
        mk("doomed", "b");
        mk("keep", "c");

        cascade_delete(&store, &registry, "doomed");

        let doomed = store
            .list(&pods, Some("doomed"), None, None, None, None)
            .unwrap();
        let keep = store
            .list(&pods, Some("keep"), None, None, None, None)
            .unwrap();
        assert_eq!(doomed.items.len(), 0, "target namespace contents must be cascaded");
        assert_eq!(keep.items.len(), 1, "a bystander namespace must be untouched");
    }
}
