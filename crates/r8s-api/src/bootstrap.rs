use r8s_store::{Store, backend::ResourceRef};
use r8s_types::GroupVersionResource;

pub fn bootstrap_namespaces(store: &Store) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::new("", "v1", "namespaces");

    for ns_name in ["default", "kube-system", "kube-public", "kube-node-lease"] {
        let resource_ref = ResourceRef {
            gvr: &gvr,
            namespace: None,
            name: ns_name,
        };
        if store.get(&resource_ref)?.is_none() {
            let ns = serde_json::json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": ns_name },
                "spec": {},
                "status": {"phase": "Active"},
            });
            store.create(resource_ref, &ns)?;
            tracing::info!("bootstrapped namespace '{ns_name}'");
        }
    }
    Ok(())
}
