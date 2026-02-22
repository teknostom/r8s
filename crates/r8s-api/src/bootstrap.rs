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
    // Bootstrap the "kubernetes" service in default namespace
    let svc_gvr = GroupVersionResource::new("", "v1", "services");
    let svc_ref = ResourceRef {
        gvr: &svc_gvr,
        namespace: Some("default"),
        name: "kubernetes",
    };
    if store.get(&svc_ref)?.is_none() {
        let svc = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "kubernetes",
                "namespace": "default",
                "labels": {
                    "component": "apiserver",
                    "provider": "kubernetes",
                },
            },
            "spec": {
                "type": "ClusterIP",
                "clusterIP": "10.96.0.1",
                "ports": [{
                    "name": "https",
                    "port": 443,
                    "targetPort": 6443,
                    "protocol": "TCP",
                }],
            },
        });
        store.create(svc_ref, &svc)?;
        tracing::info!("bootstrapped 'kubernetes' service");
    }

    // Bootstrap endpoints for the kubernetes service
    let ep_gvr = GroupVersionResource::new("", "v1", "endpoints");
    let ep_ref = ResourceRef {
        gvr: &ep_gvr,
        namespace: Some("default"),
        name: "kubernetes",
    };
    if store.get(&ep_ref)?.is_none() {
        let ep = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {
                "name": "kubernetes",
                "namespace": "default",
            },
            "subsets": [{
                "addresses": [{"ip": "10.244.0.1"}],
                "ports": [{
                    "name": "https",
                    "port": 6443,
                    "protocol": "TCP",
                }],
            }],
        });
        store.create(ep_ref, &ep)?;
        tracing::info!("bootstrapped 'kubernetes' endpoints");
    }

    Ok(())
}

pub fn bootstrap_traefik(store: &Store, data_dir: &std::path::Path) -> anyhow::Result<()> {
    let sa_dir = data_dir.join("serviceaccount");
    std::fs::create_dir_all(&sa_dir)?;
    std::fs::write(sa_dir.join("token"), "dummy")?;
    std::fs::write(sa_dir.join("ca.crt"), "")?;
    std::fs::write(sa_dir.join("namespace"), "default")?;

    let deploy_gvr = GroupVersionResource::new("apps", "v1", "deployments");
    let deploy_ref = ResourceRef {
        gvr: &deploy_gvr,
        namespace: Some("kube-system"),
        name: "traefik",
    };
    if store.get(&deploy_ref)?.is_none() {
        let deploy = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "traefik",
                "namespace": "kube-system",
                "labels": {"app": "traefik"},
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "traefik"}},
                "template": {
                    "metadata": {"labels": {"app": "traefik"}},
                    "spec": {
                        "containers": [{
                            "name": "traefik",
                            "image": "traefik:v2.11",
                            "args": [
                                "--entrypoints.web.address=:80",
                                "--providers.kubernetesingress",
                                "--providers.kubernetesingress.endpoint=http://10.244.0.1:6443",
                                "--log.level=INFO",
                            ],
                        }],
                    },
                },
            },
        });
        store.create(deploy_ref, &deploy)?;
        tracing::info!("bootstrapped traefik deployment");
    }

    let svc_gvr = GroupVersionResource::new("", "v1", "services");
    let svc_ref = ResourceRef {
        gvr: &svc_gvr,
        namespace: Some("kube-system"),
        name: "traefik",
    };
    if store.get(&svc_ref)?.is_none() {
        let svc = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "traefik",
                "namespace": "kube-system",
                "labels": {"app": "traefik"},
            },
            "spec": {
                "type": "LoadBalancer",
                "selector": {"app": "traefik"},
                "clusterIP": "10.96.0.80",
                "ports": [{
                    "name": "web",
                    "port": 80,
                    "targetPort": 80,
                    "protocol": "TCP",
                }],
            },
        });
        store.create(svc_ref, &svc)?;
        tracing::info!("bootstrapped traefik service");
    }

    Ok(())
}
