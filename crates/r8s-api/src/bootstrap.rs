use std::collections::BTreeMap;

use r8s_store::{Store, backend::ResourceRef};
use r8s_types::{
    GroupVersionResource, IngressClass, Namespace, ObjectMeta, Service,
    endpoints::{EndpointAddress, EndpointPort, EndpointSubset, Endpoints},
    ingressclass::IngressClassSpec,
    namespace::NamespaceStatus,
    service::{ServicePort, ServiceSpec},
};

pub fn bootstrap_namespaces(store: &Store) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::new("", "v1", "namespaces");

    for ns_name in ["default", "kube-system", "kube-public", "kube-node-lease"] {
        let resource_ref = ResourceRef {
            gvr: &gvr,
            namespace: None,
            name: ns_name,
        };
        if store.get(&resource_ref)?.is_none() {
            let ns = Namespace {
                api_version: "v1".into(),
                kind: "Namespace".into(),
                metadata: ObjectMeta {
                    name: Some(ns_name.into()),
                    ..Default::default()
                },
                status: Some(NamespaceStatus {
                    phase: Some("Active".into()),
                }),
            };
            store.create(resource_ref, &serde_json::to_value(&ns)?)?;
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
        let svc = Service {
            api_version: "v1".into(),
            kind: "Service".into(),
            metadata: ObjectMeta {
                name: Some("kubernetes".into()),
                namespace: Some("default".into()),
                labels: BTreeMap::from([
                    ("component".into(), "apiserver".into()),
                    ("provider".into(), "kubernetes".into()),
                ]),
                ..Default::default()
            },
            spec: ServiceSpec {
                type_: Some("ClusterIP".into()),
                cluster_ip: Some("10.96.0.1".into()),
                ports: vec![ServicePort {
                    name: Some("https".into()),
                    port: 443,
                    target_port: Some(6443),
                    protocol: "TCP".into(),
                }],
                ..Default::default()
            },
        };
        store.create(svc_ref, &serde_json::to_value(&svc)?)?;
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
        let ep = Endpoints {
            api_version: "v1".into(),
            kind: "Endpoints".into(),
            metadata: ObjectMeta {
                name: Some("kubernetes".into()),
                namespace: Some("default".into()),
                ..Default::default()
            },
            subsets: vec![EndpointSubset {
                addresses: vec![EndpointAddress {
                    ip: "10.244.0.1".into(),
                    target_ref: None,
                }],
                ports: vec![EndpointPort {
                    port: 6443,
                    protocol: "TCP".into(),
                    name: Some("https".into()),
                }],
            }],
        };
        store.create(ep_ref, &serde_json::to_value(&ep)?)?;
        tracing::info!("bootstrapped 'kubernetes' endpoints");
    }

    Ok(())
}

pub fn bootstrap_ingress_class(store: &Store) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::new("networking.k8s.io", "v1", "ingressclasses");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: None,
        name: "r8s",
    };
    if store.get(&rref)?.is_none() {
        let ic = IngressClass {
            api_version: "networking.k8s.io/v1".into(),
            kind: "IngressClass".into(),
            metadata: ObjectMeta {
                name: Some("r8s".into()),
                annotations: BTreeMap::from([(
                    "ingressclass.kubernetes.io/is-default-class".into(),
                    "true".into(),
                )]),
                ..Default::default()
            },
            spec: Some(IngressClassSpec {
                controller: Some("r8s.dev/ingress-controller".into()),
            }),
        };
        store.create(rref, &serde_json::to_value(&ic)?)?;
        tracing::info!("bootstrapped 'r8s' IngressClass");
    }
    Ok(())
}
