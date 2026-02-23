use std::collections::BTreeMap;

use r8s_store::{Store, backend::ResourceRef};
use r8s_types::{
    EndpointAddress, EndpointPort, EndpointSubset, Endpoints, GroupVersionResource, IngressClass,
    IngressClassSpec, IntOrString, Namespace, NamespaceStatus, ObjectMeta, Service, ServicePort,
    ServiceSpec,
};

pub fn bootstrap_namespaces(store: &Store) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::namespaces();

    for ns_name in ["default", "kube-system", "kube-public", "kube-node-lease"] {
        let resource_ref = ResourceRef {
            gvr: &gvr,
            namespace: None,
            name: ns_name,
        };
        if store.get(&resource_ref)?.is_none() {
            let ns = Namespace {
                metadata: ObjectMeta {
                    name: Some(ns_name.into()),
                    ..Default::default()
                },
                spec: None,
                status: Some(NamespaceStatus {
                    conditions: None,
                    phase: Some("Active".into()),
                }),
            };
            store.create(resource_ref, &serde_json::to_value(&ns)?)?;
            tracing::info!("bootstrapped namespace '{ns_name}'");
        }
    }
    // Bootstrap the "kubernetes" service in default namespace
    let svc_gvr = GroupVersionResource::services();
    let svc_ref = ResourceRef {
        gvr: &svc_gvr,
        namespace: Some("default"),
        name: "kubernetes",
    };
    if store.get(&svc_ref)?.is_none() {
        let svc = Service {
            metadata: ObjectMeta {
                name: Some("kubernetes".into()),
                namespace: Some("default".into()),
                labels: Some(BTreeMap::from([
                    ("component".into(), "apiserver".into()),
                    ("provider".into(), "kubernetes".into()),
                ])),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("ClusterIP".into()),
                cluster_ip: Some("10.96.0.1".into()),
                ports: Some(vec![ServicePort {
                    name: Some("https".into()),
                    port: 443,
                    target_port: Some(IntOrString::Int(6443)),
                    protocol: Some("TCP".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            status: None,
        };
        store.create(svc_ref, &serde_json::to_value(&svc)?)?;
        tracing::info!("bootstrapped 'kubernetes' service");
    }

    // Bootstrap endpoints for the kubernetes service
    let ep_gvr = GroupVersionResource::endpoints();
    let ep_ref = ResourceRef {
        gvr: &ep_gvr,
        namespace: Some("default"),
        name: "kubernetes",
    };
    if store.get(&ep_ref)?.is_none() {
        let ep = Endpoints {
            metadata: ObjectMeta {
                name: Some("kubernetes".into()),
                namespace: Some("default".into()),
                ..Default::default()
            },
            subsets: Some(vec![EndpointSubset {
                addresses: Some(vec![EndpointAddress {
                    ip: "10.244.0.1".into(),
                    target_ref: None,
                    ..Default::default()
                }]),
                ports: Some(vec![EndpointPort {
                    port: 6443,
                    protocol: Some("TCP".into()),
                    name: Some("https".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            }]),
        };
        store.create(ep_ref, &serde_json::to_value(&ep)?)?;
        tracing::info!("bootstrapped 'kubernetes' endpoints");
    }

    Ok(())
}

pub fn bootstrap_ingress_class(store: &Store) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::ingress_classes();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: None,
        name: "r8s",
    };
    if store.get(&rref)?.is_none() {
        let ic = IngressClass {
            metadata: ObjectMeta {
                name: Some("r8s".into()),
                annotations: Some(BTreeMap::from([(
                    "ingressclass.kubernetes.io/is-default-class".into(),
                    "true".into(),
                )])),
                ..Default::default()
            },
            spec: Some(IngressClassSpec {
                controller: Some("r8s.dev/ingress-controller".into()),
                ..Default::default()
            }),
        };
        store.create(rref, &serde_json::to_value(&ic)?)?;
        tracing::info!("bootstrapped 'r8s' IngressClass");
    }
    Ok(())
}
