use std::sync::{Arc, RwLock};

use rustc_hash::FxHashMap;

use crate::{GroupVersionResource, ResourceType};

struct RegistryInner {
    by_gvr: FxHashMap<String, Arc<ResourceType>>,
    by_resource: FxHashMap<String, Arc<ResourceType>>,
}

/// Thread-safe registry of known resource types.
///
/// Uses interior mutability so the CRD controller can register new types
/// at runtime while the API server reads concurrently.
#[derive(Clone)]
pub struct ResourceRegistry {
    inner: Arc<RwLock<RegistryInner>>,
}

impl Default for ResourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(RegistryInner {
                by_gvr: FxHashMap::default(),
                by_resource: FxHashMap::default(),
            })),
        }
    }

    pub fn register(&self, rt: ResourceType) {
        let rt = Arc::new(rt);
        let mut inner = self.inner.write().unwrap();
        inner.by_gvr.insert(rt.gvr.key_prefix(), Arc::clone(&rt));
        inner.by_resource.insert(rt.gvr.resource.clone(), rt);
    }

    pub fn unregister(&self, gvr: &GroupVersionResource) {
        let mut inner = self.inner.write().unwrap();
        inner.by_gvr.remove(&gvr.key_prefix());
        inner.by_resource.remove(&gvr.resource);
    }

    pub fn get_by_gvr(&self, gvr: &GroupVersionResource) -> Option<Arc<ResourceType>> {
        let inner = self.inner.read().unwrap();
        inner.by_gvr.get(&gvr.key_prefix()).cloned()
    }

    pub fn get_by_resource(&self, resource: &str) -> Option<Arc<ResourceType>> {
        let inner = self.inner.read().unwrap();
        inner.by_resource.get(resource).cloned()
    }

    pub fn iter(&self) -> Vec<Arc<ResourceType>> {
        let inner = self.inner.read().unwrap();
        inner.by_gvr.values().cloned().collect()
    }

    pub fn resources_for_group_version(
        &self,
        group: &str,
        version: &str,
    ) -> Vec<Arc<ResourceType>> {
        let inner = self.inner.read().unwrap();
        inner
            .by_gvr
            .values()
            .filter(|rt| rt.gvr.group == group && rt.gvr.version == version)
            .cloned()
            .collect()
    }

    pub fn default_mvp() -> Self {
        let r = Self::new();

        // Core/v1
        for (resource, kind, namespaced, singular, short) in [
            ("pods", "Pod", true, "pod", vec!["po"]),
            ("namespaces", "Namespace", false, "namespace", vec!["ns"]),
            ("configmaps", "ConfigMap", true, "configmap", vec!["cm"]),
            ("secrets", "Secret", true, "secret", vec![]),
            ("services", "Service", true, "service", vec!["svc"]),
            (
                "serviceaccounts",
                "ServiceAccount",
                true,
                "serviceaccount",
                vec!["sa"],
            ),
            ("endpoints", "Endpoints", true, "endpoints", vec!["ep"]),
            ("nodes", "Node", false, "node", vec!["no"]),
            ("events", "Event", true, "event", vec!["ev"]),
        ] {
            r.register(ResourceType {
                gvr: GroupVersionResource::new("", "v1", resource),
                kind: kind.to_string(),
                namespaced,
                singular: singular.to_string(),
                short_names: short.into_iter().map(String::from).collect(),
                subresources: vec![],
            });
        }

        // Apps/v1
        for (resource, kind, singular, short) in [
            ("deployments", "Deployment", "deployment", vec!["deploy"]),
            ("replicasets", "ReplicaSet", "replicaset", vec!["rs"]),
            ("statefulsets", "StatefulSet", "statefulset", vec!["sts"]),
            ("daemonsets", "DaemonSet", "daemonset", vec!["ds"]),
        ] {
            r.register(ResourceType {
                gvr: GroupVersionResource::new("apps", "v1", resource),
                kind: kind.to_string(),
                namespaced: true,
                singular: singular.to_string(),
                short_names: short.into_iter().map(String::from).collect(),
                subresources: vec![],
            });
        }

        r.register(ResourceType {
            gvr: GroupVersionResource::new("networking.k8s.io", "v1", "ingresses"),
            kind: "Ingress".to_string(),
            namespaced: true,
            short_names: vec!["ing".to_string()],
            singular: "ingress".to_string(),
            subresources: vec![],
        });
        r.register(ResourceType {
            gvr: GroupVersionResource::new("networking.k8s.io", "v1", "ingressclasses"),
            kind: "IngressClass".to_string(),
            namespaced: false,
            short_names: vec![],
            singular: "ingressclass".to_string(),
            subresources: vec![],
        });
        r.register(ResourceType {
            gvr: GroupVersionResource::new("discovery.k8s.io", "v1", "endpointslices"),
            kind: "EndpointSlice".to_string(),
            namespaced: true,
            short_names: vec![],
            singular: "endpointslice".to_string(),
            subresources: vec![],
        });

        // policy/v1
        r.register(ResourceType {
            gvr: GroupVersionResource::new("policy", "v1", "poddisruptionbudgets"),
            kind: "PodDisruptionBudget".to_string(),
            namespaced: true,
            singular: "poddisruptionbudget".to_string(),
            short_names: vec!["pdb".to_string()],
            subresources: vec![],
        });

        // Batch/v1
        for (resource, kind, singular, short) in [
            ("jobs", "Job", "job", vec![]),
            ("cronjobs", "CronJob", "cronjob", vec!["cj"]),
        ] {
            r.register(ResourceType {
                gvr: GroupVersionResource::new("batch", "v1", resource),
                kind: kind.to_string(),
                namespaced: true,
                singular: singular.to_string(),
                short_names: short.into_iter().map(String::from).collect(),
                subresources: vec![],
            });
        }

        // autoscaling/v2
        r.register(ResourceType {
            gvr: GroupVersionResource::new("autoscaling", "v2", "horizontalpodautoscalers"),
            kind: "HorizontalPodAutoscaler".to_string(),
            namespaced: true,
            singular: "horizontalpodautoscaler".to_string(),
            short_names: vec!["hpa".to_string()],
            subresources: vec![],
        });

        r.register(ResourceType {
            gvr: GroupVersionResource::new(
                "apiextensions.k8s.io",
                "v1",
                "customresourcedefinitions",
            ),
            kind: "CustomResourceDefinition".to_string(),
            namespaced: false,
            singular: "customresourcedefinition".to_string(),
            short_names: vec!["crd".to_string(), "crds".to_string()],
            subresources: vec![],
        });

        r
    }
}
