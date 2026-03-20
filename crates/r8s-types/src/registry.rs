use std::sync::{Arc, RwLock};

use rustc_hash::FxHashMap;

use crate::{GroupVersionResource, ResourceType};

struct RegistryInner {
    by_gvr: FxHashMap<String, Arc<ResourceType>>,
    by_resource: FxHashMap<String, Arc<ResourceType>>,
}

/// Interior mutability lets the CRD controller register types at runtime
/// while the API server reads concurrently.
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
        let mut inner = self.inner.write().expect("registry lock poisoned");
        inner.by_gvr.insert(rt.gvr.key_prefix(), Arc::clone(&rt));
        inner.by_resource.insert(rt.gvr.resource.clone(), rt);
    }

    pub fn unregister(&self, gvr: &GroupVersionResource) {
        let mut inner = self.inner.write().expect("registry lock poisoned");
        inner.by_gvr.remove(&gvr.key_prefix());
        inner.by_resource.remove(&gvr.resource);
    }

    pub fn get_by_gvr(&self, gvr: &GroupVersionResource) -> Option<Arc<ResourceType>> {
        let inner = self.inner.read().expect("registry lock poisoned");
        inner.by_gvr.get(&gvr.key_prefix()).cloned()
    }

    pub fn get_by_resource(&self, resource: &str) -> Option<Arc<ResourceType>> {
        let inner = self.inner.read().expect("registry lock poisoned");
        inner.by_resource.get(resource).cloned()
    }

    pub fn iter(&self) -> Vec<Arc<ResourceType>> {
        let inner = self.inner.read().expect("registry lock poisoned");
        inner.by_gvr.values().cloned().collect()
    }

    pub fn resources_for_group_version(
        &self,
        group: &str,
        version: &str,
    ) -> Vec<Arc<ResourceType>> {
        let inner = self.inner.read().expect("registry lock poisoned");
        inner
            .by_gvr
            .values()
            .filter(|rt| rt.gvr.group == group && rt.gvr.version == version)
            .cloned()
            .collect()
    }

    #[allow(clippy::type_complexity)]
    pub fn default_mvp() -> Self {
        let r = Self::new();

        let types: &[(&str, &str, &str, &str, bool, &str, &[&str])] = &[
            ("", "v1", "pods", "Pod", true, "pod", &["po"]),
            (
                "",
                "v1",
                "namespaces",
                "Namespace",
                false,
                "namespace",
                &["ns"],
            ),
            (
                "",
                "v1",
                "configmaps",
                "ConfigMap",
                true,
                "configmap",
                &["cm"],
            ),
            ("", "v1", "secrets", "Secret", true, "secret", &[]),
            ("", "v1", "services", "Service", true, "service", &["svc"]),
            (
                "",
                "v1",
                "serviceaccounts",
                "ServiceAccount",
                true,
                "serviceaccount",
                &["sa"],
            ),
            (
                "",
                "v1",
                "endpoints",
                "Endpoints",
                true,
                "endpoints",
                &["ep"],
            ),
            ("", "v1", "nodes", "Node", false, "node", &["no"]),
            ("", "v1", "events", "Event", true, "event", &["ev"]),
            (
                "apps",
                "v1",
                "deployments",
                "Deployment",
                true,
                "deployment",
                &["deploy"],
            ),
            (
                "apps",
                "v1",
                "replicasets",
                "ReplicaSet",
                true,
                "replicaset",
                &["rs"],
            ),
            (
                "apps",
                "v1",
                "statefulsets",
                "StatefulSet",
                true,
                "statefulset",
                &["sts"],
            ),
            (
                "apps",
                "v1",
                "daemonsets",
                "DaemonSet",
                true,
                "daemonset",
                &["ds"],
            ),
            (
                "networking.k8s.io",
                "v1",
                "ingresses",
                "Ingress",
                true,
                "ingress",
                &["ing"],
            ),
            (
                "networking.k8s.io",
                "v1",
                "ingressclasses",
                "IngressClass",
                false,
                "ingressclass",
                &[],
            ),
            (
                "discovery.k8s.io",
                "v1",
                "endpointslices",
                "EndpointSlice",
                true,
                "endpointslice",
                &[],
            ),
            (
                "policy",
                "v1",
                "poddisruptionbudgets",
                "PodDisruptionBudget",
                true,
                "poddisruptionbudget",
                &["pdb"],
            ),
            ("batch", "v1", "jobs", "Job", true, "job", &[]),
            (
                "batch",
                "v1",
                "cronjobs",
                "CronJob",
                true,
                "cronjob",
                &["cj"],
            ),
            (
                "autoscaling",
                "v2",
                "horizontalpodautoscalers",
                "HorizontalPodAutoscaler",
                true,
                "horizontalpodautoscaler",
                &["hpa"],
            ),
            (
                "apiextensions.k8s.io",
                "v1",
                "customresourcedefinitions",
                "CustomResourceDefinition",
                false,
                "customresourcedefinition",
                &["crd", "crds"],
            ),
        ];

        for &(group, version, resource, kind, namespaced, singular, short_names) in types {
            r.register(ResourceType {
                gvr: GroupVersionResource::new(group, version, resource),
                kind: kind.to_string(),
                namespaced,
                singular: singular.to_string(),
                short_names: short_names.iter().map(|s| s.to_string()).collect(),
                subresources: vec![],
            });
        }

        r
    }
}
