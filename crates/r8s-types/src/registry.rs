use rustc_hash::FxHashMap;

use crate::{GroupVersionResource, ResourceType};

#[derive(Default)]
pub struct ResourceRegistry {
    by_gvr: FxHashMap<String, ResourceType>,
    by_resource: FxHashMap<String, ResourceType>,
}

impl ResourceRegistry {
    pub fn new() -> Self {
        Self {
            by_gvr: FxHashMap::default(),
            by_resource: FxHashMap::default(),
        }
    }

    pub fn register(&mut self, rt: ResourceType) {
        self.by_gvr.insert(rt.gvr.key_prefix(), rt.clone());
        self.by_resource.insert(rt.gvr.resource.clone(), rt);
    }

    pub fn get_by_gvr(&self, gvr: &GroupVersionResource) -> Option<&ResourceType> {
        self.by_gvr.get(&gvr.key_prefix())
    }

    pub fn get_by_resource(&self, resource: &str) -> Option<&ResourceType> {
        self.by_resource.get(resource)
    }

    pub fn iter(&self) -> impl Iterator<Item = &ResourceType> {
        self.by_gvr.values()
    }

    pub fn resources_for_group_version(&self, group: &str, version: &str) -> Vec<&ResourceType> {
        self.by_gvr
            .values()
            .filter(|rt| rt.gvr.group == group && rt.gvr.version == version)
            .collect()
    }

    pub fn default_mvp() -> Self {
        let mut r = Self::new();

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
