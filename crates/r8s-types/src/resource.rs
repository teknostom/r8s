use serde::{Deserialize, Serialize};

/// Identifies an API group, version, and resource (e.g. apps/v1/deployments).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupVersionResource {
    pub group: String,
    pub version: String,
    pub resource: String,
}

impl GroupVersionResource {
    pub fn new(group: &str, version: &str, resource: &str) -> Self {
        Self {
            group: group.to_string(),
            version: version.to_string(),
            resource: resource.to_string(),
        }
    }

    /// Storage key prefix for this GVR: "{group}/{version}/{resource}"
    /// For core/v1 resources, group is empty so the key is "/v1/{resource}".
    pub fn key_prefix(&self) -> String {
        format!("{}/{}/{}", self.group, self.version, self.resource)
    }

    // Core v1
    pub fn pods() -> Self { Self::new("", "v1", "pods") }
    pub fn nodes() -> Self { Self::new("", "v1", "nodes") }
    pub fn namespaces() -> Self { Self::new("", "v1", "namespaces") }
    pub fn services() -> Self { Self::new("", "v1", "services") }
    pub fn endpoints() -> Self { Self::new("", "v1", "endpoints") }
    pub fn secrets() -> Self { Self::new("", "v1", "secrets") }
    pub fn configmaps() -> Self { Self::new("", "v1", "configmaps") }
    pub fn service_accounts() -> Self { Self::new("", "v1", "serviceaccounts") }

    // Apps v1
    pub fn deployments() -> Self { Self::new("apps", "v1", "deployments") }
    pub fn replica_sets() -> Self { Self::new("apps", "v1", "replicasets") }
    pub fn stateful_sets() -> Self { Self::new("apps", "v1", "statefulsets") }

    // Networking v1
    pub fn ingresses() -> Self { Self::new("networking.k8s.io", "v1", "ingresses") }
    pub fn ingress_classes() -> Self { Self::new("networking.k8s.io", "v1", "ingressclasses") }

    // Discovery v1
    pub fn endpoint_slices() -> Self { Self::new("discovery.k8s.io", "v1", "endpointslices") }

    // Apiextensions v1
    pub fn crds() -> Self { Self::new("apiextensions.k8s.io", "v1", "customresourcedefinitions") }
}

/// Metadata about a registered resource type.
#[derive(Debug, Clone)]
pub struct ResourceType {
    pub gvr: GroupVersionResource,
    pub kind: String,
    pub namespaced: bool,
    pub short_names: Vec<String>,
    pub singular: String,
    /// Subresources this type supports (e.g. "status", "scale").
    pub subresources: Vec<String>,
}
