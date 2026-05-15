use serde::{Deserialize, Serialize};

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

    pub fn key_prefix(&self) -> String {
        format!("{}/{}/{}", self.group, self.version, self.resource)
    }

    pub fn pods() -> Self {
        Self::new("", "v1", "pods")
    }
    pub fn nodes() -> Self {
        Self::new("", "v1", "nodes")
    }
    pub fn namespaces() -> Self {
        Self::new("", "v1", "namespaces")
    }
    pub fn services() -> Self {
        Self::new("", "v1", "services")
    }
    pub fn endpoints() -> Self {
        Self::new("", "v1", "endpoints")
    }
    pub fn secrets() -> Self {
        Self::new("", "v1", "secrets")
    }
    pub fn configmaps() -> Self {
        Self::new("", "v1", "configmaps")
    }
    pub fn service_accounts() -> Self {
        Self::new("", "v1", "serviceaccounts")
    }

    pub fn deployments() -> Self {
        Self::new("apps", "v1", "deployments")
    }
    pub fn replica_sets() -> Self {
        Self::new("apps", "v1", "replicasets")
    }
    pub fn stateful_sets() -> Self {
        Self::new("apps", "v1", "statefulsets")
    }
    pub fn daemon_sets() -> Self {
        Self::new("apps", "v1", "daemonsets")
    }

    pub fn ingresses() -> Self {
        Self::new("networking.k8s.io", "v1", "ingresses")
    }
    pub fn ingress_classes() -> Self {
        Self::new("networking.k8s.io", "v1", "ingressclasses")
    }

    pub fn endpoint_slices() -> Self {
        Self::new("discovery.k8s.io", "v1", "endpointslices")
    }

    pub fn jobs() -> Self {
        Self::new("batch", "v1", "jobs")
    }
    pub fn cron_jobs() -> Self {
        Self::new("batch", "v1", "cronjobs")
    }

    pub fn crds() -> Self {
        Self::new("apiextensions.k8s.io", "v1", "customresourcedefinitions")
    }
}

#[derive(Debug, Clone)]
pub struct ResourceType {
    pub gvr: GroupVersionResource,
    pub kind: String,
    pub namespaced: bool,
    pub short_names: Vec<String>,
    pub singular: String,
    pub subresources: Vec<String>,
    /// OpenAPI v3 schema for this resource's objects. Populated for CRDs from
    /// their definition, and for built-in types from vendored upstream specs.
    /// `None` means the API server admits objects of this kind without schema
    /// validation and doesn't expose a schema in /openapi/v3.
    pub schema: Option<serde_json::Value>,
}
