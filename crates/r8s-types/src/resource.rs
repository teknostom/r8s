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

    /// Version-agnostic store coordinate shared by every served version of a
    /// resource, so an object written through one version is readable through
    /// the others.
    pub fn storage(group: &str, resource: &str) -> Self {
        Self::new(group, "", resource)
    }

    /// `group/version`, or bare `version` for the core group.
    pub fn api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.clone()
        } else {
            format!("{}/{}", self.group, self.version)
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
    pub fn events() -> Self {
        Self::new("", "v1", "events")
    }
    pub fn resource_quotas() -> Self {
        Self::new("", "v1", "resourcequotas")
    }
    pub fn pod_templates() -> Self {
        Self::new("", "v1", "podtemplates")
    }
    pub fn persistent_volumes() -> Self {
        Self::new("", "v1", "persistentvolumes")
    }
    pub fn persistent_volume_claims() -> Self {
        Self::new("", "v1", "persistentvolumeclaims")
    }
    pub fn storage_classes() -> Self {
        Self::new("storage.k8s.io", "v1", "storageclasses")
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
    pub fn controller_revisions() -> Self {
        Self::new("apps", "v1", "controllerrevisions")
    }
    pub fn replication_controllers() -> Self {
        Self::new("", "v1", "replicationcontrollers")
    }
    pub fn pod_disruption_budgets() -> Self {
        Self::new("policy", "v1", "poddisruptionbudgets")
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
    /// OpenAPI v3 schema for validation; `None` if the kind has no schema.
    pub schema: Option<serde_json::Value>,
    /// Store coordinate shared with the other served versions of a CRD;
    /// `None` stores under `gvr`.
    pub storage_gvr: Option<GroupVersionResource>,
}

impl ResourceType {
    /// Where objects of this type live in the store.
    pub fn storage_gvr(&self) -> &GroupVersionResource {
        self.storage_gvr.as_ref().unwrap_or(&self.gvr)
    }
}
