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
