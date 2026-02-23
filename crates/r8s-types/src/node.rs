use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::meta::ObjectMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    #[serde(default = "node_api_version")]
    pub api_version: String,
    #[serde(default = "node_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<NodeStatus>,
}

fn node_api_version() -> String {
    "v1".into()
}
fn node_kind() -> String {
    "Node".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<NodeCondition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_info: Option<NodeSystemInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocatable: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSystemInfo {
    pub operating_system: String,
    pub architecture: String,
    pub kubelet_version: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_roundtrip() {
        let json = r#"{"apiVersion":"v1","kind":"Node","metadata":{"name":"r8s-node","labels":{"kubernetes.io/hostname":"r8s-node"}},"status":{"conditions":[{"type":"Ready","status":"True","lastHeartbeatTime":"2025-01-01T00:00:00Z","lastTransitionTime":"2025-01-01T00:00:00Z","reason":"KubeletReady","message":"r8s node is ready"}],"nodeInfo":{"operatingSystem":"linux","architecture":"amd64","kubeletVersion":"v1.32.0-r8s"},"capacity":{"cpu":"4","memory":"8Gi","pods":"110"},"allocatable":{"cpu":"4","memory":"8Gi","pods":"110"}}}"#;
        let node: Node = serde_json::from_str(json).unwrap();
        assert_eq!(node.metadata.name.as_deref(), Some("r8s-node"));
        let status = node.status.as_ref().unwrap();
        assert_eq!(status.conditions[0].type_, "Ready");
        assert_eq!(status.node_info.as_ref().unwrap().kubelet_version, "v1.32.0-r8s");
        assert_eq!(status.capacity.as_ref().unwrap()["cpu"], "4");

        let out = serde_json::to_value(&node).unwrap();
        assert_eq!(out["status"]["nodeInfo"]["kubeletVersion"], "v1.32.0-r8s");
        assert_eq!(out["status"]["conditions"][0]["lastHeartbeatTime"], "2025-01-01T00:00:00Z");
    }
}
