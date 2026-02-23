use serde::{Deserialize, Serialize};

use crate::meta::{LabelSelector, ObjectMeta, PodTemplateSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSet {
    #[serde(default = "rs_api_version")]
    pub api_version: String,
    #[serde(default = "rs_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: ReplicaSetSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ReplicaSetStatus>,
}

fn rs_api_version() -> String {
    "apps/v1".into()
}
fn rs_kind() -> String {
    "ReplicaSet".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSetSpec {
    #[serde(default = "default_one")]
    pub replicas: u64,
    #[serde(default)]
    pub selector: LabelSelector,
    #[serde(default)]
    pub template: PodTemplateSpec,
}

fn default_one() -> u64 {
    1
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaSetStatus {
    #[serde(default)]
    pub replicas: u64,
    #[serde(default)]
    pub ready_replicas: u64,
    #[serde(default)]
    pub available_replicas: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicaset_roundtrip() {
        let json = r#"{"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"nginx-abc","namespace":"default","ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"nginx","uid":"deploy-uid","controller":true}]},"spec":{"replicas":2,"selector":{"matchLabels":{"app":"nginx"}},"template":{"metadata":{"labels":{"app":"nginx"}},"spec":{"containers":[{"name":"nginx","image":"nginx:latest"}]}}},"status":{"replicas":2,"readyReplicas":2,"availableReplicas":2}}"#;
        let rs: ReplicaSet = serde_json::from_str(json).unwrap();
        assert_eq!(rs.spec.replicas, 2);
        assert_eq!(rs.metadata.owner_references[0].kind, "Deployment");
        let status = rs.status.as_ref().unwrap();
        assert_eq!(status.available_replicas, 2);

        let out = serde_json::to_value(&rs).unwrap();
        assert_eq!(out["status"]["readyReplicas"], 2);
        assert_eq!(out["metadata"]["ownerReferences"][0]["blockOwnerDeletion"].as_bool(), None);
    }
}
