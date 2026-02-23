use serde::{Deserialize, Serialize};

use crate::meta::{LabelSelector, ObjectMeta, PodTemplateSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deployment {
    #[serde(default = "deploy_api_version")]
    pub api_version: String,
    #[serde(default = "deploy_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: DeploymentSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<DeploymentStatus>,
}

fn deploy_api_version() -> String {
    "apps/v1".into()
}
fn deploy_kind() -> String {
    "Deployment".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentSpec {
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
pub struct DeploymentStatus {
    #[serde(default)]
    pub replicas: u64,
    #[serde(default)]
    pub ready_replicas: u64,
    #[serde(default)]
    pub available_replicas: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<DeploymentCondition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_roundtrip() {
        let json = r#"{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"nginx","namespace":"default"},"spec":{"replicas":3,"selector":{"matchLabels":{"app":"nginx"}},"template":{"metadata":{"labels":{"app":"nginx"}},"spec":{"containers":[{"name":"nginx","image":"nginx:latest"}]}}},"status":{"replicas":3,"readyReplicas":3,"availableReplicas":3,"conditions":[{"type":"Available","status":"True","reason":"MinimumReplicasAvailable"}]}}"#;
        let deploy: Deployment = serde_json::from_str(json).unwrap();
        assert_eq!(deploy.spec.replicas, 3);
        assert_eq!(
            deploy.spec.selector.match_labels.as_ref().unwrap()["app"],
            "nginx"
        );
        let status = deploy.status.as_ref().unwrap();
        assert_eq!(status.ready_replicas, 3);
        assert_eq!(status.conditions[0].type_, "Available");

        let out = serde_json::to_value(&deploy).unwrap();
        assert_eq!(out["spec"]["selector"]["matchLabels"]["app"], "nginx");
        assert_eq!(out["status"]["readyReplicas"], 3);
        assert_eq!(out["status"]["availableReplicas"], 3);
    }
}
