use serde::{Deserialize, Serialize};

use crate::meta::{LabelSelector, ObjectMeta, PodTemplateSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSet {
    #[serde(default = "sts_api_version")]
    pub api_version: String,
    #[serde(default = "sts_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: StatefulSetSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<StatefulSetStatus>,
}

fn sts_api_version() -> String {
    "apps/v1".into()
}
fn sts_kind() -> String {
    "StatefulSet".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetSpec {
    #[serde(default = "default_one")]
    pub replicas: u64,
    #[serde(default)]
    pub selector: LabelSelector,
    #[serde(default)]
    pub template: PodTemplateSpec,
    #[serde(default)]
    pub service_name: String,
}

fn default_one() -> u64 {
    1
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetStatus {
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
    fn statefulset_roundtrip() {
        let json = r#"{"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":"postgres","namespace":"default"},"spec":{"serviceName":"postgres","replicas":1,"selector":{"matchLabels":{"app":"postgres"}},"template":{"metadata":{"labels":{"app":"postgres"}},"spec":{"containers":[{"name":"postgres","image":"postgres:16"}]}}},"status":{"replicas":1,"readyReplicas":1,"availableReplicas":1}}"#;
        let sts: StatefulSet = serde_json::from_str(json).unwrap();
        assert_eq!(sts.spec.replicas, 1);
        assert_eq!(sts.spec.service_name, "postgres");
        assert_eq!(
            sts.spec.selector.match_labels.as_ref().unwrap()["app"],
            "postgres"
        );
        let status = sts.status.as_ref().unwrap();
        assert_eq!(status.ready_replicas, 1);

        let out = serde_json::to_value(&sts).unwrap();
        assert_eq!(out["spec"]["serviceName"], "postgres");
        assert_eq!(out["status"]["readyReplicas"], 1);
    }
}
