use serde::{Deserialize, Serialize};

use crate::meta::ObjectMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressClass {
    #[serde(default = "ic_api_version")]
    pub api_version: String,
    #[serde(default = "ic_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<IngressClassSpec>,
}

fn ic_api_version() -> String {
    "networking.k8s.io/v1".into()
}
fn ic_kind() -> String {
    "IngressClass".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngressClassSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller: Option<String>,
}
