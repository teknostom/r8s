use serde::{Deserialize, Serialize};

use crate::meta::ObjectMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Namespace {
    #[serde(default = "ns_api_version")]
    pub api_version: String,
    #[serde(default = "ns_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<NamespaceStatus>,
}

fn ns_api_version() -> String {
    "v1".into()
}
fn ns_kind() -> String {
    "Namespace".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamespaceStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
}
