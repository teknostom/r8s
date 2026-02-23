use serde::{Deserialize, Serialize};

use crate::meta::ObjectMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccount {
    #[serde(default = "sa_api_version")]
    pub api_version: String,
    #[serde(default = "sa_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
}

fn sa_api_version() -> String {
    "v1".into()
}
fn sa_kind() -> String {
    "ServiceAccount".into()
}
