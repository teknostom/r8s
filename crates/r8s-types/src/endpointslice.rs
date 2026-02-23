use serde::{Deserialize, Serialize};

use crate::endpoints::EndpointPort;
use crate::meta::ObjectMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointSlice {
    #[serde(default = "es_api_version")]
    pub api_version: String,
    #[serde(default = "es_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address_type: Option<String>,
    #[serde(default)]
    pub endpoints: Vec<SliceEndpoint>,
    #[serde(default)]
    pub ports: Vec<EndpointPort>,
}

fn es_api_version() -> String {
    "discovery.k8s.io/v1".into()
}
fn es_kind() -> String {
    "EndpointSlice".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceEndpoint {
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<EndpointConditions>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointConditions {
    #[serde(default)]
    pub ready: bool,
}
