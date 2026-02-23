use serde::{Deserialize, Serialize};

use crate::meta::{ObjectMeta, ObjectReference};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Endpoints {
    #[serde(default = "ep_api_version")]
    pub api_version: String,
    #[serde(default = "ep_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub subsets: Vec<EndpointSubset>,
}

fn ep_api_version() -> String {
    "v1".into()
}
fn ep_kind() -> String {
    "Endpoints".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EndpointSubset {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<EndpointAddress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<EndpointPort>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointAddress {
    pub ip: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_ref: Option<ObjectReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointPort {
    pub port: u32,
    #[serde(default = "default_protocol")]
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

fn default_protocol() -> String {
    "TCP".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_roundtrip() {
        let json = r#"{"apiVersion":"v1","kind":"Endpoints","metadata":{"name":"nginx"},"subsets":[{"addresses":[{"ip":"10.0.0.2","targetRef":{"kind":"Pod","name":"nginx-abc","namespace":"default"}}],"ports":[{"port":80,"protocol":"TCP"}]}]}"#;
        let ep: Endpoints = serde_json::from_str(json).unwrap();
        assert_eq!(ep.subsets[0].addresses[0].ip, "10.0.0.2");
        assert_eq!(ep.subsets[0].ports[0].port, 80);
        let tref = ep.subsets[0].addresses[0].target_ref.as_ref().unwrap();
        assert_eq!(tref.kind.as_deref(), Some("Pod"));

        let out = serde_json::to_value(&ep).unwrap();
        assert_eq!(out["subsets"][0]["addresses"][0]["targetRef"]["kind"], "Pod");
    }
}
