use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::meta::ObjectMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Service {
    #[serde(default = "svc_api_version")]
    pub api_version: String,
    #[serde(default = "svc_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: ServiceSpec,
}

fn svc_api_version() -> String {
    "v1".into()
}
fn svc_kind() -> String {
    "Service".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSpec {
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    #[serde(default, rename = "clusterIP", skip_serializing_if = "Option::is_none")]
    pub cluster_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<ServicePort>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServicePort {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub port: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_port: Option<u32>,
    #[serde(default = "default_protocol")]
    pub protocol: String,
}

fn default_protocol() -> String {
    "TCP".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_roundtrip() {
        let json = r#"{"apiVersion":"v1","kind":"Service","metadata":{"name":"nginx","namespace":"default"},"spec":{"type":"ClusterIP","clusterIP":"10.96.0.10","selector":{"app":"nginx"},"ports":[{"port":80,"targetPort":8080,"protocol":"TCP"}]}}"#;
        let svc: Service = serde_json::from_str(json).unwrap();
        assert_eq!(svc.spec.type_.as_deref(), Some("ClusterIP"));
        assert_eq!(svc.spec.cluster_ip.as_deref(), Some("10.96.0.10"));
        assert_eq!(svc.spec.selector.as_ref().unwrap()["app"], "nginx");
        assert_eq!(svc.spec.ports[0].port, 80);
        assert_eq!(svc.spec.ports[0].target_port, Some(8080));

        let out = serde_json::to_value(&svc).unwrap();
        assert_eq!(out["spec"]["type"], "ClusterIP");
        assert_eq!(out["spec"]["clusterIP"], "10.96.0.10");
    }
}
