use serde::{Deserialize, Serialize};

use crate::meta::ObjectMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ingress {
    #[serde(default = "ing_api_version")]
    pub api_version: String,
    #[serde(default = "ing_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: IngressSpec,
}

fn ing_api_version() -> String {
    "networking.k8s.io/v1".into()
}
fn ing_kind() -> String {
    "Ingress".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress_class_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<IngressRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngressRule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpIngressRuleValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpIngressRuleValue {
    #[serde(default)]
    pub paths: Vec<HttpIngressPath>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpIngressPath {
    #[serde(default = "default_slash")]
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_type: Option<String>,
    pub backend: IngressBackend,
}

fn default_slash() -> String {
    "/".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngressBackend {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<IngressServiceBackend>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngressServiceBackend {
    pub name: String,
    pub port: ServiceBackendPort,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceBackendPort {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub number: Option<u16>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingress_roundtrip() {
        let json = r#"{"apiVersion":"networking.k8s.io/v1","kind":"Ingress","metadata":{"name":"nginx","namespace":"default"},"spec":{"ingressClassName":"r8s","rules":[{"host":"nginx.local","http":{"paths":[{"path":"/","pathType":"Prefix","backend":{"service":{"name":"nginx","port":{"number":80}}}}]}}]}}"#;
        let ing: Ingress = serde_json::from_str(json).unwrap();
        assert_eq!(ing.spec.ingress_class_name.as_deref(), Some("r8s"));
        let rule = &ing.spec.rules[0];
        assert_eq!(rule.host.as_deref(), Some("nginx.local"));
        let path = &rule.http.as_ref().unwrap().paths[0];
        assert_eq!(path.path, "/");
        assert_eq!(path.path_type.as_deref(), Some("Prefix"));
        let svc = path.backend.service.as_ref().unwrap();
        assert_eq!(svc.name, "nginx");
        assert_eq!(svc.port.number, Some(80));

        let out = serde_json::to_value(&ing).unwrap();
        assert_eq!(out["spec"]["ingressClassName"], "r8s");
        assert_eq!(out["spec"]["rules"][0]["http"]["paths"][0]["pathType"], "Prefix");
    }
}
