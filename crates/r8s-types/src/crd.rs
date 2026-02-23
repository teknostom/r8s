use serde::{Deserialize, Serialize};

use crate::meta::ObjectMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomResourceDefinition {
    #[serde(default = "crd_api_version")]
    pub api_version: String,
    #[serde(default = "crd_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: CrdSpec,
}

fn crd_api_version() -> String {
    "apiextensions.k8s.io/v1".into()
}
fn crd_kind() -> String {
    "CustomResourceDefinition".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrdSpec {
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub versions: Vec<CrdVersion>,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub names: CrdNames,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrdVersion {
    pub name: String,
    #[serde(default)]
    pub served: bool,
    #[serde(default)]
    pub storage: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrdNames {
    #[serde(default)]
    pub plural: String,
    #[serde(default)]
    pub singular: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub short_names: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crd_roundtrip() {
        let json = r#"{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition","metadata":{"name":"crontabs.stable.example.com"},"spec":{"group":"stable.example.com","versions":[{"name":"v1","served":true,"storage":true}],"scope":"Namespaced","names":{"plural":"crontabs","singular":"crontab","kind":"CronTab","shortNames":["ct"]}}}"#;
        let crd: CustomResourceDefinition = serde_json::from_str(json).unwrap();
        assert_eq!(crd.spec.group, "stable.example.com");
        assert_eq!(crd.spec.names.plural, "crontabs");
        assert_eq!(crd.spec.names.kind, "CronTab");
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.versions[0].name, "v1");
        assert!(crd.spec.versions[0].served);

        let out = serde_json::to_value(&crd).unwrap();
        assert_eq!(out["spec"]["group"], "stable.example.com");
        assert_eq!(out["spec"]["names"]["shortNames"][0], "ct");
        assert_eq!(out["spec"]["scope"], "Namespaced");
    }
}
