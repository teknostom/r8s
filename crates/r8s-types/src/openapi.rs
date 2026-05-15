//! Vendored upstream Kubernetes OpenAPI v3 schemas, matching the k8s version
//! pinned by k8s_openapi (currently v1.32). Used to attach a schema to each
//! built-in ResourceType so the API server can serve /openapi/v3 and validate
//! incoming objects at admission time.

use rustc_hash::FxHashMap;
use std::sync::OnceLock;

/// Spec files vendored from kubernetes/kubernetes@release-1.32, paired with
/// the (group, version) they describe so /openapi/v3 handlers can find them.
/// One entry per group/version that r8s exposes a built-in resource for.
pub const SPEC_FILES: &[(&str, &str, &[u8])] = &[
    (
        "",
        "v1",
        include_bytes!("../openapi/v3/api__v1_openapi.json"),
    ),
    (
        "apps",
        "v1",
        include_bytes!("../openapi/v3/apis__apps__v1_openapi.json"),
    ),
    (
        "batch",
        "v1",
        include_bytes!("../openapi/v3/apis__batch__v1_openapi.json"),
    ),
    (
        "networking.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__networking.k8s.io__v1_openapi.json"),
    ),
    (
        "discovery.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__discovery.k8s.io__v1_openapi.json"),
    ),
    (
        "policy",
        "v1",
        include_bytes!("../openapi/v3/apis__policy__v1_openapi.json"),
    ),
    (
        "autoscaling",
        "v2",
        include_bytes!("../openapi/v3/apis__autoscaling__v2_openapi.json"),
    ),
    (
        "apiextensions.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__apiextensions.k8s.io__v1_openapi.json"),
    ),
    (
        "rbac.authorization.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__rbac.authorization.k8s.io__v1_openapi.json"),
    ),
];

/// Returns the raw vendored OpenAPI v3 spec bytes for a built-in (group, version),
/// or `None` if r8s has no vendored spec for that GV (e.g. a CRD group).
pub fn spec_bytes_for(group: &str, version: &str) -> Option<&'static [u8]> {
    SPEC_FILES
        .iter()
        .find(|(g, v, _)| *g == group && *v == version)
        .map(|(_, _, b)| *b)
}

/// Vendored OpenAPI v2 (Swagger 2.0) spec covering every built-in resource in
/// this k8s version. CRD schemas are merged into a clone of this at request
/// time by the API server.
pub const SWAGGER_V2_JSON: &[u8] = include_bytes!("../openapi/v2/swagger.json");

/// Key: (group, version, kind). Empty string for the core group.
type GvkKey = (String, String, String);

static INDEX: OnceLock<FxHashMap<GvkKey, serde_json::Value>> = OnceLock::new();

fn build_index() -> FxHashMap<GvkKey, serde_json::Value> {
    let mut index = FxHashMap::default();
    for &(_, _, bytes) in SPEC_FILES {
        let spec: serde_json::Value = match serde_json::from_slice(bytes) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let schemas = match spec
            .get("components")
            .and_then(|c| c.get("schemas"))
            .and_then(|s| s.as_object())
        {
            Some(m) => m,
            None => continue,
        };
        for (_name, schema) in schemas {
            let gvks = match schema
                .get("x-kubernetes-group-version-kind")
                .and_then(|v| v.as_array())
            {
                Some(a) => a,
                None => continue,
            };
            for gvk in gvks {
                let group = gvk
                    .get("group")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let version = match gvk.get("version").and_then(|v| v.as_str()) {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                let kind = match gvk.get("kind").and_then(|v| v.as_str()) {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                index.insert((group, version, kind), schema.clone());
            }
        }
    }
    index
}

/// Look up the OpenAPI v3 schema for a built-in resource by GVK. Returns
/// `None` for kinds we don't have a vendored spec for (e.g. CRDs).
pub fn schema_for(group: &str, version: &str, kind: &str) -> Option<serde_json::Value> {
    let index = INDEX.get_or_init(build_index);
    index
        .get(&(group.to_string(), version.to_string(), kind.to_string()))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_core_pod() {
        let s = schema_for("", "v1", "Pod").expect("Pod schema present");
        assert!(s.get("properties").is_some());
    }

    #[test]
    fn finds_apps_deployment() {
        let s = schema_for("apps", "v1", "Deployment").expect("Deployment schema present");
        assert!(s.get("properties").is_some());
    }

    #[test]
    fn finds_crd_type() {
        let s = schema_for("apiextensions.k8s.io", "v1", "CustomResourceDefinition")
            .expect("CRD schema present");
        assert!(s.get("properties").is_some());
    }

    #[test]
    fn missing_returns_none() {
        assert!(schema_for("nope", "v1", "Nothing").is_none());
    }
}
