//! Vendored Kubernetes OpenAPI schemas (k8s v1.32): per-GV v3 docs attached to
//! built-in ResourceTypes for /openapi/v3 and admission validation, and the
//! Swagger 2.0 doc covering every built-in group for /openapi/v2 and
//! server-side apply list semantics.

use rustc_hash::FxHashMap;
use serde_json::Value;
use std::sync::OnceLock;

/// Vendored spec files, keyed by the (group, version) they describe.
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
    (
        "flowcontrol.apiserver.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__flowcontrol.apiserver.k8s.io__v1_openapi.json"),
    ),
    (
        "apiregistration.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__apiregistration.k8s.io__v1_openapi.json"),
    ),
    (
        "coordination.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__coordination.k8s.io__v1_openapi.json"),
    ),
    (
        "scheduling.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__scheduling.k8s.io__v1_openapi.json"),
    ),
    (
        "node.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__node.k8s.io__v1_openapi.json"),
    ),
    (
        "storage.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__storage.k8s.io__v1_openapi.json"),
    ),
    (
        "admissionregistration.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__admissionregistration.k8s.io__v1_openapi.json"),
    ),
    (
        "certificates.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__certificates.k8s.io__v1_openapi.json"),
    ),
    (
        "events.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__events.k8s.io__v1_openapi.json"),
    ),
    (
        "authentication.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__authentication.k8s.io__v1_openapi.json"),
    ),
    (
        "authorization.k8s.io",
        "v1",
        include_bytes!("../openapi/v3/apis__authorization.k8s.io__v1_openapi.json"),
    ),
    (
        "autoscaling",
        "v1",
        include_bytes!("../openapi/v3/apis__autoscaling__v1_openapi.json"),
    ),
];

/// Raw OpenAPI v3 spec bytes for a (group, version); `None` if not vendored.
pub fn spec_bytes_for(group: &str, version: &str) -> Option<&'static [u8]> {
    SPEC_FILES
        .iter()
        .find(|(g, v, _)| *g == group && *v == version)
        .map(|(_, _, b)| *b)
}

/// Vendored OpenAPI v2 (Swagger 2.0) spec for all built-in resources.
pub const SWAGGER_V2_JSON: &[u8] = include_bytes!("../openapi/v2/swagger.json");

static SWAGGER_V2: OnceLock<Value> = OnceLock::new();

/// Parsed Swagger 2.0 doc.
pub fn swagger_v2() -> &'static Value {
    SWAGGER_V2.get_or_init(|| {
        serde_json::from_slice(SWAGGER_V2_JSON)
            .unwrap_or_else(|e| panic!("vendored swagger.json is invalid JSON: {e}"))
    })
}

static V3_DOCS: OnceLock<Vec<Value>> = OnceLock::new();

/// Parsed per-GV v3 docs, in `SPEC_FILES` order.
fn v3_docs() -> &'static [Value] {
    V3_DOCS.get_or_init(|| {
        SPEC_FILES
            .iter()
            .map(|&(group, version, bytes)| {
                serde_json::from_slice(bytes).unwrap_or_else(|e| {
                    panic!("vendored OpenAPI spec for {group}/{version} is invalid JSON: {e}")
                })
            })
            .collect()
    })
}

/// `components.schemas` of a v3 doc.
fn v3_schemas(doc: &Value) -> Option<&serde_json::Map<String, Value>> {
    doc.get("components")?.get("schemas")?.as_object()
}

/// Key: (group, version, kind). Empty string for the core group.
type GvkKey = (String, String, String);

/// (group, version, kind) triples from a schema's `x-kubernetes-group-version-kind`.
fn gvks_of(schema: &Value) -> impl Iterator<Item = GvkKey> + '_ {
    schema
        .get("x-kubernetes-group-version-kind")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter_map(|gvk| {
            let group = gvk.get("group").and_then(|v| v.as_str()).unwrap_or("");
            let version = gvk.get("version").and_then(|v| v.as_str())?;
            let kind = gvk.get("kind").and_then(|v| v.as_str())?;
            Some((group.to_string(), version.to_string(), kind.to_string()))
        })
}

static INDEX: OnceLock<FxHashMap<GvkKey, Value>> = OnceLock::new();

fn build_index() -> FxHashMap<GvkKey, Value> {
    let mut index = FxHashMap::default();
    for doc in v3_docs() {
        let Some(schemas) = v3_schemas(doc) else {
            continue;
        };
        for schema in schemas.values() {
            for gvk in gvks_of(schema) {
                index.insert(gvk, schema.clone());
            }
        }
    }
    index
}

/// OpenAPI v3 schema for a built-in resource by GVK; `None` if not vendored.
pub fn schema_for(group: &str, version: &str, kind: &str) -> Option<Value> {
    let index = INDEX.get_or_init(build_index);
    index
        .get(&(group.to_string(), version.to_string(), kind.to_string()))
        .cloned()
}

/// Every vendored schema by definition name (`io.k8s.api.core.v1.Pod`).
struct Definitions {
    by_name: FxHashMap<&'static str, &'static Value>,
    by_gvk: FxHashMap<GvkKey, &'static str>,
}

static DEFINITIONS: OnceLock<Definitions> = OnceLock::new();

fn build_definitions() -> Definitions {
    let mut defs = Definitions {
        by_name: FxHashMap::default(),
        by_gvk: FxHashMap::default(),
    };
    let mut add = |name: &'static str, schema: &'static Value| {
        defs.by_name.insert(name, schema);
        for gvk in gvks_of(schema) {
            defs.by_gvk.insert(gvk, name);
        }
    };
    if let Some(v2) = swagger_v2().get("definitions").and_then(|d| d.as_object()) {
        for (name, schema) in v2 {
            add(name.as_str(), schema);
        }
    }
    // The v3 docs carry `default`s the v2 doc lacks; they win where vendored.
    for doc in v3_docs() {
        for (name, schema) in v3_schemas(doc).into_iter().flatten() {
            add(name.as_str(), schema);
        }
    }
    defs
}

fn definitions() -> &'static Definitions {
    DEFINITIONS.get_or_init(build_definitions)
}

/// Schema for a definition name or a `$ref` to one, in either doc style
/// (`#/definitions/<name>` or `#/components/schemas/<name>`).
pub fn definition(name_or_ref: &str) -> Option<&'static Value> {
    let name = name_or_ref.rsplit('/').next().unwrap_or(name_or_ref);
    definitions().by_name.get(name).copied()
}

/// Schema for a built-in resource by GVK, covering every group in the
/// vendored swagger (unlike `schema_for`, which needs a per-GV v3 doc).
pub fn definition_for(group: &str, version: &str, kind: &str) -> Option<&'static Value> {
    let defs = definitions();
    let name = defs
        .by_gvk
        .get(&(group.to_string(), version.to_string(), kind.to_string()))?;
    defs.by_name.get(name).copied()
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

    #[test]
    fn definition_resolves_both_ref_styles() {
        let name = "io.k8s.api.core.v1.Container";
        assert!(definition(name).is_some());
        assert!(definition(&format!("#/definitions/{name}")).is_some());
        assert!(definition(&format!("#/components/schemas/{name}")).is_some());
        assert!(definition("#/definitions/io.k8s.Nothing").is_none());
    }

    #[test]
    fn definition_for_covers_every_vendored_group() {
        assert!(
            schema_for(
                "admissionregistration.k8s.io",
                "v1",
                "ValidatingWebhookConfiguration"
            )
            .is_some()
        );
        let s = definition_for(
            "admissionregistration.k8s.io",
            "v1",
            "ValidatingWebhookConfiguration",
        )
        .expect("swagger covers admissionregistration");
        assert_eq!(
            s["properties"]["webhooks"]["x-kubernetes-list-map-keys"][0],
            "name"
        );
    }

    #[test]
    fn v3_overlay_supplies_defaults() {
        let port = definition("io.k8s.api.core.v1.ContainerPort").unwrap();
        assert_eq!(port["properties"]["protocol"]["default"], "TCP");
    }
}
