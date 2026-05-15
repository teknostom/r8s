//! Prost-generated bindings for gnostic's `OpenAPIv2.proto` (Swagger 2.0),
//! plus a converter from `serde_json::Value` (the shape the rest of r8s
//! works with) into the generated `Document` message.
//!
//! The converter handles the subset of Swagger 2.0 that Kubernetes actually
//! emits — anything outside that subset is silently dropped. In particular:
//!
//! - `paths`, `parameters`, `responses`, `security`, `tags`, `externalDocs`
//!   are intentionally skipped (k8s clients use /apis discovery to map kinds
//!   and don't need them for validation).
//! - `x-*` keys at every level are preserved as `vendor_extension` entries
//!   with YAML-encoded values; `x-kubernetes-group-version-kind` is what
//!   lets clients find the right schema for a given GVK.

pub mod openapi {
    pub mod v2 {
        // Generated code: some prost-emitted enums have a large size difference
        // between variants. Boxing them would mean fork-maintaining the proto
        // bindings, so just silence the lint here.
        #![allow(clippy::large_enum_variant)]
        include!(concat!(env!("OUT_DIR"), "/openapi.v2.rs"));
    }
}

use openapi::v2 as pb;
use serde_json::{Map, Value};

/// Convert a Swagger 2.0 JSON document into the prost-generated `Document`.
pub fn json_to_document(v: &Value) -> pb::Document {
    let obj = v.as_object();
    pb::Document {
        swagger: str_field(obj, "swagger", "2.0"),
        info: obj.and_then(|o| o.get("info")).map(json_to_info),
        definitions: obj
            .and_then(|o| o.get("definitions"))
            .and_then(|d| d.as_object())
            .map(json_to_definitions),
        vendor_extension: obj.map(collect_vendor_extensions).unwrap_or_default(),
        ..Default::default()
    }
}

fn json_to_info(v: &Value) -> pb::Info {
    let obj = v.as_object();
    pb::Info {
        title: str_field(obj, "title", ""),
        version: str_field(obj, "version", ""),
        description: str_field(obj, "description", ""),
        terms_of_service: str_field(obj, "termsOfService", ""),
        ..Default::default()
    }
}

fn json_to_definitions(map: &Map<String, Value>) -> pb::Definitions {
    let mut entries = Vec::with_capacity(map.len());
    for (name, schema_v) in map {
        entries.push(pb::NamedSchema {
            name: name.clone(),
            value: Some(json_to_schema(schema_v)),
        });
    }
    pb::Definitions {
        additional_properties: entries,
    }
}

fn json_to_schema(v: &Value) -> pb::Schema {
    let Some(obj) = v.as_object() else {
        return pb::Schema::default();
    };
    pb::Schema {
        r#ref: str_field(Some(obj), "$ref", ""),
        format: str_field(Some(obj), "format", ""),
        title: str_field(Some(obj), "title", ""),
        description: str_field(Some(obj), "description", ""),
        pattern: str_field(Some(obj), "pattern", ""),
        discriminator: str_field(Some(obj), "discriminator", ""),
        read_only: bool_field(Some(obj), "readOnly"),
        exclusive_maximum: bool_field(Some(obj), "exclusiveMaximum"),
        exclusive_minimum: bool_field(Some(obj), "exclusiveMinimum"),
        unique_items: bool_field(Some(obj), "uniqueItems"),
        multiple_of: f64_field(Some(obj), "multipleOf"),
        maximum: f64_field(Some(obj), "maximum"),
        minimum: f64_field(Some(obj), "minimum"),
        max_length: i64_field(Some(obj), "maxLength"),
        min_length: i64_field(Some(obj), "minLength"),
        max_items: i64_field(Some(obj), "maxItems"),
        min_items: i64_field(Some(obj), "minItems"),
        max_properties: i64_field(Some(obj), "maxProperties"),
        min_properties: i64_field(Some(obj), "minProperties"),
        required: obj
            .get("required")
            .and_then(|x| x.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        r#type: obj.get("type").map(json_to_type_item),
        items: obj.get("items").map(json_to_items_item),
        properties: obj
            .get("properties")
            .and_then(|p| p.as_object())
            .map(json_to_properties),
        additional_properties: obj
            .get("additionalProperties")
            .and_then(json_to_additional_properties),
        all_of: obj
            .get("allOf")
            .and_then(|x| x.as_array())
            .map(|a| a.iter().map(json_to_schema).collect())
            .unwrap_or_default(),
        r#enum: obj
            .get("enum")
            .and_then(|x| x.as_array())
            .map(|a| a.iter().map(json_to_any).collect())
            .unwrap_or_default(),
        default: obj.get("default").map(json_to_any),
        vendor_extension: collect_vendor_extensions(obj),
        ..Default::default()
    }
}

fn json_to_type_item(v: &Value) -> pb::TypeItem {
    let value = match v {
        Value::String(s) => vec![s.clone()],
        Value::Array(arr) => arr
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        _ => vec![],
    };
    pb::TypeItem { value }
}

fn json_to_items_item(v: &Value) -> pb::ItemsItem {
    let schemas = match v {
        Value::Object(_) => vec![json_to_schema(v)],
        Value::Array(arr) => arr.iter().map(json_to_schema).collect(),
        _ => vec![],
    };
    pb::ItemsItem { schema: schemas }
}

fn json_to_properties(map: &Map<String, Value>) -> pb::Properties {
    let mut entries = Vec::with_capacity(map.len());
    for (name, schema_v) in map {
        entries.push(pb::NamedSchema {
            name: name.clone(),
            value: Some(json_to_schema(schema_v)),
        });
    }
    pb::Properties {
        additional_properties: entries,
    }
}

fn json_to_additional_properties(v: &Value) -> Option<Box<pb::AdditionalPropertiesItem>> {
    use pb::additional_properties_item::Oneof;
    let oneof = match v {
        Value::Bool(b) => Some(Oneof::Boolean(*b)),
        Value::Object(_) => Some(Oneof::Schema(Box::new(json_to_schema(v)))),
        _ => return None,
    };
    Some(Box::new(pb::AdditionalPropertiesItem { oneof }))
}

/// Wraps an arbitrary JSON value as an `Any`. gnostic represents unstructured
/// payloads as YAML strings; serialized JSON is valid YAML.
fn json_to_any(v: &Value) -> pb::Any {
    pb::Any {
        value: None,
        yaml: serde_json::to_string(v).unwrap_or_default(),
    }
}

/// Collect every `x-*` key in this object as a NamedAny.
fn collect_vendor_extensions(obj: &Map<String, Value>) -> Vec<pb::NamedAny> {
    obj.iter()
        .filter(|(k, _)| k.starts_with("x-"))
        .map(|(k, v)| pb::NamedAny {
            name: k.clone(),
            value: Some(json_to_any(v)),
        })
        .collect()
}

fn str_field(obj: Option<&Map<String, Value>>, key: &str, default: &str) -> String {
    obj.and_then(|o| o.get(key))
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
}

fn bool_field(obj: Option<&Map<String, Value>>, key: &str) -> bool {
    obj.and_then(|o| o.get(key))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

fn f64_field(obj: Option<&Map<String, Value>>, key: &str) -> f64 {
    obj.and_then(|o| o.get(key))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0)
}

fn i64_field(obj: Option<&Map<String, Value>>, key: &str) -> i64 {
    obj.and_then(|o| o.get(key))
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn empty_doc_encodes() {
        let v = serde_json::json!({"swagger": "2.0", "info": {"title": "t", "version": "v"}});
        let doc = json_to_document(&v);
        assert_eq!(doc.swagger, "2.0");
        let mut buf = Vec::new();
        doc.encode(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn schema_roundtrips_basics() {
        let v = serde_json::json!({
            "swagger": "2.0",
            "info": {"title": "t", "version": "v"},
            "definitions": {
                "Pet": {
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": {"type": "string"},
                        "age": {"type": "integer", "format": "int32"},
                    },
                },
            },
        });
        let doc = json_to_document(&v);
        let defs = doc.definitions.as_ref().expect("definitions present");
        assert_eq!(defs.additional_properties.len(), 1);
        let pet = &defs.additional_properties[0];
        assert_eq!(pet.name, "Pet");
        let pet_schema = pet.value.as_ref().expect("pet schema");
        assert_eq!(pet_schema.r#type.as_ref().unwrap().value, vec!["object"]);
        assert_eq!(pet_schema.required, vec!["name"]);
        let props = pet_schema.properties.as_ref().expect("props");
        assert_eq!(props.additional_properties.len(), 2);
    }

    #[test]
    fn vendor_extensions_preserved() {
        let v = serde_json::json!({
            "definitions": {
                "Pod": {
                    "type": "object",
                    "x-kubernetes-group-version-kind": [
                        {"group": "", "version": "v1", "kind": "Pod"},
                    ],
                },
            },
        });
        let doc = json_to_document(&v);
        let pod = &doc.definitions.as_ref().unwrap().additional_properties[0];
        let ext = &pod.value.as_ref().unwrap().vendor_extension;
        assert_eq!(ext.len(), 1);
        assert_eq!(ext[0].name, "x-kubernetes-group-version-kind");
    }

    #[test]
    fn full_swagger_encodes_without_error() {
        let bytes = crate::openapi::SWAGGER_V2_JSON;
        let v: Value = serde_json::from_slice(bytes).unwrap();
        let doc = json_to_document(&v);
        let mut buf = Vec::new();
        doc.encode(&mut buf).expect("vendored swagger encodes");
        assert!(buf.len() > 100_000); // sanity: big doc → big proto
    }
}
