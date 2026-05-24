//! Converters from Kubernetes protobuf-decoded messages to JSON `Value`s
//! matching the wire shape served by kube-apiserver.
//!
//! The prost-generated types don't carry the exact JSON field names k8s clients
//! expect (`openAPIV3Schema`, `x-kubernetes-preserve-unknown-fields`, etc.), so
//! each resource type that needs protobuf round-trip gets a hand-rolled walker
//! here. We start with `CustomResourceDefinition` — the only protobuf path
//! exercised by sig-api-machinery conformance today.

use prost::Message;
use serde_json::{Map, Value, json};

use crate::k8s_pb::apiextensions_v1 as pbapi;
use crate::k8s_pb::core_v1 as pbcore;
use crate::k8s_pb::meta_v1 as pbmeta;
use crate::k8s_pb::runtime as pbruntime;

/// Magic prefix every k8s protobuf body starts with (`k8s\0`).
const K8S_MAGIC: &[u8] = b"k8s\0";

/// Decode a kubernetes protobuf request body into a JSON `Value` matching the
/// wire shape kube-apiserver serves. Returns `None` if the body doesn't have
/// the k8s envelope, isn't for a known GVK, or fails to decode.
///
/// Currently handles `apiextensions.k8s.io/v1 CustomResourceDefinition` only;
/// every other GVK falls through to `None`, which callers should treat as a
/// 415 Unsupported Media Type.
pub fn decode_k8s_protobuf_to_json(body: &[u8]) -> Option<Value> {
    if !body.starts_with(K8S_MAGIC) {
        return None;
    }
    let envelope = &body[K8S_MAGIC.len()..];

    // `runtime.Unknown` envelope: field 1 = TypeMeta, field 2 = raw resource.
    // NB: `runtime.TypeMeta` has (apiVersion=1, kind=2) — opposite of the
    // namesake `meta.v1.TypeMeta` (kind=1, apiVersion=2). Decoding the
    // envelope with the wrong one silently swaps the fields.
    let type_meta_bytes = read_message_field(envelope, 1)?;
    let raw = read_message_field(envelope, 2)?;
    let type_meta = pbruntime::TypeMeta::decode(type_meta_bytes).ok()?;
    let api_version = type_meta.api_version.as_deref().unwrap_or("");
    let kind = type_meta.kind.as_deref().unwrap_or("");

    match (api_version, kind) {
        ("apiextensions.k8s.io/v1", "CustomResourceDefinition") => {
            let crd = pbapi::CustomResourceDefinition::decode(raw).ok()?;
            Some(crd_to_json(&crd, api_version, kind))
        }
        ("v1", "Secret") => {
            let secret = pbcore::Secret::decode(raw).ok()?;
            Some(secret_to_json(&secret, api_version, kind))
        }
        _ => None,
    }
}

// ─── Secret walker ──────────────────────────────────────────────────────────
//
// Helm 3 stores its release info as a Secret via the typed client, which
// serializes as protobuf. Decoder must preserve `data` losslessly (it carries
// the gzip'd chart) — the old metadata-only path silently dropped it and
// broke `helm uninstall`.

fn secret_to_json(s: &pbcore::Secret, api_version: &str, kind: &str) -> Value {
    let mut obj = Map::new();
    obj.insert("apiVersion".into(), json!(api_version));
    obj.insert("kind".into(), json!(kind));
    if let Some(meta) = s.metadata.as_ref() {
        obj.insert("metadata".into(), object_meta_to_json(meta));
    }
    if let Some(t) = s.r#type.as_deref() {
        obj.insert("type".into(), json!(t));
    }
    if let Some(im) = s.immutable {
        obj.insert("immutable".into(), json!(im));
    }
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    if !s.data.is_empty() {
        let mut data = Map::new();
        for (k, v) in &s.data {
            data.insert(k.clone(), json!(b64.encode(v)));
        }
        obj.insert("data".into(), Value::Object(data));
    }
    // `stringData` is a write-side convenience: per the k8s API contract the
    // server folds each entry into `data` (base64-encoding the UTF-8 bytes)
    // and the persisted object only has `data`. Mirror that here so callers
    // see a single canonical shape.
    if !s.string_data.is_empty() {
        let data_entry = obj.entry("data".to_string()).or_insert_with(|| {
            Value::Object(Map::new())
        });
        if let Some(data_obj) = data_entry.as_object_mut() {
            for (k, v) in &s.string_data {
                data_obj.insert(k.clone(), json!(b64.encode(v.as_bytes())));
            }
        }
    }
    Value::Object(obj)
}

// ─── CRD walker ─────────────────────────────────────────────────────────────

fn crd_to_json(crd: &pbapi::CustomResourceDefinition, api_version: &str, kind: &str) -> Value {
    let mut obj = Map::new();
    obj.insert("apiVersion".into(), json!(api_version));
    obj.insert("kind".into(), json!(kind));
    if let Some(meta) = crd.metadata.as_ref() {
        obj.insert("metadata".into(), object_meta_to_json(meta));
    }
    if let Some(spec) = crd.spec.as_ref() {
        obj.insert("spec".into(), crd_spec_to_json(spec));
    }
    if let Some(status) = crd.status.as_ref() {
        obj.insert("status".into(), crd_status_to_json(status));
    }
    Value::Object(obj)
}

fn crd_spec_to_json(spec: &pbapi::CustomResourceDefinitionSpec) -> Value {
    let mut obj = Map::new();
    obj.insert("group".into(), json!(spec.group.clone().unwrap_or_default()));
    if let Some(names) = spec.names.as_ref() {
        obj.insert("names".into(), crd_names_to_json(names));
    }
    obj.insert("scope".into(), json!(spec.scope.clone().unwrap_or_default()));
    if !spec.versions.is_empty() {
        let versions: Vec<Value> = spec.versions.iter().map(crd_version_to_json).collect();
        obj.insert("versions".into(), Value::Array(versions));
    }
    if let Some(conversion) = spec.conversion.as_ref() {
        obj.insert("conversion".into(), crd_conversion_to_json(conversion));
    }
    if let Some(p) = spec.preserve_unknown_fields {
        obj.insert("preserveUnknownFields".into(), json!(p));
    }
    Value::Object(obj)
}

fn crd_names_to_json(n: &pbapi::CustomResourceDefinitionNames) -> Value {
    let mut obj = Map::new();
    obj.insert("plural".into(), json!(n.plural.clone().unwrap_or_default()));
    if let Some(s) = &n.singular {
        obj.insert("singular".into(), json!(s));
    }
    if !n.short_names.is_empty() {
        obj.insert("shortNames".into(), json!(n.short_names));
    }
    obj.insert("kind".into(), json!(n.kind.clone().unwrap_or_default()));
    if let Some(lk) = &n.list_kind {
        obj.insert("listKind".into(), json!(lk));
    }
    if !n.categories.is_empty() {
        obj.insert("categories".into(), json!(n.categories));
    }
    Value::Object(obj)
}

fn crd_version_to_json(v: &pbapi::CustomResourceDefinitionVersion) -> Value {
    let mut obj = Map::new();
    obj.insert("name".into(), json!(v.name.clone().unwrap_or_default()));
    obj.insert("served".into(), json!(v.served.unwrap_or(false)));
    obj.insert("storage".into(), json!(v.storage.unwrap_or(false)));
    if let Some(d) = v.deprecated {
        obj.insert("deprecated".into(), json!(d));
    }
    if let Some(msg) = &v.deprecation_warning {
        obj.insert("deprecationWarning".into(), json!(msg));
    }
    if let Some(schema) = v.schema.as_ref() {
        obj.insert("schema".into(), crd_validation_to_json(schema));
    }
    if let Some(sub) = v.subresources.as_ref() {
        obj.insert("subresources".into(), crd_subresources_to_json(sub));
    }
    if !v.additional_printer_columns.is_empty() {
        let cols: Vec<Value> = v
            .additional_printer_columns
            .iter()
            .map(crd_column_to_json)
            .collect();
        obj.insert("additionalPrinterColumns".into(), Value::Array(cols));
    }
    if !v.selectable_fields.is_empty() {
        let sf: Vec<Value> = v
            .selectable_fields
            .iter()
            .map(|f| json!({ "jsonPath": f.json_path.clone().unwrap_or_default() }))
            .collect();
        obj.insert("selectableFields".into(), Value::Array(sf));
    }
    Value::Object(obj)
}

fn crd_validation_to_json(v: &pbapi::CustomResourceValidation) -> Value {
    let mut obj = Map::new();
    if let Some(s) = v.open_apiv3_schema.as_ref() {
        obj.insert("openAPIV3Schema".into(), json_schema_props_to_json(s));
    }
    Value::Object(obj)
}

fn crd_subresources_to_json(s: &pbapi::CustomResourceSubresources) -> Value {
    let mut obj = Map::new();
    if s.status.is_some() {
        obj.insert("status".into(), json!({}));
    }
    if let Some(scale) = s.scale.as_ref() {
        let mut so = Map::new();
        so.insert(
            "specReplicasPath".into(),
            json!(scale.spec_replicas_path.clone().unwrap_or_default()),
        );
        so.insert(
            "statusReplicasPath".into(),
            json!(scale.status_replicas_path.clone().unwrap_or_default()),
        );
        if let Some(sp) = &scale.label_selector_path {
            so.insert("labelSelectorPath".into(), json!(sp));
        }
        obj.insert("scale".into(), Value::Object(so));
    }
    Value::Object(obj)
}

fn crd_column_to_json(c: &pbapi::CustomResourceColumnDefinition) -> Value {
    let mut obj = Map::new();
    obj.insert("name".into(), json!(c.name.clone().unwrap_or_default()));
    obj.insert("type".into(), json!(c.r#type.clone().unwrap_or_default()));
    obj.insert("jsonPath".into(), json!(c.json_path.clone().unwrap_or_default()));
    if let Some(d) = &c.description {
        obj.insert("description".into(), json!(d));
    }
    if let Some(f) = &c.format {
        obj.insert("format".into(), json!(f));
    }
    if let Some(p) = c.priority {
        obj.insert("priority".into(), json!(p));
    }
    Value::Object(obj)
}

fn crd_conversion_to_json(c: &pbapi::CustomResourceConversion) -> Value {
    let mut obj = Map::new();
    obj.insert(
        "strategy".into(),
        json!(c.strategy.clone().unwrap_or_default()),
    );
    if let Some(wc) = c.webhook.as_ref() {
        let mut wo = Map::new();
        if let Some(cc) = wc.client_config.as_ref() {
            let mut co = Map::new();
            if let Some(u) = &cc.url {
                co.insert("url".into(), json!(u));
            }
            if let Some(svc) = cc.service.as_ref() {
                let mut so = Map::new();
                so.insert("namespace".into(), json!(svc.namespace.clone().unwrap_or_default()));
                so.insert("name".into(), json!(svc.name.clone().unwrap_or_default()));
                if let Some(p) = &svc.path {
                    so.insert("path".into(), json!(p));
                }
                if let Some(port) = svc.port {
                    so.insert("port".into(), json!(port));
                }
                co.insert("service".into(), Value::Object(so));
            }
            if let Some(ca) = cc.ca_bundle.as_deref()
                && !ca.is_empty()
            {
                use base64::Engine;
                co.insert(
                    "caBundle".into(),
                    json!(base64::engine::general_purpose::STANDARD.encode(ca)),
                );
            }
            wo.insert("clientConfig".into(), Value::Object(co));
        }
        if !wc.conversion_review_versions.is_empty() {
            wo.insert(
                "conversionReviewVersions".into(),
                json!(wc.conversion_review_versions),
            );
        }
        obj.insert("webhook".into(), Value::Object(wo));
    }
    Value::Object(obj)
}

fn crd_status_to_json(s: &pbapi::CustomResourceDefinitionStatus) -> Value {
    let mut obj = Map::new();
    if !s.conditions.is_empty() {
        let conds: Vec<Value> = s
            .conditions
            .iter()
            .map(|c| {
                let mut co = Map::new();
                co.insert("type".into(), json!(c.r#type.clone().unwrap_or_default()));
                co.insert("status".into(), json!(c.status.clone().unwrap_or_default()));
                if let Some(t) = c.last_transition_time.as_ref() {
                    co.insert("lastTransitionTime".into(), time_to_json(t));
                }
                if let Some(r) = &c.reason {
                    co.insert("reason".into(), json!(r));
                }
                if let Some(m) = &c.message {
                    co.insert("message".into(), json!(m));
                }
                Value::Object(co)
            })
            .collect();
        obj.insert("conditions".into(), Value::Array(conds));
    }
    if let Some(names) = s.accepted_names.as_ref() {
        obj.insert("acceptedNames".into(), crd_names_to_json(names));
    }
    if !s.stored_versions.is_empty() {
        obj.insert("storedVersions".into(), json!(s.stored_versions));
    }
    Value::Object(obj)
}

// ─── ObjectMeta walker (subset) ─────────────────────────────────────────────

fn object_meta_to_json(m: &pbmeta::ObjectMeta) -> Value {
    let mut obj = Map::new();
    if let Some(v) = &m.name {
        obj.insert("name".into(), json!(v));
    }
    if let Some(v) = &m.generate_name {
        obj.insert("generateName".into(), json!(v));
    }
    if let Some(v) = &m.namespace {
        obj.insert("namespace".into(), json!(v));
    }
    if let Some(v) = &m.uid {
        obj.insert("uid".into(), json!(v));
    }
    if let Some(v) = &m.resource_version {
        obj.insert("resourceVersion".into(), json!(v));
    }
    if let Some(t) = m.creation_timestamp.as_ref() {
        obj.insert("creationTimestamp".into(), time_to_json(t));
    }
    if let Some(t) = m.deletion_timestamp.as_ref() {
        obj.insert("deletionTimestamp".into(), time_to_json(t));
    }
    if !m.labels.is_empty() {
        obj.insert("labels".into(), string_map_to_json(&m.labels));
    }
    if !m.annotations.is_empty() {
        obj.insert("annotations".into(), string_map_to_json(&m.annotations));
    }
    if !m.owner_references.is_empty() {
        let refs: Vec<Value> = m
            .owner_references
            .iter()
            .map(|o| {
                let mut r = Map::new();
                r.insert(
                    "apiVersion".into(),
                    json!(o.api_version.clone().unwrap_or_default()),
                );
                r.insert("kind".into(), json!(o.kind.clone().unwrap_or_default()));
                r.insert("name".into(), json!(o.name.clone().unwrap_or_default()));
                r.insert("uid".into(), json!(o.uid.clone().unwrap_or_default()));
                if let Some(c) = o.controller {
                    r.insert("controller".into(), json!(c));
                }
                if let Some(b) = o.block_owner_deletion {
                    r.insert("blockOwnerDeletion".into(), json!(b));
                }
                Value::Object(r)
            })
            .collect();
        obj.insert("ownerReferences".into(), Value::Array(refs));
    }
    if !m.finalizers.is_empty() {
        obj.insert("finalizers".into(), json!(m.finalizers));
    }
    Value::Object(obj)
}

// ─── JSONSchemaProps walker ─────────────────────────────────────────────────

fn json_schema_props_to_json(s: &pbapi::JsonSchemaProps) -> Value {
    let mut obj = Map::new();

    // Scalar primitives mirrored 1:1 ────────────────────────────────────────
    macro_rules! str_opt {
        ($field:ident, $json:literal) => {
            if let Some(v) = &s.$field {
                obj.insert($json.into(), json!(v));
            }
        };
    }
    // JSONSchemaProps uses `omitempty` semantics for its bool fields — false
    // is the zero value and is dropped on the wire. Some proto generators
    // (Go's gogoproto with bare `bool` Go fields) emit explicit `false`
    // anyway; upstream's OpenAPI converter discards those before serving.
    // Mirror that or the conformance schema-equality check fails.
    macro_rules! bool_opt {
        ($field:ident, $json:literal) => {
            if let Some(true) = s.$field {
                obj.insert($json.into(), json!(true));
            }
        };
    }
    macro_rules! f64_opt {
        ($field:ident, $json:literal) => {
            if let Some(v) = s.$field {
                obj.insert($json.into(), json!(v));
            }
        };
    }
    macro_rules! i64_opt {
        ($field:ident, $json:literal) => {
            if let Some(v) = s.$field {
                obj.insert($json.into(), json!(v));
            }
        };
    }

    str_opt!(id, "id");
    str_opt!(schema, "$schema");
    str_opt!(r#ref, "$ref");
    str_opt!(description, "description");
    str_opt!(r#type, "type");
    str_opt!(format, "format");
    str_opt!(title, "title");
    str_opt!(pattern, "pattern");
    f64_opt!(maximum, "maximum");
    bool_opt!(exclusive_maximum, "exclusiveMaximum");
    f64_opt!(minimum, "minimum");
    bool_opt!(exclusive_minimum, "exclusiveMinimum");
    i64_opt!(max_length, "maxLength");
    i64_opt!(min_length, "minLength");
    i64_opt!(max_items, "maxItems");
    i64_opt!(min_items, "minItems");
    bool_opt!(unique_items, "uniqueItems");
    f64_opt!(multiple_of, "multipleOf");
    i64_opt!(max_properties, "maxProperties");
    i64_opt!(min_properties, "minProperties");
    if !s.required.is_empty() {
        obj.insert("required".into(), json!(s.required));
    }

    if let Some(d) = s.default.as_ref() {
        obj.insert("default".into(), raw_json_to_value(d.raw.as_deref().unwrap_or(&[])));
    }

    if !s.r#enum.is_empty() {
        let arr: Vec<Value> = s.r#enum.iter().map(|j| raw_json_to_value(j.raw.as_deref().unwrap_or(&[]))).collect();
        obj.insert("enum".into(), Value::Array(arr));
    }

    if let Some(items) = s.items.as_ref() {
        if let Some(schema) = items.schema.as_ref() {
            obj.insert("items".into(), json_schema_props_to_json(schema));
        } else if !items.j_son_schemas.is_empty() {
            obj.insert(
                "items".into(),
                Value::Array(
                    items
                        .j_son_schemas
                        .iter()
                        .map(json_schema_props_to_json)
                        .collect(),
                ),
            );
        }
    }

    if !s.all_of.is_empty() {
        obj.insert(
            "allOf".into(),
            Value::Array(s.all_of.iter().map(json_schema_props_to_json).collect()),
        );
    }
    if !s.one_of.is_empty() {
        obj.insert(
            "oneOf".into(),
            Value::Array(s.one_of.iter().map(json_schema_props_to_json).collect()),
        );
    }
    if !s.any_of.is_empty() {
        obj.insert(
            "anyOf".into(),
            Value::Array(s.any_of.iter().map(json_schema_props_to_json).collect()),
        );
    }
    if let Some(not) = s.not.as_ref() {
        obj.insert("not".into(), json_schema_props_to_json(not));
    }

    if !s.properties.is_empty() {
        let mut props = Map::new();
        for (k, v) in &s.properties {
            props.insert(k.clone(), json_schema_props_to_json(v));
        }
        obj.insert("properties".into(), Value::Object(props));
    }
    if let Some(ap) = s.additional_properties.as_ref() {
        if let Some(schema) = ap.schema.as_deref() {
            obj.insert("additionalProperties".into(), json_schema_props_to_json(schema));
        } else if let Some(allows) = ap.allows {
            obj.insert("additionalProperties".into(), json!(allows));
        }
    }
    if !s.pattern_properties.is_empty() {
        let mut props = Map::new();
        for (k, v) in &s.pattern_properties {
            props.insert(k.clone(), json_schema_props_to_json(v));
        }
        obj.insert("patternProperties".into(), Value::Object(props));
    }
    if !s.dependencies.is_empty() {
        let mut deps = Map::new();
        for (k, v) in &s.dependencies {
            if let Some(schema) = v.schema.as_ref() {
                deps.insert(k.clone(), json_schema_props_to_json(schema));
            } else if !v.property.is_empty() {
                deps.insert(k.clone(), json!(v.property));
            }
        }
        obj.insert("dependencies".into(), Value::Object(deps));
    }
    if let Some(ai) = s.additional_items.as_ref() {
        if let Some(schema) = ai.schema.as_deref() {
            obj.insert("additionalItems".into(), json_schema_props_to_json(schema));
        } else if let Some(allows) = ai.allows {
            obj.insert("additionalItems".into(), json!(allows));
        }
    }
    if !s.definitions.is_empty() {
        let mut defs = Map::new();
        for (k, v) in &s.definitions {
            defs.insert(k.clone(), json_schema_props_to_json(v));
        }
        obj.insert("definitions".into(), Value::Object(defs));
    }

    bool_opt!(nullable, "nullable");

    if let Some(ed) = s.external_docs.as_ref() {
        let mut e = Map::new();
        if let Some(d) = &ed.description {
            e.insert("description".into(), json!(d));
        }
        if let Some(u) = &ed.url {
            e.insert("url".into(), json!(u));
        }
        obj.insert("externalDocs".into(), Value::Object(e));
    }
    if let Some(ex) = s.example.as_ref() {
        obj.insert("example".into(), raw_json_to_value(ex.raw.as_deref().unwrap_or(&[])));
    }

    // x-kubernetes-* extensions ─────────────────────────────────────────────
    bool_opt!(x_kubernetes_preserve_unknown_fields, "x-kubernetes-preserve-unknown-fields");
    bool_opt!(x_kubernetes_embedded_resource, "x-kubernetes-embedded-resource");
    bool_opt!(x_kubernetes_int_or_string, "x-kubernetes-int-or-string");
    if !s.x_kubernetes_list_map_keys.is_empty() {
        obj.insert(
            "x-kubernetes-list-map-keys".into(),
            json!(s.x_kubernetes_list_map_keys),
        );
    }
    str_opt!(x_kubernetes_list_type, "x-kubernetes-list-type");
    str_opt!(x_kubernetes_map_type, "x-kubernetes-map-type");
    if !s.x_kubernetes_validations.is_empty() {
        let arr: Vec<Value> = s
            .x_kubernetes_validations
            .iter()
            .map(|v| {
                let mut o = Map::new();
                o.insert("rule".into(), json!(v.rule.clone().unwrap_or_default()));
                if let Some(m) = &v.message {
                    o.insert("message".into(), json!(m));
                }
                if let Some(me) = &v.message_expression {
                    o.insert("messageExpression".into(), json!(me));
                }
                if let Some(r) = &v.reason {
                    o.insert("reason".into(), json!(r));
                }
                if let Some(p) = &v.field_path {
                    o.insert("fieldPath".into(), json!(p));
                }
                if let Some(o2) = v.optional_old_self {
                    o.insert("optionalOldSelf".into(), json!(o2));
                }
                Value::Object(o)
            })
            .collect();
        obj.insert("x-kubernetes-validations".into(), Value::Array(arr));
    }

    Value::Object(obj)
}

// ─── Primitive helpers ──────────────────────────────────────────────────────

/// k8s `meta.v1.Time` has proto fields `seconds: i64` + `nanos: i32` but the
/// JSON form is an RFC3339 timestamp string. Empty timestamps serialize as
/// `null`, matching upstream.
fn time_to_json(t: &pbmeta::Time) -> Value {
    use chrono::TimeZone;
    let secs = t.seconds.unwrap_or(0);
    let nanos = t.nanos.unwrap_or(0);
    if secs == 0 && nanos == 0 {
        return Value::Null;
    }
    match chrono::Utc.timestamp_opt(secs, nanos as u32) {
        chrono::offset::LocalResult::Single(dt) => json!(dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        _ => Value::Null,
    }
}

fn string_map_to_json(m: &std::collections::HashMap<String, String>) -> Value {
    let mut o = Map::new();
    for (k, v) in m {
        o.insert(k.clone(), json!(v));
    }
    Value::Object(o)
}

/// `runtime.RawExtension` (used by JSONSchemaProps.default, .example, .enum
/// items) carries a raw JSON byte slice. Parse it back into a `Value`.
fn raw_json_to_value(raw: &[u8]) -> Value {
    serde_json::from_slice(raw).unwrap_or(Value::Null)
}

// ─── Internal varint helpers (just enough to locate envelope sub-messages) ──

fn read_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for (i, &b) in data.iter().enumerate() {
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

fn read_message_field(data: &[u8], target: u32) -> Option<&[u8]> {
    let mut pos = 0;
    while pos < data.len() {
        let (tag, n) = read_varint(&data[pos..])?;
        pos += n;
        let field = (tag >> 3) as u32;
        let wire = (tag & 7) as u8;
        if wire != 2 {
            // Skip non-length-delimited fields.
            let skip = match wire {
                0 => read_varint(&data[pos..]).map(|(_, n)| n)?,
                1 => 8,
                5 => 4,
                _ => return None,
            };
            pos += skip;
            continue;
        }
        let (len, n) = read_varint(&data[pos..])?;
        pos += n;
        let end = pos + len as usize;
        if end > data.len() {
            return None;
        }
        if field == target {
            return Some(&data[pos..end]);
        }
        pos = end;
    }
    None
}
