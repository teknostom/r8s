//! `fieldValidation=Ignore|Warn|Strict` for request bodies: duplicate keys in
//! the document and fields the resource's schema does not declare. Warn (the
//! default) reports them as `Warning:` headers, Strict rejects with 400.

use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use r8s_types::openapi::definition;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};

use crate::handler::RouteContext;
use crate::response;

const OBJECT_META: &str = "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Ignore,
    Warn,
    Strict,
}

pub fn mode(raw_query: Option<&str>) -> Mode {
    raw_query
        .into_iter()
        .flat_map(|q| q.split('&'))
        .find_map(|p| p.strip_prefix("fieldValidation="))
        .map(|v| match v {
            "Strict" => Mode::Strict,
            "Ignore" => Mode::Ignore,
            _ => Mode::Warn,
        })
        .unwrap_or(Mode::Warn)
}

/// Which schema dialect the resource is described in.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Flavor {
    /// Vendored OpenAPI v2 definition: objects without declared properties
    /// are free-form.
    BuiltIn,
    /// CRD structural schema: undeclared fields are unknown unless the
    /// subtree preserves unknown fields.
    Custom,
}

/// A parsed body plus the duplicate keys met while parsing, in document order.
pub struct Decoded {
    pub value: Value,
    pub duplicates: Vec<String>,
}

/// Unknown and duplicate fields found in one request body.
#[derive(Default, Debug)]
pub struct Findings {
    pub unknown: Vec<String>,
    pub duplicates: Vec<String>,
}

impl Findings {
    pub fn is_empty(&self) -> bool {
        self.unknown.is_empty() && self.duplicates.is_empty()
    }
}

/// Parse JSON, recording duplicate keys by path (`spec.replicas`).
pub fn decode_json(bytes: &[u8]) -> Result<Decoded, serde_json::Error> {
    let mut path = Vec::new();
    let mut duplicates = Vec::new();
    let mut de = serde_json::Deserializer::from_slice(bytes);
    let value = Node {
        path: &mut path,
        duplicates: &mut duplicates,
    }
    .deserialize(&mut de)?;
    de.end()?;
    Ok(Decoded { value, duplicates })
}

/// Parse YAML, recording duplicate keys by path.
pub fn decode_yaml(bytes: &[u8]) -> Result<Decoded, serde_yaml::Error> {
    let mut path = Vec::new();
    let mut duplicates = Vec::new();
    let value = Node {
        path: &mut path,
        duplicates: &mut duplicates,
    }
    .deserialize(serde_yaml::Deserializer::from_slice(bytes))?;
    Ok(Decoded { value, duplicates })
}

/// Upstream's YAML-to-JSON failure for a duplicate mapping key, with the line
/// of the repeated key found by scanning the text.
pub fn yaml_duplicate_error(text: &[u8], duplicate_paths: &[String]) -> String {
    let text = String::from_utf8_lossy(text);
    let mut lines = Vec::new();
    for path in duplicate_paths {
        let key = path.rsplit('.').next().unwrap_or(path);
        let key = key.split('[').next().unwrap_or(key);
        let mut seen: Vec<(usize, usize)> = Vec::new(); // (indent, line)
        for (i, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            let indent = line.len() - trimmed.len();
            let is_key = trimmed
                .strip_prefix("- ")
                .unwrap_or(trimmed)
                .strip_prefix(key)
                .is_some_and(|rest| rest.starts_with(':'));
            if is_key {
                if seen.iter().any(|(ind, _)| *ind == indent) {
                    lines.push(format!(
                        "  line {}: key \"{key}\" already set in map",
                        i + 1
                    ));
                    break;
                }
                seen.push((indent, i + 1));
            }
        }
        if lines.len() < duplicate_paths.len() {
            lines.push(format!("  key \"{key}\" already set in map"));
        }
    }
    format!(
        "error converting YAML to JSON: yaml: unmarshal errors:\n{}",
        lines.join("\n")
    )
}

struct Node<'a> {
    path: &'a mut Vec<String>,
    duplicates: &'a mut Vec<String>,
}

impl<'de> DeserializeSeed<'de> for Node<'_> {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Node<'_> {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }
    fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
        Ok(Value::from(v))
    }
    fn visit_u64<E>(self, v: u64) -> Result<Value, E> {
        Ok(Value::from(v))
    }
    fn visit_f64<E>(self, v: f64) -> Result<Value, E> {
        Ok(serde_json::Number::from_f64(v).map_or(Value::Null, Value::Number))
    }
    fn visit_str<E>(self, v: &str) -> Result<Value, E> {
        Ok(Value::String(v.to_string()))
    }
    fn visit_string<E>(self, v: String) -> Result<Value, E> {
        Ok(Value::String(v))
    }
    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        d.deserialize_any(self)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        let mut index = 0;
        loop {
            self.path.push(format!("[{index}]"));
            let next = seq.next_element_seed(Node {
                path: self.path,
                duplicates: self.duplicates,
            });
            self.path.pop();
            match next? {
                Some(item) => items.push(item),
                None => break,
            }
            index += 1;
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut object = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if object.contains_key(&key) {
                self.duplicates.push(join(self.path, &key));
            }
            self.path.push(key.clone());
            let value = map.next_value_seed(Node {
                path: self.path,
                duplicates: self.duplicates,
            });
            self.path.pop();
            object.insert(key, value?);
        }
        Ok(Value::Object(object))
    }
}

/// `a.b[0].c`: segments joined with dots, indexes attached to their parent.
fn join(path: &[String], key: &str) -> String {
    let mut out = String::new();
    for seg in path.iter().map(String::as_str).chain([key]) {
        if !seg.starts_with('[') && !out.is_empty() {
            out.push('.');
        }
        out.push_str(seg);
    }
    out
}

/// The schema a resource's bodies are checked against, if it has one.
fn schema_for(ctx: &RouteContext) -> Option<(&Value, Flavor)> {
    let rt = &ctx.resource_type;
    if let Some(def) = r8s_types::openapi::definition_for(&rt.gvr.group, &rt.gvr.version, &rt.kind)
    {
        return Some((def, Flavor::BuiltIn));
    }
    if r8s_types::openapi::spec_bytes_for(&rt.gvr.group, &rt.gvr.version).is_some() {
        return None;
    }
    rt.schema.as_ref().map(|s| (s, Flavor::Custom))
}

/// Fields of `value` the resource's schema does not declare, in document
/// order; empty when the resource has no schema.
pub fn unknown_fields(ctx: &RouteContext, value: &Value) -> Vec<String> {
    let Some((schema, flavor)) = schema_for(ctx) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    walk(value, Some(schema), flavor, true, &mut Vec::new(), &mut out);
    out
}

/// Drop the fields a custom resource's structural schema does not declare
/// (apiextensions pruning). Built-ins and schemaless CRDs are left alone.
pub fn prune(ctx: &RouteContext, value: &mut Value) {
    if !matches!(schema_for(ctx), Some((_, Flavor::Custom))) {
        return;
    }
    for path in unknown_fields(ctx, value) {
        remove_path(value, &path);
    }
}

/// Remove the field at a dotted path with `[i]` indexes, e.g. `spec.a[0].x`.
fn remove_path(value: &mut Value, path: &str) {
    let mut segments: Vec<String> = Vec::new();
    for part in path.split('.') {
        let (name, indexes) = match part.find('[') {
            Some(i) => (&part[..i], &part[i..]),
            None => (part, ""),
        };
        if !name.is_empty() {
            segments.push(name.to_string());
        }
        for idx in indexes.trim_start_matches('[').split('[') {
            if let Some(n) = idx.strip_suffix(']') {
                segments.push(format!("[{n}]"));
            }
        }
    }
    let Some((last, parents)) = segments.split_last() else {
        return;
    };
    let mut cur = value;
    for seg in parents {
        cur = match seg.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            Some(i) => match i.parse::<usize>().ok().and_then(|i| cur.get_mut(i)) {
                Some(v) => v,
                None => return,
            },
            None => match cur.get_mut(seg.as_str()) {
                Some(v) => v,
                None => return,
            },
        };
    }
    if let Some(map) = cur.as_object_mut() {
        map.remove(last.as_str());
    }
}

fn walk(
    value: &Value,
    schema: Option<&Value>,
    flavor: Flavor,
    resource_root: bool,
    path: &mut Vec<String>,
    out: &mut Vec<String>,
) {
    let schema = schema.and_then(resolve);
    match value {
        Value::Object(fields) => {
            let Some(schema) = schema else {
                return;
            };
            let preserve = schema
                .get("x-kubernetes-preserve-unknown-fields")
                .and_then(Value::as_bool)
                == Some(true);
            let embedded = resource_root
                || schema
                    .get("x-kubernetes-embedded-resource")
                    .and_then(Value::as_bool)
                    == Some(true);
            let properties = schema.get("properties").and_then(Value::as_object);
            let additional = schema.get("additionalProperties");
            for (key, child) in fields {
                path.push(key.clone());
                let declared = properties.and_then(|p| p.get(key));
                if embedded && flavor == Flavor::Custom && key == "metadata" {
                    walk(
                        child,
                        definition(OBJECT_META),
                        Flavor::BuiltIn,
                        false,
                        path,
                        out,
                    );
                } else if let Some(child_schema) = declared {
                    walk(child, Some(child_schema), flavor, false, path, out);
                } else if embedded
                    && flavor == Flavor::Custom
                    && matches!(key.as_str(), "apiVersion" | "kind")
                {
                } else if let Some(extra @ Value::Object(_)) = additional {
                    walk(child, Some(extra), flavor, false, path, out);
                } else if additional.is_some_and(|a| a.as_bool() == Some(true)) || preserve {
                } else if properties.is_none() && flavor == Flavor::BuiltIn {
                    // A vendored object without declared properties is free-form
                    // (RawExtension, untyped maps).
                } else {
                    out.push(join(&path[..path.len() - 1], key));
                }
                path.pop();
            }
        }
        Value::Array(items) => {
            let item_schema = schema.and_then(|s| s.get("items"));
            for (i, item) in items.iter().enumerate() {
                path.push(format!("[{i}]"));
                walk(item, item_schema, flavor, false, path, out);
                path.pop();
            }
        }
        _ => {}
    }
}

/// Follow a `$ref` (v2) or single-element `allOf: [{$ref}]` (v3).
fn resolve(schema: &Value) -> Option<&Value> {
    if let Some(r) = schema.get("$ref").and_then(Value::as_str) {
        return definition(r);
    }
    if let Some([inner]) = schema
        .get("allOf")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        && let Some(r) = inner.get("$ref").and_then(Value::as_str)
    {
        return definition(r);
    }
    Some(schema)
}

/// The 400 for a Strict request with findings, in the wording of upstream's
/// strict decoder (built-ins) or the CRD field validator (custom resources).
pub fn strict_error(ctx: &RouteContext, findings: &Findings) -> Response {
    let rt = &ctx.resource_type;
    let custom = schema_for(ctx).is_some_and(|(_, f)| f == Flavor::Custom);
    let mut parts: Vec<String> = Vec::new();
    for field in &findings.unknown {
        parts.push(if custom {
            format!(".{field}: field not declared in schema")
        } else {
            format!("unknown field \"{field}\"")
        });
    }
    for field in &findings.duplicates {
        parts.push(format!("duplicate field \"{field}\""));
    }
    let detail = format!("strict decoding error: {}", parts.join(", "));
    let message = if custom {
        detail
    } else {
        format!(
            "{} in version \"{}\" cannot be handled as a {}: {detail}",
            rt.kind, rt.gvr.version, rt.kind
        )
    };
    response::status_error(StatusCode::BAD_REQUEST, "BadRequest", &message)
}

/// Append one `Warning: 299 - "..."` header per finding.
pub fn attach_warnings(mut resp: Response, findings: &Findings) -> Response {
    let texts = findings
        .unknown
        .iter()
        .map(|f| format!("unknown field \"{f}\""))
        .chain(
            findings
                .duplicates
                .iter()
                .map(|f| format!("duplicate field \"{f}\"")),
        );
    for text in texts {
        let quoted = format!("299 - \"{}\"", text.replace('"', "\\\""));
        if let Ok(value) = HeaderValue::from_str(&quoted) {
            resp.headers_mut().append("warning", value);
        }
    }
    resp
}

/// Findings for a body (already parsed, with its duplicates) under `mode`.
/// Strict with findings is an error response.
#[allow(clippy::result_large_err)]
pub fn check(
    ctx: &RouteContext,
    mode: Mode,
    decoded: Decoded,
) -> Result<(Value, Findings), Response> {
    if mode == Mode::Ignore {
        return Ok((decoded.value, Findings::default()));
    }
    let findings = Findings {
        unknown: unknown_fields(ctx, &decoded.value),
        duplicates: decoded.duplicates,
    };
    if mode == Mode::Strict && !findings.is_empty() {
        return Err(strict_error(ctx, &findings));
    }
    Ok((decoded.value, findings))
}

#[cfg(test)]
mod tests {
    use super::*;
    use r8s_types::{GroupVersionResource, ResourceType};
    use std::sync::Arc;

    fn ctx(
        group: &str,
        version: &str,
        resource: &str,
        kind: &str,
        schema: Option<Value>,
    ) -> RouteContext {
        RouteContext {
            resource_type: Arc::new(ResourceType {
                gvr: GroupVersionResource::new(group, version, resource),
                kind: kind.into(),
                namespaced: true,
                singular: String::new(),
                short_names: vec![],
                subresources: vec![],
                schema,
                storage_gvr: None,
            }),
        }
    }

    #[test]
    fn json_duplicates_are_reported_by_path() {
        let d =
            decode_json(br#"{"spec":{"replicas":2,"replicas":3,"a":[{"x":1,"x":2}]}}"#).unwrap();
        assert_eq!(d.duplicates, vec!["spec.replicas", "spec.a[0].x"]);
        assert_eq!(d.value["spec"]["replicas"], 3);
    }

    #[test]
    fn typed_unknown_fields_follow_the_vendored_schema() {
        let c = ctx("apps", "v1", "deployments", "Deployment", None);
        let v: Value = serde_json::from_str(
            r#"{"metadata":{"name":"d","unknownMeta":"x","labels":{"a":"b"}},
                "spec":{"unknownField":"foo","replicas":1,"selector":{"matchLabels":{"a":"b"}},
                        "template":{"metadata":{"labels":{"a":"b"}},"spec":{"containers":[{"name":"c","image":"i","bogus":1}]}}}}"#,
        )
        .unwrap();
        // serde_json keeps object keys sorted, so findings come out in key order.
        assert_eq!(
            unknown_fields(&c, &v),
            vec![
                "metadata.unknownMeta",
                "spec.template.spec.containers[0].bogus",
                "spec.unknownField"
            ]
        );
        let d =
            decode_json(br#"{"spec":{"unknownField":"foo","replicas":2,"replicas":3}}"#).unwrap();
        let err = check(&c, Mode::Strict, d).err().unwrap();
        assert_eq!(err.status(), 400);
    }

    #[test]
    fn custom_resources_check_metadata_and_declared_properties() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"spec": {"type": "object", "x-kubernetes-preserve-unknown-fields": true,
                "properties": {"template": {"type": "object", "x-kubernetes-embedded-resource": true,
                    "properties": {"metadata": {"type": "object"}, "spec": {"type": "object"}}}}}}
        });
        let c = ctx("g", "v1", "foos", "Foo", Some(schema));
        let v: Value = serde_yaml::from_str(
            "apiVersion: g/v1\nkind: Foo\nmetadata:\n  name: x\n  unknownMeta: u\nunknownField: z\nspec:\n  anything: ok\n  template:\n    apiVersion: a/v1\n    kind: Sub\n    metadata:\n      unknownSubMeta: u\n      name: s\n    spec:\n      free: 1\n",
        )
        .unwrap();
        assert_eq!(
            unknown_fields(&c, &v),
            vec![
                "metadata.unknownMeta",
                "spec.template.metadata.unknownSubMeta",
                "spec.template.spec.free",
                "unknownField"
            ]
        );
        let none = ctx("g", "v1", "foos", "Foo", None);
        assert!(
            unknown_fields(&none, &v).is_empty(),
            "no schema, nothing is unknown"
        );
    }

    #[test]
    fn prune_drops_undeclared_custom_resource_fields() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"data": {"type": "object", "properties": {
                "mutation-start": {"type": "string"}, "mutation-stage-1": {"type": "string"}}},
                "list": {"type": "array", "items": {"type": "object", "properties": {"a": {"type": "string"}}}}}
        });
        let c = ctx("g", "v1", "foos", "Foo", Some(schema));
        let mut v = serde_json::json!({
            "apiVersion": "g/v1", "kind": "Foo", "metadata": {"name": "x", "labels": {"k": "v"}},
            "data": {"mutation-start": "yes", "mutation-stage-1": "yes", "mutation-stage-2": "yes"},
            "list": [{"a": "1", "b": "2"}]
        });
        prune(&c, &mut v);
        assert_eq!(
            v["data"],
            serde_json::json!({"mutation-start": "yes", "mutation-stage-1": "yes"})
        );
        assert_eq!(v["list"], serde_json::json!([{"a": "1"}]));
        assert_eq!(v["metadata"]["labels"]["k"], "v");

        // Built-ins are never pruned.
        let d = ctx("apps", "v1", "deployments", "Deployment", None);
        let mut dep = serde_json::json!({"spec": {"unknownField": "foo"}});
        prune(&d, &mut dep);
        assert_eq!(dep["spec"]["unknownField"], "foo");
    }

    #[test]
    fn yaml_duplicates_name_the_line() {
        let y = b"\napiVersion: g/v1\nkind: K\nmetadata:\n  name: x\nspec:\n  unknown: uk1\n  foo: foo1\n  foo: foo2\n  cronSpec: \"* * * * */5\"\n";
        let d = decode_yaml(y).unwrap();
        assert_eq!(d.duplicates, vec!["spec.foo"]);
        assert!(
            yaml_duplicate_error(y, &d.duplicates)
                .contains("line 9: key \"foo\" already set in map")
        );
    }

    #[test]
    fn mode_parses_the_query() {
        assert_eq!(
            mode(Some("fieldManager=x&fieldValidation=Strict")),
            Mode::Strict
        );
        assert_eq!(mode(Some("fieldValidation=Ignore")), Mode::Ignore);
        assert_eq!(mode(None), Mode::Warn);
    }
}
