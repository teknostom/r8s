//! Server-side apply merge (`application/apply-patch+yaml`).
//!
//! Merges the applied configuration into the stored object following the
//! schema's list semantics: `x-kubernetes-list-type: map` lists merge
//! element-wise on `x-kubernetes-list-map-keys`, `set` lists union, atomic and
//! unannotated lists replace. A list with `x-kubernetes-patch-merge-key` but no
//! list type merges on that key, as kube-openapi's schema conversion does.
//! Absent key fields take the schema `default` before matching. Nulls delete.
//!
//! Field ownership (`managedFields`) is not tracked, so fields and list entries
//! absent from the applied configuration are kept.

use r8s_types::openapi::definition;
use serde_json::{Map, Value};

const OBJECT_META: &str = "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta";

/// Merge `patch` into `target`. `schema` is the resource's root schema (a
/// vendored definition or a CRD's structural schema); `None` merges objects
/// structurally and replaces lists. `metadata` always follows `ObjectMeta`.
pub fn apply(target: &mut Value, patch: &Value, schema: Option<&Value>) {
    let Some(patch_obj) = patch.as_object() else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let Some(target_obj) = target.as_object_mut() else {
        return;
    };
    let schema = schema.and_then(resolve);
    for (key, value) in patch_obj {
        let field_schema = if key == "metadata" {
            definition(OBJECT_META)
        } else {
            property_schema(schema, key)
        };
        merge_field(target_obj, key, value, field_schema);
    }
}

fn merge_field(target: &mut Map<String, Value>, key: &str, value: &Value, schema: Option<&Value>) {
    if value.is_null() {
        target.remove(key);
        return;
    }
    let entry = target.entry(key).or_insert(Value::Null);
    merge(entry, value, schema);
}

fn merge(target: &mut Value, patch: &Value, schema: Option<&Value>) {
    let schema = schema.and_then(resolve);
    match patch {
        Value::Object(patch_obj) => {
            if !target.is_object() {
                *target = Value::Object(Map::new());
            }
            let Some(target_obj) = target.as_object_mut() else {
                return;
            };
            for (key, value) in patch_obj {
                merge_field(target_obj, key, value, property_schema(schema, key));
            }
        }
        Value::Array(items) => match list_kind(schema) {
            ListKind::Map(keys) => {
                let item_schema = schema.and_then(|s| s.get("items"));
                merge_list_map(target, items, &keys, item_schema);
            }
            ListKind::Set => merge_list_set(target, items),
            ListKind::Atomic => *target = patch.clone(),
        },
        _ => *target = patch.clone(),
    }
}

/// Follow a `$ref` (v2) or single-element `allOf: [{$ref}]` (v3) to its
/// definition. Unknown refs resolve to `None`; everything else is itself.
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

/// Unresolved schema of a field: a declared property, else the
/// `additionalProperties` schema for map-typed objects.
fn property_schema<'a>(schema: Option<&'a Value>, key: &str) -> Option<&'a Value> {
    let schema = schema?;
    if let Some(prop) = schema.get("properties").and_then(|p| p.get(key)) {
        return Some(prop);
    }
    match schema.get("additionalProperties") {
        Some(extra @ Value::Object(_)) => Some(extra),
        _ => None,
    }
}

enum ListKind {
    Map(Vec<String>),
    Set,
    Atomic,
}

fn list_kind(schema: Option<&Value>) -> ListKind {
    let Some(schema) = schema else {
        return ListKind::Atomic;
    };
    let merge_key = schema
        .get("x-kubernetes-patch-merge-key")
        .and_then(Value::as_str)
        .map(|k| vec![k.to_string()]);
    let keys = match schema.get("x-kubernetes-list-type").and_then(Value::as_str) {
        Some("map") => schema
            .get("x-kubernetes-list-map-keys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|k| !k.is_empty())
            .or(merge_key),
        Some("set") => return ListKind::Set,
        Some(_) => None,
        None => merge_key,
    };
    match keys {
        Some(keys) => ListKind::Map(keys),
        None => ListKind::Atomic,
    }
}

fn merge_list_map(
    target: &mut Value,
    items: &[Value],
    keys: &[String],
    item_schema: Option<&Value>,
) {
    if !target.is_array() {
        *target = Value::Array(Vec::new());
    }
    let Some(existing) = target.as_array_mut() else {
        return;
    };
    let item_schema = item_schema.and_then(resolve);
    for item in items {
        if item.is_null() {
            continue;
        }
        let position = key_of(item, keys, item_schema).and_then(|key| {
            existing
                .iter()
                .position(|e| key_of(e, keys, item_schema).as_ref() == Some(&key))
        });
        match position {
            Some(i) => merge(&mut existing[i], item, item_schema),
            None => {
                let mut fresh = Value::Null;
                merge(&mut fresh, item, item_schema);
                existing.push(fresh);
            }
        }
    }
}

/// Key tuple of a list-map element, taking absent keys from the item schema's
/// `default`. `None` if a key is still missing.
fn key_of(item: &Value, keys: &[String], item_schema: Option<&Value>) -> Option<Vec<Value>> {
    keys.iter()
        .map(|key| {
            item.get(key)
                .filter(|v| !v.is_null())
                .or_else(|| item_schema?.get("properties")?.get(key)?.get("default"))
                .cloned()
        })
        .collect()
}

fn merge_list_set(target: &mut Value, items: &[Value]) {
    if !target.is_array() {
        *target = Value::Array(Vec::new());
    }
    let Some(existing) = target.as_array_mut() else {
        return;
    };
    for item in items {
        if !existing.contains(item) {
            existing.push(item.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use r8s_types::openapi::definition_for;
    use serde_json::json;

    fn webhook_schema() -> &'static Value {
        definition_for(
            "admissionregistration.k8s.io",
            "v1",
            "ValidatingWebhookConfiguration",
        )
        .unwrap()
    }

    #[test]
    fn webhooks_merge_by_name() {
        let rules = json!([{"apiGroups": ["cert-manager.io"], "resources": ["*"]}]);
        let mut stored = json!({
            "webhooks": [
                {"name": "a.example", "rules": rules, "clientConfig": {"service": {"name": "s"}}},
                {"name": "b.example", "rules": rules, "clientConfig": {"service": {"name": "s"}}}
            ]
        });
        let patch = json!({
            "webhooks": [{"name": "a.example", "clientConfig": {"caBundle": "Q0E="}}]
        });
        apply(&mut stored, &patch, Some(webhook_schema()));
        let webhooks = stored["webhooks"].as_array().unwrap();
        assert_eq!(webhooks.len(), 2);
        assert_eq!(webhooks[0]["rules"], rules);
        assert_eq!(webhooks[0]["clientConfig"]["service"]["name"], "s");
        assert_eq!(webhooks[0]["clientConfig"]["caBundle"], "Q0E=");
        assert_eq!(webhooks[1]["rules"], rules);
        assert!(webhooks[1]["clientConfig"].get("caBundle").is_none());
    }

    #[test]
    fn unmatched_elements_append() {
        let mut stored = json!({"webhooks": [{"name": "a"}]});
        let patch = json!({"webhooks": [{"name": "b"}, {"name": "a", "timeoutSeconds": 5}]});
        apply(&mut stored, &patch, Some(webhook_schema()));
        assert_eq!(
            stored["webhooks"],
            json!([{"name": "a", "timeoutSeconds": 5}, {"name": "b"}])
        );
    }

    #[test]
    fn nested_list_maps_follow_refs() {
        let pod = definition_for("", "v1", "Pod").unwrap();
        let mut stored = json!({
            "spec": {"containers": [
                {"name": "app", "image": "a:1", "env": [{"name": "A", "value": "1"}]},
                {"name": "side", "image": "s:1"}
            ]}
        });
        let patch = json!({
            "spec": {"containers": [
                {"name": "app", "image": "a:2", "env": [{"name": "B", "value": "2"}]}
            ]}
        });
        apply(&mut stored, &patch, Some(pod));
        let containers = stored["spec"]["containers"].as_array().unwrap();
        assert_eq!(containers.len(), 2);
        assert_eq!(containers[0]["image"], "a:2");
        assert_eq!(
            containers[0]["env"],
            json!([{"name": "A", "value": "1"}, {"name": "B", "value": "2"}])
        );
        assert_eq!(containers[1]["image"], "s:1");
    }

    #[test]
    fn absent_key_takes_schema_default() {
        let pod = definition_for("", "v1", "Pod").unwrap();
        let mut stored = json!({
            "spec": {"containers": [{"name": "app", "ports": [{"containerPort": 8080, "protocol": "TCP"}]}]}
        });
        let patch = json!({
            "spec": {"containers": [{"name": "app", "ports": [{"containerPort": 8080, "name": "http"}]}]}
        });
        apply(&mut stored, &patch, Some(pod));
        assert_eq!(
            stored["spec"]["containers"][0]["ports"],
            json!([{"containerPort": 8080, "protocol": "TCP", "name": "http"}])
        );
    }

    #[test]
    fn atomic_and_unannotated_lists_replace() {
        let pod = definition_for("", "v1", "Pod").unwrap();
        let mut stored = json!({"spec": {"tolerations": [{"key": "a"}, {"key": "b"}]}});
        apply(
            &mut stored,
            &json!({"spec": {"tolerations": [{"key": "c"}]}}),
            Some(pod),
        );
        assert_eq!(stored["spec"]["tolerations"], json!([{"key": "c"}]));

        let mut stored = json!({"items": [{"name": "a"}]});
        apply(&mut stored, &json!({"items": [{"name": "b"}]}), None);
        assert_eq!(stored["items"], json!([{"name": "b"}]));
    }

    #[test]
    fn set_lists_union() {
        let mut stored = json!({"metadata": {"finalizers": ["a", "b"]}});
        apply(
            &mut stored,
            &json!({"metadata": {"finalizers": ["c", "a"]}}),
            None,
        );
        assert_eq!(stored["metadata"]["finalizers"], json!(["a", "b", "c"]));
    }

    #[test]
    fn metadata_uses_object_meta_without_resource_schema() {
        let crd_schema = json!({"type": "object", "properties": {"spec": {"type": "object"}}});
        let mut stored = json!({"metadata": {"ownerReferences": [{"uid": "1", "name": "x"}]}});
        let patch = json!({
            "metadata": {
                "labels": {"k": "v"},
                "ownerReferences": [{"uid": "2", "name": "y"}, {"uid": "1", "controller": true}]
            }
        });
        apply(&mut stored, &patch, Some(&crd_schema));
        assert_eq!(stored["metadata"]["labels"], json!({"k": "v"}));
        assert_eq!(
            stored["metadata"]["ownerReferences"],
            json!([{"uid": "1", "name": "x", "controller": true}, {"uid": "2", "name": "y"}])
        );
    }

    #[test]
    fn crd_structural_schema_list_map_keys() {
        let schema = json!({
            "type": "object",
            "properties": {"spec": {"type": "object", "properties": {
                "targets": {
                    "type": "array",
                    "x-kubernetes-list-type": "map",
                    "x-kubernetes-list-map-keys": ["host", "port"],
                    "items": {"type": "object", "properties": {
                        "host": {"type": "string"},
                        "port": {"type": "integer", "default": 443},
                        "weight": {"type": "integer"}
                    }}
                }
            }}}
        });
        let mut stored = json!({"spec": {"targets": [
            {"host": "a", "port": 443, "weight": 1},
            {"host": "a", "port": 80, "weight": 2}
        ]}});
        let patch = json!({"spec": {"targets": [{"host": "a", "weight": 9}]}});
        apply(&mut stored, &patch, Some(&schema));
        assert_eq!(
            stored["spec"]["targets"],
            json!([{"host": "a", "port": 443, "weight": 9}, {"host": "a", "port": 80, "weight": 2}])
        );
    }

    #[test]
    fn merge_key_without_list_type_merges() {
        let schema = json!({"properties": {"xs": {
            "type": "array",
            "x-kubernetes-patch-merge-key": "id",
            "x-kubernetes-patch-strategy": "merge",
            "items": {"type": "object"}
        }}});
        let mut stored = json!({"xs": [{"id": 1, "a": 1}]});
        apply(
            &mut stored,
            &json!({"xs": [{"id": 1, "b": 2}]}),
            Some(&schema),
        );
        assert_eq!(stored["xs"], json!([{"id": 1, "a": 1, "b": 2}]));
    }

    #[test]
    fn nulls_delete_and_scalars_replace() {
        let mut stored = json!({"spec": {"a": 1, "b": {"c": 1, "d": 2}}, "gone": true});
        let patch = json!({"spec": {"a": 2, "b": {"c": null}}, "gone": null});
        apply(&mut stored, &patch, None);
        assert_eq!(stored, json!({"spec": {"a": 2, "b": {"d": 2}}}));
    }

    #[test]
    fn apply_onto_empty_strips_nulls() {
        let mut fresh = Value::Null;
        let patch = json!({"spec": {"a": null, "xs": [{"id": 1, "n": null}]}});
        let schema = json!({"properties": {"spec": {"properties": {"xs": {
            "x-kubernetes-list-type": "map",
            "x-kubernetes-list-map-keys": ["id"]
        }}}}});
        apply(&mut fresh, &patch, Some(&schema));
        assert_eq!(fresh, json!({"spec": {"xs": [{"id": 1}]}}));
    }

    #[test]
    fn additional_properties_schema_applies_to_map_values() {
        let schema = json!({"properties": {"byName": {
            "type": "object",
            "additionalProperties": {"properties": {"xs": {
                "x-kubernetes-list-type": "map",
                "x-kubernetes-list-map-keys": ["id"]
            }}}
        }}});
        let mut stored = json!({"byName": {"k": {"xs": [{"id": 1, "a": 1}]}}});
        apply(
            &mut stored,
            &json!({"byName": {"k": {"xs": [{"id": 1, "b": 2}]}}}),
            Some(&schema),
        );
        assert_eq!(
            stored["byName"]["k"]["xs"],
            json!([{"id": 1, "a": 1, "b": 2}])
        );
    }
}
