//! Apply `default` values from a CRD's openAPIV3Schema to a custom resource.
//!
//! Walks the schema alongside the object: an absent property with a `default`
//! is inserted and then descended into, present properties are descended into,
//! and `additionalProperties` / `items` schemas apply to every value / element.
//! `x-kubernetes-preserve-unknown-fields` only affects pruning, so declared
//! properties under it still get their defaults.

use serde_json::Value;

pub fn apply_defaults(object: &mut Value, schema: &Value) {
    walk(object, schema);
}

fn walk(value: &mut Value, schema: &Value) {
    let Some(schema_obj) = schema.as_object() else {
        return;
    };

    if let Some(props) = schema_obj.get("properties").and_then(|p| p.as_object())
        && let Some(obj) = value.as_object_mut()
    {
        for (key, prop_schema) in props {
            match obj.get_mut(key) {
                Some(child) => walk(child, prop_schema),
                None => {
                    if let Some(default) = prop_schema.get("default") {
                        obj.insert(key.clone(), default.clone());
                        if let Some(child) = obj.get_mut(key) {
                            walk(child, prop_schema);
                        }
                    }
                }
            }
        }
    }
    if let Some(value_schema) = schema_obj
        .get("additionalProperties")
        .filter(|s| s.is_object())
        && let Some(obj) = value.as_object_mut()
    {
        for child in obj.values_mut() {
            walk(child, value_schema);
        }
    }
    if let Some(item_schema) = schema_obj.get("items")
        && let Some(arr) = value.as_array_mut()
    {
        for item in arr.iter_mut() {
            walk(item, item_schema);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fills_absent_field_with_default() {
        let schema = json!({
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "properties": {
                        "scrapeInterval": {"type": "string", "default": "30s"},
                        "evaluationInterval": {"type": "string", "default": "30s"}
                    }
                }
            }
        });
        let mut obj = json!({"spec": {"replicas": 1}});
        apply_defaults(&mut obj, &schema);
        assert_eq!(obj["spec"]["scrapeInterval"], "30s");
        assert_eq!(obj["spec"]["evaluationInterval"], "30s");
        // Present fields untouched.
        assert_eq!(obj["spec"]["replicas"], 1);
    }

    #[test]
    fn does_not_override_present_value() {
        let schema = json!({
            "type": "object",
            "properties": {
                "x": {"type": "string", "default": "d"}
            }
        });
        let mut obj = json!({"x": "set"});
        apply_defaults(&mut obj, &schema);
        assert_eq!(obj["x"], "set");
    }

    #[test]
    fn applies_defaults_inside_array_items() {
        let schema = json!({
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"weight": {"type": "integer", "default": 1}}
                    }
                }
            }
        });
        let mut obj = json!({"items": [{"name": "a"}, {"name": "b", "weight": 5}]});
        apply_defaults(&mut obj, &schema);
        assert_eq!(obj["items"][0]["weight"], 1);
        assert_eq!(obj["items"][1]["weight"], 5);
    }

    #[test]
    fn preserve_unknown_fields_root_still_defaults_declared_properties() {
        let schema = json!({
            "type": "object",
            "x-kubernetes-preserve-unknown-fields": true,
            "properties": {
                "a": {"type": "string", "default": "A"}
            }
        });
        let mut obj = json!({"unknown": 1});
        apply_defaults(&mut obj, &schema);
        assert_eq!(obj["a"], "A");
        assert_eq!(obj["unknown"], 1);
    }

    #[test]
    fn defaults_apply_inside_additional_properties_values() {
        let schema = json!({
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {"enabled": {"type": "boolean", "default": true}}
            }
        });
        let mut obj = json!({"x": {}, "y": {"enabled": false}});
        apply_defaults(&mut obj, &schema);
        assert_eq!(obj["x"]["enabled"], true);
        assert_eq!(obj["y"]["enabled"], false);
    }

    #[test]
    fn nested_default_object_is_filled_recursively() {
        // An absent field whose default is an object still gets its own nested
        // defaults applied.
        let schema = json!({
            "type": "object",
            "properties": {
                "tls": {
                    "type": "object",
                    "default": {},
                    "properties": {"mode": {"type": "string", "default": "strict"}}
                }
            }
        });
        let mut obj = json!({});
        apply_defaults(&mut obj, &schema);
        assert_eq!(obj["tls"]["mode"], "strict");
    }
}
