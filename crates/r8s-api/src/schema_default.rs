//! Apply `default` values from a CRD's openAPIV3Schema to a custom resource.
//!
//! Kubernetes' apiserver fills in schema defaults for absent fields at admission
//! time (and on read from storage). Operators rely on this heavily — e.g. the
//! prometheus-operator reads `spec.scrapeInterval` expecting the CRD's
//! `default: "30s"` to be present; without defaulting it sees an empty string
//! and emits `scrape_interval: ""`, which Prometheus then rejects.
//!
//! We walk the structural schema recursively: for object schemas, every
//! property that's absent but has a `default` is inserted (then descended into,
//! so nested defaults under that default also land); present properties are
//! descended into so their own nested defaults fill in. Array `items` schemas
//! are applied to each element.

use serde_json::Value;

pub fn apply_defaults(object: &mut Value, schema: &Value) {
    walk(object, schema);
}

fn walk(value: &mut Value, schema: &Value) {
    let Some(schema_obj) = schema.as_object() else {
        return;
    };

    // Don't descend into opaque subtrees — we can't reason about their shape.
    if schema_obj
        .get("x-kubernetes-preserve-unknown-fields")
        .and_then(|v| v.as_bool())
        == Some(true)
    {
        return;
    }

    match schema_obj.get("type").and_then(|t| t.as_str()) {
        Some("object") => {
            let Some(props) = schema_obj.get("properties").and_then(|p| p.as_object()) else {
                return;
            };
            let Some(obj) = value.as_object_mut() else {
                return;
            };
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
        Some("array") => {
            if let Some(item_schema) = schema_obj.get("items")
                && let Some(arr) = value.as_array_mut()
            {
                for item in arr.iter_mut() {
                    walk(item, item_schema);
                }
            }
        }
        _ => {}
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
