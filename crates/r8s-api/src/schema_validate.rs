//! Minimal CR-against-OpenAPI-v3-schema validator.
//!
//! kubectl 1.32 client-side validation does not enforce `enum` constraints
//! reliably (and `--validate=server` shifts the check to the API server),
//! so the upstream conformance suite expects the server to reject CRs that
//! violate the CRD's schema. We hand-roll enough of a validator to cover
//! the conformance cases: `enum`, recursive `type`/`properties`/`items`,
//! and `x-kubernetes-preserve-unknown-fields` (which silences validation
//! at that subtree).
//!
//! Intentionally tiny: a full JSON-schema validator is a large dependency
//! and the surface area we need is small. Extend as conformance demands.

use serde_json::Value;

pub fn validate(object: &Value, schema: &Value) -> Result<(), String> {
    walk(object, schema, "")
}

fn walk(value: &Value, schema: &Value, path: &str) -> Result<(), String> {
    let Some(obj) = schema.as_object() else {
        return Ok(());
    };
    // preserve-unknown-fields at any level disables further validation
    // for that subtree — that's its whole point.
    if obj
        .get("x-kubernetes-preserve-unknown-fields")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return Ok(());
    }
    if let Some(enums) = obj.get("enum").and_then(|v| v.as_array())
        && !enums.iter().any(|e| e == value)
    {
        let allowed: Vec<String> = enums
            .iter()
            .map(|v| v.to_string())
            .collect();
        return Err(format!(
            "{path}: Unsupported value: {value}: supported values: {}",
            allowed.join(", ")
        ));
    }
    match value {
        Value::Object(map) => {
            if let Some(props) = obj.get("properties").and_then(|p| p.as_object()) {
                for (k, v) in map {
                    if let Some(sub_schema) = props.get(k) {
                        let sub_path = if path.is_empty() {
                            k.clone()
                        } else {
                            format!("{path}.{k}")
                        };
                        walk(v, sub_schema, &sub_path)?;
                    }
                }
            }
        }
        Value::Array(items) => {
            if let Some(item_schema) = obj.get("items") {
                for (i, v) in items.iter().enumerate() {
                    let sub_path = format!("{path}[{i}]");
                    walk(v, item_schema, &sub_path)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn enum_accepts_match() {
        let schema = json!({
            "type": "object",
            "properties": {
                "feeling": {"type": "string", "enum": ["Great", "Down"]}
            }
        });
        assert!(validate(&json!({"feeling": "Great"}), &schema).is_ok());
    }

    #[test]
    fn enum_rejects_outside() {
        let schema = json!({
            "type": "object",
            "properties": {
                "feeling": {"type": "string", "enum": ["Great", "Down"]}
            }
        });
        let err = validate(&json!({"feeling": "Mediocre"}), &schema).unwrap_err();
        assert!(err.contains("feeling"), "{err}");
        assert!(err.contains("Great"), "{err}");
    }

    #[test]
    fn enum_inside_array_items() {
        let schema = json!({
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "properties": {
                        "bars": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "feeling": {"enum": ["Great", "Down"]}
                                }
                            }
                        }
                    }
                }
            }
        });
        let bad = json!({"spec": {"bars": [{"feeling": "Weird"}]}});
        let err = validate(&bad, &schema).unwrap_err();
        assert!(err.contains("spec.bars[0].feeling"), "{err}");
    }

    #[test]
    fn preserve_unknown_fields_short_circuits() {
        let schema = json!({
            "type": "object",
            "x-kubernetes-preserve-unknown-fields": true,
            "properties": {
                "feeling": {"enum": ["A"]}
            }
        });
        // Even though "feeling" violates the enum, preserve-unknown-fields
        // at this level disables validation.
        assert!(validate(&json!({"feeling": "X"}), &schema).is_ok());
    }

    #[test]
    fn unknown_object_keys_are_ignored() {
        let schema = json!({
            "type": "object",
            "properties": {"a": {"type": "string"}}
        });
        // We don't enforce "no additional properties" — only constraints.
        // Server-side strict structural validation is a separate concern.
        assert!(validate(&json!({"a": "x", "b": "y"}), &schema).is_ok());
    }
}
