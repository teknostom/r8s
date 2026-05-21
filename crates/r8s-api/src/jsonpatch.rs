//! RFC 6902 JSON Patch — used to apply mutating admission webhook responses.
//!
//! Webhooks send back a base64-encoded JSONPatch array; we decode and apply it
//! to the object before passing it on to the next webhook (or to the store).

use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum JsonPatchError {
    #[error("invalid path '{0}': {1}")]
    InvalidPath(String, &'static str),
    #[error("path '{0}' does not exist")]
    PathNotFound(String),
    #[error("test op failed at '{0}'")]
    TestFailed(String),
    #[error("malformed patch op: {0}")]
    Malformed(String),
}

pub fn apply(target: &mut Value, patch: &Value) -> Result<(), JsonPatchError> {
    let ops = patch
        .as_array()
        .ok_or_else(|| JsonPatchError::Malformed("patch must be an array".into()))?;
    for op in ops {
        apply_op(target, op)?;
    }
    Ok(())
}

fn apply_op(target: &mut Value, op: &Value) -> Result<(), JsonPatchError> {
    let kind = op
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or_else(|| JsonPatchError::Malformed("missing 'op'".into()))?;
    let path = op
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| JsonPatchError::Malformed("missing 'path'".into()))?
        .to_string();
    match kind {
        "add" => {
            let value = op
                .get("value")
                .cloned()
                .ok_or_else(|| JsonPatchError::Malformed("add missing 'value'".into()))?;
            op_add(target, &path, value)
        }
        "remove" => op_remove(target, &path).map(|_| ()),
        "replace" => {
            let value = op
                .get("value")
                .cloned()
                .ok_or_else(|| JsonPatchError::Malformed("replace missing 'value'".into()))?;
            op_replace(target, &path, value)
        }
        "move" => {
            let from = op
                .get("from")
                .and_then(|v| v.as_str())
                .ok_or_else(|| JsonPatchError::Malformed("move missing 'from'".into()))?
                .to_string();
            let value = op_remove(target, &from)?;
            op_add(target, &path, value)
        }
        "copy" => {
            let from = op
                .get("from")
                .and_then(|v| v.as_str())
                .ok_or_else(|| JsonPatchError::Malformed("copy missing 'from'".into()))?;
            let value = resolve_pointer(target, from)?.clone();
            op_add(target, &path, value)
        }
        "test" => {
            let want = op
                .get("value")
                .ok_or_else(|| JsonPatchError::Malformed("test missing 'value'".into()))?;
            let got = resolve_pointer(target, &path)?;
            if got == want {
                Ok(())
            } else {
                Err(JsonPatchError::TestFailed(path))
            }
        }
        other => Err(JsonPatchError::Malformed(format!("unknown op '{other}'"))),
    }
}

/// Split a JSON Pointer (RFC 6901) into its individual tokens with `~`/`/`
/// unescaping. `""` → `[]`, `"/"` → `[""]`, `"/a/b"` → `["a", "b"]`.
fn parse_pointer(path: &str) -> Result<Vec<String>, JsonPatchError> {
    if path.is_empty() {
        return Ok(Vec::new());
    }
    if !path.starts_with('/') {
        return Err(JsonPatchError::InvalidPath(
            path.into(),
            "must start with '/'",
        ));
    }
    Ok(path[1..]
        .split('/')
        .map(|seg| seg.replace("~1", "/").replace("~0", "~"))
        .collect())
}

fn resolve_pointer<'a>(target: &'a Value, path: &str) -> Result<&'a Value, JsonPatchError> {
    let tokens = parse_pointer(path)?;
    let mut cur = target;
    for tok in &tokens {
        cur = match cur {
            Value::Object(map) => map
                .get(tok)
                .ok_or_else(|| JsonPatchError::PathNotFound(path.into()))?,
            Value::Array(arr) => {
                let idx: usize = tok
                    .parse()
                    .map_err(|_| JsonPatchError::InvalidPath(path.into(), "expected index"))?;
                arr.get(idx)
                    .ok_or_else(|| JsonPatchError::PathNotFound(path.into()))?
            }
            _ => return Err(JsonPatchError::PathNotFound(path.into())),
        };
    }
    Ok(cur)
}

fn op_add(target: &mut Value, path: &str, value: Value) -> Result<(), JsonPatchError> {
    let tokens = parse_pointer(path)?;
    if tokens.is_empty() {
        *target = value;
        return Ok(());
    }
    let (last, rest) = tokens.split_last().expect("checked non-empty");
    let parent = traverse_mut(target, rest, path)?;
    match parent {
        Value::Object(map) => {
            map.insert(last.clone(), value);
            Ok(())
        }
        Value::Array(arr) => {
            if last == "-" {
                arr.push(value);
                return Ok(());
            }
            let idx: usize = last
                .parse()
                .map_err(|_| JsonPatchError::InvalidPath(path.into(), "expected index or '-'"))?;
            if idx > arr.len() {
                return Err(JsonPatchError::PathNotFound(path.into()));
            }
            arr.insert(idx, value);
            Ok(())
        }
        _ => Err(JsonPatchError::PathNotFound(path.into())),
    }
}

fn op_remove(target: &mut Value, path: &str) -> Result<Value, JsonPatchError> {
    let tokens = parse_pointer(path)?;
    if tokens.is_empty() {
        return Err(JsonPatchError::InvalidPath(
            path.into(),
            "cannot remove root",
        ));
    }
    let (last, rest) = tokens.split_last().expect("checked non-empty");
    let parent = traverse_mut(target, rest, path)?;
    match parent {
        Value::Object(map) => map
            .remove(last)
            .ok_or_else(|| JsonPatchError::PathNotFound(path.into())),
        Value::Array(arr) => {
            let idx: usize = last
                .parse()
                .map_err(|_| JsonPatchError::InvalidPath(path.into(), "expected index"))?;
            if idx >= arr.len() {
                return Err(JsonPatchError::PathNotFound(path.into()));
            }
            Ok(arr.remove(idx))
        }
        _ => Err(JsonPatchError::PathNotFound(path.into())),
    }
}

fn op_replace(target: &mut Value, path: &str, value: Value) -> Result<(), JsonPatchError> {
    op_remove(target, path)?;
    op_add(target, path, value)
}

fn traverse_mut<'a>(
    target: &'a mut Value,
    tokens: &[String],
    full_path: &str,
) -> Result<&'a mut Value, JsonPatchError> {
    let mut cur = target;
    for tok in tokens {
        cur = match cur {
            Value::Object(map) => map
                .get_mut(tok)
                .ok_or_else(|| JsonPatchError::PathNotFound(full_path.into()))?,
            Value::Array(arr) => {
                let idx: usize = tok
                    .parse()
                    .map_err(|_| JsonPatchError::InvalidPath(full_path.into(), "expected index"))?;
                arr.get_mut(idx)
                    .ok_or_else(|| JsonPatchError::PathNotFound(full_path.into()))?
            }
            _ => return Err(JsonPatchError::PathNotFound(full_path.into())),
        };
    }
    Ok(cur)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn assert_patch(target: Value, patch: Value, expected: Value) {
        let mut t = target;
        apply(&mut t, &patch).expect("apply");
        assert_eq!(t, expected);
    }

    #[test]
    fn add_object_field() {
        assert_patch(
            json!({"a": 1}),
            json!([{"op": "add", "path": "/b", "value": 2}]),
            json!({"a": 1, "b": 2}),
        );
    }

    #[test]
    fn add_to_array_with_dash() {
        assert_patch(
            json!({"l": [1, 2]}),
            json!([{"op": "add", "path": "/l/-", "value": 3}]),
            json!({"l": [1, 2, 3]}),
        );
    }

    #[test]
    fn replace_nested() {
        assert_patch(
            json!({"a": {"b": 1}}),
            json!([{"op": "replace", "path": "/a/b", "value": 2}]),
            json!({"a": {"b": 2}}),
        );
    }

    #[test]
    fn remove_then_add_pattern() {
        // Typical mutating-webhook patch: replace by remove+add.
        assert_patch(
            json!({"k": "old"}),
            json!([
                {"op": "remove", "path": "/k"},
                {"op": "add", "path": "/k", "value": "new"}
            ]),
            json!({"k": "new"}),
        );
    }

    #[test]
    fn pointer_escapes() {
        let mut t = json!({"a/b": 1, "~tilde": 2});
        let v = resolve_pointer(&t, "/a~1b").unwrap();
        assert_eq!(*v, json!(1));
        let v = resolve_pointer(&t, "/~0tilde").unwrap();
        assert_eq!(*v, json!(2));
        // remove with escape too
        let removed = op_remove(&mut t, "/a~1b").unwrap();
        assert_eq!(removed, json!(1));
    }

    #[test]
    fn move_op() {
        assert_patch(
            json!({"a": 1, "b": 2}),
            json!([{"op": "move", "from": "/a", "path": "/c"}]),
            json!({"b": 2, "c": 1}),
        );
    }

    #[test]
    fn test_op_succeeds() {
        let mut t = json!({"x": "ok"});
        apply(&mut t, &json!([{"op": "test", "path": "/x", "value": "ok"}])).unwrap();
    }

    #[test]
    fn test_op_fails() {
        let mut t = json!({"x": "ok"});
        let err = apply(&mut t, &json!([{"op": "test", "path": "/x", "value": "no"}])).unwrap_err();
        assert!(matches!(err, JsonPatchError::TestFailed(_)));
    }

    #[test]
    fn realistic_cert_manager_mutation() {
        // cert-manager's mutating webhook typically fills in
        // spec.username/groups/uid from the AdmissionReview's userInfo.
        let target = json!({
            "apiVersion": "cert-manager.io/v1",
            "kind": "CertificateRequest",
            "metadata": {"name": "test"},
            "spec": {"request": "<csr>", "issuerRef": {"name": "ca"}}
        });
        let patch = json!([
            {"op": "add", "path": "/spec/username", "value": "system:admin"},
            {"op": "add", "path": "/spec/groups", "value": ["system:masters"]},
            {"op": "add", "path": "/spec/uid", "value": "abc-123"}
        ]);
        let expected = json!({
            "apiVersion": "cert-manager.io/v1",
            "kind": "CertificateRequest",
            "metadata": {"name": "test"},
            "spec": {
                "request": "<csr>",
                "issuerRef": {"name": "ca"},
                "username": "system:admin",
                "groups": ["system:masters"],
                "uid": "abc-123"
            }
        });
        assert_patch(target, patch, expected);
    }
}
