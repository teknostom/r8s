use serde_json::Value;

pub fn json_merge_patch(target: &mut Value, patch: &Value) {
    if let Some(patch_obj) = patch.as_object() {
        if !target.is_object() {
            *target = serde_json::json!({});
        }
        let Some(target_obj) = target.as_object_mut() else {
            return;
        };
        for (key, value) in patch_obj {
            if value.is_null() {
                target_obj.remove(key);
            } else {
                let entry = target_obj
                    .entry(key.clone())
                    .or_insert(serde_json::json!(null));
                json_merge_patch(entry, value);
            }
        }
    } else {
        *target = patch.clone();
    }
}
