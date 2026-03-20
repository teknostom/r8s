use axum::body::Bytes;
use axum::response::Response;

use crate::response::object_response;

pub async fn self_subject_access_review(body: Bytes) -> Response {
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    object_response(&serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectAccessReview",
        "metadata": {},
        "spec": body.get("spec").cloned().unwrap_or_default(),
        "status": {
            "allowed": true,
        },
    }))
}

pub async fn self_subject_rules_review(body: Bytes) -> Response {
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    object_response(&serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectRulesReview",
        "metadata": {},
        "spec": body.get("spec").cloned().unwrap_or_default(),
        "status": {
            "incomplete": false,
            "nonResourceRules": [{
                "verbs": ["*"],
                "nonResourceURLs": ["*"],
            }],
            "resourceRules": [{
                "verbs": ["*"],
                "apiGroups": ["*"],
                "resources": ["*"],
            }],
        },
    }))
}
