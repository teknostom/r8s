use axum::http::StatusCode;
use axum::{body::Body, response::Response};
use r8s_store::error::StoreError;
use serde_json::Value;

pub fn json_response(status: u16, object: &Value) -> Response {
    let body = serde_json::to_vec(object).unwrap_or_else(|e| {
        format!(r#"{{"kind":"Status","status":"Failure","message":"serialization error: {e}","code":500}}"#).into_bytes()
    });
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .expect("valid response")
}

pub fn object_response(object: &Value) -> Response {
    json_response(200, object)
}

pub fn created_response(object: &Value) -> Response {
    json_response(201, object)
}

pub fn list_response(
    api_version: &str,
    kind: &str,
    rv: u64,
    continue_token: Option<&str>,
    items: Vec<Value>,
) -> Response {
    let mut metadata = serde_json::json!({
        "resourceVersion": rv.to_string(),
    });
    if let Some(token) = continue_token {
        metadata["continue"] = serde_json::json!(token);
    }

    let body = serde_json::json!({
        "apiVersion": api_version,
        "kind": format!("{kind}List"),
        "metadata": metadata,
        "items": items,
    });

    object_response(&body)
}

pub fn status_error(status: StatusCode, reason: &str, message: &str) -> Response {
    let body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Status",
        "status": "Failure",
        "message": message,
        "reason": reason,
        "code": status.as_u16(),
    });

    json_response(status.as_u16(), &body)
}

pub fn store_error_response(err: &StoreError) -> Response {
    let (status, reason) = match err {
        StoreError::AlreadyExists { .. } => (StatusCode::CONFLICT, "AlreadyExists"),
        StoreError::NotFound { .. } => (StatusCode::NOT_FOUND, "NotFound"),
        StoreError::Conflict { .. } => (StatusCode::CONFLICT, "Conflict"),
        StoreError::Internal(..) => (StatusCode::INTERNAL_SERVER_ERROR, "InternalError"),
    };
    status_error(status, reason, &err.to_string())
}

pub fn anyhow_error_response(err: anyhow::Error) -> Response {
    match err.downcast_ref::<StoreError>() {
        Some(store_err) => store_error_response(store_err),
        None => status_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &err.to_string(),
        ),
    }
}

pub fn watch_event_line(event_type: &str, object: &Value) -> Vec<u8> {
    let frame = serde_json::json!({
        "type": event_type,
        "object": object,
    });
    let mut bytes = serde_json::to_vec(&frame).unwrap_or_default();
    bytes.push(b'\n');
    bytes
}
