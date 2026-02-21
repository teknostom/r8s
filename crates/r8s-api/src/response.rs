use axum::http::StatusCode;
use axum::{body::Body, response::Response};
use r8s_store::error::StoreError;
use serde_json::Value;

pub fn object_response(object: &Value) -> Response {
    Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(object).unwrap()))
        .unwrap()
}

pub fn created_response(object: &Value) -> Response {
    Response::builder()
        .status(201)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(object).unwrap()))
        .unwrap()
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

    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

pub fn store_error_response(err: &StoreError) -> Response {
    let (code, reason) = match err {
        StoreError::AlreadyExists { .. } => (409, "AlreadyExists"),
        StoreError::NotFound { .. } => (404, "NotFound"),
        StoreError::Conflict { .. } => (409, "Conflict"),
        StoreError::Internal(..) => (500, "InternalError"),
    };
    status_error(
        StatusCode::from_u16(code).unwrap(),
        reason,
        &err.to_string(),
    )
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
    let mut bytes = serde_json::to_vec(&frame).unwrap();
    bytes.push(b'\n');
    bytes
}
