use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;
use base64::Engine;
use hyper::StatusCode;
use r8s_store::Store;
use r8s_types::GroupVersionResource;

use axum::extract::Path;
use r8s_store::backend::ResourceRef;
use r8s_types::sa_token::{Binding, SigningKey, claims};

use crate::discovery::AppState;
use crate::handler::wants_table;
use crate::response::{object_response, status_error};

/// `TokenRequest` (`serviceaccounts/{name}/token`): a bound token for the
/// service account, signed with the cluster key, for the audiences and
/// lifetime asked (an hour by default, at least ten minutes), optionally
/// bound to a Pod.
pub async fn token_request(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(resp) = reject_table(&headers) {
        return resp;
    }
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let sa_gvr = r8s_types::GroupVersionResource::service_accounts();
    let sa = match state.store.get(&ResourceRef {
        gvr: &sa_gvr,
        namespace: Some(&namespace),
        name: &name,
    }) {
        Ok(Some(sa)) => sa,
        _ => {
            return status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("serviceaccounts \"{name}\" not found"),
            );
        }
    };
    let spec = body
        .get("spec")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let audiences: Vec<String> = spec["audiences"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let expiration = spec["expirationSeconds"].as_i64().unwrap_or(3600).max(600);
    let mut binding = Binding {
        namespace: namespace.clone(),
        service_account: name.clone(),
        service_account_uid: sa["metadata"]["uid"].as_str().unwrap_or("").to_string(),
        pod: None,
        node: None,
    };
    let bound = &spec["boundObjectRef"];
    if bound["kind"].as_str() == Some("Pod")
        && let Some(pod_name) = bound["name"].as_str()
    {
        let pods = r8s_types::GroupVersionResource::pods();
        let pod = match state.store.get(&ResourceRef {
            gvr: &pods,
            namespace: Some(&namespace),
            name: pod_name,
        }) {
            Ok(Some(p)) => p,
            _ => {
                return status_error(
                    StatusCode::NOT_FOUND,
                    "NotFound",
                    &format!("pods \"{pod_name}\" not found"),
                );
            }
        };
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        if bound["uid"].as_str().is_some_and(|u| u != uid) {
            return status_error(
                StatusCode::CONFLICT,
                "Conflict",
                &format!("pods \"{pod_name}\" uid does not match the bound object reference"),
            );
        }
        binding.pod = Some((pod_name.to_string(), uid.to_string()));
    }
    let key = match SigningKey::load_or_create(&state.data_dir) {
        Ok(k) => k,
        Err(e) => {
            return status_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("token signing key: {e}"),
            );
        }
    };
    let now = chrono::Utc::now();
    let token = key.mint(&claims(&binding, &audiences, expiration, now.timestamp()));
    let expires = now + chrono::Duration::seconds(expiration);
    object_response(&serde_json::json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenRequest",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "creationTimestamp": r8s_types::Time(now),
        },
        "spec": {
            "audiences": audiences,
            "expirationSeconds": expiration,
            "boundObjectRef": bound,
        },
        "status": {
            "token": token,
            "expirationTimestamp": r8s_types::Time(expires),
        },
    }))
}

/// The bearer token the generated admin kubeconfig carries.
const ADMIN_TOKEN: &str = "r8s-admin";
const SA_TOKEN_SECRET_TYPE: &str = "kubernetes.io/service-account-token";

/// Who a token belongs to, and the audiences it was issued for (`None` for
/// tokens without any).
struct Identity {
    user: serde_json::Value,
    audiences: Option<Vec<String>>,
}

/// The identity behind a bearer token: the cluster admin token, a bound token
/// the cluster key signed, or a service account whose token Secret holds it.
fn identify(state: &AppState, token: &str) -> Option<Identity> {
    if token == ADMIN_TOKEN {
        return Some(Identity {
            user: serde_json::json!({
                "username": "r8s-admin",
                "groups": ["system:masters", "system:authenticated"],
            }),
            audiences: None,
        });
    }
    if let Ok(key) = SigningKey::load_or_create(&state.data_dir)
        && let Ok(claims) = key.verify(token, chrono::Utc::now().timestamp())
    {
        return Some(bound_identity(&claims));
    }
    legacy_identity(&state.store, token).map(|user| Identity {
        user,
        audiences: None,
    })
}

/// The user a bound token's claims describe, with the extras upstream
/// attaches: the credential id and the bound pod and node.
fn bound_identity(claims: &serde_json::Value) -> Identity {
    let k8s = &claims["kubernetes.io"];
    let namespace = k8s["namespace"].as_str().unwrap_or("");
    let mut extra = serde_json::Map::new();
    if let Some(jti) = claims["jti"].as_str() {
        extra.insert(
            "authentication.kubernetes.io/credential-id".into(),
            serde_json::json!([format!("JTI={jti}")]),
        );
    }
    for (object, key) in [("pod", "pod"), ("node", "node")] {
        for field in ["name", "uid"] {
            if let Some(v) = k8s[object][field].as_str() {
                extra.insert(
                    format!("authentication.kubernetes.io/{key}-{field}"),
                    serde_json::json!([v]),
                );
            }
        }
    }
    Identity {
        user: serde_json::json!({
            "username": claims["sub"].as_str().unwrap_or(""),
            "uid": k8s["serviceaccount"]["uid"].as_str().unwrap_or(""),
            "groups": [
                "system:serviceaccounts",
                format!("system:serviceaccounts:{namespace}"),
                "system:authenticated",
            ],
            "extra": extra,
        }),
        audiences: claims["aud"].as_array().map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        }),
    }
}

/// A legacy token: the one in a service account's token Secret.
fn legacy_identity(store: &Store, token: &str) -> Option<serde_json::Value> {
    let secrets = store
        .list(
            &GroupVersionResource::secrets(),
            None,
            None,
            None,
            None,
            None,
        )
        .ok()?;
    let b64 = base64::engine::general_purpose::STANDARD;
    let secret = secrets.items.iter().find(|s| {
        s.get("type").and_then(|t| t.as_str()) == Some(SA_TOKEN_SECRET_TYPE)
            && s.pointer("/data/token")
                .and_then(|v| v.as_str())
                .and_then(|v| b64.decode(v).ok())
                .is_some_and(|stored| stored == token.as_bytes())
    })?;
    let namespace = secret.pointer("/metadata/namespace")?.as_str()?;
    let annotations = secret.pointer("/metadata/annotations")?;
    let name = annotations
        .get("kubernetes.io/service-account.name")?
        .as_str()?;
    let uid = annotations
        .get("kubernetes.io/service-account.uid")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    Some(serde_json::json!({
        "username": format!("system:serviceaccount:{namespace}:{name}"),
        "uid": uid,
        "groups": [
            "system:serviceaccounts",
            format!("system:serviceaccounts:{namespace}"),
            "system:authenticated",
        ],
    }))
}

/// `TokenReview`: says who a bearer token belongs to, when it was issued for
/// one of the audiences the reviewer accepts (the API server's own when the
/// review names none).
pub async fn token_review(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(resp) = reject_table(&headers) {
        return resp;
    }
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let token = body
        .pointer("/spec/token")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let wanted: Vec<String> = body
        .pointer("/spec/audiences")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .filter(|a: &Vec<String>| !a.is_empty())
        .unwrap_or_else(|| vec![r8s_types::sa_token::ISSUER.to_string()]);
    let status = match identify(&state, token) {
        Some(Identity {
            user,
            audiences: None,
        }) => serde_json::json!({"authenticated": true, "user": user}),
        Some(Identity {
            user,
            audiences: Some(issued),
        }) => {
            let matched: Vec<&String> = issued.iter().filter(|a| wanted.contains(a)).collect();
            if matched.is_empty() {
                serde_json::json!({
                    "authenticated": false,
                    "user": {},
                    "error": format!("[invalid bearer token, token audiences {issued:?} is invalid for the target audiences {wanted:?}]"),
                })
            } else {
                serde_json::json!({"authenticated": true, "user": user, "audiences": matched})
            }
        }
        None => serde_json::json!({
            "authenticated": false,
            "user": {},
            "error": "[invalid bearer token, token not found]",
        }),
    };
    let mut spec = body.get("spec").cloned().unwrap_or_default();
    if let Some(obj) = spec.as_object_mut() {
        obj.remove("token");
    }
    object_response(&serde_json::json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "metadata": body.get("metadata").cloned().unwrap_or_else(|| serde_json::json!({})),
        "spec": spec,
        "status": status,
    }))
}

/// The review kinds have no table form; a Table `Accept` is answered 406 as
/// upstream does for backends without metadata.
fn reject_table(headers: &HeaderMap) -> Option<Response> {
    wants_table(headers).then(|| {
        status_error(
            StatusCode::NOT_ACCEPTABLE,
            "NotAcceptable",
            "only the following media types are accepted: application/json, application/yaml, \
             application/vnd.kubernetes.protobuf",
        )
    })
}

pub async fn self_subject_access_review(headers: HeaderMap, body: Bytes) -> Response {
    if let Some(resp) = reject_table(&headers) {
        return resp;
    }
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

/// `SubjectAccessReview` — extension apiservers (metrics-server) delegate
/// authorization here for the front-proxy user. r8s has no RBAC, so allow.
pub async fn subject_access_review(headers: HeaderMap, body: Bytes) -> Response {
    if let Some(resp) = reject_table(&headers) {
        return resp;
    }
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    object_response(&serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SubjectAccessReview",
        "metadata": {},
        "spec": body.get("spec").cloned().unwrap_or_default(),
        "status": {
            "allowed": true,
        },
    }))
}

/// `/.well-known/openid-configuration`: the service account issuer's OIDC
/// discovery document.
pub async fn openid_configuration() -> Response {
    object_response(&r8s_types::sa_token::openid_configuration())
}

/// `/openid/v1/jwks`: the issuer's signing key.
pub async fn openid_jwks(axum::extract::State(state): axum::extract::State<AppState>) -> Response {
    match SigningKey::load_or_create(&state.data_dir) {
        Ok(key) => {
            let mut resp = object_response(&serde_json::json!({"keys": [key.jwk()]}));
            resp.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/jwk-set+json"),
            );
            resp
        }
        Err(e) => crate::response::status_error(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &format!("signing key: {e}"),
        ),
    }
}

/// `localsubjectaccessreviews`: the namespaced review, allowed like the
/// rest since nothing is denied.
pub async fn local_subject_access_review(
    headers: HeaderMap,
    axum::extract::Path(namespace): axum::extract::Path<String>,
    body: Bytes,
) -> Response {
    if let Some(resp) = reject_table(&headers) {
        return resp;
    }
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    object_response(&serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "LocalSubjectAccessReview",
        "metadata": {"namespace": namespace},
        "spec": body.get("spec").cloned().unwrap_or_default(),
        "status": {
            "allowed": true,
        },
    }))
}

pub async fn self_subject_rules_review(headers: HeaderMap, body: Bytes) -> Response {
    if let Some(resp) = reject_table(&headers) {
        return resp;
    }
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
