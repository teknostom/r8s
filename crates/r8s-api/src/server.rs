use std::{net::SocketAddr, sync::Arc};
use tokio_util::sync::CancellationToken;

use axum::{
    Extension, Router,
    extract::{DefaultBodyLimit, Query, Request, State},
    http::HeaderMap,
    response::Response,
    routing::{get, post},
};
use hyper::{Method, StatusCode};
use r8s_store::Store;
use r8s_types::{GroupVersionResource, registry::ResourceRegistry};

use crate::{
    auth::{self_subject_access_review, self_subject_rules_review},
    discovery::{
        ApiState, AppState, get_api_groups, get_api_versions, get_core_v1_resources,
        get_group_version_resources, get_version,
    },
    handler::{
        RouteContext, create_cluster, create_ns, delete_cluster, delete_ns, get_cluster, get_ns,
        list_all_ns, list_cluster, list_ns, patch_cluster, patch_ns, pod_logs_ns, update_cluster,
        update_ns,
        // _impl functions for dynamic dispatch
        create_impl, delete_impl, get_impl, list_impl, patch_impl, require_json, update_impl,
    },
    params::ListParams,
    response::status_error,
};

async fn log_requests(req: Request, next: axum::middleware::Next) -> axum::response::Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let resp = next.run(req).await;
    tracing::info!("{} {} -> {}", method, uri, resp.status());
    resp
}

fn not_found() -> Response {
    status_error(
        StatusCode::NOT_FOUND,
        "NotFound",
        "the server could not find the requested resource",
    )
}

/// Parsed API path for dynamic dispatch.
struct ApiPath {
    group: String,
    version: String,
    resource: String,
    namespace: Option<String>,
    name: Option<String>,
}

/// Parse `/apis/{group}/{version}/...` paths into structured components.
fn parse_api_path(path: &str) -> Option<ApiPath> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    // /apis/{group}/{version}/{resource}
    // /apis/{group}/{version}/{resource}/{name}
    // /apis/{group}/{version}/namespaces/{ns}/{resource}
    // /apis/{group}/{version}/namespaces/{ns}/{resource}/{name}
    if parts.len() < 4 || parts[0] != "apis" {
        return None;
    }
    let group = parts[1].to_string();
    let version = parts[2].to_string();

    if parts.len() >= 6 && parts[3] == "namespaces" {
        // Namespaced: /apis/g/v/namespaces/{ns}/{resource}[/{name}]
        let ns = parts[4].to_string();
        let resource = parts[5].to_string();
        let name = parts.get(6).map(|s| s.to_string());
        Some(ApiPath {
            group,
            version,
            resource,
            namespace: Some(ns),
            name,
        })
    } else {
        // Cluster-scoped: /apis/g/v/{resource}[/{name}]
        let resource = parts[3].to_string();
        let name = parts.get(4).map(|s| s.to_string());
        Some(ApiPath {
            group,
            version,
            resource,
            namespace: None,
            name,
        })
    }
}

/// Dynamic dispatch fallback: handles CRD resource requests that don't have static routes.
async fn dynamic_dispatch(
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
    headers: HeaderMap,
    req: Request,
) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    let api_path = match parse_api_path(&path) {
        Some(p) => p,
        None => return not_found(),
    };

    let gvr = GroupVersionResource::new(&api_path.group, &api_path.version, &api_path.resource);
    let rt = match state.registry.get_by_gvr(&gvr) {
        Some(rt) => rt,
        None => return not_found(),
    };

    let ctx = RouteContext {
        resource_type: rt,
    };

    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();

    match (method, api_path.name) {
        (Method::GET, None) => list_impl(&state, &ctx, api_path.namespace.as_deref(), params, &headers),
        (Method::POST, None) => {
            let json = match require_json(&headers, &body) {
                Ok(v) => v,
                Err(resp) => return *resp,
            };
            create_impl(&state, &ctx, api_path.namespace.as_deref(), json)
        }
        (Method::GET, Some(ref name)) => {
            get_impl(&state, &ctx, api_path.namespace.as_deref(), name, &headers)
        }
        (Method::PUT, Some(ref name)) => {
            let json = match require_json(&headers, &body) {
                Ok(v) => v,
                Err(resp) => return *resp,
            };
            update_impl(&state, &ctx, api_path.namespace.as_deref(), name, json)
        }
        (Method::PATCH, Some(ref name)) => {
            patch_impl(&state, &ctx, api_path.namespace.as_deref(), name, body)
        }
        (Method::DELETE, Some(ref name)) => {
            delete_impl(&state, &ctx, api_path.namespace.as_deref(), name)
        }
        _ => not_found(),
    }
}

/// The K8s-compatible API server built on axum.
pub struct ApiServer {
    state: AppState,
}

impl ApiServer {
    pub fn new(store: Store, registry: ResourceRegistry, data_dir: std::path::PathBuf) -> Self {
        Self {
            state: Arc::new(ApiState {
                store,
                registry,
                data_dir,
                next_cluster_ip: std::sync::atomic::AtomicU32::new(2),
            }),
        }
    }

    pub async fn serve(self, addr: SocketAddr, shutdown: CancellationToken) -> anyhow::Result<()> {
        let mut router = Router::new()
            .route("/version", get(get_version))
            .route("/api", get(get_api_versions))
            .route("/api/v1", get(get_core_v1_resources))
            .route("/apis", get(get_api_groups))
            .route("/apis/{group}/{version}", get(get_group_version_resources))
            .route(
                "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
                post(self_subject_access_review),
            )
            .route(
                "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews",
                post(self_subject_rules_review),
            );
        for rt in self.state.registry.iter() {
            let ctx = RouteContext {
                resource_type: Arc::clone(&rt),
            };
            let base = if rt.gvr.group.is_empty() {
                format!("/api/{}", rt.gvr.version)
            } else {
                format!("/apis/{}/{}", rt.gvr.group, rt.gvr.version)
            };

            if rt.namespaced {
                let collection = format!("{base}/namespaces/{{ns}}/{}", rt.gvr.resource);
                let item = format!("{base}/namespaces/{{ns}}/{}/{{name}}", rt.gvr.resource);
                let all_ns = format!("{base}/{}", rt.gvr.resource);
                router = router
                    .route(
                        &collection,
                        get(list_ns).post(create_ns).layer(Extension(ctx.clone())),
                    )
                    .route(
                        &item,
                        get(get_ns)
                            .put(update_ns)
                            .patch(patch_ns)
                            .delete(delete_ns)
                            .layer(Extension(ctx.clone())),
                    )
                    .route(&all_ns, get(list_all_ns).layer(Extension(ctx)));
                if rt.gvr.resource == "pods" {
                    let log_route =
                        format!("{base}/namespaces/{{ns}}/{}/{{name}}/log", rt.gvr.resource);
                    router = router.route(&log_route, get(pod_logs_ns))
                }
            } else {
                let collection = format!("{base}/{}", rt.gvr.resource);
                let item = format!("{base}/{}/{{name}}", rt.gvr.resource);
                router = router
                    .route(
                        &collection,
                        get(list_cluster)
                            .post(create_cluster)
                            .layer(Extension(ctx.clone())),
                    )
                    .route(
                        &item,
                        get(get_cluster)
                            .put(update_cluster)
                            .patch(patch_cluster)
                            .delete(delete_cluster)
                            .layer(Extension(ctx)),
                    );
            }
        }

        let router = router
            .fallback(dynamic_dispatch)
            .with_state(self.state)
            .layer(DefaultBodyLimit::max(1024 * 1024)) // 1MB
            .layer(axum::middleware::from_fn(log_requests));
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!("API server listening on {addr}");
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await?;
        Ok(())
    }
}
