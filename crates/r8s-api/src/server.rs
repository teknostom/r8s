use std::{net::SocketAddr, sync::Arc};

use axum::{
    Extension, Router,
    extract::Request,
    routing::{get, post},
};
use hyper::StatusCode;
use r8s_store::Store;
use r8s_types::registry::ResourceRegistry;

use crate::{
    auth::{self_subject_access_review, self_subject_rules_review},
    discovery::{
        ApiState, AppState, get_api_groups, get_api_versions, get_core_v1_resources,
        get_group_version_resources, get_version,
    },
    handler::{
        RouteContext, create_cluster, create_ns, delete_cluster, delete_ns, get_cluster, get_ns,
        list_all_ns, list_cluster, list_ns, patch_cluster, patch_ns, update_cluster, update_ns, pod_logs_ns
    },
};

async fn log_requests(req: Request, next: axum::middleware::Next) -> axum::response::Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let resp = next.run(req).await;
    tracing::info!("{} {} -> {}", method, uri, resp.status());
    resp
}

async fn fallback(req: Request) -> (StatusCode, String) {
    tracing::warn!("unhandled: {} {}", req.method(), req.uri());
    (
        StatusCode::NOT_FOUND,
        "{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"status\":\"Failure\",\"message\":\"the server could not find the requested resource\",\"reason\":\"NotFound\",\"code\":404}".to_string(),
    )
}

/// The K8s-compatible API server built on axum.
pub struct ApiServer {
    state: AppState,
}

impl ApiServer {
    pub fn new(store: Store, registry: ResourceRegistry) -> Self {
        Self {
            state: Arc::new(ApiState { store, registry }),
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> anyhow::Result<()> {
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
                resource_type: Arc::new(rt.clone()),
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
                        let log_route = format!("{base}/namespaces/{{ns}}/{}/{{name}}/log", rt.gvr.resource);
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
            .with_state(self.state)
            .fallback(fallback)
            .layer(axum::middleware::from_fn(log_requests));
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!("API server listening on {addr}");
        axum::serve(listener, router).await?;
        Ok(())
    }
}
