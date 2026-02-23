//! Built-in L7 ingress controller.
//!
//! Watches Ingress and Endpoints resources in the store, builds a route table,
//! and runs an HTTP reverse proxy on port 80. Backs off for Ingresses that
//! specify an `ingressClassName` other than `"r8s"`.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use http_body_util::{Either, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1 as server_http1;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use r8s_store::Store;
use r8s_types::{Endpoints, GroupVersionResource, Ingress};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

struct Backend {
    addr: SocketAddr,
}

struct Route {
    host: Option<String>,
    path: String,
    prefix: bool,
    backends: Vec<Backend>,
    counter: AtomicUsize,
}

impl Route {
    fn next_backend(&self) -> Option<SocketAddr> {
        if self.backends.is_empty() {
            return None;
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.backends.len();
        Some(self.backends[idx].addr)
    }

    fn matches(&self, host: Option<&str>, path: &str) -> bool {
        if let Some(ref route_host) = self.host {
            match host {
                Some(h) if h == route_host => {}
                _ => return false,
            }
        }
        if self.prefix {
            path.starts_with(&self.path) || (self.path == "/" && path.is_empty())
        } else {
            path == self.path
        }
    }
}

type RouteTable = Vec<Route>;

fn rebuild_routes(store: &Store) -> Vec<Route> {
    let ingresses = match store.list_as::<Ingress>(&GroupVersionResource::ingresses(), None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("ingress: failed to list ingresses: {e}");
            return Vec::new();
        }
    };

    let mut routes = Vec::new();

    for ing in &ingresses {

        let spec = match ing.spec.as_ref() {
            Some(s) => s,
            None => continue,
        };

        // Backoff: skip if ingressClassName is set and not "r8s"
        if let Some(ref class) = spec.ingress_class_name {
            if class != "r8s" {
                continue;
            }
        }

        let ing_ns = ing.metadata.namespace.as_deref().unwrap_or("default");

        let rules = spec.rules.as_deref().unwrap_or_default();

        for rule in rules {
            let host = rule.host.clone();

            let http = match &rule.http {
                Some(h) => h,
                None => continue,
            };

            for path_entry in &http.paths {
                let path = path_entry
                    .path
                    .clone()
                    .unwrap_or_else(|| "/".to_string());
                let prefix = path_entry.path_type.as_str() != "Exact";

                let svc_backend = match &path_entry.backend.service {
                    Some(s) => s,
                    None => continue,
                };
                let svc_port = match svc_backend
                    .port
                    .as_ref()
                    .and_then(|p| p.number)
                {
                    Some(p) => p as u16,
                    None => continue,
                };

                // Resolve backends from Endpoints
                let ep_gvr = GroupVersionResource::endpoints();
                let ep_ref = r8s_store::backend::ResourceRef {
                    gvr: &ep_gvr,
                    namespace: Some(ing_ns),
                    name: &svc_backend.name,
                };

                let backends = match store.get_as::<Endpoints>(&ep_ref) {
                    Ok(Some(ep)) => {
                        let mut addrs = Vec::new();
                        for subset in ep.subsets.as_deref().unwrap_or_default() {
                            let target_port = subset
                                .ports
                                .as_ref()
                                .and_then(|p| p.first())
                                .map(|p| p.port as u16)
                                .unwrap_or(svc_port);

                            for addr in subset.addresses.as_deref().unwrap_or_default() {
                                if let Ok(ip) = addr.ip.parse() {
                                    addrs.push(Backend {
                                        addr: SocketAddr::new(ip, target_port),
                                    });
                                }
                            }
                        }
                        addrs
                    }
                    _ => Vec::new(),
                };

                if !backends.is_empty() {
                    routes.push(Route {
                        host: host.clone(),
                        path,
                        prefix,
                        backends,
                        counter: AtomicUsize::new(0),
                    });
                }
            }
        }
    }

    // Sort longest path first for correct prefix matching
    routes.sort_by(|a, b| b.path.len().cmp(&a.path.len()));
    routes
}

/// Response body type: either a static error message or a streamed backend response.
type ProxyBody = Either<Full<Bytes>, Incoming>;

fn error_response(status: u16, msg: &str) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .body(Either::Left(Full::new(Bytes::from(msg.to_string()))))
        .expect("valid response")
}

async fn proxy_request(
    req: Request<Incoming>,
    routes: Arc<RwLock<RouteTable>>,
) -> Result<Response<ProxyBody>, hyper::Error> {
    let host = req
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .map(|h| h.split(':').next().unwrap_or(h).to_string());
    let path = req.uri().path().to_string();

    let backend_addr = {
        let table = routes.read().unwrap();
        table
            .iter()
            .find(|r| r.matches(host.as_deref(), &path))
            .and_then(|r| r.next_backend())
    };

    let backend_addr = match backend_addr {
        Some(addr) => addr,
        None => return Ok(error_response(404, "no matching ingress route\n")),
    };

    // Connect to backend
    let stream = match TcpStream::connect(backend_addr).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("ingress: backend {backend_addr} connect failed: {e}");
            return Ok(error_response(502, "bad gateway\n"));
        }
    };

    let io = TokioIo::new(stream);
    let (mut sender, conn) = match hyper::client::conn::http1::handshake(io).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("ingress: backend {backend_addr} handshake failed: {e}");
            return Ok(error_response(502, "bad gateway\n"));
        }
    };
    tokio::spawn(conn);

    match sender.send_request(req).await {
        Ok(resp) => {
            // Stream the response body directly -- no buffering
            let (parts, body) = resp.into_parts();
            Ok(Response::from_parts(parts, Either::Right(body)))
        }
        Err(e) => {
            tracing::warn!("ingress: backend {backend_addr} request failed: {e}");
            Ok(error_response(502, "bad gateway\n"))
        }
    }
}

pub async fn run_ingress_proxy(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    let routes: Arc<RwLock<RouteTable>> = Arc::new(RwLock::new(Vec::new()));

    // Initial route build
    *routes.write().unwrap() = rebuild_routes(&store);

    // Spawn watcher task
    let watcher_routes = routes.clone();
    let watcher_store = store.clone();
    let watcher_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let mut ing_rx = watcher_store.watch(&GroupVersionResource::ingresses());
        let mut ep_rx = watcher_store.watch(&GroupVersionResource::endpoints());

        loop {
            tokio::select! {
                _ = watcher_shutdown.cancelled() => return,
                event = ing_rx.recv() => {
                    match event {
                        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                            *watcher_routes.write().unwrap() = rebuild_routes(&watcher_store);
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
                event = ep_rx.recv() => {
                    match event {
                        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                            *watcher_routes.write().unwrap() = rebuild_routes(&watcher_store);
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        }
    });

    // Start HTTP server on port 80
    let listener = TcpListener::bind("0.0.0.0:80").await?;
    tracing::info!("ingress proxy listening on :80");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("ingress proxy shutting down");
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, _addr) = accepted?;
                let routes = routes.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let service = hyper::service::service_fn(move |req| {
                        proxy_request(req, routes.clone())
                    });
                    if let Err(e) = server_http1::Builder::new().serve_connection(io, service).await {
                        if !e.is_incomplete_message() {
                            tracing::debug!("ingress: connection error: {e}");
                        }
                    }
                });
            }
        }
    }
}
