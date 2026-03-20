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
            tracing::warn!("failed to list ingresses: {e}");
            return Vec::new();
        }
    };

    let mut routes = Vec::new();

    for ing in &ingresses {
        let spec = match ing.spec.as_ref() {
            Some(s) => s,
            None => continue,
        };

        if spec
            .ingress_class_name
            .as_deref()
            .is_some_and(|c| c != "r8s")
        {
            continue;
        }

        let ing_ns = ing.metadata.namespace.as_deref().unwrap_or("default");

        for rule in spec.rules.as_deref().unwrap_or_default() {
            let host = rule.host.clone();
            let http = match &rule.http {
                Some(h) => h,
                None => continue,
            };

            for path_entry in &http.paths {
                let path = path_entry.path.clone().unwrap_or_else(|| "/".to_string());
                let prefix = path_entry.path_type.as_str() != "Exact";

                let svc_backend = match &path_entry.backend.service {
                    Some(s) => s,
                    None => continue,
                };
                let svc_port = match svc_backend.port.as_ref().and_then(|p| p.number) {
                    Some(p) => p as u16,
                    None => continue,
                };

                let backends = resolve_backends(store, ing_ns, &svc_backend.name, svc_port);
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

    // Longest path first for correct prefix matching
    routes.sort_by_key(|r| std::cmp::Reverse(r.path.len()));
    routes
}

fn resolve_backends(
    store: &Store,
    namespace: &str,
    service_name: &str,
    service_port: u16,
) -> Vec<Backend> {
    let gvr = GroupVersionResource::endpoints();
    let ep_ref = r8s_store::backend::ResourceRef {
        gvr: &gvr,
        namespace: Some(namespace),
        name: service_name,
    };

    let ep: Endpoints = match store.get_as::<Endpoints>(&ep_ref) {
        Ok(Some(ep)) => ep,
        _ => return Vec::new(),
    };

    let mut backends = Vec::new();
    for subset in ep.subsets.as_deref().unwrap_or_default() {
        let target_port = subset
            .ports
            .as_ref()
            .and_then(|p| p.first())
            .map(|p| p.port as u16)
            .unwrap_or(service_port);

        for addr in subset.addresses.as_deref().unwrap_or_default() {
            if let Ok(ip) = addr.ip.parse() {
                backends.push(Backend {
                    addr: SocketAddr::new(ip, target_port),
                });
            }
        }
    }
    backends
}

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
        let table = routes.read().expect("route table lock poisoned");
        table
            .iter()
            .find(|r| r.matches(host.as_deref(), &path))
            .and_then(|r| r.next_backend())
    };

    let backend_addr = match backend_addr {
        Some(addr) => addr,
        None => return Ok(error_response(404, "no matching ingress route\n")),
    };

    let stream = match TcpStream::connect(backend_addr).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("backend {backend_addr} connect failed: {e}");
            return Ok(error_response(502, "bad gateway\n"));
        }
    };

    let io = TokioIo::new(stream);
    let (mut sender, conn) = match hyper::client::conn::http1::handshake(io).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("backend {backend_addr} handshake failed: {e}");
            return Ok(error_response(502, "bad gateway\n"));
        }
    };
    tokio::spawn(conn);

    match sender.send_request(req).await {
        Ok(resp) => {
            let (parts, body) = resp.into_parts();
            Ok(Response::from_parts(parts, Either::Right(body)))
        }
        Err(e) => {
            tracing::warn!("backend {backend_addr} request failed: {e}");
            Ok(error_response(502, "bad gateway\n"))
        }
    }
}

pub async fn run_ingress_proxy(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    let routes: Arc<RwLock<RouteTable>> = Arc::new(RwLock::new(rebuild_routes(&store)));

    let watcher_routes = routes.clone();
    let watcher_store = store.clone();
    let watcher_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let mut ing_rx = watcher_store.watch(&GroupVersionResource::ingresses());
        let mut ep_rx = watcher_store.watch(&GroupVersionResource::endpoints());

        loop {
            let changed = tokio::select! {
                _ = watcher_shutdown.cancelled() => return,
                r = ing_rx.recv() => !matches!(r, Err(broadcast::error::RecvError::Closed)),
                r = ep_rx.recv() => !matches!(r, Err(broadcast::error::RecvError::Closed)),
            };
            if changed {
                *watcher_routes.write().expect("route table lock poisoned") =
                    rebuild_routes(&watcher_store);
            } else {
                return;
            }
        }
    });

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
                    if let Err(e) = server_http1::Builder::new().serve_connection(io, service).await
                        && !e.is_incomplete_message()
                    {
                        tracing::debug!("connection error: {e}");
                    }
                });
            }
        }
    }
}
