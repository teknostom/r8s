//! `kubectl exec` endpoint.
//!
//! Upgrades the HTTP request to a WebSocket using one of Kubernetes's
//! `*.channel.k8s.io` subprotocols (v4 or v5), then multiplexes
//! stdin/stdout/stderr/error/resize streams as binary frames whose first byte
//! is the channel ID.
//!
//! - channel 0: stdin   (client → server)
//! - channel 1: stdout  (server → client)
//! - channel 2: stderr  (server → client; absent when tty=true)
//! - channel 3: error   (server → client; last frame is a Status JSON with
//!   the exit code, framed as metav1.Status)
//! - channel 4: resize  (client → server; JSON {"Width":<cols>,"Height":<rows>})
//! - channel 255: CLOSE (v5 only; a frame `[255, <channel>]` half-closes one
//!   stream). The case that matters is stdin: `kubectl exec -i` sends
//!   `[255, 0]` when its input reaches EOF while keeping the socket open for
//!   stdout, and we must drop our stdin sender so the runtime closes the
//!   container's stdin and the exec'd process sees EOF instead of blocking
//!   forever on a read.

use axum::{
    body::Body,
    extract::{
        Path, RawQuery, Request, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::header::{CONNECTION, UPGRADE},
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use hyper::StatusCode;
use hyper_util::rt::TokioIo;
use r8s_runtime::{ExecConfig, ExecStreams};
use r8s_store::backend::ResourceRef;
use r8s_types::{GroupVersionResource, Pod};
use serde::Deserialize;

use crate::admission::{self, AdmissionCtx, Operation};
use crate::discovery::AppState;
use crate::handler::admission_error_response;
use crate::response::status_error;

const SUBPROTO_V5: &str = "v5.channel.k8s.io";
const SUBPROTO_V4: &str = "v4.channel.k8s.io";
/// The original protocol: no status object on the error channel, only a
/// failure's message text.
const SUBPROTO_V1: &str = "channel.k8s.io";

const CHAN_STDIN: u8 = 0;
const CHAN_STDOUT: u8 = 1;
const CHAN_STDERR: u8 = 2;
const CHAN_ERROR: u8 = 3;
const CHAN_RESIZE: u8 = 4;
/// v5 reserved channel for CLOSE control frames: `[255, <channel-to-close>]`.
const CHAN_CLOSE: u8 = 255;

#[derive(Debug, Default)]
pub struct ExecParams {
    pub container: Option<String>,
    pub command: Vec<String>,
    pub tty: bool,
    // The stream flags only feed the admission review object; the WS
    // multiplex always carries all three streams.
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
}

impl ExecParams {
    /// The `PodExecOptions` / `PodAttachOptions` object webhooks see for a
    /// CONNECT on the `exec` / `attach` subresource.
    fn options_object(&self, kind: &str) -> serde_json::Value {
        let mut obj = serde_json::json!({
            "kind": kind,
            "apiVersion": "v1",
            "stdin": self.stdin,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "tty": self.tty,
            "container": self.container.clone().unwrap_or_default(),
        });
        if kind == "PodExecOptions" {
            obj["command"] = serde_json::json!(self.command);
        }
        obj
    }
}

/// Run validating webhooks for a CONNECT on a pod subresource. `Some` is the
/// error response to send back to the client.
async fn connect_admission(
    state: &AppState,
    namespace: &str,
    name: &str,
    subresource: &str,
    options: &serde_json::Value,
) -> Option<Response> {
    let pods_gvr = GroupVersionResource::pods();
    let ctx = AdmissionCtx {
        store: &state.store,
        gvr: &pods_gvr,
        kind: "Pod",
        operation: Operation::Connect,
        namespace: Some(namespace),
        name,
        subresource: Some(subresource),
        dry_run: false,
        user_info: serde_json::json!({}),
    };
    admission::invoke_validating(&ctx, options, None)
        .await
        .err()
        .map(admission_error_response)
}

/// `kubectl attach`: admission runs so webhooks on `pods/attach` can deny it;
/// an allowed attach is not implemented by the runtime.
pub async fn pod_attach(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    RawQuery(query): RawQuery,
) -> Response {
    let params = parse_exec_params(query.as_deref());
    if let Some(resp) = connect_admission(
        &state,
        &namespace,
        &name,
        "attach",
        &params.options_object("PodAttachOptions"),
    )
    .await
    {
        return resp;
    }
    status_error(
        StatusCode::NOT_IMPLEMENTED,
        "NotImplemented",
        "attach is not supported by this server; use exec",
    )
}

/// kubectl encodes the exec command as repeated `command=` query params, e.g.
/// `?command=/bin/sh&command=-c&command=ls`. serde_urlencoded (axum's default
/// `Query` extractor) collapses repeated keys to the last value, so we walk
/// the raw query string ourselves.
fn parse_exec_params(query: Option<&str>) -> ExecParams {
    let mut out = ExecParams::default();
    let Some(q) = query else {
        return out;
    };
    for pair in q.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let decoded = percent_decode(v);
        let flag = matches!(decoded.as_str(), "true" | "1");
        match k {
            "container" => out.container = Some(decoded),
            "command" => out.command.push(decoded),
            "tty" => out.tty = flag,
            "stdin" => out.stdin = flag,
            "stdout" => out.stdout = flag,
            "stderr" => out.stderr = flag,
            _ => {}
        }
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push(((h << 4) | l) as u8);
                i += 3;
                continue;
            }
        } else if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_default()
}

pub async fn pod_exec(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    let params = parse_exec_params(query.as_deref());
    if let Some(resp) = connect_admission(
        &state,
        &namespace,
        &name,
        "exec",
        &params.options_object("PodExecOptions"),
    )
    .await
    {
        return resp;
    }
    let runtime = match state.exec_runtime.clone() {
        Some(r) => r,
        None => {
            return status_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailable",
                "exec is not enabled on this server",
            );
        }
    };

    if params.command.is_empty() {
        return status_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            "exec requires at least one command argument",
        );
    }

    let container_id = match resolve_container(&state, &namespace, &name, &params) {
        Ok(id) => id,
        Err(resp) => return resp,
    };

    let command = params.command.clone();
    let tty = params.tty;

    ws.protocols([SUBPROTO_V5, SUBPROTO_V4, SUBPROTO_V1])
        .on_upgrade(move |socket| async move {
            // Diagnostic: which subprotocol did we land on? v4 has no stdin
            // half-close signal at all; only v5 sends a CLOSE control frame.
            let negotiated = socket
                .protocol()
                .and_then(|p| p.to_str().ok())
                .unwrap_or("<none>")
                .to_string();
            tracing::info!(
                subprotocol = %negotiated,
                container_id = %container_id,
                "exec session upgraded"
            );
            // No subprotocol means v1, as upstream treats it.
            let v1 = negotiated == SUBPROTO_V1 || negotiated == "<none>";
            let streams = match runtime
                .exec(ExecConfig {
                    container_id: r8s_runtime::ContainerId(container_id),
                    command,
                    tty,
                    initial_size: None,
                })
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    let _ = send_error_status(socket, &format!("exec failed: {e}"), v1).await;
                    return;
                }
            };

            if let Err(e) = pump(socket, streams, tty, v1).await {
                tracing::debug!(error = %e, "exec session ended with error");
            }
        })
}

/// The runtime id of the container to exec into: `?container=`, else the
/// pod's first container, as the kubelet resolves it.
#[allow(clippy::result_large_err)]
fn resolve_container(
    state: &AppState,
    namespace: &str,
    name: &str,
    params: &ExecParams,
) -> Result<String, Response> {
    let pods_gvr = GroupVersionResource::pods();
    let rref = ResourceRef {
        gvr: &pods_gvr,
        namespace: Some(namespace),
        name,
    };
    let pod_obj = match state.store.get(&rref) {
        Ok(Some(v)) => v,
        _ => {
            return Err(status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("pod {namespace}/{name} not found"),
            ));
        }
    };
    let pod: Pod = serde_json::from_value(pod_obj).map_err(|e| {
        status_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            &format!("decode pod: {e}"),
        )
    })?;
    let spec = pod
        .spec
        .as_ref()
        .ok_or_else(|| status_error(StatusCode::NOT_FOUND, "NotFound", "pod has no spec"))?;
    let target_container = match &params.container {
        Some(c) => c.clone(),
        None => spec
            .containers
            .first()
            .map(|c| c.name.clone())
            .ok_or_else(|| {
                status_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequest",
                    "pod has no containers",
                )
            })?,
    };
    let known = spec
        .containers
        .iter()
        .map(|c| c.name.as_str())
        .chain(
            spec.init_containers
                .iter()
                .flatten()
                .map(|c| c.name.as_str()),
        )
        .chain(
            spec.ephemeral_containers
                .iter()
                .flatten()
                .map(|c| c.name.as_str()),
        )
        .any(|n| n == target_container);
    if !known {
        return Err(status_error(
            StatusCode::NOT_FOUND,
            "NotFound",
            &format!("container {target_container} not found in pod"),
        ));
    }
    Ok(r8s_runtime::ContainerId::derive(namespace, name, &target_container).0)
}

/// `pods/exec` over SPDY/3.1: the transport client-go's SPDY executor and
/// kubectl's WebSocket fallback use (a POST with `Upgrade: SPDY/3.1`).
pub async fn pod_exec_spdy(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    req: Request,
) -> Response {
    let params = parse_exec_params(query.as_deref());
    if let Some(resp) = connect_admission(
        &state,
        &namespace,
        &name,
        "exec",
        &params.options_object("PodExecOptions"),
    )
    .await
    {
        return resp;
    }
    let Some(runtime) = state.exec_runtime.clone() else {
        return status_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            "exec is not enabled on this server",
        );
    };
    if params.command.is_empty() {
        return status_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            "exec requires at least one command argument",
        );
    }
    let opts = crate::spdy::StreamOpts {
        stdin: params.stdin,
        stdout: params.stdout,
        stderr: params.stderr && !params.tty,
        tty: params.tty,
    };
    if !opts.stdin && !opts.stdout && !opts.stderr {
        return status_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            "you must specify at least 1 of stdin, stdout, stderr",
        );
    }
    let container_id = match resolve_container(&state, &namespace, &name, &params) {
        Ok(id) => id,
        Err(resp) => return resp,
    };

    let header_has = |header: &str, needle: &str| {
        req.headers()
            .get_all(header)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .any(|v| v.to_ascii_lowercase().contains(needle))
    };
    if !header_has("connection", "upgrade") || !header_has("upgrade", "spdy/3.1") {
        return status_error(
            StatusCode::BAD_REQUEST,
            "BadRequest",
            "unable to upgrade: missing upgrade headers in request",
        );
    }
    let offered: Vec<String> = req
        .headers()
        .get_all("x-stream-protocol-version")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();
    let Some(protocol_name) = crate::spdy::negotiate(&offered) else {
        let mut resp = status_error(
            StatusCode::FORBIDDEN,
            "Forbidden",
            &format!(
                "unable to upgrade: unable to negotiate protocol: client supports {offered:?}, \
                 server accepts {:?}",
                crate::spdy::SUPPORTED_PROTOCOLS
            ),
        );
        for p in crate::spdy::SUPPORTED_PROTOCOLS {
            if let Ok(v) = p.parse() {
                resp.headers_mut()
                    .append("x-accepted-stream-protocol-versions", v);
            }
        }
        return resp;
    };
    let protocol = crate::spdy::Protocol::from_name(protocol_name);
    let config = ExecConfig {
        container_id: r8s_runtime::ContainerId(container_id.clone()),
        command: params.command.clone(),
        tty: params.tty,
        initial_size: None,
    };

    let on_upgrade = hyper::upgrade::on(req);
    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => {
                tracing::info!(container_id, protocol = protocol_name, "spdy exec session");
                crate::spdy::serve_exec(TokioIo::new(upgraded), protocol, opts, runtime, config)
                    .await;
            }
            Err(e) => tracing::warn!("spdy exec: upgrade failed: {e}"),
        }
    });
    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "SPDY/3.1")
        .header("X-Stream-Protocol-Version", protocol_name)
        .body(Body::empty())
        .expect("valid response")
}

async fn pump(
    socket: WebSocket,
    mut streams: ExecStreams,
    tty: bool,
    v1: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Forward stdout chunks → WS as channel-1 frames.
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<Message>(32);
    let out_tx_for_stdout = out_tx.clone();
    let mut stdout_rx = std::mem::replace(
        &mut streams.stdout_rx,
        tokio::sync::mpsc::channel(1).1, // sentinel; we won't use it again
    );
    let stdout_task = tokio::spawn(async move {
        while let Some(chunk) = stdout_rx.recv().await {
            let mut frame = Vec::with_capacity(chunk.len() + 1);
            frame.push(CHAN_STDOUT);
            frame.extend_from_slice(&chunk);
            if out_tx_for_stdout
                .send(Message::Binary(frame.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // Forward stderr (only when not tty).
    let stderr_task = streams.stderr_rx.take().map(|mut stderr_rx| {
        let out_tx_for_stderr = out_tx.clone();
        tokio::spawn(async move {
            while let Some(chunk) = stderr_rx.recv().await {
                let mut frame = Vec::with_capacity(chunk.len() + 1);
                frame.push(CHAN_STDERR);
                frame.extend_from_slice(&chunk);
                if out_tx_for_stderr
                    .send(Message::Binary(frame.into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        })
    });

    // Drain out_rx → WS sink. Done in this task (not spawned) so the
    // function holds the WS sink and can also forward the final error frame.
    let writer_task: tokio::task::JoinHandle<Result<(), axum::Error>> = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            ws_tx.send(msg).await?;
        }
        // No more output — final error frame is sent by the wait-task below.
        Ok(())
    });

    // Wait for exit in a side task; when it fires, emit the metav1.Status
    // frame on channel 3 then close the writer side by dropping out_tx.
    let exit_rx = streams.exit_rx;
    let close_tx = out_tx.clone();
    tokio::spawn(async move {
        let code = exit_rx.await.unwrap_or(-1);
        // The output goes out first: the forwarders end once the process
        // has closed its pipes.
        let _ = stdout_task.await;
        if let Some(task) = stderr_task {
            let _ = task.await;
        }
        if let Some(frame) = exit_status_frame(code, v1) {
            let _ = close_tx.send(Message::Binary(frame.into())).await;
        }
        // Tell the client the session is over with a *normal* (1000) close.
        // `Close(None)` sends no status code, which kubectl's stream readers
        // surface as `websocket: close 1005 (no status)` errors even on a
        // clean exit. An explicit 1000 closes the streams cleanly.
        let _ = close_tx
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: 1000,
                reason: "".into(),
            })))
            .await;
        // Dropping close_tx (and the original out_tx above) will let out_rx
        // close, ending the writer_task.
    });

    let _ = tty;

    // Drive stdin/resize from WS → runtime.
    while let Some(msg) = ws_rx.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => return Err(Box::new(e)),
        };
        match msg {
            Message::Binary(bytes) => {
                if bytes.is_empty() {
                    continue;
                }
                let chan = bytes[0];
                let payload = &bytes[1..];
                // Diagnostic: log every non-stdin-data frame so we can see what
                // the client actually sends when its stdin reaches EOF.
                if chan != CHAN_STDIN {
                    let head = &payload[..payload.len().min(4)];
                    tracing::info!(
                        chan,
                        payload_len = payload.len(),
                        ?head,
                        "exec: control frame"
                    );
                }
                // A v5 CLOSE for stdin (`[255, 0]`) means the client's input
                // hit EOF. Drop our stdin sender so the runtime closes the
                // container's stdin FIFO and the exec'd process sees EOF —
                // without this, `kubectl exec -i <<EOF` (cat, a console
                // producer) blocks forever waiting for more input.
                if is_stdin_close(chan, payload) {
                    tracing::info!("exec: stdin CLOSE received, closing container stdin");
                    drop(std::mem::replace(
                        &mut streams.stdin_tx,
                        tokio::sync::mpsc::channel(1).0,
                    ));
                    continue;
                }
                match chan {
                    CHAN_STDIN => {
                        if streams.stdin_tx.send(payload.to_vec()).await.is_err() {
                            break;
                        }
                    }
                    CHAN_RESIZE => {
                        if let Ok(resize) = serde_json::from_slice::<ResizeMsg>(payload) {
                            let _ = streams.resize_tx.send((resize.width, resize.height)).await;
                        }
                    }
                    _ => {}
                }
            }
            Message::Close(_) => break,
            _ => {} // text, ping, pong — ignore
        }
    }

    // Closing the WS read side also implicitly closes stdin (we drop the
    // sender when this function returns).
    drop(streams.stdin_tx);
    drop(streams.resize_tx);
    let _ = writer_task.await;
    Ok(())
}

/// True when an incoming WS frame is a v5 CLOSE control message for the stdin
/// channel (`[255, 0]`). kubectl sends this when its stdin reaches EOF while
/// keeping the rest of the session open for stdout/stderr.
fn is_stdin_close(chan: u8, payload: &[u8]) -> bool {
    chan == CHAN_CLOSE && payload.first() == Some(&CHAN_STDIN)
}

#[derive(Deserialize)]
struct ResizeMsg {
    #[serde(rename = "Width")]
    width: u16,
    #[serde(rename = "Height")]
    height: u16,
}

/// The error-channel frame that ends a session with exit `code`: a
/// `metav1.Status` from v4 on; under v1 only a failure's message, and
/// nothing at all on success.
fn exit_status_frame(code: i32, v1: bool) -> Option<Vec<u8>> {
    let message = format!("command terminated with non-zero exit code: {code}");
    let body = match (v1, code) {
        (true, 0) => return None,
        (true, _) => message.into_bytes(),
        (false, 0) => serde_json::to_vec(&serde_json::json!({
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Success",
        }))
        .unwrap_or_default(),
        (false, _) => serde_json::to_vec(&serde_json::json!({
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Failure",
            "reason": "NonZeroExitCode",
            "message": message,
            "details": {
                "causes": [{
                    "reason": "ExitCode",
                    "message": format!("{code}"),
                }]
            }
        }))
        .unwrap_or_default(),
    };
    let mut frame = Vec::with_capacity(body.len() + 1);
    frame.push(CHAN_ERROR);
    frame.extend_from_slice(&body);
    Some(frame)
}

async fn send_error_status(socket: WebSocket, message: &str, v1: bool) -> Result<(), axum::Error> {
    let (mut tx, _rx) = socket.split();
    let body = if v1 {
        message.as_bytes().to_vec()
    } else {
        serde_json::to_vec(&serde_json::json!({
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Failure",
            "message": message,
        }))
        .unwrap_or_default()
    };
    let mut frame = Vec::with_capacity(body.len() + 1);
    frame.push(CHAN_ERROR);
    frame.extend_from_slice(&body);
    tx.send(Message::Binary(frame.into())).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn v1_sessions_end_silently_on_success() {
        use super::{CHAN_ERROR, exit_status_frame};
        assert!(exit_status_frame(0, true).is_none());
        let failed = exit_status_frame(2, true).unwrap();
        assert_eq!(failed[0], CHAN_ERROR);
        assert_eq!(
            std::str::from_utf8(&failed[1..]).unwrap(),
            "command terminated with non-zero exit code: 2"
        );
        let v4 = exit_status_frame(0, false).unwrap();
        let status: serde_json::Value = serde_json::from_slice(&v4[1..]).unwrap();
        assert_eq!(status["status"], "Success");
    }

    use super::{CHAN_CLOSE, CHAN_STDIN, is_stdin_close};

    #[test]
    fn detects_v5_stdin_close() {
        // kubectl sends [255, 0] when its stdin reaches EOF.
        assert!(is_stdin_close(CHAN_CLOSE, &[CHAN_STDIN]));
    }

    #[test]
    fn ignores_non_stdin_close_and_data() {
        assert!(!is_stdin_close(CHAN_CLOSE, &[1])); // closing stdout, not stdin
        assert!(!is_stdin_close(CHAN_STDIN, b"hello")); // real stdin data
        assert!(!is_stdin_close(CHAN_CLOSE, &[])); // malformed: no channel byte
        assert!(!is_stdin_close(4, &[0])); // resize frame, not a close
    }
}
