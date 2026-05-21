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
//!                       the exit code, framed as metav1.Status)
//! - channel 4: resize  (client → server; JSON {"Width":<cols>,"Height":<rows>})
//!
//! v5 adds CLOSE control messages (channel ID with empty payload signals the
//! peer to close that half of the stream). We accept v5 but don't depend on
//! the CLOSE semantics — if a peer sends one, we drop the corresponding mpsc
//! sender, which is the same effect.

use axum::{
    extract::{
        Path, RawQuery, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use hyper::StatusCode;
use r8s_runtime::{ExecConfig, ExecStreams};
use r8s_store::backend::ResourceRef;
use r8s_types::{GroupVersionResource, Pod};
use serde::Deserialize;

use crate::discovery::AppState;
use crate::response::status_error;

const SUBPROTO_V5: &str = "v5.channel.k8s.io";
const SUBPROTO_V4: &str = "v4.channel.k8s.io";

const CHAN_STDIN: u8 = 0;
const CHAN_STDOUT: u8 = 1;
const CHAN_STDERR: u8 = 2;
const CHAN_ERROR: u8 = 3;
const CHAN_RESIZE: u8 = 4;

#[derive(Debug, Default)]
pub struct ExecParams {
    pub container: Option<String>,
    pub command: Vec<String>,
    pub tty: bool,
    // `stdin`/`stdout`/`stderr` flags exist on the wire but we always provide
    // all three (the WS multiplex makes it trivial), so we don't read them.
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
        match k {
            "container" => out.container = Some(decoded),
            "command" => out.command.push(decoded),
            "tty" => out.tty = matches!(decoded.as_str(), "true" | "1"),
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

    // Resolve which container to attach to: explicit ?container=, else the
    // first container in the pod's spec — same semantics as real kubelet.
    let pods_gvr = GroupVersionResource::pods();
    let rref = ResourceRef {
        gvr: &pods_gvr,
        namespace: Some(&namespace),
        name: &name,
    };
    let pod_obj = match state.store.get(&rref) {
        Ok(Some(v)) => v,
        _ => {
            return status_error(
                StatusCode::NOT_FOUND,
                "NotFound",
                &format!("pod {namespace}/{name} not found"),
            );
        }
    };
    let pod: Pod = match serde_json::from_value(pod_obj) {
        Ok(p) => p,
        Err(e) => {
            return status_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("decode pod: {e}"),
            );
        }
    };
    let spec = match pod.spec.as_ref() {
        Some(s) => s,
        None => {
            return status_error(StatusCode::NOT_FOUND, "NotFound", "pod has no spec");
        }
    };
    let target_container = match &params.container {
        Some(c) => c.clone(),
        None => match spec.containers.first().map(|c| c.name.clone()) {
            Some(n) => n,
            None => {
                return status_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequest",
                    "pod has no containers",
                );
            }
        },
    };
    if !spec.containers.iter().any(|c| c.name == target_container) {
        return status_error(
            StatusCode::NOT_FOUND,
            "NotFound",
            &format!("container {target_container} not found in pod"),
        );
    }

    // Match the same naming rule the kubelet uses when creating containers.
    let container_id = container_id_for(&name, &target_container);

    let command = params.command.clone();
    let tty = params.tty;

    ws.protocols([SUBPROTO_V5, SUBPROTO_V4])
        .on_upgrade(move |socket| async move {
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
                    let _ = send_error_status(socket, &format!("exec failed: {e}"), 1).await;
                    return;
                }
            };

            if let Err(e) = pump(socket, streams, tty).await {
                tracing::debug!(error = %e, "exec session ended with error");
            }
        })
}

/// kubelet-style container ID derivation. Must match
/// `r8s_kubelet::container_id_for` byte-for-byte; we redeclare it here to
/// avoid api-server depending on kubelet, since the rule is small and stable.
fn container_id_for(pod_name: &str, container_name: &str) -> String {
    const MAX_LEN: usize = 76;
    let naive = format!("{pod_name}_{container_name}");
    if naive.len() <= MAX_LEN {
        return naive;
    }
    let hash = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        pod_name.hash(&mut h);
        format!("{:08x}", h.finish() as u32)
    };
    let suffix_len = 1 + hash.len() + 1 + container_name.len();
    let pod_budget = MAX_LEN.saturating_sub(suffix_len);
    let truncated_pod: String = pod_name
        .chars()
        .take(pod_budget)
        .collect::<String>()
        .trim_end_matches(['-', '_', '.'])
        .to_string();
    format!("{truncated_pod}_{hash}_{container_name}")
}

async fn pump(
    socket: WebSocket,
    mut streams: ExecStreams,
    tty: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Forward stdout chunks → WS as channel-1 frames.
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<Message>(32);
    let out_tx_for_stdout = out_tx.clone();
    let mut stdout_rx = std::mem::replace(
        &mut streams.stdout_rx,
        tokio::sync::mpsc::channel(1).1, // sentinel; we won't use it again
    );
    tokio::spawn(async move {
        while let Some(chunk) = stdout_rx.recv().await {
            let mut frame = Vec::with_capacity(chunk.len() + 1);
            frame.push(CHAN_STDOUT);
            frame.extend_from_slice(&chunk);
            if out_tx_for_stdout.send(Message::Binary(frame.into())).await.is_err() {
                break;
            }
        }
    });

    // Forward stderr (only when not tty).
    if let Some(mut stderr_rx) = streams.stderr_rx.take() {
        let out_tx_for_stderr = out_tx.clone();
        tokio::spawn(async move {
            while let Some(chunk) = stderr_rx.recv().await {
                let mut frame = Vec::with_capacity(chunk.len() + 1);
                frame.push(CHAN_STDERR);
                frame.extend_from_slice(&chunk);
                if out_tx_for_stderr.send(Message::Binary(frame.into())).await.is_err() {
                    break;
                }
            }
        });
    }

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
        let status = if code == 0 {
            serde_json::json!({
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Success",
            })
        } else {
            serde_json::json!({
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "reason": "NonZeroExitCode",
                "message": format!("command terminated with non-zero exit code: {code}"),
                "details": {
                    "causes": [{
                        "reason": "ExitCode",
                        "message": format!("{code}"),
                    }]
                }
            })
        };
        let body = serde_json::to_vec(&status).unwrap_or_default();
        let mut frame = Vec::with_capacity(body.len() + 1);
        frame.push(CHAN_ERROR);
        frame.extend_from_slice(&body);
        let _ = close_tx.send(Message::Binary(frame.into())).await;
        // Tell the client the session is over. Without this, kubectl waits
        // forever for more frames — sending Close gives it a clean shutdown
        // signal, after which our ws_rx loop sees Close and exits too.
        let _ = close_tx.send(Message::Close(None)).await;
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
                match chan {
                    CHAN_STDIN => {
                        if streams.stdin_tx.send(payload.to_vec()).await.is_err() {
                            break;
                        }
                    }
                    CHAN_RESIZE => {
                        if let Ok(resize) = serde_json::from_slice::<ResizeMsg>(payload) {
                            let _ = streams
                                .resize_tx
                                .send((resize.width, resize.height))
                                .await;
                        }
                    }
                    _ => {
                        // v5 CLOSE arrives as a 1-byte frame with the channel
                        // ID being closed. We treat any unknown channel as a
                        // hint to drop our matching sender.
                        if payload.is_empty() && chan == CHAN_STDIN {
                            drop(std::mem::replace(
                                &mut streams.stdin_tx,
                                tokio::sync::mpsc::channel(1).0,
                            ));
                        }
                    }
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

#[derive(Deserialize)]
struct ResizeMsg {
    #[serde(rename = "Width")]
    width: u16,
    #[serde(rename = "Height")]
    height: u16,
}

async fn send_error_status(
    socket: WebSocket,
    message: &str,
    _exit_code: i32,
) -> Result<(), axum::Error> {
    let (mut tx, _rx) = socket.split();
    let status = serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "status": "Failure",
        "message": message,
    });
    let body = serde_json::to_vec(&status).unwrap_or_default();
    let mut frame = Vec::with_capacity(body.len() + 1);
    frame.push(CHAN_ERROR);
    frame.extend_from_slice(&body);
    tx.send(Message::Binary(frame.into())).await
}

