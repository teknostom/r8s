use axum::body::Body;
use http_body_util::BodyExt;
use r8s_tests::*;
use r8s_types::GroupVersionResource;
use tokio_stream::StreamExt;
use tower::ServiceExt;

fn req(method: &str, uri: &str) -> axum::http::Request<Body> {
    axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

async fn body_text(resp: axum::http::Response<Body>) -> String {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Create a pod with one container and write the given CRI lines into its
/// `<container-id>.log` file. Returns the container id used in pod status.
async fn setup_pod_with_log(cluster: &TestCluster, name: &str, cri_lines: &str) -> String {
    let container_id = format!("{name}_app");
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"containers": [{"name": "app", "image": "scratch"}]},
        "status": {
            "containerStatuses": [{
                "name": "app",
                "image": "scratch",
                "imageID": "scratch",
                "containerID": container_id,
                "ready": false,
                "started": false,
                "restartCount": 0,
                "state": {"terminated": {"exitCode": 0}},
            }]
        }
    });
    cluster.create(&GroupVersionResource::pods(), "default", name, &pod);

    let logs_dir = cluster.data_dir().join("logs");
    std::fs::create_dir_all(&logs_dir).unwrap();
    std::fs::write(logs_dir.join(format!("{container_id}.log")), cri_lines).unwrap();
    container_id
}

#[tokio::test]
async fn logs_returns_stderr_only_pod() {
    let cluster = TestCluster::start().await;
    let cri = "\
2026-05-15T22:06:00.000000000Z stderr F first error line
2026-05-15T22:06:01.000000000Z stderr F second error line
";
    setup_pod_with_log(&cluster, "stderr-only", cri).await;

    let app = cluster.api_router();
    let resp = app
        .oneshot(req(
            "GET",
            "/api/v1/namespaces/default/pods/stderr-only/log",
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = body_text(resp).await;
    assert_eq!(body, "first error line\nsecond error line\n");

    cluster.shutdown().await;
}

#[tokio::test]
async fn logs_interleaves_stdout_and_stderr_in_write_order() {
    let cluster = TestCluster::start().await;
    let cri = "\
2026-05-15T22:06:00.000000000Z stdout F starting up
2026-05-15T22:06:01.000000000Z stderr F warning: thing happened
2026-05-15T22:06:02.000000000Z stdout F finished
";
    setup_pod_with_log(&cluster, "mixed", cri).await;

    let body = body_text(
        cluster
            .api_router()
            .oneshot(req("GET", "/api/v1/namespaces/default/pods/mixed/log"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        body, "starting up\nwarning: thing happened\nfinished\n",
        "ordering must follow file order, not stdout-then-stderr"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn logs_concatenates_partial_fragments() {
    let cluster = TestCluster::start().await;
    let cri = "\
2026-05-15T22:06:00.000000000Z stdout P hello,
2026-05-15T22:06:00.000001000Z stdout F  world
";
    setup_pod_with_log(&cluster, "partials", cri).await;

    let body = body_text(
        cluster
            .api_router()
            .oneshot(req("GET", "/api/v1/namespaces/default/pods/partials/log"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(body, "hello, world\n");

    cluster.shutdown().await;
}

#[tokio::test]
async fn logs_with_timestamps_query_param() {
    let cluster = TestCluster::start().await;
    let cri = "\
2026-05-15T22:06:00.000000000Z stdout F line one
2026-05-15T22:06:01.000000000Z stderr F line two
";
    setup_pod_with_log(&cluster, "ts", cri).await;

    let body = body_text(
        cluster
            .api_router()
            .oneshot(req(
                "GET",
                "/api/v1/namespaces/default/pods/ts/log?timestamps=true",
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        body,
        "2026-05-15T22:06:00.000000000Z line one\n2026-05-15T22:06:01.000000000Z line two\n"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn logs_tail_lines_counts_logical_lines() {
    let cluster = TestCluster::start().await;
    let cri = "\
2026-05-15T22:06:00.000000000Z stdout F a
2026-05-15T22:06:01.000000000Z stdout F b
2026-05-15T22:06:02.000000000Z stdout P c-
2026-05-15T22:06:03.000000000Z stdout F continued
2026-05-15T22:06:04.000000000Z stdout F d
";
    setup_pod_with_log(&cluster, "tail", cri).await;

    let body = body_text(
        cluster
            .api_router()
            .oneshot(req(
                "GET",
                "/api/v1/namespaces/default/pods/tail/log?tailLines=2",
            ))
            .await
            .unwrap(),
    )
    .await;
    // Logical lines are: a, b, "c-continued", d  → tail 2 = "c-continued", "d"
    assert_eq!(body, "c-continued\nd\n");

    cluster.shutdown().await;
}

#[tokio::test]
async fn logs_follow_streams_appended_lines() {
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    let cluster = TestCluster::start().await;
    let initial = "\
2026-05-15T22:06:00.000000000Z stdout F first
";
    let cid = setup_pod_with_log(&cluster, "follow-pod", initial).await;
    let log_path = cluster.data_dir().join("logs").join(format!("{cid}.log"));

    let app = cluster.api_router();
    let resp = app
        .oneshot(req(
            "GET",
            "/api/v1/namespaces/default/pods/follow-pod/log?follow=true",
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Convert axum body into an AsyncRead so we can read incrementally.
    let mut body_reader = tokio_util::io::StreamReader::new(
        resp.into_body()
            .into_data_stream()
            .map(|r| r.map_err(|e| std::io::Error::other(e.to_string()))),
    );

    // Initial chunk should arrive promptly.
    let mut buf = [0u8; 32];
    let n = tokio::time::timeout(Duration::from_secs(2), body_reader.read(&mut buf))
        .await
        .expect("timed out reading initial chunk")
        .unwrap();
    assert_eq!(&buf[..n], b"first\n");

    // Append more CRI lines and expect to see them within the poll interval.
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap();
        writeln!(
            f,
            "2026-05-15T22:06:01.000000000Z stderr F appended line"
        )
        .unwrap();
    }

    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(2), body_reader.read(&mut buf))
        .await
        .expect("timed out waiting for appended line")
        .unwrap();
    assert_eq!(&buf[..n], b"appended line\n");

    drop(body_reader); // closes stream → spawned follow task exits on next send
    cluster.shutdown().await;
}

#[tokio::test]
async fn logs_missing_log_file_returns_empty_200() {
    let cluster = TestCluster::start().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "no-log", "namespace": "default"},
        "spec": {"containers": [{"name": "app", "image": "scratch"}]},
        "status": {
            "containerStatuses": [{
                "name": "app",
                "image": "scratch",
                "imageID": "scratch",
                "containerID": "no-log_app",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-15T22:06:00Z"}},
            }]
        }
    });
    cluster.create(&GroupVersionResource::pods(), "default", "no-log", &pod);

    let resp = cluster
        .api_router()
        .oneshot(req("GET", "/api/v1/namespaces/default/pods/no-log/log"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(body_text(resp).await, "");

    cluster.shutdown().await;
}
