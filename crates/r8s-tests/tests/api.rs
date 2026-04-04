use axum::body::Body;
use http_body_util::BodyExt;
use r8s_tests::*;
use tower::ServiceExt;

fn json_request(method: &str, uri: &str, body: Option<serde_json::Value>) -> axum::http::Request<Body> {
    let builder = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(b) = body {
        builder.body(Body::from(serde_json::to_vec(&b).unwrap())).unwrap()
    } else {
        builder.body(Body::empty()).unwrap()
    }
}

async fn response_json(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn api_create_and_get() {
    let cluster = TestCluster::start().await;
    let app = cluster.api_router();

    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {"containers": [{"name": "app", "image": "nginx:latest"}]}
    });

    // POST to create
    let resp = app.clone()
        .oneshot(json_request("POST", "/api/v1/namespaces/default/pods", Some(pod)))
        .await.unwrap();
    assert_eq!(resp.status(), 201, "POST should return 201 Created");

    let created = response_json(resp).await;
    assert_eq!(created["metadata"]["name"].as_str(), Some("test-pod"));
    assert!(created["metadata"]["uid"].as_str().is_some(), "should have uid");

    // GET it back
    let resp = app.clone()
        .oneshot(json_request("GET", "/api/v1/namespaces/default/pods/test-pod", None))
        .await.unwrap();
    assert_eq!(resp.status(), 200);

    let fetched = response_json(resp).await;
    assert_eq!(fetched["metadata"]["name"].as_str(), Some("test-pod"));

    cluster.shutdown().await;
}

#[tokio::test]
async fn api_list_with_label_selector() {
    let cluster = TestCluster::start().await;
    let app = cluster.api_router();

    // Create two pods with different labels
    let pod_a = serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "pod-a", "namespace": "default", "labels": {"env": "prod"}},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]}
    });
    let pod_b = serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "pod-b", "namespace": "default", "labels": {"env": "dev"}},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]}
    });

    app.clone().oneshot(json_request("POST", "/api/v1/namespaces/default/pods", Some(pod_a))).await.unwrap();
    app.clone().oneshot(json_request("POST", "/api/v1/namespaces/default/pods", Some(pod_b))).await.unwrap();

    // List with label selector
    let resp = app.clone()
        .oneshot(json_request("GET", "/api/v1/namespaces/default/pods?labelSelector=env%3Dprod", None))
        .await.unwrap();
    assert_eq!(resp.status(), 200);

    let list = response_json(resp).await;
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "should match only 1 pod with env=prod");
    assert_eq!(items[0]["metadata"]["name"].as_str(), Some("pod-a"));

    cluster.shutdown().await;
}

#[tokio::test]
async fn api_update_resource() {
    let cluster = TestCluster::start().await;
    let app = cluster.api_router();

    let pod = serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "upd-pod", "namespace": "default"},
        "spec": {"containers": [{"name": "app", "image": "nginx:1.24"}]}
    });

    let resp = app.clone()
        .oneshot(json_request("POST", "/api/v1/namespaces/default/pods", Some(pod)))
        .await.unwrap();
    let created = response_json(resp).await;
    let rv = created["metadata"]["resourceVersion"].as_str().unwrap();

    // PUT update with correct resourceVersion
    let updated = serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "upd-pod", "namespace": "default", "resourceVersion": rv},
        "spec": {"containers": [{"name": "app", "image": "nginx:1.25"}]}
    });

    let resp = app.clone()
        .oneshot(json_request("PUT", "/api/v1/namespaces/default/pods/upd-pod", Some(updated)))
        .await.unwrap();
    assert_eq!(resp.status(), 200, "PUT should return 200");

    let result = response_json(resp).await;
    assert_eq!(result["spec"]["containers"][0]["image"].as_str(), Some("nginx:1.25"));

    cluster.shutdown().await;
}

#[tokio::test]
async fn api_patch_resource() {
    let cluster = TestCluster::start().await;
    let app = cluster.api_router();

    let pod = serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "patch-pod", "namespace": "default", "labels": {"app": "web"}},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]}
    });
    app.clone().oneshot(json_request("POST", "/api/v1/namespaces/default/pods", Some(pod))).await.unwrap();

    // PATCH to add an annotation
    let patch = serde_json::json!({
        "metadata": {"annotations": {"note": "patched"}}
    });

    let req = axum::http::Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/default/pods/patch-pod")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(serde_json::to_vec(&patch).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);

    let result = response_json(resp).await;
    assert_eq!(result["metadata"]["annotations"]["note"].as_str(), Some("patched"));
    // Original label should still be present
    assert_eq!(result["metadata"]["labels"]["app"].as_str(), Some("web"));

    cluster.shutdown().await;
}

#[tokio::test]
async fn api_delete_resource() {
    let cluster = TestCluster::start().await;
    let app = cluster.api_router();

    let pod = serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "del-pod", "namespace": "default"},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]}
    });
    app.clone().oneshot(json_request("POST", "/api/v1/namespaces/default/pods", Some(pod))).await.unwrap();

    // DELETE
    let resp = app.clone()
        .oneshot(json_request("DELETE", "/api/v1/namespaces/default/pods/del-pod", None))
        .await.unwrap();
    assert_eq!(resp.status(), 200, "DELETE should return 200");

    // GET should return 404
    let resp = app.clone()
        .oneshot(json_request("GET", "/api/v1/namespaces/default/pods/del-pod", None))
        .await.unwrap();
    assert_eq!(resp.status(), 404, "GET after DELETE should return 404");

    cluster.shutdown().await;
}

#[tokio::test]
async fn api_404_not_found() {
    let cluster = TestCluster::start().await;
    let app = cluster.api_router();

    let resp = app.clone()
        .oneshot(json_request("GET", "/api/v1/namespaces/default/pods/does-not-exist", None))
        .await.unwrap();
    assert_eq!(resp.status(), 404);

    cluster.shutdown().await;
}

#[tokio::test]
async fn api_409_conflict() {
    let cluster = TestCluster::start().await;
    let app = cluster.api_router();

    let pod = serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "conflict-pod", "namespace": "default"},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]}
    });
    app.clone().oneshot(json_request("POST", "/api/v1/namespaces/default/pods", Some(pod))).await.unwrap();

    // PUT with stale resourceVersion
    let stale_update = serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "conflict-pod", "namespace": "default", "resourceVersion": "99999"},
        "spec": {"containers": [{"name": "app", "image": "nginx:1.25"}]}
    });

    let resp = app.clone()
        .oneshot(json_request("PUT", "/api/v1/namespaces/default/pods/conflict-pod", Some(stale_update)))
        .await.unwrap();
    assert_eq!(resp.status(), 409, "PUT with stale resourceVersion should return 409 Conflict");

    cluster.shutdown().await;
}

#[tokio::test]
async fn api_duplicate_create_returns_409() {
    let cluster = TestCluster::start().await;
    let app = cluster.api_router();

    let pod = serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "dup-pod", "namespace": "default"},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]}
    });

    let resp = app.clone()
        .oneshot(json_request("POST", "/api/v1/namespaces/default/pods", Some(pod.clone())))
        .await.unwrap();
    assert_eq!(resp.status(), 201);

    let resp = app.clone()
        .oneshot(json_request("POST", "/api/v1/namespaces/default/pods", Some(pod)))
        .await.unwrap();
    assert_eq!(resp.status(), 409, "duplicate POST should return 409");

    cluster.shutdown().await;
}
