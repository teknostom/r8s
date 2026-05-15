use std::time::Duration;

use r8s_tests::*;
use r8s_types::GroupVersionResource;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn daemonset_creates_pod() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::daemon_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(
        &gvr,
        "default",
        "node-agent",
        &make_daemonset("node-agent", "node-agent"),
    );
    let ds_uid = cluster.uid(&gvr, "default", "node-agent");

    let found = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by(v, &ds_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(found, "DaemonSet should create exactly 1 pod");

    let status_ok = wait_for(
        &cluster.store,
        &gvr,
        Some("default"),
        "node-agent",
        |v| {
            v["status"]["desiredNumberScheduled"].as_i64() == Some(1)
                && v["status"]["currentNumberScheduled"].as_i64() == Some(1)
        },
        TIMEOUT,
    )
    .await;
    assert!(
        status_ok,
        "DaemonSet status should show desired=1, current=1"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn daemonset_pod_replacement() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::daemon_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(
        &gvr,
        "default",
        "ds-replace",
        &make_daemonset("ds-replace", "ds-replace"),
    );
    let ds_uid = cluster.uid(&gvr, "default", "ds-replace");

    let found = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by(v, &ds_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(found, "DS should create 1 pod");

    // Find and delete the DS pod
    let victim = cluster
        .list(&pod_gvr, "default")
        .into_iter()
        .find(|v| is_owned_by(v, &ds_uid))
        .unwrap();
    let victim_name = victim["metadata"]["name"].as_str().unwrap().to_string();
    let victim_uid = victim["metadata"]["uid"].as_str().unwrap().to_string();
    cluster.delete(&pod_gvr, "default", &victim_name);

    // Wait for replacement (different UID)
    let replaced = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by(v, &ds_uid) && v["metadata"]["uid"].as_str() != Some(&victim_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(replaced, "DS should recreate deleted pod");

    cluster.shutdown().await;
}
