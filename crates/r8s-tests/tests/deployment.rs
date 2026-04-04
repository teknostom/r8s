use std::time::Duration;

use r8s_tests::*;
use r8s_types::GroupVersionResource;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn deployment_creates_rs_and_pods() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let rs_gvr = GroupVersionResource::replica_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "nginx", &make_deployment("nginx", 3, "nginx"));
    let deploy_uid = cluster.uid(&gvr, "default", "nginx");

    let rs_found = wait_for_count(
        &cluster.store, &rs_gvr, Some("default"),
        |v| is_owned_by(v, &deploy_uid), 1, TIMEOUT,
    ).await;
    assert!(rs_found, "ReplicaSet should be created for deployment");

    let pods_found = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "nginx"), 3, TIMEOUT,
    ).await;
    assert!(pods_found, "3 pods should be created for deployment");

    let status_ok = wait_for(
        &cluster.store, &gvr, Some("default"), "nginx",
        |v| v["status"]["replicas"].as_i64() == Some(3), TIMEOUT,
    ).await;
    assert!(status_ok, "deployment status should show 3 replicas");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_scale_up() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "scale-up", &make_deployment("scale-up", 2, "scale-up"));

    let found = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "scale-up"), 2, TIMEOUT,
    ).await;
    assert!(found, "should start with 2 pods");

    // Scale up to 5
    let mut val = cluster.get(&gvr, "default", "scale-up");
    val["spec"]["replicas"] = serde_json::json!(5);
    cluster.update(&gvr, "default", "scale-up", &val);

    let scaled = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "scale-up"), 5, TIMEOUT,
    ).await;
    assert!(scaled, "should scale to 5 pods");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_scale_down() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "scale-down", &make_deployment("scale-down", 3, "scale-down"));

    let found = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "scale-down"), 3, TIMEOUT,
    ).await;
    assert!(found, "should start with 3 pods");

    // Scale down to 1
    let mut val = cluster.get(&gvr, "default", "scale-down");
    val["spec"]["replicas"] = serde_json::json!(1);
    cluster.update(&gvr, "default", "scale-down", &val);

    let scaled = wait_for_exact_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "scale-down"), 1, TIMEOUT,
    ).await;
    assert!(scaled, "should scale down to 1 pod");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_rolling_update() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let rs_gvr = GroupVersionResource::replica_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "rolling", &make_deployment("rolling", 2, "rolling"));
    let deploy_uid = cluster.uid(&gvr, "default", "rolling");

    let found = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "rolling"), 2, TIMEOUT,
    ).await;
    assert!(found, "should start with 2 pods");

    // Count ReplicaSets before update
    let rs_before = cluster.list(&rs_gvr, "default").iter()
        .filter(|v| is_owned_by(v, &deploy_uid))
        .count();

    // Change the template to trigger rolling update
    let mut val = cluster.get(&gvr, "default", "rolling");
    val["spec"]["template"]["spec"]["containers"][0]["image"] = serde_json::json!("nginx:1.25");
    cluster.update(&gvr, "default", "rolling", &val);

    // Wait for a new RS to be created
    let new_rs = wait_for_count(
        &cluster.store, &rs_gvr, Some("default"),
        |v| is_owned_by(v, &deploy_uid), rs_before + 1, TIMEOUT,
    ).await;
    assert!(new_rs, "rolling update should create a new ReplicaSet");

    // Wait for old RS to be scaled to 0
    tokio::time::sleep(Duration::from_millis(500)).await;
    let old_rs_scaled_down = cluster.list(&rs_gvr, "default").iter()
        .filter(|v| is_owned_by(v, &deploy_uid))
        .any(|v| v["spec"]["replicas"].as_i64() == Some(0));
    assert!(old_rs_scaled_down, "old ReplicaSet should be scaled to 0");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_zero_replicas() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "zero", &make_deployment("zero", 0, "zero"));

    // Give controllers time to reconcile
    tokio::time::sleep(Duration::from_secs(1)).await;

    let count = cluster.list(&pod_gvr, "default").iter()
        .filter(|v| has_label(v, "app", "zero"))
        .count();
    assert_eq!(count, 0, "deployment with replicas=0 should create no pods");

    cluster.shutdown().await;
}

#[tokio::test]
async fn replicaset_pod_replacement() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "replace", &make_deployment("replace", 2, "replace"));

    let found = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "replace"), 2, TIMEOUT,
    ).await;
    assert!(found, "should start with 2 pods");

    // Find and delete one pod
    let victim = cluster.list(&pod_gvr, "default").into_iter()
        .find(|v| has_label(v, "app", "replace"))
        .unwrap();
    let victim_name = victim["metadata"]["name"].as_str().unwrap().to_string();
    let victim_uid = victim["metadata"]["uid"].as_str().unwrap().to_string();
    cluster.delete(&pod_gvr, "default", &victim_name);

    // Wait for replacement — 2 pods again, none with the old UID
    let replaced = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "replace") && v["metadata"]["uid"].as_str() != Some(&victim_uid),
        2, TIMEOUT,
    ).await;
    assert!(replaced, "RS should recreate deleted pod");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------
// readyReplicas / availableReplicas reflect actual pod readiness
// ---------------------------------------------------------------

#[tokio::test]
async fn rs_ready_replicas_reflects_pod_readiness() {
    let cluster = TestCluster::start().await;
    let deploy_gvr = GroupVersionResource::deployments();
    let rs_gvr = GroupVersionResource::replica_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&deploy_gvr, "default", "rr-deploy", &make_deployment("rr-deploy", 2, "rr-deploy"));

    let both_running = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "rr-deploy") && has_phase(v, "Running"), 2, TIMEOUT,
    ).await;
    assert!(both_running, "both pods should be Running");

    // Allow a reconcile cycle so RS and Deployment statuses are updated
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rs_val = cluster.list(&rs_gvr, "default").into_iter()
        .find(|v| has_label(v, "app", "rr-deploy"))
        .expect("ReplicaSet should exist");
    let rs_ready = rs_val["status"]["readyReplicas"].as_i64().unwrap_or(0);
    assert_eq!(rs_ready, 2, "RS readyReplicas should equal 2 when both pods are Ready");

    let deploy_val = cluster.get(&deploy_gvr, "default", "rr-deploy");
    let deploy_ready = deploy_val["status"]["readyReplicas"].as_i64().unwrap_or(0);
    assert_eq!(deploy_ready, 2, "Deployment readyReplicas should equal 2");

    cluster.shutdown().await;
}
