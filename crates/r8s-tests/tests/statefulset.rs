use std::time::Duration;

use r8s_tests::*;
use r8s_types::GroupVersionResource;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn statefulset_ordered_pods() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::stateful_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "web", &make_statefulset("web", 2, "web"));

    let web0 = wait_for(&cluster.store, &pod_gvr, Some("default"), "web-0", |_| true, TIMEOUT).await;
    let web1 = wait_for(&cluster.store, &pod_gvr, Some("default"), "web-1", |_| true, TIMEOUT).await;
    assert!(web0, "StatefulSet should create pod web-0");
    assert!(web1, "StatefulSet should create pod web-1");

    let status_ok = wait_for(
        &cluster.store, &gvr, Some("default"), "web",
        |v| v["status"]["replicas"].as_i64() == Some(2), TIMEOUT,
    ).await;
    assert!(status_ok, "StatefulSet status should show 2 replicas");

    cluster.shutdown().await;
}

#[tokio::test]
async fn statefulset_scale_down() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::stateful_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "sd", &make_statefulset("sd", 3, "sd"));

    let found = wait_for(&cluster.store, &pod_gvr, Some("default"), "sd-2", |_| true, TIMEOUT).await;
    assert!(found, "sd-2 should exist");

    // Scale down to 1
    let mut val = cluster.get(&gvr, "default", "sd");
    val["spec"]["replicas"] = serde_json::json!(1);
    cluster.update(&gvr, "default", "sd", &val);

    // sd-2 and sd-1 should be deleted (highest ordinals first)
    let sd2_gone = wait_for_deletion(&cluster.store, &pod_gvr, Some("default"), "sd-2", TIMEOUT).await;
    let sd1_gone = wait_for_deletion(&cluster.store, &pod_gvr, Some("default"), "sd-1", TIMEOUT).await;
    assert!(sd2_gone, "sd-2 should be deleted (highest ordinal first)");
    assert!(sd1_gone, "sd-1 should be deleted");
    assert!(cluster.try_get(&pod_gvr, "default", "sd-0").is_some(), "sd-0 should still exist");

    cluster.shutdown().await;
}

#[tokio::test]
async fn statefulset_ordinal_gap_fill() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::stateful_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "gap", &make_statefulset("gap", 2, "gap"));

    let found0 = wait_for(&cluster.store, &pod_gvr, Some("default"), "gap-0", |_| true, TIMEOUT).await;
    let found1 = wait_for(&cluster.store, &pod_gvr, Some("default"), "gap-1", |_| true, TIMEOUT).await;
    assert!(found0 && found1, "gap-0 and gap-1 should exist");

    // Delete gap-0 (the lowest ordinal)
    let old_uid = cluster.uid(&pod_gvr, "default", "gap-0");
    cluster.delete(&pod_gvr, "default", "gap-0");

    // Wait for gap-0 to be recreated (different UID)
    let recreated = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "gap-0",
        |v| v["metadata"]["uid"].as_str() != Some(&old_uid), TIMEOUT,
    ).await;
    assert!(recreated, "STS should recreate gap-0 (not gap-2)");

    // Verify gap-2 was NOT created
    assert!(
        cluster.try_get(&pod_gvr, "default", "gap-2").is_none(),
        "STS should fill ordinal gap, not create gap-2"
    );

    cluster.shutdown().await;
}
