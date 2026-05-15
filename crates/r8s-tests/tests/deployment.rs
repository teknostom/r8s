use std::time::Duration;

use r8s_store::watch::WatchEventType;
use r8s_tests::*;
use r8s_types::GroupVersionResource;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn deployment_creates_rs_and_pods() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let rs_gvr = GroupVersionResource::replica_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(
        &gvr,
        "default",
        "nginx",
        &make_deployment("nginx", 3, "nginx"),
    );
    let deploy_uid = cluster.uid(&gvr, "default", "nginx");

    let rs_found = wait_for_count(
        &cluster.store,
        &rs_gvr,
        Some("default"),
        |v| is_owned_by(v, &deploy_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(rs_found, "ReplicaSet should be created for deployment");

    let pods_found = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "nginx"),
        3,
        TIMEOUT,
    )
    .await;
    assert!(pods_found, "3 pods should be created for deployment");

    let status_ok = wait_for(
        &cluster.store,
        &gvr,
        Some("default"),
        "nginx",
        |v| v["status"]["replicas"].as_i64() == Some(3),
        TIMEOUT,
    )
    .await;
    assert!(status_ok, "deployment status should show 3 replicas");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_scale_up() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(
        &gvr,
        "default",
        "scale-up",
        &make_deployment("scale-up", 2, "scale-up"),
    );

    let found = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "scale-up"),
        2,
        TIMEOUT,
    )
    .await;
    assert!(found, "should start with 2 pods");

    // Scale up to 5
    let mut val = cluster.get(&gvr, "default", "scale-up");
    val["spec"]["replicas"] = serde_json::json!(5);
    cluster.update(&gvr, "default", "scale-up", &val);

    let scaled = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "scale-up"),
        5,
        TIMEOUT,
    )
    .await;
    assert!(scaled, "should scale to 5 pods");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_scale_down() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(
        &gvr,
        "default",
        "scale-down",
        &make_deployment("scale-down", 3, "scale-down"),
    );

    let found = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "scale-down"),
        3,
        TIMEOUT,
    )
    .await;
    assert!(found, "should start with 3 pods");

    // Scale down to 1
    let mut val = cluster.get(&gvr, "default", "scale-down");
    val["spec"]["replicas"] = serde_json::json!(1);
    cluster.update(&gvr, "default", "scale-down", &val);

    let scaled = wait_for_exact_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "scale-down"),
        1,
        TIMEOUT,
    )
    .await;
    assert!(scaled, "should scale down to 1 pod");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_rolling_update() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let rs_gvr = GroupVersionResource::replica_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(
        &gvr,
        "default",
        "rolling",
        &make_deployment("rolling", 2, "rolling"),
    );
    let deploy_uid = cluster.uid(&gvr, "default", "rolling");

    let found = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "rolling"),
        2,
        TIMEOUT,
    )
    .await;
    assert!(found, "should start with 2 pods");

    // Count ReplicaSets before update
    let rs_before = cluster
        .list(&rs_gvr, "default")
        .iter()
        .filter(|v| is_owned_by(v, &deploy_uid))
        .count();

    // Change the template to trigger rolling update
    let mut val = cluster.get(&gvr, "default", "rolling");
    val["spec"]["template"]["spec"]["containers"][0]["image"] = serde_json::json!("nginx:1.25");
    cluster.update(&gvr, "default", "rolling", &val);

    // Wait for a new RS to be created
    let new_rs = wait_for_count(
        &cluster.store,
        &rs_gvr,
        Some("default"),
        |v| is_owned_by(v, &deploy_uid),
        rs_before + 1,
        TIMEOUT,
    )
    .await;
    assert!(new_rs, "rolling update should create a new ReplicaSet");

    // Wait for old RS to be scaled to 0
    tokio::time::sleep(Duration::from_millis(500)).await;
    let old_rs_scaled_down = cluster
        .list(&rs_gvr, "default")
        .iter()
        .filter(|v| is_owned_by(v, &deploy_uid))
        .any(|v| v["spec"]["replicas"].as_i64() == Some(0));
    assert!(old_rs_scaled_down, "old ReplicaSet should be scaled to 0");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_rolling_update_respects_surge() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let rs_gvr = GroupVersionResource::replica_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(
        &gvr,
        "default",
        "surge",
        &make_deployment("surge", 4, "surge"),
    );
    let deploy_uid = cluster.uid(&gvr, "default", "surge");

    let ready = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "surge") && has_phase(v, "Running"),
        4,
        TIMEOUT,
    )
    .await;
    assert!(ready, "all 4 pods should be Running before rollout");

    // Give one extra reconcile cycle so RS status.availableReplicas reflects readiness.
    tokio::time::sleep(Duration::from_millis(700)).await;

    let initial_rs_name = cluster
        .list(&rs_gvr, "default")
        .into_iter()
        .find(|v| is_owned_by(v, &deploy_uid))
        .and_then(|v| v["metadata"]["name"].as_str().map(String::from))
        .expect("initial RS exists");

    // Subscribe to RS watch events BEFORE triggering the update so we can
    // capture the new RS's state at the moment of creation. The rollout
    // can otherwise complete faster than a polling loop can sample.
    let mut rs_watch = cluster.store.watch(&rs_gvr);

    let mut val = cluster.get(&gvr, "default", "surge");
    val["spec"]["template"]["spec"]["containers"][0]["image"] = serde_json::json!("nginx:1.25");
    cluster.update(&gvr, "default", "surge", &val);

    // Capture the Added event for the new RS — that gives us its creation-time
    // replica count, which must be <= maxSurge (1 = ceil(4 * 25%)).
    let new_rs_initial = tokio::time::timeout(TIMEOUT, async {
        loop {
            let event = rs_watch.recv().await.expect("watch channel closed");
            if !matches!(event.event_type, WatchEventType::Added) {
                continue;
            }
            if event.object["metadata"]["name"].as_str() == Some(&initial_rs_name) {
                continue;
            }
            if !is_owned_by(&event.object, &deploy_uid) {
                continue;
            }
            break event.object;
        }
    })
    .await
    .expect("timed out waiting for new RS Added event");

    let new_rs_name = new_rs_initial["metadata"]["name"]
        .as_str()
        .expect("new RS has name")
        .to_string();
    let initial_new_replicas = new_rs_initial["spec"]["replicas"].as_i64().unwrap_or(-1);
    assert!(
        (0..=1).contains(&initial_new_replicas),
        "new RS must start at <= maxSurge (1), got {initial_new_replicas}"
    );

    let converged = wait_for(
        &cluster.store,
        &rs_gvr,
        Some("default"),
        &new_rs_name,
        |v| v["spec"]["replicas"].as_i64() == Some(4),
        TIMEOUT,
    )
    .await;
    assert!(converged, "new RS should reach desired replicas (4)");

    let old_drained = wait_for(
        &cluster.store,
        &rs_gvr,
        Some("default"),
        &initial_rs_name,
        |v| v["spec"]["replicas"].as_i64() == Some(0),
        TIMEOUT,
    )
    .await;
    assert!(old_drained, "old RS should drain to 0");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_recreate_strategy_is_immediate() {
    use r8s_types::DeploymentStrategy;

    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let rs_gvr = GroupVersionResource::replica_sets();
    let pod_gvr = GroupVersionResource::pods();

    let mut deploy = make_deployment("recreate", 3, "recreate");
    deploy.spec.as_mut().unwrap().strategy = Some(DeploymentStrategy {
        type_: Some("Recreate".into()),
        rolling_update: None,
    });
    cluster.create(&gvr, "default", "recreate", &deploy);
    let deploy_uid = cluster.uid(&gvr, "default", "recreate");

    let ready = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "recreate") && has_phase(v, "Running"),
        3,
        TIMEOUT,
    )
    .await;
    assert!(ready, "all 3 pods should be Running");

    let initial_rs_name = cluster
        .list(&rs_gvr, "default")
        .into_iter()
        .find(|v| is_owned_by(v, &deploy_uid))
        .and_then(|v| v["metadata"]["name"].as_str().map(String::from))
        .expect("initial RS exists");

    let mut val = cluster.get(&gvr, "default", "recreate");
    val["spec"]["template"]["spec"]["containers"][0]["image"] = serde_json::json!("nginx:1.25");
    cluster.update(&gvr, "default", "recreate", &val);

    let uid_for_filter = deploy_uid.clone();
    let initial_rs_name_for_filter = initial_rs_name.clone();
    let new_rs_full = wait_for_count(
        &cluster.store,
        &rs_gvr,
        Some("default"),
        move |v| {
            is_owned_by(v, &uid_for_filter)
                && v["metadata"]["name"].as_str() != Some(&initial_rs_name_for_filter)
                && v["spec"]["replicas"].as_i64() == Some(3)
        },
        1,
        TIMEOUT,
    )
    .await;
    assert!(
        new_rs_full,
        "Recreate: new RS should be created at full replicas"
    );

    let old_zero = wait_for(
        &cluster.store,
        &rs_gvr,
        Some("default"),
        &initial_rs_name,
        |v| v["spec"]["replicas"].as_i64() == Some(0),
        TIMEOUT,
    )
    .await;
    assert!(old_zero, "Recreate: old RS should be scaled to 0");

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

    let count = cluster
        .list(&pod_gvr, "default")
        .iter()
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

    cluster.create(
        &gvr,
        "default",
        "replace",
        &make_deployment("replace", 2, "replace"),
    );

    let found = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "replace"),
        2,
        TIMEOUT,
    )
    .await;
    assert!(found, "should start with 2 pods");

    // Find and delete one pod
    let victim = cluster
        .list(&pod_gvr, "default")
        .into_iter()
        .find(|v| has_label(v, "app", "replace"))
        .unwrap();
    let victim_name = victim["metadata"]["name"].as_str().unwrap().to_string();
    let victim_uid = victim["metadata"]["uid"].as_str().unwrap().to_string();
    cluster.delete(&pod_gvr, "default", &victim_name);

    // Wait for replacement — 2 pods again, none with the old UID
    let replaced = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "replace") && v["metadata"]["uid"].as_str() != Some(&victim_uid),
        2,
        TIMEOUT,
    )
    .await;
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

    cluster.create(
        &deploy_gvr,
        "default",
        "rr-deploy",
        &make_deployment("rr-deploy", 2, "rr-deploy"),
    );

    let both_running = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "rr-deploy") && has_phase(v, "Running"),
        2,
        TIMEOUT,
    )
    .await;
    assert!(both_running, "both pods should be Running");

    // Allow a reconcile cycle so RS and Deployment statuses are updated
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rs_val = cluster
        .list(&rs_gvr, "default")
        .into_iter()
        .find(|v| has_label(v, "app", "rr-deploy"))
        .expect("ReplicaSet should exist");
    let rs_ready = rs_val["status"]["readyReplicas"].as_i64().unwrap_or(0);
    assert_eq!(
        rs_ready, 2,
        "RS readyReplicas should equal 2 when both pods are Ready"
    );

    let deploy_val = cluster.get(&deploy_gvr, "default", "rr-deploy");
    let deploy_ready = deploy_val["status"]["readyReplicas"].as_i64().unwrap_or(0);
    assert_eq!(deploy_ready, 2, "Deployment readyReplicas should equal 2");

    cluster.shutdown().await;
}
