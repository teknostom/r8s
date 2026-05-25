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

    let web0 = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "web-0",
        |_| true,
        TIMEOUT,
    )
    .await;
    let web1 = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "web-1",
        |_| true,
        TIMEOUT,
    )
    .await;
    assert!(web0, "StatefulSet should create pod web-0");
    assert!(web1, "StatefulSet should create pod web-1");

    let status_ok = wait_for(
        &cluster.store,
        &gvr,
        Some("default"),
        "web",
        |v| v["status"]["replicas"].as_i64() == Some(2),
        TIMEOUT,
    )
    .await;
    assert!(status_ok, "StatefulSet status should show 2 replicas");

    cluster.shutdown().await;
}

#[tokio::test]
async fn statefulset_scale_down() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::stateful_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "sd", &make_statefulset("sd", 3, "sd"));

    let found = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "sd-2",
        |_| true,
        TIMEOUT,
    )
    .await;
    assert!(found, "sd-2 should exist");

    // Scale down to 1
    let mut val = cluster.get(&gvr, "default", "sd");
    val["spec"]["replicas"] = serde_json::json!(1);
    cluster.update(&gvr, "default", "sd", &val);

    // sd-2 and sd-1 should be deleted (highest ordinals first)
    let sd2_gone =
        wait_for_deletion(&cluster.store, &pod_gvr, Some("default"), "sd-2", TIMEOUT).await;
    let sd1_gone =
        wait_for_deletion(&cluster.store, &pod_gvr, Some("default"), "sd-1", TIMEOUT).await;
    assert!(sd2_gone, "sd-2 should be deleted (highest ordinal first)");
    assert!(sd1_gone, "sd-1 should be deleted");
    assert!(
        cluster.try_get(&pod_gvr, "default", "sd-0").is_some(),
        "sd-0 should still exist"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn statefulset_volume_claim_template() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::stateful_sets();
    let pod_gvr = GroupVersionResource::pods();
    let pvc_gvr = GroupVersionResource::persistent_volume_claims();
    let pv_gvr = GroupVersionResource::persistent_volumes();

    let sts = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": { "name": "db", "namespace": "default" },
        "spec": {
            "replicas": 1,
            "serviceName": "db",
            "selector": { "matchLabels": { "app": "db" } },
            "template": {
                "metadata": { "labels": { "app": "db" } },
                "spec": { "containers": [ {
                    "name": "app",
                    "image": "nginx:latest",
                    "volumeMounts": [ { "name": "data", "mountPath": "/data" } ]
                } ] }
            },
            "volumeClaimTemplates": [ {
                "metadata": { "name": "data" },
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": { "requests": { "storage": "1Gi" } }
                }
            } ]
        }
    });
    cluster.create(&gvr, "default", "db", &sts);

    assert!(
        wait_for(&cluster.store, &pod_gvr, Some("default"), "db-0", |_| true, TIMEOUT).await,
        "StatefulSet should create pod db-0"
    );

    // The volumeClaimTemplate should produce a per-ordinal PVC that the
    // provisioner binds to a freshly carved PV.
    let pvc_bound = wait_for(
        &cluster.store,
        &pvc_gvr,
        Some("default"),
        "data-db-0",
        |v| v["status"]["phase"] == serde_json::json!("Bound"),
        TIMEOUT,
    )
    .await;
    assert!(pvc_bound, "PVC data-db-0 should be provisioned and Bound");

    let pvc = cluster.get(&pvc_gvr, "default", "data-db-0");
    let pv_name = pvc["spec"]["volumeName"].as_str().unwrap().to_string();
    assert!(
        wait_for(&cluster.store, &pv_gvr, None, &pv_name, |_| true, TIMEOUT).await,
        "the bound PV should exist"
    );

    // The pod must carry a persistentVolumeClaim volume wired to that PVC.
    let pod = cluster.get(&pod_gvr, "default", "db-0");
    let vols = pod["spec"]["volumes"].as_array().cloned().unwrap_or_default();
    assert!(
        vols.iter().any(|v| {
            v["name"] == serde_json::json!("data")
                && v["persistentVolumeClaim"]["claimName"] == serde_json::json!("data-db-0")
        }),
        "pod should mount the PVC as volume 'data', got: {vols:?}"
    );

    // Persistence: deleting the pod recreates it against the *same* PVC.
    let pvc_uid = cluster.uid(&pvc_gvr, "default", "data-db-0");
    let old_pod_uid = cluster.uid(&pod_gvr, "default", "db-0");
    cluster.delete(&pod_gvr, "default", "db-0");
    let recreated = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "db-0",
        |v| v["metadata"]["uid"].as_str() != Some(&old_pod_uid),
        TIMEOUT,
    )
    .await;
    assert!(recreated, "db-0 should be recreated");
    assert_eq!(
        cluster.uid(&pvc_gvr, "default", "data-db-0"),
        pvc_uid,
        "the PVC (and its data) must survive pod recreation"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn statefulset_ordinal_gap_fill() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::stateful_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "gap", &make_statefulset("gap", 2, "gap"));

    let found0 = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "gap-0",
        |_| true,
        TIMEOUT,
    )
    .await;
    let found1 = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "gap-1",
        |_| true,
        TIMEOUT,
    )
    .await;
    assert!(found0 && found1, "gap-0 and gap-1 should exist");

    // Delete gap-0 (the lowest ordinal)
    let old_uid = cluster.uid(&pod_gvr, "default", "gap-0");
    cluster.delete(&pod_gvr, "default", "gap-0");

    // Wait for gap-0 to be recreated (different UID)
    let recreated = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "gap-0",
        |v| v["metadata"]["uid"].as_str() != Some(&old_uid),
        TIMEOUT,
    )
    .await;
    assert!(recreated, "STS should recreate gap-0 (not gap-2)");

    // Verify gap-2 was NOT created
    assert!(
        cluster.try_get(&pod_gvr, "default", "gap-2").is_none(),
        "STS should fill ordinal gap, not create gap-2"
    );

    cluster.shutdown().await;
}
