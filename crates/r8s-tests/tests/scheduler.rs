use std::time::Duration;

use r8s_tests::*;
use r8s_types::GroupVersionResource;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn scheduler_registers_node() {
    let cluster = TestCluster::start().await;
    let node_gvr = GroupVersionResource::nodes();

    let node = cluster
        .store
        .get(&r8s_store::backend::ResourceRef {
            gvr: &node_gvr,
            namespace: None,
            name: "r8s-node",
        })
        .unwrap();
    assert!(
        node.is_some(),
        "Node 'r8s-node' should exist after TestCluster starts"
    );

    let node_val = node.unwrap();
    assert_eq!(node_val["metadata"]["name"].as_str(), Some("r8s-node"));

    // Verify Ready condition
    let conditions = node_val["status"]["conditions"].as_array().unwrap();
    let ready = conditions
        .iter()
        .find(|c| c["type"].as_str() == Some("Ready"))
        .unwrap();
    assert_eq!(ready["status"].as_str(), Some("True"));

    cluster.shutdown().await;
}

#[tokio::test]
async fn scheduler_sets_pod_scheduled_condition() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(
        &pod_gvr,
        "default",
        "sched-test",
        &make_pod("sched-test", "nginx:latest"),
    );

    // Wait for PodScheduled=True condition
    let scheduled = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "sched-test",
        |v| has_condition(v, "PodScheduled", "True"),
        TIMEOUT,
    )
    .await;
    assert!(
        scheduled,
        "scheduler should set PodScheduled=True condition"
    );

    // Verify nodeName is set
    let val = cluster.get(&pod_gvr, "default", "sched-test");
    assert_eq!(
        val["spec"]["nodeName"].as_str(),
        Some("r8s-node"),
        "scheduler should set nodeName"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn scheduler_skips_already_scheduled() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();

    // Create a pod with nodeName already set to a different node
    let mut pod = make_pod("pre-sched", "nginx:latest");
    pod.spec.as_mut().unwrap().node_name = Some("other-node".into());
    cluster.create(&pod_gvr, "default", "pre-sched", &pod);

    // Wait a moment
    tokio::time::sleep(Duration::from_millis(500)).await;

    // nodeName should still be "other-node"
    let val = cluster.get(&pod_gvr, "default", "pre-sched");
    assert_eq!(
        val["spec"]["nodeName"].as_str(),
        Some("other-node"),
        "scheduler should not re-schedule already-assigned pod"
    );

    cluster.shutdown().await;
}
