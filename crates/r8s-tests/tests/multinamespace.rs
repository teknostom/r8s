use std::time::Duration;

use r8s_store::backend::ResourceRef;
use r8s_tests::*;
use r8s_types::*;

const TIMEOUT: Duration = Duration::from_secs(15);

fn create_namespace(cluster: &TestCluster, name: &str) {
    let ns = Namespace {
        metadata: ObjectMeta {
            name: Some(name.into()),
            ..Default::default()
        },
        spec: None,
        status: None,
    };
    let ns_gvr = GroupVersionResource::namespaces();
    let rref = ResourceRef { gvr: &ns_gvr, namespace: None, name };
    cluster.store.create(rref, &serde_json::to_value(&ns).unwrap()).unwrap();
}

#[tokio::test]
async fn resources_isolated_by_namespace() {
    let cluster = TestCluster::start().await;
    let deploy_gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();

    // Create two namespaces
    create_namespace(&cluster, "ns-a");
    create_namespace(&cluster, "ns-b");

    // Create same-named deployment in both namespaces
    let mut deploy_a = make_deployment("web", 2, "web");
    deploy_a.metadata.namespace = Some("ns-a".into());
    cluster.create(&deploy_gvr, "ns-a", "web", &deploy_a);

    let mut deploy_b = make_deployment("web", 1, "web");
    deploy_b.metadata.namespace = Some("ns-b".into());
    cluster.create(&deploy_gvr, "ns-b", "web", &deploy_b);

    // Wait for pods in each namespace
    let ns_a_pods = wait_for_count(
        &cluster.store, &pod_gvr, Some("ns-a"),
        |v| has_label(v, "app", "web"), 2, TIMEOUT,
    ).await;
    assert!(ns_a_pods, "ns-a should have 2 pods");

    let ns_b_pods = wait_for_count(
        &cluster.store, &pod_gvr, Some("ns-b"),
        |v| has_label(v, "app", "web"), 1, TIMEOUT,
    ).await;
    assert!(ns_b_pods, "ns-b should have 1 pod");

    // Verify exact counts (ns-a didn't leak into ns-b)
    let a_count = cluster.list(&pod_gvr, "ns-a").iter()
        .filter(|v| has_label(v, "app", "web"))
        .count();
    let b_count = cluster.list(&pod_gvr, "ns-b").iter()
        .filter(|v| has_label(v, "app", "web"))
        .count();
    assert_eq!(a_count, 2, "ns-a should have exactly 2 web pods");
    assert_eq!(b_count, 1, "ns-b should have exactly 1 web pod");

    cluster.shutdown().await;
}

#[tokio::test]
async fn gc_respects_namespace() {
    let cluster = TestCluster::start().await;
    let deploy_gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();

    create_namespace(&cluster, "gc-a");
    create_namespace(&cluster, "gc-b");

    let mut deploy_a = make_deployment("gc-web", 1, "gc-web");
    deploy_a.metadata.namespace = Some("gc-a".into());
    cluster.create(&deploy_gvr, "gc-a", "gc-web", &deploy_a);

    let mut deploy_b = make_deployment("gc-web", 1, "gc-web");
    deploy_b.metadata.namespace = Some("gc-b".into());
    cluster.create(&deploy_gvr, "gc-b", "gc-web", &deploy_b);

    // Wait for pods in both
    let a_ready = wait_for_count(
        &cluster.store, &pod_gvr, Some("gc-a"),
        |v| has_label(v, "app", "gc-web"), 1, TIMEOUT,
    ).await;
    let b_ready = wait_for_count(
        &cluster.store, &pod_gvr, Some("gc-b"),
        |v| has_label(v, "app", "gc-web"), 1, TIMEOUT,
    ).await;
    assert!(a_ready && b_ready, "both namespaces should have pods");

    // Delete deployment in gc-a only
    cluster.delete(&deploy_gvr, "gc-a", "gc-web");

    let a_gone = wait_for_zero(
        &cluster.store, &pod_gvr, Some("gc-a"),
        |v| has_label(v, "app", "gc-web"), TIMEOUT,
    ).await;
    assert!(a_gone, "gc-a pods should be cleaned up");

    // gc-b pods should still exist
    let b_still = cluster.list(&pod_gvr, "gc-b").iter()
        .filter(|v| has_label(v, "app", "gc-web"))
        .count();
    assert_eq!(b_still, 1, "gc-b pod should be unaffected by gc-a deletion");

    cluster.shutdown().await;
}
