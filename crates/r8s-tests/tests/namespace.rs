use std::time::Duration;

use r8s_store::backend::ResourceRef;
use r8s_tests::*;
use r8s_types::*;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn namespace_creates_default_sa() {
    let cluster = TestCluster::start().await;
    let ns_gvr = GroupVersionResource::namespaces();
    let sa_gvr = GroupVersionResource::service_accounts();

    let ns = Namespace {
        metadata: ObjectMeta {
            name: Some("test-ns".into()),
            ..Default::default()
        },
        spec: None,
        status: None,
    };
    let rref = ResourceRef { gvr: &ns_gvr, namespace: None, name: "test-ns" };
    cluster.store.create(rref, &serde_json::to_value(&ns).unwrap()).unwrap();

    let sa_found = wait_for(
        &cluster.store, &sa_gvr, Some("test-ns"), "default",
        |_| true, TIMEOUT,
    ).await;
    assert!(sa_found, "namespace controller should create 'default' ServiceAccount");

    cluster.shutdown().await;
}

#[tokio::test]
async fn namespace_sa_idempotent() {
    let cluster = TestCluster::start().await;
    let ns_gvr = GroupVersionResource::namespaces();
    let sa_gvr = GroupVersionResource::service_accounts();

    // Pre-create the SA
    let sa = ServiceAccount {
        metadata: ObjectMeta {
            name: Some("default".into()),
            namespace: Some("idem-ns".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    cluster.create(&sa_gvr, "idem-ns", "default", &sa);
    let old_uid = cluster.uid(&sa_gvr, "idem-ns", "default");

    // Now create the namespace
    let ns = Namespace {
        metadata: ObjectMeta {
            name: Some("idem-ns".into()),
            ..Default::default()
        },
        spec: None,
        status: None,
    };
    let rref = ResourceRef { gvr: &ns_gvr, namespace: None, name: "idem-ns" };
    cluster.store.create(rref, &serde_json::to_value(&ns).unwrap()).unwrap();

    // Give controller time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // SA should still exist with the same UID (not recreated)
    let current_uid = cluster.uid(&sa_gvr, "idem-ns", "default");
    assert_eq!(old_uid, current_uid, "SA should not be recreated (idempotent)");

    cluster.shutdown().await;
}
