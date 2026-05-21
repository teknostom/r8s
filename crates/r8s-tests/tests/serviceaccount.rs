use std::time::Duration;

use r8s_store::backend::ResourceRef;
use r8s_tests::*;
use r8s_types::*;

const TIMEOUT: Duration = Duration::from_secs(15);

const SA_NAME_ANNOTATION: &str = "kubernetes.io/service-account.name";

/// List secrets in the namespace whose `service-account.name` annotation
/// matches `sa_name`.
fn token_secrets_for(
    cluster: &TestCluster,
    namespace: &str,
    sa_name: &str,
) -> Vec<serde_json::Value> {
    let secret_gvr = GroupVersionResource::secrets();
    cluster
        .list(&secret_gvr, namespace)
        .into_iter()
        .filter(|s| s.get("type").and_then(|v| v.as_str()) == Some("kubernetes.io/service-account-token"))
        .filter(|s| {
            s.get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get(SA_NAME_ANNOTATION))
                .and_then(|v| v.as_str())
                == Some(sa_name)
        })
        .collect()
}

#[tokio::test]
async fn sa_delete_cascades_to_token_secret() {
    let cluster = TestCluster::start().await;
    let ns_gvr = GroupVersionResource::namespaces();
    let sa_gvr = GroupVersionResource::service_accounts();

    cluster.create(
        &ns_gvr,
        "",
        "cascade-ns",
        &Namespace {
            metadata: ObjectMeta {
                name: Some("cascade-ns".into()),
                ..Default::default()
            },
            spec: None,
            status: None,
        },
    );

    let sa = ServiceAccount {
        metadata: ObjectMeta {
            name: Some("worker".into()),
            namespace: Some("cascade-ns".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    cluster.create(&sa_gvr, "cascade-ns", "worker", &sa);

    // Wait for the SA controller to mint a token Secret.
    let secret_appeared = poll_for(TIMEOUT, || {
        !token_secrets_for(&cluster, "cascade-ns", "worker").is_empty()
    })
    .await;
    assert!(secret_appeared, "SA controller should mint a token Secret");

    let token_name = token_secrets_for(&cluster, "cascade-ns", "worker")[0]
        ["metadata"]["name"]
        .as_str()
        .unwrap()
        .to_string();

    // Now delete the SA — cascade should drop the token.
    cluster.delete(&sa_gvr, "cascade-ns", "worker");

    let secret_gvr = GroupVersionResource::secrets();
    let gone = wait_for_deletion(
        &cluster.store,
        &secret_gvr,
        Some("cascade-ns"),
        &token_name,
        TIMEOUT,
    )
    .await;
    assert!(
        gone,
        "token Secret '{token_name}' should be deleted when its SA is deleted"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn sa_recreate_does_not_orphan_new_token() {
    // After deleting and recreating an SA with the same name, the cascade for
    // the *old* SA must not touch the *new* SA's token (UID match, not name).
    let cluster = TestCluster::start().await;
    let ns_gvr = GroupVersionResource::namespaces();
    let sa_gvr = GroupVersionResource::service_accounts();

    cluster.create(
        &ns_gvr,
        "",
        "recreate-ns",
        &Namespace {
            metadata: ObjectMeta {
                name: Some("recreate-ns".into()),
                ..Default::default()
            },
            spec: None,
            status: None,
        },
    );

    let make_sa = || ServiceAccount {
        metadata: ObjectMeta {
            name: Some("svc".into()),
            namespace: Some("recreate-ns".into()),
            ..Default::default()
        },
        ..Default::default()
    };

    cluster.create(&sa_gvr, "recreate-ns", "svc", &make_sa());
    poll_for(TIMEOUT, || {
        !token_secrets_for(&cluster, "recreate-ns", "svc").is_empty()
    })
    .await;

    cluster.delete(&sa_gvr, "recreate-ns", "svc");
    // Let the cascade run.
    tokio::time::sleep(Duration::from_millis(200)).await;

    cluster.create(&sa_gvr, "recreate-ns", "svc", &make_sa());
    let new_token_minted = poll_for(TIMEOUT, || {
        !token_secrets_for(&cluster, "recreate-ns", "svc").is_empty()
    })
    .await;
    assert!(new_token_minted, "new SA should get its own token Secret");

    // Give any errant cascade time to fire.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let new_tokens = token_secrets_for(&cluster, "recreate-ns", "svc");
    assert!(
        !new_tokens.is_empty(),
        "new SA's token must survive the prior SA's cascade"
    );

    let rref = ResourceRef {
        gvr: &sa_gvr,
        namespace: Some("recreate-ns"),
        name: "svc",
    };
    assert!(
        cluster.store.get(&rref).unwrap().is_some(),
        "the new SA itself must still exist"
    );

    cluster.shutdown().await;
}

/// Poll until `pred()` is true or timeout. Returns whether the predicate held.
async fn poll_for<F: Fn() -> bool>(timeout: Duration, pred: F) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if pred() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
