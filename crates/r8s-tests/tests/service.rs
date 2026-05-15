use std::collections::BTreeMap;
use std::time::Duration;

use r8s_tests::*;
use r8s_types::*;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn endpoints_from_service_selector() {
    let cluster = TestCluster::start().await;
    let deploy_gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();
    let svc_gvr = GroupVersionResource::services();
    let ep_gvr = GroupVersionResource::endpoints();

    cluster.create(
        &deploy_gvr,
        "default",
        "ep-web",
        &make_deployment("ep-web", 2, "ep-web"),
    );

    let pods_running = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "ep-web") && has_phase(v, "Running"),
        2,
        TIMEOUT,
    )
    .await;
    assert!(pods_running, "2 pods should be Running");

    let svc = make_service(
        "ep-svc",
        BTreeMap::from([("app".into(), "ep-web".into())]),
        80,
    );
    cluster.create(&svc_gvr, "default", "ep-svc", &svc);

    let ep_found = wait_for(
        &cluster.store,
        &ep_gvr,
        Some("default"),
        "ep-svc",
        |v| {
            v["subsets"][0]["addresses"]
                .as_array()
                .map(|a| a.len() >= 2)
                .unwrap_or(false)
        },
        TIMEOUT,
    )
    .await;
    assert!(
        ep_found,
        "Endpoints should have 2 addresses from matching pods"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn endpoints_update_on_pod_change() {
    let cluster = TestCluster::start().await;
    let deploy_gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();
    let svc_gvr = GroupVersionResource::services();
    let ep_gvr = GroupVersionResource::endpoints();

    cluster.create(
        &deploy_gvr,
        "default",
        "ep-upd",
        &make_deployment("ep-upd", 1, "ep-upd"),
    );

    let running = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "ep-upd") && has_phase(v, "Running"),
        1,
        TIMEOUT,
    )
    .await;
    assert!(running, "1 pod should be Running");

    let svc = make_service(
        "ep-upd-svc",
        BTreeMap::from([("app".into(), "ep-upd".into())]),
        80,
    );
    cluster.create(&svc_gvr, "default", "ep-upd-svc", &svc);

    let ep1 = wait_for(
        &cluster.store,
        &ep_gvr,
        Some("default"),
        "ep-upd-svc",
        |v| {
            v["subsets"][0]["addresses"]
                .as_array()
                .map(|a| a.len() == 1)
                .unwrap_or(false)
        },
        TIMEOUT,
    )
    .await;
    assert!(ep1, "Endpoints should have 1 address initially");

    // Scale up to 3
    let mut val = cluster.get(&deploy_gvr, "default", "ep-upd");
    val["spec"]["replicas"] = serde_json::json!(3);
    cluster.update(&deploy_gvr, "default", "ep-upd", &val);

    let three_running = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "ep-upd") && has_phase(v, "Running"),
        3,
        TIMEOUT,
    )
    .await;
    assert!(three_running, "3 pods should be Running");

    let ep3 = wait_for(
        &cluster.store,
        &ep_gvr,
        Some("default"),
        "ep-upd-svc",
        |v| {
            v["subsets"][0]["addresses"]
                .as_array()
                .map(|a| a.len() >= 3)
                .unwrap_or(false)
        },
        TIMEOUT,
    )
    .await;
    assert!(ep3, "Endpoints should have 3 addresses after scale-up");

    cluster.shutdown().await;
}

#[tokio::test]
async fn endpoints_only_running_pods() {
    let cluster = TestCluster::start().await;
    let deploy_gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();
    let svc_gvr = GroupVersionResource::services();
    let ep_gvr = GroupVersionResource::endpoints();

    cluster.create(
        &deploy_gvr,
        "default",
        "ep-run",
        &make_deployment("ep-run", 1, "ep-run"),
    );

    let running = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "ep-run") && has_phase(v, "Running"),
        1,
        TIMEOUT,
    )
    .await;
    assert!(running, "1 pod should be Running");

    // Create a non-running "pending" pod manually
    let pending_pod = Pod {
        metadata: ObjectMeta {
            name: Some("ep-pending".into()),
            namespace: Some("default".into()),
            labels: Some(BTreeMap::from([("app".into(), "ep-run".into())])),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: Some("fake-node".into()), // Not our node, won't be started
            containers: vec![Container {
                name: "app".into(),
                image: Some("nginx:latest".into()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some("Pending".into()),
            ..Default::default()
        }),
    };
    cluster.create(&pod_gvr, "default", "ep-pending", &pending_pod);

    // Create service matching both pods
    let svc = make_service(
        "ep-run-svc",
        BTreeMap::from([("app".into(), "ep-run".into())]),
        80,
    );
    cluster.create(&svc_gvr, "default", "ep-run-svc", &svc);

    // Endpoints should only have 1 address (the Running pod)
    let ep_ok = wait_for(
        &cluster.store,
        &ep_gvr,
        Some("default"),
        "ep-run-svc",
        |v| {
            v["subsets"][0]["addresses"]
                .as_array()
                .map(|a| a.len() == 1)
                .unwrap_or(false)
        },
        TIMEOUT,
    )
    .await;
    assert!(
        ep_ok,
        "Endpoints should only include Running pods (1 address, not 2)"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn endpoints_no_selector_skipped() {
    let cluster = TestCluster::start().await;
    let svc_gvr = GroupVersionResource::services();
    let ep_gvr = GroupVersionResource::endpoints();

    // Create a Service with no selector
    let svc = Service {
        metadata: ObjectMeta {
            name: Some("no-sel".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                port: 80,
                protocol: Some("TCP".into()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    };
    cluster.create(&svc_gvr, "default", "no-sel", &svc);

    // Wait a bit, then verify no Endpoints were created
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(
        cluster.try_get(&ep_gvr, "default", "no-sel").is_none(),
        "Service without selector should not auto-create Endpoints"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn endpointslice_created() {
    let cluster = TestCluster::start().await;
    let deploy_gvr = GroupVersionResource::deployments();
    let pod_gvr = GroupVersionResource::pods();
    let svc_gvr = GroupVersionResource::services();
    let es_gvr = GroupVersionResource::endpoint_slices();

    cluster.create(
        &deploy_gvr,
        "default",
        "ep-slice",
        &make_deployment("ep-slice", 1, "ep-slice"),
    );

    let running = wait_for_count(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        |v| has_label(v, "app", "ep-slice") && has_phase(v, "Running"),
        1,
        TIMEOUT,
    )
    .await;
    assert!(running, "1 pod should be Running");

    let svc = make_service(
        "ep-slice-svc",
        BTreeMap::from([("app".into(), "ep-slice".into())]),
        80,
    );
    cluster.create(&svc_gvr, "default", "ep-slice-svc", &svc);

    // EndpointSlice name is typically "<service-name>-<hash>" but we wait by content
    // The EndpointSlice should be created with the service's name as a label
    let es_found = wait_for(
        &cluster.store,
        &es_gvr,
        Some("default"),
        "ep-slice-svc",
        |v| v["addressType"].as_str() == Some("IPv4"),
        TIMEOUT,
    )
    .await;
    assert!(
        es_found,
        "EndpointSlice should be created alongside Endpoints"
    );

    cluster.shutdown().await;
}

// ---------------------------------------------------------------
// Readiness probe gates endpoint membership
// ---------------------------------------------------------------

#[tokio::test]
async fn kubelet_readiness_probe_gates_endpoints() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();
    let svc_gvr = GroupVersionResource::services();
    let ep_gvr = GroupVersionResource::endpoints();

    // Pod with a TCP readiness probe to a port that won't be open.
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some("probe-unready".into()),
            namespace: Some("default".into()),
            labels: Some(BTreeMap::from([("app".into(), "probe-unready".into())])),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "worker".into(),
                image: Some("busybox:latest".into()),
                readiness_probe: Some(Probe {
                    tcp_socket: Some(TCPSocketAction {
                        port: IntOrString::Int(9999),
                        ..Default::default()
                    }),
                    period_seconds: Some(1),
                    failure_threshold: Some(1),
                    timeout_seconds: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };
    cluster.create(&pod_gvr, "default", "probe-unready", &pod);

    let svc = make_service(
        "probe-unready-svc",
        BTreeMap::from([("app".into(), "probe-unready".into())]),
        80,
    );
    cluster.create(&svc_gvr, "default", "probe-unready-svc", &svc);

    // Wait for pod to be Running
    let running = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "probe-unready",
        |v| has_phase(v, "Running"),
        TIMEOUT,
    )
    .await;
    assert!(running, "pod should reach Running phase");

    // Wait for Endpoints to be created
    let ep_created = wait_for(
        &cluster.store,
        &ep_gvr,
        Some("default"),
        "probe-unready-svc",
        |_| true,
        TIMEOUT,
    )
    .await;
    assert!(ep_created, "Endpoints should be created for the service");

    // Allow one reconcile cycle
    tokio::time::sleep(Duration::from_millis(200)).await;

    let ep_val = cluster.get(&ep_gvr, "default", "probe-unready-svc");
    let addr_count = ep_val["subsets"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|s| s["addresses"].as_array())
        .map(|a| a.len())
        .unwrap_or(0);

    assert_eq!(
        addr_count, 0,
        "pod with failing readiness probe should not appear in Endpoints"
    );

    cluster.shutdown().await;
}
