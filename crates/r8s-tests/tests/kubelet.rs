use std::collections::BTreeMap;
use std::time::Duration;

use r8s_tests::*;
use r8s_types::*;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn kubelet_restart_always() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();

    // Create a standalone pod with restartPolicy=Always (default)
    let pod = make_pod("restart-always", "busybox:latest");
    cluster.create(&pod_gvr, "default", "restart-always", &pod);

    let running = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "restart-always",
        |v| has_phase(v, "Running"), TIMEOUT,
    ).await;
    assert!(running, "pod should reach Running");

    let original_uid = cluster.uid(&pod_gvr, "default", "restart-always");

    // Simulate container crash
    cluster.runtime.stop_matching("restart-always");

    // Pod should restart in-place: same UID, restartCount > 0, back to Running
    let restarted = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "restart-always",
        |v| {
            let restart_count = v["status"]["containerStatuses"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|c| c["restartCount"].as_i64())
                .unwrap_or(0);
            has_phase(v, "Running") && restart_count >= 1
        },
        TIMEOUT,
    ).await;
    assert!(restarted, "restartPolicy=Always should restart in-place with restartCount >= 1");

    let new_uid = cluster.uid(&pod_gvr, "default", "restart-always");
    assert_eq!(original_uid, new_uid, "pod UID should not change on in-place restart");

    cluster.shutdown().await;
}

#[tokio::test]
async fn kubelet_restart_on_failure_success() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();

    let mut pod = make_pod("onfail-ok", "busybox:latest");
    pod.spec.as_mut().unwrap().restart_policy = Some("OnFailure".into());
    pod.metadata.labels = Some(BTreeMap::from([("test".into(), "onfail".into())]));
    cluster.create(&pod_gvr, "default", "onfail-ok", &pod);

    let running = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "onfail-ok",
        |v| has_phase(v, "Running"), TIMEOUT,
    ).await;
    assert!(running, "pod should reach Running");

    // Simulate successful exit (code 0)
    cluster.runtime.stop_matching("onfail-ok");

    let succeeded = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "onfail-ok",
        |v| has_phase(v, "Succeeded"), TIMEOUT,
    ).await;
    assert!(succeeded, "restartPolicy=OnFailure + exit 0 should mark pod Succeeded");

    cluster.shutdown().await;
}

#[tokio::test]
async fn kubelet_restart_on_failure_fail() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();

    let mut pod = make_pod("onfail-err", "busybox:latest");
    pod.spec.as_mut().unwrap().restart_policy = Some("OnFailure".into());
    pod.metadata.labels = Some(BTreeMap::from([("test".into(), "onfail-err".into())]));
    cluster.create(&pod_gvr, "default", "onfail-err", &pod);

    let running = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "onfail-err",
        |v| has_phase(v, "Running"), TIMEOUT,
    ).await;
    assert!(running, "pod should reach Running");

    let original_uid = cluster.uid(&pod_gvr, "default", "onfail-err");

    // Simulate failed exit (code 1)
    cluster.runtime.stop_matching_with_code("onfail-err", 1);

    // Pod should restart in-place (OnFailure + failure → restart)
    let restarted = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "onfail-err",
        |v| {
            let restart_count = v["status"]["containerStatuses"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|c| c["restartCount"].as_i64())
                .unwrap_or(0);
            has_phase(v, "Running") && restart_count >= 1
        },
        TIMEOUT,
    ).await;
    assert!(restarted, "restartPolicy=OnFailure + exit 1 should restart in-place");

    let new_uid = cluster.uid(&pod_gvr, "default", "onfail-err");
    assert_eq!(original_uid, new_uid, "pod UID should not change on restart");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------
// Startup probe gates readiness
// ---------------------------------------------------------------

#[tokio::test]
async fn kubelet_startup_probe_gates_readiness() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some("startup-pod".into()),
            namespace: Some("default".into()),
            labels: Some(BTreeMap::from([("app".into(), "startup-pod".into())])),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "worker".into(),
                image: Some("busybox:latest".into()),
                startup_probe: Some(Probe {
                    tcp_socket: Some(TCPSocketAction {
                        port: IntOrString::Int(8080),
                        ..Default::default()
                    }),
                    period_seconds: Some(1),
                    failure_threshold: Some(30),
                    timeout_seconds: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };

    cluster.create(&pod_gvr, "default", "startup-pod", &pod);

    let running = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "startup-pod",
        |v| has_phase(v, "Running"), TIMEOUT,
    ).await;
    assert!(running, "pod should reach Running phase");

    // Give a health tick to propagate readiness status
    tokio::time::sleep(Duration::from_millis(200)).await;

    let pod_val = cluster.get(&pod_gvr, "default", "startup-pod");
    let ready_cond = pod_val["status"]["conditions"]
        .as_array()
        .and_then(|c| c.iter().find(|c| c["type"].as_str() == Some("Ready")))
        .and_then(|c| c["status"].as_str());

    assert_eq!(ready_cond, Some("False"), "pod with pending startup probe should have Ready=False");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------
// startedAt is stable across status updates
// ---------------------------------------------------------------

#[tokio::test]
async fn kubelet_started_at_is_stable() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();

    let pod = make_pod("stable-ts", "busybox:latest");
    cluster.create(&pod_gvr, "default", "stable-ts", &pod);

    let running = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "stable-ts",
        |v| has_phase(v, "Running"), TIMEOUT,
    ).await;
    assert!(running, "pod should reach Running");

    let first_val = cluster.get(&pod_gvr, "default", "stable-ts");
    let first_started_at = first_val["status"]["containerStatuses"][0]["state"]["running"]["startedAt"]
        .as_str().unwrap_or("").to_string();
    let first_start_time = first_val["status"]["startTime"]
        .as_str().unwrap_or("").to_string();
    assert!(!first_started_at.is_empty(), "startedAt should be set");
    assert!(!first_start_time.is_empty(), "startTime should be set");

    // Wait for several health ticks
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let later_val = cluster.get(&pod_gvr, "default", "stable-ts");
    let later_started_at = later_val["status"]["containerStatuses"][0]["state"]["running"]["startedAt"]
        .as_str().unwrap_or("").to_string();
    let later_start_time = later_val["status"]["startTime"]
        .as_str().unwrap_or("").to_string();

    assert_eq!(first_started_at, later_started_at, "startedAt should not change across status updates");
    assert_eq!(first_start_time, later_start_time, "startTime should not change across status updates");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------
// Exponential backoff between restarts
// ---------------------------------------------------------------

#[tokio::test]
async fn kubelet_restart_backoff() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();

    let pod = make_pod("backoff-pod", "busybox:latest");
    cluster.create(&pod_gvr, "default", "backoff-pod", &pod);

    let running = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "backoff-pod",
        |v| has_phase(v, "Running"), TIMEOUT,
    ).await;
    assert!(running, "pod should reach Running");

    // First crash → restartCount = 1
    cluster.runtime.stop_matching("backoff-pod");

    let first_restart = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "backoff-pod",
        |v| {
            let count = v["status"]["containerStatuses"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|c| c["restartCount"].as_i64())
                .unwrap_or(0);
            has_phase(v, "Running") && count >= 1
        },
        TIMEOUT,
    ).await;
    assert!(first_restart, "first restart should complete");

    // Second crash immediately after first restart
    cluster.runtime.stop_matching("backoff-pod");

    // After 2s, pod must NOT have restarted yet (backoff = 10s after first restart)
    tokio::time::sleep(Duration::from_secs(2)).await;
    let val = cluster.get(&pod_gvr, "default", "backoff-pod");
    let count_after_2s = val["status"]["containerStatuses"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|c| c["restartCount"].as_i64())
        .unwrap_or(0);
    assert_eq!(count_after_2s, 1, "pod should not restart within backoff window (restartCount should still be 1)");

    // After the 10s backoff expires the pod should restart (restartCount = 2)
    let second_restart = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "backoff-pod",
        |v| {
            let count = v["status"]["containerStatuses"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|c| c["restartCount"].as_i64())
                .unwrap_or(0);
            has_phase(v, "Running") && count >= 2
        },
        Duration::from_secs(20), // 10s backoff + margin
    ).await;
    assert!(second_restart, "second restart should happen after backoff expires");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------
// CrashLoopBackOff status reporting
// ---------------------------------------------------------------

#[tokio::test]
async fn kubelet_crashloopbackoff_status() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();

    let pod = make_pod("clb-pod", "busybox:latest");
    cluster.create(&pod_gvr, "default", "clb-pod", &pod);

    let running = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "clb-pod",
        |v| has_phase(v, "Running"), TIMEOUT,
    ).await;
    assert!(running, "pod should reach Running");

    // First crash → immediate restart (no backoff for first crash)
    cluster.runtime.stop_matching("clb-pod");

    let first_restart = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "clb-pod",
        |v| {
            let count = v["status"]["containerStatuses"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|c| c["restartCount"].as_i64())
                .unwrap_or(0);
            has_phase(v, "Running") && count >= 1
        },
        TIMEOUT,
    ).await;
    assert!(first_restart, "first restart should complete");

    // Second crash — this triggers the 10s backoff window
    cluster.runtime.stop_matching("clb-pod");

    // During backoff, container state should show CrashLoopBackOff
    let crashloop = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "clb-pod",
        |v| {
            let cs = &v["status"]["containerStatuses"];
            let reason = cs[0]["state"]["waiting"]["reason"].as_str();
            reason == Some("CrashLoopBackOff")
        },
        TIMEOUT,
    ).await;
    assert!(crashloop, "container should show CrashLoopBackOff during backoff");

    // Verify last_state shows Terminated
    let val = cluster.get(&pod_gvr, "default", "clb-pod");
    let cs = &val["status"]["containerStatuses"][0];

    let last_terminated = cs["lastState"]["terminated"]["exitCode"].as_i64();
    assert_eq!(last_terminated, Some(0), "lastState should show Terminated with exit code");

    let reason = cs["lastState"]["terminated"]["reason"].as_str();
    assert_eq!(reason, Some("Completed"), "lastState reason should be Completed for exit code 0");

    // Container should be not ready during CrashLoopBackOff
    assert_eq!(cs["ready"].as_bool(), Some(false), "container should not be ready during CrashLoopBackOff");
    assert_eq!(cs["started"].as_bool(), Some(false), "container should not be started during CrashLoopBackOff");

    cluster.shutdown().await;
}

#[tokio::test]
async fn kubelet_crashloopbackoff_error_exit() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();

    let mut pod = make_pod("clb-err", "busybox:latest");
    pod.spec.as_mut().unwrap().restart_policy = Some("OnFailure".into());
    cluster.create(&pod_gvr, "default", "clb-err", &pod);

    let running = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "clb-err",
        |v| has_phase(v, "Running"), TIMEOUT,
    ).await;
    assert!(running, "pod should reach Running");

    // Crash with non-zero exit code
    cluster.runtime.stop_matching_with_code("clb-err", 137);

    let first_restart = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "clb-err",
        |v| {
            let count = v["status"]["containerStatuses"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|c| c["restartCount"].as_i64())
                .unwrap_or(0);
            has_phase(v, "Running") && count >= 1
        },
        TIMEOUT,
    ).await;
    assert!(first_restart, "first restart should complete");

    // Second crash with error code
    cluster.runtime.stop_matching_with_code("clb-err", 1);

    // During backoff, last_state should show the error exit code
    let crashloop = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "clb-err",
        |v| {
            let cs = &v["status"]["containerStatuses"][0];
            let waiting_reason = cs["state"]["waiting"]["reason"].as_str();
            let last_exit = cs["lastState"]["terminated"]["exitCode"].as_i64();
            let last_reason = cs["lastState"]["terminated"]["reason"].as_str();
            waiting_reason == Some("CrashLoopBackOff")
                && last_exit == Some(1)
                && last_reason == Some("Error")
        },
        TIMEOUT,
    ).await;
    assert!(crashloop, "should show CrashLoopBackOff with Error terminated state");

    cluster.shutdown().await;
}
