use std::time::Duration;

use r8s_tests::*;
use r8s_types::*;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn gc_cascade_delete() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::deployments();
    let rs_gvr = GroupVersionResource::replica_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "gc-test", &make_deployment("gc-test", 2, "gc-test"));

    let pods_created = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "gc-test"), 2, TIMEOUT,
    ).await;
    assert!(pods_created, "2 pods should be created");

    // Delete the deployment
    cluster.delete(&gvr, "default", "gc-test");

    // Wait for all gc-test pods to be gone
    let gc_done = wait_for_zero(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "gc-test"), TIMEOUT,
    ).await;
    assert!(gc_done, "GC should clean up all pods after deployment deletion");

    // ReplicaSets should also be gone
    let rs_gone = wait_for_zero(
        &cluster.store, &rs_gvr, Some("default"),
        |v| v["metadata"]["name"].as_str().map(|n| n.starts_with("gc-test")).unwrap_or(false),
        TIMEOUT,
    ).await;
    assert!(rs_gone, "GC should clean up all ReplicaSets");

    cluster.shutdown().await;
}

#[tokio::test]
async fn gc_statefulset_cascade() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::stateful_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "gc-sts", &make_statefulset("gc-sts", 2, "gc-sts"));

    let found = wait_for(
        &cluster.store, &pod_gvr, Some("default"), "gc-sts-0",
        |_| true, TIMEOUT,
    ).await;
    assert!(found, "gc-sts-0 should exist");

    cluster.delete(&gvr, "default", "gc-sts");

    let gc_done = wait_for_zero(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "gc-sts"), TIMEOUT,
    ).await;
    assert!(gc_done, "GC should delete STS owned pods");

    cluster.shutdown().await;
}

#[tokio::test]
async fn gc_daemonset_cascade() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::daemon_sets();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "gc-ds", &make_daemonset("gc-ds", "gc-ds"));
    let ds_uid = cluster.uid(&gvr, "default", "gc-ds");

    let found = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| is_owned_by(v, &ds_uid), 1, TIMEOUT,
    ).await;
    assert!(found, "DS should create 1 pod");

    cluster.delete(&gvr, "default", "gc-ds");

    let gc_done = wait_for_zero(
        &cluster.store, &pod_gvr, Some("default"),
        |v| has_label(v, "app", "gc-ds"), TIMEOUT,
    ).await;
    assert!(gc_done, "GC should delete DS owned pod");

    cluster.shutdown().await;
}

#[tokio::test]
async fn gc_cronjob_cascade() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::cron_jobs();
    let job_gvr = GroupVersionResource::jobs();

    cluster.create(&gvr, "default", "gc-cj", &make_cronjob("gc-cj", "* * * * *"));
    let cj_uid = cluster.uid(&gvr, "default", "gc-cj");

    let found = wait_for_count(
        &cluster.store, &job_gvr, Some("default"),
        |v| is_owned_by(v, &cj_uid), 1, TIMEOUT,
    ).await;
    assert!(found, "CronJob should create a Job");

    cluster.delete(&gvr, "default", "gc-cj");

    let gc_done = wait_for_zero(
        &cluster.store, &job_gvr, Some("default"),
        |v| is_owned_by(v, &cj_uid), TIMEOUT,
    ).await;
    assert!(gc_done, "GC should delete CronJob owned Jobs");

    cluster.shutdown().await;
}

#[tokio::test]
async fn gc_job_cascade() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::jobs();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "gc-job", &make_job("gc-job"));
    let job_uid = cluster.uid(&gvr, "default", "gc-job");

    let found = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| is_owned_by(v, &job_uid), 1, TIMEOUT,
    ).await;
    assert!(found, "Job should create a pod");

    cluster.delete(&gvr, "default", "gc-job");

    let gc_done = wait_for_zero(
        &cluster.store, &pod_gvr, Some("default"),
        |v| is_owned_by(v, &job_uid), TIMEOUT,
    ).await;
    assert!(gc_done, "GC should delete Job owned pods");

    cluster.shutdown().await;
}
