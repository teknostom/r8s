use std::time::Duration;

use r8s_tests::*;
use r8s_types::GroupVersionResource;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn job_runs_to_completion() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::jobs();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "test-job", &make_job("test-job"));
    let job_uid = cluster.uid(&gvr, "default", "test-job");

    // Wait for pod to reach Running
    let pod_running = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| is_owned_by(v, &job_uid) && has_phase(v, "Running"), 1, TIMEOUT,
    ).await;
    assert!(pod_running, "Job pod should reach Running phase");

    // Simulate container exit (exit code 0)
    cluster.runtime.stop_matching("test-job");

    // Wait for pod to transition to Succeeded
    let pod_succeeded = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| is_owned_by(v, &job_uid) && has_phase(v, "Succeeded"), 1, TIMEOUT,
    ).await;
    assert!(pod_succeeded, "Job pod should transition to Succeeded");

    // Wait for Job to be marked Complete
    let job_complete = wait_for(
        &cluster.store, &gvr, Some("default"), "test-job",
        |v| has_condition(v, "Complete", "True"), TIMEOUT,
    ).await;
    assert!(job_complete, "Job should be marked Complete");

    cluster.shutdown().await;
}

#[tokio::test]
async fn job_backoff_limit() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::jobs();
    let pod_gvr = GroupVersionResource::pods();

    let mut job = make_job("fail-job");
    job.spec.as_mut().unwrap().backoff_limit = Some(2);
    cluster.create(&gvr, "default", "fail-job", &job);
    let job_uid = cluster.uid(&gvr, "default", "fail-job");

    // Simulate repeated failures
    for _round in 0..3 {
        let pod_running = wait_for_count(
            &cluster.store, &pod_gvr, Some("default"),
            |v| is_owned_by(v, &job_uid) && has_phase(v, "Running"), 1, TIMEOUT,
        ).await;
        if !pod_running {
            break; // Job may already be failed
        }
        cluster.runtime.stop_matching_with_code("fail-job", 1);
        tokio::time::sleep(Duration::from_millis(800)).await;
    }

    // Wait for Job to be marked Failed
    let job_failed = wait_for(
        &cluster.store, &gvr, Some("default"), "fail-job",
        |v| has_condition(v, "Failed", "True"), TIMEOUT,
    ).await;
    assert!(job_failed, "Job should be marked Failed after exceeding backoff limit");

    cluster.shutdown().await;
}

#[tokio::test]
async fn job_parallelism() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::jobs();
    let pod_gvr = GroupVersionResource::pods();

    let mut job = make_job("par-job");
    job.spec.as_mut().unwrap().completions = Some(3);
    job.spec.as_mut().unwrap().parallelism = Some(2);
    cluster.create(&gvr, "default", "par-job", &job);
    let job_uid = cluster.uid(&gvr, "default", "par-job");

    // Wait for 2 pods (parallelism=2)
    let found = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| is_owned_by(v, &job_uid) && has_phase(v, "Running"), 2, TIMEOUT,
    ).await;
    assert!(found, "should create 2 pods (parallelism=2)");

    // Verify no more than 2 active pods
    let active = cluster.list(&pod_gvr, "default").iter()
        .filter(|v| is_owned_by(v, &job_uid))
        .filter(|v| !has_phase(v, "Succeeded") && !has_phase(v, "Failed"))
        .count();
    assert!(active <= 2, "max 2 pods should be active at once, got {active}");

    cluster.shutdown().await;
}

#[tokio::test]
async fn job_already_complete_no_reconcile() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::jobs();
    let pod_gvr = GroupVersionResource::pods();

    cluster.create(&gvr, "default", "done-job", &make_job("done-job"));
    let job_uid = cluster.uid(&gvr, "default", "done-job");

    // Wait for pod to be running, then complete it
    let running = wait_for_count(
        &cluster.store, &pod_gvr, Some("default"),
        |v| is_owned_by(v, &job_uid) && has_phase(v, "Running"), 1, TIMEOUT,
    ).await;
    assert!(running, "job pod should reach Running");

    cluster.runtime.stop_matching("done-job");

    let complete = wait_for(
        &cluster.store, &gvr, Some("default"), "done-job",
        |v| has_condition(v, "Complete", "True"), TIMEOUT,
    ).await;
    assert!(complete, "job should be Complete");

    // Count pods owned by this job
    let pods_before = cluster.list(&pod_gvr, "default").iter()
        .filter(|v| is_owned_by(v, &job_uid))
        .count();

    // Trigger a re-reconcile by touching the job
    let mut val = cluster.get(&gvr, "default", "done-job");
    val["metadata"]["annotations"] = serde_json::json!({"test": "trigger"});
    cluster.update(&gvr, "default", "done-job", &val);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let pods_after = cluster.list(&pod_gvr, "default").iter()
        .filter(|v| is_owned_by(v, &job_uid))
        .count();
    assert_eq!(pods_before, pods_after, "complete Job should not create more pods on re-reconcile");

    cluster.shutdown().await;
}
