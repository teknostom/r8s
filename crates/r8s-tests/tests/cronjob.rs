use std::time::Duration;

use r8s_tests::*;
use r8s_types::GroupVersionResource;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn cronjob_creates_job() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::cron_jobs();
    let job_gvr = GroupVersionResource::jobs();

    cluster.create(
        &gvr,
        "default",
        "test-cron",
        &make_cronjob("test-cron", "* * * * *"),
    );
    let cj_uid = cluster.uid(&gvr, "default", "test-cron");

    let found = wait_for_count(
        &cluster.store,
        &job_gvr,
        Some("default"),
        |v| is_owned_by(v, &cj_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(found, "CronJob should create a Job");

    cluster.shutdown().await;
}

#[tokio::test]
async fn cronjob_suspend() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::cron_jobs();
    let job_gvr = GroupVersionResource::jobs();

    let mut cj = make_cronjob("suspended", "* * * * *");
    cj.spec.as_mut().unwrap().suspend = Some(true);
    cluster.create(&gvr, "default", "suspended", &cj);
    let cj_uid = cluster.uid(&gvr, "default", "suspended");

    // Wait a bit — suspended CronJob should not create any Jobs
    tokio::time::sleep(Duration::from_secs(2)).await;

    let owned = cluster
        .list(&job_gvr, "default")
        .iter()
        .filter(|v| is_owned_by(v, &cj_uid))
        .count();
    assert_eq!(owned, 0, "suspended CronJob should not create any Jobs");

    cluster.shutdown().await;
}

#[tokio::test]
async fn cronjob_concurrency_forbid() {
    let cluster = TestCluster::start().await;
    let gvr = GroupVersionResource::cron_jobs();
    let job_gvr = GroupVersionResource::jobs();

    let mut cj = make_cronjob("forbid-cron", "* * * * *");
    cj.spec.as_mut().unwrap().concurrency_policy = Some("Forbid".into());
    cluster.create(&gvr, "default", "forbid-cron", &cj);
    let cj_uid = cluster.uid(&gvr, "default", "forbid-cron");

    // Wait for first job
    let first_job = wait_for_count(
        &cluster.store,
        &job_gvr,
        Some("default"),
        |v| is_owned_by(v, &cj_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(first_job, "CronJob should create first Job");

    // The first job will still be active (not completed). Wait and verify
    // no second job is created while the first is still running.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let job_count = cluster
        .list(&job_gvr, "default")
        .iter()
        .filter(|v| is_owned_by(v, &cj_uid))
        .count();
    assert_eq!(
        job_count, 1,
        "Forbid policy should prevent creating a second job while first is active"
    );

    cluster.shutdown().await;
}
