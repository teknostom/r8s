use std::collections::BTreeMap;
use std::time::Duration;

use r8s_store::backend::ResourceRef;
use r8s_tests::{TestCluster, wait_for, wait_for_count, wait_for_exact_count, wait_for_zero};
use r8s_types::*;

const TIMEOUT: Duration = Duration::from_secs(15);

fn is_owned_by_uid(val: &serde_json::Value, owner_uid: &str) -> bool {
    val["metadata"]["ownerReferences"]
        .as_array()
        .map(|refs| refs.iter().any(|r| r["uid"].as_str() == Some(owner_uid)))
        .unwrap_or(false)
}

fn get_uid(
    store: &r8s_store::Store,
    gvr: &GroupVersionResource,
    ns: Option<&str>,
    name: &str,
) -> String {
    let rref = ResourceRef {
        gvr,
        namespace: ns,
        name,
    };
    store.get(&rref).unwrap().unwrap()["metadata"]["uid"]
        .as_str()
        .unwrap()
        .to_string()
}

// ---------------------------------------------------------------
// Deployment → ReplicaSet → Pods
// ---------------------------------------------------------------

fn make_deployment(name: &str, replicas: i32, app_label: &str) -> Deployment {
    Deployment {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([("app".into(), app_label.into())])),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("app".into(), app_label.into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "app".into(),
                        image: Some("nginx:latest".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    }
}

#[tokio::test]
async fn deployment_creates_rs_and_pods() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let deploy = make_deployment("nginx", 3, "nginx");
    let gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    // Wait for at least one ReplicaSet to be created
    let rs_gvr = GroupVersionResource::replica_sets();
    let deploy_uid = get_uid(store, &gvr, Some("default"), "nginx");
    let rs_found = wait_for_count(
        store,
        &rs_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &deploy_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(rs_found, "ReplicaSet should be created for deployment");

    // Wait for 3 pods
    let pod_gvr = GroupVersionResource::pods();
    let pods_found = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| v["metadata"]["labels"]["app"].as_str() == Some("nginx"),
        3,
        TIMEOUT,
    )
    .await;
    assert!(pods_found, "3 pods should be created for deployment");

    // Verify deployment status shows 3 replicas
    let status_ok = wait_for(
        store,
        &gvr,
        Some("default"),
        "nginx",
        |v| v["status"]["replicas"].as_i64() == Some(3),
        TIMEOUT,
    )
    .await;
    assert!(status_ok, "deployment status should show 3 replicas");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------
// DaemonSet → Pod
// ---------------------------------------------------------------

#[tokio::test]
async fn daemonset_creates_pod() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let ds = DaemonSet {
        metadata: ObjectMeta {
            name: Some("node-agent".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(DaemonSetSpec {
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([("app".into(), "node-agent".into())])),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("app".into(), "node-agent".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "agent".into(),
                        image: Some("busybox:latest".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::daemon_sets();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "node-agent",
    };
    store
        .create(rref, &serde_json::to_value(&ds).unwrap())
        .unwrap();

    // Wait for exactly 1 pod
    let ds_uid = get_uid(store, &gvr, Some("default"), "node-agent");
    let pod_gvr = GroupVersionResource::pods();
    let found = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &ds_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(found, "DaemonSet should create exactly 1 pod");

    // Verify DS status
    let status_ok = wait_for(
        store,
        &gvr,
        Some("default"),
        "node-agent",
        |v| {
            v["status"]["desiredNumberScheduled"].as_i64() == Some(1)
                && v["status"]["currentNumberScheduled"].as_i64() == Some(1)
        },
        TIMEOUT,
    )
    .await;
    assert!(
        status_ok,
        "DaemonSet status should show desired=1, current=1"
    );

    cluster.shutdown().await;
}

// ---------------------------------------------------------------
// Job → Pod → Complete
// ---------------------------------------------------------------

#[tokio::test]
async fn job_runs_to_completion() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let job = Job {
        metadata: ObjectMeta {
            name: Some("test-job".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("job-name".into(), "test-job".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "worker".into(),
                        image: Some("busybox:latest".into()),
                        command: Some(vec!["echo".into(), "hello".into()]),
                        ..Default::default()
                    }],
                    restart_policy: Some("Never".into()),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::jobs();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "test-job",
    };
    store
        .create(rref, &serde_json::to_value(&job).unwrap())
        .unwrap();

    // Wait for pod to be created and reach Running
    let job_uid = get_uid(store, &gvr, Some("default"), "test-job");
    let pod_gvr = GroupVersionResource::pods();
    let pod_running = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &job_uid) && v["status"]["phase"].as_str() == Some("Running"),
        1,
        TIMEOUT,
    )
    .await;
    assert!(pod_running, "Job pod should reach Running phase");

    // Simulate container exit (exit code 0)
    cluster.runtime.stop_matching("test-job");

    // Wait for pod to transition to Succeeded (kubelet health check at 500ms)
    let pod_succeeded = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &job_uid) && v["status"]["phase"].as_str() == Some("Succeeded"),
        1,
        TIMEOUT,
    )
    .await;
    assert!(pod_succeeded, "Job pod should transition to Succeeded");

    // Wait for Job to be marked Complete
    let job_complete = wait_for(
        store,
        &gvr,
        Some("default"),
        "test-job",
        |v| {
            v["status"]["conditions"]
                .as_array()
                .map(|cs| {
                    cs.iter().any(|c| {
                        c["type"].as_str() == Some("Complete")
                            && c["status"].as_str() == Some("True")
                    })
                })
                .unwrap_or(false)
        },
        TIMEOUT,
    )
    .await;
    assert!(job_complete, "Job should be marked Complete");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------
// CronJob → Job
// ---------------------------------------------------------------

#[tokio::test]
async fn cronjob_creates_job() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let cj = CronJob {
        metadata: ObjectMeta {
            name: Some("test-cron".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(CronJobSpec {
            schedule: "* * * * *".into(), // every minute — always matches
            job_template: JobTemplateSpec {
                metadata: None,
                spec: Some(JobSpec {
                    template: PodTemplateSpec {
                        metadata: Some(ObjectMeta {
                            labels: Some(BTreeMap::from([("cron".into(), "test".into())])),
                            ..Default::default()
                        }),
                        spec: Some(PodSpec {
                            containers: vec![Container {
                                name: "cron-worker".into(),
                                image: Some("busybox:latest".into()),
                                ..Default::default()
                            }],
                            restart_policy: Some("Never".into()),
                            ..Default::default()
                        }),
                    },
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::cron_jobs();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "test-cron",
    };
    store
        .create(rref, &serde_json::to_value(&cj).unwrap())
        .unwrap();

    // Wait for a Job to be created by the CronJob controller
    let cj_uid = get_uid(store, &gvr, Some("default"), "test-cron");
    let job_gvr = GroupVersionResource::jobs();
    let found = wait_for_count(
        store,
        &job_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &cj_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(found, "CronJob should create a Job");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------
// GC cascade: delete Deployment → RS + Pods cleaned up
// ---------------------------------------------------------------

#[tokio::test]
async fn gc_cascade_delete() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let deploy = make_deployment("gc-test", 2, "gc-test");
    let gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gc-test",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    // Wait for 2 pods to exist
    let pod_gvr = GroupVersionResource::pods();
    let pods_created = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| v["metadata"]["labels"]["app"].as_str() == Some("gc-test"),
        2,
        TIMEOUT,
    )
    .await;
    assert!(pods_created, "2 pods should be created");

    // Delete the deployment
    let del_ref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gc-test",
    };
    store.delete(&del_ref).unwrap();

    // Wait for all gc-test pods to be gone
    let gc_done = wait_for_zero(
        store,
        &pod_gvr,
        Some("default"),
        |v| v["metadata"]["labels"]["app"].as_str() == Some("gc-test"),
        TIMEOUT,
    )
    .await;
    assert!(
        gc_done,
        "GC should clean up all pods after deployment deletion"
    );

    // ReplicaSets should also be gone
    let rs_gvr = GroupVersionResource::replica_sets();
    let rs_gone = wait_for_zero(
        store,
        &rs_gvr,
        Some("default"),
        |v| {
            v["metadata"]["name"]
                .as_str()
                .map(|n| n.starts_with("gc-test"))
                .unwrap_or(false)
        },
        TIMEOUT,
    )
    .await;
    assert!(rs_gone, "GC should clean up all ReplicaSets");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------
// StatefulSet → ordered pods
// ---------------------------------------------------------------

#[tokio::test]
async fn statefulset_ordered_pods() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let sts = StatefulSet {
        metadata: ObjectMeta {
            name: Some("web".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(2),
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([("app".into(), "web".into())])),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("app".into(), "web".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "web".into(),
                        image: Some("nginx:latest".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            service_name: "web".into(),
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::stateful_sets();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "web",
    };
    store
        .create(rref, &serde_json::to_value(&sts).unwrap())
        .unwrap();

    // Wait for web-0 and web-1
    let pod_gvr = GroupVersionResource::pods();
    let web0 = wait_for(store, &pod_gvr, Some("default"), "web-0", |_| true, TIMEOUT).await;
    let web1 = wait_for(store, &pod_gvr, Some("default"), "web-1", |_| true, TIMEOUT).await;

    assert!(web0, "StatefulSet should create pod web-0");
    assert!(web1, "StatefulSet should create pod web-1");

    // Verify STS status
    let status_ok = wait_for(
        store,
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

// ===============================================================
// Controller Scaling & Lifecycle
// ===============================================================

#[tokio::test]
async fn deployment_scale_up() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let deploy = make_deployment("scale-up", 2, "scale-up");
    let gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "scale-up",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let found = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| v["metadata"]["labels"]["app"].as_str() == Some("scale-up"),
        2,
        TIMEOUT,
    )
    .await;
    assert!(found, "should start with 2 pods");

    // Scale up to 5
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "scale-up",
    };
    let mut val = store.get(&rref).unwrap().unwrap();
    val["spec"]["replicas"] = serde_json::json!(5);
    store.update(&rref, &val).unwrap();

    let scaled = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| v["metadata"]["labels"]["app"].as_str() == Some("scale-up"),
        5,
        TIMEOUT,
    )
    .await;
    assert!(scaled, "should scale to 5 pods");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_scale_down() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let deploy = make_deployment("scale-down", 3, "scale-down");
    let gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "scale-down",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let found = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| v["metadata"]["labels"]["app"].as_str() == Some("scale-down"),
        3,
        TIMEOUT,
    )
    .await;
    assert!(found, "should start with 3 pods");

    // Scale down to 1
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "scale-down",
    };
    let mut val = store.get(&rref).unwrap().unwrap();
    val["spec"]["replicas"] = serde_json::json!(1);
    store.update(&rref, &val).unwrap();

    let scaled = wait_for_exact_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| v["metadata"]["labels"]["app"].as_str() == Some("scale-down"),
        1,
        TIMEOUT,
    )
    .await;
    assert!(scaled, "should scale down to 1 pod");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_rolling_update() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let deploy = make_deployment("rolling", 2, "rolling");
    let gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "rolling",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let found = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| v["metadata"]["labels"]["app"].as_str() == Some("rolling"),
        2,
        TIMEOUT,
    )
    .await;
    assert!(found, "should start with 2 pods");

    // Count ReplicaSets before update
    let rs_gvr = GroupVersionResource::replica_sets();
    let deploy_uid = get_uid(store, &gvr, Some("default"), "rolling");
    let rs_before = store
        .list(&rs_gvr, Some("default"), None, None, None, None)
        .unwrap()
        .items
        .iter()
        .filter(|v| is_owned_by_uid(v, &deploy_uid))
        .count();

    // Change the template (different image) to trigger rolling update
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "rolling",
    };
    let mut val = store.get(&rref).unwrap().unwrap();
    val["spec"]["template"]["spec"]["containers"][0]["image"] = serde_json::json!("nginx:1.25");
    store.update(&rref, &val).unwrap();

    // Wait for a new RS to be created
    let new_rs = wait_for_count(
        store,
        &rs_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &deploy_uid),
        rs_before + 1,
        TIMEOUT,
    )
    .await;
    assert!(new_rs, "rolling update should create a new ReplicaSet");

    // Wait for old RS to be scaled to 0
    tokio::time::sleep(Duration::from_millis(500)).await;
    let rs_list = store
        .list(&rs_gvr, Some("default"), None, None, None, None)
        .unwrap();
    let old_rs_scaled_down = rs_list
        .items
        .iter()
        .filter(|v| is_owned_by_uid(v, &deploy_uid))
        .any(|v| v["spec"]["replicas"].as_i64() == Some(0));
    assert!(old_rs_scaled_down, "old ReplicaSet should be scaled to 0");

    cluster.shutdown().await;
}

#[tokio::test]
async fn deployment_zero_replicas() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let deploy = make_deployment("zero", 0, "zero");
    let gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "zero",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    // Give controllers time to reconcile
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify no pods exist with this label
    let pod_gvr = GroupVersionResource::pods();
    let result = store
        .list(&pod_gvr, Some("default"), None, None, None, None)
        .unwrap();
    let count = result
        .items
        .iter()
        .filter(|v| v["metadata"]["labels"]["app"].as_str() == Some("zero"))
        .count();
    assert_eq!(count, 0, "deployment with replicas=0 should create no pods");

    cluster.shutdown().await;
}

#[tokio::test]
async fn replicaset_pod_replacement() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let deploy = make_deployment("replace", 2, "replace");
    let gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "replace",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let found = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| v["metadata"]["labels"]["app"].as_str() == Some("replace"),
        2,
        TIMEOUT,
    )
    .await;
    assert!(found, "should start with 2 pods");

    // Get the name of one pod and delete it
    let pods = store
        .list(&pod_gvr, Some("default"), None, None, None, None)
        .unwrap();
    let victim = pods
        .items
        .iter()
        .find(|v| v["metadata"]["labels"]["app"].as_str() == Some("replace"))
        .unwrap();
    let victim_name = victim["metadata"]["name"].as_str().unwrap();
    let victim_uid = victim["metadata"]["uid"].as_str().unwrap().to_string();

    let pod_ref = ResourceRef {
        gvr: &pod_gvr,
        namespace: Some("default"),
        name: victim_name,
    };
    store.delete(&pod_ref).unwrap();

    // Wait for replacement — 2 pods again, none with the old UID
    let replaced = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| {
            v["metadata"]["labels"]["app"].as_str() == Some("replace")
                && v["metadata"]["uid"].as_str() != Some(&victim_uid)
        },
        2,
        TIMEOUT,
    )
    .await;
    assert!(replaced, "RS should recreate deleted pod");

    cluster.shutdown().await;
}

#[tokio::test]
async fn statefulset_scale_down() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let sts = StatefulSet {
        metadata: ObjectMeta {
            name: Some("sd".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(3),
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([("app".into(), "sd".into())])),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("app".into(), "sd".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "app".into(),
                        image: Some("nginx:latest".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            service_name: "sd".into(),
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::stateful_sets();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "sd",
    };
    store
        .create(rref, &serde_json::to_value(&sts).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let found = wait_for(store, &pod_gvr, Some("default"), "sd-2", |_| true, TIMEOUT).await;
    assert!(found, "sd-2 should exist");

    // Scale down to 1
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "sd",
    };
    let mut val = store.get(&rref).unwrap().unwrap();
    val["spec"]["replicas"] = serde_json::json!(1);
    store.update(&rref, &val).unwrap();

    // sd-2 and sd-1 should be deleted (highest ordinals first)
    let _sd2_gone = wait_for(
        store,
        &pod_gvr,
        Some("default"),
        "sd-2",
        |_| false, // will return false as long as it exists
        Duration::from_secs(1),
    )
    .await;
    // wait_for returns false if condition never met OR resource not found — check directly
    tokio::time::sleep(Duration::from_secs(2)).await;
    let sd2 = store
        .get(&ResourceRef {
            gvr: &pod_gvr,
            namespace: Some("default"),
            name: "sd-2",
        })
        .unwrap();
    let sd1 = store
        .get(&ResourceRef {
            gvr: &pod_gvr,
            namespace: Some("default"),
            name: "sd-1",
        })
        .unwrap();
    let sd0 = store
        .get(&ResourceRef {
            gvr: &pod_gvr,
            namespace: Some("default"),
            name: "sd-0",
        })
        .unwrap();

    assert!(
        sd2.is_none(),
        "sd-2 should be deleted (highest ordinal first)"
    );
    assert!(sd1.is_none(), "sd-1 should be deleted");
    assert!(sd0.is_some(), "sd-0 should still exist");

    cluster.shutdown().await;
}

#[tokio::test]
async fn statefulset_ordinal_gap_fill() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let sts = StatefulSet {
        metadata: ObjectMeta {
            name: Some("gap".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(2),
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([("app".into(), "gap".into())])),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("app".into(), "gap".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "app".into(),
                        image: Some("nginx:latest".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            service_name: "gap".into(),
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::stateful_sets();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gap",
    };
    store
        .create(rref, &serde_json::to_value(&sts).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let found0 = wait_for(store, &pod_gvr, Some("default"), "gap-0", |_| true, TIMEOUT).await;
    let found1 = wait_for(store, &pod_gvr, Some("default"), "gap-1", |_| true, TIMEOUT).await;
    assert!(found0 && found1, "gap-0 and gap-1 should exist");

    // Delete gap-0 (the lowest ordinal)
    let old_uid = get_uid(store, &pod_gvr, Some("default"), "gap-0");
    let pod_ref = ResourceRef {
        gvr: &pod_gvr,
        namespace: Some("default"),
        name: "gap-0",
    };
    store.delete(&pod_ref).unwrap();

    // Wait for gap-0 to be recreated (different UID)
    let recreated = wait_for(
        store,
        &pod_gvr,
        Some("default"),
        "gap-0",
        |v| v["metadata"]["uid"].as_str() != Some(&old_uid),
        TIMEOUT,
    )
    .await;
    assert!(recreated, "STS should recreate gap-0 (not gap-2)");

    // Verify gap-2 was NOT created
    let gap2 = store
        .get(&ResourceRef {
            gvr: &pod_gvr,
            namespace: Some("default"),
            name: "gap-2",
        })
        .unwrap();
    assert!(
        gap2.is_none(),
        "STS should fill ordinal gap, not create gap-2"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn daemonset_pod_replacement() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let ds = DaemonSet {
        metadata: ObjectMeta {
            name: Some("ds-replace".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(DaemonSetSpec {
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([("app".into(), "ds-replace".into())])),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("app".into(), "ds-replace".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "agent".into(),
                        image: Some("busybox:latest".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::daemon_sets();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "ds-replace",
    };
    store
        .create(rref, &serde_json::to_value(&ds).unwrap())
        .unwrap();

    let ds_uid = get_uid(store, &gvr, Some("default"), "ds-replace");
    let pod_gvr = GroupVersionResource::pods();
    let found = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &ds_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(found, "DS should create 1 pod");

    // Find and delete the DS pod
    let pods = store
        .list(&pod_gvr, Some("default"), None, None, None, None)
        .unwrap();
    let victim = pods
        .items
        .iter()
        .find(|v| is_owned_by_uid(v, &ds_uid))
        .unwrap();
    let victim_name = victim["metadata"]["name"].as_str().unwrap();
    let victim_uid = victim["metadata"]["uid"].as_str().unwrap().to_string();

    let pod_ref = ResourceRef {
        gvr: &pod_gvr,
        namespace: Some("default"),
        name: victim_name,
    };
    store.delete(&pod_ref).unwrap();

    // Wait for replacement (different UID)
    let replaced = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &ds_uid) && v["metadata"]["uid"].as_str() != Some(&victim_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(replaced, "DS should recreate deleted pod");

    cluster.shutdown().await;
}

// ===============================================================
// Job & CronJob Edge Cases
// ===============================================================

#[tokio::test]
async fn job_backoff_limit() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let job = Job {
        metadata: ObjectMeta {
            name: Some("fail-job".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            backoff_limit: Some(2),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("job-name".into(), "fail-job".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "fail-worker".into(),
                        image: Some("busybox:latest".into()),
                        ..Default::default()
                    }],
                    restart_policy: Some("Never".into()),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::jobs();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "fail-job",
    };
    store
        .create(rref, &serde_json::to_value(&job).unwrap())
        .unwrap();

    let job_uid = get_uid(store, &gvr, Some("default"), "fail-job");
    let pod_gvr = GroupVersionResource::pods();

    // Simulate repeated failures: wait for pod, fail it, repeat
    for _round in 0..3 {
        let pod_running = wait_for_count(
            store,
            &pod_gvr,
            Some("default"),
            |v| is_owned_by_uid(v, &job_uid) && v["status"]["phase"].as_str() == Some("Running"),
            1,
            TIMEOUT,
        )
        .await;
        if !pod_running {
            break; // Job may already be failed
        }
        // Fail with exit code 1
        cluster.runtime.stop_matching_with_code("fail-job", 1);
        // Wait for kubelet to process the failure
        tokio::time::sleep(Duration::from_millis(800)).await;
    }

    // Wait for Job to be marked Failed
    let job_failed = wait_for(
        store,
        &gvr,
        Some("default"),
        "fail-job",
        |v| {
            v["status"]["conditions"]
                .as_array()
                .map(|cs| {
                    cs.iter().any(|c| {
                        c["type"].as_str() == Some("Failed") && c["status"].as_str() == Some("True")
                    })
                })
                .unwrap_or(false)
        },
        TIMEOUT,
    )
    .await;
    assert!(
        job_failed,
        "Job should be marked Failed after exceeding backoff limit"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn job_parallelism() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let job = Job {
        metadata: ObjectMeta {
            name: Some("par-job".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            completions: Some(3),
            parallelism: Some(2),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("job-name".into(), "par-job".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "par-worker".into(),
                        image: Some("busybox:latest".into()),
                        ..Default::default()
                    }],
                    restart_policy: Some("Never".into()),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::jobs();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "par-job",
    };
    store
        .create(rref, &serde_json::to_value(&job).unwrap())
        .unwrap();

    let job_uid = get_uid(store, &gvr, Some("default"), "par-job");
    let pod_gvr = GroupVersionResource::pods();

    // Wait for 2 pods (parallelism=2)
    let found = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &job_uid) && v["status"]["phase"].as_str() == Some("Running"),
        2,
        TIMEOUT,
    )
    .await;
    assert!(found, "should create 2 pods (parallelism=2)");

    // Verify no more than 2 active pods
    let pods = store
        .list(&pod_gvr, Some("default"), None, None, None, None)
        .unwrap();
    let active = pods
        .items
        .iter()
        .filter(|v| is_owned_by_uid(v, &job_uid))
        .filter(|v| {
            let phase = v["status"]["phase"].as_str();
            phase != Some("Succeeded") && phase != Some("Failed")
        })
        .count();
    assert!(
        active <= 2,
        "max 2 pods should be active at once, got {active}"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn job_already_complete_no_reconcile() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    // Create a job, let it complete, then verify no new pods on re-reconcile
    let job = Job {
        metadata: ObjectMeta {
            name: Some("done-job".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("job-name".into(), "done-job".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "worker".into(),
                        image: Some("busybox:latest".into()),
                        ..Default::default()
                    }],
                    restart_policy: Some("Never".into()),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::jobs();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "done-job",
    };
    store
        .create(rref, &serde_json::to_value(&job).unwrap())
        .unwrap();

    let job_uid = get_uid(store, &gvr, Some("default"), "done-job");
    let pod_gvr = GroupVersionResource::pods();

    // Wait for pod to be running, then complete it
    let running = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &job_uid) && v["status"]["phase"].as_str() == Some("Running"),
        1,
        TIMEOUT,
    )
    .await;
    assert!(running, "job pod should reach Running");

    cluster.runtime.stop_matching("done-job");

    let complete = wait_for(
        store,
        &gvr,
        Some("default"),
        "done-job",
        |v| {
            v["status"]["conditions"]
                .as_array()
                .map(|cs| {
                    cs.iter().any(|c| {
                        c["type"].as_str() == Some("Complete")
                            && c["status"].as_str() == Some("True")
                    })
                })
                .unwrap_or(false)
        },
        TIMEOUT,
    )
    .await;
    assert!(complete, "job should be Complete");

    // Count pods owned by this job
    let pods_before = store
        .list(&pod_gvr, Some("default"), None, None, None, None)
        .unwrap()
        .items
        .iter()
        .filter(|v| is_owned_by_uid(v, &job_uid))
        .count();

    // Trigger a re-reconcile by touching the job
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "done-job",
    };
    let mut val = store.get(&rref).unwrap().unwrap();
    val["metadata"]["annotations"] = serde_json::json!({"test": "trigger"});
    let _ = store.update(&rref, &val);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let pods_after = store
        .list(&pod_gvr, Some("default"), None, None, None, None)
        .unwrap()
        .items
        .iter()
        .filter(|v| is_owned_by_uid(v, &job_uid))
        .count();

    assert_eq!(
        pods_before, pods_after,
        "complete Job should not create more pods on re-reconcile"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn cronjob_suspend() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let cj = CronJob {
        metadata: ObjectMeta {
            name: Some("suspended".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(CronJobSpec {
            schedule: "* * * * *".into(),
            suspend: Some(true),
            job_template: JobTemplateSpec {
                metadata: None,
                spec: Some(JobSpec {
                    template: PodTemplateSpec {
                        metadata: None,
                        spec: Some(PodSpec {
                            containers: vec![Container {
                                name: "worker".into(),
                                image: Some("busybox:latest".into()),
                                ..Default::default()
                            }],
                            restart_policy: Some("Never".into()),
                            ..Default::default()
                        }),
                    },
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::cron_jobs();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "suspended",
    };
    store
        .create(rref, &serde_json::to_value(&cj).unwrap())
        .unwrap();

    let cj_uid = get_uid(store, &gvr, Some("default"), "suspended");
    let job_gvr = GroupVersionResource::jobs();

    // Wait a bit — suspended CronJob should not create any Jobs
    tokio::time::sleep(Duration::from_secs(2)).await;

    let jobs = store
        .list(&job_gvr, Some("default"), None, None, None, None)
        .unwrap();
    let owned = jobs
        .items
        .iter()
        .filter(|v| is_owned_by_uid(v, &cj_uid))
        .count();
    assert_eq!(owned, 0, "suspended CronJob should not create any Jobs");

    cluster.shutdown().await;
}

// ===============================================================
// GC Cascade Chains
// ===============================================================

#[tokio::test]
async fn gc_statefulset_cascade() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let sts = StatefulSet {
        metadata: ObjectMeta {
            name: Some("gc-sts".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(2),
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([("app".into(), "gc-sts".into())])),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("app".into(), "gc-sts".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "app".into(),
                        image: Some("nginx:latest".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            service_name: "gc-sts".into(),
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::stateful_sets();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gc-sts",
    };
    store
        .create(rref, &serde_json::to_value(&sts).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let found = wait_for(
        store,
        &pod_gvr,
        Some("default"),
        "gc-sts-0",
        |_| true,
        TIMEOUT,
    )
    .await;
    assert!(found, "gc-sts-0 should exist");

    // Delete the StatefulSet
    let del_ref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gc-sts",
    };
    store.delete(&del_ref).unwrap();

    // Wait for owned pods to be GC'd
    let gc_done = wait_for_zero(
        store,
        &pod_gvr,
        Some("default"),
        |v| v["metadata"]["labels"]["app"].as_str() == Some("gc-sts"),
        TIMEOUT,
    )
    .await;
    assert!(gc_done, "GC should delete STS owned pods");

    cluster.shutdown().await;
}

#[tokio::test]
async fn gc_daemonset_cascade() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let ds = DaemonSet {
        metadata: ObjectMeta {
            name: Some("gc-ds".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(DaemonSetSpec {
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([("app".into(), "gc-ds".into())])),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("app".into(), "gc-ds".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "agent".into(),
                        image: Some("busybox:latest".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::daemon_sets();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gc-ds",
    };
    store
        .create(rref, &serde_json::to_value(&ds).unwrap())
        .unwrap();

    let ds_uid = get_uid(store, &gvr, Some("default"), "gc-ds");
    let pod_gvr = GroupVersionResource::pods();
    let found = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &ds_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(found, "DS should create 1 pod");

    // Delete DaemonSet
    let del_ref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gc-ds",
    };
    store.delete(&del_ref).unwrap();

    let gc_done = wait_for_zero(
        store,
        &pod_gvr,
        Some("default"),
        |v| v["metadata"]["labels"]["app"].as_str() == Some("gc-ds"),
        TIMEOUT,
    )
    .await;
    assert!(gc_done, "GC should delete DS owned pod");

    cluster.shutdown().await;
}

#[tokio::test]
async fn gc_cronjob_cascade() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let cj = CronJob {
        metadata: ObjectMeta {
            name: Some("gc-cj".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(CronJobSpec {
            schedule: "* * * * *".into(),
            job_template: JobTemplateSpec {
                metadata: None,
                spec: Some(JobSpec {
                    template: PodTemplateSpec {
                        metadata: None,
                        spec: Some(PodSpec {
                            containers: vec![Container {
                                name: "worker".into(),
                                image: Some("busybox:latest".into()),
                                ..Default::default()
                            }],
                            restart_policy: Some("Never".into()),
                            ..Default::default()
                        }),
                    },
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::cron_jobs();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gc-cj",
    };
    store
        .create(rref, &serde_json::to_value(&cj).unwrap())
        .unwrap();

    let cj_uid = get_uid(store, &gvr, Some("default"), "gc-cj");
    let job_gvr = GroupVersionResource::jobs();
    let found = wait_for_count(
        store,
        &job_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &cj_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(found, "CronJob should create a Job");

    // Delete the CronJob
    let del_ref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gc-cj",
    };
    store.delete(&del_ref).unwrap();

    // Jobs should be GC'd
    let gc_done = wait_for_zero(
        store,
        &job_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &cj_uid),
        TIMEOUT,
    )
    .await;
    assert!(gc_done, "GC should delete CronJob owned Jobs");

    cluster.shutdown().await;
}

#[tokio::test]
async fn gc_job_cascade() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let job = Job {
        metadata: ObjectMeta {
            name: Some("gc-job".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("job-name".into(), "gc-job".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "worker".into(),
                        image: Some("busybox:latest".into()),
                        ..Default::default()
                    }],
                    restart_policy: Some("Never".into()),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::jobs();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gc-job",
    };
    store
        .create(rref, &serde_json::to_value(&job).unwrap())
        .unwrap();

    let job_uid = get_uid(store, &gvr, Some("default"), "gc-job");
    let pod_gvr = GroupVersionResource::pods();
    let found = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &job_uid),
        1,
        TIMEOUT,
    )
    .await;
    assert!(found, "Job should create a pod");

    // Delete the Job
    let del_ref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gc-job",
    };
    store.delete(&del_ref).unwrap();

    let gc_done = wait_for_zero(
        store,
        &pod_gvr,
        Some("default"),
        |v| is_owned_by_uid(v, &job_uid),
        TIMEOUT,
    )
    .await;
    assert!(gc_done, "GC should delete Job owned pods");

    cluster.shutdown().await;
}

// ===============================================================
// Endpoints Controller
// ===============================================================

fn make_service(name: &str, selector: BTreeMap<String, String>, port: i32) -> Service {
    Service {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(selector),
            ports: Some(vec![ServicePort {
                port,
                protocol: Some("TCP".into()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

#[tokio::test]
async fn endpoints_from_service_selector() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    // Create a deployment with app=ep-web to get running pods
    let deploy = make_deployment("ep-web", 2, "ep-web");
    let deploy_gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &deploy_gvr,
        namespace: Some("default"),
        name: "ep-web",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let pods_running = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| {
            v["metadata"]["labels"]["app"].as_str() == Some("ep-web")
                && v["status"]["phase"].as_str() == Some("Running")
        },
        2,
        TIMEOUT,
    )
    .await;
    assert!(pods_running, "2 pods should be Running");

    // Create a Service matching those pods
    let svc = make_service(
        "ep-svc",
        BTreeMap::from([("app".into(), "ep-web".into())]),
        80,
    );
    let svc_gvr = GroupVersionResource::services();
    let rref = ResourceRef {
        gvr: &svc_gvr,
        namespace: Some("default"),
        name: "ep-svc",
    };
    store
        .create(rref, &serde_json::to_value(&svc).unwrap())
        .unwrap();

    // Wait for Endpoints to be created with addresses
    let ep_gvr = GroupVersionResource::endpoints();
    let ep_found = wait_for(
        store,
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
    let store = &cluster.store;

    // Create 1 pod
    let deploy = make_deployment("ep-upd", 1, "ep-upd");
    let deploy_gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &deploy_gvr,
        namespace: Some("default"),
        name: "ep-upd",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let running = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| {
            v["metadata"]["labels"]["app"].as_str() == Some("ep-upd")
                && v["status"]["phase"].as_str() == Some("Running")
        },
        1,
        TIMEOUT,
    )
    .await;
    assert!(running, "1 pod should be Running");

    // Create service
    let svc = make_service(
        "ep-upd-svc",
        BTreeMap::from([("app".into(), "ep-upd".into())]),
        80,
    );
    let svc_gvr = GroupVersionResource::services();
    let rref = ResourceRef {
        gvr: &svc_gvr,
        namespace: Some("default"),
        name: "ep-upd-svc",
    };
    store
        .create(rref, &serde_json::to_value(&svc).unwrap())
        .unwrap();

    let ep_gvr = GroupVersionResource::endpoints();
    let ep1 = wait_for(
        store,
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
    let rref = ResourceRef {
        gvr: &deploy_gvr,
        namespace: Some("default"),
        name: "ep-upd",
    };
    let mut val = store.get(&rref).unwrap().unwrap();
    val["spec"]["replicas"] = serde_json::json!(3);
    store.update(&rref, &val).unwrap();

    // Wait for 3 running pods
    let three_running = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| {
            v["metadata"]["labels"]["app"].as_str() == Some("ep-upd")
                && v["status"]["phase"].as_str() == Some("Running")
        },
        3,
        TIMEOUT,
    )
    .await;
    assert!(three_running, "3 pods should be Running");

    // Endpoints should update to 3 addresses
    let ep3 = wait_for(
        store,
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
    let store = &cluster.store;

    // Create a Running pod via deployment
    let deploy = make_deployment("ep-run", 1, "ep-run");
    let deploy_gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &deploy_gvr,
        namespace: Some("default"),
        name: "ep-run",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let running = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| {
            v["metadata"]["labels"]["app"].as_str() == Some("ep-run")
                && v["status"]["phase"].as_str() == Some("Running")
        },
        1,
        TIMEOUT,
    )
    .await;
    assert!(running, "1 pod should be Running");

    // Create a non-running "pending" pod manually (no spec.nodeName so kubelet won't pick it up,
    // but scheduler will assign it... so set nodeName to a fake node)
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
    let rref = ResourceRef {
        gvr: &pod_gvr,
        namespace: Some("default"),
        name: "ep-pending",
    };
    store
        .create(rref, &serde_json::to_value(&pending_pod).unwrap())
        .unwrap();

    // Create service matching both pods
    let svc = make_service(
        "ep-run-svc",
        BTreeMap::from([("app".into(), "ep-run".into())]),
        80,
    );
    let svc_gvr = GroupVersionResource::services();
    let rref = ResourceRef {
        gvr: &svc_gvr,
        namespace: Some("default"),
        name: "ep-run-svc",
    };
    store
        .create(rref, &serde_json::to_value(&svc).unwrap())
        .unwrap();

    // Wait for Endpoints — should only have 1 address (the Running pod)
    let ep_gvr = GroupVersionResource::endpoints();
    let ep_ok = wait_for(
        store,
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
    let store = &cluster.store;

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
            // No selector
            ..Default::default()
        }),
        status: None,
    };

    let svc_gvr = GroupVersionResource::services();
    let rref = ResourceRef {
        gvr: &svc_gvr,
        namespace: Some("default"),
        name: "no-sel",
    };
    store
        .create(rref, &serde_json::to_value(&svc).unwrap())
        .unwrap();

    // Wait a bit, then verify no Endpoints were created
    tokio::time::sleep(Duration::from_secs(1)).await;

    let ep_gvr = GroupVersionResource::endpoints();
    let ep = store
        .get(&ResourceRef {
            gvr: &ep_gvr,
            namespace: Some("default"),
            name: "no-sel",
        })
        .unwrap();
    assert!(
        ep.is_none(),
        "Service without selector should not auto-create Endpoints"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn endpointslice_created() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    // Create a deployment to get running pods
    let deploy = make_deployment("ep-slice", 1, "ep-slice");
    let deploy_gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &deploy_gvr,
        namespace: Some("default"),
        name: "ep-slice",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let running = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| {
            v["metadata"]["labels"]["app"].as_str() == Some("ep-slice")
                && v["status"]["phase"].as_str() == Some("Running")
        },
        1,
        TIMEOUT,
    )
    .await;
    assert!(running, "1 pod should be Running");

    // Create service
    let svc = make_service(
        "ep-slice-svc",
        BTreeMap::from([("app".into(), "ep-slice".into())]),
        80,
    );
    let svc_gvr = GroupVersionResource::services();
    let rref = ResourceRef {
        gvr: &svc_gvr,
        namespace: Some("default"),
        name: "ep-slice-svc",
    };
    store
        .create(rref, &serde_json::to_value(&svc).unwrap())
        .unwrap();

    // Wait for EndpointSlice to be created
    let es_gvr = GroupVersionResource::endpoint_slices();
    let es_found = wait_for(
        store,
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

// ===============================================================
// Namespace Controller
// ===============================================================

#[tokio::test]
async fn namespace_creates_default_sa() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    // Create a new namespace
    let ns = Namespace {
        metadata: ObjectMeta {
            name: Some("test-ns".into()),
            ..Default::default()
        },
        spec: None,
        status: None,
    };
    let ns_gvr = GroupVersionResource::namespaces();
    let rref = ResourceRef {
        gvr: &ns_gvr,
        namespace: None,
        name: "test-ns",
    };
    store
        .create(rref, &serde_json::to_value(&ns).unwrap())
        .unwrap();

    // Wait for the "default" ServiceAccount to be created in this namespace
    let sa_gvr = GroupVersionResource::service_accounts();
    let sa_found = wait_for(
        store,
        &sa_gvr,
        Some("test-ns"),
        "default",
        |_| true,
        TIMEOUT,
    )
    .await;
    assert!(
        sa_found,
        "namespace controller should create 'default' ServiceAccount"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn namespace_sa_idempotent() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    // Pre-create the SA in a namespace that doesn't exist yet
    let sa = ServiceAccount {
        metadata: ObjectMeta {
            name: Some("default".into()),
            namespace: Some("idem-ns".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    let sa_gvr = GroupVersionResource::service_accounts();
    let rref = ResourceRef {
        gvr: &sa_gvr,
        namespace: Some("idem-ns"),
        name: "default",
    };
    store
        .create(rref, &serde_json::to_value(&sa).unwrap())
        .unwrap();
    let old_uid = get_uid(store, &sa_gvr, Some("idem-ns"), "default");

    // Now create the namespace — controller should see SA already exists and not error
    let ns = Namespace {
        metadata: ObjectMeta {
            name: Some("idem-ns".into()),
            ..Default::default()
        },
        spec: None,
        status: None,
    };
    let ns_gvr = GroupVersionResource::namespaces();
    let rref = ResourceRef {
        gvr: &ns_gvr,
        namespace: None,
        name: "idem-ns",
    };
    store
        .create(rref, &serde_json::to_value(&ns).unwrap())
        .unwrap();

    // Give controller time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // SA should still exist with the same UID (not recreated)
    let current_uid = get_uid(store, &sa_gvr, Some("idem-ns"), "default");
    assert_eq!(
        old_uid, current_uid,
        "SA should not be recreated (idempotent)"
    );

    cluster.shutdown().await;
}

// ===============================================================
// CRD Controller
// ===============================================================

#[tokio::test]
async fn crd_registration() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let crd = CustomResourceDefinition {
        metadata: ObjectMeta {
            name: Some("foos.example.com".into()),
            ..Default::default()
        },
        spec: CustomResourceDefinitionSpec {
            group: "example.com".into(),
            names: CustomResourceDefinitionNames {
                plural: "foos".into(),
                singular: Some("foo".into()),
                kind: "Foo".into(),
                short_names: Some(vec!["fo".into()]),
                ..Default::default()
            },
            scope: "Namespaced".into(),
            versions: vec![CustomResourceDefinitionVersion {
                name: "v1".into(),
                served: true,
                storage: true,
                ..Default::default()
            }],
            ..Default::default()
        },
        status: None,
    };

    let crd_gvr = GroupVersionResource::crds();
    let rref = ResourceRef {
        gvr: &crd_gvr,
        namespace: None,
        name: "foos.example.com",
    };
    store
        .create(rref, &serde_json::to_value(&crd).unwrap())
        .unwrap();

    // Wait for it to be registered
    tokio::time::sleep(Duration::from_millis(500)).await;

    let foo_gvr = GroupVersionResource::new("example.com", "v1", "foos");
    let registered = cluster.registry.get_by_gvr(&foo_gvr);
    assert!(
        registered.is_some(),
        "CRD should register resource type in registry"
    );
    assert_eq!(registered.unwrap().kind, "Foo");

    cluster.shutdown().await;
}

#[tokio::test]
async fn crd_unregistration() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let crd = CustomResourceDefinition {
        metadata: ObjectMeta {
            name: Some("bars.example.com".into()),
            ..Default::default()
        },
        spec: CustomResourceDefinitionSpec {
            group: "example.com".into(),
            names: CustomResourceDefinitionNames {
                plural: "bars".into(),
                singular: Some("bar".into()),
                kind: "Bar".into(),
                short_names: None,
                ..Default::default()
            },
            scope: "Namespaced".into(),
            versions: vec![CustomResourceDefinitionVersion {
                name: "v1alpha1".into(),
                served: true,
                storage: true,
                ..Default::default()
            }],
            ..Default::default()
        },
        status: None,
    };

    let crd_gvr = GroupVersionResource::crds();
    let rref = ResourceRef {
        gvr: &crd_gvr,
        namespace: None,
        name: "bars.example.com",
    };
    store
        .create(rref, &serde_json::to_value(&crd).unwrap())
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    let bar_gvr = GroupVersionResource::new("example.com", "v1alpha1", "bars");
    assert!(
        cluster.registry.get_by_gvr(&bar_gvr).is_some(),
        "CRD should be registered"
    );

    // Delete the CRD
    let del_ref = ResourceRef {
        gvr: &crd_gvr,
        namespace: None,
        name: "bars.example.com",
    };
    store.delete(&del_ref).unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        cluster.registry.get_by_gvr(&bar_gvr).is_none(),
        "CRD should be unregistered after deletion"
    );

    cluster.shutdown().await;
}

// ===============================================================
// Kubelet — restartPolicy
// ===============================================================

#[tokio::test]
async fn kubelet_restart_always() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    // Create deployment (restartPolicy defaults to Always) with 1 replica
    let deploy = make_deployment("restart-always", 1, "restart-always");
    let deploy_gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &deploy_gvr,
        namespace: Some("default"),
        name: "restart-always",
    };
    store
        .create(rref, &serde_json::to_value(&deploy).unwrap())
        .unwrap();

    let pod_gvr = GroupVersionResource::pods();
    let running = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| {
            v["metadata"]["labels"]["app"].as_str() == Some("restart-always")
                && v["status"]["phase"].as_str() == Some("Running")
        },
        1,
        TIMEOUT,
    )
    .await;
    assert!(running, "pod should reach Running");

    // Get the current pod's UID
    let pods = store
        .list(&pod_gvr, Some("default"), None, None, None, None)
        .unwrap();
    let original = pods
        .items
        .iter()
        .find(|v| v["metadata"]["labels"]["app"].as_str() == Some("restart-always"))
        .unwrap();
    let original_uid = original["metadata"]["uid"].as_str().unwrap().to_string();

    // Simulate container crash
    cluster.runtime.stop_matching("restart-always");

    // Pod should be deleted (restartPolicy=Always) and RS should recreate
    let replaced = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| {
            v["metadata"]["labels"]["app"].as_str() == Some("restart-always")
                && v["metadata"]["uid"].as_str() != Some(&original_uid)
                && v["status"]["phase"].as_str() == Some("Running")
        },
        1,
        TIMEOUT,
    )
    .await;
    assert!(
        replaced,
        "restartPolicy=Always should delete pod; RS recreates it"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn kubelet_restart_on_failure_success() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    // Create a Job pod (restartPolicy=Never forced by Job controller, but let's test OnFailure manually)
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some("onfail-ok".into()),
            namespace: Some("default".into()),
            labels: Some(BTreeMap::from([("test".into(), "onfail".into())])),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "onfail-worker".into(),
                image: Some("busybox:latest".into()),
                ..Default::default()
            }],
            restart_policy: Some("OnFailure".into()),
            ..Default::default()
        }),
        status: None,
    };

    let pod_gvr = GroupVersionResource::pods();
    let rref = ResourceRef {
        gvr: &pod_gvr,
        namespace: Some("default"),
        name: "onfail-ok",
    };
    store
        .create(rref, &serde_json::to_value(&pod).unwrap())
        .unwrap();

    // Wait for Running
    let running = wait_for(
        store,
        &pod_gvr,
        Some("default"),
        "onfail-ok",
        |v| v["status"]["phase"].as_str() == Some("Running"),
        TIMEOUT,
    )
    .await;
    assert!(running, "pod should reach Running");

    // Simulate successful exit (code 0)
    cluster.runtime.stop_matching("onfail-ok");

    // Pod should be marked Succeeded and kept in store
    let succeeded = wait_for(
        store,
        &pod_gvr,
        Some("default"),
        "onfail-ok",
        |v| v["status"]["phase"].as_str() == Some("Succeeded"),
        TIMEOUT,
    )
    .await;
    assert!(
        succeeded,
        "restartPolicy=OnFailure + exit 0 should mark pod Succeeded"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn kubelet_restart_on_failure_fail() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some("onfail-err".into()),
            namespace: Some("default".into()),
            labels: Some(BTreeMap::from([("test".into(), "onfail-err".into())])),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "onfail-err-worker".into(),
                image: Some("busybox:latest".into()),
                ..Default::default()
            }],
            restart_policy: Some("OnFailure".into()),
            ..Default::default()
        }),
        status: None,
    };

    let pod_gvr = GroupVersionResource::pods();
    let rref = ResourceRef {
        gvr: &pod_gvr,
        namespace: Some("default"),
        name: "onfail-err",
    };
    store
        .create(rref, &serde_json::to_value(&pod).unwrap())
        .unwrap();

    let running = wait_for(
        store,
        &pod_gvr,
        Some("default"),
        "onfail-err",
        |v| v["status"]["phase"].as_str() == Some("Running"),
        TIMEOUT,
    )
    .await;
    assert!(running, "pod should reach Running");

    // Simulate failed exit (code 1)
    cluster.runtime.stop_matching_with_code("onfail-err", 1);

    // Pod should be deleted (OnFailure + failure → delete for retry)
    let _deleted = wait_for(
        store,
        &pod_gvr,
        Some("default"),
        "onfail-err",
        |_| false, // will timeout if pod still exists
        Duration::from_secs(3),
    )
    .await;
    // If the pod was deleted, wait_for will keep returning Ok(Some(val)) until it's gone
    // Then it returns Ok(None) which doesn't call condition. So it times out.
    // Let's check directly after a delay.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let pod_val = store
        .get(&ResourceRef {
            gvr: &pod_gvr,
            namespace: Some("default"),
            name: "onfail-err",
        })
        .unwrap();
    assert!(
        pod_val.is_none(),
        "restartPolicy=OnFailure + exit 1 should delete pod"
    );

    cluster.shutdown().await;
}

// ===============================================================
// Scheduler
// ===============================================================

#[tokio::test]
async fn scheduler_registers_node() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let node_gvr = GroupVersionResource::nodes();
    let node = store
        .get(&ResourceRef {
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
    let store = &cluster.store;

    // Create a standalone pod — scheduler should assign nodeName and set PodScheduled
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some("sched-test".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "app".into(),
                image: Some("nginx:latest".into()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };

    let pod_gvr = GroupVersionResource::pods();
    let rref = ResourceRef {
        gvr: &pod_gvr,
        namespace: Some("default"),
        name: "sched-test",
    };
    store
        .create(rref, &serde_json::to_value(&pod).unwrap())
        .unwrap();

    // Wait for pod to have PodScheduled=True condition
    let scheduled = wait_for(
        store,
        &pod_gvr,
        Some("default"),
        "sched-test",
        |v| {
            v["status"]["conditions"]
                .as_array()
                .map(|cs| {
                    cs.iter().any(|c| {
                        c["type"].as_str() == Some("PodScheduled")
                            && c["status"].as_str() == Some("True")
                    })
                })
                .unwrap_or(false)
        },
        TIMEOUT,
    )
    .await;
    assert!(
        scheduled,
        "scheduler should set PodScheduled=True condition"
    );

    // Also verify nodeName is set
    let val = store
        .get(&ResourceRef {
            gvr: &pod_gvr,
            namespace: Some("default"),
            name: "sched-test",
        })
        .unwrap()
        .unwrap();
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
    let store = &cluster.store;

    // Create a pod with nodeName already set to a different node
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some("pre-sched".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: Some("other-node".into()),
            containers: vec![Container {
                name: "app".into(),
                image: Some("nginx:latest".into()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    };

    let pod_gvr = GroupVersionResource::pods();
    let rref = ResourceRef {
        gvr: &pod_gvr,
        namespace: Some("default"),
        name: "pre-sched",
    };
    store
        .create(rref, &serde_json::to_value(&pod).unwrap())
        .unwrap();

    // Wait a moment
    tokio::time::sleep(Duration::from_millis(500)).await;

    // nodeName should still be "other-node" — scheduler should not re-assign
    let val = store
        .get(&ResourceRef {
            gvr: &pod_gvr,
            namespace: Some("default"),
            name: "pre-sched",
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        val["spec"]["nodeName"].as_str(),
        Some("other-node"),
        "scheduler should not re-schedule already-assigned pod"
    );

    cluster.shutdown().await;
}
