use std::collections::BTreeMap;
use std::time::Duration;

use r8s_store::backend::ResourceRef;
use r8s_tests::{TestCluster, wait_for, wait_for_count};
use r8s_types::*;

const TIMEOUT: Duration = Duration::from_secs(15);

fn is_owned_by_uid(val: &serde_json::Value, owner_uid: &str) -> bool {
    val["metadata"]["ownerReferences"]
        .as_array()
        .map(|refs| refs.iter().any(|r| r["uid"].as_str() == Some(owner_uid)))
        .unwrap_or(false)
}

fn get_uid(store: &r8s_store::Store, gvr: &GroupVersionResource, ns: Option<&str>, name: &str) -> String {
    let rref = ResourceRef { gvr, namespace: ns, name };
    store
        .get(&rref)
        .unwrap()
        .unwrap()["metadata"]["uid"]
        .as_str()
        .unwrap()
        .to_string()
}

// ---------------------------------------------------------------
// Deployment → ReplicaSet → Pods
// ---------------------------------------------------------------

#[tokio::test]
async fn deployment_creates_rs_and_pods() {
    let cluster = TestCluster::start().await;
    let store = &cluster.store;

    let deploy = Deployment {
        metadata: ObjectMeta {
            name: Some("nginx".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(3),
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([("app".into(), "nginx".into())])),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("app".into(), "nginx".into())])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "nginx".into(),
                        image: Some("nginx:latest".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    store.create(rref, &serde_json::to_value(&deploy).unwrap()).unwrap();

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
        |v| {
            v["metadata"]["labels"]["app"].as_str() == Some("nginx")
        },
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
    store.create(rref, &serde_json::to_value(&ds).unwrap()).unwrap();

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
    assert!(status_ok, "DaemonSet status should show desired=1, current=1");

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
    store.create(rref, &serde_json::to_value(&job).unwrap()).unwrap();

    // Wait for pod to be created and reach Running
    let job_uid = get_uid(store, &gvr, Some("default"), "test-job");
    let pod_gvr = GroupVersionResource::pods();
    let pod_running = wait_for_count(
        store,
        &pod_gvr,
        Some("default"),
        |v| {
            is_owned_by_uid(v, &job_uid)
                && v["status"]["phase"].as_str() == Some("Running")
        },
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
        |v| {
            is_owned_by_uid(v, &job_uid)
                && v["status"]["phase"].as_str() == Some("Succeeded")
        },
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
    store.create(rref, &serde_json::to_value(&cj).unwrap()).unwrap();

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

    let deploy = Deployment {
        metadata: ObjectMeta {
            name: Some("gc-test".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(2),
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from([("app".into(), "gc-test".into())])),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([("app".into(), "gc-test".into())])),
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
    };

    let gvr = GroupVersionResource::deployments();
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "gc-test",
    };
    store.create(rref, &serde_json::to_value(&deploy).unwrap()).unwrap();

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
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let mut gc_done = false;
    loop {
        if let Ok(result) = store.list(&pod_gvr, Some("default"), None, None, None, None) {
            let remaining = result
                .items
                .iter()
                .filter(|v| v["metadata"]["labels"]["app"].as_str() == Some("gc-test"))
                .count();
            if remaining == 0 {
                gc_done = true;
                break;
            }
        }
        if tokio::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(gc_done, "GC should clean up all pods after deployment deletion");

    // ReplicaSets should also be gone
    let rs_gvr = GroupVersionResource::replica_sets();
    if let Ok(result) = store.list(&rs_gvr, Some("default"), None, None, None, None) {
        let remaining = result
            .items
            .iter()
            .filter(|v| {
                v["metadata"]["name"]
                    .as_str()
                    .map(|n| n.starts_with("gc-test"))
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(remaining, 0, "GC should clean up all ReplicaSets");
    }

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
    store.create(rref, &serde_json::to_value(&sts).unwrap()).unwrap();

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
