//! Job controller.
//!
//! Drives a Job's pods toward `spec.completions` successes, honoring
//! `parallelism`, `backoffLimit`, `activeDeadlineSeconds`, and `suspend`.
//!
//! Only the default (NonIndexed) `completionMode` is modeled: completion is
//! counted by number of succeeded pods, not by covered index. An `Indexed` Job
//! runs but is treated as NonIndexed — pods get no completion-index, so a Job
//! that relies on per-index semantics won't behave to spec. Single-node
//! workloads don't use it.

use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    GroupVersionResource, JobCondition, JobStatus, ObjectMeta, Pod, PodTemplateSpec, Time,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::is_owned_by;

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("job controller started");
    let gvr = GroupVersionResource::jobs();

    reconcile_all(&store);

    let mut job_rx = store.watch(&gvr);
    let mut pod_rx = store.watch(&GroupVersionResource::pods());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("job controller shutting down");
                return Ok(());
            }
            event = job_rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = reconcile_job(&store, &event.object) {
                            tracing::warn!("job reconcile error: {e}");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
            event = pod_rx.recv() => {
                match event {
                    Ok(event) if matches!(event.event_type, WatchEventType::Modified | WatchEventType::Deleted) => {
                        let pod: Pod = match serde_json::from_value(event.object.as_ref().clone()) {
                            Ok(p) => p,
                            Err(_) => continue,
                        };
                        if let Some(owner) = crate::find_owner(&pod.metadata, "Job") {
                            let resource_ref = ResourceRef {
                                gvr: &GroupVersionResource::jobs(),
                                namespace: pod.metadata.namespace.as_deref(),
                                name: &owner.name,
                            };
                            if let Ok(Some(job)) = store.get(&resource_ref) {
                                let _ = reconcile_job(&store, &job);
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => reconcile_all(&store),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

fn reconcile_all(store: &Store) {
    let gvr = GroupVersionResource::jobs();
    let result = match store.list(&gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("job controller list error: {e}");
            return;
        }
    };
    for job in &result.items {
        if let Err(e) = reconcile_job(store, job) {
            tracing::warn!("job reconcile error: {e}");
        }
    }
}

fn reconcile_job(store: &Store, job_value: &serde_json::Value) -> anyhow::Result<()> {
    let job: r8s_types::Job = serde_json::from_value(job_value.clone())?;
    let job_name = job
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Job has no name"))?;
    let job_ns = job.metadata.namespace.as_deref();
    let job_uid = job.metadata.uid.as_deref().unwrap_or("");

    let gvr = GroupVersionResource::jobs();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: job_ns,
        name: job_name,
    };
    let current: r8s_types::Job = match store.get_as(&resource_ref)? {
        Some(v) => v,
        None => return Ok(()),
    };
    let current_uid = current.metadata.uid.as_deref().unwrap_or(job_uid);

    if crate::job_condition_true(&current, &["Complete", "Failed"]) {
        return Ok(());
    }

    let spec = current
        .spec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Job has no spec"))?;

    let completions = spec.completions.unwrap_or(1) as i32;
    let parallelism = spec.parallelism.unwrap_or(1) as i32;
    let backoff_limit = spec.backoff_limit.unwrap_or(6) as i32;
    let template = &spec.template;

    let pod_gvr = GroupVersionResource::pods();
    let owned: Vec<Pod> = store
        .list_as::<Pod>(&pod_gvr, job_ns)?
        .into_iter()
        .filter(|p| is_owned_by(&p.metadata, current_uid))
        .collect();

    let suspended = spec.suspend == Some(true);

    let mut active = 0i32;
    let mut succeeded = 0i32;
    let mut failed = 0i32;

    for pod in &owned {
        match pod.status.as_ref().and_then(|s| s.phase.as_deref()) {
            Some("Succeeded") => succeeded += 1,
            Some("Failed") => failed += 1,
            // Active = not terminal and not being deleted (upstream
            // `IsPodActive`). A terminating pod counted active would stall
            // replacement creation until the store finally dropped it.
            _ if pod.metadata.deletion_timestamp.is_none() => active += 1,
            _ => {}
        }
    }

    let now = Time(chrono::Utc::now());
    let existing_start = current.status.as_ref().and_then(|s| s.start_time.clone());
    // The clock only runs while the Job is admitted to run: a suspended Job that
    // never started has no start time (upstream leaves it nil), so `activeDeadline`
    // isn't measured against a suspend window.
    let start_time = if suspended {
        existing_start.clone()
    } else {
        Some(existing_start.clone().unwrap_or_else(|| now.clone()))
    };

    if succeeded >= completions {
        let status = JobStatus {
            active: positive(active),
            succeeded: Some(succeeded),
            failed: Some(failed),
            start_time,
            completion_time: Some(now.clone()),
            conditions: Some(vec![condition("Complete", "True", None, None, &now)]),
            ..Default::default()
        };
        update_job_status(store, job_name, job_ns, &status)?;
        tracing::info!("job '{job_name}': completed ({succeeded}/{completions} succeeded)");
        return Ok(());
    }

    // Suspended: tear down running pods and stop creating them, but keep the
    // succeeded/failed tallies. Resuming (suspend=false) re-enters the create
    // path below.
    if suspended {
        delete_active_pods(store, &owned, job_ns);
        let status = JobStatus {
            active: None,
            succeeded: Some(succeeded),
            failed: Some(failed),
            start_time,
            completion_time: None,
            conditions: Some(vec![condition(
                "Suspended",
                "True",
                Some("JobSuspended"),
                Some("Job is suspended"),
                &now,
            )]),
            ..Default::default()
        };
        update_job_status(store, job_name, job_ns, &status)?;
        tracing::info!("job '{job_name}': suspended");
        return Ok(());
    }

    // activeDeadlineSeconds: wall-clock cap measured from start. On expiry the
    // Job fails and its running pods are reaped.
    if let (Some(deadline), Some(start)) = (spec.active_deadline_seconds, start_time.as_ref())
        && (now.0 - start.0).num_seconds() >= deadline
    {
        delete_active_pods(store, &owned, job_ns);
        let status = JobStatus {
            active: None,
            succeeded: Some(succeeded),
            failed: Some(failed),
            start_time,
            completion_time: None,
            conditions: Some(vec![condition(
                "Failed",
                "True",
                Some("DeadlineExceeded"),
                Some("Job was active longer than specified deadline"),
                &now,
            )]),
            ..Default::default()
        };
        update_job_status(store, job_name, job_ns, &status)?;
        tracing::info!("job '{job_name}': failed (active deadline {deadline}s exceeded)");
        return Ok(());
    }

    if failed > backoff_limit {
        let status = JobStatus {
            active: positive(active),
            succeeded: Some(succeeded),
            failed: Some(failed),
            start_time,
            completion_time: None,
            conditions: Some(vec![condition(
                "Failed",
                "True",
                Some("BackoffLimitExceeded"),
                Some(&format!(
                    "Job has reached the specified backoff limit ({backoff_limit})"
                )),
                &now,
            )]),
            ..Default::default()
        };
        update_job_status(store, job_name, job_ns, &status)?;
        tracing::info!("job '{job_name}': failed (backoff limit exceeded, {failed} failures)");
        return Ok(());
    }

    let needed = (completions - succeeded - active).min(parallelism - active);
    if needed > 0 {
        for _ in 0..needed {
            create_pod(store, job_name, current_uid, job_ns, template)?;
        }
        tracing::info!(
            "job '{job_name}': created {needed} pods (active={active}, succeeded={succeeded}, failed={failed})"
        );
    }

    let new_active = active + needed.max(0);
    let status = JobStatus {
        active: positive(new_active),
        succeeded: Some(succeeded),
        failed: Some(failed),
        start_time,
        completion_time: None,
        conditions: None,
        ..Default::default()
    };
    update_job_status(store, job_name, job_ns, &status)?;

    Ok(())
}

fn positive(n: i32) -> Option<i32> {
    (n > 0).then_some(n)
}

fn condition(
    type_: &str,
    status: &str,
    reason: Option<&str>,
    message: Option<&str>,
    now: &Time,
) -> JobCondition {
    JobCondition {
        type_: type_.into(),
        status: status.into(),
        reason: reason.map(String::from),
        message: message.map(String::from),
        last_probe_time: Some(now.clone()),
        last_transition_time: Some(now.clone()),
    }
}

/// Delete the Job's still-running pods (used on suspend and deadline expiry).
/// Succeeded/failed pods are left in place — they carry the tally and are useful
/// for inspection.
fn delete_active_pods(store: &Store, owned: &[Pod], namespace: Option<&str>) {
    let gvr = GroupVersionResource::pods();
    for pod in owned {
        let terminal = matches!(
            pod.status.as_ref().and_then(|s| s.phase.as_deref()),
            Some("Succeeded") | Some("Failed")
        );
        if terminal || pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        if let Some(name) = pod.metadata.name.as_deref() {
            let rref = ResourceRef {
                gvr: &gvr,
                namespace,
                name,
            };
            let _ = store.delete(&rref);
        }
    }
}

fn create_pod(
    store: &Store,
    job_name: &str,
    job_uid: &str,
    namespace: Option<&str>,
    template: &PodTemplateSpec,
) -> anyhow::Result<()> {
    let pod_name = format!("{job_name}-{}", crate::random_suffix());
    let labels = template.metadata.as_ref().and_then(|m| m.labels.clone());

    let mut spec = template.spec.clone();
    if let Some(ref mut s) = spec {
        s.restart_policy = Some("Never".into());
    }

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            namespace: namespace.map(String::from),
            labels,
            owner_references: Some(vec![crate::owner_reference(
                "batch/v1",
                "Job",
                job_name,
                job_uid,
            )]),
            ..Default::default()
        },
        spec,
        status: None,
    };

    let gvr = GroupVersionResource::pods();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name: &pod_name,
    };
    let mut pod_value = serde_json::to_value(&pod)?;
    crate::pod_admission::inject_sa_token(store, &mut pod_value);
    store.create(resource_ref, &pod_value)?;
    Ok(())
}

fn update_job_status(
    store: &Store,
    job_name: &str,
    job_ns: Option<&str>,
    status: &JobStatus,
) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::jobs();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: job_ns,
        name: job_name,
    };

    let current = match store.get(&resource_ref)? {
        Some(v) => v,
        None => return Ok(()),
    };

    let new_status_val = serde_json::to_value(status)?;
    if current.get("status") == Some(&new_status_val) {
        return Ok(());
    }

    let mut updated = current;
    if let Some(obj) = updated.as_object_mut() {
        obj.insert("status".to_string(), new_status_val);
    }

    match store.update(&resource_ref, &updated) {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("job status update conflict for '{job_name}': {e}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(&dir.path().join("t.db")).unwrap();
        (dir, s)
    }

    fn template() -> Value {
        json!({
            "metadata": {"labels": {"app": "j"}},
            "spec": {"containers": [{"name": "c", "image": "img"}]}
        })
    }

    fn create_job(store: &Store, name: &str, obj: Value) -> Value {
        let gvr = GroupVersionResource::jobs();
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: Some("default"),
                    name,
                },
                &obj,
            )
            .unwrap()
    }

    fn create_pod(store: &Store, name: &str, uid: &str, phase: Option<&str>, terminating: bool) {
        let gvr = GroupVersionResource::pods();
        let mut meta = json!({
            "name": name, "namespace": "default",
            "ownerReferences": [{
                "apiVersion": "batch/v1", "kind": "Job", "name": "j",
                "uid": uid, "controller": true
            }]
        });
        if terminating {
            meta["deletionTimestamp"] = json!("2020-01-01T00:00:00Z");
        }
        let mut obj = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": meta,
            "spec": {"containers": []}
        });
        if let Some(p) = phase {
            obj["status"] = json!({"phase": p});
        }
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: Some("default"),
                    name,
                },
                &obj,
            )
            .unwrap();
    }

    fn get_job(store: &Store, name: &str) -> Value {
        let gvr = GroupVersionResource::jobs();
        store
            .get(&ResourceRef {
                gvr: &gvr,
                namespace: Some("default"),
                name,
            })
            .unwrap()
            .unwrap()
    }

    fn pod_exists(store: &Store, name: &str) -> bool {
        let gvr = GroupVersionResource::pods();
        store
            .get(&ResourceRef {
                gvr: &gvr,
                namespace: Some("default"),
                name,
            })
            .unwrap()
            .is_some()
    }

    fn owned_pod_count(store: &Store, uid: &str) -> usize {
        let gvr = GroupVersionResource::pods();
        store
            .list_as::<Pod>(&gvr, Some("default"))
            .unwrap()
            .iter()
            .filter(|p| is_owned_by(&p.metadata, uid))
            .count()
    }

    fn has_condition(job: &Value, type_: &str, reason: Option<&str>) -> bool {
        job["status"]["conditions"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|c| {
                c["type"] == type_
                    && c["status"] == "True"
                    && reason.is_none_or(|r| c["reason"] == r)
            })
    }

    #[test]
    fn suspend_deletes_active_pods_and_marks_condition() {
        let (_d, store) = store();
        let job = create_job(
            &store,
            "j",
            json!({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {"name": "j", "namespace": "default"},
                "spec": {"suspend": true, "template": template()}
            }),
        );
        let uid = job["metadata"]["uid"].as_str().unwrap().to_string();
        create_pod(&store, "j-run", &uid, Some("Running"), false);

        reconcile_job(&store, &job).unwrap();

        assert!(!pod_exists(&store, "j-run"), "suspend reaps running pods");
        assert_eq!(owned_pod_count(&store, &uid), 0, "no replacement while suspended");
        assert!(has_condition(&get_job(&store, "j"), "Suspended", Some("JobSuspended")));
    }

    #[test]
    fn active_deadline_fails_job() {
        let (_d, store) = store();
        let job = create_job(
            &store,
            "j",
            json!({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {"name": "j", "namespace": "default"},
                "spec": {"activeDeadlineSeconds": 1, "template": template()},
                "status": {"startTime": "2000-01-01T00:00:00Z"}
            }),
        );
        let uid = job["metadata"]["uid"].as_str().unwrap().to_string();
        create_pod(&store, "j-run", &uid, Some("Running"), false);

        reconcile_job(&store, &job).unwrap();

        assert!(!pod_exists(&store, "j-run"), "deadline reaps running pods");
        assert!(has_condition(
            &get_job(&store, "j"),
            "Failed",
            Some("DeadlineExceeded")
        ));
    }

    #[test]
    fn terminating_pod_is_not_active_so_replacement_is_created() {
        let (_d, store) = store();
        let job = create_job(
            &store,
            "j",
            json!({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {"name": "j", "namespace": "default"},
                "spec": {"template": template()}
            }),
        );
        let uid = job["metadata"]["uid"].as_str().unwrap().to_string();
        // A running pod that's being deleted must not count as active.
        create_pod(&store, "j-old", &uid, Some("Running"), true);

        reconcile_job(&store, &job).unwrap();

        assert_eq!(
            owned_pod_count(&store, &uid),
            2,
            "terminating pod plus a fresh replacement"
        );
        assert_eq!(get_job(&store, "j")["status"]["active"], json!(1));
    }

    #[test]
    fn completes_when_succeeded_meets_completions() {
        let (_d, store) = store();
        let job = create_job(
            &store,
            "j",
            json!({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {"name": "j", "namespace": "default"},
                "spec": {"completions": 1, "template": template()}
            }),
        );
        let uid = job["metadata"]["uid"].as_str().unwrap().to_string();
        create_pod(&store, "j-done", &uid, Some("Succeeded"), false);

        reconcile_job(&store, &job).unwrap();

        let j = get_job(&store, "j");
        assert!(has_condition(&j, "Complete", None));
        assert!(j["status"]["completionTime"].is_string());
    }

    #[test]
    fn backoff_limit_exceeded_fails_job() {
        let (_d, store) = store();
        let job = create_job(
            &store,
            "j",
            json!({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {"name": "j", "namespace": "default"},
                "spec": {"backoffLimit": 2, "template": template()}
            }),
        );
        let uid = job["metadata"]["uid"].as_str().unwrap().to_string();
        for i in 0..3 {
            create_pod(&store, &format!("j-f{i}"), &uid, Some("Failed"), false);
        }

        reconcile_job(&store, &job).unwrap();

        assert!(has_condition(
            &get_job(&store, "j"),
            "Failed",
            Some("BackoffLimitExceeded")
        ));
    }
}
