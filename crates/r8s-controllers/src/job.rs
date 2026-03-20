use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    GroupVersionResource, JobCondition, JobStatus, ObjectMeta, OwnerReference, Pod,
    PodTemplateSpec, Time,
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
                        let pod: Pod = match serde_json::from_value(event.object) {
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

    let finished = current
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_deref())
        .unwrap_or_default()
        .iter()
        .any(|c| (c.type_ == "Complete" || c.type_ == "Failed") && c.status == "True");
    if finished {
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

    let mut active = 0i32;
    let mut succeeded = 0i32;
    let mut failed = 0i32;

    for pod in &owned {
        match pod.status.as_ref().and_then(|s| s.phase.as_deref()) {
            Some("Succeeded") => succeeded += 1,
            Some("Failed") => failed += 1,
            _ => active += 1,
        }
    }

    let now = Time(chrono::Utc::now());

    let start_time = current
        .status
        .as_ref()
        .and_then(|s| s.start_time.clone())
        .unwrap_or_else(|| now.clone());

    if succeeded >= completions {
        let status = JobStatus {
            active: if active > 0 { Some(active) } else { None },
            succeeded: Some(succeeded),
            failed: Some(failed),
            start_time: Some(start_time),
            completion_time: Some(Time(chrono::Utc::now())),
            conditions: Some(vec![JobCondition {
                type_: "Complete".into(),
                status: "True".into(),
                last_probe_time: Some(now.clone()),
                last_transition_time: Some(now),
                ..Default::default()
            }]),
            ..Default::default()
        };
        update_job_status(store, job_name, job_ns, &status)?;
        tracing::info!("job '{job_name}': completed ({succeeded}/{completions} succeeded)");
        return Ok(());
    }

    if failed > backoff_limit {
        let status = JobStatus {
            active: if active > 0 { Some(active) } else { None },
            succeeded: Some(succeeded),
            failed: Some(failed),
            start_time: Some(start_time),
            completion_time: None,
            conditions: Some(vec![JobCondition {
                type_: "Failed".into(),
                status: "True".into(),
                reason: Some("BackoffLimitExceeded".into()),
                message: Some(format!(
                    "Job has reached the specified backoff limit ({backoff_limit})"
                )),
                last_probe_time: Some(now.clone()),
                last_transition_time: Some(now),
            }]),
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
        active: if new_active > 0 {
            Some(new_active)
        } else {
            None
        },
        succeeded: Some(succeeded),
        failed: Some(failed),
        start_time: Some(start_time),
        completion_time: None,
        conditions: None,
        ..Default::default()
    };
    update_job_status(store, job_name, job_ns, &status)?;

    Ok(())
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
            owner_references: Some(vec![OwnerReference {
                api_version: "batch/v1".into(),
                kind: "Job".into(),
                name: job_name.into(),
                uid: job_uid.into(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
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
    store.create(resource_ref, &serde_json::to_value(&pod)?)?;
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
    updated["status"] = new_status_val;

    match store.update(&resource_ref, &updated) {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("job status update conflict for '{job_name}': {e}");
            Ok(())
        }
    }
}
