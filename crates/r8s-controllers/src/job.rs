use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    GroupVersionResource, JobCondition, JobStatus, ObjectMeta, OwnerReference, Pod,
    PodTemplateSpec, Time,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::is_owned_by;

fn random_suffix() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvxyz0123456789";
    let mut rng = rand::rng();
    (0..5)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

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
                        if let Some(owner) = pod.metadata.owner_references.as_deref().unwrap_or_default().iter().find(|r| r.kind == "Job") {
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

    // Skip if already Complete or Failed
    if let Some(status) = &current.status {
        if let Some(conditions) = &status.conditions {
            for c in conditions {
                if (c.type_ == "Complete" || c.type_ == "Failed")
                    && c.status == "True"
                {
                    return Ok(());
                }
            }
        }
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
            _ => active += 1, // Pending or Running
        }
    }

    let now = Time(chrono::Utc::now());

    // Set startTime on first reconcile
    let start_time = current
        .status
        .as_ref()
        .and_then(|s| s.start_time.clone())
        .unwrap_or_else(|| now.clone());

    if succeeded >= completions {
        // Job complete
        update_job_status(
            store,
            job_name,
            job_ns,
            active,
            succeeded,
            failed,
            &start_time,
            Some(JobCondition {
                type_: "Complete".into(),
                status: "True".into(),
                last_probe_time: Some(now.clone()),
                last_transition_time: Some(now),
                ..Default::default()
            }),
            true,
        )?;
        tracing::info!("job '{job_name}': completed ({succeeded}/{completions} succeeded)");
        return Ok(());
    }

    if failed > backoff_limit {
        // Job failed
        update_job_status(
            store,
            job_name,
            job_ns,
            active,
            succeeded,
            failed,
            &start_time,
            Some(JobCondition {
                type_: "Failed".into(),
                status: "True".into(),
                reason: Some("BackoffLimitExceeded".into()),
                message: Some(format!("Job has reached the specified backoff limit ({backoff_limit})")),
                last_probe_time: Some(now.clone()),
                last_transition_time: Some(now),
            }),
            false,
        )?;
        tracing::info!("job '{job_name}': failed (backoff limit exceeded, {failed} failures)");
        return Ok(());
    }

    // Create more pods if needed
    let needed = (completions - succeeded - active).min(parallelism - active);
    if needed > 0 {
        for _ in 0..needed {
            create_job_pod(store, job_name, current_uid, job_ns, template)?;
        }
        tracing::info!("job '{job_name}': created {needed} pods (active={active}, succeeded={succeeded}, failed={failed})");
    }

    // Update status
    let new_active = active + needed.max(0);
    update_job_status(
        store,
        job_name,
        job_ns,
        new_active,
        succeeded,
        failed,
        &start_time,
        None,
        false,
    )?;

    Ok(())
}

fn create_job_pod(
    store: &Store,
    job_name: &str,
    job_uid: &str,
    namespace: Option<&str>,
    template: &PodTemplateSpec,
) -> anyhow::Result<()> {
    let pod_name = format!("{job_name}-{}", random_suffix());
    let labels = template
        .metadata
        .as_ref()
        .and_then(|m| m.labels.clone());

    // Force restartPolicy to Never for Job pods
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

#[allow(clippy::too_many_arguments)]
fn update_job_status(
    store: &Store,
    job_name: &str,
    job_ns: Option<&str>,
    active: i32,
    succeeded: i32,
    failed: i32,
    start_time: &Time,
    condition: Option<JobCondition>,
    set_completion_time: bool,
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

    let active_val = if active > 0 { Some(active) } else { None };
    let completion_time = if set_completion_time {
        Some(Time(chrono::Utc::now()))
    } else {
        None
    };
    let conditions = condition.map(|c| vec![c]);

    let new_status = JobStatus {
        active: active_val,
        succeeded: Some(succeeded),
        failed: Some(failed),
        start_time: Some(start_time.clone()),
        completion_time,
        conditions,
        ..Default::default()
    };
    let new_status_val = serde_json::to_value(&new_status)?;
    if current.get("status") == Some(&new_status_val) {
        return Ok(());
    }

    let mut updated = current;
    updated["status"] = new_status_val;

    match store.update(&resource_ref, &updated) {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::debug!("job status update conflict for '{job_name}', will retry: {e}");
            Ok(())
        }
    }
}
