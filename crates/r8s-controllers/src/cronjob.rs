use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    CronJobStatus, GroupVersionResource, JobTemplateSpec, ObjectMeta, ObjectReference,
    OwnerReference, Time,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::is_owned_by;

pub async fn run(store: Store, shutdown: CancellationToken) -> anyhow::Result<()> {
    tracing::info!("cronjob controller started");

    reconcile_all(&store);

    let mut cj_rx = store.watch(&GroupVersionResource::cron_jobs());
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("cronjob controller shutting down");
                return Ok(());
            }
            _ = tick.tick() => {
                reconcile_all(&store);
            }
            event = cj_rx.recv() => {
                match event {
                    Ok(event) if !matches!(event.event_type, WatchEventType::Deleted) => {
                        if let Err(e) = reconcile_cronjob(&store, &event.object) {
                            tracing::warn!("cronjob reconcile error: {e}");
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
    let gvr = GroupVersionResource::cron_jobs();
    let result = match store.list(&gvr, None, None, None, None, None) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("cronjob controller list error: {e}");
            return;
        }
    };
    for cj in &result.items {
        if let Err(e) = reconcile_cronjob(store, cj) {
            tracing::warn!("cronjob reconcile error: {e}");
        }
    }
}

/// Minimal wrapper to deserialize just what we need from Job status.
#[derive(serde::Deserialize)]
struct JobMeta {
    metadata: ObjectMeta,
    status: Option<JobStatusBrief>,
}

#[derive(serde::Deserialize)]
struct JobStatusBrief {
    conditions: Option<Vec<JobConditionBrief>>,
}

#[derive(serde::Deserialize)]
struct JobConditionBrief {
    #[serde(rename = "type")]
    type_: String,
    status: String,
}

fn is_job_finished(job: &JobMeta) -> bool {
    job.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| {
            cs.iter()
                .any(|c| (c.type_ == "Complete" || c.type_ == "Failed") && c.status == "True")
        })
        .unwrap_or(false)
}

fn is_job_succeeded(job: &JobMeta) -> bool {
    job.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .map(|cs| cs.iter().any(|c| c.type_ == "Complete" && c.status == "True"))
        .unwrap_or(false)
}

fn reconcile_cronjob(store: &Store, cj_value: &serde_json::Value) -> anyhow::Result<()> {
    let cj: r8s_types::CronJob = serde_json::from_value(cj_value.clone())?;
    let cj_name = cj
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("CronJob has no name"))?;
    let cj_ns = cj.metadata.namespace.as_deref();
    let cj_uid = cj.metadata.uid.as_deref().unwrap_or("");

    let gvr = GroupVersionResource::cron_jobs();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace: cj_ns,
        name: cj_name,
    };
    let current: r8s_types::CronJob = match store.get_as(&resource_ref)? {
        Some(v) => v,
        None => return Ok(()),
    };
    let current_uid = current.metadata.uid.as_deref().unwrap_or(cj_uid);

    let spec = current
        .spec
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("CronJob has no spec"))?;

    // Check if suspended
    if spec.suspend.unwrap_or(false) {
        return Ok(());
    }

    let schedule = &spec.schedule;
    let concurrency_policy = spec
        .concurrency_policy
        .as_deref()
        .unwrap_or("Allow");
    let job_template = &spec.job_template;

    let now = chrono::Utc::now();

    // List owned Jobs
    let job_gvr = GroupVersionResource::jobs();
    let owned_jobs: Vec<JobMeta> = store
        .list_as::<JobMeta>(&job_gvr, cj_ns)?
        .into_iter()
        .filter(|j| is_owned_by(&j.metadata, current_uid))
        .collect();

    let active_jobs: Vec<&JobMeta> = owned_jobs.iter().filter(|j| !is_job_finished(j)).collect();

    // Check if we should run
    let last_schedule = current
        .status
        .as_ref()
        .and_then(|s| s.last_schedule_time.as_ref())
        .map(|t| t.0);
    let due = should_run(schedule, last_schedule, now);

    if due {
        match concurrency_policy {
            "Forbid" if !active_jobs.is_empty() => {
                tracing::debug!(
                    "cronjob '{cj_name}': skipping (concurrencyPolicy=Forbid, {} active)",
                    active_jobs.len()
                );
            }
            "Replace" if !active_jobs.is_empty() => {
                // Delete active jobs first
                for job in &active_jobs {
                    if let Some(name) = job.metadata.name.as_deref() {
                        let job_ref = ResourceRef {
                            gvr: &job_gvr,
                            namespace: cj_ns,
                            name,
                        };
                        let _ = store.delete(&job_ref);
                        tracing::info!("cronjob '{cj_name}': replaced active job '{name}'");
                    }
                }
                create_job(store, cj_name, current_uid, cj_ns, job_template)?;
            }
            _ => {
                // Allow (or Forbid/Replace with no active jobs)
                create_job(store, cj_name, current_uid, cj_ns, job_template)?;
            }
        }
    }

    // Enforce history limits
    let success_limit = spec.successful_jobs_history_limit.unwrap_or(3) as usize;
    let failed_limit = spec.failed_jobs_history_limit.unwrap_or(1) as usize;
    cleanup_history(store, &owned_jobs, cj_ns, success_limit, failed_limit);

    // Update status
    let active_refs: Vec<ObjectReference> = owned_jobs
        .iter()
        .filter(|j| !is_job_finished(j))
        .filter_map(|j| {
            Some(ObjectReference {
                api_version: Some("batch/v1".into()),
                kind: Some("Job".into()),
                name: j.metadata.name.clone(),
                namespace: j.metadata.namespace.clone(),
                uid: j.metadata.uid.clone(),
                ..Default::default()
            })
        })
        .collect();

    let last_successful = owned_jobs
        .iter()
        .filter(|j| is_job_succeeded(j))
        .filter_map(|j| j.metadata.creation_timestamp.clone())
        .max_by_key(|t| t.0)
        ;

    let new_status = CronJobStatus {
        active: Some(active_refs),
        last_schedule_time: if due {
            Some(Time(now))
        } else {
            current.status.as_ref().and_then(|s| s.last_schedule_time.clone())
        },
        last_successful_time: last_successful.or_else(|| {
            current.status.as_ref().and_then(|s| s.last_successful_time.clone())
        }),
    };
    let new_status_val = serde_json::to_value(&new_status)?;

    let current_val = match store.get(&resource_ref)? {
        Some(v) => v,
        None => return Ok(()),
    };
    if current_val.get("status") == Some(&new_status_val) {
        return Ok(());
    }
    let mut updated = current_val;
    updated["status"] = new_status_val;
    let _ = store.update(&resource_ref, &updated);

    Ok(())
}

fn create_job(
    store: &Store,
    cj_name: &str,
    cj_uid: &str,
    namespace: Option<&str>,
    job_template: &JobTemplateSpec,
) -> anyhow::Result<()> {
    let timestamp = chrono::Utc::now().timestamp();
    let job_name = format!("{cj_name}-{timestamp}");

    let job = r8s_types::Job {
        metadata: ObjectMeta {
            name: Some(job_name.clone()),
            namespace: namespace.map(String::from),
            labels: job_template
                .metadata
                .as_ref()
                .and_then(|m| m.labels.clone()),
            owner_references: Some(vec![OwnerReference {
                api_version: "batch/v1".into(),
                kind: "CronJob".into(),
                name: cj_name.into(),
                uid: cj_uid.into(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]),
            ..Default::default()
        },
        spec: job_template.spec.clone(),
        status: None,
    };

    let gvr = GroupVersionResource::jobs();
    let resource_ref = ResourceRef {
        gvr: &gvr,
        namespace,
        name: &job_name,
    };
    store.create(resource_ref, &serde_json::to_value(&job)?)?;
    tracing::info!("cronjob '{cj_name}': created job '{job_name}'");
    Ok(())
}

fn cleanup_history(
    store: &Store,
    jobs: &[JobMeta],
    namespace: Option<&str>,
    success_limit: usize,
    failed_limit: usize,
) {
    let gvr = GroupVersionResource::jobs();

    let mut succeeded: Vec<&JobMeta> = jobs.iter().filter(|j| is_job_succeeded(j)).collect();
    let mut failed: Vec<&JobMeta> = jobs
        .iter()
        .filter(|j| is_job_finished(j) && !is_job_succeeded(j))
        .collect();

    // Sort by creation timestamp (oldest first)
    succeeded.sort_by_key(|j| j.metadata.creation_timestamp.as_ref().map(|t| t.0));
    failed.sort_by_key(|j| j.metadata.creation_timestamp.as_ref().map(|t| t.0));

    // Delete excess (oldest first)
    if succeeded.len() > success_limit {
        for job in &succeeded[..succeeded.len() - success_limit] {
            if let Some(name) = job.metadata.name.as_deref() {
                let job_ref = ResourceRef {
                    gvr: &gvr,
                    namespace,
                    name,
                };
                let _ = store.delete(&job_ref);
            }
        }
    }
    if failed.len() > failed_limit {
        for job in &failed[..failed.len() - failed_limit] {
            if let Some(name) = job.metadata.name.as_deref() {
                let job_ref = ResourceRef {
                    gvr: &gvr,
                    namespace,
                    name,
                };
                let _ = store.delete(&job_ref);
            }
        }
    }
}

// --- Minimal cron parser ---

/// Check if the cron schedule is due given the last run time and current time.
fn should_run(
    schedule: &str,
    last_run: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let fields: Vec<&str> = schedule.split_whitespace().collect();
    if fields.len() != 5 {
        tracing::warn!("invalid cron schedule '{schedule}': expected 5 fields");
        return false;
    }

    let minutes = match parse_cron_field(fields[0], 0, 59) {
        Some(v) => v,
        None => return false,
    };
    let hours = match parse_cron_field(fields[1], 0, 23) {
        Some(v) => v,
        None => return false,
    };
    let days_of_month = match parse_cron_field(fields[2], 1, 31) {
        Some(v) => v,
        None => return false,
    };
    let months = match parse_cron_field(fields[3], 1, 12) {
        Some(v) => v,
        None => return false,
    };
    let days_of_week = match parse_cron_field(fields[4], 0, 6) {
        Some(v) => v,
        None => return false,
    };

    use chrono::{Datelike, Timelike};

    let now_min = now.minute();
    let now_hour = now.hour();
    let now_dom = now.day();
    let now_month = now.month();
    let now_dow = now.weekday().num_days_from_sunday();

    let matches = minutes.contains(&now_min)
        && hours.contains(&now_hour)
        && days_of_month.contains(&now_dom)
        && months.contains(&now_month)
        && days_of_week.contains(&now_dow);

    if !matches {
        return false;
    }

    // Don't run if we already ran this minute
    if let Some(last) = last_run {
        if last.date_naive() == now.date_naive()
            && last.time().hour() == now_hour
            && last.time().minute() == now_min
        {
            return false;
        }
    }

    true
}

/// Parse a single cron field into a set of allowed values.
/// Supports: * */N N N-M N,M,O
fn parse_cron_field(field: &str, min: u32, max: u32) -> Option<Vec<u32>> {
    let mut values = Vec::new();

    for part in field.split(',') {
        if part == "*" {
            return Some((min..=max).collect());
        } else if let Some(step) = part.strip_prefix("*/") {
            let step: u32 = step.parse().ok()?;
            if step == 0 {
                return None;
            }
            let mut v = min;
            while v <= max {
                values.push(v);
                v += step;
            }
        } else if part.contains('-') {
            let mut parts = part.split('-');
            let start: u32 = parts.next()?.parse().ok()?;
            let end: u32 = parts.next()?.parse().ok()?;
            for v in start..=end.min(max) {
                values.push(v);
            }
        } else {
            let v: u32 = part.parse().ok()?;
            values.push(v);
        }
    }

    Some(values)
}
