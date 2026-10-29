//! Job controller.
//!
//! Drives a Job's pods toward `spec.completions` successes, honoring
//! `parallelism`, `backoffLimit`, `activeDeadlineSeconds`, and `suspend`.
//!
//! `Indexed` completion mode gives each pod a completion index and tracks
//! `status.completedIndexes`; `podFailurePolicy` rules decide whether a failed
//! pod counts, is ignored, or fails the Job.

use k8s_openapi::api::batch::v1::PodFailurePolicy;
use r8s_store::{Store, backend::ResourceRef, watch::WatchEventType};
use r8s_types::{
    EnvVar, GroupVersionResource, JobCondition, JobStatus, LabelSelector, ObjectMeta, Pod,
    PodTemplateSpec, Time,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

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
                        } else {
                            for job in crate::orphan_candidates(
                                &store,
                                &GroupVersionResource::jobs(),
                                &pod,
                                crate::spec_selector,
                            ) {
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

    let completions = spec.completions.unwrap_or(1);
    let parallelism = spec.parallelism.unwrap_or(1);
    let backoff_limit = spec.backoff_limit.unwrap_or(6);
    let indexed = spec.completion_mode.as_deref() == Some("Indexed");
    let template = &spec.template;

    // The apiserver stamps `spec.selector` on admission; mirror it here so the
    // Job can find its pods by label like upstream's controller does.
    let selector = match spec.selector.clone() {
        Some(sel) => sel,
        None => {
            let sel = LabelSelector {
                match_labels: Some(std::collections::BTreeMap::from([(
                    CONTROLLER_UID_LABEL.to_string(),
                    current_uid.to_string(),
                )])),
                ..Default::default()
            };
            persist_selector(store, &resource_ref, &sel);
            sel
        }
    };
    let owner = crate::owner_reference("batch/v1", "Job", job_name, current_uid);
    let owned = crate::claim_pods(
        store,
        job_ns,
        &owner,
        current.metadata.deletion_timestamp.is_some(),
        |p| crate::selector_matches(&selector, p.metadata.labels.as_ref()),
    )?;

    let suspended = spec.suspend == Some(true);
    let tally = Tally::of(&owned, indexed, spec.pod_failure_policy.as_ref(), job_ns);
    let (active, succeeded, failed) = (tally.active, tally.succeeded, tally.failed);
    let completed_indexes = indexed.then(|| format_indexes(&tally.succeeded_indexes));

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
    let base = JobStatus {
        succeeded: Some(succeeded),
        failed: Some(failed),
        ready: Some(tally.ready),
        completed_indexes: completed_indexes.clone(),
        start_time: start_time.clone(),
        ..Default::default()
    };

    if succeeded >= completions {
        let status = JobStatus {
            active: positive(active),
            completion_time: Some(now.clone()),
            conditions: Some(vec![condition("Complete", "True", None, None, &now)]),
            ..base
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
            ready: None,
            conditions: Some(vec![condition(
                "Suspended",
                "True",
                Some("JobSuspended"),
                Some("Job is suspended"),
                &now,
            )]),
            ..base
        };
        update_job_status(store, job_name, job_ns, &status)?;
        tracing::info!("job '{job_name}': suspended");
        return Ok(());
    }

    let fail_with = |reason: &str, message: &str| -> anyhow::Result<()> {
        delete_active_pods(store, &owned, job_ns);
        let status = JobStatus {
            active: None,
            ready: None,
            conditions: Some(vec![condition(
                "Failed",
                "True",
                Some(reason),
                Some(message),
                &now,
            )]),
            ..base.clone()
        };
        update_job_status(store, job_name, job_ns, &status)?;
        tracing::info!("job '{job_name}': failed ({reason}: {message})");
        Ok(())
    };

    if let Some(message) = tally.fail_job.as_deref() {
        return fail_with("PodFailurePolicy", message);
    }

    // activeDeadlineSeconds: wall-clock cap measured from start. On expiry the
    // Job fails and its running pods are reaped.
    if let (Some(deadline), Some(start)) = (spec.active_deadline_seconds, start_time.as_ref())
        && (now.0 - start.0).num_seconds() >= deadline
    {
        return fail_with(
            "DeadlineExceeded",
            "Job was active longer than specified deadline",
        );
    }

    if failed > backoff_limit {
        let status = JobStatus {
            active: positive(active),
            conditions: Some(vec![condition(
                "Failed",
                "True",
                Some("BackoffLimitExceeded"),
                Some(&format!(
                    "Job has reached the specified backoff limit ({backoff_limit})"
                )),
                &now,
            )]),
            ..base
        };
        update_job_status(store, job_name, job_ns, &status)?;
        tracing::info!("job '{job_name}': failed (backoff limit exceeded, {failed} failures)");
        return Ok(());
    }

    let needed = (completions - succeeded - active)
        .min(parallelism - active)
        .max(0);
    let indexes: Vec<Option<i32>> = if indexed {
        (0..completions)
            .filter(|i| !tally.succeeded_indexes.contains(i) && !tally.active_indexes.contains(i))
            .take(needed as usize)
            .map(Some)
            .collect()
    } else {
        vec![None; needed as usize]
    };
    for index in &indexes {
        create_pod(store, job_name, current_uid, job_ns, template, *index)?;
    }
    if !indexes.is_empty() {
        tracing::info!(
            "job '{job_name}': created {} pods (active={active}, succeeded={succeeded}, failed={failed})",
            indexes.len()
        );
    }

    let status = JobStatus {
        active: positive(active + indexes.len() as i32),
        conditions: None,
        ..base
    };
    update_job_status(store, job_name, job_ns, &status)?;

    Ok(())
}

/// What a Failed pod does to the Job under `spec.podFailurePolicy`.
enum FailureAction {
    Count,
    Ignore,
    FailJob(String),
}

/// Pod counts for one pass.
#[derive(Default)]
struct Tally {
    active: i32,
    ready: i32,
    succeeded: i32,
    failed: i32,
    succeeded_indexes: std::collections::BTreeSet<i32>,
    active_indexes: std::collections::BTreeSet<i32>,
    fail_job: Option<String>,
}

impl Tally {
    fn of(
        pods: &[Pod],
        indexed: bool,
        policy: Option<&PodFailurePolicy>,
        namespace: Option<&str>,
    ) -> Tally {
        let mut t = Tally::default();
        for pod in pods {
            let index = completion_index(pod);
            match crate::pod_phase(pod) {
                "Succeeded" => {
                    t.succeeded += 1;
                    if let Some(i) = index {
                        t.succeeded_indexes.insert(i);
                    }
                }
                "Failed" => match failure_action(policy, pod, namespace) {
                    FailureAction::Ignore => {}
                    FailureAction::Count => t.failed += 1,
                    FailureAction::FailJob(message) => {
                        t.failed += 1;
                        t.fail_job.get_or_insert(message);
                    }
                },
                // Active = not terminal and not being deleted (upstream
                // `IsPodActive`). A terminating pod counted active would stall
                // replacement creation until the store finally dropped it.
                _ if pod.metadata.deletion_timestamp.is_none() => {
                    t.active += 1;
                    if crate::pod_ready(pod) {
                        t.ready += 1;
                    }
                    if let Some(i) = index {
                        t.active_indexes.insert(i);
                    }
                }
                _ => {}
            }
        }
        if indexed {
            t.succeeded = t.succeeded_indexes.len() as i32;
        }
        t
    }
}

fn completion_index(pod: &Pod) -> Option<i32> {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(COMPLETION_INDEX_ANNOTATION))
        .and_then(|s| s.parse().ok())
}

/// `status.completedIndexes`: sorted indexes as ranges, e.g. `0-2,4`.
fn format_indexes(indexes: &std::collections::BTreeSet<i32>) -> String {
    let mut out = Vec::new();
    let mut iter = indexes.iter().copied().peekable();
    while let Some(start) = iter.next() {
        let mut end = start;
        while iter.peek() == Some(&(end + 1)) {
            end = iter.next().unwrap();
        }
        out.push(if start == end {
            start.to_string()
        } else {
            format!("{start}-{end}")
        });
    }
    out.join(",")
}

/// The first matching `podFailurePolicy` rule decides; no match counts the
/// failure against `backoffLimit`.
fn failure_action(
    policy: Option<&PodFailurePolicy>,
    pod: &Pod,
    namespace: Option<&str>,
) -> FailureAction {
    let Some(policy) = policy else {
        return FailureAction::Count;
    };
    let pod_name = pod.metadata.name.as_deref().unwrap_or("");
    let ns = namespace.unwrap_or("default");
    let status = pod.status.as_ref();
    for (i, rule) in policy.rules.iter().enumerate() {
        let action = |message: String| match rule.action.as_str() {
            "Ignore" => FailureAction::Ignore,
            "FailJob" => FailureAction::FailJob(message),
            _ => FailureAction::Count,
        };
        if let Some(req) = rule.on_exit_codes.as_ref() {
            let statuses = status
                .and_then(|s| s.init_container_statuses.as_deref())
                .unwrap_or_default()
                .iter()
                .chain(
                    status
                        .and_then(|s| s.container_statuses.as_deref())
                        .unwrap_or_default(),
                );
            for cs in statuses {
                if req.container_name.as_ref().is_some_and(|n| *n != cs.name) {
                    continue;
                }
                let Some(code) = cs
                    .state
                    .as_ref()
                    .and_then(|s| s.terminated.as_ref())
                    .map(|t| t.exit_code)
                    .filter(|c| *c != 0)
                else {
                    continue;
                };
                let matched = match req.operator.as_str() {
                    "In" => req.values.contains(&code),
                    "NotIn" => !req.values.contains(&code),
                    _ => false,
                };
                if matched {
                    return action(format!(
                        "Container {} for pod {ns}/{pod_name} failed with exit code {code} matching {} rule at index {i}",
                        cs.name, rule.action
                    ));
                }
            }
        } else if let Some(patterns) = rule.on_pod_conditions.as_deref() {
            let conditions = status
                .and_then(|s| s.conditions.as_deref())
                .unwrap_or_default();
            if let Some(pattern) = patterns.iter().find(|p| {
                conditions
                    .iter()
                    .any(|c| c.type_ == p.type_ && c.status == p.status)
            }) {
                return action(format!(
                    "Pod {ns}/{pod_name} has condition {} matching {} rule at index {i}",
                    pattern.type_, rule.action
                ));
            }
        }
    }
    FailureAction::Count
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

/// Create a pod from the template; an Indexed job's pod carries its completion
/// index as annotation, label, `JOB_COMPLETION_INDEX` and hostname.
fn create_pod(
    store: &Store,
    job_name: &str,
    job_uid: &str,
    namespace: Option<&str>,
    template: &PodTemplateSpec,
    index: Option<i32>,
) -> anyhow::Result<()> {
    let pod_name = match index {
        Some(i) => format!("{job_name}-{i}-{}", crate::random_suffix()),
        None => format!("{job_name}-{}", crate::random_suffix()),
    };
    // Template labels plus the controller labels upstream adds, in both the
    // prefixed and legacy spellings; `job-name` is what `kubectl` and the e2e
    // helpers select on.
    let mut labels = template
        .metadata
        .as_ref()
        .and_then(|m| m.labels.clone())
        .unwrap_or_default();
    for (key, value) in [
        (CONTROLLER_UID_LABEL, job_uid),
        (JOB_NAME_LABEL, job_name),
        ("controller-uid", job_uid),
        ("job-name", job_name),
    ] {
        labels
            .entry(key.to_string())
            .or_insert_with(|| value.to_string());
    }
    let mut annotations = template
        .metadata
        .as_ref()
        .and_then(|m| m.annotations.clone())
        .unwrap_or_default();

    let mut spec = template.spec.clone();
    if let Some(ref mut s) = spec {
        s.restart_policy.get_or_insert_with(|| "Never".to_string());
        if let Some(i) = index {
            s.hostname = Some(format!("{job_name}-{i}"));
            for container in s
                .containers
                .iter_mut()
                .chain(s.init_containers.iter_mut().flatten())
            {
                let env = container.env.get_or_insert_with(Vec::new);
                if !env.iter().any(|e| e.name == COMPLETION_INDEX_ENV) {
                    env.push(EnvVar {
                        name: COMPLETION_INDEX_ENV.to_string(),
                        value: Some(i.to_string()),
                        value_from: None,
                    });
                }
            }
        }
    }
    if let Some(i) = index {
        labels.insert(COMPLETION_INDEX_ANNOTATION.to_string(), i.to_string());
        annotations.insert(COMPLETION_INDEX_ANNOTATION.to_string(), i.to_string());
    }

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            namespace: namespace.map(String::from),
            labels: Some(labels),
            annotations: (!annotations.is_empty()).then_some(annotations),
            owner_references: Some(vec![crate::owner_reference(
                "batch/v1", "Job", job_name, job_uid,
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
    crate::pod_admission::admit_pod(store, &mut pod_value);
    store.create(resource_ref, &pod_value)?;
    Ok(())
}

const CONTROLLER_UID_LABEL: &str = "batch.kubernetes.io/controller-uid";
const JOB_NAME_LABEL: &str = "batch.kubernetes.io/job-name";
const COMPLETION_INDEX_ANNOTATION: &str = "batch.kubernetes.io/job-completion-index";
const COMPLETION_INDEX_ENV: &str = "JOB_COMPLETION_INDEX";
/// Condition types this controller owns; others are left as clients set them.
const MANAGED_CONDITIONS: &[&str] = &[
    "Complete",
    "Failed",
    "Suspended",
    "FailureTarget",
    "SuccessCriteriaMet",
];

/// Write `spec.selector` onto a Job created without one. Best effort: a
/// conflict just leaves it for the next reconcile.
fn persist_selector(store: &Store, job_ref: &ResourceRef, selector: &LabelSelector) {
    let Ok(Some(mut job)) = store.get(job_ref) else {
        return;
    };
    let Ok(sel) = serde_json::to_value(selector) else {
        return;
    };
    if let Some(spec) = job.get_mut("spec").and_then(|s| s.as_object_mut()) {
        spec.insert("selector".to_string(), sel);
    }
    if let Err(e) = store.update(job_ref, &job) {
        tracing::debug!("job '{}': persist selector: {e}", job_ref.name);
    }
}

/// Store `status`, keeping conditions of types this controller does not manage.
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

    let mut status = status.clone();
    let foreign: Vec<JobCondition> = current
        .get("status")
        .and_then(|s| s.get("conditions"))
        .cloned()
        .and_then(|c| serde_json::from_value::<Vec<JobCondition>>(c).ok())
        .unwrap_or_default()
        .into_iter()
        .filter(|c| !MANAGED_CONDITIONS.contains(&c.type_.as_str()))
        .collect();
    if !foreign.is_empty() {
        status
            .conditions
            .get_or_insert_with(Vec::new)
            .extend(foreign);
    }

    let new_status_val = serde_json::to_value(&status)?;
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
            "labels": {CONTROLLER_UID_LABEL: uid, JOB_NAME_LABEL: "j"},
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
            .filter(|p| crate::is_owned_by(&p.metadata, uid))
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
        assert_eq!(
            owned_pod_count(&store, &uid),
            0,
            "no replacement while suspended"
        );
        assert!(has_condition(
            &get_job(&store, "j"),
            "Suspended",
            Some("JobSuspended")
        ));
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

    fn create_pod_with(store: &Store, name: &str, uid: &str, meta_extra: Value, status: Value) {
        let gvr = GroupVersionResource::pods();
        let mut meta = json!({
            "name": name, "namespace": "default",
            "labels": {CONTROLLER_UID_LABEL: uid, JOB_NAME_LABEL: "j"},
            "ownerReferences": [{
                "apiVersion": "batch/v1", "kind": "Job", "name": "j",
                "uid": uid, "controller": true
            }]
        });
        if let Some(extra) = meta_extra.as_object() {
            for (k, v) in extra {
                meta[k] = v.clone();
            }
        }
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: Some("default"),
                    name,
                },
                &json!({"apiVersion": "v1", "kind": "Pod", "metadata": meta,
                        "spec": {"containers": []}, "status": status}),
            )
            .unwrap();
    }

    fn owned_pods(store: &Store, uid: &str) -> Vec<Pod> {
        let mut pods: Vec<Pod> = store
            .list_as::<Pod>(&GroupVersionResource::pods(), Some("default"))
            .unwrap()
            .into_iter()
            .filter(|p| crate::is_owned_by(&p.metadata, uid))
            .collect();
        pods.sort_by_key(|p| p.metadata.name.clone());
        pods
    }

    fn set_phase(store: &Store, name: &str, phase: &str) {
        let gvr = GroupVersionResource::pods();
        let rref = ResourceRef {
            gvr: &gvr,
            namespace: Some("default"),
            name,
        };
        let mut pod = store.get(&rref).unwrap().unwrap();
        pod["status"] = json!({"phase": phase});
        store.update(&rref, &pod).unwrap();
    }

    #[test]
    fn indexed_job_assigns_indexes_and_tracks_completed_ones() {
        let (_d, store) = store();
        let job = create_job(
            &store,
            "j",
            json!({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {"name": "j", "namespace": "default"},
                "spec": {"completions": 3, "parallelism": 2, "completionMode": "Indexed",
                         "template": template()}
            }),
        );
        let uid = job["metadata"]["uid"].as_str().unwrap().to_string();
        reconcile_job(&store, &job).unwrap();

        let pods = owned_pods(&store, &uid);
        assert_eq!(pods.len(), 2);
        for (pod, index) in pods.iter().zip(["0", "1"]) {
            let name = pod.metadata.name.as_deref().unwrap();
            assert!(name.starts_with(&format!("j-{index}-")), "{name}");
            assert_eq!(
                pod.metadata.annotations.as_ref().unwrap()[COMPLETION_INDEX_ANNOTATION],
                index
            );
            assert_eq!(
                pod.metadata.labels.as_ref().unwrap()[COMPLETION_INDEX_ANNOTATION],
                index
            );
            let spec = pod.spec.as_ref().unwrap();
            assert_eq!(
                spec.hostname.as_deref(),
                Some(format!("j-{index}").as_str())
            );
            assert_eq!(spec.restart_policy.as_deref(), Some("Never"));
            let env = spec.containers[0].env.as_ref().unwrap();
            assert_eq!(env[0].name, COMPLETION_INDEX_ENV);
            assert_eq!(env[0].value.as_deref(), Some(index));
        }

        let first = pods[0].metadata.name.clone().unwrap();
        set_phase(&store, &first, "Succeeded");
        reconcile_job(&store, &job).unwrap();
        let j = get_job(&store, "j");
        assert_eq!(j["status"]["succeeded"], 1);
        assert_eq!(j["status"]["completedIndexes"], "0");
        let pods = owned_pods(&store, &uid);
        assert_eq!(pods.len(), 3);
        assert!(
            pods.iter()
                .any(|p| p.metadata.name.as_deref().unwrap().starts_with("j-2-"))
        );

        for pod in owned_pods(&store, &uid) {
            set_phase(&store, pod.metadata.name.as_deref().unwrap(), "Succeeded");
        }
        reconcile_job(&store, &job).unwrap();
        let j = get_job(&store, "j");
        assert_eq!(j["status"]["completedIndexes"], "0-2");
        assert!(has_condition(&j, "Complete", None));
    }

    #[test]
    fn pod_failure_policy_fails_job_on_matching_exit_code() {
        let (_d, store) = store();
        let job = create_job(
            &store,
            "j",
            json!({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {"name": "j", "namespace": "default"},
                "spec": {"completions": 4, "parallelism": 2, "template": template(),
                         "podFailurePolicy": {"rules": [
                            {"action": "FailJob", "onExitCodes": {"operator": "In", "values": [1]}}
                         ]}}
            }),
        );
        let uid = job["metadata"]["uid"].as_str().unwrap().to_string();
        create_pod_with(
            &store,
            "j-bad",
            &uid,
            json!({}),
            json!({"phase": "Failed", "containerStatuses": [
                {"name": "c", "state": {"terminated": {"exitCode": 1}}}
            ]}),
        );
        create_pod(&store, "j-run", &uid, Some("Running"), false);
        reconcile_job(&store, &job).unwrap();
        let j = get_job(&store, "j");
        assert!(has_condition(&j, "Failed", Some("PodFailurePolicy")));
        assert!(
            j["status"]["conditions"][0]["message"]
                .as_str()
                .unwrap()
                .contains("exit code 1 matching FailJob rule at index 0")
        );
        assert!(!pod_exists(&store, "j-run"), "active pods are reaped");
    }

    #[test]
    fn pod_failure_policy_ignores_disruption_target_failures() {
        let (_d, store) = store();
        let job = create_job(
            &store,
            "j",
            json!({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {"name": "j", "namespace": "default"},
                "spec": {"completions": 2, "parallelism": 1, "backoffLimit": 0, "template": template(),
                         "podFailurePolicy": {"rules": [
                            {"action": "Ignore", "onPodConditions": [{"type": "DisruptionTarget", "status": "True"}]}
                         ]}}
            }),
        );
        let uid = job["metadata"]["uid"].as_str().unwrap().to_string();
        create_pod_with(
            &store,
            "j-evicted",
            &uid,
            json!({}),
            json!({"phase": "Failed", "conditions": [{"type": "DisruptionTarget", "status": "True"}]}),
        );
        reconcile_job(&store, &job).unwrap();
        let j = get_job(&store, "j");
        assert_eq!(j["status"]["failed"], 0);
        assert!(!has_condition(&j, "Failed", None));
        assert_eq!(
            owned_pod_count(&store, &uid),
            2,
            "a replacement pod is created"
        );
    }

    #[test]
    fn foreign_conditions_and_restart_policy_are_kept() {
        let (_d, store) = store();
        let mut tmpl = template();
        tmpl["spec"]["restartPolicy"] = json!("OnFailure");
        let job = create_job(
            &store,
            "j",
            json!({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {"name": "j", "namespace": "default"},
                "spec": {"template": tmpl},
                "status": {"conditions": [{"type": "CustomConditionType", "status": "True"}]}
            }),
        );
        let uid = job["metadata"]["uid"].as_str().unwrap().to_string();
        reconcile_job(&store, &job).unwrap();
        let j = get_job(&store, "j");
        assert!(has_condition(&j, "CustomConditionType", None));
        assert_eq!(j["status"]["active"], 1);
        let pods = owned_pods(&store, &uid);
        assert_eq!(
            pods[0].spec.as_ref().unwrap().restart_policy.as_deref(),
            Some("OnFailure")
        );
    }
}
