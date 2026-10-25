//! CronJob controller.
//!
//! Each tick (and on any CronJob change) evaluates every CronJob's schedule in
//! its `spec.timeZone` (default UTC). For a scheduled slot that elapsed since
//! `status.lastScheduleTime` and is still within `startingDeadlineSeconds`, it
//! creates a Job — subject to `concurrencyPolicy`. Job names derive from the
//! scheduled slot, so re-running a slot is idempotent (the create collides and
//! is ignored).
//!
//! The cron grammar is the standard five fields with lists, ranges, and steps
//! (`a-b/n`, `*/n`, `n/n`), named months (`JAN`..`DEC`) and weekdays
//! (`SUN`..`SAT`, plus `7` = Sunday), and the day-of-month / day-of-week OR rule
//! (when both are restricted, a match on either fires).

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use chrono_tz::Tz;
use r8s_store::{StoreError, backend::ResourceRef, watch::WatchEventType, Store};
use r8s_types::{
    CronJobStatus, GroupVersionResource, Job, JobTemplateSpec, ObjectMeta, ObjectReference, Time,
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

fn is_finished(job: &Job) -> bool {
    crate::job_condition_true(job, &["Complete", "Failed"])
}

fn is_succeeded(job: &Job) -> bool {
    crate::job_condition_true(job, &["Complete"])
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

    if spec.suspend.unwrap_or(false) {
        return Ok(());
    }

    let concurrency_policy = spec.concurrency_policy.as_deref().unwrap_or("Allow");
    let job_template = &spec.job_template;

    let sched = match Schedule::parse(&spec.schedule) {
        Some(s) => s,
        None => {
            tracing::warn!(
                "cronjob '{cj_name}': invalid schedule '{}'; skipping",
                spec.schedule
            );
            return Ok(());
        }
    };
    let tz = match spec.time_zone.as_deref() {
        Some(name) => match name.parse::<Tz>() {
            Ok(tz) => tz,
            Err(_) => {
                tracing::warn!("cronjob '{cj_name}': unknown timeZone '{name}'; using UTC");
                Tz::UTC
            }
        },
        None => Tz::UTC,
    };

    let now = Utc::now();

    let owned_jobs: Vec<Job> = store
        .list_as::<Job>(&GroupVersionResource::jobs(), cj_ns)?
        .into_iter()
        .filter(|j| is_owned_by(&j.metadata, current_uid))
        .collect();
    let active_jobs: Vec<&Job> = owned_jobs.iter().filter(|j| !is_finished(j)).collect();

    let last_schedule = current
        .status
        .as_ref()
        .and_then(|s| s.last_schedule_time.as_ref())
        .map(|t| t.0);
    // Before the first run, measure missed schedules from creation.
    let earliest = last_schedule
        .or_else(|| current.metadata.creation_timestamp.as_ref().map(|t| t.0))
        .unwrap_or(now);

    // The most recent slot we haven't acted on yet, if any.
    let scheduled = most_recent_due(&sched, tz, earliest, now);

    if let Some(slot) = scheduled {
        let missed_by = now - slot;
        let within_deadline = spec
            .starting_deadline_seconds
            .is_none_or(|d| missed_by <= Duration::seconds(d.max(0)));

        if !within_deadline {
            tracing::info!(
                "cronjob '{cj_name}': slot {slot} missed by {}s, past startingDeadlineSeconds; skipping",
                missed_by.num_seconds()
            );
        } else {
            match concurrency_policy {
                "Forbid" if !active_jobs.is_empty() => {
                    tracing::debug!(
                        "cronjob '{cj_name}': slot {slot} skipped (concurrencyPolicy=Forbid, {} active)",
                        active_jobs.len()
                    );
                }
                "Replace" if !active_jobs.is_empty() => {
                    let job_gvr = GroupVersionResource::jobs();
                    for job in &active_jobs {
                        if let Some(name) = job.metadata.name.as_deref() {
                            let _ = store.delete(&ResourceRef {
                                gvr: &job_gvr,
                                namespace: cj_ns,
                                name,
                            });
                            tracing::info!("cronjob '{cj_name}': replaced active job '{name}'");
                        }
                    }
                    create_job(store, cj_name, current_uid, cj_ns, job_template, slot)?;
                }
                _ => {
                    create_job(store, cj_name, current_uid, cj_ns, job_template, slot)?;
                }
            }
        }
    }

    let success_limit = spec.successful_jobs_history_limit.unwrap_or(3) as usize;
    let failed_limit = spec.failed_jobs_history_limit.unwrap_or(1) as usize;
    cleanup_history(store, &owned_jobs, cj_ns, success_limit, failed_limit);

    // Re-list so status reflects the job just created/replaced.
    let owned_now: Vec<Job> = store
        .list_as::<Job>(&GroupVersionResource::jobs(), cj_ns)?
        .into_iter()
        .filter(|j| is_owned_by(&j.metadata, current_uid))
        .collect();

    let active_refs: Vec<ObjectReference> = owned_now
        .iter()
        .filter(|j| !is_finished(j))
        .map(|j| ObjectReference {
            api_version: Some("batch/v1".into()),
            kind: Some("Job".into()),
            name: j.metadata.name.clone(),
            namespace: j.metadata.namespace.clone(),
            uid: j.metadata.uid.clone(),
            ..Default::default()
        })
        .collect();

    let last_successful = owned_now
        .iter()
        .filter(|j| is_succeeded(j))
        .filter_map(|j| j.metadata.creation_timestamp.clone())
        .max_by_key(|t| t.0);

    // Advance lastScheduleTime past the slot we processed (even if concurrency
    // or the deadline skipped the actual run) so the same slot isn't re-evaluated
    // every tick.
    let last_schedule_time = scheduled
        .map(Time)
        .or_else(|| last_schedule.map(Time));

    let new_status = CronJobStatus {
        active: Some(active_refs),
        last_schedule_time,
        last_successful_time: last_successful.or_else(|| {
            current
                .status
                .as_ref()
                .and_then(|s| s.last_successful_time.clone())
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
    if let Some(obj) = updated.as_object_mut() {
        obj.insert("status".to_string(), new_status_val);
    }
    let _ = store.update(&resource_ref, &updated);

    Ok(())
}

fn create_job(
    store: &Store,
    cj_name: &str,
    cj_uid: &str,
    namespace: Option<&str>,
    job_template: &JobTemplateSpec,
    scheduled: DateTime<Utc>,
) -> anyhow::Result<()> {
    // Name by the scheduled slot: re-running the same slot yields the same name,
    // so a duplicate create is an idempotent no-op rather than a second Job.
    let job_name = format!("{cj_name}-{}", scheduled.timestamp());

    let job = Job {
        metadata: ObjectMeta {
            name: Some(job_name.clone()),
            namespace: namespace.map(String::from),
            labels: job_template
                .metadata
                .as_ref()
                .and_then(|m| m.labels.clone()),
            owner_references: Some(vec![crate::owner_reference(
                "batch/v1",
                "CronJob",
                cj_name,
                cj_uid,
            )]),
            ..Default::default()
        },
        spec: job_template.spec.clone(),
        status: None,
    };

    let resource_ref = ResourceRef {
        gvr: &GroupVersionResource::jobs(),
        namespace,
        name: &job_name,
    };
    match store.create(resource_ref, &serde_json::to_value(&job)?) {
        Ok(_) => tracing::info!("cronjob '{cj_name}': created job '{job_name}'"),
        Err(StoreError::AlreadyExists { .. }) => {
            tracing::debug!("cronjob '{cj_name}': job '{job_name}' already exists for this slot");
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

fn cleanup_history(
    store: &Store,
    jobs: &[Job],
    namespace: Option<&str>,
    success_limit: usize,
    failed_limit: usize,
) {
    let gvr = GroupVersionResource::jobs();

    let mut succeeded: Vec<&Job> = jobs.iter().filter(|j| is_succeeded(j)).collect();
    let mut failed: Vec<&Job> = jobs
        .iter()
        .filter(|j| is_finished(j) && !is_succeeded(j))
        .collect();

    succeeded.sort_by_key(|j| j.metadata.creation_timestamp.as_ref().map(|t| t.0));
    failed.sort_by_key(|j| j.metadata.creation_timestamp.as_ref().map(|t| t.0));

    let reap = |batch: &[&Job]| {
        for job in batch {
            if let Some(name) = job.metadata.name.as_deref() {
                let _ = store.delete(&ResourceRef {
                    gvr: &gvr,
                    namespace,
                    name,
                });
            }
        }
    };
    if succeeded.len() > success_limit {
        reap(&succeeded[..succeeded.len() - success_limit]);
    }
    if failed.len() > failed_limit {
        reap(&failed[..failed.len() - failed_limit]);
    }
}

/// The most recent scheduled slot in `(earliest, now]`, or None. Scans backward
/// at minute granularity and returns the first (hence latest) match. Bounded so
/// a long-idle controller can't scan unboundedly; only the most recent missed
/// slot is acted on (cron doesn't backfill).
fn most_recent_due(
    sched: &Schedule,
    tz: Tz,
    earliest: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let now_min = now.with_second(0)?.with_nanosecond(0)?;
    let start = (earliest + Duration::minutes(1))
        .with_second(0)?
        .with_nanosecond(0)?;
    if start > now_min {
        return None;
    }
    const MAX_SCAN_MINUTES: i64 = 366 * 24 * 60;
    let scan_from = if (now_min - start).num_minutes() > MAX_SCAN_MINUTES {
        now_min - Duration::minutes(MAX_SCAN_MINUTES)
    } else {
        start
    };
    let mut t = now_min;
    while t >= scan_from {
        if sched.matches(t, tz) {
            return Some(t);
        }
        t -= Duration::minutes(1);
    }
    None
}

const MONTH_NAMES: &[(&str, u32)] = &[
    ("jan", 1),
    ("feb", 2),
    ("mar", 3),
    ("apr", 4),
    ("may", 5),
    ("jun", 6),
    ("jul", 7),
    ("aug", 8),
    ("sep", 9),
    ("oct", 10),
    ("nov", 11),
    ("dec", 12),
];
const DAY_NAMES: &[(&str, u32)] = &[
    ("sun", 0),
    ("mon", 1),
    ("tue", 2),
    ("wed", 3),
    ("thu", 4),
    ("fri", 5),
    ("sat", 6),
];

struct Schedule {
    minutes: Vec<u32>,
    hours: Vec<u32>,
    doms: Vec<u32>,
    months: Vec<u32>,
    dows: Vec<u32>,
    dom_restricted: bool,
    dow_restricted: bool,
}

impl Schedule {
    fn parse(spec: &str) -> Option<Schedule> {
        let f: Vec<&str> = spec.split_whitespace().collect();
        if f.len() != 5 {
            return None;
        }
        let minutes = parse_cron_field(f[0], 0, 59, &[])?;
        let hours = parse_cron_field(f[1], 0, 23, &[])?;
        let doms = parse_cron_field(f[2], 1, 31, &[])?;
        let months = parse_cron_field(f[3], 1, 12, MONTH_NAMES)?;
        // Day-of-week allows 0-7 with 7 == Sunday; normalize to 0-6.
        let mut dows = parse_cron_field(f[4], 0, 7, DAY_NAMES)?;
        for d in dows.iter_mut() {
            if *d == 7 {
                *d = 0;
            }
        }
        dows.sort_unstable();
        dows.dedup();

        let star = |x: &str| x == "*" || x == "?";
        Some(Schedule {
            minutes,
            hours,
            doms,
            months,
            dows,
            dom_restricted: !star(f[2]),
            dow_restricted: !star(f[4]),
        })
    }

    fn matches(&self, t: DateTime<Utc>, tz: Tz) -> bool {
        let local = t.with_timezone(&tz);
        let dom = local.day();
        let dow = local.weekday().num_days_from_sunday();
        // Cron's OR rule: when BOTH day fields are restricted, either matching
        // fires; otherwise the restricted one (if any) gates.
        let day_ok = match (self.dom_restricted, self.dow_restricted) {
            (false, false) => true,
            (true, false) => self.doms.contains(&dom),
            (false, true) => self.dows.contains(&dow),
            (true, true) => self.doms.contains(&dom) || self.dows.contains(&dow),
        };
        self.minutes.contains(&local.minute())
            && self.hours.contains(&local.hour())
            && self.months.contains(&local.month())
            && day_ok
    }
}

/// Parse one comma-list cron field into its set of matching values. Supports
/// `*`, `a`, `a-b`, and any of those with a `/step`, plus symbolic names.
/// Returns None on any out-of-range or malformed token.
fn parse_cron_field(field: &str, min: u32, max: u32, names: &[(&str, u32)]) -> Option<Vec<u32>> {
    let mut values = Vec::new();
    for part in field.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (r, s.parse::<u32>().ok().filter(|&s| s != 0)?),
            None => (part, 1),
        };
        let (lo, hi) = if range == "*" || range == "?" {
            (min, max)
        } else if let Some((a, b)) = range.split_once('-') {
            (resolve_token(a, names)?, resolve_token(b, names)?)
        } else {
            let v = resolve_token(range, names)?;
            // "n/step" runs n..=max; a bare "n" is just n.
            if part.contains('/') { (v, max) } else { (v, v) }
        };
        if lo > hi || lo < min || hi > max {
            return None;
        }
        let mut v = lo;
        while v <= hi {
            values.push(v);
            v += step;
        }
    }
    if values.is_empty() {
        return None;
    }
    Some(values)
}

fn resolve_token(token: &str, names: &[(&str, u32)]) -> Option<u32> {
    if let Ok(n) = token.parse::<u32>() {
        return Some(n);
    }
    let lower = token.to_ascii_lowercase();
    names.iter().find(|(name, _)| *name == lower).map(|(_, v)| *v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn matches(schedule: &str, ts: &str) -> bool {
        Schedule::parse(schedule).unwrap().matches(dt(ts), Tz::UTC)
    }

    #[test]
    fn basic_fields() {
        assert!(matches("5 4 * * *", "2026-07-01T04:05:00Z"));
        assert!(!matches("5 4 * * *", "2026-07-01T04:06:00Z"));
        // */15 minutes.
        assert!(matches("*/15 * * * *", "2026-07-01T10:30:00Z"));
        assert!(!matches("*/15 * * * *", "2026-07-01T10:31:00Z"));
    }

    #[test]
    fn range_with_step() {
        // Every 2 hours from 9 to 17.
        let s = "0 9-17/2 * * *";
        assert!(matches(s, "2026-07-01T09:00:00Z"));
        assert!(matches(s, "2026-07-01T11:00:00Z"));
        assert!(matches(s, "2026-07-01T17:00:00Z"));
        assert!(!matches(s, "2026-07-01T10:00:00Z"));
    }

    #[test]
    fn named_months_and_days() {
        // 2026-07-01 is a Wednesday.
        assert!(matches("0 0 * JUL WED", "2026-07-01T00:00:00Z"));
        assert!(!matches("0 0 * JAN WED", "2026-07-01T00:00:00Z"));
        // MON-FRI weekday range.
        assert!(matches("0 0 * * MON-FRI", "2026-07-01T00:00:00Z"));
        assert!(!matches("0 0 * * SAT,SUN", "2026-07-01T00:00:00Z"));
    }

    #[test]
    fn sunday_is_zero_or_seven() {
        // 2026-07-05 is a Sunday.
        assert!(matches("0 0 * * 0", "2026-07-05T00:00:00Z"));
        assert!(matches("0 0 * * 7", "2026-07-05T00:00:00Z"));
        assert!(matches("0 0 * * SUN", "2026-07-05T00:00:00Z"));
    }

    #[test]
    fn dom_dow_or_rule() {
        // When both DOM and DOW are restricted, EITHER matching fires.
        // 2026-07-01 is a Wednesday, day-of-month 1.
        // DOM=15 (no), DOW=WED (yes) → fires by OR.
        assert!(matches("0 0 15 * WED", "2026-07-01T00:00:00Z"));
        // DOM=1 (yes), DOW=MON (no) → fires by OR.
        assert!(matches("0 0 1 * MON", "2026-07-01T00:00:00Z"));
        // Neither matches → no fire.
        assert!(!matches("0 0 15 * MON", "2026-07-01T00:00:00Z"));
    }

    #[test]
    fn dom_restricted_dow_star_is_and_only_on_dom() {
        // DOW is '*', so only DOM gates: day 1 matches, day 2 does not.
        assert!(matches("0 0 1 * *", "2026-07-01T00:00:00Z"));
        assert!(!matches("0 0 1 * *", "2026-07-02T00:00:00Z"));
    }

    #[test]
    fn timezone_shifts_match() {
        let s = "0 9 * * *"; // 09:00 local
        let tz: Tz = "America/New_York".parse().unwrap();
        // 2026-07-01 09:00 EDT == 13:00 UTC.
        assert!(Schedule::parse(s).unwrap().matches(dt("2026-07-01T13:00:00Z"), tz));
        assert!(!Schedule::parse(s).unwrap().matches(dt("2026-07-01T09:00:00Z"), tz));
    }

    #[test]
    fn invalid_schedules_rejected() {
        assert!(Schedule::parse("* * * *").is_none()); // 4 fields
        assert!(Schedule::parse("60 * * * *").is_none()); // minute out of range
        assert!(Schedule::parse("0 24 * * *").is_none()); // hour out of range
        assert!(Schedule::parse("0 0 * * 8").is_none()); // dow out of range
        assert!(Schedule::parse("0 0 * BOGUS *").is_none()); // bad month name
    }

    #[test]
    fn most_recent_due_finds_missed_slot() {
        let sched = Schedule::parse("*/5 * * * *").unwrap();
        // last handled 10:00; now 10:12 → most recent slot is 10:10.
        let earliest = dt("2026-07-01T10:00:00Z");
        let now = dt("2026-07-01T10:12:00Z");
        let slot = most_recent_due(&sched, Tz::UTC, earliest, now).unwrap();
        assert_eq!(slot, dt("2026-07-01T10:10:00Z"));
    }

    #[test]
    fn most_recent_due_none_when_no_slot_elapsed() {
        let sched = Schedule::parse("0 0 * * *").unwrap();
        let earliest = dt("2026-07-01T10:00:00Z");
        let now = dt("2026-07-01T10:12:00Z");
        assert!(most_recent_due(&sched, Tz::UTC, earliest, now).is_none());
    }
}
