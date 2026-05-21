use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rustc_hash::{FxHashMap, FxHashSet};

type LabelMap = FxHashMap<String, FxHashMap<String, FxHashSet<String>>>;

#[derive(Default, Clone)]
pub struct LabelIndex {
    index: Arc<Mutex<LabelMap>>,
}

pub struct FieldSelector {
    pub requirements: Vec<FieldRequirement>,
}

pub enum Requirement {
    Equals(String, String),
    NotEquals(String, String),
    Exists(String),
    NotExists(String),
}

pub enum FieldRequirement {
    Equals(String, String),
    NotEquals(String, String),
}

pub struct LabelSelector {
    pub requirements: Vec<Requirement>,
}

impl LabelSelector {
    pub fn matches(&self, object: &serde_json::Value) -> bool {
        let labels = object
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.as_object());
        self.requirements.iter().all(|req| match req {
            Requirement::Equals(key, value) => {
                labels.and_then(|l| l.get(key)).and_then(|v| v.as_str()) == Some(value.as_str())
            }
            Requirement::NotEquals(key, value) => {
                labels.and_then(|l| l.get(key)).and_then(|v| v.as_str()) != Some(value.as_str())
            }
            Requirement::Exists(key) => labels.is_some_and(|l| l.contains_key(key)),
            Requirement::NotExists(key) => !labels.is_some_and(|l| l.contains_key(key)),
        })
    }

    pub fn parse(s: &str) -> anyhow::Result<Self> {
        if s.is_empty() {
            return Ok(Self {
                requirements: vec![],
            });
        }

        Ok(Self {
            requirements: s
                .split(',')
                .map(|part| {
                    let part = part.trim();
                    if let Some((key, value)) = part.split_once("!=") {
                        Requirement::NotEquals(key.into(), value.into())
                    } else if let Some((key, value)) = part.split_once('=') {
                        Requirement::Equals(key.into(), value.into())
                    } else if let Some(key) = part.strip_prefix('!') {
                        Requirement::NotExists(key.into())
                    } else {
                        Requirement::Exists(part.into())
                    }
                })
                .collect(),
        })
    }
}

impl FieldSelector {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        if s.is_empty() {
            return Ok(Self {
                requirements: vec![],
            });
        }

        let mut requirements = Vec::new();
        for part in s.split(',') {
            let part = part.trim();
            if let Some((key, value)) = part.split_once("!=") {
                requirements.push(FieldRequirement::NotEquals(key.into(), value.into()));
            } else if let Some((key, value)) = part.split_once('=') {
                requirements.push(FieldRequirement::Equals(key.into(), value.into()));
            } else {
                anyhow::bail!("invalid field selector: {part}");
            }
        }
        Ok(Self { requirements })
    }

    /// Match a stored object against the selector. `gvr` is the resource's
    /// key prefix (e.g. `"/v1/nodes"`) so we can apply the same per-resource
    /// selectable-field conversion Kubernetes does — including supplying the
    /// zero value for absent boolean defaults (e.g. `spec.unschedulable` on
    /// Node, which the e2e suite filters on before any test runs).
    pub fn matches(&self, object: &serde_json::Value, gvr: &str) -> bool {
        self.requirements.iter().all(|req| match req {
            FieldRequirement::Equals(field, expected) => {
                selectable_field(gvr, object, field).as_deref() == Some(expected.as_str())
            }
            FieldRequirement::NotEquals(field, expected) => {
                selectable_field(gvr, object, field).as_deref() != Some(expected.as_str())
            }
        })
    }
}

/// Resolve a selectable field for a stored object, applying per-resource
/// defaults for fields whose Go zero value is meaningful (mirrors
/// `*ToSelectableFields` in upstream Kubernetes).
fn selectable_field(gvr: &str, obj: &serde_json::Value, field: &str) -> Option<String> {
    if let Some(v) = extract_field(obj, field) {
        return Some(v);
    }
    match (gvr, field) {
        ("/v1/nodes", "spec.unschedulable") => Some("false".into()),
        ("/v1/pods", "spec.nodeName") => Some(String::new()),
        _ => None,
    }
}

fn extract_field(obj: &serde_json::Value, path: &str) -> Option<String> {
    let mut current = obj;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    match current {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

impl LabelIndex {
    pub fn update(&self, gvr_prefix: &str, resource_key: &str, labels: &BTreeMap<String, String>) {
        let mut map = self.index.lock().expect("label index lock poisoned");
        let inner = map.entry(gvr_prefix.to_string()).or_default();

        for key_set in inner.values_mut() {
            key_set.remove(resource_key);
        }

        for (key, value) in labels {
            let label_string = format!("{key}={value}");
            inner
                .entry(label_string)
                .or_default()
                .insert(resource_key.to_string());
        }
    }

    pub fn remove(&self, gvr_prefix: &str, resource_key: &str) {
        self.update(gvr_prefix, resource_key, &BTreeMap::new());
    }

    pub fn matches(&self, gvr_prefix: &str, selector: &LabelSelector) -> Option<FxHashSet<String>> {
        let map = self.index.lock().expect("label index lock poisoned");
        let inner = map.get(gvr_prefix)?;

        let mut result: Option<FxHashSet<String>> = None;

        for requirement in &selector.requirements {
            if let Requirement::Equals(key, value) = requirement {
                let label_string = format!("{key}={value}");
                let matching = inner.get(&label_string).cloned().unwrap_or_default();
                result = Some(match result {
                    Some(existing) => existing.intersection(&matching).cloned().collect(),
                    None => matching,
                });
            }
        }

        result
    }
}
