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
    /// Check if an object's labels satisfy all requirements.
    pub fn matches(&self, object: &serde_json::Value) -> bool {
        let labels = object["metadata"]["labels"].as_object();
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

    pub fn matches(&self, object: &serde_json::Value) -> bool {
        self.requirements.iter().all(|req| match req {
            FieldRequirement::Equals(field, expected) => {
                extract_field(object, field).as_deref() == Some(expected.as_str())
            }
            FieldRequirement::NotEquals(field, expected) => {
                extract_field(object, field).as_deref() != Some(expected.as_str())
            }
        })
    }
}

fn extract_field(obj: &serde_json::Value, path: &str) -> Option<String> {
    let mut current = obj;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    current.as_str().map(|s| s.to_string())
}

impl LabelIndex {
    pub fn new() -> Self {
        Self {
            index: Arc::new(Mutex::new(FxHashMap::default())),
        }
    }

    pub fn update(
        &self,
        gvr_prefix: &str,
        resource_key: &str,
        labels: &BTreeMap<String, String>,
    ) {
        let mut map = self.index.lock().expect("Failed to lock index. FATAL");
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
        let map = self.index.lock().expect("Failed to lock index. FATAL");
        let inner = map.get(gvr_prefix)?;

        let mut result: Option<FxHashSet<String>> = None;

        for requirement in &selector.requirements {
            if let Requirement::Equals(key, value) = requirement {
                let label_string = format!("{key}={value}");
                let matching = inner.get(&label_string).cloned().unwrap_or_default();
                result = Some(match result {
                    Some(exisiting) => exisiting.intersection(&matching).cloned().collect(),
                    None => matching,
                });
            }
        }

        result
    }
}
