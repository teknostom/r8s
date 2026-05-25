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
    /// `key in (v1, v2, ...)` — label value must equal one of the listed values.
    In(String, Vec<String>),
    /// `key notin (v1, v2, ...)` — label value must NOT equal any of the listed values.
    /// Note: an absent label vacuously satisfies `notin`, matching k8s semantics.
    NotIn(String, Vec<String>),
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
        let lookup = |key: &str| {
            labels
                .and_then(|l| l.get(key))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        };
        self.requirements.iter().all(|req| match req {
            Requirement::Equals(key, value) => lookup(key).as_deref() == Some(value.as_str()),
            Requirement::NotEquals(key, value) => lookup(key).as_deref() != Some(value.as_str()),
            Requirement::Exists(key) => labels.is_some_and(|l| l.contains_key(key)),
            Requirement::NotExists(key) => !labels.is_some_and(|l| l.contains_key(key)),
            Requirement::In(key, values) => lookup(key).is_some_and(|v| values.iter().any(|x| x == &v)),
            Requirement::NotIn(key, values) => {
                lookup(key).is_none_or(|v| !values.iter().any(|x| x == &v))
            }
        })
    }

    /// Parse a Kubernetes label selector. Supports the full set of operators
    /// upstream's `k8s.io/apimachinery/pkg/labels` accepts:
    ///
    ///   `key=value`         equality
    ///   `key!=value`        inequality
    ///   `key in (v1,v2)`    membership
    ///   `key notin (v1,v2)` non-membership
    ///   `key`               exists
    ///   `!key`              not-exists
    ///
    /// Top-level requirements are comma-separated, but commas inside the
    /// `in`/`notin` value list are not separators — the splitter respects
    /// parenthesis nesting accordingly.
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        if s.trim().is_empty() {
            return Ok(Self {
                requirements: vec![],
            });
        }

        let parts = split_top_level_commas(s);
        let mut requirements = Vec::with_capacity(parts.len());
        for raw in parts {
            let part = raw.trim();
            if part.is_empty() {
                continue;
            }
            requirements.push(parse_requirement(part)?);
        }
        Ok(Self { requirements })
    }
}

fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut depth = 0i32;
    for ch in s.chars() {
        match ch {
            '(' => {
                depth += 1;
                buf.push(ch);
            }
            ')' => {
                depth -= 1;
                buf.push(ch);
            }
            ',' if depth == 0 => {
                out.push(std::mem::take(&mut buf));
            }
            _ => buf.push(ch),
        }
    }
    if !buf.is_empty() {
        out.push(buf);
    }
    out
}

fn parse_requirement(part: &str) -> anyhow::Result<Requirement> {
    // Try `in` / `notin` first — they're delimited by whitespace, unlike `=` /
    // `!=` which can appear inside a value list (e.g. `key in (a=b)`).
    if let Some(rest) = part.strip_prefix("!") {
        return Ok(Requirement::NotExists(rest.trim().to_string()));
    }
    // Look for ` notin (` and ` in (` as the operator (whitespace-bounded so
    // we don't match a literal "in" inside a label key).
    if let Some((key, values)) = split_set_op(part, "notin") {
        return Ok(Requirement::NotIn(key, values));
    }
    if let Some((key, values)) = split_set_op(part, "in") {
        return Ok(Requirement::In(key, values));
    }
    if let Some((key, value)) = part.split_once("!=") {
        return Ok(Requirement::NotEquals(
            key.trim().to_string(),
            value.trim().to_string(),
        ));
    }
    if let Some((key, value)) = part.split_once("==") {
        return Ok(Requirement::Equals(
            key.trim().to_string(),
            value.trim().to_string(),
        ));
    }
    if let Some((key, value)) = part.split_once('=') {
        return Ok(Requirement::Equals(
            key.trim().to_string(),
            value.trim().to_string(),
        ));
    }
    Ok(Requirement::Exists(part.trim().to_string()))
}

fn split_set_op(part: &str, op: &str) -> Option<(String, Vec<String>)> {
    // Match `<key> <op> (<v1>, <v2>, ...)`. Operator must be surrounded by
    // whitespace; values are inside the parens, comma-separated.
    let mid_pattern = format!(" {op} ");
    let idx = part.find(&mid_pattern)?;
    let key = part[..idx].trim().to_string();
    let after = part[idx + mid_pattern.len()..].trim();
    let inner = after.strip_prefix('(')?.strip_suffix(')')?;
    let values: Vec<String> = inner
        .split(',')
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect();
    Some((key, values))
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
            // Tolerate empty terms, e.g. the leading comma in
            // `,type!=helm.sh/release.v1` that client-go emits when it joins an
            // empty selector with a real one (ingress-nginx's Secret informer
            // does exactly this). Upstream ignores empty terms rather than 400.
            if part.is_empty() {
                continue;
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_selector_tolerates_leading_comma() {
        // ingress-nginx's Secret informer sends `,type!=helm.sh/release.v1`
        // (client-go joins an empty selector with a real one). Must parse.
        let sel = FieldSelector::parse(",type!=helm.sh/release.v1").expect("should parse");
        let helm = serde_json::json!({ "type": "helm.sh/release.v1" });
        let tls = serde_json::json!({ "type": "kubernetes.io/tls" });
        // Helm release secret excluded; TLS secret kept.
        assert!(!sel.matches(&helm, "/v1/secrets"));
        assert!(sel.matches(&tls, "/v1/secrets"));
    }

    #[test]
    fn field_selector_equals_matches() {
        let sel = FieldSelector::parse("metadata.name=foo").expect("should parse");
        assert!(sel.matches(&serde_json::json!({ "metadata": { "name": "foo" } }), "/v1/pods"));
        assert!(!sel.matches(&serde_json::json!({ "metadata": { "name": "bar" } }), "/v1/pods"));
    }

    #[test]
    fn field_selector_rejects_term_without_operator() {
        assert!(FieldSelector::parse("garbage").is_err());
    }

    #[test]
    fn field_selector_empty_is_ok() {
        assert!(FieldSelector::parse("").expect("empty ok").matches(
            &serde_json::json!({ "type": "anything" }),
            "/v1/secrets"
        ));
    }
}
