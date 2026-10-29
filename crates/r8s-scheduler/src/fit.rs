//! Whether a pod fits the node, as the default scheduler's filter plugins
//! decide it: NodeName, NodeUnschedulable, TaintToleration, NodeAffinity
//! (nodeSelector and required affinity), NodeResourcesFit; and which
//! lower-priority pods preemption would evict to make room.

use r8s_types::quantity::parse_quantity;
use rustc_hash::FxHashMap;
use serde_json::Value;

/// Why a pod does not fit, in the scheduler's FailedScheduling wording.
#[derive(Debug, Clone, PartialEq)]
pub enum Misfit {
    /// A filter preemption cannot fix: selectors, affinity, taints.
    Node(String),
    /// Insufficient resources, by resource name.
    Resources(Vec<String>),
}

impl Misfit {
    pub fn message(&self) -> String {
        let reason = match self {
            Misfit::Node(r) => r.clone(),
            Misfit::Resources(names) => names
                .iter()
                .map(|n| format!("Insufficient {n}"))
                .collect::<Vec<_>>()
                .join(", "),
        };
        format!("0/1 nodes are available: 1 {reason}.")
    }
}

/// Every filter but resources.
pub fn node_fits(pod: &Value, node: &Value) -> Result<(), Misfit> {
    let spec = &pod["spec"];
    let node_name = node["metadata"]["name"].as_str().unwrap_or("");
    if let Some(wanted) = spec["nodeName"].as_str()
        && !wanted.is_empty()
        && wanted != node_name
    {
        return Err(Misfit::Node(
            "node(s) didn't match the requested node name".into(),
        ));
    }
    let tolerations = spec["tolerations"].as_array().cloned().unwrap_or_default();
    if node["spec"]["unschedulable"].as_bool() == Some(true) {
        let unschedulable_taint = serde_json::json!({
            "key": "node.kubernetes.io/unschedulable", "effect": "NoSchedule"
        });
        if !tolerations
            .iter()
            .any(|t| tolerates(t, &unschedulable_taint))
        {
            return Err(Misfit::Node("node(s) were unschedulable".into()));
        }
    }
    for taint in node["spec"]["taints"].as_array().into_iter().flatten() {
        let effect = taint["effect"].as_str().unwrap_or("");
        if !matches!(effect, "NoSchedule" | "NoExecute") {
            continue;
        }
        if !tolerations.iter().any(|t| tolerates(t, taint)) {
            let key = taint["key"].as_str().unwrap_or("");
            let value = taint["value"].as_str().unwrap_or("");
            let shown = if value.is_empty() {
                key.to_string()
            } else {
                format!("{key}: {value}")
            };
            return Err(Misfit::Node(format!(
                "node(s) had untolerated taint {{{shown}}}"
            )));
        }
    }
    let labels = &node["metadata"]["labels"];
    let selector_ok = spec["nodeSelector"]
        .as_object()
        .into_iter()
        .flatten()
        .all(|(k, v)| labels.get(k) == Some(v));
    let affinity_ok = match spec
        .pointer("/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution")
    {
        None | Some(Value::Null) => true,
        Some(required) => required["nodeSelectorTerms"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|term| term_matches(term, node)),
    };
    if !selector_ok || !affinity_ok {
        return Err(Misfit::Node(
            "node(s) didn't match Pod's node affinity/selector".into(),
        ));
    }
    Ok(())
}

/// A `nodeSelectorTerm`: all its expressions and fields hold; an empty term
/// matches nothing.
fn term_matches(term: &Value, node: &Value) -> bool {
    let expressions = term["matchExpressions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let fields = term["matchFields"].as_array().cloned().unwrap_or_default();
    if expressions.is_empty() && fields.is_empty() {
        return false;
    }
    let labels = &node["metadata"]["labels"];
    expressions
        .iter()
        .all(|req| requirement_holds(req, labels[req["key"].as_str().unwrap_or("")].as_str()))
        && fields.iter().all(|req| {
            let value = match req["key"].as_str() {
                Some("metadata.name") => node["metadata"]["name"].as_str(),
                _ => None,
            };
            requirement_holds(req, value)
        })
}

fn requirement_holds(req: &Value, value: Option<&str>) -> bool {
    let values: Vec<&str> = req["values"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .collect();
    let number = |s: &str| s.parse::<i64>().ok();
    match req["operator"].as_str().unwrap_or("") {
        "In" => value.is_some_and(|v| values.contains(&v)),
        "NotIn" => value.is_none_or(|v| !values.contains(&v)),
        "Exists" => value.is_some(),
        "DoesNotExist" => value.is_none(),
        "Gt" => {
            matches!((value.and_then(number), values.first().and_then(|v| number(v))), (Some(a), Some(b)) if a > b)
        }
        "Lt" => {
            matches!((value.and_then(number), values.first().and_then(|v| number(v))), (Some(a), Some(b)) if a < b)
        }
        _ => false,
    }
}

/// Whether `toleration` tolerates `taint`.
pub fn tolerates(toleration: &Value, taint: &Value) -> bool {
    let s = |v: &Value, k: &str| v[k].as_str().unwrap_or("").to_string();
    let effect = s(toleration, "effect");
    if !effect.is_empty() && effect != s(taint, "effect") {
        return false;
    }
    let exists = s(toleration, "operator") == "Exists";
    let key = s(toleration, "key");
    if key.is_empty() {
        return exists;
    }
    key == s(taint, "key") && (exists || s(toleration, "value") == s(taint, "value"))
}

/// A pod's requests per resource: the containers' sum or the largest init
/// container's, whichever is more, plus overhead; and one `pods` slot.
pub fn pod_requests(pod: &Value) -> FxHashMap<String, f64> {
    let requests_of = |c: &Value| -> FxHashMap<String, f64> {
        c.pointer("/resources/requests")
            .and_then(|r| r.as_object())
            .into_iter()
            .flatten()
            .filter_map(|(k, v)| Some((k.clone(), quantity(v)?)))
            .collect()
    };
    let mut total: FxHashMap<String, f64> = FxHashMap::default();
    for c in pod["spec"]["containers"].as_array().into_iter().flatten() {
        for (k, v) in requests_of(c) {
            *total.entry(k).or_default() += v;
        }
    }
    for c in pod["spec"]["initContainers"]
        .as_array()
        .into_iter()
        .flatten()
    {
        for (k, v) in requests_of(c) {
            let entry = total.entry(k).or_default();
            *entry = entry.max(v);
        }
    }
    for (k, v) in pod["spec"]["overhead"].as_object().into_iter().flatten() {
        if let Some(v) = quantity(v) {
            *total.entry(k.clone()).or_default() += v;
        }
    }
    *total.entry("pods".into()).or_default() += 1.0;
    total
}

fn quantity(v: &Value) -> Option<f64> {
    match v {
        Value::String(s) => parse_quantity(s),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

/// The node's allocatable amounts; a resource only in `capacity` (an
/// extended resource patched onto the node) counts at its capacity.
pub fn allocatable(node: &Value) -> FxHashMap<String, f64> {
    let mut out: FxHashMap<String, f64> = FxHashMap::default();
    for field in ["capacity", "allocatable"] {
        for (k, v) in node["status"][field].as_object().into_iter().flatten() {
            if let Some(v) = quantity(v) {
                out.insert(k.clone(), v);
            }
        }
    }
    out
}

/// Whether a pod still holds its node's resources: bound and not finished.
pub fn holds_resources(pod: &Value, node_name: &str) -> bool {
    pod["spec"]["nodeName"].as_str() == Some(node_name)
        && !matches!(
            pod["status"]["phase"].as_str(),
            Some("Succeeded") | Some("Failed")
        )
}

/// The resources `pod` lacks on a node with `free` left over.
fn lacking(request: &FxHashMap<String, f64>, free: &FxHashMap<String, f64>) -> Vec<String> {
    let mut names: Vec<String> = request
        .iter()
        .filter(|(k, v)| **v > 0.0 && free.get(*k).copied().unwrap_or(0.0) + 1e-9 < **v)
        .map(|(k, _)| k.clone())
        .collect();
    names.sort();
    names
}

/// Free amounts once `pods` take their requests from `allocatable`.
fn free_after<'a>(
    allocatable: &FxHashMap<String, f64>,
    pods: impl Iterator<Item = &'a Value>,
) -> FxHashMap<String, f64> {
    let mut free = allocatable.clone();
    for pod in pods {
        for (k, v) in pod_requests(pod) {
            *free.entry(k).or_default() -= v;
        }
    }
    free
}

/// A pod's host ports as (hostIP, protocol, port); an empty hostIP is
/// every address.
fn host_ports(pod: &Value) -> Vec<(String, String, i64)> {
    pod["spec"]["containers"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|c| c["ports"].as_array().cloned().unwrap_or_default())
        .filter_map(|p| {
            let port = p["hostPort"].as_i64().filter(|n| *n > 0)?;
            let ip = p["hostIP"].as_str().unwrap_or("");
            let ip = if ip.is_empty() { "0.0.0.0" } else { ip };
            let proto = p["protocol"].as_str().unwrap_or("TCP");
            Some((ip.to_string(), proto.to_string(), port))
        })
        .collect()
}

/// The NodePorts filter: a host port another pod on the node already binds,
/// on the same protocol and an overlapping address (0.0.0.0 overlaps all).
pub fn ports_fit(pod: &Value, others: &[&Value]) -> Result<(), Misfit> {
    let wanted = host_ports(pod);
    if wanted.is_empty() {
        return Ok(());
    }
    let taken: Vec<(String, String, i64)> = others.iter().flat_map(|p| host_ports(p)).collect();
    let clash = wanted.iter().any(|(ip, proto, port)| {
        taken.iter().any(|(tip, tproto, tport)| {
            port == tport && proto == tproto && (ip == tip || ip == "0.0.0.0" || tip == "0.0.0.0")
        })
    });
    if clash {
        Err(Misfit::Node(
            "node(s) didn't have free ports for the requested pod ports".into(),
        ))
    } else {
        Ok(())
    }
}

/// Whether `pod` fits next to `others` (the pods holding the node).
pub fn resources_fit(pod: &Value, node: &Value, others: &[&Value]) -> Result<(), Misfit> {
    let free = free_after(&allocatable(node), others.iter().copied());
    let missing = lacking(&pod_requests(pod), &free);
    if missing.is_empty() {
        Ok(())
    } else {
        Err(Misfit::Resources(missing))
    }
}

pub fn priority(pod: &Value) -> i64 {
    pod["spec"]["priority"].as_i64().unwrap_or(0)
}

/// The lower-priority pods whose eviction lets `pod` fit, fewest and
/// lowest-priority first as upstream's victim selection with reprieve:
/// every lower-priority pod is removed, then the highest-priority ones are
/// added back while `pod` still fits. `None` when even removing them all
/// does not make room, or the pod may not preempt.
pub fn victims<'a>(pod: &Value, node: &Value, others: &[&'a Value]) -> Option<Vec<&'a Value>> {
    if pod["spec"]["preemptionPolicy"].as_str() == Some("Never") {
        return None;
    }
    let mine = priority(pod);
    let (mut candidates, kept): (Vec<&Value>, Vec<&Value>) = others
        .iter()
        .partition(|p| priority(p) < mine && p["metadata"]["deletionTimestamp"].is_null());
    if candidates.is_empty() {
        return None;
    }
    let request = pod_requests(pod);
    let allocatable = allocatable(node);
    if !lacking(&request, &free_after(&allocatable, kept.iter().copied())).is_empty() {
        return None;
    }
    // Reprieve the most important first.
    candidates.sort_by_key(|p| std::cmp::Reverse(priority(p)));
    let mut staying = kept;
    let mut evicted = Vec::new();
    for candidate in candidates {
        staying.push(candidate);
        if !lacking(&request, &free_after(&allocatable, staying.iter().copied())).is_empty() {
            staying.pop();
            evicted.push(candidate);
        }
    }
    Some(evicted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn node() -> Value {
        json!({
            "metadata": {"name": "n1", "labels": {"kubernetes.io/hostname": "n1", "disk": "ssd"}},
            "spec": {},
            "status": {"allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"},
                       "capacity": {"cpu": "4", "memory": "8Gi", "pods": "110", "example.com/foo": "5"}}
        })
    }

    fn pod(extra: Value) -> Value {
        let mut p = json!({"metadata": {"name": "p"}, "spec": {"containers": [{"name": "c"}]}});
        for (k, v) in extra.as_object().unwrap() {
            p["spec"][k] = v.clone();
        }
        p
    }

    fn requesting(name: &str, prio: i64, resource: &str, amount: &str) -> Value {
        json!({"metadata": {"name": name}, "spec": {"nodeName": "n1", "priority": prio,
               "containers": [{"name": "c", "resources": {"requests": {resource: amount}}}]},
               "status": {"phase": "Running"}})
    }

    #[test]
    fn selectors_affinity_and_taints() {
        let n = node();
        assert!(node_fits(&pod(json!({"nodeSelector": {"disk": "ssd"}})), &n).is_ok());
        assert!(matches!(
            node_fits(&pod(json!({"nodeSelector": {"disk": "hdd"}})), &n),
            Err(Misfit::Node(_))
        ));
        let affinity = |op: &str, values: Value| {
            pod(
                json!({"affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution":
                {"nodeSelectorTerms": [{"matchExpressions": [{"key": "disk", "operator": op, "values": values}]}]}}}}),
            )
        };
        assert!(node_fits(&affinity("In", json!(["ssd", "nvme"])), &n).is_ok());
        assert!(node_fits(&affinity("NotIn", json!(["ssd"])), &n).is_err());
        assert!(node_fits(&affinity("Exists", json!([])), &n).is_ok());
        let by_name = pod(
            json!({"affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution":
            {"nodeSelectorTerms": [{"matchFields": [{"key": "metadata.name", "operator": "In", "values": ["n1"]}]}]}}}}),
        );
        assert!(node_fits(&by_name, &n).is_ok());

        let mut tainted = node();
        tainted["spec"]["taints"] =
            json!([{"key": "dedicated", "value": "x", "effect": "NoSchedule"}]);
        assert!(node_fits(&pod(json!({})), &tainted).is_err());
        let tolerant = pod(
            json!({"tolerations": [{"key": "dedicated", "operator": "Equal", "value": "x", "effect": "NoSchedule"}]}),
        );
        assert!(node_fits(&tolerant, &tainted).is_ok());
        tainted["spec"]["taints"] = json!([{"key": "soft", "effect": "PreferNoSchedule"}]);
        assert!(node_fits(&pod(json!({})), &tainted).is_ok());
    }

    #[test]
    fn resources_count_bound_pods_and_extended_capacity() {
        let n = node();
        let filler = requesting("f", 0, "cpu", "3");
        let others = vec![&filler];
        let small = requesting("s", 0, "cpu", "500m");
        assert!(resources_fit(&small, &n, &others).is_ok());
        let big = requesting("b", 0, "cpu", "2");
        assert_eq!(
            resources_fit(&big, &n, &others),
            Err(Misfit::Resources(vec!["cpu".into()]))
        );
        let foo = requesting("x", 0, "example.com/foo", "6");
        assert!(resources_fit(&foo, &n, &[]).is_err());
        let foo = requesting("x", 0, "example.com/foo", "5");
        assert!(resources_fit(&foo, &n, &[]).is_ok());
    }

    #[test]
    fn host_ports_clash_on_overlapping_addresses_only() {
        let with_port = |ip: &str, proto: &str| {
            json!({"spec": {"containers": [{"name": "c", "ports": [
                {"containerPort": 8080, "hostPort": 54323, "hostIP": ip, "protocol": proto}]}]}})
        };
        let loopback = with_port("127.0.0.1", "TCP");
        let others = vec![&loopback];
        assert!(ports_fit(&with_port("127.0.0.1", "TCP"), &others).is_err());
        assert!(ports_fit(&with_port("10.244.0.1", "TCP"), &others).is_ok());
        assert!(ports_fit(&with_port("127.0.0.1", "UDP"), &others).is_ok());
        assert!(
            ports_fit(&with_port("", "TCP"), &others).is_err(),
            "0.0.0.0 overlaps every address"
        );
        assert!(ports_fit(&pod(json!({})), &others).is_ok());
    }

    #[test]
    fn preemption_evicts_the_fewest_lowest_priority_pods() {
        let n = node();
        let low = requesting("low", 1, "example.com/foo", "2");
        let medium = requesting("medium", 100, "example.com/foo", "2");
        let others = vec![&low, &medium];
        let high = requesting("high", 1000, "example.com/foo", "2");
        let chosen = victims(&high, &n, &others).unwrap();
        let names: Vec<&str> = chosen
            .iter()
            .map(|p| p["metadata"]["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["low"]);

        let peer = requesting("peer", 1, "example.com/foo", "2");
        assert!(
            victims(&peer, &n, &others).is_none(),
            "no lower-priority pod to evict"
        );
        let mut never = high.clone();
        never["spec"]["preemptionPolicy"] = json!("Never");
        assert!(victims(&never, &n, &others).is_none());
    }
}
