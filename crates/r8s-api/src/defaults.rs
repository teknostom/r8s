//! Pod and pod-template defaults the apiserver fills in on write
//! (upstream `SetDefaults_PodSpec`, `SetDefaults_Container` and friends).

use serde_json::{Value, json};

/// Default a pod, or the pod template inside a workload, in place.
pub fn apply(group: &str, resource: &str, body: &mut Value) {
    match (group, resource) {
        ("", "pods") => default_pod_spec(&mut body["spec"]),
        ("", "replicationcontrollers")
        | ("apps", "deployments" | "replicasets" | "daemonsets" | "statefulsets")
        | ("batch", "jobs") => {
            if body["spec"]["template"].is_object() {
                default_pod_spec(&mut body["spec"]["template"]["spec"]);
            }
        }
        ("batch", "cronjobs") if body["spec"]["jobTemplate"]["spec"]["template"].is_object() => {
            default_pod_spec(&mut body["spec"]["jobTemplate"]["spec"]["template"]["spec"]);
        }
        ("", "limitranges") => default_limit_range(body),
        _ => {}
    }
}

/// A limit's `defaultRequest` takes its `default` for the resources it
/// leaves out.
fn default_limit_range(body: &mut Value) {
    for limit in array_mut(&mut body["spec"], "limits") {
        let Some(defaults) = limit.get("default").and_then(|d| d.as_object()).cloned() else {
            continue;
        };
        let Some(limit) = limit.as_object_mut() else {
            continue;
        };
        let requests = limit.entry("defaultRequest").or_insert_with(|| json!({}));
        if !requests.is_object() {
            *requests = json!({});
        }
        if let Some(requests) = requests.as_object_mut() {
            for (key, value) in defaults {
                requests.entry(key).or_insert(value);
            }
        }
    }
}

fn set_default(obj: &mut Value, key: &str, value: Value) {
    if let Some(map) = obj.as_object_mut()
        && !map.get(key).is_some_and(|v| !v.is_null())
    {
        map.insert(key.to_string(), value);
    }
}

/// The elements of the array at `key`, if any. Indexing a missing key with
/// `IndexMut` would insert `null`, which clients and later admission steps
/// take for an explicit value.
fn array_mut<'a>(obj: &'a mut Value, key: &str) -> impl Iterator<Item = &'a mut Value> {
    obj.get_mut(key)
        .and_then(Value::as_array_mut)
        .into_iter()
        .flatten()
}

pub fn default_pod_spec(spec: &mut Value) {
    if !spec.is_object() {
        return;
    }
    set_default(spec, "restartPolicy", json!("Always"));
    set_default(spec, "dnsPolicy", json!("ClusterFirst"));
    set_default(spec, "terminationGracePeriodSeconds", json!(30));
    set_default(spec, "schedulerName", json!("default-scheduler"));
    set_default(spec, "securityContext", json!({}));
    set_default(spec, "enableServiceLinks", json!(true));
    for list in ["initContainers", "containers"] {
        for container in array_mut(spec, list) {
            default_container(container);
        }
    }
    for volume in array_mut(spec, "volumes") {
        for source in ["secret", "configMap", "downwardAPI"] {
            if volume[source].is_object() {
                set_default(&mut volume[source], "defaultMode", json!(420));
            }
        }
        if volume["projected"].is_object() {
            set_default(&mut volume["projected"], "defaultMode", json!(420));
        }
        if volume["hostPath"].is_object() {
            set_default(&mut volume["hostPath"], "type", json!(""));
        }
    }
}

fn default_container(container: &mut Value) {
    if !container.is_object() {
        return;
    }
    let image = container["image"].as_str().unwrap_or("");
    let pull_policy = if image_tag_is_latest(image) {
        "Always"
    } else {
        "IfNotPresent"
    };
    set_default(container, "imagePullPolicy", json!(pull_policy));
    set_default(
        container,
        "terminationMessagePath",
        json!("/dev/termination-log"),
    );
    set_default(container, "terminationMessagePolicy", json!("File"));
    set_default(container, "resources", json!({}));
    // A limit without a request is its own request.
    if let Some(limits) = container
        .get("resources")
        .and_then(|r| r.get("limits"))
        .and_then(|l| l.as_object())
        .cloned()
        && let Some(resources) = container
            .get_mut("resources")
            .and_then(|r| r.as_object_mut())
    {
        let requests = resources.entry("requests").or_insert_with(|| json!({}));
        if !requests.is_object() {
            *requests = json!({});
        }
        if let Some(requests) = requests.as_object_mut() {
            for (key, value) in limits {
                requests.entry(key).or_insert(value);
            }
        }
    }
    for port in array_mut(container, "ports") {
        set_default(port, "protocol", json!("TCP"));
    }
    for env in array_mut(container, "env") {
        if env["valueFrom"]["fieldRef"].is_object() {
            set_default(&mut env["valueFrom"]["fieldRef"], "apiVersion", json!("v1"));
        }
    }
    for probe in ["livenessProbe", "readinessProbe", "startupProbe"] {
        if container[probe].is_object() {
            default_probe(&mut container[probe]);
        }
    }
    for hook in ["postStart", "preStop"] {
        if container["lifecycle"][hook]["httpGet"].is_object() {
            set_default(
                &mut container["lifecycle"][hook]["httpGet"],
                "scheme",
                json!("HTTP"),
            );
        }
    }
}

/// `:latest` or no tag pulls Always; a digest or other tag IfNotPresent.
fn image_tag_is_latest(image: &str) -> bool {
    if image.is_empty() || image.contains('@') {
        return false;
    }
    let last_segment = image.rsplit('/').next().unwrap_or(image);
    match last_segment.rsplit_once(':') {
        Some((_, tag)) => tag == "latest",
        None => true,
    }
}

/// Zero is unset for these, as for Go's non-pointer ints.
fn default_probe(probe: &mut Value) {
    for (key, default) in [
        ("timeoutSeconds", 1),
        ("periodSeconds", 10),
        ("successThreshold", 1),
        ("failureThreshold", 3),
    ] {
        if probe[key].as_i64() == Some(0) {
            probe[key] = json!(default);
        }
        set_default(probe, key, json!(default));
    }
    if probe["httpGet"].is_object() {
        set_default(&mut probe["httpGet"], "scheme", json!("HTTP"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_lists_stay_absent() {
        let mut pod = json!({"spec": {"containers": [{"name": "a", "image": "nginx"}]}});
        apply("", "pods", &mut pod);
        let spec = pod["spec"].as_object().unwrap();
        assert!(!spec.contains_key("volumes"));
        assert!(!spec.contains_key("initContainers"));
        let container = spec["containers"][0].as_object().unwrap();
        assert!(!container.contains_key("ports"));
        assert!(!container.contains_key("env"));
    }

    #[test]
    fn zero_probe_fields_take_defaults() {
        let mut pod = json!({"spec": {"containers": [{"name": "a", "image": "nginx",
            "livenessProbe": {"tcpSocket": {"port": 8080}, "timeoutSeconds": 0,
                              "periodSeconds": 0, "successThreshold": 0,
                              "failureThreshold": 0, "initialDelaySeconds": 0}}]}});
        apply("", "pods", &mut pod);
        let probe = &pod["spec"]["containers"][0]["livenessProbe"];
        assert_eq!(probe["timeoutSeconds"], 1);
        assert_eq!(probe["periodSeconds"], 10);
        assert_eq!(probe["successThreshold"], 1);
        assert_eq!(probe["failureThreshold"], 3);
        assert_eq!(probe["initialDelaySeconds"], 0);
    }

    #[test]
    fn pod_gets_upstream_defaults() {
        let mut pod = json!({"spec": {"containers": [
            {"name": "a", "image": "nginx"},
            {"name": "b", "image": "nginx:1.25", "imagePullPolicy": "Never",
             "ports": [{"containerPort": 80}],
             "readinessProbe": {"httpGet": {"path": "/", "port": 80}}}
        ], "volumes": [{"name": "s", "secret": {"secretName": "x"}}, {"name": "e", "emptyDir": {}}]}});
        apply("", "pods", &mut pod);
        let spec = &pod["spec"];
        assert_eq!(spec["restartPolicy"], "Always");
        assert_eq!(spec["dnsPolicy"], "ClusterFirst");
        assert_eq!(spec["terminationGracePeriodSeconds"], 30);
        assert_eq!(spec["schedulerName"], "default-scheduler");
        assert_eq!(spec["securityContext"], json!({}));
        assert_eq!(spec["containers"][0]["imagePullPolicy"], "Always");
        assert_eq!(spec["containers"][0]["terminationMessagePolicy"], "File");
        assert_eq!(
            spec["containers"][0]["terminationMessagePath"],
            "/dev/termination-log"
        );
        assert_eq!(spec["containers"][1]["imagePullPolicy"], "Never");
        assert_eq!(spec["containers"][1]["ports"][0]["protocol"], "TCP");
        assert_eq!(spec["containers"][1]["readinessProbe"]["periodSeconds"], 10);
        assert_eq!(
            spec["containers"][1]["readinessProbe"]["httpGet"]["scheme"],
            "HTTP"
        );
        assert_eq!(spec["volumes"][0]["secret"]["defaultMode"], 420);
        assert_eq!(spec["volumes"][1]["emptyDir"], json!({}));
    }

    #[test]
    fn workload_templates_are_defaulted() {
        let mut deploy = json!({"spec": {"template": {"spec": {"containers": [{"name": "c", "image": "img:1"}]}}}});
        apply("apps", "deployments", &mut deploy);
        assert_eq!(
            deploy["spec"]["template"]["spec"]["restartPolicy"],
            "Always"
        );
        assert_eq!(
            deploy["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"],
            "IfNotPresent"
        );
        let mut cj = json!({"spec": {"jobTemplate": {"spec": {"template": {"spec": {"restartPolicy": "Never", "containers": []}}}}}});
        apply("batch", "cronjobs", &mut cj);
        assert_eq!(
            cj["spec"]["jobTemplate"]["spec"]["template"]["spec"]["restartPolicy"],
            "Never"
        );
        assert_eq!(
            cj["spec"]["jobTemplate"]["spec"]["template"]["spec"]["dnsPolicy"],
            "ClusterFirst"
        );
        let mut cm = json!({"data": {"a": "b"}});
        apply("", "configmaps", &mut cm);
        assert_eq!(cm, json!({"data": {"a": "b"}}));
    }

    #[test]
    fn latest_detection() {
        assert!(image_tag_is_latest("nginx"));
        assert!(image_tag_is_latest("registry:5000/nginx:latest"));
        assert!(!image_tag_is_latest("registry:5000/nginx:1.2"));
        assert!(!image_tag_is_latest("nginx@sha256:abcd"));
    }
}
