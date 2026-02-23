use serde::{Deserialize, Serialize};

use crate::meta::ObjectMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pod {
    #[serde(default = "pod_api_version")]
    pub api_version: String,
    #[serde(default = "pod_kind")]
    pub kind: String,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: PodSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<PodStatus>,
}

fn pod_api_version() -> String {
    "v1".into()
}
fn pod_kind() -> String {
    "Pod".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodSpec {
    #[serde(default)]
    pub containers: Vec<Container>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<Volume>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Container {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<EnvVar>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvVar {
    pub name: String,
    #[serde(default)]
    pub value: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, rename = "hostIP", skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,
    #[serde(default, rename = "podIP", skip_serializing_if = "Option::is_none")]
    pub pod_ip: Option<String>,
    #[serde(default, rename = "podIPs", skip_serializing_if = "Option::is_none")]
    pub pod_ips: Option<Vec<PodIP>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<PodCondition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub container_statuses: Vec<ContainerStatusInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodIP {
    pub ip: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodCondition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStatusInfo {
    pub name: String,
    pub image: String,
    #[serde(default, rename = "imageID")]
    pub image_id: String,
    #[serde(default, rename = "containerID", skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub started: bool,
    #[serde(default)]
    pub restart_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ContainerState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running: Option<ContainerStateRunning>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContainerStateRunning {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty_dir: Option<EmptyDirVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_path: Option<HostPathVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapVolumeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretVolumeSource>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EmptyDirVolumeSource {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostPathVolumeSource {
    pub path: String,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapVolumeSource {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretVolumeSource {
    pub secret_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMount {
    pub name: String,
    pub mount_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_roundtrip() {
        let json = r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"nginx","namespace":"default","uid":"abc-123"},"spec":{"nodeName":"node1","containers":[{"name":"nginx","image":"nginx:latest","command":["/bin/sh"],"args":["-c","sleep 3600"],"env":[{"name":"FOO","value":"bar"}],"workingDir":"/app"}]},"status":{"phase":"Running","podIP":"10.0.0.2","hostIP":"127.0.0.1","podIPs":[{"ip":"10.0.0.2"}],"startTime":"2025-01-01T00:00:00Z","conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2025-01-01T00:00:00Z"}],"containerStatuses":[{"name":"nginx","image":"nginx:latest","imageID":"nginx:latest","containerID":"abc","ready":true,"started":true,"restartCount":0,"state":{"running":{"startedAt":"2025-01-01T00:00:00Z"}}}]}}"#;
        let pod: Pod = serde_json::from_str(json).unwrap();
        assert_eq!(pod.metadata.name.as_deref(), Some("nginx"));
        assert_eq!(pod.spec.node_name.as_deref(), Some("node1"));
        assert_eq!(pod.spec.containers.len(), 1);
        assert_eq!(pod.spec.containers[0].name, "nginx");
        assert_eq!(pod.spec.containers[0].command.as_ref().unwrap()[0], "/bin/sh");
        assert_eq!(pod.spec.containers[0].env.as_ref().unwrap()[0].name, "FOO");
        let status = pod.status.as_ref().unwrap();
        assert_eq!(status.phase.as_deref(), Some("Running"));
        assert_eq!(status.pod_ip.as_deref(), Some("10.0.0.2"));
        assert_eq!(status.container_statuses[0].name, "nginx");
        assert!(status.container_statuses[0].ready);

        // Round-trip: verify camelCase
        let out = serde_json::to_value(&pod).unwrap();
        assert_eq!(out["apiVersion"], "v1");
        assert_eq!(out["spec"]["nodeName"], "node1");
        assert_eq!(out["status"]["podIP"], "10.0.0.2");
        assert_eq!(out["status"]["hostIP"], "127.0.0.1");
        assert_eq!(out["status"]["containerStatuses"][0]["restartCount"], 0);
        assert!(out["status"]["containerStatuses"][0]["state"]["running"]["startedAt"].is_string());
    }

    #[test]
    fn pod_with_volumes_roundtrip() {
        let json = r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"app","namespace":"default","uid":"uid-1"},"spec":{"containers":[{"name":"app","image":"app:latest","volumeMounts":[{"name":"data","mountPath":"/data"},{"name":"config","mountPath":"/etc/config","readOnly":true}]}],"volumes":[{"name":"data","emptyDir":{}},{"name":"config","configMap":{"name":"my-config"}},{"name":"host","hostPath":{"path":"/var/log","type":"Directory"}},{"name":"creds","secret":{"secretName":"my-secret"}}]}}"#;
        let pod: Pod = serde_json::from_str(json).unwrap();
        assert_eq!(pod.spec.volumes.len(), 4);
        assert_eq!(pod.spec.volumes[0].name, "data");
        assert!(pod.spec.volumes[0].empty_dir.is_some());
        assert_eq!(pod.spec.volumes[1].config_map.as_ref().unwrap().name, "my-config");
        assert_eq!(pod.spec.volumes[2].host_path.as_ref().unwrap().path, "/var/log");
        assert_eq!(pod.spec.volumes[3].secret.as_ref().unwrap().secret_name, "my-secret");

        let vms = pod.spec.containers[0].volume_mounts.as_ref().unwrap();
        assert_eq!(vms.len(), 2);
        assert_eq!(vms[0].name, "data");
        assert_eq!(vms[0].mount_path, "/data");
        assert_eq!(vms[1].read_only, Some(true));

        // Round-trip: verify camelCase
        let out = serde_json::to_value(&pod).unwrap();
        assert_eq!(out["spec"]["volumes"][0]["emptyDir"], serde_json::json!({}));
        assert_eq!(out["spec"]["volumes"][1]["configMap"]["name"], "my-config");
        assert_eq!(out["spec"]["volumes"][2]["hostPath"]["path"], "/var/log");
        assert_eq!(out["spec"]["volumes"][2]["hostPath"]["type"], "Directory");
        assert_eq!(out["spec"]["volumes"][3]["secret"]["secretName"], "my-secret");
        assert_eq!(out["spec"]["containers"][0]["volumeMounts"][0]["mountPath"], "/data");
        assert_eq!(out["spec"]["containers"][0]["volumeMounts"][1]["readOnly"], true);
    }
}
