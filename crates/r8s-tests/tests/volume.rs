use std::time::Duration;

use r8s_tests::*;
use r8s_types::*;

const TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn emptydir_volume_mounted() {
    let cluster = TestCluster::start().await;
    let pod_gvr = GroupVersionResource::pods();

    let mut pod = make_pod("emptydir-pod", "nginx:latest");
    let spec = pod.spec.as_mut().unwrap();
    spec.volumes = Some(vec![Volume {
        name: "scratch".into(),
        empty_dir: Some(EmptyDirVolumeSource::default()),
        ..Default::default()
    }]);
    spec.containers[0].volume_mounts = Some(vec![VolumeMount {
        name: "scratch".into(),
        mount_path: "/data".into(),
        ..Default::default()
    }]);

    cluster.create(&pod_gvr, "default", "emptydir-pod", &pod);

    let running = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "emptydir-pod",
        |v| has_phase(v, "Running"),
        TIMEOUT,
    )
    .await;
    assert!(running, "pod with emptyDir volume should reach Running");

    cluster.shutdown().await;
}

#[tokio::test]
async fn configmap_volume_mounted() {
    let cluster = TestCluster::start().await;
    let cm_gvr = GroupVersionResource::configmaps();
    let pod_gvr = GroupVersionResource::pods();

    // Create ConfigMap
    let cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some("test-cm".into()),
            namespace: Some("default".into()),
            ..Default::default()
        },
        data: Some(std::collections::BTreeMap::from([(
            "app.conf".into(),
            "key=value".into(),
        )])),
        ..Default::default()
    };
    cluster.create(&cm_gvr, "default", "test-cm", &cm);

    // Create Pod that mounts the ConfigMap
    let mut pod = make_pod("cm-pod", "nginx:latest");
    let spec = pod.spec.as_mut().unwrap();
    spec.volumes = Some(vec![Volume {
        name: "config".into(),
        config_map: Some(ConfigMapVolumeSource {
            name: "test-cm".into(),
            ..Default::default()
        }),
        ..Default::default()
    }]);
    spec.containers[0].volume_mounts = Some(vec![VolumeMount {
        name: "config".into(),
        mount_path: "/etc/config".into(),
        read_only: Some(true),
        ..Default::default()
    }]);

    cluster.create(&pod_gvr, "default", "cm-pod", &pod);

    let running = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "cm-pod",
        |v| has_phase(v, "Running"),
        TIMEOUT,
    )
    .await;
    assert!(running, "pod with configMap volume should reach Running");

    cluster.shutdown().await;
}

#[tokio::test]
async fn secret_volume_mounted() {
    let cluster = TestCluster::start().await;
    let secret_gvr = GroupVersionResource::secrets();
    let pod_gvr = GroupVersionResource::pods();

    // Create Secret as raw JSON (data values are base64-encoded strings)
    let secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret", "namespace": "default"},
        "data": {"password": "c2VjcmV0MTIz"}
    });
    cluster.create(&secret_gvr, "default", "test-secret", &secret);

    // Create Pod that mounts the Secret
    let mut pod = make_pod("secret-pod", "nginx:latest");
    let spec = pod.spec.as_mut().unwrap();
    spec.volumes = Some(vec![Volume {
        name: "creds".into(),
        secret: Some(SecretVolumeSource {
            secret_name: Some("test-secret".into()),
            ..Default::default()
        }),
        ..Default::default()
    }]);
    spec.containers[0].volume_mounts = Some(vec![VolumeMount {
        name: "creds".into(),
        mount_path: "/etc/creds".into(),
        read_only: Some(true),
        ..Default::default()
    }]);

    cluster.create(&pod_gvr, "default", "secret-pod", &pod);

    let running = wait_for(
        &cluster.store,
        &pod_gvr,
        Some("default"),
        "secret-pod",
        |v| has_phase(v, "Running"),
        TIMEOUT,
    )
    .await;
    assert!(running, "pod with secret volume should reach Running");

    cluster.shutdown().await;
}
