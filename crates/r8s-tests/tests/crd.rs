use std::time::Duration;

use r8s_store::backend::ResourceRef;
use r8s_tests::TestCluster;
use r8s_types::*;

#[tokio::test]
async fn crd_registration() {
    let cluster = TestCluster::start().await;
    let crd_gvr = GroupVersionResource::crds();

    let crd = CustomResourceDefinition {
        metadata: ObjectMeta {
            name: Some("foos.example.com".into()),
            ..Default::default()
        },
        spec: CustomResourceDefinitionSpec {
            group: "example.com".into(),
            names: CustomResourceDefinitionNames {
                plural: "foos".into(),
                singular: Some("foo".into()),
                kind: "Foo".into(),
                short_names: Some(vec!["fo".into()]),
                ..Default::default()
            },
            scope: "Namespaced".into(),
            versions: vec![CustomResourceDefinitionVersion {
                name: "v1".into(),
                served: true,
                storage: true,
                ..Default::default()
            }],
            ..Default::default()
        },
        status: None,
    };

    let rref = ResourceRef {
        gvr: &crd_gvr,
        namespace: None,
        name: "foos.example.com",
    };
    cluster
        .store
        .create(rref, &serde_json::to_value(&crd).unwrap())
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let foo_gvr = GroupVersionResource::new("example.com", "v1", "foos");
    let registered = cluster.registry.get_by_gvr(&foo_gvr);
    assert!(
        registered.is_some(),
        "CRD should register resource type in registry"
    );
    assert_eq!(registered.unwrap().kind, "Foo");

    cluster.shutdown().await;
}

#[tokio::test]
async fn crd_unregistration() {
    let cluster = TestCluster::start().await;
    let crd_gvr = GroupVersionResource::crds();

    let crd = CustomResourceDefinition {
        metadata: ObjectMeta {
            name: Some("bars.example.com".into()),
            ..Default::default()
        },
        spec: CustomResourceDefinitionSpec {
            group: "example.com".into(),
            names: CustomResourceDefinitionNames {
                plural: "bars".into(),
                singular: Some("bar".into()),
                kind: "Bar".into(),
                short_names: None,
                ..Default::default()
            },
            scope: "Namespaced".into(),
            versions: vec![CustomResourceDefinitionVersion {
                name: "v1alpha1".into(),
                served: true,
                storage: true,
                ..Default::default()
            }],
            ..Default::default()
        },
        status: None,
    };

    let rref = ResourceRef {
        gvr: &crd_gvr,
        namespace: None,
        name: "bars.example.com",
    };
    cluster
        .store
        .create(rref, &serde_json::to_value(&crd).unwrap())
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    let bar_gvr = GroupVersionResource::new("example.com", "v1alpha1", "bars");
    assert!(
        cluster.registry.get_by_gvr(&bar_gvr).is_some(),
        "CRD should be registered"
    );

    // Delete the CRD
    let del_ref = ResourceRef {
        gvr: &crd_gvr,
        namespace: None,
        name: "bars.example.com",
    };
    cluster.store.delete(&del_ref).unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        cluster.registry.get_by_gvr(&bar_gvr).is_none(),
        "CRD should be unregistered after deletion"
    );

    cluster.shutdown().await;
}
