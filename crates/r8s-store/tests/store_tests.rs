use r8s_store::backend::{ResourceRef, Store};
use r8s_store::error::StoreError;
use r8s_store::index::{FieldSelector, LabelSelector};
use r8s_store::revision::RevisionCounter;
use r8s_store::watch::WatchEventType;
use r8s_types::GroupVersionResource;
use tempfile::TempDir;

fn test_gvr() -> GroupVersionResource {
    GroupVersionResource::new("", "v1", "pods")
}

fn test_object(name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "containers": [{"name": "main", "image": "nginx"}]
        }
    })
}

fn test_object_with_labels(
    name: &str,
    namespace: &str,
    labels: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels,
        },
        "spec": {
            "containers": [{"name": "main", "image": "nginx"}]
        }
    })
}

fn open_temp_store(dir: &TempDir) -> Store {
    Store::open(&dir.path().join("test.db")).expect("failed to open store")
}

// ── RevisionCounter ──────────────────────────────────────────────────

#[test]
fn revision_counter_starts_at_initial() {
    let counter = RevisionCounter::new(0);
    assert_eq!(counter.current(), 0);
}

#[test]
fn revision_counter_next_is_monotonic() {
    let counter = RevisionCounter::new(0);
    assert_eq!(counter.next(), 1);
    assert_eq!(counter.next(), 2);
    assert_eq!(counter.next(), 3);
    assert_eq!(counter.current(), 3);
}

#[test]
fn revision_counter_starts_from_nonzero() {
    let counter = RevisionCounter::new(100);
    assert_eq!(counter.next(), 101);
    assert_eq!(counter.current(), 101);
}

// ── Store: create + get ──────────────────────────────────────────────

#[tokio::test]
async fn create_and_get() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let obj = test_object("nginx", "default");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };

    let created = store.create(rref, &obj).unwrap();

    // Metadata should be injected
    let meta = &created["metadata"];
    assert_eq!(meta["name"], "nginx");
    assert!(meta["resourceVersion"].as_str().is_some());
    assert!(meta["uid"].as_str().is_some());
    assert!(meta["creationTimestamp"].as_str().is_some());

    // resourceVersion should be "1" (first mutation)
    assert_eq!(meta["resourceVersion"].as_str().unwrap(), "1");

    // Get it back
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let fetched = store.get(&rref).unwrap().expect("should exist");
    assert_eq!(fetched["metadata"]["name"], "nginx");
    assert_eq!(fetched["metadata"]["uid"], created["metadata"]["uid"]);
}

#[tokio::test]
async fn create_preserves_preset_uid_and_creation_timestamp() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    // The API layer stamps uid/creationTimestamp before validating admission
    // webhooks run; the store must persist that identity, not mint a new one
    // (the object a webhook approved and the object stored must be the same).
    let mut obj = test_object("stamped", "default");
    obj["metadata"]["uid"] = serde_json::json!("0a1b2c3d-1111-4222-8333-444455556666");
    obj["metadata"]["creationTimestamp"] = serde_json::json!("2026-06-13T00:00:00+00:00");

    let created = store
        .create(
            ResourceRef {
                gvr: &gvr,
                namespace: Some("default"),
                name: "stamped",
            },
            &obj,
        )
        .unwrap();

    assert_eq!(
        created["metadata"]["uid"].as_str().unwrap(),
        "0a1b2c3d-1111-4222-8333-444455556666"
    );
    assert_eq!(
        created["metadata"]["creationTimestamp"].as_str().unwrap(),
        "2026-06-13T00:00:00+00:00"
    );
    // resourceVersion stays server-owned regardless.
    assert!(created["metadata"]["resourceVersion"].as_str().is_some());
}

#[tokio::test]
async fn create_duplicate_returns_already_exists() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();
    let obj = test_object("nginx", "default");

    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    store.create(rref, &obj).unwrap();

    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let err = store.create(rref, &obj).unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists { .. }));
}

#[tokio::test]
async fn get_nonexistent_returns_none() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "ghost",
    };
    assert!(store.get(&rref).unwrap().is_none());
}

// ── Store: update ────────────────────────────────────────────────────

#[tokio::test]
async fn update_existing() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let obj = test_object("nginx", "default");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let created = store.create(rref, &obj).unwrap();
    let rv1 = created["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_string();

    // Update with matching resourceVersion
    let mut updated_obj = created.clone();
    updated_obj["spec"]["containers"][0]["image"] = serde_json::json!("nginx:latest");

    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let updated = store.update(&rref, &updated_obj).unwrap();
    let rv2 = updated["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_string();

    // resourceVersion must have changed
    assert_ne!(rv1, rv2);
    // Image should be updated
    assert_eq!(updated["spec"]["containers"][0]["image"], "nginx:latest");
}

#[tokio::test]
async fn update_nonexistent_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();
    let obj = test_object("ghost", "default");

    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "ghost",
    };
    let err = store.update(&rref, &obj).unwrap_err();
    assert!(matches!(err, StoreError::NotFound { .. }));
}

#[tokio::test]
async fn update_with_stale_revision_returns_conflict() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let obj = test_object("nginx", "default");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let created = store.create(rref, &obj).unwrap();

    // First update changes the image (and so bumps resourceVersion past 1).
    let mut modified = created.clone();
    modified["spec"]["containers"][0]["image"] = serde_json::json!("nginx:1.26");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let _updated = store.update(&rref, &modified).unwrap();

    // Second update carries the original (now stale) resourceVersion — conflict.
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let err = store.update(&rref, &created).unwrap_err();
    assert!(matches!(err, StoreError::Conflict { .. }));
}

// ── Store: delete ────────────────────────────────────────────────────

#[tokio::test]
async fn delete_existing() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let obj = test_object("nginx", "default");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    store.create(rref, &obj).unwrap();

    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let deleted = store.delete(&rref).unwrap();
    assert!(deleted.is_some());
    assert_eq!(deleted.unwrap()["metadata"]["name"], "nginx");

    // Should be gone
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    assert!(store.get(&rref).unwrap().is_none());
}

#[tokio::test]
async fn delete_nonexistent_returns_none() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "ghost",
    };
    assert!(store.delete(&rref).unwrap().is_none());
}

// ── Store: list ──────────────────────────────────────────────────────

#[tokio::test]
async fn list_all_in_namespace() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    for name in ["alpha", "bravo", "charlie"] {
        let obj = test_object(name, "default");
        let rref = ResourceRef {
            gvr: &gvr,
            namespace: Some("default"),
            name,
        };
        store.create(rref, &obj).unwrap();
    }

    let result = store
        .list(&gvr, Some("default"), None, None, None, None)
        .unwrap();
    assert_eq!(result.items.len(), 3);
    assert!(result.continue_token.is_none());
}

#[tokio::test]
async fn list_filters_by_namespace() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let obj_a = test_object("pod-a", "ns-a");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("ns-a"),
        name: "pod-a",
    };
    store.create(rref, &obj_a).unwrap();

    let obj_b = test_object("pod-b", "ns-b");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("ns-b"),
        name: "pod-b",
    };
    store.create(rref, &obj_b).unwrap();

    let result = store
        .list(&gvr, Some("ns-a"), None, None, None, None)
        .unwrap();
    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0]["metadata"]["name"], "pod-a");

    // List across all namespaces
    let result = store.list(&gvr, None, None, None, None, None).unwrap();
    assert_eq!(result.items.len(), 2);
}

#[tokio::test]
async fn list_with_label_selector() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let obj1 = test_object_with_labels("web", "default", serde_json::json!({"app": "web"}));
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "web",
    };
    store.create(rref, &obj1).unwrap();

    let obj2 = test_object_with_labels("db", "default", serde_json::json!({"app": "db"}));
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "db",
    };
    store.create(rref, &obj2).unwrap();

    let selector = LabelSelector::parse("app=web").unwrap();
    let result = store
        .list(&gvr, Some("default"), Some(&selector), None, None, None)
        .unwrap();
    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0]["metadata"]["name"], "web");
}

#[tokio::test]
async fn list_with_field_selector() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    for name in ["alpha", "bravo"] {
        let obj = test_object(name, "default");
        let rref = ResourceRef {
            gvr: &gvr,
            namespace: Some("default"),
            name,
        };
        store.create(rref, &obj).unwrap();
    }

    let selector = FieldSelector::parse("metadata.name=alpha").unwrap();
    let result = store
        .list(&gvr, Some("default"), None, Some(&selector), None, None)
        .unwrap();
    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0]["metadata"]["name"], "alpha");
}

#[tokio::test]
async fn list_with_pagination() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    for name in ["a", "b", "c", "d", "e"] {
        let obj = test_object(name, "default");
        let rref = ResourceRef {
            gvr: &gvr,
            namespace: Some("default"),
            name,
        };
        store.create(rref, &obj).unwrap();
    }

    // First page: limit 2
    let page1 = store
        .list(&gvr, Some("default"), None, None, Some(2), None)
        .unwrap();
    assert_eq!(page1.items.len(), 2);
    assert!(page1.continue_token.is_some());

    // Second page
    let page2 = store
        .list(
            &gvr,
            Some("default"),
            None,
            None,
            Some(2),
            page1.continue_token.as_deref(),
        )
        .unwrap();
    assert_eq!(page2.items.len(), 2);
    assert!(page2.continue_token.is_some());

    // Third page — only 1 left
    let page3 = store
        .list(
            &gvr,
            Some("default"),
            None,
            None,
            Some(2),
            page2.continue_token.as_deref(),
        )
        .unwrap();
    assert_eq!(page3.items.len(), 1);
    assert!(page3.continue_token.is_none());

    // All names should be unique across pages
    let all_names: Vec<&str> = page1
        .items
        .iter()
        .chain(&page2.items)
        .chain(&page3.items)
        .map(|i| i["metadata"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(all_names.len(), 5);
    let mut sorted = all_names.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 5);
}

// ── Store: watch ─────────────────────────────────────────────────────

#[tokio::test]
async fn watch_receives_events() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let mut rx = store.watch(&gvr);

    // Create
    let obj = test_object("nginx", "default");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let created = store.create(rref, &obj).unwrap();

    let event = rx.recv().await.unwrap();
    assert!(matches!(event.event_type, WatchEventType::Added));
    assert_eq!(event.object["metadata"]["name"], "nginx");

    // Update
    let mut modified = created.clone();
    modified["spec"]["containers"][0]["image"] = serde_json::json!("nginx:1.26");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    store.update(&rref, &modified).unwrap();

    let event = rx.recv().await.unwrap();
    assert!(matches!(event.event_type, WatchEventType::Modified));

    // Delete
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    store.delete(&rref).unwrap();

    let event = rx.recv().await.unwrap();
    assert!(matches!(event.event_type, WatchEventType::Deleted));
}

// ── Store: resource version persists across reopens ──────────────────

/// Revision counter must resume from where it left off after reopening.
#[tokio::test]
async fn revision_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let path2 = dir.path().join("test2.db");

    // Create a store and write one resource
    let store = Store::open(&path).unwrap();
    let gvr = test_gvr();
    let obj = test_object("nginx", "default");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let created = store.create(rref, &obj).unwrap();
    let rv1: u64 = created["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    // Copy the db file so we can open it independently
    std::fs::copy(&path, &path2).unwrap();

    // Open the copy — revision should continue past rv1
    let store2 = Store::open(&path2).unwrap();
    let obj2 = test_object("redis", "default");
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "redis",
    };
    let created2 = store2.create(rref, &obj2).unwrap();
    let rv2: u64 = created2["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    assert!(
        rv2 > rv1,
        "revision should be higher after reopen (rv1={rv1}, rv2={rv2})"
    );
}

// ── LabelSelector parsing ────────────────────────────────────────────

#[test]
fn label_selector_parse_equals() {
    let sel = LabelSelector::parse("app=web").unwrap();
    assert_eq!(sel.requirements.len(), 1);
}

#[test]
fn label_selector_parse_multiple() {
    let sel = LabelSelector::parse("app=web,env!=prod,tier").unwrap();
    assert_eq!(sel.requirements.len(), 3);
}

#[test]
fn label_selector_parse_empty() {
    let sel = LabelSelector::parse("").unwrap();
    assert_eq!(sel.requirements.len(), 0);
}

// ── FieldSelector parsing ────────────────────────────────────────────

#[test]
fn field_selector_parse_equals() {
    let sel = FieldSelector::parse("metadata.name=nginx").unwrap();
    assert_eq!(sel.requirements.len(), 1);
}

#[test]
fn field_selector_parse_not_equals() {
    let sel = FieldSelector::parse("metadata.namespace!=kube-system").unwrap();
    assert_eq!(sel.requirements.len(), 1);
}

#[test]
fn field_selector_parse_invalid() {
    let result = FieldSelector::parse("just-a-key");
    assert!(result.is_err());
}

// ── FieldSelector matching ───────────────────────────────────────────

#[test]
fn field_selector_matches_nested_field() {
    let sel = FieldSelector::parse("metadata.name=nginx").unwrap();
    let obj = serde_json::json!({"metadata": {"name": "nginx"}});
    assert!(sel.matches(&obj, "/v1/pods"));

    let obj2 = serde_json::json!({"metadata": {"name": "redis"}});
    assert!(!sel.matches(&obj2, "/v1/pods"));
}

#[test]
fn field_selector_not_equals() {
    let sel = FieldSelector::parse("metadata.namespace!=kube-system").unwrap();
    let obj = serde_json::json!({"metadata": {"namespace": "default"}});
    assert!(sel.matches(&obj, "/v1/pods"));

    let obj2 = serde_json::json!({"metadata": {"namespace": "kube-system"}});
    assert!(!sel.matches(&obj2, "/v1/pods"));
}

/// Injected defaults supply a value for fields an object omits; a present
/// field always overrides the default. The store stays type-agnostic — the
/// test, not the store, decides what `spec.unschedulable` defaults to.
#[test]
fn field_selector_applies_injected_defaults() {
    fn defaults(_object: &serde_json::Value, _gvr: &str, field: &str) -> Option<String> {
        (field == "spec.unschedulable").then(|| "false".to_string())
    }

    let sel = FieldSelector::parse("spec.unschedulable=false")
        .unwrap()
        .with_defaults(defaults);
    let absent = serde_json::json!({"metadata": {"name": "n1"}, "spec": {}});
    assert!(sel.matches(&absent, "example.com/v1/widgets"));

    let cordoned = serde_json::json!({"metadata": {"name": "n2"}, "spec": {"unschedulable": true}});
    assert!(!sel.matches(&cordoned, "example.com/v1/widgets"));

    // Without injected defaults, an absent field has no value to match against.
    let plain = FieldSelector::parse("spec.unschedulable=false").unwrap();
    assert!(!plain.matches(&absent, "example.com/v1/widgets"));
}

// ── Store: durable watch replay ──────────────────────────────────────

#[tokio::test]
async fn watch_from_replays_events_after_revision() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let created = store
        .create(
            ResourceRef {
                gvr: &gvr,
                namespace: Some("default"),
                name: "a",
            },
            &test_object("a", "default"),
        )
        .unwrap();
    store
        .create(
            ResourceRef {
                gvr: &gvr,
                namespace: Some("default"),
                name: "b",
            },
            &test_object("b", "default"),
        )
        .unwrap();

    let since_a: u64 = created["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    // Resuming just after "a" must replay "b" and nothing else.
    let (history, _rx) = store.watch_from(&gvr, since_a).unwrap();
    let names: Vec<&str> = history
        .iter()
        .map(|e| e.object["metadata"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["b"]);
}

#[tokio::test]
async fn watch_from_streams_after_replay() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let created = store
        .create(
            ResourceRef {
                gvr: &gvr,
                namespace: Some("default"),
                name: "a",
            },
            &test_object("a", "default"),
        )
        .unwrap();
    let rv: u64 = created["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    // No buffered events after `rv`, so the replay is empty and the receiver
    // carries the next mutation live.
    let (history, mut rx) = store.watch_from(&gvr, rv).unwrap();
    assert!(history.is_empty());

    store
        .create(
            ResourceRef {
                gvr: &gvr,
                namespace: Some("default"),
                name: "b",
            },
            &test_object("b", "default"),
        )
        .unwrap();

    let event = rx.recv().await.unwrap();
    assert!(matches!(event.event_type, WatchEventType::Added));
    assert_eq!(event.object["metadata"]["name"], "b");
}

// ── Store: list_and_watch ────────────────────────────────────────────

#[tokio::test]
async fn list_and_watch_snapshots_then_streams() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    store
        .create(
            ResourceRef {
                gvr: &gvr,
                namespace: Some("default"),
                name: "existing",
            },
            &test_object("existing", "default"),
        )
        .unwrap();

    let (snapshot, mut rx) = store
        .list_and_watch(&gvr, Some("default"), None, None)
        .unwrap();
    assert_eq!(snapshot.items.len(), 1);
    assert_eq!(snapshot.items[0]["metadata"]["name"], "existing");

    // A mutation after the snapshot is delivered only on the live stream.
    store
        .create(
            ResourceRef {
                gvr: &gvr,
                namespace: Some("default"),
                name: "fresh",
            },
            &test_object("fresh", "default"),
        )
        .unwrap();

    let event = rx.recv().await.unwrap();
    assert!(matches!(event.event_type, WatchEventType::Added));
    assert_eq!(event.object["metadata"]["name"], "fresh");
}

// ── Store: no-op update ──────────────────────────────────────────────

/// Re-submitting an unchanged object must not bump resourceVersion or emit an
/// event; controllers rely on this to avoid reconcile feedback loops.
#[tokio::test]
async fn update_without_changes_is_a_noop() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let created = store
        .create(
            ResourceRef {
                gvr: &gvr,
                namespace: Some("default"),
                name: "nginx",
            },
            &test_object("nginx", "default"),
        )
        .unwrap();
    let rv = created["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_string();

    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let again = store.update(&rref, &created).unwrap();
    assert_eq!(again["metadata"]["resourceVersion"].as_str().unwrap(), rv);
}

// ── Store: pagination across a deleted continue key ──────────────────

/// If the row named by a continue token is deleted between pages, the next
/// page must resume at the following row rather than skipping it.
#[tokio::test]
async fn list_pagination_survives_deleted_continue_key() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    for name in ["a", "b", "c", "d", "e"] {
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: Some("default"),
                    name,
                },
                &test_object(name, "default"),
            )
            .unwrap();
    }

    let page1 = store
        .list(&gvr, Some("default"), None, None, Some(2), None)
        .unwrap();
    assert_eq!(page1.items.len(), 2);
    assert_eq!(page1.items[1]["metadata"]["name"], "b");
    let token = page1
        .continue_token
        .clone()
        .expect("expected continue token");

    // Delete the row the token points at, then resume.
    store
        .delete(&ResourceRef {
            gvr: &gvr,
            namespace: Some("default"),
            name: "b",
        })
        .unwrap();

    let page2 = store
        .list(&gvr, Some("default"), None, None, Some(2), Some(&token))
        .unwrap();
    let names: Vec<&str> = page2
        .items
        .iter()
        .map(|i| i["metadata"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["c", "d"],
        "must not skip 'c' after 'b' was deleted"
    );
}

// ── Store: no-op detection ignores resourceVersion ───────────────────

#[tokio::test]
async fn update_ignoring_resource_version_is_a_noop() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();

    let created = store
        .create(
            ResourceRef {
                gvr: &gvr,
                namespace: Some("default"),
                name: "nginx",
            },
            &test_object("nginx", "default"),
        )
        .unwrap();
    let rv = created["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .to_string();

    // Identical content but with resourceVersion stripped is still a no-op.
    let mut incoming = created.clone();
    incoming["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");

    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    let again = store.update(&rref, &incoming).unwrap();
    assert_eq!(again["metadata"]["resourceVersion"].as_str().unwrap(), rv);
}

// ── Store: apiserver-shaped continue tokens and remainingItemCount ─────────

mod paging_tokens {
    use base64::prelude::*;
    use r8s_store::{Store, StoreError, backend::ResourceRef};
    use r8s_types::GroupVersionResource;

    fn store_with(n: usize) -> (tempfile::TempDir, Store, GroupVersionResource) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("s.db")).unwrap();
        let gvr = GroupVersionResource::new("", "v1", "podtemplates");
        for i in 0..n {
            let name = format!("template-{i:04}");
            store
                .create(
                    ResourceRef {
                        gvr: &gvr,
                        namespace: Some("default"),
                        name: &name,
                    },
                    &serde_json::json!({"metadata": {"name": name, "namespace": "default"}}),
                )
                .unwrap();
        }
        (dir, store, gvr)
    }

    fn decode(token: &str) -> serde_json::Value {
        serde_json::from_slice(&BASE64_URL_SAFE_NO_PAD.decode(token).unwrap()).unwrap()
    }

    #[test]
    fn pages_carry_remaining_count_and_one_resource_version() {
        let (_d, store, gvr) = store_with(5);
        let page1 = store
            .list(&gvr, Some("default"), None, None, Some(2), None)
            .unwrap();
        assert_eq!(page1.items.len(), 2);
        assert_eq!(page1.remaining_item_count, Some(3));
        let token = page1.continue_token.clone().expect("more pages");
        let decoded = decode(&token);
        assert_eq!(decoded["v"], "meta.k8s.io/v1");
        assert_eq!(decoded["rv"], page1.resource_version);
        assert_eq!(decoded["start"], "template-0001");

        let page2 = store
            .list(&gvr, Some("default"), None, None, Some(2), Some(&token))
            .unwrap();
        assert_eq!(page2.items.len(), 2);
        assert_eq!(page2.remaining_item_count, Some(1));
        assert_eq!(page2.resource_version, page1.resource_version);

        let page3 = store
            .list(
                &gvr,
                Some("default"),
                None,
                None,
                Some(2),
                page2.continue_token.as_deref(),
            )
            .unwrap();
        assert_eq!(page3.items.len(), 1);
        assert!(page3.continue_token.is_none(), "last page has no continue");
        assert!(page3.remaining_item_count.is_none());
        let names: Vec<_> = [&page1, &page2, &page3]
            .iter()
            .flat_map(|p| {
                p.items
                    .iter()
                    .map(|i| i["metadata"]["name"].as_str().unwrap().to_string())
            })
            .collect();
        assert_eq!(
            names,
            [
                "template-0000",
                "template-0001",
                "template-0002",
                "template-0003",
                "template-0004"
            ]
        );
    }

    #[test]
    fn exact_fit_page_has_no_continue() {
        let (_d, store, gvr) = store_with(4);
        let page = store
            .list(&gvr, Some("default"), None, None, Some(4), None)
            .unwrap();
        assert_eq!(page.items.len(), 4);
        assert!(page.continue_token.is_none());
        assert!(page.remaining_item_count.is_none());
    }

    #[test]
    fn selector_suppresses_remaining_count() {
        let (_d, store, gvr) = store_with(5);
        let sel = r8s_store::index::LabelSelector::parse("").unwrap();
        let page = store
            .list(&gvr, Some("default"), Some(&sel), None, Some(2), None)
            .unwrap();
        assert!(page.continue_token.is_some());
        assert!(page.remaining_item_count.is_none());
    }

    #[test]
    fn bad_tokens_are_rejected() {
        let (_d, store, gvr) = store_with(2);
        for token in [
            "not-base64!",
            &BASE64_URL_SAFE_NO_PAD.encode(r#"{"v":"meta.k8s.io/v1","rv":0,"start":"a"}"#),
            &BASE64_URL_SAFE_NO_PAD.encode(r#"{"v":"other","rv":5,"start":"a"}"#),
        ] {
            assert!(matches!(
                store.list(&gvr, Some("default"), None, None, Some(1), Some(token)),
                Err(StoreError::InvalidContinueToken)
            ));
        }
    }

    #[test]
    fn pruned_revision_expires_the_token_into_an_inconsistent_one() {
        let (_d, store, gvr) = store_with(3);
        let page1 = store
            .list(&gvr, Some("default"), None, None, Some(1), None)
            .unwrap();
        let token = page1.continue_token.unwrap();
        // Push the revision log past its retention window so page1's revision is gone.
        let cm = GroupVersionResource::new("", "v1", "configmaps");
        for i in 0..1100 {
            let name = format!("churn-{i}");
            store
                .create(
                    ResourceRef {
                        gvr: &cm,
                        namespace: Some("default"),
                        name: &name,
                    },
                    &serde_json::json!({"metadata": {"name": name, "namespace": "default"}}),
                )
                .unwrap();
        }
        let err = store
            .list(&gvr, Some("default"), None, None, Some(1), Some(&token))
            .err()
            .expect("expired token");
        let StoreError::ContinueExpired { inconsistent_token } = err else {
            panic!("unexpected error: {err}");
        };
        let decoded = decode(&inconsistent_token);
        assert_eq!(decoded["rv"], -1);
        assert_eq!(decoded["start"], "template-0000");

        let resumed = store
            .list(
                &gvr,
                Some("default"),
                None,
                None,
                Some(1),
                Some(&inconsistent_token),
            )
            .unwrap();
        assert_eq!(resumed.items[0]["metadata"]["name"], "template-0001");
        assert!(
            resumed.resource_version > page1.resource_version,
            "served at the latest revision"
        );
    }
}

#[tokio::test]
async fn watch_events_carry_previous_object_and_deletion_revision() {
    let dir = TempDir::new().unwrap();
    let store = open_temp_store(&dir);
    let gvr = test_gvr();
    let mut rx = store.watch(&gvr);
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };

    let created = store
        .create(rref, &test_object("nginx", "default"))
        .unwrap();
    let added = rx.recv().await.unwrap();
    assert!(added.prev_object.is_none());

    let mut modified = created.clone();
    modified["metadata"]["labels"] = serde_json::json!({"tier": "web"});
    let rref = ResourceRef {
        gvr: &gvr,
        namespace: Some("default"),
        name: "nginx",
    };
    store.update(&rref, &modified).unwrap();
    let event = rx.recv().await.unwrap();
    assert!(matches!(event.event_type, WatchEventType::Modified));
    let prev = event.prev_object.as_ref().expect("previous object");
    assert_eq!(
        prev["metadata"]["resourceVersion"],
        created["metadata"]["resourceVersion"]
    );
    assert!(prev["metadata"].get("labels").is_none());
    assert_eq!(event.object["metadata"]["labels"]["tier"], "web");

    store.delete(&rref).unwrap();
    let deleted = rx.recv().await.unwrap();
    assert!(matches!(deleted.event_type, WatchEventType::Deleted));
    assert_eq!(
        deleted.object["metadata"]["resourceVersion"],
        deleted.resource_version.to_string(),
        "the DELETED object carries the deletion revision"
    );

    // Replayed history carries the same previous object.
    let (history, _rx) = store.watch_from(&gvr, added.resource_version).unwrap();
    assert_eq!(history.len(), 2);
    assert!(history[0].prev_object.is_some());
    assert_eq!(
        history[1].object["metadata"]["resourceVersion"],
        history[1].resource_version.to_string()
    );
}
