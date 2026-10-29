//! ControllerRevision history shared by the StatefulSet and DaemonSet
//! controllers: one revision per distinct pod template, named
//! `<owner>-<hash>` and labelled `controller-revision-hash`.

use std::collections::BTreeMap;

use r8s_store::{Store, StoreError, backend::ResourceRef};
use r8s_types::{
    ControllerRevision, GroupVersionResource, ObjectMeta, PodTemplateSpec, RawExtension,
};
use serde_json::{Value, json};

pub(crate) const REVISION_HASH_LABEL: &str = "controller-revision-hash";

/// The workload a revision belongs to.
pub(crate) struct Parent<'a> {
    pub api_version: &'a str,
    pub kind: &'a str,
    pub name: &'a str,
    pub namespace: Option<&'a str>,
    pub uid: &'a str,
}

impl Parent<'_> {
    fn revision_ref<'a>(&'a self, gvr: &'a GroupVersionResource, name: &'a str) -> ResourceRef<'a> {
        ResourceRef {
            gvr,
            namespace: self.namespace,
            name,
        }
    }
}

/// Revision data `{"spec":{"template":{..., "$patch":"replace"}}}`. The
/// template is put in the shape Go's typed marshalling gives it, so a client
/// computing the same patch from the served workload gets equal bytes.
pub(crate) fn template_patch(template: &PodTemplateSpec) -> Value {
    let mut template = serde_json::to_value(template).unwrap_or(Value::Null);
    go_shape(&mut template);
    if let Some(t) = template.as_object_mut() {
        if let Some(meta) = t
            .entry("metadata")
            .or_insert_with(|| json!({}))
            .as_object_mut()
        {
            meta.insert("creationTimestamp".into(), Value::Null);
        }
        if let Some(spec) = t.get_mut("spec").and_then(|s| s.as_object_mut()) {
            for list in ["containers", "initContainers", "ephemeralContainers"] {
                for container in spec
                    .get_mut(list)
                    .and_then(|c| c.as_array_mut())
                    .into_iter()
                    .flatten()
                {
                    if let Some(c) = container.as_object_mut() {
                        c.entry("resources").or_insert_with(|| json!({}));
                    }
                }
            }
        }
        t.insert("$patch".into(), json!("replace"));
    }
    json!({"spec": {"template": template}})
}

/// Pod-template fields Go marshals as maps with `omitempty`.
const OMITEMPTY_MAPS: &[&str] = &[
    "labels",
    "annotations",
    "nodeSelector",
    "limits",
    "requests",
    "matchLabels",
    "overhead",
];

/// Drop what Go's `omitempty` drops: empty strings, `false`, zero numbers
/// (except `containerPort`, which has no `omitempty`), empty arrays, and empty
/// maps for the map-typed fields.
fn go_shape(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for v in map.values_mut() {
                go_shape(v);
            }
            map.retain(|k, v| !omitted(k, v));
        }
        Value::Array(items) => {
            for item in items {
                go_shape(item);
            }
        }
        _ => {}
    }
}

fn omitted(key: &str, value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Bool(b) => !b,
        Value::Number(n) => n.as_f64() == Some(0.0) && key != "containerPort",
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty() && OMITEMPTY_MAPS.contains(&key),
    }
}

/// FNV-1 32-bit over the serialized data and the collision count, encoded like
/// Go's `rand.SafeEncodeString`.
pub(crate) fn hash(data: &Value, collision_count: i32) -> String {
    let mut h: u32 = 0x811c_9dc5;
    for b in serde_json::to_vec(data)
        .unwrap_or_default()
        .iter()
        .chain(collision_count.to_string().as_bytes())
    {
        h = h.wrapping_mul(0x0100_0193) ^ u32::from(*b);
    }
    safe_encode(&h.to_string())
}

fn safe_encode(s: &str) -> String {
    const ALPHANUMS: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
    s.bytes()
        .map(|b| ALPHANUMS[usize::from(b) % ALPHANUMS.len()] as char)
        .collect()
}

pub(crate) fn revision_name(parent: &str, hash: &str) -> String {
    let prefix = parent.get(..223).unwrap_or(parent);
    format!("{prefix}-{hash}")
}

pub(crate) fn name_of(rev: &ControllerRevision) -> &str {
    rev.metadata.name.as_deref().unwrap_or("")
}

pub(crate) fn hash_of(rev: &ControllerRevision) -> &str {
    rev.metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(REVISION_HASH_LABEL))
        .map(String::as_str)
        .unwrap_or("")
}

pub(crate) fn data_of(rev: &ControllerRevision) -> Option<&Value> {
    rev.data.as_ref().map(|d| &d.0)
}

/// The pod template a revision records.
pub(crate) fn template_of(rev: &ControllerRevision) -> Option<PodTemplateSpec> {
    let mut template = data_of(rev)?.get("spec")?.get("template")?.clone();
    if let Some(t) = template.as_object_mut() {
        t.remove("$patch");
        if let Some(meta) = t.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            meta.remove("creationTimestamp");
        }
    }
    serde_json::from_value(template).ok()
}

/// Revisions owned by `parent`, oldest first (revision, creationTimestamp, name).
pub(crate) fn owned_revisions(
    store: &Store,
    parent: &Parent,
) -> anyhow::Result<Vec<ControllerRevision>> {
    let gvr = GroupVersionResource::controller_revisions();
    let mut revisions: Vec<ControllerRevision> = store
        .list_as::<ControllerRevision>(&gvr, parent.namespace)?
        .into_iter()
        .filter(|r| crate::is_owned_by(&r.metadata, parent.uid))
        .collect();
    revisions.sort_by(|a, b| {
        a.revision
            .cmp(&b.revision)
            .then_with(|| {
                a.metadata
                    .creation_timestamp
                    .cmp(&b.metadata.creation_timestamp)
            })
            .then_with(|| a.metadata.name.cmp(&b.metadata.name))
    });
    Ok(revisions)
}

/// Create a revision of `data`, bumping `collision_count` past names already
/// taken by a different template.
pub(crate) fn create_revision(
    store: &Store,
    parent: &Parent,
    mut labels: BTreeMap<String, String>,
    annotations: Option<BTreeMap<String, String>>,
    data: Value,
    revision: i64,
    collision_count: &mut i32,
) -> anyhow::Result<ControllerRevision> {
    let gvr = GroupVersionResource::controller_revisions();
    loop {
        let hash = hash(&data, *collision_count);
        let name = revision_name(parent.name, &hash);
        labels.insert(REVISION_HASH_LABEL.to_string(), hash);
        let rev = ControllerRevision {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: parent.namespace.map(String::from),
                labels: Some(labels.clone()),
                annotations: annotations.clone(),
                owner_references: Some(vec![crate::owner_reference(
                    parent.api_version,
                    parent.kind,
                    parent.name,
                    parent.uid,
                )]),
                ..Default::default()
            },
            data: Some(RawExtension(data.clone())),
            revision,
        };
        let rref = parent.revision_ref(&gvr, &name);
        match store.create(rref, &serde_json::to_value(&rev)?) {
            Ok(created) => return Ok(serde_json::from_value(created)?),
            Err(StoreError::AlreadyExists { .. }) => {
                let existing: Option<ControllerRevision> =
                    store.get_as(&parent.revision_ref(&gvr, &name))?;
                if let Some(existing) = existing
                    && data_of(&existing) == Some(&data)
                {
                    return Ok(existing);
                }
                *collision_count += 1;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Renumber an existing revision (a rollback reuses the old template's
/// revision as the newest one).
pub(crate) fn set_revision_number(
    store: &Store,
    parent: &Parent,
    rev: &ControllerRevision,
    revision: i64,
) -> anyhow::Result<ControllerRevision> {
    if rev.revision == revision {
        return Ok(rev.clone());
    }
    let gvr = GroupVersionResource::controller_revisions();
    let rref = parent.revision_ref(&gvr, name_of(rev));
    let Some(mut value) = store.get(&rref)? else {
        return Ok(rev.clone());
    };
    value["revision"] = json!(revision);
    Ok(serde_json::from_value(store.update(&rref, &value)?)?)
}

pub(crate) fn delete_revision(
    store: &Store,
    parent: &Parent,
    rev: &ControllerRevision,
) -> anyhow::Result<()> {
    let gvr = GroupVersionResource::controller_revisions();
    store.delete(&parent.revision_ref(&gvr, name_of(rev)))?;
    Ok(())
}

/// Delete the oldest revisions `live` does not claim, keeping at most `limit`
/// of them. `revisions` is oldest first.
pub(crate) fn truncate(
    store: &Store,
    parent: &Parent,
    revisions: &[ControllerRevision],
    limit: i32,
    live: impl Fn(&ControllerRevision) -> bool,
) -> anyhow::Result<()> {
    if limit < 0 {
        return Ok(());
    }
    let stale: Vec<&ControllerRevision> = revisions.iter().filter(|r| !live(r)).collect();
    let excess = stale.len().saturating_sub(limit as usize);
    for rev in stale.into_iter().take(excess) {
        delete_revision(store, parent, rev)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn template(json: Value) -> PodTemplateSpec {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn template_patch_has_go_shape() {
        let patch = template_patch(&template(json!({
            "metadata": {"labels": {"app": "x"}, "annotations": {}},
            "spec": {
                "hostNetwork": false,
                "nodeName": "",
                "containers": [{
                    "name": "c",
                    "image": "img",
                    "workingDir": "",
                    "ports": [{"containerPort": 0, "hostPort": 0}],
                    "securityContext": {}
                }]
            }
        })));
        let t = &patch["spec"]["template"];
        assert_eq!(t["$patch"], "replace");
        assert!(t["metadata"]["creationTimestamp"].is_null());
        assert!(t["metadata"].get("annotations").is_none());
        assert!(t["spec"].get("hostNetwork").is_none());
        assert!(t["spec"].get("nodeName").is_none());
        let c = &t["spec"]["containers"][0];
        assert_eq!(c["resources"], json!({}));
        assert_eq!(c["securityContext"], json!({}));
        assert!(c.get("workingDir").is_none());
        assert_eq!(c["ports"][0], json!({"containerPort": 0}));
    }

    #[test]
    fn hash_depends_on_data_and_collision_count() {
        let a = json!({"spec": {"template": {"x": 1}}});
        let b = json!({"spec": {"template": {"x": 2}}});
        assert_eq!(hash(&a, 0), hash(&a, 0));
        assert_ne!(hash(&a, 0), hash(&b, 0));
        assert_ne!(hash(&a, 0), hash(&a, 1));
        assert!(
            hash(&a, 0)
                .bytes()
                .all(|c| b"bcdfghjklmnpqrstvwxz2456789".contains(&c))
        );
    }

    #[test]
    fn template_round_trips_through_revision_data() {
        let tmpl = template(json!({
            "metadata": {"labels": {"app": "x"}},
            "spec": {"containers": [{"name": "c", "image": "img:1"}]}
        }));
        let rev = ControllerRevision {
            data: Some(RawExtension(template_patch(&tmpl))),
            ..Default::default()
        };
        let back = template_of(&rev).unwrap();
        assert_eq!(
            back.spec.unwrap().containers[0].image.as_deref(),
            Some("img:1")
        );
        assert_eq!(back.metadata.unwrap().labels.unwrap()["app"], "x");
    }

    #[test]
    fn create_revision_reuses_equal_and_bumps_collisions() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.db")).unwrap();
        let parent = Parent {
            api_version: "apps/v1",
            kind: "StatefulSet",
            name: "web",
            namespace: Some("default"),
            uid: "uid-1",
        };
        let data = json!({"spec": {"template": {"a": 1}}});
        let mut collisions = 0;
        let first = create_revision(
            &store,
            &parent,
            BTreeMap::new(),
            None,
            data.clone(),
            1,
            &mut collisions,
        )
        .unwrap();
        let again = create_revision(
            &store,
            &parent,
            BTreeMap::new(),
            None,
            data.clone(),
            2,
            &mut collisions,
        )
        .unwrap();
        assert_eq!(name_of(&first), name_of(&again));
        assert_eq!(again.revision, 1);
        assert_eq!(collisions, 0);
        assert_eq!(hash_of(&first), hash(&data, 0));
        assert_eq!(name_of(&first), format!("web-{}", hash(&data, 0)));

        // A different template whose name is taken moves to the next count.
        let gvr = GroupVersionResource::controller_revisions();
        let taken = revision_name("web", &hash(&json!({"b": 2}), 0));
        let mut squatter = serde_json::to_value(&ControllerRevision {
            metadata: ObjectMeta {
                name: Some(taken.clone()),
                namespace: Some("default".into()),
                ..Default::default()
            },
            data: Some(RawExtension(json!({"other": true}))),
            revision: 9,
        })
        .unwrap();
        squatter["metadata"]["namespace"] = json!("default");
        store
            .create(
                ResourceRef {
                    gvr: &gvr,
                    namespace: Some("default"),
                    name: &taken,
                },
                &squatter,
            )
            .unwrap();
        let moved = create_revision(
            &store,
            &parent,
            BTreeMap::new(),
            None,
            json!({"b": 2}),
            2,
            &mut collisions,
        )
        .unwrap();
        assert_eq!(collisions, 1);
        assert_eq!(
            name_of(&moved),
            revision_name("web", &hash(&json!({"b": 2}), 1))
        );

        let owned = owned_revisions(&store, &parent).unwrap();
        assert_eq!(owned.len(), 2);
        assert_eq!(owned[0].revision, 1);
        assert_eq!(owned[1].revision, 2);

        truncate(&store, &parent, &owned, 0, |r| r.revision == 2).unwrap();
        let left = owned_revisions(&store, &parent).unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].revision, 2);
    }
}
