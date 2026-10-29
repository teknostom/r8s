//! `events.k8s.io/v1` Events share the core `v1` Event storage, as upstream
//! serves both groups from one resource: objects are stored in the core
//! shape and converted on the way in and out.

use serde_json::{Map, Value};

use crate::handler::RouteContext;

pub const EVENTS_V1: &str = "events.k8s.io/v1";

/// (core field, events.k8s.io field).
const RENAMES: &[(&str, &str)] = &[
    ("involvedObject", "regarding"),
    ("message", "note"),
    ("source", "deprecatedSource"),
    ("firstTimestamp", "deprecatedFirstTimestamp"),
    ("lastTimestamp", "deprecatedLastTimestamp"),
    ("count", "deprecatedCount"),
    ("reportingComponent", "reportingController"),
];

/// Whether `ctx` is the events.k8s.io Event resource.
pub fn applies(ctx: &RouteContext) -> bool {
    let gvr = &ctx.resource_type.gvr;
    gvr.group == "events.k8s.io" && gvr.resource == "events"
}

/// `obj` in `desired` (`v1` or `events.k8s.io/v1`), from either shape.
pub fn convert(obj: Value, desired: &str) -> Value {
    let current = obj
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("v1");
    match (current == EVENTS_V1, desired == EVENTS_V1) {
        (false, true) => rename(obj, EVENTS_V1, |(core, events)| (core, events)),
        (true, false) => rename(obj, "v1", |(core, events)| (events, core)),
        _ => obj,
    }
}

fn rename(
    obj: Value,
    api_version: &str,
    direction: impl Fn((&'static str, &'static str)) -> (&'static str, &'static str),
) -> Value {
    let Value::Object(mut map) = obj else {
        return obj;
    };
    let mut out = Map::new();
    for &pair in RENAMES {
        let (from, to) = direction(pair);
        if let Some(value) = map.remove(from)
            && !is_empty(&value)
        {
            out.insert(to.to_string(), value);
        }
    }
    out.extend(map);
    out.insert("apiVersion".to_string(), Value::String(api_version.into()));
    out.insert("kind".to_string(), Value::String("Event".into()));
    Value::Object(out)
}

/// An empty object, the zero value Go clients send for an unset struct.
fn is_empty(value: &Value) -> bool {
    value.is_null() || value.as_object().is_some_and(|o| o.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trips_between_the_two_shapes() {
        let events_v1 = json!({
            "apiVersion": EVENTS_V1, "kind": "Event",
            "metadata": {"name": "e", "namespace": "ns"},
            "regarding": {"kind": "Pod", "name": "p"},
            "note": "This is e", "reason": "Test", "type": "Normal", "action": "Do",
            "reportingController": "test-controller", "reportingInstance": "test-node",
            "eventTime": "2026-10-27T00:00:00.000000Z",
            "deprecatedSource": {},
        });
        let core = convert(events_v1.clone(), "v1");
        assert_eq!(core["apiVersion"], "v1");
        assert_eq!(core["involvedObject"]["name"], "p");
        assert_eq!(core["message"], "This is e");
        assert_eq!(core["reportingComponent"], "test-controller");
        assert!(core.get("source").is_none());
        assert!(core.get("note").is_none());

        let back = convert(core, EVENTS_V1);
        let mut expected = events_v1;
        expected.as_object_mut().unwrap().remove("deprecatedSource");
        assert_eq!(back, expected);
    }

    #[test]
    fn same_shape_passes_through() {
        let core = json!({"apiVersion": "v1", "kind": "Event", "message": "m"});
        assert_eq!(convert(core.clone(), "v1"), core);
    }
}
