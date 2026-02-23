//! Minimal decoder for the Kubernetes protobuf wire format.
//!
//! The Go client-go library sends requests with `Content-Type: application/vnd.kubernetes.protobuf`.
//! The wire format is: 4-byte magic `k8s\0` followed by a protobuf-encoded `Unknown` envelope:
//!
//! ```proto
//! message Unknown {
//!     optional TypeMeta typeMeta = 1;   // apiVersion + kind
//!     optional bytes raw = 2;           // protobuf-encoded resource
//!     optional string contentEncoding = 3;
//!     optional string contentType = 4;
//! }
//! ```
//!
//! The `raw` field contains the protobuf-encoded resource, where by Kubernetes convention:
//! - Field 1 is always ObjectMeta for top-level resources
//!
//! We decode just enough to extract metadata (name, namespace, labels, annotations)
//! and construct a JSON object that the store can persist.

const K8S_MAGIC: &[u8] = b"k8s\0";

/// Decode a protobuf varint. Returns (value, bytes_consumed).
fn decode_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

/// Skip a protobuf field value based on its wire type. Returns bytes consumed.
fn skip_field(data: &[u8], wire_type: u8) -> Option<usize> {
    match wire_type {
        0 => decode_varint(data).map(|(_, n)| n),
        1 => Some(8),
        2 => {
            let (len, n) = decode_varint(data)?;
            Some(n + len as usize)
        }
        5 => Some(4),
        _ => None,
    }
}

/// Extract all occurrences of a length-delimited field (wire type 2) by field number.
fn extract_fields<'a>(data: &'a [u8], target: u32) -> Vec<&'a [u8]> {
    let mut results = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        let (tag, n) = match decode_varint(&data[pos..]) {
            Some(v) => v,
            None => break,
        };
        pos += n;
        let field_number = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;

        if wire_type == 2 {
            let (len, n) = match decode_varint(&data[pos..]) {
                Some(v) => v,
                None => break,
            };
            pos += n;
            let len = len as usize;
            if pos + len > data.len() {
                break;
            }
            if field_number == target {
                results.push(&data[pos..pos + len]);
            }
            pos += len;
        } else {
            match skip_field(&data[pos..], wire_type) {
                Some(n) => pos += n,
                None => break,
            }
        }
    }
    results
}

/// Extract the first occurrence of a length-delimited field.
fn extract_field<'a>(data: &'a [u8], target: u32) -> Option<&'a [u8]> {
    extract_fields(data, target).into_iter().next()
}

/// Extract a string field.
fn extract_string(data: &[u8], field_number: u32) -> Option<String> {
    extract_field(data, field_number)
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(String::from)
}

/// Extract a `map<string, string>` protobuf field.
/// Protobuf encodes maps as repeated messages with key=1, value=2.
fn extract_string_map(
    data: &[u8],
    field_number: u32,
) -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::new();
    for entry in extract_fields(data, field_number) {
        if let (Some(k), Some(v)) = (extract_string(entry, 1), extract_string(entry, 2)) {
            map.insert(k, serde_json::Value::String(v));
        }
    }
    map
}

/// Try to decode a Kubernetes protobuf request body into a JSON value.
///
/// Extracts apiVersion, kind, and ObjectMeta (name, namespace, labels, annotations).
/// The spec/status are not decoded since we don't have the per-type proto definitions,
/// but this is sufficient for store-only resources (PDB, HPA, etc.).
pub fn decode_k8s_protobuf(body: &[u8]) -> Option<serde_json::Value> {
    if !body.starts_with(K8S_MAGIC) {
        return None;
    }
    let envelope = &body[K8S_MAGIC.len()..];

    // Unknown envelope: field 1 = TypeMeta, field 2 = raw
    let type_meta = extract_field(envelope, 1)?;
    let raw = extract_field(envelope, 2)?;

    let api_version = extract_string(type_meta, 1).unwrap_or_default();
    let kind = extract_string(type_meta, 2).unwrap_or_default();

    // Resource message: field 1 = ObjectMeta (by Kubernetes convention)
    let object_meta = extract_field(raw, 1)?;

    let name = extract_string(object_meta, 1).unwrap_or_default();
    let namespace = extract_string(object_meta, 3);
    let labels = extract_string_map(object_meta, 11);
    let annotations = extract_string_map(object_meta, 12);

    let mut metadata = serde_json::Map::new();
    metadata.insert("name".into(), serde_json::Value::String(name));
    if let Some(ns) = namespace {
        metadata.insert("namespace".into(), serde_json::Value::String(ns));
    }
    if !labels.is_empty() {
        metadata.insert("labels".into(), serde_json::Value::Object(labels));
    }
    if !annotations.is_empty() {
        metadata.insert("annotations".into(), serde_json::Value::Object(annotations));
    }

    let mut obj = serde_json::Map::new();
    obj.insert(
        "apiVersion".into(),
        serde_json::Value::String(api_version),
    );
    obj.insert("kind".into(), serde_json::Value::String(kind));
    obj.insert("metadata".into(), serde_json::Value::Object(metadata));

    Some(serde_json::Value::Object(obj))
}
