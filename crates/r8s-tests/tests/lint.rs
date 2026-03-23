use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Collect all production `.rs` source files (crates/*/src/**/*.rs).
fn production_sources() -> Vec<PathBuf> {
    let crates_dir = workspace_root().join("crates");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&crates_dir).expect("cannot read crates/") {
        let crate_dir = entry.unwrap().path();
        let src_dir = crate_dir.join("src");
        if src_dir.is_dir() {
            collect_rs_files(&src_dir, &mut files);
        }
    }
    files
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Returns true if a line has `identifier["` — raw indexing on serde_json::Value.
/// Returns false for array literals like `&["`, `!["`, `(["`, `, "`.
fn has_raw_value_indexing(line: &str) -> bool {
    let mut search_from = 0;
    while let Some(pos) = line[search_from..].find("[\"") {
        let abs_pos = search_from + pos;
        if abs_pos > 0 {
            let prev = line.as_bytes()[abs_pos - 1];
            // word char before [" means identifier["key"] — raw indexing
            // non-word chars (&, !, comma, space, paren) mean array literal
            if prev.is_ascii_alphanumeric() || prev == b'_' || prev == b']' {
                return true;
            }
        }
        search_from = abs_pos + 2;
    }
    false
}

#[test]
fn no_raw_serde_json_indexing() {
    let mut violations = Vec::new();

    for path in production_sources() {
        let content = std::fs::read_to_string(&path).unwrap();
        for (line_num, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.starts_with("//") || trimmed.starts_with('*') {
                continue;
            }
            if has_raw_value_indexing(trimmed) {
                let rel = path.strip_prefix(workspace_root()).unwrap_or(&path);
                violations.push(format!("  {}:{}: {}", rel.display(), line_num + 1, trimmed));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "\nRaw indexing on serde_json::Value detected. Use .get() instead:\n{}\n",
        violations.join("\n")
    );
}

#[test]
fn no_serde_json_serialization_unwrap() {
    let mut violations = Vec::new();

    for path in production_sources() {
        let content = std::fs::read_to_string(&path).unwrap();
        for (line_num, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.starts_with("//") {
                continue;
            }
            let has_serde_ser = trimmed.contains("serde_json::to_vec")
                || trimmed.contains("serde_json::to_string")
                || trimmed.contains("serde_json::to_value");
            if has_serde_ser && trimmed.contains(".unwrap()") {
                let rel = path.strip_prefix(workspace_root()).unwrap_or(&path);
                violations.push(format!("  {}:{}: {}", rel.display(), line_num + 1, trimmed));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "\nserde_json serialization with .unwrap() detected. Use ? or unwrap_or_default() instead:\n{}\n",
        violations.join("\n")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detection_logic() {
        // Should detect
        assert!(has_raw_value_indexing(r#"obj["metadata"]"#));
        assert!(has_raw_value_indexing(r#"value["key"]["nested"]"#));
        assert!(has_raw_value_indexing(r#"body["spec"]["type"]"#));

        // Should NOT detect (array/slice literals)
        assert!(!has_raw_value_indexing(r#"&["foo", "bar"]"#));
        assert!(!has_raw_value_indexing(r#"vec!["hello"]"#));
        assert!(!has_raw_value_indexing(r#".args(["apply", "-f"])"#));
        assert!(!has_raw_value_indexing(r#"for ns in ["default", "kube-system"]"#));
        assert!(!has_raw_value_indexing(r#"str_val(obj, &["metadata", "name"])"#));
    }
}
