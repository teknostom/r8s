use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub cluster: Option<ClusterConfig>,
    #[serde(default)]
    pub dependencies: Vec<Release>,
    pub app: Option<Release>,
}

#[derive(Debug, Deserialize)]
pub struct ClusterConfig {
    pub name: Option<String>,
    pub namespace: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Release {
    pub name: String,
    pub chart: String,
    pub version: Option<String>,
    pub namespace: Option<String>,
    #[serde(default)]
    pub values: Vec<PathBuf>,
    #[serde(default)]
    pub set: BTreeMap<String, String>,
}

pub fn load(path: &Path) -> anyhow::Result<Config> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", path.display()))?;
    let config: Config = toml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("failed to parse {}: {e}", path.display()))?;
    validate(&config, path)?;
    Ok(config)
}

fn validate(config: &Config, config_path: &Path) -> anyhow::Result<()> {
    if config.dependencies.is_empty() && config.app.is_none() {
        anyhow::bail!("r8s.toml must define at least one dependency or an app");
    }

    let base_dir = config_path.parent().unwrap_or(Path::new("."));

    let mut names = Vec::new();
    for dep in &config.dependencies {
        validate_release(dep, base_dir)?;
        names.push(&dep.name);
    }
    if let Some(app) = &config.app {
        validate_release(app, base_dir)?;
        if names.contains(&&app.name) {
            anyhow::bail!("duplicate release name '{}'", app.name);
        }
    }

    // Check for duplicate dependency names
    let mut seen = rustc_hash::FxHashSet::default();
    for name in &names {
        if !seen.insert(name) {
            anyhow::bail!("duplicate dependency name '{name}'");
        }
    }

    Ok(())
}

fn validate_release(release: &Release, base_dir: &Path) -> anyhow::Result<()> {
    if release.name.is_empty() {
        anyhow::bail!("release name cannot be empty");
    }
    if release.chart.is_empty() {
        anyhow::bail!("chart cannot be empty for release '{}'", release.name);
    }
    for values_file in &release.values {
        let resolved = base_dir.join(values_file);
        if !resolved.exists() {
            anyhow::bail!(
                "values file '{}' not found (resolved to '{}')",
                values_file.display(),
                resolved.display()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_config() {
        let toml = r#"
[cluster]
name = "myproject"
namespace = "prod"

[[dependencies]]
name = "postgresql"
chart = "bitnami/postgresql"
version = "16.0.0"
values = []

[[dependencies]]
name = "redis"
chart = "bitnami/redis"

[app]
name = "myapp"
chart = "./charts/myapp"
values = []
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(
            config.cluster.as_ref().unwrap().name.as_deref(),
            Some("myproject")
        );
        assert_eq!(config.dependencies.len(), 2);
        assert_eq!(config.dependencies[0].name, "postgresql");
        assert_eq!(config.dependencies[0].version.as_deref(), Some("16.0.0"));
        assert_eq!(config.dependencies[1].name, "redis");
        assert_eq!(config.app.as_ref().unwrap().name, "myapp");
    }

    #[test]
    fn parse_deps_only() {
        let toml = r#"
[[dependencies]]
name = "postgresql"
chart = "bitnami/postgresql"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.dependencies.len(), 1);
        assert!(config.app.is_none());
    }

    #[test]
    fn parse_app_only() {
        let toml = r#"
[app]
name = "myapp"
chart = "./charts/myapp"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.dependencies.is_empty());
        assert!(config.app.is_some());
    }

    #[test]
    fn parse_set_overrides() {
        let toml = r#"
[[dependencies]]
name = "pg"
chart = "bitnami/postgresql"

[dependencies.set]
"auth.postgresPassword" = "secret"
"primary.resources.requests.memory" = "256Mi"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        let dep = &config.dependencies[0];
        assert_eq!(dep.set.get("auth.postgresPassword").unwrap(), "secret");
    }

    #[test]
    fn empty_config_fails_validation() {
        let toml = "";
        let config: Config = toml::from_str(toml).unwrap();
        let err = validate(&config, Path::new("r8s.toml")).unwrap_err();
        assert!(err.to_string().contains("at least one"));
    }

    #[test]
    fn duplicate_names_fail() {
        let toml = r#"
[[dependencies]]
name = "pg"
chart = "bitnami/postgresql"

[[dependencies]]
name = "pg"
chart = "bitnami/postgresql"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        let err = validate(&config, Path::new("r8s.toml")).unwrap_err();
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn app_name_conflicts_with_dep() {
        let toml = r#"
[[dependencies]]
name = "myapp"
chart = "bitnami/postgresql"

[app]
name = "myapp"
chart = "./charts/myapp"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        let err = validate(&config, Path::new("r8s.toml")).unwrap_err();
        assert!(err.to_string().contains("duplicate"));
    }
}
