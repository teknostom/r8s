use std::path::Path;
use std::process::Command;

use crate::config::Release;

pub fn check_helm() -> anyhow::Result<()> {
    let status = Command::new("helm")
        .arg("version")
        .arg("--short")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    match status {
        Ok(s) if s.success() => Ok(()),
        Ok(_) => anyhow::bail!("helm found but returned an error. Check your helm installation."),
        Err(_) => anyhow::bail!("helm not found. Install it: https://helm.sh/docs/intro/install/"),
    }
}

/// Render chart via `helm template`, then apply with `kubectl apply --validate=false`.
pub fn install_release(
    release: &Release,
    kubeconfig: &Path,
    base_dir: &Path,
    default_namespace: &str,
) -> anyhow::Result<()> {
    let ns = release
        .namespace
        .as_deref()
        .unwrap_or(default_namespace);

    // helm template to render manifests
    let mut cmd = Command::new("helm");
    cmd.arg("template");
    cmd.arg(&release.name);
    cmd.arg(&release.chart);
    cmd.args(["--namespace", ns]);

    if let Some(version) = &release.version {
        cmd.args(["--version", version]);
    }

    for values_file in &release.values {
        let resolved = base_dir.join(values_file);
        cmd.arg("-f").arg(&resolved);
    }

    for (key, val) in &release.set {
        cmd.arg("--set").arg(format!("{key}={val}"));
    }

    let output = cmd
        .output()
        .map_err(|e| anyhow::anyhow!("failed to run helm template: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("helm template failed for '{}': {stderr}", release.name);
    }

    // Create namespace first
    let ns_status = Command::new("kubectl")
        .args(["create", "namespace", ns])
        .arg("--kubeconfig")
        .arg(kubeconfig)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    // Ignore error — namespace may already exist
    let _ = ns_status;

    // kubectl apply the rendered manifests
    let mut apply = Command::new("kubectl");
    apply.args(["apply", "--validate=false", "-f", "-"]);
    apply.args(["--namespace", ns]);
    apply.arg("--kubeconfig").arg(kubeconfig);
    apply.stdin(std::process::Stdio::piped());

    let mut child = apply
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to run kubectl apply: {e}"))?;

    use std::io::Write;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(&output.stdout)?;
    }

    let status = child.wait()?;
    if !status.success() {
        anyhow::bail!("kubectl apply failed for release '{}'", release.name);
    }

    Ok(())
}
