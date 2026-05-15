//! Self-signed CA + apiserver TLS cert generation.
//!
//! Generated once per cluster on first boot, persisted under
//! `<data_dir>/certs/` so the same CA survives restarts. The CA PEM is
//! also embedded in ServiceAccount token Secrets so in-cluster clients
//! trust the apiserver they're dialing through the `kubernetes` Service.

use std::path::{Path, PathBuf};

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose, SanType};

const CA_CERT_FILE: &str = "ca.crt";
const CA_KEY_FILE: &str = "ca.key";
const SERVER_CERT_FILE: &str = "tls.crt";
const SERVER_KEY_FILE: &str = "tls.key";

/// Cluster TLS material loaded from disk (or freshly minted).
#[derive(Clone, Debug)]
pub struct CertBundle {
    pub ca_pem: String,
    pub server_cert_pem: String,
    pub server_key_pem: String,
}

impl CertBundle {
    pub fn ca_pem(&self) -> &str {
        &self.ca_pem
    }
}

/// Returns the cluster's TLS bundle, generating it on first call.
pub fn ensure_cluster_certs(data_dir: &Path) -> anyhow::Result<CertBundle> {
    let cert_dir = data_dir.join("certs");
    std::fs::create_dir_all(&cert_dir)?;

    let ca_cert_path = cert_dir.join(CA_CERT_FILE);
    let ca_key_path = cert_dir.join(CA_KEY_FILE);
    let server_cert_path = cert_dir.join(SERVER_CERT_FILE);
    let server_key_path = cert_dir.join(SERVER_KEY_FILE);

    if !all_present(&[
        &ca_cert_path,
        &ca_key_path,
        &server_cert_path,
        &server_key_path,
    ]) {
        generate(&cert_dir)?;
    }

    Ok(CertBundle {
        ca_pem: std::fs::read_to_string(&ca_cert_path)?,
        server_cert_pem: std::fs::read_to_string(&server_cert_path)?,
        server_key_pem: std::fs::read_to_string(&server_key_path)?,
    })
}

fn all_present(paths: &[&PathBuf]) -> bool {
    paths.iter().all(|p| p.exists())
}

fn generate(cert_dir: &Path) -> anyhow::Result<()> {
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "r8s-ca");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_cert = ca_params.self_signed(&ca_key)?;

    let server_key = KeyPair::generate()?;
    let dns_sans = [
        "kubernetes",
        "kubernetes.default",
        "kubernetes.default.svc",
        "kubernetes.default.svc.cluster.local",
        "localhost",
    ];
    let ip_sans = ["10.96.0.1", "10.244.0.1", "127.0.0.1"];
    let mut server_params = CertificateParams::new(Vec::<String>::new())?;
    server_params
        .distinguished_name
        .push(DnType::CommonName, "kube-apiserver");
    server_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    server_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    for s in dns_sans {
        server_params
            .subject_alt_names
            .push(SanType::DnsName(s.try_into()?));
    }
    for s in ip_sans {
        server_params
            .subject_alt_names
            .push(SanType::IpAddress(s.parse()?));
    }
    let server_cert = server_params.signed_by(&server_key, &ca_cert, &ca_key)?;

    std::fs::write(cert_dir.join(CA_CERT_FILE), ca_cert.pem())?;
    std::fs::write(cert_dir.join(CA_KEY_FILE), ca_key.serialize_pem())?;
    std::fs::write(cert_dir.join(SERVER_CERT_FILE), server_cert.pem())?;
    std::fs::write(cert_dir.join(SERVER_KEY_FILE), server_key.serialize_pem())?;
    tracing::info!(
        "generated cluster TLS material under {}",
        cert_dir.display()
    );
    Ok(())
}
