//! Service account tokens: ES256 JWTs the node signs with a per-cluster
//! P-256 key and the API server verifies; the public key is published as the
//! issuer's JWKS for OIDC discovery. Claims follow the bound tokens
//! upstream's TokenRequest API issues.

use std::path::Path;

use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{
    ECDSA_P256_SHA256_FIXED, ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair,
    UnparsedPublicKey,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use sha2::{Digest, Sha256};

/// Issuer, and the audience a token gets when the request names none.
pub const ISSUER: &str = "https://kubernetes.default.svc.cluster.local";
/// Where the issuer's keys are served, through the kubernetes Service.
pub const JWKS_URI: &str = "https://kubernetes.default.svc.cluster.local/openid/v1/jwks";
/// PKCS#8 of the P-256 key.
const KEY_FILE: &str = "certs/sa-signing-p256.key";

/// The cluster's token signing key.
pub struct SigningKey(EcdsaKeyPair);

/// What a token is bound to.
#[derive(Debug, Default, Clone)]
pub struct Binding {
    pub namespace: String,
    pub service_account: String,
    pub service_account_uid: String,
    pub pod: Option<(String, String)>,
    pub node: Option<(String, String)>,
}

impl SigningKey {
    /// The key under `data_dir`, created on first use.
    pub fn load_or_create(data_dir: &Path) -> std::io::Result<SigningKey> {
        let path = data_dir.join(KEY_FILE);
        if let Ok(pkcs8) = std::fs::read(&path)
            && let Ok(key) = SigningKey::from_pkcs8(&pkcs8)
        {
            return Ok(key);
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let pkcs8 = SigningKey::generate_pkcs8()?;
        std::fs::write(&path, &pkcs8)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }
        SigningKey::from_pkcs8(&pkcs8)
    }

    /// A fresh key's PKCS#8.
    pub fn generate_pkcs8() -> std::io::Result<Vec<u8>> {
        EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
            .map(|doc| doc.as_ref().to_vec())
            .map_err(|e| std::io::Error::other(format!("generate signing key: {e}")))
    }

    pub fn from_pkcs8(pkcs8: &[u8]) -> std::io::Result<SigningKey> {
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8)
            .map(SigningKey)
            .map_err(|e| std::io::Error::other(format!("signing key: {e}")))
    }

    /// The key id: the public point's SHA-256, base64url.
    pub fn key_id(&self) -> String {
        B64.encode(Sha256::digest(self.0.public_key().as_ref()))
    }

    /// The public key as a JWK.
    pub fn jwk(&self) -> serde_json::Value {
        let point = self.0.public_key().as_ref();
        // Uncompressed: 0x04 || x || y.
        let (x, y) = point
            .get(1..)
            .unwrap_or_default()
            .split_at(point.len().saturating_sub(1) / 2);
        serde_json::json!({
            "kty": "EC",
            "crv": "P-256",
            "x": B64.encode(x),
            "y": B64.encode(y),
            "kid": self.key_id(),
            "alg": "ES256",
            "use": "sig",
        })
    }

    /// A signed token carrying `claims`.
    pub fn mint(&self, claims: &serde_json::Value) -> String {
        let header = B64.encode(
            serde_json::to_vec(
                &serde_json::json!({"alg": "ES256", "kid": self.key_id(), "typ": "JWT"}),
            )
            .unwrap_or_default(),
        );
        let payload = B64.encode(serde_json::to_vec(claims).unwrap_or_default());
        let signing_input = format!("{header}.{payload}");
        let signature = self
            .0
            .sign(&SystemRandom::new(), signing_input.as_bytes())
            .map(|s| B64.encode(s.as_ref()))
            .unwrap_or_default();
        format!("{signing_input}.{signature}")
    }

    /// The claims of a token this key signed, if it is intact and current.
    pub fn verify(&self, token: &str, now: i64) -> Result<serde_json::Value, &'static str> {
        let mut parts = token.split('.');
        let (Some(header), Some(payload), Some(signature), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err("token is not a JWT");
        };
        let given = B64.decode(signature).map_err(|_| "malformed signature")?;
        UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, self.0.public_key().as_ref())
            .verify(format!("{header}.{payload}").as_bytes(), &given)
            .map_err(|_| "signature mismatch")?;
        let claims: serde_json::Value =
            serde_json::from_slice(&B64.decode(payload).map_err(|_| "malformed payload")?)
                .map_err(|_| "malformed claims")?;
        if claims["exp"].as_i64().is_some_and(|exp| exp <= now) {
            return Err("token has expired");
        }
        if claims["nbf"].as_i64().is_some_and(|nbf| nbf > now) {
            return Err("token is not valid yet");
        }
        Ok(claims)
    }
}

/// The claims of a token for `binding`, valid for `expiration_seconds` from
/// `now`, for `audiences` (the API server's own when empty).
pub fn claims(
    binding: &Binding,
    audiences: &[String],
    expiration_seconds: i64,
    now: i64,
) -> serde_json::Value {
    let audiences: Vec<&str> = if audiences.is_empty() {
        vec![ISSUER]
    } else {
        audiences.iter().map(String::as_str).collect()
    };
    let mut kubernetes = serde_json::json!({
        "namespace": binding.namespace,
        "serviceaccount": {
            "name": binding.service_account,
            "uid": binding.service_account_uid,
        },
    });
    if let Some((name, uid)) = &binding.pod {
        kubernetes["pod"] = serde_json::json!({"name": name, "uid": uid});
    }
    if let Some((name, uid)) = &binding.node {
        kubernetes["node"] = serde_json::json!({"name": name, "uid": uid});
    }
    serde_json::json!({
        "aud": audiences,
        "exp": now + expiration_seconds,
        "iat": now,
        "nbf": now,
        "iss": ISSUER,
        "jti": uuid::Uuid::new_v4().to_string(),
        "kubernetes.io": kubernetes,
        "sub": format!("system:serviceaccount:{}:{}", binding.namespace, binding.service_account),
    })
}

/// The payload of a JWT, without verifying it.
pub fn unverified_claims(token: &str) -> Option<serde_json::Value> {
    let payload = token.split('.').nth(1)?;
    serde_json::from_slice(&B64.decode(payload).ok()?).ok()
}

/// The issuer's OIDC discovery document.
pub fn openid_configuration() -> serde_json::Value {
    serde_json::json!({
        "issuer": ISSUER,
        "jwks_uri": JWKS_URI,
        "response_types_supported": ["id_token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["ES256"],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> SigningKey {
        SigningKey::from_pkcs8(&SigningKey::generate_pkcs8().unwrap()).unwrap()
    }

    #[test]
    fn jwk_carries_the_public_point() {
        let key = key();
        let jwk = key.jwk();
        assert_eq!(B64.decode(jwk["x"].as_str().unwrap()).unwrap().len(), 32);
        assert_eq!(B64.decode(jwk["y"].as_str().unwrap()).unwrap().len(), 32);
        assert_eq!(jwk["kid"], key.key_id());
        let token = key.mint(&serde_json::json!({"sub": "x"}));
        let header: serde_json::Value =
            serde_json::from_slice(&B64.decode(token.split('.').next().unwrap()).unwrap()).unwrap();
        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["kid"], key.key_id());
    }

    #[test]
    fn minted_tokens_verify_and_expire() {
        let key = key();
        let binding = Binding {
            namespace: "ns".into(),
            service_account: "builder".into(),
            service_account_uid: "u1".into(),
            pod: Some(("p".into(), "pu".into())),
            node: None,
        };
        let token = key.mint(&claims(&binding, &[], 3600, 1000));
        assert_eq!(token.split('.').count(), 3);
        let verified = key.verify(&token, 2000).unwrap();
        assert_eq!(verified["sub"], "system:serviceaccount:ns:builder");
        assert_eq!(verified["aud"][0], ISSUER);
        assert_eq!(verified["kubernetes.io"]["pod"]["name"], "p");
        assert!(verified["jti"].as_str().is_some_and(|j| !j.is_empty()));
        assert_eq!(key.verify(&token, 5000), Err("token has expired"));
        assert_eq!(key.verify(&token, 999), Err("token is not valid yet"));

        let other = super::tests::key();
        assert_eq!(other.verify(&token, 2000), Err("signature mismatch"));
        let mut forged = token.clone();
        forged.replace_range(..1, "f");
        assert!(other.verify(&forged, 2000).is_err());
        assert_eq!(key.verify("not.a", 2000), Err("token is not a JWT"));
    }

    #[test]
    fn key_is_created_once_under_data_dir() {
        let dir = tempfile::tempdir().unwrap();
        let a = SigningKey::load_or_create(dir.path()).unwrap();
        let b = SigningKey::load_or_create(dir.path()).unwrap();
        assert_eq!(a.key_id(), b.key_id());
    }
}
