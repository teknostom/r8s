#!/usr/bin/env bash
# End-to-end test: issue a self-signed cert and verify the resulting Secret
# contains a parseable X.509 cert with the expected CN.
set -euo pipefail

NS=cert-manager-test
CN=r8s-integration-test

kubectl apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${NS}
---
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: selfsigned
  namespace: ${NS}
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: integration-cert
  namespace: ${NS}
spec:
  secretName: integration-cert-tls
  commonName: ${CN}
  issuerRef:
    name: selfsigned
    kind: Issuer
EOF

# Wait for cert-manager to drive the Certificate to Ready (creates
# CertificateRequest, signs it, materializes the Secret).
kubectl -n "$NS" wait --for=condition=Ready certificate/integration-cert --timeout=60s

# Pull the cert out of the Secret and confirm it parses + carries the
# right CN. openssl is the universally-available x509 toolkit; if it's
# missing we just fail loud.
crt=$(kubectl -n "$NS" get secret integration-cert-tls -o jsonpath='{.data.tls\.crt}' | base64 -d)
echo "$crt" | openssl x509 -noout -subject | grep -q "CN ?= ?${CN}" \
    || { echo "cert subject CN mismatch"; echo "$crt" | openssl x509 -noout -subject; exit 1; }

echo "OK: certificate issued with CN=${CN}"
