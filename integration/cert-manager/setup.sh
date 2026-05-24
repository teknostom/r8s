#!/usr/bin/env bash
# Prerequisites for cert-manager: namespace + Jetstack repo.
set -euo pipefail

# `kubectl create namespace` uses the typed client which serializes the
# request body as protobuf; r8s doesn't have a Namespace protobuf decoder.
# The dynamic-client path used by `kubectl apply -f-` sends JSON.
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: cert-manager
EOF

helm repo list 2>/dev/null | grep -q '^jetstack\b' \
    || helm repo add jetstack https://charts.jetstack.io
helm repo update jetstack
