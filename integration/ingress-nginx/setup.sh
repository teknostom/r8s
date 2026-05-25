#!/usr/bin/env bash
# Prerequisites for ingress-nginx: namespace + the ingress-nginx Helm repo.
set -euo pipefail

# `kubectl apply -f-` sends JSON via the dynamic client; `kubectl create
# namespace` would use protobuf, which r8s doesn't decode for Namespace.
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: ingress-nginx
EOF

helm repo list 2>/dev/null | grep -q '^ingress-nginx\b' \
    || helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update ingress-nginx
