#!/usr/bin/env bash
# Prerequisites for Argo CD: namespace + the argo Helm repo.
#
# Argo CD is the operator-pattern workhorse of the suite: a CRD-driven
# controller set (the application-controller is a StatefulSet) that clones Git
# repos and applies manifests against the in-cluster API. It exercises CRDs,
# multiple controllers, a StatefulSet, Redis, and the full GitOps reconcile
# loop in one chart — and it's something people genuinely expect to work.
set -euo pipefail

# `kubectl apply -f-` sends JSON via the dynamic client; `kubectl create
# namespace` would use protobuf, which r8s doesn't decode for Namespace.
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: argocd
EOF

helm repo list 2>/dev/null | grep -q '^argo\b' \
    || helm repo add argo https://argoproj.github.io/argo-helm
helm repo update argo
