#!/usr/bin/env bash
# Prerequisites for postgresql: namespace + the Bitnami Helm repo.
#
# This suite is the storage workhorse: unlike cert-manager/ingress-nginx/
# metrics-server (all stateless), bitnami/postgresql is a StatefulSet with a
# volumeClaimTemplate, so it forces the whole PVC -> StorageClass -> dynamic
# PV -> bind -> mount path into existence. It's also r8s's own headline use
# case (the README's r8s.toml example declares bitnami/postgresql).
set -euo pipefail

# `kubectl apply -f-` sends JSON via the dynamic client; `kubectl create
# namespace` would use protobuf, which r8s doesn't decode for Namespace.
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: postgresql
EOF

helm repo list 2>/dev/null | grep -q '^bitnami\b' \
    || helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update bitnami
