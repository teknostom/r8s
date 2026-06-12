#!/usr/bin/env bash
# Prerequisites for redis: namespace + the Bitnami Helm repo.
#
# This suite is the clustered-StatefulSet workhorse: postgresql proved a
# *single* StatefulSet pod with a PVC, but nothing yet exercises pods finding
# each other. bitnami/redis in replication mode is the lightest real chart
# that does — replicas dial the master by its per-pod headless-Service DNS
# name (`redis-master-0.redis-headless...`), which is the same mechanism
# every clustered chart (kafka, rabbitmq, etcd, mongodb) bootstraps with.
set -euo pipefail

# `kubectl apply -f-` sends JSON via the dynamic client; `kubectl create
# namespace` would use protobuf, which r8s doesn't decode for Namespace.
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: redis
EOF

helm repo list 2>/dev/null | grep -q '^bitnami\b' \
    || helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update bitnami
