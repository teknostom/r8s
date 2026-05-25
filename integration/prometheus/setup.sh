#!/usr/bin/env bash
# Prerequisites for kube-prometheus-stack: namespace + prometheus-community repo.
#
# This is the densest chart in the suite. It installs the Prometheus Operator,
# which then *generates* a Prometheus StatefulSet from a Prometheus custom
# resource, discovers scrape targets via ServiceMonitor CRs across the cluster,
# and scrapes pod `/metrics` over the bridge. So it exercises, in one shot:
# large CRDs, an operator that creates workloads from CRs, cross-resource
# service discovery (needs the pod's projected ServiceAccount token + API
# access), and the actual scrape data path.
set -euo pipefail

# `kubectl apply -f-` sends JSON via the dynamic client; `kubectl create
# namespace` would use protobuf, which r8s doesn't decode for Namespace.
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: monitoring
EOF

helm repo list 2>/dev/null | grep -q '^prometheus-community\b' \
    || helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update prometheus-community
