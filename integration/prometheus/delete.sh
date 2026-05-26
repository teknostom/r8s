#!/usr/bin/env bash
# kube-prometheus-stack installs the monitoring.coreos.com CRDs (Prometheus,
# ServiceMonitor, …); helm uninstall leaves them. Remove the stragglers.
set -uo pipefail

kubectl get crd -o name 2>/dev/null \
    | grep 'monitoring\.coreos\.com$' \
    | xargs -r kubectl delete --ignore-not-found --wait=false 2>/dev/null || true
