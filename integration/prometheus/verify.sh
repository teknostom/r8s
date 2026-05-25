#!/usr/bin/env bash
# Wait for the operator and the operator-generated Prometheus to be Ready.
#
# `helm install --wait` only covers the chart's own resources (the operator
# Deployment + kube-state-metrics). The Prometheus StatefulSet is created by the
# operator from the Prometheus CR *after* install, so we wait for it here — and
# its readiness is itself a signal that r8s accepted the (very large) CRDs and
# the operator could reconcile a CR into a workload.
set -euo pipefail

kubectl -n monitoring wait \
    --for=condition=Available deploy \
    --selector app.kubernetes.io/name=kube-prometheus-stack-prometheus-operator \
    --timeout=3m

# The Prometheus CRD must be registered for the operator to have done anything.
kubectl get crd prometheuses.monitoring.coreos.com -o name >/dev/null

# Wait for the operator-generated Prometheus pod (prometheus-<release>-...-0).
kubectl -n monitoring wait \
    --for=condition=Ready pod \
    --selector app.kubernetes.io/name=prometheus \
    --timeout=3m
