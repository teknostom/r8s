#!/usr/bin/env bash
# Install kube-prometheus-stack, scoped to the operator + Prometheus + the one
# scrape target that actually exists on r8s (kube-state-metrics).
set -euo pipefail

# Why each flag — the goal is to isolate "does the operator → Prometheus →
# scrape loop work" and not drown in failures for things r8s doesn't model:
#   * alertmanager/grafana/nodeExporter disabled — orthogonal components; each
#     would add pods (and node-exporter needs hostNetwork) that aren't part of
#     the scrape loop under test.
#   * kube{ControllerManager,Scheduler,Etcd,Proxy}/coreDns disabled — r8s has no
#     such pods, so their default ServiceMonitors would just be permanently-down
#     targets and pure noise.
# kube-state-metrics stays enabled: it's a real Deployment that Prometheus must
# discover and scrape, which is the actual assertion in run.sh.
# prometheusOperator admissionWebhooks/tls stay at their defaults (ON): the
# certgen Job mints the webhook cert and the chart's default PrometheusRules
# must pass through the operator's validating webhook during install.
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
    --namespace monitoring \
    --set alertmanager.enabled=false \
    --set grafana.enabled=false \
    --set nodeExporter.enabled=false \
    --set kubeControllerManager.enabled=false \
    --set kubeScheduler.enabled=false \
    --set kubeEtcd.enabled=false \
    --set kubeProxy.enabled=false \
    --set coreDns.enabled=false \
    --wait --timeout 5m
