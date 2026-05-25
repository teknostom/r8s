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
#   * prometheusOperator.admissionWebhooks disabled — webhook+certgen surface is
#     already covered by cert-manager; skip the extra cert Job here.
# kube-state-metrics stays enabled: it's a real Deployment that Prometheus must
# discover and scrape, which is the actual assertion in run.sh.
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
    --set prometheusOperator.admissionWebhooks.enabled=false \
    --set prometheusOperator.tls.enabled=false \
    --wait --timeout 5m
