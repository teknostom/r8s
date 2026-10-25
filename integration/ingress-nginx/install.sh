#!/usr/bin/env bash
# Install ingress-nginx via the official Helm chart.
set -euo pipefail

# Notes on the flags:
#   * service.type=ClusterIP — r8s has no NodePort/LoadBalancer data-path, and
#     the test reaches the controller pod directly over the r8s bridge anyway.
# admissionWebhooks stay at their default (ON): the kube-webhook-certgen
# helm-hook Jobs mint the cert, and the apiserver must then dispatch the
# validating webhook when run.sh creates its Ingress — a webhook on a *core*
# kind, which cert-manager's (CRD-scoped) webhook doesn't cover.
helm install ingress-nginx ingress-nginx/ingress-nginx \
    --namespace ingress-nginx \
    --set controller.service.type=ClusterIP \
    --wait --timeout 3m
