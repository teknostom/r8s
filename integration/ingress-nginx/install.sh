#!/usr/bin/env bash
# Install ingress-nginx via the official Helm chart.
set -euo pipefail

# Notes on the flags:
#   * service.type=ClusterIP — r8s has no NodePort/LoadBalancer data-path, and
#     the test reaches the controller pod directly over the r8s bridge anyway.
#   * admissionWebhooks.enabled=false — its validating webhook is redundant with
#     cert-manager's (already covered) and its certgen-job cert flow is extra
#     surface. Re-enabling it is a planned follow-up increment.
helm install ingress-nginx ingress-nginx/ingress-nginx \
    --namespace ingress-nginx \
    --set controller.service.type=ClusterIP \
    --set controller.admissionWebhooks.enabled=false \
    --wait --timeout 3m
