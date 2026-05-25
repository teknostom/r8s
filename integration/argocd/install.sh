#!/usr/bin/env bash
# Install Argo CD via the official Helm chart (argoproj.io CRDs included).
set -euo pipefail

# dex.enabled=false — the SSO/Dex server is irrelevant to the GitOps path under
# test and just adds a pod to wait on. Everything else is left at defaults on
# purpose: this installs the application-controller (a StatefulSet),
# repo-server, redis, the server, and the argoproj.io CRDs, so the suite covers
# a real multi-controller + CRD operator workload rather than a trimmed-down one.
helm install argocd argo/argo-cd \
    --namespace argocd \
    --set dex.enabled=false \
    --wait --timeout 5m
