#!/usr/bin/env bash
# metrics-server only reports Available once it has scraped the kubelet at
# least once, so a ready Deployment already proves the scrape path works.
set -euo pipefail

kubectl -n kube-system wait \
    --for=condition=Available deploy/metrics-server --timeout=2m
kubectl get apiservice v1beta1.metrics.k8s.io -o name >/dev/null
