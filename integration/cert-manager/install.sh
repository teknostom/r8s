#!/usr/bin/env bash
# Install cert-manager via the official Helm chart, CRDs included.
set -euo pipefail

helm install cert-manager jetstack/cert-manager \
    --namespace cert-manager \
    --set crds.enabled=true \
    --wait --timeout 3m
