#!/usr/bin/env bash
# cert-manager installs cluster-scoped CRDs (crds.enabled=true); helm uninstall
# leaves them behind. Remove so a re-run starts from a clean slate.
set -uo pipefail

kubectl get crd -o name 2>/dev/null \
    | grep -E '(^|/)(.*\.)?(cert-manager\.io|acme\.cert-manager\.io)$' \
    | xargs -r kubectl delete --ignore-not-found --wait=false 2>/dev/null || true
