#!/usr/bin/env bash
# The IngressClass is cluster-scoped and retained on helm uninstall.
set -uo pipefail

kubectl delete ingressclass nginx --ignore-not-found --wait=false 2>/dev/null || true
