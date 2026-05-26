#!/usr/bin/env bash
# StatefulSet PVCs — and the PVs the r8s provisioner bound to them — are
# retained on `helm uninstall` by design, so the postgres data volume would
# otherwise survive into the next run. Drop both.
set -uo pipefail

kubectl -n postgresql delete pvc --all --ignore-not-found --wait=false 2>/dev/null || true

# PVs bound to this namespace's claims are now orphaned; remove them too.
for pv in $(kubectl get pv \
    -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="postgresql")]}{.metadata.name}{"\n"}{end}' \
    2>/dev/null); do
    kubectl delete pv "$pv" --ignore-not-found --wait=false 2>/dev/null || true
done
