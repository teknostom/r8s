#!/usr/bin/env bash
# Wait for the postgresql StatefulSet pod to be Ready and confirm its PVC
# actually bound. `helm install --wait` covers pod readiness, but the PVC
# Bound check is the storage-specific assertion: it's the first place a
# missing provisioner / unhandled volumeClaimTemplate shows up.
set -euo pipefail

NS=postgresql
POD=postgresql-0
# volumeClaimTemplate is named `data`; per-pod PVC is `data-<sts>-<ordinal>`.
PVC=data-postgresql-0

kubectl -n "$NS" wait \
    --for=condition=Ready pod/"$POD" \
    --timeout=3m

phase=$(kubectl -n "$NS" get pvc "$PVC" -o jsonpath='{.status.phase}' 2>/dev/null || true)
if [[ "$phase" != "Bound" ]]; then
    echo "FAIL: PVC $PVC is '${phase:-missing}', expected Bound"
    echo "--- pvcs ---"; kubectl -n "$NS" get pvc 2>&1 || true
    echo "--- pvs ---";  kubectl get pv 2>&1 || true
    echo "--- storageclasses ---"; kubectl get storageclass 2>&1 || true
    exit 1
fi

echo "OK: pod $POD Ready and PVC $PVC Bound"
