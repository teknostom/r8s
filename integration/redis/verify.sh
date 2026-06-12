#!/usr/bin/env bash
# Wait for all three redis pods to be Ready and confirm every per-ordinal PVC
# bound. `helm install --wait` covers readiness; the PVC loop is the
# storage-specific assertion — postgresql proved one volumeClaimTemplate PVC,
# this is the first time multiple ordinals each need their own.
set -euo pipefail

NS=redis

kubectl -n "$NS" wait \
    --for=condition=Ready pod \
    --selector app.kubernetes.io/name=redis \
    --timeout=3m

# volumeClaimTemplate is named `redis-data`; per-pod PVC is `<tmpl>-<sts>-<ordinal>`.
for pvc in redis-data-redis-master-0 redis-data-redis-replicas-0 redis-data-redis-replicas-1; do
    phase=$(kubectl -n "$NS" get pvc "$pvc" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    if [[ "$phase" != "Bound" ]]; then
        echo "FAIL: PVC $pvc is '${phase:-missing}', expected Bound"
        echo "--- pvcs ---"; kubectl -n "$NS" get pvc 2>&1 || true
        echo "--- pvs ---";  kubectl get pv 2>&1 || true
        exit 1
    fi
done

echo "OK: 3 redis pods Ready, 3 PVCs Bound"
