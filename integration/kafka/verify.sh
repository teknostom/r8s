#!/usr/bin/env bash
# Wait for the kafka controller pod to be Ready and confirm its PVC bound.
# Readiness here is already a strong signal: a KRaft node only turns Ready
# after it has resolved its own per-pod record (quorum bootstrap through the
# publishNotReadyAddresses headless Service) and opened the listeners.
set -euo pipefail

NS=kafka
POD=kafka-controller-0
# volumeClaimTemplate is named `data`; per-pod PVC is `<tmpl>-<sts>-<ordinal>`.
PVC=data-kafka-controller-0

kubectl -n "$NS" wait \
    --for=condition=Ready pod/"$POD" \
    --timeout=3m

phase=$(kubectl -n "$NS" get pvc "$PVC" -o jsonpath='{.status.phase}' 2>/dev/null || true)
if [[ "$phase" != "Bound" ]]; then
    echo "FAIL: PVC $PVC is '${phase:-missing}', expected Bound"
    echo "--- pvcs ---"; kubectl -n "$NS" get pvc 2>&1 || true
    echo "--- pvs ---";  kubectl get pv 2>&1 || true
    exit 1
fi

echo "OK: pod $POD Ready and PVC $PVC Bound"
