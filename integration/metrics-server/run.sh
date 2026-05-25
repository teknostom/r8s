#!/usr/bin/env bash
# End-to-end test: `kubectl top nodes` must return real metrics, which means
# the full path works — metrics-server scraped r8s's kubelet /metrics/resource,
# and the apiserver aggregated `metrics.k8s.io` and proxied the request to it.
set -euo pipefail

for i in $(seq 1 24); do
    if kubectl top nodes 2>/dev/null | grep -q 'r8s-node'; then
        echo "OK: metrics flowing end-to-end:"
        kubectl top nodes
        exit 0
    fi
    sleep 5
done

echo "FAIL: kubectl top nodes returned no metrics after 2m"
echo "--- apiservice condition ---"
kubectl get apiservice v1beta1.metrics.k8s.io -o jsonpath='{.status.conditions}{"\n"}' 2>&1 || true
echo "--- raw aggregated endpoint (tests the proxy directly) ---"
kubectl get --raw "/apis/metrics.k8s.io/v1beta1/nodes" 2>&1 | head -c 600 || true
echo ""
echo "--- metrics-server logs ---"
kubectl -n kube-system logs deploy/metrics-server --tail=30 2>&1 || true
exit 1
