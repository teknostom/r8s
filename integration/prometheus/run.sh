#!/usr/bin/env bash
# End-to-end test: Prometheus must actually *scrape* kube-state-metrics. That
# single fact proves the whole chain worked — the operator turned the Prometheus
# CR into a running server, that server loaded the ServiceMonitor-derived scrape
# config, performed in-cluster service discovery (which needs its projected
# ServiceAccount token + API access), reached the kube-state-metrics pod over
# the bridge, and got a 200. We assert via Prometheus's own API: the `up` series
# for the kube-state-metrics job equals 1.
set -euo pipefail

NS=monitoring

IP=$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=prometheus \
    -o jsonpath='{.items[0].status.podIP}')
echo "prometheus pod IP: ${IP}"
PROM="http://${IP}:9090"

# Wait for Prometheus to be serving.
for _ in $(seq 1 30); do
    curl -sf --max-time 5 "${PROM}/-/ready" >/dev/null 2>&1 && break
    sleep 2
done

# Then wait for the kube-state-metrics target to come up (discovery + a scrape
# interval or two). `up{job="kube-state-metrics"}` is 1 when the scrape succeeds.
for _ in $(seq 1 30); do
    resp=$(curl -sG --max-time 5 "${PROM}/api/v1/query" \
        --data-urlencode 'query=up{job="kube-state-metrics"}' 2>/dev/null || true)
    if printf '%s' "$resp" | grep -q ',"1"\]'; then
        echo "OK: Prometheus scraped kube-state-metrics (up == 1)"
        exit 0
    fi
    sleep 5
done

echo "FAIL: kube-state-metrics never reported up after ~2.5m"
echo "last query response: ${resp:-<empty>}"
echo "--- discovered targets (health + lastError) ---"
curl -sG --max-time 5 "${PROM}/api/v1/targets" 2>&1 | head -c 1500 || true
echo ""
echo "--- prometheus logs (tail) ---"
kubectl -n "$NS" logs -l app.kubernetes.io/name=prometheus -c prometheus --tail=40 2>&1 | tail -40 || true
echo "--- operator logs (tail) ---"
kubectl -n "$NS" logs -l app.kubernetes.io/name=kube-prometheus-stack-prometheus-operator --tail=20 2>&1 | tail -20 || true
exit 1
