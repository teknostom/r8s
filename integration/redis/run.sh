#!/usr/bin/env bash
# End-to-end test of headless-Service DNS and StatefulSet peer discovery —
# the blindspot this suite exists to map. Three layered assertions, each one
# meaningless without the previous:
#   1. The DNS records themselves: from inside a pod, the per-pod name
#      resolves to exactly the master's pod IP, and the headless Service
#      name returns every backing pod.
#   2. The topology those records enable: each replica reports
#      master_link_status:up against the master's DNS *name* (not an IP),
#      and the master counts both replicas.
#   3. The data path over that topology: a key SET on the master is
#      readable from both replicas.
set -euo pipefail

NS=redis
PW=r8s-integration   # must match auth.password in install.sh
HEADLESS=redis-headless.${NS}.svc.cluster.local
MASTER_FQDN=redis-master-0.${HEADLESS}
MARKER="r8s-$(date +%s)-${RANDOM}"   # unique per run: stale PVC data can't false-pass
REPLICAS=(redis-replicas-0 redis-replicas-1)

redis_cli() {
    local pod="$1"; shift
    kubectl -n "$NS" exec "$pod" -- env REDISCLI_AUTH="$PW" redis-cli "$@"
}

diag() {
    echo "--- endpoints (redis-headless) ---"
    kubectl -n "$NS" get endpoints redis-headless -o yaml 2>&1 | head -40 || true
    for pod in redis-master-0 "${REPLICAS[@]}"; do
        echo "--- INFO replication ($pod) ---"
        redis_cli "$pod" INFO replication 2>&1 | tr -d '\r' || true
    done
}

# 1a. Per-pod record: resolve the master's stable DNS name from a replica and
#     compare against the pod IP the apiserver reports. Exact match or bust.
master_ip=$(kubectl -n "$NS" get pod redis-master-0 -o jsonpath='{.status.podIP}')
resolved=$(kubectl -n "$NS" exec redis-replicas-0 -- getent hosts "$MASTER_FQDN" \
    | awk '{print $1}' | head -n1)
if [[ -z "$resolved" || "$resolved" != "$master_ip" ]]; then
    echo "FAIL: $MASTER_FQDN resolved to '${resolved:-<nothing>}', expected pod IP $master_ip"
    diag
    exit 1
fi
echo "OK: per-pod DNS record $MASTER_FQDN -> $master_ip"

# 1b. Headless Service record: must return one A record per backing pod
#     (1 master + 2 replicas), not a single virtual IP.
ip_count=$(kubectl -n "$NS" exec redis-replicas-0 -- getent ahosts "$HEADLESS" \
    | awk '{print $1}' | sort -u | wc -l)
if [[ "$ip_count" -ne 3 ]]; then
    echo "FAIL: $HEADLESS returned $ip_count unique IPs, expected 3 (one per pod)"
    kubectl -n "$NS" exec redis-replicas-0 -- getent ahosts "$HEADLESS" 2>&1 || true
    diag
    exit 1
fi
echo "OK: headless DNS record $HEADLESS -> 3 pod IPs"

# 2. Replication link. Replicas were configured (by the chart) with the
#    master's FQDN, so a live link proves they resolved and dialed it. Give
#    the links a moment — pod readiness doesn't gate on the initial sync.
for _ in $(seq 1 30); do
    slaves=$(redis_cli redis-master-0 INFO replication | tr -d '\r' \
        | awk -F: '/^connected_slaves:/{print $2}')
    [[ "${slaves:-0}" -ge 2 ]] && break
    sleep 3
done
if [[ "${slaves:-0}" -lt 2 ]]; then
    echo "FAIL: master sees connected_slaves=${slaves:-0} after 90s, expected 2"
    diag
    exit 1
fi
for pod in "${REPLICAS[@]}"; do
    info=$(redis_cli "$pod" INFO replication | tr -d '\r')
    host=$(awk -F: '/^master_host:/{print $2}' <<<"$info")
    link=$(awk -F: '/^master_link_status:/{print $2}' <<<"$info")
    if [[ "$host" != "$MASTER_FQDN" || "$link" != "up" ]]; then
        echo "FAIL: $pod master_host='$host' link='$link', expected '$MASTER_FQDN' / 'up'"
        diag
        exit 1
    fi
done
echo "OK: master reports 2 replicas; both link to $MASTER_FQDN"

# 3. Data actually flows master -> replicas.
out=$(redis_cli redis-master-0 SET r8s:integration "$MARKER")
if [[ "$out" != "OK" ]]; then
    echo "FAIL: SET on master returned '$out'"
    diag
    exit 1
fi
for pod in "${REPLICAS[@]}"; do
    got=""
    for _ in $(seq 1 20); do
        got=$(redis_cli "$pod" GET r8s:integration | tr -d '\r')
        [[ "$got" == "$MARKER" ]] && break
        sleep 2
    done
    if [[ "$got" != "$MARKER" ]]; then
        echo "FAIL: $pod GET returned '${got:-<empty>}', expected '$MARKER'"
        diag
        exit 1
    fi
done
echo "OK: key written on master read back from both replicas"
