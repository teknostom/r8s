#!/usr/bin/env bash
# End-to-end test: prove data survives a pod restart, which is the whole
# reason persistent storage exists. Write a row, delete postgresql-0 so the
# StatefulSet recreates it and re-binds the *same* PVC, then read the row
# back. This exercises kubectl exec, StatefulSet pod recreation, and — the
# part r8s is being tested on — a PVC whose data actually persists.
set -euo pipefail

NS=postgresql
POD=postgresql-0
PW=r8s-integration   # must match auth.postgresPassword in install.sh
MARKER=survived-restart

# Run a SQL statement inside the primary. -tA = tuples-only, unaligned, so
# the output is exactly the value with no header/whitespace noise.
psql_exec() {
    kubectl -n "$NS" exec "$POD" -- \
        env PGPASSWORD="$PW" psql -U postgres -d postgres -tAc "$1"
}

# 1. Write a row.
psql_exec "CREATE TABLE IF NOT EXISTS r8s_persist (id int PRIMARY KEY, msg text);"
psql_exec "INSERT INTO r8s_persist (id, msg) VALUES (1, '${MARKER}')
           ON CONFLICT (id) DO UPDATE SET msg = EXCLUDED.msg;"

# 2. Restart: delete the pod and let the StatefulSet recreate postgresql-0
#    against the same PVC.
kubectl -n "$NS" delete pod "$POD" --wait=true

# kubectl wait errors out if the pod doesn't exist yet, so poll for the
# recreated pod to come back Ready rather than waiting on a single object.
ready=""
for _ in $(seq 1 36); do
    ready=$(kubectl -n "$NS" get pod "$POD" \
        -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
    [[ "$ready" == "True" ]] && break
    sleep 5
done
if [[ "$ready" != "True" ]]; then
    echo "FAIL: $POD did not return to Ready after restart"
    kubectl -n "$NS" get pod "$POD" -o wide 2>&1 || true
    kubectl -n "$NS" describe pod "$POD" 2>&1 | tail -30 || true
    exit 1
fi

# 3. Read the row back. Retry briefly: pg_isready can flip Ready a beat
#    before connections are accepted.
got=""
for _ in $(seq 1 12); do
    got=$(psql_exec "SELECT msg FROM r8s_persist WHERE id = 1;" 2>/dev/null || true)
    [[ -n "$got" ]] && break
    sleep 5
done

if [[ "$got" == "$MARKER" ]]; then
    echo "OK: row survived pod restart (read back: '${got}')"
    exit 0
fi

echo "FAIL: expected '${MARKER}' after restart, got '${got:-<empty>}'"
echo "--- pvc ---"; kubectl -n "$NS" get pvc 2>&1 || true
echo "--- postgresql logs ---"; kubectl -n "$NS" logs "$POD" --tail=30 2>&1 || true
exit 1
