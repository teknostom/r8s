#!/usr/bin/env bash
# Run every integration suite in `integration/*/`.
#
# Each suite is a directory with four required scripts run in order:
#   setup.sh   → prerequisites (namespaces, CRDs, registry creds, etc.)
#   install.sh → helm install / kubectl apply / whatever brings the chart up
#   verify.sh  → wait for the chart to settle (pods Ready, webhooks reachable)
#   run.sh     → the actual workload test (issue a cert, sync an app, etc.)
#
# After each suite we tear it back down so charts don't accumulate on the single
# node — left running, the heavy charts (argocd, prometheus) saturate the runner
# and later suites time out ("context deadline exceeded"). Teardown is:
#   1. `helm uninstall` every release the suite added (owner-ref GC then drops
#      the ReplicaSets/Pods, and the kubelet reaps the containers),
#   2. an optional per-suite `delete.sh` for stragglers helm leaves behind
#      (cluster-scoped CRDs, retained StatefulSet PVCs/PVs),
#   3. delete the suite's namespaces (system namespaces are protected),
#   4. wait for the workload pods to actually drain.
#
# Cleanup runs whether the suite passed or failed, so one failure can't pile its
# charts onto the next. A suite failure is recorded and the run continues.

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAILED=()

# Namespaces we must never delete during cleanup.
PROTECTED_NS=" kube-system kube-public kube-node-lease default "

# Releases that already existed before we started (normally none on CI). Only
# releases that appear *after* this snapshot are ours to uninstall.
BASELINE_RELEASES="$(helm ls -A 2>/dev/null | awk 'NR>1{print $1}' | sort -u)"

drain_pods() {
    # Wait until no pods remain outside the protected/system namespaces.
    for _ in $(seq 1 60); do
        local left
        left="$(kubectl get pods -A --no-headers 2>/dev/null \
            | awk '{print $1}' \
            | grep -vxE 'kube-system|kube-public|kube-node-lease|default' \
            | wc -l)"
        [[ "${left:-0}" -eq 0 ]] && return 0
        sleep 2
    done
    echo "  cleanup: WARN pods still draining after 2m"
    kubectl get pods -A 2>/dev/null || true
}

cleanup_suite() {
    local chart_dir="$1" name="$2"
    echo "  ---- cleanup: $name ----"

    # 1. Uninstall every helm release that appeared during this suite. This
    #    also removes the cluster-scoped objects helm owns (ClusterRoles,
    #    webhooks, APIServices) that a namespace delete wouldn't reach.
    while read -r rel ns; do
        [[ -z "$rel" ]] && continue
        grep -qxF "$rel" <<<"$BASELINE_RELEASES" && continue
        echo "  cleanup: helm uninstall $rel -n $ns"
        helm uninstall "$rel" -n "$ns" --wait --timeout 3m || true
    done < <(helm ls -A 2>/dev/null | awk 'NR>1{print $1" "$2}')

    # 2. Suite-specific stragglers helm doesn't remove (cluster-scoped CRDs,
    #    retained PVs).
    if [[ -f "$chart_dir/delete.sh" ]]; then
        echo "  cleanup: $name delete.sh (stragglers)"
        bash "$chart_dir/delete.sh" || true
    fi

    # 3. Delete every non-system namespace — the suite's own namespaces plus any
    #    the workload test created (e.g. argocd's "guestbook", the ingress
    #    "echo" namespace), which helm knows nothing about. Namespace deletion
    #    cascades, so this also drains the pods still inside them.
    for ns in $(kubectl get ns -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
        [[ "$PROTECTED_NS" == *" $ns "* ]] && continue
        kubectl delete namespace "$ns" --wait=false --ignore-not-found >/dev/null 2>&1 || true
    done

    # 4. Make sure the workload pods are actually gone before the next suite.
    drain_pods
}

for chart_dir in "$ROOT_DIR"/*/; do
    [[ -d "$chart_dir" ]] || continue
    name="$(basename "$chart_dir")"
    echo ""
    echo "================================================================"
    echo "  $name"
    echo "================================================================"
    if bash "$chart_dir/setup.sh"   && \
       bash "$chart_dir/install.sh" && \
       bash "$chart_dir/verify.sh"  && \
       bash "$chart_dir/run.sh"; then
        echo "PASS: $name"
    else
        echo "FAIL: $name"
        FAILED+=("$name")
        # Capture state in the CI log before cleanup wipes it.
        echo "  ---- diagnostics: $name ----"
        kubectl get pods -A -o wide 2>/dev/null || true
        kubectl get events -A 2>/dev/null | tail -n 30 || true
    fi
    cleanup_suite "$chart_dir" "$name"
done

echo ""
echo "================================================================"
if [[ ${#FAILED[@]} -eq 0 ]]; then
    echo "All integration suites passed."
    exit 0
else
    echo "Failed: ${FAILED[*]}"
    exit 1
fi
