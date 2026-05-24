#!/usr/bin/env bash
# Run every integration suite in `integration/*/`.
#
# Each suite is a directory with four scripts run in order:
#   setup.sh  → prerequisites (namespaces, CRDs, registry creds, etc.)
#   install.sh → helm install / kubectl apply / whatever brings the chart up
#   verify.sh → wait for the chart to settle (pods Ready, webhooks reachable)
#   run.sh     → the actual workload test (issue a cert, sync an app, etc.)
#
# Any non-zero exit aborts the whole run. Cleanup is `sudo r8s down` — we
# don't try to undo per-chart state because the cluster is throwaway.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAILED=()

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
    fi
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
