#!/usr/bin/env bash
set -euo pipefail

# Kubernetes conformance test runner for r8s
# Extracts e2e.test from the official conformance image and runs it against the r8s API server.
#
# Usage:
#   ./scripts/conformance.sh              # Run default tier (api)
#   ./scripts/conformance.sh api          # API-level tests only (fast, no pods needed)
#   ./scripts/conformance.sh apps         # Apps controller tests (deployments, jobs, etc.)
#   ./scripts/conformance.sh node         # Node/container tests (probes, security context, etc.)
#   ./scripts/conformance.sh all          # Full conformance suite
#   FOCUS="some regex" ./scripts/conformance.sh custom   # Custom focus regex

TIER="${1:-api}"

# Auto-detect kubeconfig: use KUBECONFIG env, or find the first cluster
if [ -z "${KUBECONFIG:-}" ]; then
    KUBECONFIG=$(find /var/lib/r8s/clusters -name kubeconfig 2>/dev/null | head -1 || true)
    if [ -z "$KUBECONFIG" ]; then
        echo "Error: No kubeconfig found. Start a cluster first: sudo r8s up"
        exit 1
    fi
    echo "Auto-detected kubeconfig: $KUBECONFIG"
fi
CONFORMANCE_IMAGE="${CONFORMANCE_IMAGE:-registry.k8s.io/conformance:v1.32.0}"
RESULTS_DIR="${RESULTS_DIR:-./conformance-results}"
TIMEOUT="${TIMEOUT:-1h}"
SPEC_POLL="${SPEC_POLL:-30s}"

mkdir -p "$RESULTS_DIR"

# Extract e2e.test binary from conformance image (cached)
if [ ! -f "$RESULTS_DIR/e2e.test" ]; then
    echo "Extracting e2e.test from $CONFORMANCE_IMAGE..."
    container_id=$(docker create "$CONFORMANCE_IMAGE")
    docker cp "$container_id:/usr/local/bin/e2e.test" "$RESULTS_DIR/e2e.test"
    docker rm "$container_id" > /dev/null
    chmod +x "$RESULTS_DIR/e2e.test"
    echo "Extracted to $RESULTS_DIR/e2e.test"
fi

# Build focus/skip based on tier
case "$TIER" in
    api)
        # sig-api-machinery conformance tests: CRUD, watch, CRDs, namespaces, etc.
        # Fast because they test API operations, not pod lifecycle.
        FOCUS='\[sig-api-machinery\].*\[Conformance\]'
        SKIP=''
        echo "Tier: api (sig-api-machinery conformance tests)"
        ;;
    apps)
        # sig-apps conformance tests: deployments, jobs, statefulsets, daemonsets, RC
        FOCUS='\[sig-apps\].*\[Conformance\]'
        SKIP=''
        echo "Tier: apps (sig-apps conformance tests)"
        ;;
    node)
        # sig-node conformance tests: probes, security context, pods, volumes
        FOCUS='\[sig-node\].*\[Conformance\]'
        SKIP=''
        echo "Tier: node (sig-node conformance tests)"
        ;;
    network)
        # sig-network conformance tests: services, ingress, endpoints
        FOCUS='\[sig-network\].*\[Conformance\]'
        SKIP=''
        echo "Tier: network (sig-network conformance tests)"
        ;;
    storage)
        # sig-storage conformance tests: volumes, configmaps, secrets in volumes
        FOCUS='\[sig-storage\].*\[Conformance\]'
        SKIP=''
        echo "Tier: storage (sig-storage conformance tests)"
        ;;
    all)
        # Full conformance suite
        FOCUS='\[Conformance\]'
        SKIP=''
        TIMEOUT="${TIMEOUT:-3h}"
        echo "Tier: all (full conformance suite)"
        ;;
    custom)
        # Custom focus from env var
        FOCUS="${FOCUS:?Set FOCUS env var for custom tier}"
        SKIP="${SKIP:-}"
        echo "Tier: custom (FOCUS=$FOCUS)"
        ;;
    *)
        echo "Unknown tier: $TIER"
        echo "Usage: $0 {api|apps|node|network|storage|all|custom}"
        exit 1
        ;;
esac

echo ""
echo "Running Kubernetes conformance tests against r8s..."
echo "  KUBECONFIG: $KUBECONFIG"
echo "  Results:    $RESULTS_DIR"
echo "  Suite timeout: $TIMEOUT"
echo "  Spec poll:     $SPEC_POLL"
echo ""

# Build args
ARGS=(
    --kubeconfig="$KUBECONFIG"
    --kube-api-content-type=application/json
    --ginkgo.focus="$FOCUS"
    --ginkgo.no-color
    --ginkgo.v
    --ginkgo.timeout="$TIMEOUT"
    --ginkgo.poll-progress-after="$SPEC_POLL"
    --report-dir="$RESULTS_DIR"
)
if [ -n "${SKIP:-}" ]; then
    ARGS+=(--ginkgo.skip="$SKIP")
fi

"$RESULTS_DIR/e2e.test" "${ARGS[@]}" \
    2>&1 | tee "$RESULTS_DIR/conformance.log" || true

# Summary — parse from Ginkgo output line
echo ""
echo "=== Conformance Results ($TIER) ==="
if [ -f "$RESULTS_DIR/conformance.log" ]; then
    summary=$(grep -oP '\d+ Passed \| \d+ Failed \| \d+ Pending \| \d+ Skipped' "$RESULTS_DIR/conformance.log" | tail -1 || true)
    if [ -n "$summary" ]; then
        passed=$(echo "$summary" | grep -oP '^\d+')
        failed=$(echo "$summary" | grep -oP '(\d+) Failed' | grep -oP '^\d+')
        echo "$summary"
        echo ""
        echo "Conformance ($TIER): $passed passed, $failed failed (of 411 total)"
    else
        echo "Could not parse results from Ginkgo output"
    fi
fi
