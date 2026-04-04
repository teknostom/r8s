#!/usr/bin/env bash
set -euo pipefail

# Isolated conformance test runner for r8s.
# Spins up a fresh r8sd for each individual test, runs it, tears down.
# This avoids cascading failures from accumulated server state.
#
# Usage:
#   sudo ./scripts/conformance-isolated.sh              # sig-api-machinery (default)
#   sudo ./scripts/conformance-isolated.sh api           # same
#   sudo ./scripts/conformance-isolated.sh apps          # sig-apps
#   sudo ./scripts/conformance-isolated.sh node          # sig-node
#   sudo ./scripts/conformance-isolated.sh all           # full conformance
#   FOCUS="regex" sudo ./scripts/conformance-isolated.sh custom

TIER="${1:-api}"
R8SD="${R8SD:-./target/release/r8sd}"
E2E="${E2E:-./conformance-results/e2e.test}"
RESULTS_DIR="${RESULTS_DIR:-./conformance-results}"
RESULTS_FILE="$RESULTS_DIR/isolated-results.txt"
PER_TEST_TIMEOUT="${PER_TEST_TIMEOUT:-120s}"

# Tier → focus regex
case "$TIER" in
    api)   FOCUS='\[sig-api-machinery\].*\[Conformance\]' ;;
    apps)  FOCUS='\[sig-apps\].*\[Conformance\]' ;;
    node)  FOCUS='\[sig-node\].*\[Conformance\]' ;;
    network) FOCUS='\[sig-network\].*\[Conformance\]' ;;
    storage) FOCUS='\[sig-storage\].*\[Conformance\]' ;;
    all)   FOCUS='\[Conformance\]' ;;
    custom) FOCUS="${FOCUS:?Set FOCUS env var for custom tier}" ;;
    *)     echo "Unknown tier: $TIER"; echo "Usage: $0 {api|apps|node|network|storage|all|custom}"; exit 1 ;;
esac

# Preflight checks
if [[ $EUID -ne 0 ]]; then
    echo "Error: must run as root (r8sd needs network setup)"
    exit 1
fi
if [[ ! -x "$R8SD" ]]; then
    echo "Error: r8sd not found at $R8SD (run: cargo build --release)"
    exit 1
fi
if [[ ! -x "$E2E" ]]; then
    echo "Error: e2e.test not found at $E2E"
    exit 1
fi

mkdir -p "$RESULTS_DIR"

# --- Step 1: Extract test names via dry-run ---
echo "Extracting test names for tier '$TIER'..."
TESTS=()
while IFS= read -r line; do
    # Strip trailing Ginkgo v2 labels like " [sig-api-machinery, Conformance]"
    # These are comma-separated labels in brackets at the end, not part of the matchable spec text.
    clean=$(echo "$line" | sed 's/ \[[a-zA-Z-]*, [A-Za-z]*\]$//')
    TESTS+=("$clean")
done < <("$E2E" --ginkgo.dry-run --ginkgo.v --ginkgo.no-color \
    --ginkgo.focus="$FOCUS" --kubeconfig=/dev/null 2>&1 \
    | grep '^\[sig-\|^\[Conformance\]' || true)

TOTAL=${#TESTS[@]}
if [[ $TOTAL -eq 0 ]]; then
    echo "No tests found matching focus: $FOCUS"
    exit 1
fi
echo "Found $TOTAL tests"
echo ""

# --- Helper: escape regex special characters for ginkgo --focus ---
escape_regex() {
    printf '%s' "$1" | sed 's/[][().*+?{}|^$\\]/\\&/g'
}

# --- Helper: write a minimal kubeconfig ---
write_kubeconfig() {
    cat > "$1" <<'KC'
apiVersion: v1
kind: Config
clusters:
- cluster:
    server: http://127.0.0.1:6443
  name: r8s-test
contexts:
- context:
    cluster: r8s-test
    user: r8s-admin
  name: r8s-test
current-context: r8s-test
users:
- name: r8s-admin
  user: {}
KC
}

# --- Helper: wait for TCP port ---
wait_for_port() {
    local deadline=$((SECONDS + 10))
    while [[ $SECONDS -lt $deadline ]]; do
        if bash -c "echo >/dev/tcp/127.0.0.1/6443" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

# --- Helper: kill r8sd cleanly ---
kill_r8sd() {
    local pid="$1"
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        local deadline=$((SECONDS + 10))
        while kill -0 "$pid" 2>/dev/null && [[ $SECONDS -lt $deadline ]]; do
            sleep 0.1
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    fi
}

# --- Helper: kill any stale r8sd processes on port 6443 ---
kill_stale_r8sd() {
    local stale_pids
    stale_pids=$(ss -tlnp 2>/dev/null | grep ':6443 ' | grep -oP 'pid=\K[0-9]+' || true)
    if [[ -n "$stale_pids" ]]; then
        echo "Killing stale r8sd on port 6443 (pids: $stale_pids)..."
        for pid in $stale_pids; do
            kill -9 "$pid" 2>/dev/null || true
        done
        sleep 0.5
    fi
}

# --- Run tests ---
PASSED=0
FAILED=0
ERRORS=0
FAILED_NAMES=()

: > "$RESULTS_FILE"

# Kill any stale r8sd before starting
kill_stale_r8sd

for i in "${!TESTS[@]}"; do
    TEST_NAME="${TESTS[$i]}"
    N=$((i + 1))
    TMPDIR=$(mktemp -d /tmp/r8s-iso-$$-XXXX)
    KUBECONFIG="$TMPDIR/kubeconfig"

    write_kubeconfig "$KUBECONFIG"

    # Ensure port 6443 is free before starting
    kill_stale_r8sd

    # Start r8sd
    "$R8SD" --data-dir "$TMPDIR" > "$TMPDIR/r8sd.log" 2>&1 &
    R8SD_PID=$!

    # Wait for ready
    if ! wait_for_port; then
        ERRORS=$((ERRORS + 1))
        FAILED_NAMES+=("$TEST_NAME")
        echo "ERRO [$N/$TOTAL] [pass:$PASSED fail:$((FAILED + ERRORS))] r8sd failed to start: $TEST_NAME"
        echo "ERRO $TEST_NAME" >> "$RESULTS_FILE"
        kill_r8sd "$R8SD_PID"
        rm -rf "$TMPDIR"
        continue
    fi

    # Run the single test
    START_TIME=$SECONDS
    ESCAPED=$(escape_regex "$TEST_NAME")

    if "$E2E" \
        --kubeconfig="$KUBECONFIG" \
        --ginkgo.focus="${ESCAPED}" \
        --ginkgo.no-color \
        --ginkgo.timeout="$PER_TEST_TIMEOUT" \
        > "$TMPDIR/e2e.log" 2>&1; then
        ELAPSED=$((SECONDS - START_TIME))
        PASSED=$((PASSED + 1))
        echo "PASS [$N/$TOTAL] (${ELAPSED}s) [pass:$PASSED fail:$((FAILED + ERRORS))] $TEST_NAME"
        echo "PASS $TEST_NAME" >> "$RESULTS_FILE"
    else
        ELAPSED=$((SECONDS - START_TIME))
        FAILED=$((FAILED + 1))
        FAILED_NAMES+=("$TEST_NAME")
        echo "FAIL [$N/$TOTAL] (${ELAPSED}s) [pass:$PASSED fail:$((FAILED + ERRORS))] $TEST_NAME"
        echo "FAIL $TEST_NAME" >> "$RESULTS_FILE"
        # Preserve failure logs
        FAIL_DIR="$RESULTS_DIR/failure-logs"
        mkdir -p "$FAIL_DIR"
        cp "$TMPDIR/e2e.log" "$FAIL_DIR/test-${N}-e2e.log" 2>/dev/null || true
        cp "$TMPDIR/r8sd.log" "$FAIL_DIR/test-${N}-r8sd.log" 2>/dev/null || true
        # Preserve container logs
        for logfile in "$TMPDIR"/logs/*.log; do
            [[ -s "$logfile" ]] && cp "$logfile" "$FAIL_DIR/test-${N}-$(basename "$logfile")" 2>/dev/null || true
        done
    fi

    # Tear down
    kill_r8sd "$R8SD_PID"
    rm -rf "$TMPDIR"
done

# --- Summary ---
echo ""
echo "=============================="
echo "  Conformance Results ($TIER)"
echo "=============================="
echo "Total: $TOTAL | Passed: $PASSED | Failed: $((FAILED + ERRORS))"
if [[ ${#FAILED_NAMES[@]} -gt 0 ]]; then
    echo ""
    echo "Failed tests:"
    for name in "${FAILED_NAMES[@]}"; do
        echo "  - $name"
    done
fi
echo ""
echo "Results written to: $RESULTS_FILE"
