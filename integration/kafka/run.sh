#!/usr/bin/env bash
# End-to-end test of the kafka client protocol against r8s networking. A
# client pod bootstraps via the ClusterIP Service; the broker's metadata
# answer redirects it to the advertised per-pod listener
# (`kafka-controller-0.kafka-controller-headless...`), which the client must
# resolve and dial to do anything. The assertions stack:
#   1. The metadata answer really advertises the per-pod FQDN.
#   2. Topic create + produce + consume round-trip — which can only pass if
#      the client followed that redirect, authenticated over SASL, and the
#      broker persisted to its PVC.
set -euo pipefail

NS=kafka
BOOTSTRAP=kafka.${NS}.svc.cluster.local:9092
ADVERTISED=kafka-controller-0.kafka-controller-headless.${NS}.svc.cluster.local
TOPIC="r8s-test-$(date +%s)"   # unique per run: retained PVC data can't false-pass
CLIENT=kafka-client

kexec() {
    kubectl -n "$NS" exec "$CLIENT" -- "$@"
}

diag() {
    echo "--- kafka-controller-0 logs (tail) ---"
    kubectl -n "$NS" logs kafka-controller-0 --tail=40 2>&1 || true
    echo "--- endpoints (kafka-controller-headless) ---"
    kubectl -n "$NS" get endpoints kafka-controller-headless -o yaml 2>&1 | head -30 || true
    echo "--- DNS from client ---"
    kexec getent hosts "$ADVERTISED" 2>&1 || true
}

# Client pod: same image the broker runs (already pulled on the node), kept
# alive with sleep so we can exec the console tools repeatedly.
IMG=$(kubectl -n "$NS" get sts kafka-controller \
    -o jsonpath='{.spec.template.spec.containers[0].image}')
kubectl -n "$NS" run "$CLIENT" --image "$IMG" --restart=Never \
    --command -- sleep infinity
kubectl -n "$NS" wait --for=condition=Ready pod/"$CLIENT" --timeout=2m

# SASL client config, exactly as the chart's NOTES.txt prescribes. The
# password must match sasl.client.passwords in install.sh.
kubectl -n "$NS" exec -i "$CLIENT" -- bash -c 'cat > /tmp/client.properties' <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="user1" password="r8s-integration";
EOF

# 1. The broker must advertise itself by per-pod DNS name, not an IP. This
#    call doubles as the SASL smoke test — it fails outright on bad auth.
#    `|| true` on the substitutions: a failed command must fall through to
#    the FAIL branch and its diagnostics, not abort silently via `set -e`.
#    Capture the *full* output, not `| head -n1`: this is a JVM tool that
#    prints ~80 lines and ignores SIGPIPE, so closing the pipe early (head)
#    doesn't stop it — the `kubectl exec` stream never closes and the command
#    substitution hangs indefinitely. The advertised FQDN is in the output
#    regardless, so a substring check is all we need.
versions=$(kexec kafka-broker-api-versions.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config /tmp/client.properties 2>/dev/null) || true
if [[ "$versions" != *"$ADVERTISED"* ]]; then
    echo "FAIL: broker metadata doesn't advertise $ADVERTISED"
    echo "got: ${versions:-<empty>}"
    diag
    exit 1
fi
echo "OK: broker advertises $ADVERTISED"

# 2. Round-trip: create a topic, produce 5 messages, consume them back.
if ! kexec kafka-topics.sh --create --if-not-exists \
    --topic "$TOPIC" --partitions 1 --replication-factor 1 \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config /tmp/client.properties; then
    echo "FAIL: topic create failed"
    diag
    exit 1
fi

kubectl -n "$NS" exec -i "$CLIENT" -- kafka-console-producer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --producer.config /tmp/client.properties <<'EOF'
r8s-msg-1
r8s-msg-2
r8s-msg-3
r8s-msg-4
r8s-msg-5
EOF

got=$(kexec kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --from-beginning --max-messages 5 --timeout-ms 60000 \
    --consumer.config /tmp/client.properties 2>/dev/null | sort) || true
want=$(printf 'r8s-msg-%d\n' 1 2 3 4 5)
if [[ "$got" != "$want" ]]; then
    echo "FAIL: consumed messages don't match produced ones"
    echo "got:  ${got:-<nothing>}"
    echo "want: $want"
    diag
    exit 1
fi
echo "OK: 5 messages produced and consumed through the advertised listener"
