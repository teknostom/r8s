#!/usr/bin/env bash
# Install kafka via the Bitnami Helm chart, single-node KRaft.
set -euo pipefail

# Notes on the flags:
#   * controller.replicaCount=1 — one combined controller+broker (KRaft
#     quorum of one). Multi-broker replication re-tests what redis already
#     proves, at 3x the JVM weight; the per-pod DNS path is exercised either
#     way because the advertised listeners and quorum voters use per-pod
#     FQDNs even with a single node.
#   * controller.heapOpts — the chart default is -XX:MaxRAMPercentage=75,
#     which sizes the heap from the cgroup memory limit. r8s doesn't write
#     resource limits into the container spec yet, so the JVM would see the
#     *host's* RAM and size for 75% of that. Pin the heap until cgroup
#     limits are enforced; drop this flag when they land.
#   * controller.persistence.size=2Gi — default 8Gi; KRaft metadata plus a
#     handful of test messages.
#   * sasl.client.passwords — fixed so run.sh can authenticate
#     deterministically (the chart defaults all listeners to SASL_PLAINTEXT
#     and random-generates passwords otherwise). Auth stays at chart
#     defaults on purpose.
#   * image.repository=bitnamilegacy/kafka — as of 2025-08-28 Broadcom moved
#     every versioned (non-`latest`) Bitnami image out of docker.io/bitnami
#     into the read-only archive docker.io/bitnamilegacy. The chart still pins
#     a versioned tag (4.0.0-debian-12-r10), so a *cold* pull from the default
#     repo 404s ("NotFound: ... not found"); the legacy repo still serves the
#     identical digest. allowInsecureImages=true is required because the chart
#     runs a preflight that blocks any image not in the official catalog.
#     Drop both when we move off Bitnami legacy images.
#   * overrideConfiguration replication factors — Kafka defaults the internal
#     __consumer_offsets and transaction-state topics to RF=3, which a single
#     broker can never satisfy: creation loops forever ("Sent auto-creation
#     request for Set(__consumer_offsets) ...") and any consumer group hangs,
#     so the console consumer reads nothing. Pin them to 1 for the single-node
#     topology. This isn't a chart opt-out — a 1-broker cluster genuinely
#     cannot host RF=3 internal topics; multi-broker would drop these.
# NetworkPolicy/PodDisruptionBudget defaults stay ON (registered kinds).
# Timeout is generous: the first run does a cold pull of the (large) kafka
# image plus a KRaft storage format before the controller turns Ready.
helm install kafka bitnami/kafka \
    --namespace kafka \
    --set global.security.allowInsecureImages=true \
    --set image.repository=bitnamilegacy/kafka \
    --set controller.replicaCount=1 \
    --set controller.heapOpts="-Xmx512m -Xms512m" \
    --set controller.persistence.size=2Gi \
    --set sasl.client.passwords=r8s-integration \
    --set "overrideConfiguration.offsets\.topic\.replication\.factor=1" \
    --set "overrideConfiguration.transaction\.state\.log\.replication\.factor=1" \
    --set "overrideConfiguration.transaction\.state\.log\.min\.isr=1" \
    --wait --timeout 10m
