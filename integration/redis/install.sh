#!/usr/bin/env bash
# Install redis via the Bitnami Helm chart in replication mode.
set -euo pipefail

# Notes on the flags:
#   * architecture=replication — the point of the suite. One `redis-master`
#     StatefulSet plus a `redis-replicas` StatefulSet whose pods connect to
#     `redis-master-0.redis-headless.redis.svc.cluster.local` — a per-pod
#     headless-Service DNS record. If headless DNS doesn't work, the replicas
#     never sync and `--wait` times out here.
#   * auth.password — fixed so run.sh can connect deterministically (the
#     chart would otherwise random-generate it into the Secret).
#   * replica.replicaCount=2 — the default 3 adds a pod without adding
#     coverage; 2 already proves replica fan-out and per-ordinal PVCs.
#   * persistence.size=1Gi — default is 8Gi per pod; the data here is a
#     handful of test keys.
#   * networkPolicy.enabled=false / pdb.create=false — the chart defaults
#     both ON, but r8s advertises neither networking.k8s.io/v1 NetworkPolicy
#     nor policy/v1 PodDisruptionBudget in discovery, so helm can't even map
#     the objects ("no matches for kind ..."). Same opt-outs as the
#     postgresql suite; registering the kinds (inert at minimum) is a
#     planned follow-up.
helm install redis bitnami/redis \
    --namespace redis \
    --set architecture=replication \
    --set auth.password=r8s-integration \
    --set replica.replicaCount=2 \
    --set master.persistence.size=1Gi \
    --set replica.persistence.size=1Gi \
    --set networkPolicy.enabled=false \
    --set master.pdb.create=false \
    --set replica.pdb.create=false \
    --wait --timeout 5m
