#!/usr/bin/env bash
# Install postgresql via the Bitnami Helm chart, standalone with persistence.
set -euo pipefail

# Notes on the flags:
#   * architecture=standalone — single primary, one StatefulSet replica. No
#     read replicas; this is a local-dev dependency, not an HA deployment.
#   * auth.postgresPassword — fixed so run.sh can connect deterministically
#     (the chart would otherwise random-generate it into the Secret).
#   * primary.persistence.enabled=true — left ON deliberately. This is the
#     point of the suite: the volumeClaimTemplate must produce a PVC that
#     gets dynamically provisioned and bound. If r8s has no default
#     StorageClass / provisioner yet, the pod stays Pending here and helm
#     times out — that's the blindspot this suite is meant to map.
#   * primary.networkPolicy.enabled=false / primary.pdb.create=false — the
#     chart defaults both ON, but r8s advertises neither
#     networking.k8s.io/v1 NetworkPolicy nor policy/v1 PodDisruptionBudget
#     in discovery, so helm can't even map the objects ("no matches for
#     kind ..."). Both are unrelated to the storage path under test;
#     registering them (inert at minimum) is a planned follow-up.
helm install postgresql bitnami/postgresql \
    --namespace postgresql \
    --set architecture=standalone \
    --set auth.postgresPassword=r8s-integration \
    --set primary.persistence.enabled=true \
    --set primary.persistence.size=1Gi \
    --set primary.networkPolicy.enabled=false \
    --set primary.pdb.create=false \
    --wait --timeout 3m
