#!/usr/bin/env bash
# Prerequisites for kafka: namespace + the Bitnami Helm repo.
#
# This suite covers what redis couldn't. Redis proved StatefulSet *peers*
# resolving each other; kafka's protocol makes a third-party client do it:
# the client bootstraps via the ClusterIP Service, and the broker's metadata
# answer redirects it to the per-pod advertised listener
# (`kafka-controller-0.kafka-controller-headless...`) — so produce/consume
# only works if another pod can resolve the broker's per-pod record
# mid-protocol. It's also the first KRaft bootstrap (the controller dials
# *itself* as a quorum voter before it is Ready, which is what
# publishNotReadyAddresses exists for) and the first JVM workload.
set -euo pipefail

# `kubectl apply -f-` sends JSON via the dynamic client; `kubectl create
# namespace` would use protobuf, which r8s doesn't decode for Namespace.
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: kafka
EOF

helm repo list 2>/dev/null | grep -q '^bitnami\b' \
    || helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update bitnami
