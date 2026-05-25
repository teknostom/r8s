#!/usr/bin/env bash
# Install metrics-server via Helm.
set -euo pipefail

# --kubelet-insecure-tls: r8s's kubelet metrics server uses the cluster's
#   self-signed serving cert, which metrics-server won't otherwise trust.
# --kubelet-preferred-address-types=InternalIP: scrape the node's InternalIP
#   (10.244.0.1, the r8s bridge gateway, reachable from the metrics-server pod).
helm install metrics-server metrics-server/metrics-server \
    --namespace kube-system \
    --set 'args={--kubelet-insecure-tls,--kubelet-preferred-address-types=InternalIP}' \
    --wait --timeout 3m
