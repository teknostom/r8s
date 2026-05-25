#!/usr/bin/env bash
# Prerequisites for metrics-server: the metrics-server Helm repo.
# (Installs into the existing kube-system namespace.)
set -euo pipefail

helm repo list 2>/dev/null | grep -q '^metrics-server\b' \
    || helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
helm repo update metrics-server
