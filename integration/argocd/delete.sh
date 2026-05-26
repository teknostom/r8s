#!/usr/bin/env bash
# Argo CD installs the argoproj.io CRDs (Application, AppProject, …); helm
# uninstall leaves them behind. Remove the stragglers.
set -uo pipefail

kubectl get crd -o name 2>/dev/null \
    | grep 'argoproj\.io$' \
    | xargs -r kubectl delete --ignore-not-found --wait=false 2>/dev/null || true
