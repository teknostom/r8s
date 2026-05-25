#!/usr/bin/env bash
# Wait for Argo CD to settle. `helm install --wait` already covers the
# Deployments and the application-controller StatefulSet, but give the server an
# explicit beat and confirm the Application CRD registered — the GitOps test
# below depends on it.
set -euo pipefail

kubectl -n argocd wait \
    --for=condition=Available deploy/argocd-server \
    --timeout=3m

kubectl -n argocd wait \
    --for=condition=Ready pod \
    --selector app.kubernetes.io/name=argocd-application-controller \
    --timeout=2m

kubectl get crd applications.argoproj.io -o name >/dev/null
