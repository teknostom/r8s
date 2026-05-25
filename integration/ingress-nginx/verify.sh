#!/usr/bin/env bash
# Wait for the ingress-nginx controller to be Ready. `helm install --wait`
# covers this, but give the controller pod an explicit beat in case the
# Deployment readiness lagged the helm release.
set -euo pipefail

kubectl -n ingress-nginx wait \
    --for=condition=Ready pod \
    --selector app.kubernetes.io/component=controller \
    --timeout=2m

# The chart should have created the "nginx" IngressClass.
kubectl get ingressclass nginx -o name >/dev/null
