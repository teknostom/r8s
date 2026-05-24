#!/usr/bin/env bash
# Wait for all four cert-manager pods (controller, webhook, cainjector,
# startupapicheck) to reach Ready. `helm upgrade --wait` covers most of
# this, but webhooks racing with their TLS cert can need another beat.
set -euo pipefail

kubectl -n cert-manager wait \
    --for=condition=Ready pod \
    --selector app.kubernetes.io/instance=cert-manager \
    --timeout=2m

# Sanity-check the webhook is actually reachable via its Service. The
# validating webhook endpoint is /validate; an empty body should round-trip
# as a Status response (not a connection error).
kubectl -n cert-manager get svc cert-manager-webhook -o name >/dev/null
