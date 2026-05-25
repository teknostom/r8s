#!/usr/bin/env bash
# End-to-end GitOps test: hand Argo CD the canonical `guestbook` Application
# (a public Git repo) and let it drive the whole loop — repo-server clones the
# repo, the application-controller applies the manifests against the in-cluster
# API (kubernetes.default), creates the target namespace, and reconciles to a
# running Deployment. Asserting the *workload* is Available (not just that the
# Application object exists) is what makes this telling: it can only pass if
# clone -> apply -> reconcile all worked.
set -euo pipefail

APP_NS=guestbook

kubectl apply -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: guestbook
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/argoproj/argocd-example-apps.git
    targetRevision: HEAD
    path: guestbook
  destination:
    server: https://kubernetes.default.svc
    namespace: ${APP_NS}
  syncPolicy:
    automated: {}
    syncOptions:
      - CreateNamespace=true
EOF

# The guestbook Deployment only exists once the first sync lands, so poll for
# it rather than waiting on a single object up front.
for _ in $(seq 1 36); do
    if kubectl -n "$APP_NS" get deploy guestbook-ui >/dev/null 2>&1 \
        && kubectl -n "$APP_NS" wait --for=condition=Available \
            deploy/guestbook-ui --timeout=10s >/dev/null 2>&1; then
        echo "OK: Argo CD synced guestbook from Git -> Deployment Available"
        kubectl -n argocd get application guestbook \
            -o jsonpath='guestbook: sync={.status.sync.status} health={.status.health.status}{"\n"}' \
            2>/dev/null || true
        exit 0
    fi
    sleep 5
done

echo "FAIL: guestbook did not reach a running Deployment after ~3m"
echo "--- application status ---"
kubectl -n argocd get application guestbook -o yaml 2>&1 | sed -n '/^status:/,$p' | head -60 || true
echo "--- guestbook namespace ---"
kubectl -n "$APP_NS" get all 2>&1 || true
echo "--- application-controller logs ---"
kubectl -n argocd logs -l app.kubernetes.io/name=argocd-application-controller --tail=40 2>&1 || true
echo "--- repo-server logs ---"
kubectl -n argocd logs -l app.kubernetes.io/name=argocd-repo-server --tail=20 2>&1 || true
exit 1
