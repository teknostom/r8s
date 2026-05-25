#!/usr/bin/env bash
# End-to-end test: deploy a backend + Service + Ingress, then route an HTTP
# request through the ingress-nginx controller by Host header and confirm it
# reaches the backend. This exercises L7 routing, the controller's
# EndpointSlice watching, and pod-to-pod networking.
set -euo pipefail

NS=ingress-nginx-test
HOST=demo.r8s.test
WANT=hello-from-r8s

kubectl apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${NS}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo
  namespace: ${NS}
spec:
  replicas: 1
  selector:
    matchLabels: { app: echo }
  template:
    metadata:
      labels: { app: echo }
    spec:
      containers:
        - name: echo
          image: hashicorp/http-echo:0.2.3
          args: ["-text=${WANT}"]
          ports:
            - containerPort: 5678
---
apiVersion: v1
kind: Service
metadata:
  name: echo
  namespace: ${NS}
spec:
  selector: { app: echo }
  ports:
    - port: 80
      targetPort: 5678
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: echo
  namespace: ${NS}
spec:
  ingressClassName: nginx
  rules:
    - host: ${HOST}
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: echo
                port:
                  number: 80
EOF

kubectl -n "$NS" wait --for=condition=Available deploy/echo --timeout=90s

# r8s has no NodePort/LoadBalancer, but the host is on the r8s bridge
# (10.244.0.0/16), so the controller pod IP is directly reachable.
CTRL_IP=$(kubectl -n ingress-nginx get pod \
    -l app.kubernetes.io/component=controller \
    -o jsonpath='{.items[0].status.podIP}')
echo "controller pod IP: ${CTRL_IP}"

# nginx needs a moment to observe the new Ingress and reload its config.
for i in $(seq 1 30); do
    body=$(curl -s --max-time 5 -H "Host: ${HOST}" "http://${CTRL_IP}/" || true)
    if [[ "$body" == *"${WANT}"* ]]; then
        echo "OK: ingress routed Host=${HOST} to backend (got: ${body})"
        exit 0
    fi
    sleep 2
done

echo "FAIL: ingress did not route Host=${HOST} to backend after 60s"
echo "last response body: ${body:-<empty>}"
exit 1
