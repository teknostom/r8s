# r8s

Lightweight single-node Kubernetes distribution written in Rust.

## Requirements

- Linux (x86_64)
- containerd running (`/run/containerd/containerd.sock`)
- Root access (networking requires it)

## Install

```bash
cargo install --path crates/r8s
cargo install --path crates/r8sd
```

## Usage

```bash
# Create and start a cluster
r8s create mycluster
sudo r8s up mycluster

# Point kubectl at it
export KUBECONFIG=$(r8s kubeconfig mycluster)

# Use kubectl / helm as normal
kubectl get nodes
helm install myapp ./chart --set persistence.enabled=false

# Stop / delete
sudo r8s down mycluster
r8s delete mycluster
```

## Notes

- All `kubectl apply` commands need `--validate=false` (no OpenAPI schema served yet)
- Helm charts that use StatefulSets, Deployments, Services, ConfigMaps, Secrets, CRDs all work
- If a resource type 404s, it may need to be registered — check `helm template <chart> | grep "^kind:" | sort -u`
