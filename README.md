# r8s

Lightweight single-node Kubernetes distribution written in Rust.

## Requirements

- Linux (x86_64)
- containerd running (`/run/containerd/containerd.sock`)
- Root access (networking requires it)
- Helm and kubectl (for `r8s env` commands)

## Install

```bash
cargo build --release -p r8s -p r8sd
sudo cp target/release/r8s target/release/r8sd /usr/local/bin/
```

## Quick start with r8s.toml

Add an `r8s.toml` to your project:

```toml
[cluster]
name = "myproject"
namespace = "myproject"

[[dependencies]]
name = "postgresql"
chart = "bitnami/postgresql"
version = "16.0.0"
values = ["values/postgres.yaml"]
set = { "auth.postgresPassword" = "secret" }

[app]
name = "myapp"
chart = "./deployment"
values = ["values/myapp.yaml"]
```

Then:

```bash
# Create cluster, start it, deploy deps then app
sudo r8s env up

# Export kubeconfig
export KUBECONFIG=$(r8s kubeconfig)

# Check pods
kubectl get pods -A

# Stop cluster (state preserved, restarts where it left off)
sudo r8s env down

# Restart (skips install, pods resume from saved state)
sudo r8s env up

# Destroy everything
sudo r8s env nuke
```

## r8s.toml reference

| Field | Description |
|-------|-------------|
| `cluster.name` | Cluster name (default: `"default"`) |
| `cluster.namespace` | Default namespace for releases (default: `"default"`) |
| `dependencies[]` | Helm charts installed before the app, in order |
| `app` | The main application chart, installed last |

Each release (dependency or app) supports:

| Field | Description |
|-------|-------------|
| `name` | Release name (required) |
| `chart` | Chart path or reference (required) |
| `version` | Chart version |
| `namespace` | Override namespace for this release |
| `values` | List of values file paths (relative to r8s.toml) |
| `set` | Inline key-value overrides (`--set` equivalent) |

## Manual cluster management

```bash
# Create and start a cluster
r8s create mycluster
sudo r8s up mycluster

# Point kubectl at it
export KUBECONFIG=$(r8s kubeconfig mycluster)

# Use kubectl / helm as normal
kubectl get nodes
kubectl apply -f manifest.yaml --validate=false

# Stop / delete
sudo r8s down mycluster
r8s delete mycluster
```

## Other commands

```bash
r8s list                    # List all clusters
r8s status [name]           # Show cluster status
r8s kubeconfig [name]       # Print kubeconfig path
r8s stats [name]            # Show store statistics
r8s logs [name] [-f]        # Tail daemon logs
```

## Notes

- `r8s env up` uses `helm template` + `kubectl apply --validate=false` under the hood
- Dependencies are installed in declaration order, app is installed last
- `r8s env down` just stops the daemon — all state is preserved in the store
- `r8s env nuke` stops the daemon and deletes all cluster data
- ClusterIPs are auto-assigned to services
- Pod DNS resolves service names within and across namespaces
