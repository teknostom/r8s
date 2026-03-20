# r8s

A lightweight single-node Kubernetes distribution written in Rust. Designed for local development — drop an `r8s.toml` in your project, run `sudo r8s env up`, and get a working cluster with your dependencies and app deployed.

> **Early development.** This project is under active development. APIs and behavior may change. Container leaks can occur — if you hit issues, `sudo r8s env nuke` will clean up.

## Requirements

- Linux (x86_64)
- containerd running (`sudo systemctl start containerd`)
- Root access (networking setup requires it)
- Helm and kubectl on PATH (for `r8s env` commands)

## Install

```bash
cargo build --release -p r8s -p r8sd
sudo cp target/release/r8s target/release/r8sd /usr/local/bin/
```

## The env workflow

The intended way to use r8s is with an `r8s.toml` at the root of your project. It declares your cluster, dependencies, and app — then a single command brings everything up.

### 1. Define your environment

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

### 2. Bring it up

```bash
sudo r8s env up
export KUBECONFIG=$(r8s kubeconfig)
kubectl get pods -A
```

This creates the cluster (if it doesn't exist), starts the daemon, and deploys dependencies in order followed by your app.

### 3. Develop

```bash
# Redeploy after changes
sudo r8s env up

# Stop the cluster (state is preserved)
sudo r8s env down

# Restart (pods resume from saved state)
sudo r8s env up
```

### 4. Clean up

```bash
# Tear down everything — cluster, containers, data
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

You can also manage clusters directly without `r8s.toml`:

```bash
r8s create mycluster
sudo r8s up mycluster
export KUBECONFIG=$(r8s kubeconfig mycluster)

kubectl get nodes
kubectl apply -f manifest.yaml --validate=false

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

## Accessing services

Services are reachable from the host via their ClusterIPs. r8s auto-assigns these when services are created. To find a service's IP:

```bash
kubectl get svc -n myproject
```

Then connect directly — e.g. `psql -h 10.96.0.5 -U postgres`.

## How it works

r8s runs a single daemon (`r8sd`) that bundles an API server, scheduler, kubelet, controller manager, DNS server, and ingress proxy. State is stored in an embedded database. Containers are managed through containerd. Networking uses a bridge with nftables NAT rules.

## License

MIT
