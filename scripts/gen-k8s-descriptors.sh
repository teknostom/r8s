#!/usr/bin/env bash
set -euo pipefail

# Download Kubernetes v1.32 proto files and compile into a FileDescriptorSet
# for use with prost-reflect at runtime.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROTO_DIR="$ROOT_DIR/proto"
OUTPUT="$ROOT_DIR/crates/r8s-api/k8s.desc"

K8S_TAG="v0.32.0"
API_BASE="https://raw.githubusercontent.com/kubernetes/api/${K8S_TAG}"
APIMACHINERY_BASE="https://raw.githubusercontent.com/kubernetes/apimachinery/${K8S_TAG}"
APIEXT_BASE="https://raw.githubusercontent.com/kubernetes/apiextensions-apiserver/${K8S_TAG}"

echo "Downloading Kubernetes proto files (${K8S_TAG})..."
rm -rf "$PROTO_DIR"

# --- apimachinery (foundation types) ---
for path in \
    pkg/apis/meta/v1/generated.proto \
    pkg/api/resource/generated.proto \
    pkg/runtime/generated.proto \
    pkg/runtime/schema/generated.proto \
    pkg/util/intstr/generated.proto
do
    dest="$PROTO_DIR/k8s.io/apimachinery/$path"
    mkdir -p "$(dirname "$dest")"
    curl -sSf "${APIMACHINERY_BASE}/${path}" -o "$dest"
    echo "  apimachinery/$path"
done

# --- api (core + extension API groups) ---
for group_ver in \
    core/v1 \
    apps/v1 \
    batch/v1 \
    networking/v1 \
    discovery/v1 \
    rbac/v1 \
    coordination/v1 \
    policy/v1 \
    autoscaling/v1 \
    autoscaling/v2 \
    scheduling/v1 \
    storage/v1 \
    node/v1 \
    admissionregistration/v1 \
    flowcontrol/v1 \
    events/v1 \
    certificates/v1
do
    dest="$PROTO_DIR/k8s.io/api/${group_ver}/generated.proto"
    mkdir -p "$(dirname "$dest")"
    curl -sSf "${API_BASE}/${group_ver}/generated.proto" -o "$dest"
    echo "  api/$group_ver/generated.proto"
done

# --- apiextensions-apiserver (CRD types) ---
dest="$PROTO_DIR/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/generated.proto"
mkdir -p "$(dirname "$dest")"
curl -sSf "${APIEXT_BASE}/pkg/apis/apiextensions/v1/generated.proto" -o "$dest"
echo "  apiextensions-apiserver/pkg/apis/apiextensions/v1/generated.proto"

echo ""
echo "Compiling proto descriptors..."

# Collect all proto files
PROTO_FILES=()
while IFS= read -r -d '' f; do
    PROTO_FILES+=("$f")
done < <(find "$PROTO_DIR" -name '*.proto' -print0)

protoc \
    --proto_path="$PROTO_DIR" \
    --descriptor_set_out="$OUTPUT" \
    --include_imports \
    "${PROTO_FILES[@]}"

SIZE=$(du -h "$OUTPUT" | cut -f1)
echo "Generated $OUTPUT ($SIZE)"
echo "Done."
