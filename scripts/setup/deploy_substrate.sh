#!/usr/bin/env bash
# Agent Substrate cho kagent 0.10.3: Agent chạy trong sandbox gVisor (node agent của Substrate tự đặt runtime gVisor,
# không sửa containerd của node) trên WorkerPool ít worker; actor rảnh snapshot về object storage rồi "Suspended"
# ⇒ nhiều Agent dùng chung vài worker (Warm Up / Agent Fleet). Theo kagent.dev/docs/kagent/examples/agent-substrate.
# Cần kagent đã cài (scripts/setup/deploy_kind_agents.sh). Chạy lại nhiều lần vẫn an toàn.
set -euo pipefail
cd "$(dirname "$0")/../.."
CTX=${KCTX:-kind-platform}
SUBSTRATE_VERSION=0.0.9
KAGENT_VERSION=0.10.3   # khớp deploy_kind_agents.sh; bản < 0.9.7 bỏ qua giá trị substrate mà không báo
H=(helm --kube-context "$CTX")
"${H[@]}" upgrade --install substrate-crds oci://ghcr.io/kagent-dev/substrate/helm/substrate-crds \
  --version "$SUBSTRATE_VERSION" -n ate-system --create-namespace --wait
"${H[@]}" upgrade --install substrate oci://ghcr.io/kagent-dev/substrate/helm/substrate \
  --version "$SUBSTRATE_VERSION" -n ate-system --wait --timeout 10m
# Giữ values hiện có (model qua agentgateway, proxy A2A/MCP) và chỉ bật nối với Substrate; 1 worker cho cả fleet.
"${H[@]}" upgrade kagent oci://ghcr.io/kagent-dev/kagent/helm/kagent --version "$KAGENT_VERSION" -n kagent \
  --reuse-values \
  --set controller.substrate.enabled=true \
  --set controller.substrate.ateApiEndpoint=dns:///api.ate-system.svc:443 \
  --set controller.substrate.ateApiInsecure=true \
  --set substrateWorkerPool.create=true \
  --set substrateWorkerPool.replicas="${SUBSTRATE_WORKERS:-1}" \
  --set substrateWorkerPool.ateomImage=ghcr.io/kagent-dev/substrate/ateom-gvisor:v"$SUBSTRATE_VERSION" \
  --wait --timeout 10m
kubectl --context "$CTX" apply -f infra/k8s/agents/sandbox/
# Lần đầu tạo golden snapshot (~60–90 s).
kubectl --context "$CTX" -n kagent wait sandboxagent --all --for=condition=Ready --timeout=10m
kubectl --context "$CTX" -n kagent get workerpool,sandboxagent
kubectl --context "$CTX" -n ate-system get pods
