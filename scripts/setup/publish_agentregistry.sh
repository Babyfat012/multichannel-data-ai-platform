#!/usr/bin/env bash
# Đăng MCP server + 3 Agent vào agentregistry (governance): port-forward tạm rồi `arctl apply`.
# Cần arctl v0.4.0 (khớp server). Chạy lại = cập nhật cùng tên.
set -euo pipefail
cd "$(dirname "$0")/../.."
kubectl --context kind-platform -n agentregistry port-forward svc/agentregistry 12121:12121 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF' EXIT
for _ in $(seq 1 20); do curl -fsS localhost:12121/openapi.json >/dev/null 2>&1 && break; sleep 1; done
export ARCTL_API_BASE_URL=http://localhost:12121
arctl apply -f infra/k8s/agents/registry/feature-tools.yaml   # MCP server trước, Agent tham chiếu tới nó
for f in data-agent model-agent coordinator; do arctl apply -f "infra/k8s/agents/registry/$f.yaml"; done
arctl get mcpservers
arctl get agents
