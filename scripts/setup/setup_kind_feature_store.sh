#!/usr/bin/env bash
# Dựng cụm kind `platform` + Redis (kho online Feast), rồi nối node vào mạng Compose
# để Airflow gọi được platform-control-plane:30379. Chạy lại nhiều lần vẫn an toàn.
set -euo pipefail
cd "$(dirname "$0")/../.."

kind get clusters | grep -qx platform || kind create cluster --config infra/k8s/kind-config.yaml
kubectl --context kind-platform apply -f infra/k8s/feature-store/redis.yaml
kubectl --context kind-platform -n feature-store rollout status deploy/redis --timeout=180s
docker network connect platform_default platform-control-plane 2>/dev/null || true
printf 'Redis sẵn sàng: platform-control-plane:30379 (từ mạng platform_default)\n'
