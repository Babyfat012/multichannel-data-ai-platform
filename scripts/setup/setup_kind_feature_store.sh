#!/usr/bin/env bash
# Dựng cụm kind `fsds-ml` + Redis (kho online Feast), rồi nối node vào mạng Compose
# để Airflow gọi được fsds-ml-control-plane:30379. Chạy lại nhiều lần vẫn an toàn.
set -euo pipefail
cd "$(dirname "$0")/../.."

kind get clusters | grep -qx fsds-ml || kind create cluster --config infra/k8s/kind-config.yaml
kubectl --context kind-fsds-ml apply -f infra/k8s/feature-store/redis.yaml
kubectl --context kind-fsds-ml -n feature-store rollout status deploy/redis --timeout=180s
docker network connect fsds_default fsds-ml-control-plane 2>/dev/null || true
printf 'Redis sẵn sàng: fsds-ml-control-plane:30379 (từ mạng fsds_default)\n'
