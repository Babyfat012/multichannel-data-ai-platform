#!/usr/bin/env bash
# Dựng lại phần nền dùng chung của MLE sau khi máy restart hoặc lỡ `docker compose stop`:
# MinIO + PostgreSQL + Lakekeeper, Vault (unseal), Jenkins, rồi nạp lại Redis nếu trống (Redis
# trong kind không lưu đĩa). Chạy lại nhiều lần an toàn; không bật LLM/agent (tốn RAM).
set -euo pipefail
cd "$(dirname "$0")/../.."
docker compose --profile catalog up -d --wait minio postgres lakekeeper >/dev/null
scripts/setup/setup_kind_registry.sh
scripts/setup/setup_vault.sh | tail -1
scripts/setup/setup_jenkins.sh | tail -1
keys=$(kubectl --context kind-fsds-ml -n feature-store exec deploy/redis -- redis-cli dbsize)
if [[ "$keys" == 0 ]]; then
  cutoff=${FEAST_ONLINE_END:-2026-10-13T00:00:00}  # mốc ảnh chụp online = cutoff Gold
  docker compose --profile ml-stream run --rm --no-deps feast-pusher \
    feast materialize 2025-12-01T00:00:00 "$cutoff" --views sku_return_daily >/dev/null 2>&1
  keys=$(kubectl --context kind-fsds-ml -n feature-store exec deploy/redis -- redis-cli dbsize)
fi
printf 'Nền MLE sẵn sàng: Redis %s key, Vault unsealed, Jenkins :8090\n' "$keys"
