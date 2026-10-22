#!/usr/bin/env bash
# Build Web API feature (REST + MCP), push registry cho kind fsds-ml, tạo Secret từ .env rồi `helm upgrade --atomic`.
# Mỗi lần chạy là một tag mới ⇒ rolling update; pod mới không Ready trong 2 phút ⇒ helm tự rollback.
# Dùng: [REPLICAS=1] deploy_kind_feature_api.sh [tag]   (tag mặc định: git SHA; máy local chật RAM dùng 1)
# Cần: cụm đã dựng (scripts/setup/setup_kind_feature_store.sh) và image fsds-feast:0.66.0.
set -euo pipefail
cd "$(dirname "$0")/../.."
# Credential: lấy từ môi trường nếu đã có (Jenkins: eval vault_env.sh), không thì đọc .env (máy dev).
if [ -z "${MINIO_ROOT_USER:-}" ]; then set -a; . ./.env; set +a; fi
MINIO_BUCKET="${MINIO_BUCKET:-fsds}"
K="kubectl --context kind-fsds-ml -n feature-store"
export TAG="${1:-${TAG:-$(git rev-parse HEAD)}}"
REGISTRY="${REGISTRY:-localhost:5001}"
[[ "$TAG" != latest ]] || { echo 'TAG phải bất biến, không dùng latest' >&2; exit 1; }
# Jenkins đã build/push ở stage riêng; deploy lại đúng image đó.
if [[ "${SKIP_BUILD_PUSH:-0}" != 1 ]]; then
  docker build -f docker/Dockerfile.serving -t "fsds-feature-api:$TAG" .
  scripts/run/push_images.sh "fsds-feature-api:$TAG"
fi

# Pod phân giải được `minio` của Compose vì node kind nối vào mạng fsds_default (đã kiểm 2026-10-21).
# Redis và pgvector (Feast RAG) gọi qua Service trong cụm, không qua NodePort.
$K create secret generic feature-api-env --dry-run=client -o yaml \
  --from-literal=AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD" \
  --from-literal=FEAST_S3_ENDPOINT_URL=http://minio:9000 \
  --from-literal=FEAST_REGISTRY="s3://$MINIO_BUCKET/feature-store/registry.db" \
  --from-literal=FEAST_REDIS=redis.feature-store.svc.cluster.local:6379 \
  --from-literal=FEAST_RAG_REGISTRY="s3://$MINIO_BUCKET/feature-store/rag-registry.db" \
  --from-literal=RAG_PG_HOST=pgvector.rag.svc.cluster.local \
  --from-literal=RAG_PG_PORT=5432 \
  --from-literal=RAG_PG_USER="$POSTGRES_USER" \
  --from-literal=RAG_PG_PASSWORD="$POSTGRES_PASSWORD" \
  --from-literal=KSERVE_URL=http://return-risk-predictor.ml-serving.svc.cluster.local/v1/models/return-risk:predict \
  | $K apply -f -

helm --kube-context kind-fsds-ml -n feature-store upgrade --install feature-api \
  infra/helm/feature-api --set-string image.repository="$REGISTRY/fsds-feature-api" --set-string image.tag="$TAG" --set replicas="${REPLICAS:-2}" --atomic --timeout 4m
helm --kube-context kind-fsds-ml -n feature-store history feature-api --max 5
printf 'Feature API %s: http://fsds-ml-control-plane:30800 (REST /features, MCP /mcp/)\n' "$TAG"
