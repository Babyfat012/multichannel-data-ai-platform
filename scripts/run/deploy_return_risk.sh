#!/usr/bin/env bash
# Model đang mang alias `production` trong MLflow → image KServe (model-inside-image) → registry → kind pull → apply.
# MODEL_VERSION=N chỉ để smoke test hạ tầng với một version CỤ THỂ (bỏ qua gate production).
# Cần: MLflow (Compose :5050), image platform-train, KServe đã cài trong kind.
set -euo pipefail
cd "$(dirname "$0")/../.."
set -a; . ./.env; set +a
ctx=$(mktemp -d); trap 'rm -rf "$ctx"' EXIT
# Tải model.lgb qua endpoint proxy của server (client MLflow mặc định xin presigned URL trỏ tới
# minio:9000 — tên chỉ có trong mạng Docker, máy host không phân giải được).
version=$(uv run --no-project --with "mlflow-skinny==3.17.0" python - "$ctx" <<'PY'
import os, sys, urllib.request
from mlflow import MlflowClient
uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:" + os.environ.get("MLFLOW_PORT", "5050"))
client = MlflowClient(uri)
pinned = os.environ.get("MODEL_VERSION")
v = client.get_model_version("return-risk", pinned) if pinned else client.get_model_version_by_alias("return-risk", "production")
if pinned:
    print(f"CẢNH BÁO: deploy v{pinned} theo MODEL_VERSION, không qua gate production", file=sys.stderr)
experiment = client.get_run(v.run_id).info.experiment_id
model_id = v.source.removeprefix("models:/")
url = f"{uri}/api/2.0/mlflow-artifacts/artifacts/{experiment}/models/{model_id}/artifacts/model.lgb"
urllib.request.urlretrieve(url, sys.argv[1] + "/model.lgb")
print(v.version)
PY
)
cp -r ml "$ctx/ml"
# Tag gồm version model + băm code/model: dùng lại tag cũ thì kind/kubelet giữ bản cũ theo tag.
digest=$(cat "$ctx/model.lgb" ml/*.py docker/Dockerfile.serve | sha256sum | cut -c1-8)
code_tag=${TAG:-$(git rev-parse HEAD)}
[[ "$code_tag" != latest ]] || { echo "Không dùng TAG=latest" >&2; exit 1; }
REGISTRY="${REGISTRY:-localhost:5001}"
source_image="platform-return-risk:v$version-$code_tag-$digest"
image="$REGISTRY/$source_image"
docker build -q -f docker/Dockerfile.serve --build-arg MODEL_VERSION="$version" -t "$source_image" "$ctx"
docker tag "$source_image" "$image"
docker push "$image"
K="kubectl --context ${KCTX:-kind-platform}"
$K create namespace ml-serving --dry-run=client -o yaml | $K apply -f - >/dev/null
$K -n ml-serving create secret generic platform-minio --from-literal=AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD" --dry-run=client -o yaml | $K apply -f - >/dev/null
sed -e "s|@@IMAGE@@|$image|" -e "s|@@VERSION@@|$version|" infra/k8s/ml/return-risk.yaml | $K apply -f -
$K -n ml-serving wait --for=condition=Ready inferenceservice/return-risk --timeout=300s
echo "deployed return-risk v$version ($image)"
