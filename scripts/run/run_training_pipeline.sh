#!/usr/bin/env bash
# Biên dịch ml/training_pipeline.py, tạo Secret MinIO từ .env, submit một run lên KFP trong kind.
# Cần: KFP (infra/k8s/ml), MLflow (`docker compose --profile ml up -d mlflow`), registry đã cấu hình cho kind.
set -euo pipefail
cd "$(dirname "$0")/../.."
set -a; . ./.env; set +a
export REGISTRY="${REGISTRY:-localhost:5001}"
export TAG="${TAG:-$(git rev-parse HEAD)}"
[[ "$TAG" != latest ]] || { echo 'TAG phải bất biến, không dùng latest' >&2; exit 1; }
docker build -f docker/Dockerfile.train -t "platform-train:$TAG" .
scripts/run/push_images.sh "platform-train:$TAG"
K="kubectl --context kind-platform -n kubeflow"
$K create secret generic platform-minio --from-literal=AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD" --dry-run=client -o yaml | $K apply -f - >/dev/null
mkdir -p out/kfp
$K port-forward svc/ml-pipeline-ui 8888:80 >/dev/null 2>&1 &
trap 'kill $!' EXIT
sleep 3
uv run --no-project --with "kfp==2.17.0" --with "kfp-kubernetes==2.17.0" python - "$@" <<'PY'
import sys
import kfp
from kfp import compiler
sys.path.insert(0, ".")
from ml.training_pipeline import return_risk_training

package = "out/kfp/return_risk_training.yaml"
compiler.Compiler().compile(return_risk_training, package)
client = kfp.Client(host="http://localhost:8888")
run = client.create_run_from_pipeline_package(package, arguments={}, run_name=sys.argv[1] if len(sys.argv) > 1 else None,
                                              experiment_name="return-risk")
print("run_id", run.run_id)
result = client.wait_for_run_completion(run.run_id, timeout=1800)
print("state", result.state)
PY
