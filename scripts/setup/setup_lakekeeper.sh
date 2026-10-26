#!/usr/bin/env bash
# Dựng catalog Iceberg trên Lakekeeper: bootstrap server (một lần) + tạo warehouse trỏ MinIO.
#
# Vì sao cần: Lakekeeper là "sổ" giữ con trỏ tới bản metadata mới nhất của mỗi bảng Iceberg.
# Spark không tự tạo được warehouse — phải gọi Management API của Lakekeeper trước.
# File metadata và file dữ liệu thật vẫn nằm trên MinIO, Lakekeeper chỉ giữ con trỏ.
#
# Chạy lại được (idempotent): đã bootstrap thì bỏ qua, đã có warehouse cùng tên thì bỏ qua.
#
# Dùng:  bash scripts/setup/setup_lakekeeper.sh
# Cần:   docker compose --profile catalog up -d  (lakekeeper + postgres + minio đang chạy)
set -euo pipefail

cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
# shellcheck disable=SC1091
set -a; source .env; set +a

LAKEKEEPER_URL="http://localhost:${LAKEKEEPER_PORT:-8181}"
WAREHOUSE_NAME="${WAREHOUSE_NAME:-platform}"
# Endpoint MinIO nhìn từ TRONG mạng Compose: Lakekeeper và Spark đều là container.
MINIO_ENDPOINT="${MINIO_INTERNAL_ENDPOINT:-http://minio:9000}"

say() { printf '==> %s\n' "$1"; }

# --- 1. Bootstrap --------------------------------------------------------------
# Server mới dựng chưa có project mặc định; mọi lệnh tạo warehouse sẽ lỗi nếu chưa bootstrap.
if [ "$(curl -fsS "$LAKEKEEPER_URL/management/v1/info" | python3 -c 'import sys,json;print(json.load(sys.stdin)["bootstrapped"])')" = "True" ]; then
  say "Đã bootstrap trước đó, bỏ qua."
else
  say "Bootstrap Lakekeeper..."
  curl -fsS -X POST "$LAKEKEEPER_URL/management/v1/bootstrap" \
    -H 'Content-Type: application/json' \
    -d '{"accept-terms-of-use": true}'
  say "Bootstrap xong."
fi

# --- 2. Warehouse --------------------------------------------------------------
if curl -fsS "$LAKEKEEPER_URL/management/v1/warehouse" \
     | python3 -c "import sys,json;ws=json.load(sys.stdin)['warehouses'];sys.exit(0 if any(w['name']=='$WAREHOUSE_NAME' for w in ws) else 1)"; then
  say "Warehouse '$WAREHOUSE_NAME' đã có, bỏ qua."
else
  say "Tạo warehouse '$WAREHOUSE_NAME' trỏ MinIO bucket '$MINIO_BUCKET'..."
  # flavor=s3-compat: bắt buộc cho MinIO (MinIO không phải AWS S3 thật).
  # sts-enabled=false: MinIO ở đây không chạy STS, nên không dùng vended credentials.
  #   Spark sẽ tự mang access key của mình (cấu hình trong job), không xin Lakekeeper.
  # key-prefix: mọi bảng Iceberg nằm gọn trong s3://$MINIO_BUCKET/lakehouse/, không trộn
  #   lẫn với source-exports/ của generator trong cùng bucket.
  python3 - "$LAKEKEEPER_URL" "$WAREHOUSE_NAME" "$MINIO_BUCKET" "$MINIO_ENDPOINT" \
           "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" <<'PY'
import json, sys, urllib.request

url, name, bucket, endpoint, key_id, secret = sys.argv[1:7]
body = {
    "warehouse-name": name,
    "storage-profile": {
        "type": "s3",
        "bucket": bucket,
        "key-prefix": "lakehouse",
        "region": "local-01",
        "endpoint": endpoint,
        "flavor": "s3-compat",
        "sts-enabled": False,
        "path-style-access": True,
    },
    "storage-credential": {
        "type": "s3",
        "credential-type": "access-key",
        "aws-access-key-id": key_id,
        "aws-secret-access-key": secret,
    },
}
req = urllib.request.Request(
    f"{url}/management/v1/warehouse",
    data=json.dumps(body).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(req) as r:
        print(r.read().decode())
except urllib.error.HTTPError as e:
    print(f"LỖI HTTP {e.code}: {e.read().decode()}", file=sys.stderr)
    raise SystemExit(1)
PY
  say "Tạo warehouse xong."
fi

say "Warehouse hiện có:"
curl -fsS "$LAKEKEEPER_URL/management/v1/warehouse" | python3 -m json.tool
