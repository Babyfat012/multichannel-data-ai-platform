#!/usr/bin/env bash
# Chạy hai job Silver trên cụm Spark standalone.
#
# Vì sao script RIÊNG chứ không thêm đích vào `run_bronze_evidence.sh`: script kia là
# bằng chứng đo hiệu năng của tầng Bronze (`docs/spark.md` mục 5 và 6) — mỗi đích trong đó
# gắn với một con số đã ghi vào tài liệu. Silver là tầng khác, chạy vì mục đích khác (ra
# bảng dùng được, không phải ra số so sánh), nên để riêng cho khỏi lẫn.
#
# BẮT BUỘC bật MinIO trước, cùng lý do như Bronze: `spark-master` không `depends_on` MinIO,
# nên job đọc và tính xong rồi gãy ĐÚNG LÚC GHI với `BadRequestException: Malformed request`.
#   docker compose up -d minio minio-init postgres
#   docker compose --profile spark up -d spark-master spark-worker-1 spark-worker-2
#
# ĐỪNG chạy `docker compose --profile spark up -d` trần — nó bật luôn `generator-batch`
# và sinh lại 100 GB vào MinIO.
#
# Các đích:
#   smoke    — chạy thử nhanh trên hai bảng Bronze NHỎ, ghi ra bảng Silver có hậu tố
#              `_smoke`. Dùng để kiểm job chạy được trước khi đụng bảng thật.
#   walmart  — bảng Walmart thật  -> silver.stg_walmart_order_lines
#   amazon   — bảng Amazon thật   -> silver.stg_amazon_order_lines  (NẶNG: 108 triệu dòng)
#   tatca    — walmart rồi amazon
#
# Dùng:  bash scripts/run_silver.sh [smoke|walmart|amazon|tatca]
set -euo pipefail
cd "$(dirname "$0")/.."

WHICH="${1:-smoke}"
# Driver chạy ở worker-1 chứ không ở spark-master: container master chỉ có 512 MB và bị
# Docker kill OOM (phát hiện khi chạy brief 02 của tầng Bronze).
MASTER_CONTAINER="${MASTER_CONTAINER:-fsds-spark-worker-1-1}"
ICEBERG_PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
LOG_DIR="out/silver-runs"
mkdir -p "$LOG_DIR"

submit() {
  # $1 = tên lần chạy (dùng cho tên file log), $2 = file job, $@ còn lại = cờ cho job
  local name="$1"; shift
  local job="$1"; shift
  local log="$LOG_DIR/${name}.log"
  printf '==> %s  (log: %s)\n' "$name" "$log"
  docker exec "$MASTER_CONTAINER" /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages "$ICEBERG_PKGS" \
    --driver-memory "${DRIVER_MEMORY:-600m}" \
    ${EXTRA_SUBMIT_ARGS:-} \
    "/opt/spark_jobs/$job" "$@" > "$log" 2>&1
  grep -E "^run_id|^bảng (đích|nguồn)|^Order Line|^bản trùng|^tỉ lệ|^XONG" \
    "$log" || tail -20 "$log"
}

case "$WHICH" in
  smoke)
    # Hai bảng Bronze nhỏ đã có sẵn từ lúc dựng Bronze: `*_smoke` (Walmart, 20 file) và
    # `raw_amazon_orders_sample` (20 ngày mẫu). Đủ để kiểm cả hai nhánh schema của Amazon,
    # vì 20 ngày rải đều cắt qua cả mốc đổi schema 2026-01-01.
    submit "silver-walmart-smoke" 02_silver_walmart.py \
      --source-table raw_walmart_orders_smoke --table stg_walmart_order_lines_smoke
    submit "silver-amazon-smoke" 02_silver_amazon.py \
      --source-table raw_amazon_orders_sample --table stg_amazon_order_lines_smoke
    ;;
  walmart|tatca)
    submit "silver-walmart" 02_silver_walmart.py
    ;;&
  amazon|tatca)
    submit "silver-amazon" 02_silver_amazon.py
    ;;
  *)
    echo "Tham số phải là smoke | walmart | amazon | tatca" >&2
    exit 2
    ;;
esac
