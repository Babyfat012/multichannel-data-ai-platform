#!/usr/bin/env bash
# Đo trên bảng/nguồn thật ở EC2; gọi sau DAG v2, trước pha DataHub.
set -euo pipefail
cd "$(dirname "$0")/../.."
usage() {
  echo 'Cách chạy: bash scripts/run/run_aws_measurements.sh --what skew|maintenance|warehouse'
  echo 'Đọc .env AWS. Kết quả: out/aws-evidence/<ngày>/<what>-<run>/ (JSON, log, events).'
  echo 'Skew dùng prefix nguồn S3 hiện tại; cần nguồn scale đủ history để kết luận skew.'
}
what=
while (($#)); do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --what) [[ $# -ge 2 ]] || { usage >&2; exit 2; }; what=$2; shift 2 ;;
    *) usage >&2; exit 2 ;;
  esac
done
case "$what" in skew|maintenance|warehouse) ;; *) usage >&2; exit 2 ;; esac
# Chỉ export cấu hình runtime; Docker Compose tự đọc secret từ .env.
if [[ -f .env ]]; then
  for key in FSDS_STORAGE AWS_REGION RAW_BUCKET FSDS_DATASET_NAME FSDS_SPARK_PACKAGES_AWS COMPOSE_FILE FSDS_ORDER_LIFECYCLE_CUTOFF; do
    if [[ ! -v "$key" ]]; then
      value=$( (set -a; source .env; printf '%s' "${!key:-}") )
      export "$key=$value"
    fi
  done
fi
[[ "${FSDS_STORAGE:-}" = s3 ]] || { echo 'Chỉ chạy với FSDS_STORAGE=s3' >&2; exit 2; }
: "${RAW_BUCKET:?}" "${AWS_REGION:?}" "${FSDS_SPARK_PACKAGES_AWS:?}"
compose=(docker compose --profile airflow --profile spark)
if [[ -n "$(docker ps -q --filter label=com.docker.compose.project=datahub)" ]]; then
  echo 'Dừng pha DataHub trước khi đo để đủ RAM.' >&2; exit 1
fi
day="$(date -u +%F)"
stamp="$(date -u +%Y%m%dT%H%M%S)-$$"
output="out/aws-evidence/$day/$what-$stamp"
work="/spark-events/aws-evidence/$day/$what-$stamp"
mkdir -p "$output"
"${compose[@]}" up -d --wait --wait-timeout 600 airflow spark-master spark-worker-1 spark-worker-2
"${compose[@]}" exec -T airflow mkdir -p "$work/events"
"${compose[@]}" images --format json > "$output/runtime-images.json"
# Luôn sao chép bằng chứng kể cả Spark lỗi; không dừng stack trước phép đo tiếp theo.
collect() {
  local code=$?
  trap - EXIT
  "${compose[@]}" cp "airflow:$work/." "$output/" || { [[ "$code" != 0 ]] || code=1; }
  echo "Bằng chứng $what: $output (exit $code)"
  exit "$code"
}
trap collect EXIT
submit() {
  "${compose[@]}" exec -T -e "FSDS_SPARK_EVENT_LOG_DIR=file://$work/events" airflow spark-submit \
    --master spark://spark-master:7077 --driver-memory 2g \
    --conf spark.driver.host=airflow --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.cores.max=2 --conf spark.executor.cores=1 --executor-memory 2g \
    --conf spark.jars.ivy=/opt/airflow/.ivy2 --packages "$FSDS_SPARK_PACKAGES_AWS" "$@"
}
cutoff="${FSDS_ORDER_LIFECYCLE_CUTOFF:-2026-06-01T00:00:00.000000Z}"
case "$what" in
  skew)
    submit /opt/spark_jobs/measure/measure_order_lifecycle_skew.py \
      --source "s3a://$RAW_BUCKET/order_lifecycle/${FSDS_DATASET_NAME:-fixture}" \
      --workdir "$work/skew" --output "$work/skew.json" \
      --buckets 18 --partitions 16 2>&1 | tee "$output/run.log"
    ;;
  maintenance)
    # Dùng cùng policy hiện có trên Gold fact thật, không dùng bảng demo.
    config=$(python3 - <<'PY'
import json
from pathlib import Path
config = json.loads(Path('configs/iceberg_maintenance.json').read_text())
policy = next(iter(config['tables'].values()))
print(json.dumps({'tables': {'lakehouse.gold.lifecycle_fact_order_lines': policy}}))
PY
)
    submit /opt/spark_jobs/maintenance/maintain_iceberg_tables.py \
      --config-json "$config" --execute --receipt "$work/maintenance.json" 2>&1 | tee "$output/run.log"
    submit /opt/spark_jobs/maintenance/maintain_iceberg_tables.py \
      --validate-receipt "$work/maintenance.json" 2>&1 | tee "$output/validate.log"
    ;;
  warehouse)
    run_id="${FSDS_V2_RUN_ID:-$(cat out/receipts-v2-run/latest-run-id)}"
    submit /opt/spark_jobs/warehouse/export_order_lifecycle.py \
      --run-id "$run_id" --cutoff "$cutoff" --sql-dir /opt/sql/warehouse_lifecycle \
      --receipt "$work/warehouse-export.json" 2>&1 | tee "$output/run.log"
    # Chọn SKU/dataset/thời điểm từ feature vừa export, không đoán giá trị fixture.
    "${compose[@]}" exec -T airflow python - "$cutoff" "$work/warehouse-explain.json" <<'PY' 2>&1 | tee "$output/explain.log"
import importlib.util
import sys
from spark_jobs.lib.order_lifecycle_warehouse import connect
spec = importlib.util.spec_from_file_location(
    'warehouse_measure', '/opt/fsds/scripts/evidence/measure_order_lifecycle_warehouse.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
conn = connect()
try:
    with conn.cursor() as cur:
        cur.execute('''SELECT dataset_version, seller_sku, event_timestamp, created
            FROM warehouse_lifecycle.feat_sku_return_rate_90d
            WHERE seller_sku IS NOT NULL AND event_timestamp IS NOT NULL AND created IS NOT NULL
            ORDER BY labeled_count_90d DESC, dataset_version, line_id LIMIT 1''')
        row = cur.fetchone()
        if row is None:
            raise ValueError('Không có feature để đo; không ghi bằng chứng rỗng')
finally:
    conn.close()
module.main(['--cutoff', sys.argv[1], '--receipt', sys.argv[2], '--dataset', row[0],
             '--sku', row[1], '--event-timestamp', row[2].isoformat() + 'Z',
             '--as-of', row[3].isoformat() + 'Z', '--sql-dir', '/opt/sql/warehouse_lifecycle'])
PY
    ;;
esac
