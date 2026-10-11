#!/usr/bin/env bash
# Chạy job Bronze 01 ba lần để thu bằng chứng tối ưu hiệu năng cho rubric.
#
# Vì sao ba lần (chốt 2026-10-10):
#   Lần 1 — 20 ngày, AQE TẮT  -> ảnh Spark UI thấy task lệch nhau (skew) + số giây.
#   Lần 2 — 20 ngày, AQE BẬT  -> ảnh task đều + số giây. CÙNG 20 ngày nên so giây được.
#   Lần 3 — 180 ngày, AQE BẬT -> bảng Bronze thật, đầy đủ.
# Hai lần đầu dùng bảng riêng `raw_amazon_orders_sample` để không đè bảng thật.
# 20 ngày được chọn RẢI ĐỀU trên cả dải (xem spark_jobs/day_selection.py): lấy 20 ngày
# liền nhau ở cuối thì mọi task to bằng nhau và skew không lộ ra.
#
# Event log của cả ba lần nằm ở ./out/spark-events -> mở lại bằng History Server để chụp
# ảnh sau khi job đã xong (Spark UI cổng 4040 chết cùng job).
#
# Walmart (job 01b, đọc PostgreSQL qua JDBC) có bộ ba riêng, cùng khuôn "đo rồi mới chốt":
#   wal1  — 1 kết nối JDBC   -> hành vi MẶC ĐỊNH của Spark: một task kéo cả bảng. Baseline.
#   wal16 — 16 kết nối JDBC  -> CÙNG dữ liệu, đọc song song. So giây được vì cùng quy mô.
#   wal   — bảng Walmart thật, cấu hình đã chốt.
# Hai lần đầu ghi vào `raw_walmart_orders_smoke` để không đè bảng thật.
# Cần nạp PostgreSQL trước:
#   uv run python -m generator load-postgres --source /data/raw --sample-days 20 \
#     --dsn postgresql://<user>:<pass>@localhost:5432/sources
#
# Dùng:  bash scripts/run_bronze_evidence.sh [lan1|lan2|lan3|wal1|wal16|wal|tatca]
set -euo pipefail
cd "$(dirname "$0")/.."

WHICH="${1:-tatca}"
MASTER_CONTAINER="${MASTER_CONTAINER:-fsds-spark-master-1}"
ICEBERG_PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
# Driver JDBC PostgreSQL — chỉ job 01b cần, nhưng thêm vào cả hai cho đỡ phải nhớ hai danh
# sách. Spark bỏ qua jar không dùng, chi phí là vài giây tải lần đầu rồi cache lại.
JDBC_PKGS="org.postgresql:postgresql:42.7.4"
ALL_PKGS="$ICEBERG_PKGS,$JDBC_PKGS"
LOG_DIR="out/bronze-runs"
mkdir -p "$LOG_DIR"

submit() {
  # $1 = tên lần chạy (dùng cho tên file log), $2 = file job, $@ còn lại = cờ cho job
  local name="$1"; shift
  local job="$1"; shift
  local log="$LOG_DIR/${name}.log"
  printf '==> %s  (log: %s)\n' "$name" "$log"
  docker exec "$MASTER_CONTAINER" /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages "$ALL_PKGS" \
    "/opt/spark_jobs/$job" "$@" > "$log" 2>&1
  grep -E "^run_id|^bảng (đích|nguồn)|^AQE|^ngày sẽ nạp|^chia theo|^số kết nối|^XONG|^Số dòng" \
    "$log" || tail -5 "$log"
}

case "$WHICH" in
  lan1|tatca)
    submit "lan1-20ngay-aqe-off" 01_bronze_baseline.py \
      --sample-days 20 --aqe off --table raw_amazon_orders_sample
    ;;&
  lan2|tatca)
    submit "lan2-20ngay-aqe-on" 01_bronze_baseline.py \
      --sample-days 20 --aqe on --table raw_amazon_orders_sample
    ;;&
  lan3|tatca)
    submit "lan3-180ngay-aqe-on" 01_bronze_baseline.py --aqe on --table raw_amazon_orders
    ;;&
  wal1|tatca)
    submit "wal1-jdbc-1ket-noi" 01b_bronze_walmart.py \
      --num-partitions 1 --table raw_walmart_orders_smoke
    ;;&
  wal16|tatca)
    submit "wal16-jdbc-16ket-noi" 01b_bronze_walmart.py \
      --num-partitions 16 --table raw_walmart_orders_smoke
    ;;&
  wal|tatca)
    submit "wal-that-16ket-noi" 01b_bronze_walmart.py \
      --num-partitions 16 --table raw_walmart_orders
    ;;&
  lan1|lan2|lan3|wal1|wal16|wal|tatca) ;;
  *) echo "Tham số phải là lan1 | lan2 | lan3 | wal1 | wal16 | wal | tatca" >&2; exit 2 ;;
esac

printf '\n==> Event log đã thu: %s\n' "$(ls -1 out/spark-events 2>/dev/null | wc -l) file"
printf '==> Xem lại bằng History Server:\n'
printf '    docker run --rm -p 18080:18080 -v "$PWD/out/spark-events:/spark-events" \\\n'
printf '      -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///spark-events" \\\n'
printf '      apache/spark:3.5.3-java17 /opt/spark/bin/spark-class \\\n'
printf '      org.apache.spark.deploy.history.HistoryServer\n'
printf '    rồi mở http://localhost:18080\n'
