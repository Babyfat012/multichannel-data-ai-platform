#!/usr/bin/env bash
# Chạy job Bronze 01 ba lần để thu bằng chứng tối ưu hiệu năng cho phép đo.
#
# Vì sao ba lần (chốt 2026-10-10):
#   Lần 1 — 20 ngày, AQE TẮT  -> ảnh Spark UI thấy task lệch nhau (skew) + số giây.
#   Lần 2 — 20 ngày, AQE BẬT  -> ảnh task đều + số giây. CÙNG 20 ngày nên so giây được.
#   Lần 3 — 180 ngày, AQE BẬT -> bảng Bronze thật, đầy đủ.
# Hai lần đầu dùng bảng riêng `raw_amazon_orders_sample` để không đè bảng thật.
# 20 ngày được chọn RẢI ĐỀU trên cả dải (xem spark_jobs/lib/day_selection.py): lấy 20 ngày
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
# Brief 02 (handle skew) có bộ ba riêng, KHÔNG nằm trong `tatca`. Cả ba chạy CÙNG 20 ngày
# mẫu, AQE TẮT như baseline lan1 (72,3s), chỉ đổi cấu hình — job bronze/ingest_amazon_order_snapshots.py
# giữ nguyên. Mỗi lần ghi vào một bảng thử `raw_amazon_orders_skew_*`, đo xong thì DROP:
#   skew_0 — ĐỐI CHỨNG: đúng cấu hình baseline lan1 (shuffle 200, AQE tắt), nhưng trong CÙNG
#            môi trường với skew_a/b/c (driver ở worker-1). Đã chạy 3 lần để đo dao động giữa
#            các lần giống hệt nhau: 55,9 / 63,2 / 64,8s => nhiễu 8,9s (14,5%).
#            Baseline của mọi so sánh = trung bình ba lần này = 61,3s.
#   skew_a — spark.sql.shuffle.partitions 200 -> 24. Giả thuyết: KHÔNG chữa skew (ngày to
#            nhất vẫn dồn một task), chỉ bớt task rỗng.
#   skew_b — write.distribution-mode=none. Giả thuyết: bỏ shuffle nên nhanh hơn, đổi lại
#            nhiều file nhỏ hơn.
#   skew_c — write.distribution-mode=range. Giả thuyết: cân tải tốt hơn hash, nhưng một
#            export_date đơn lẻ vẫn không chia được.
# Driver chạy ở worker-1 (limit 2,5 GB) với --driver-memory 600m, vì spark-master chỉ 512 MB
# và bị Docker kill OOM. `table-default.*` là thuộc tính catalog Iceberg: áp cho bảng MỚI
# tạo, nên chỉ bảng thử bị đổi, mặc định của job và hai bảng thật không đổi.
#
# Dùng:  bash scripts/evidence/measure_order_snapshots_bronze.sh [lan1|lan2|lan3|wal1|wal16|wal|tatca|skew_0|skew_a|skew_b|skew_c|drop_skew]
# AWS (trên EC2, /opt/platform): PLATFORM_STORAGE=s3 bash scripts/evidence/measure_order_snapshots_bronze.sh skew_0
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

WHICH="${1:-tatca}"
MASTER_CONTAINER="${MASTER_CONTAINER:-platform-spark-master-1}"
ICEBERG_PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
# Driver JDBC PostgreSQL — chỉ job 01b cần, nhưng thêm vào cả hai cho đỡ phải nhớ hai danh
# sách. Spark bỏ qua jar không dùng, chi phí là vài giây tải lần đầu rồi cache lại.
JDBC_PKGS="org.postgresql:postgresql:42.7.4"
ALL_PKGS="$ICEBERG_PKGS,$JDBC_PKGS"
# AWS: /opt/platform do rsync (sudo) ghi nên không chắc ghi được; log để trong HOME của ubuntu.
if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then LOG_DIR="${LOG_DIR:-$HOME/bronze-runs}"; else LOG_DIR="${LOG_DIR:-out/bronze-runs}"; fi
mkdir -p "$LOG_DIR"

submit() {
  # $1 = tên lần chạy (dùng cho tên file log), $2 = file job, $@ còn lại = cờ cho job
  local name="$1"; shift
  local job="$1"; shift
  local log="$LOG_DIR/${name}.log"
  printf '==> %s  (log: %s)\n' "$name" "$log"
  if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then
    # AWS (chạy ở /opt/platform trên EC2): driver trong container airflow như DAG; gói AWS và
    # nguồn s3a:// lấy từ biến AIRFLOW_VAR_* có sẵn trong container, credential qua IAM role.
    docker compose exec -T airflow bash -c 'job="$1"; shift; exec spark-submit \
      --master spark://spark-master:7077 --conf spark.driver.host=airflow \
      --conf spark.driver.bindAddress=0.0.0.0 --conf spark.jars.ivy=/opt/airflow/.ivy2 \
      --packages "$AIRFLOW_VAR_PLATFORM_SPARK_PACKAGES" '"${EXTRA_SUBMIT_ARGS:-}"' \
      "/opt/spark_jobs/$job" --source "$AIRFLOW_VAR_PLATFORM_DP1_AMAZON_SOURCE" "$@"' \
      _ "$job" "$@" > "$log" 2>&1
  else
    docker exec "$MASTER_CONTAINER" /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages "$ALL_PKGS" \
      ${EXTRA_SUBMIT_ARGS:-} \
      "/opt/spark_jobs/$job" "$@" > "$log" 2>&1
  fi
  grep -E "^run_id|^bảng (đích|nguồn)|^AQE|^ngày sẽ nạp|^chia theo|^số kết nối|^XONG|^Số dòng" \
    "$log" || tail -5 "$log"
}

case "$WHICH" in
  lan1|tatca)
    submit "lan1-20ngay-aqe-off" bronze/ingest_amazon_order_snapshots.py \
      --sample-days 20 --aqe off --table raw_amazon_orders_sample
    ;;&
  lan2|tatca)
    submit "lan2-20ngay-aqe-on" bronze/ingest_amazon_order_snapshots.py \
      --sample-days 20 --aqe on --table raw_amazon_orders_sample
    ;;&
  lan3|tatca)
    submit "lan3-180ngay-aqe-on" bronze/ingest_amazon_order_snapshots.py --aqe on --table raw_amazon_orders
    ;;&
  wal1|tatca)
    submit "wal1-jdbc-1ket-noi" bronze/ingest_walmart_order_snapshots.py \
      --num-partitions 1 --table raw_walmart_orders_smoke
    ;;&
  wal16|tatca)
    submit "wal16-jdbc-16ket-noi" bronze/ingest_walmart_order_snapshots.py \
      --num-partitions 16 --table raw_walmart_orders_smoke
    ;;&
  wal|tatca)
    submit "wal-that-16ket-noi" bronze/ingest_walmart_order_snapshots.py \
      --num-partitions 16 --table raw_walmart_orders
    ;;&
  skew_0)
    # Brief 02, đối chứng: cấu hình y hệt lan1 (shuffle 200 mặc định, AQE tắt), môi trường của skew_a/b/c.
    MASTER_CONTAINER=platform-spark-worker-1-1 EXTRA_SUBMIT_ARGS="--driver-memory 600m" \
      submit "skew_0-doichung" bronze/ingest_amazon_order_snapshots.py \
        --sample-days 20 --aqe off --table raw_amazon_orders_skew_0
    ;;
  skew_a)
    # Brief 02, phương pháp a: giảm số partition shuffle 200 -> 24 (chỉ đổi cờ có sẵn của job).
    MASTER_CONTAINER=platform-spark-worker-1-1 EXTRA_SUBMIT_ARGS="--driver-memory 600m" \
      submit "skew_a-shuffle24" bronze/ingest_amazon_order_snapshots.py \
        --sample-days 20 --aqe off --shuffle-partitions 24 --table raw_amazon_orders_skew_a
    ;;
  skew_b)
    # Brief 02, phương pháp b: write.distribution-mode=none cho bảng thử (không shuffle trước khi ghi).
    MASTER_CONTAINER=platform-spark-worker-1-1 \
      EXTRA_SUBMIT_ARGS="--driver-memory 600m --conf spark.sql.catalog.lakehouse.table-default.write.distribution-mode=none" \
      submit "skew_b-dist-none" bronze/ingest_amazon_order_snapshots.py \
        --sample-days 20 --aqe off --table raw_amazon_orders_skew_b
    ;;
  skew_c)
    # Brief 02, phương pháp c: write.distribution-mode=range cho bảng thử.
    MASTER_CONTAINER=platform-spark-worker-1-1 \
      EXTRA_SUBMIT_ARGS="--driver-memory 600m --conf spark.sql.catalog.lakehouse.table-default.write.distribution-mode=range" \
      submit "skew_c-dist-range" bronze/ingest_amazon_order_snapshots.py \
        --sample-days 20 --aqe off --table raw_amazon_orders_skew_c
    ;;
  drop_skew)
    # Xoá bảng thử sau khi đo (Trino phải đang chạy: docker compose --profile query up -d trino).
    for t in skew_0 skew_a skew_b skew_c; do
      docker compose exec -T trino trino --execute "DROP TABLE IF EXISTS lakehouse.bronze.raw_amazon_orders_$t"
    done
    ;;
  lan1|lan2|lan3|wal1|wal16|wal|tatca|skew_0|skew_a|skew_b|skew_c) ;;
  *) echo "Tham số phải là lan1 | lan2 | lan3 | wal1 | wal16 | wal | tatca | skew_0 | skew_a | skew_b | skew_c | drop_skew" >&2; exit 2 ;;
esac

if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then
  printf '\n==> AWS: event log ở /mnt/platform-data/spark-events (chủ 50000:root, quyền 660 ⇒ History thêm nhóm root: --group-add 0; --user 50000 lỗi vì uid không có tên). History Server (loopback, tunnel 18080):\n'
  printf '    docker run -d --name spark-history --group-add 0 -p 127.0.0.1:18080:18080 -v /mnt/platform-data/spark-events:/spark-events:ro \\\n'
  printf '      -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///spark-events" \\\n'
  printf '      apache/spark:3.5.3-java17 /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer\n'
  exit 0
fi
printf '\n==> Event log đã thu: %s\n' "$(ls -1 out/spark-events 2>/dev/null | wc -l) file"
printf '==> Xem lại bằng History Server:\n'
printf '    docker run --rm -p 18080:18080 -v "$PWD/out/spark-events:/spark-events" \\\n'
printf '      -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///spark-events" \\\n'
printf '      apache/spark:3.5.3-java17 /opt/spark/bin/spark-class \\\n'
printf '      org.apache.spark.deploy.history.HistoryServer\n'
printf '    rồi mở http://localhost:18080\n'
