#!/usr/bin/env bash
# Planner chạy harness CDC v2 cô lập; image/jar có sẵn, không tải mạng, trap dọn tài nguyên riêng.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
PYTHON="${PYTHON:-$PWD/.venv/bin/python}"
IMAGE=fsds-airflow:2.10.5-spark3.5.3
PREFIX="fsds-cdc-v2-$$"
WORK="$(mktemp -d /tmp/fsds-cdc-v2.XXXXXX)"
chmod 777 "$WORK"
NETWORK="$PREFIX-net"
cleanup() {
  # Chỉ dọn đúng container/mạng của harness; giữ WORK làm bằng chứng.
  docker rm -f "$PREFIX-spark" "$PREFIX-connect" "$PREFIX-kafka" "$PREFIX-postgres" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  printf 'Receipt/warehouse còn tại %s\n' "$WORK"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# Không tự tạo volume rỗng nếu cache chưa có.
docker volume inspect fsds_airflow-data >/dev/null
for image in postgres:15 apache/kafka:3.9.1 quay.io/debezium/connect:3.4 "$IMAGE"; do
  docker image inspect "$image" >/dev/null
done
# ls bắt buộc để planner thấy cache thực, rồi kiểm đủ dependency Kafka Spark offline.
docker run --rm --pull never --network none --name "$PREFIX-spark" \
  -v fsds_airflow-data:/ivy:ro --entrypoint bash "$IMAGE" -c '
  ls /ivy/.ivy2/jars
  for pattern in "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.11.0.jar" \
    "org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.3.jar" \
    "org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.3.jar" \
    "org.apache.kafka_kafka-clients-*.jar" "org.apache.commons_commons-pool2-*.jar"; do
    compgen -G "/ivy/.ivy2/jars/$pattern" >/dev/null || { echo "THIẾU JAR: $pattern" >&2; exit 1; }
  done'
# Chọn cổng host trống; mọi container dùng DNS riêng trong network mới.
read -r PG_PORT KAFKA_PORT CONNECT_PORT < <("$PYTHON" - <<'PY'
"""Giữ ba socket đồng thời để chọn ba cổng host khác nhau."""
import socket
sockets = [socket.socket() for _ in range(3)]
for sock in sockets:
    sock.bind(("127.0.0.1", 0))
print(*(sock.getsockname()[1] for sock in sockets))
for sock in sockets:
    sock.close()
PY
)
docker network create "$NETWORK" >/dev/null
docker run -d --pull never --name "$PREFIX-postgres" --network "$NETWORK" --network-alias postgres \
  -p "127.0.0.1:$PG_PORT:5432" -e POSTGRES_USER=cdc -e POSTGRES_PASSWORD=cdc -e POSTGRES_DB=cdc \
  postgres:15 -c wal_level=logical >/dev/null
docker run -d --pull never --name "$PREFIX-kafka" --network "$NETWORK" --network-alias kafka \
  -p "127.0.0.1:$KAFKA_PORT:9094" -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
  -e KAFKA_LISTENERS=INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:9094 \
  -e "KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:9092,EXTERNAL://127.0.0.1:$KAFKA_PORT" \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_NUM_PARTITIONS=1 \
  apache/kafka:3.9.1 >/dev/null
docker run -d --pull never --name "$PREFIX-connect" --network "$NETWORK" \
  -p "127.0.0.1:$CONNECT_PORT:8083" -e BOOTSTRAP_SERVERS=kafka:9092 -e GROUP_ID=cdc-v2 \
  -e CONFIG_STORAGE_TOPIC=cdc_v2_configs -e OFFSET_STORAGE_TOPIC=cdc_v2_offsets \
  -e STATUS_STORAGE_TOPIC=cdc_v2_status -e CONFIG_STORAGE_REPLICATION_FACTOR=1 \
  -e OFFSET_STORAGE_REPLICATION_FACTOR=1 -e STATUS_STORAGE_REPLICATION_FACTOR=1 \
  quay.io/debezium/connect:3.4 >/dev/null
cat > "$WORK/harness.env" <<ENV
POSTGRES_USER=cdc
POSTGRES_PASSWORD=cdc
POSTGRES_DB=cdc
POSTGRES_PORT=$PG_PORT
POSTGRES_HOST=127.0.0.1
POSTGRES_CONNECT_HOST=postgres
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:$KAFKA_PORT
DEBEZIUM_URL=http://127.0.0.1:$CONNECT_PORT
ENV
"$PYTHON" tests/harness/control_order_lifecycle_cdc.py services --env-file "$WORK/harness.env"
"$PYTHON" -m generator.generate_order_lifecycle_fixture --output "$WORK/fixture"
"$PYTHON" scripts/setup/load_order_lifecycle_walmart.py --root "$WORK/fixture" --env-file "$WORK/harness.env"
"$PYTHON" scripts/setup/setup_order_lifecycle_debezium.py --env-file "$WORK/harness.env"
"$PYTHON" tests/harness/control_order_lifecycle_cdc.py connector --env-file "$WORK/harness.env"
"$PYTHON" scripts/run/mutate_order_lifecycle_cdc.py --env-file "$WORK/harness.env" --seed 11 --output "$WORK/order_lifecycle_cdc_expected.json"
"$PYTHON" tests/harness/control_order_lifecycle_cdc.py events --env-file "$WORK/harness.env" \
  --oracle "$WORK/order_lifecycle_cdc_expected.json" --receipt "$WORK/poll.json"
spark_job() {
  # Driver local dùng Hadoop catalog trong /tmp, không trỏ Lakekeeper hay tải package.
  # Chạy bằng user mặc định của image (uid host không có trong passwd ⇒ Hadoop login NPE, và không đọc được
  # /home/airflow/.local). WORK mở quyền để container ghi; xong thì container root trả quyền file về uid host.
  find "$WORK" -user "$(id -u)" -exec chmod a+rwX {} +
  local status=0
  docker run --rm --pull never --name "$PREFIX-spark" --network "$NETWORK" \
    -e SPARK_LOCAL_HOSTNAME=localhost -e PYTHONPATH=/workspace \
    -v "$PWD":/workspace:ro -v "$WORK":/proof -v fsds_airflow-data:/ivy:ro \
    --entrypoint bash "$IMAGE" -c '
      jars=$(find /ivy/.ivy2/jars -maxdepth 1 -name "*.jar" -printf "%p," )
      "$SPARK_HOME/bin/spark-submit" --master "local[2]" --driver-memory 3g --conf spark.jars.ivy=/tmp/.ivy2 --jars "${jars%,}" "$@"
    ' spark-submit "$@" || status=$?
  docker run --rm --pull never --user 0 -v "$WORK":/proof --entrypoint chown "$IMAGE" -R "$(id -u):$(id -g)" /proof
  return "$status"
}
# Trong container /proof/warehouse cũng là thư mục WORK/warehouse dưới /tmp trên host.
spark_job /workspace/spark_jobs/cdc/ingest_order_lifecycle_changes.py --bootstrap kafka:9092 --run-id harness-v2 \
  --boundary-dir /proof/boundary --receipt /proof/ingest.json --local-warehouse file:///proof/warehouse
validate() {
  # Dùng đúng oracle và receipt của ingest, luôn kiểm cả dòng sai epoch cùng run.
  spark_job /workspace/spark_jobs/cdc/validate_order_lifecycle_changes.py --run-id harness-v2 \
    --oracle /proof/order_lifecycle_cdc_expected.json --ingest-receipt /proof/ingest.json \
    --boundary-dir /proof/boundary --receipt "/proof/$1" --local-warehouse file:///proof/warehouse
}
validate validate.json
cat > "$WORK/inject.py" <<'PY'
"""Tiêm một bản ghi giả vào Bronze để kiểm validator báo extra."""
from pyspark.sql import functions as F
from spark_jobs.lib.order_lifecycle_cdc import TARGET, build_session
spark = build_session("inject-extra-cdc-v2", "file:///proof/warehouse")
try:
    row = (spark.table(TARGET).limit(1).withColumn("purchase_order_id", F.lit("FAKE-ORDER"))
           .withColumn("kafka_offset", F.lit(999999).cast("long")))
    row.writeTo(TARGET).append()
finally:
    spark.stop()
PY
spark_job /proof/inject.py
if validate validate-extra.json; then
  echo 'LỖI: validator phải thất bại khi có extra' >&2
  exit 1
fi
"$PYTHON" - "$WORK/validate-extra.json" <<'PY'
"""Xác nhận exit lỗi thực sự do bản ghi thừa, không do lỗi khởi động Spark."""
import json
import sys
receipt = json.load(open(sys.argv[1]))
assert receipt["status"] == "failed" and receipt["checks"]["extra"] == 1, receipt
PY
printf 'CDC v2 harness PASS; proof: %s\n' "$WORK"
