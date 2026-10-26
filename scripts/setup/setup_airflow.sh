#!/usr/bin/env bash
# Nạp Connection và Variables của DP1 vào metadata DB Airflow, dùng chung cho các DAG.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

airflow_cmd() {
  docker compose --profile airflow exec -T airflow airflow "$@"
}

# Tạo credential Connections trong metadata DB, không đưa password qua argv.
docker compose --profile airflow exec -T airflow python /opt/platform/scripts/setup/bootstrap_airflow_config.py

# Airflow có thể tạo sẵn spark_default trỏ YARN. Thay đúng Connection này trong DB
# riêng của PLATFORM để chạy lại setup vẫn cho ra cấu hình như nhau.
airflow_cmd connections delete spark_default >/dev/null 2>&1 || true
airflow_cmd connections add spark_default \
  --conn-type spark --conn-host 'spark://spark-master' --conn-port 7077 \
  --conn-extra '{"deploy-mode":"client"}'

airflow_cmd variables set platform_spark_packages \
  'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.postgresql:postgresql:42.7.4'
airflow_cmd variables set platform_dp1_amazon_window_days '10'
airflow_cmd variables set platform_dp1_amazon_window_start '2026-04-05'
airflow_cmd variables set platform_dp1_amazon_table 'raw_amazon_orders_dp1'
airflow_cmd variables set platform_dp1_walmart_table 'raw_walmart_orders_dp1'

printf 'DP1 Connection + Variables đã nạp. DAG dùng bảng thử *_dp1.\n'

# Iceberg maintenance: đưa nguyên cấu hình JSON vào Variable vì configs/ chưa mount vào Airflow.
docker compose --profile airflow exec -T airflow python -c \
  'import sys; from airflow.models import Variable; Variable.set("platform_maintenance_config", sys.stdin.read())' \
  < configs/iceberg_maintenance.json

# DAG v2: đường dẫn trong container, fixture phải được planner mount trước khi trigger.
airflow_cmd variables set platform_order_lifecycle_receipt_dir '/opt/airflow/receipts/order_lifecycle'
airflow_cmd variables set platform_order_lifecycle_source_root '/data/order_lifecycle'
airflow_cmd variables set platform_order_lifecycle_cutoff '2026-06-01T00:00:00.000000Z'
airflow_cmd variables set platform_order_lifecycle_driver_memory '3g'
airflow_cmd variables set platform_order_lifecycle_executor_memory '4g'
airflow_cmd variables set platform_order_lifecycle_executor_cores '2'

# AWS (t3.xlarge): worker chỉ có 3g/1 core nên hạ executor; nguồn đọc qua s3a://.
if [ "${PLATFORM_STORAGE:-minio}" = s3 ]; then
  airflow_cmd variables set platform_spark_packages_aws "${PLATFORM_SPARK_PACKAGES_AWS:?Cần package AWS}"
  airflow_cmd variables set platform_order_lifecycle_source_root "s3a://${RAW_BUCKET:?}/order_lifecycle/${PLATFORM_DATASET_NAME:-fixture}"
  airflow_cmd variables set platform_order_lifecycle_cutoff "${PLATFORM_ORDER_LIFECYCLE_CUTOFF:-2026-06-01T00:00:00.000000Z}"
  airflow_cmd variables set platform_order_lifecycle_driver_memory '2g'
  airflow_cmd variables set platform_order_lifecycle_executor_memory '2g'
  airflow_cmd variables set platform_order_lifecycle_executor_cores '1'
  if [[ "${PLATFORM_RUNTIME_PROFILE:-standard}" == performance ]]; then
    airflow_cmd variables set platform_order_lifecycle_driver_memory '4g'
    airflow_cmd variables set platform_order_lifecycle_executor_memory '4g'
    airflow_cmd variables set platform_order_lifecycle_executor_cores '2'
  fi
fi

# CDC v2 riêng: oracle phải được planner tạo và mount trước khi trigger DAG paused.
airflow_cmd variables set platform_order_lifecycle_cdc_control_dir '/opt/airflow/platform-control/order_lifecycle_cdc'
airflow_cmd variables set platform_order_lifecycle_cdc_oracle '/opt/airflow/platform-control/order_lifecycle_cdc/order_lifecycle_cdc_expected.json'
airflow_cmd variables set platform_order_lifecycle_cdc_spark_packages 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3'

# ML Platform: kho feature. Redis nằm trong cụm kind (scripts/setup/setup_kind_feature_store.sh).
airflow_cmd variables set platform_feast_root 's3://platform/feature-store'
airflow_cmd variables set platform_feast_redis 'platform-control-plane:30379'
