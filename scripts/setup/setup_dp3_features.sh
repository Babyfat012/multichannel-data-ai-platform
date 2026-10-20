#!/usr/bin/env bash
# Nạp bảng nguồn/đích của DP3 vào Airflow; chạy sau setup/setup_airflow.sh.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

airflow_cmd() {
  docker compose --profile airflow exec -T airflow airflow "$@"
}

airflow_cmd variables set fsds_dp3_gold_source_table 'fact_order_lines_dp2'
airflow_cmd variables set fsds_dp3_feature_table 'feat_order_line_return_dp3'

printf 'DP3 feature Variables đã nạp.\n'
