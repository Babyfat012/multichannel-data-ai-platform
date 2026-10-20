#!/usr/bin/env bash
# Nạp riêng Variables của chặng Silver DP2; không đặt lại cấu hình DP1.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

airflow_cmd() {
  docker compose --profile airflow exec -T airflow airflow "$@"
}

airflow_cmd variables set fsds_dp2_amazon_bronze_table 'raw_amazon_orders_dp1'
airflow_cmd variables set fsds_dp2_walmart_bronze_table 'raw_walmart_orders_dp1'
airflow_cmd variables set fsds_dp2_amazon_silver_table 'stg_amazon_order_lines_dp2'
airflow_cmd variables set fsds_dp2_walmart_silver_table 'stg_walmart_order_lines_dp2'
airflow_cmd variables set fsds_dp2_gold_table 'fact_order_lines_dp2'
airflow_cmd variables set fsds_dp2_amazon_dim_table 'dim_amazon_order_dp2'

printf 'DP2 Silver + Gold Variables đã nạp; nguồn *_dp1, đích *_dp2.\n'
