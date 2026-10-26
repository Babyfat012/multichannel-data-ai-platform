#!/usr/bin/env bash
# Nạp Variables Silver cho ba Channel Batch mới; giữ nguyên Amazon/Walmart.
set -euo pipefail
cd "$(dirname "$0")/../.."
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

airflow_cmd() {
  docker compose --profile airflow exec -T airflow airflow "$@"
}

for channel in tiktok_shop shopify shopee; do
  airflow_cmd variables set "platform_dp2_${channel}_bronze_table" "raw_${channel}_orders_dp1"
  airflow_cmd variables set "platform_dp2_${channel}_silver_table" "stg_${channel}_order_lines_dp2"
done

printf 'DP2 Silver Variables cho ba Channel mới đã nạp.\n'
