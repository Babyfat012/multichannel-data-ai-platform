"""DP3: Gold fact -> offline feature lúc đặt Order -> Validate."""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from fsds_spark_operator import FsdsSparkSubmitOperator as SparkSubmitOperator

PACKAGES = "{{ var.value.fsds_spark_packages }}"
SOURCE = "{{ var.value.fsds_dp3_gold_source_table }}"
TARGET = "{{ var.value.fsds_dp3_feature_table }}"
RUN_ID = "{{ run_id }}"
SPARK_CONF = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
}

with DAG(
    dag_id="dp3_features",
    description="Gold fact năm Channel → feature offline lúc đặt Order; kiểm độ đầy đủ",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["fsds", "dp3", "feature"],
) as dag:
    build = SparkSubmitOperator(
        task_id="build_return_features",
        application="/opt/spark_jobs/gold/build_order_snapshot_return_features.py",
        application_args=["--source-table", SOURCE, "--table", TARGET,
                          "--run-id", RUN_ID],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )
    validate = SparkSubmitOperator(
        task_id="validate_return_features",
        application="/opt/spark_jobs/gold/validate_order_snapshot_return_features.py",
        application_args=["--source-table", SOURCE, "--table", TARGET,
                          "--run-id", RUN_ID],
        conn_id="spark_default",
        packages=PACKAGES,
        driver_memory="600m",
        conf=SPARK_CONF,
    )
    build >> validate
