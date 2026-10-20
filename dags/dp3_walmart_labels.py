"""Tập nhãn Walmart riêng cho huấn luyện, dùng bridge ba mã gốc."""

from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from fsds_spark_operator import FsdsSparkSubmitOperator as SparkSubmitOperator

with DAG(
    dag_id="dp3_walmart_labels",
    description="Nối nhãn Walmart với Gold fact qua ba mã Order Line",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["fsds", "dp3", "labels"],
) as dag:
    SparkSubmitOperator(
        task_id="build_and_validate_walmart_labels",
        application="/opt/spark_jobs/gold/build_walmart_order_snapshot_labels.py",
        application_args=[
            "--fact-table", "{{ var.value.fsds_dp3_gold_source_table }}",
            "--run-id", "{{ run_id }}",
        ],
        conn_id="spark_default",
        packages="{{ var.value.fsds_spark_packages }}",
        driver_memory="600m",
        executor_memory="1536m",
        executor_cores=1,
        conf={
            "spark.driver.host": "airflow",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.jars.ivy": "/opt/airflow/.ivy2",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m",
        },
    )
