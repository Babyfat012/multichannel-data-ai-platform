"""DE-N2: lịch bảo trì Iceberg hằng ngày, mặc định tạm dừng, bật tay sau khi kiểm tra."""

from datetime import datetime, timezone

from airflow import DAG
from fsds_spark_operator import FsdsSparkSubmitOperator

TABLES = "{{ var.value.fsds_maintenance_tables }}"
DIRECTORY = "{{ var.value.fsds_maintenance_receipt_dir }}/{{ ts_nodash }}"
APPLICATION = "/opt/spark_jobs/maintenance/maintain_iceberg_tables.py"
CONFIG = "{{ var.value.fsds_maintenance_config }}"
CONF = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
}

with DAG(
    dag_id="maintenance_iceberg",
    description="Lập kế hoạch → bảo trì + kiểm dữ liệu → kiểm receipt bằng session mới",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["fsds", "de-n2", "iceberg"],
) as dag:
    plan = FsdsSparkSubmitOperator(
        task_id="plan",
        application=APPLICATION,
        application_args=[
            "--plan-only",
            "--tables",
            TABLES,
            "--config-json",
            CONFIG,
            "--receipt",
            DIRECTORY + "/plan.json",
        ],
        conn_id="spark_default",
        packages="{{ var.value.fsds_spark_packages }}",
        driver_memory="600m",
        conf=CONF,
    )
    execute = FsdsSparkSubmitOperator(
        task_id="execute",
        application=APPLICATION,
        application_args=[
            "--execute",
            "--tables",
            TABLES,
            "--config-json",
            CONFIG,
            "--receipt",
            DIRECTORY + "/execute.json",
        ],
        conn_id="spark_default",
        packages="{{ var.value.fsds_spark_packages }}",
        driver_memory="600m",
        conf=CONF,
    )
    validate = FsdsSparkSubmitOperator(
        task_id="validate",
        application=APPLICATION,
        application_args=["--validate-receipt", DIRECTORY + "/execute.json"],
        conn_id="spark_default",
        packages="{{ var.value.fsds_spark_packages }}",
        driver_memory="600m",
        conf=CONF,
    )
    plan >> execute >> validate
