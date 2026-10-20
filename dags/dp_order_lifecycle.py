"""DAG chạy tay nguồn v2 → Silver có hợp đồng → Gold và kiểm nhãn/feature.

Chỉ đọc cấu hình tại runtime; Spark xuất receipt Bronze có schema thật.
Operator chỉ ghi receipt lỗi khi Spark thất bại.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from fsds_spark_operator import FsdsSparkSubmitOperator


class OrderLifecycleSparkSubmitOperator(FsdsSparkSubmitOperator):
    """Render tài nguyên v2 kể cả khi provider không template các trường này."""

    template_fields = tuple(dict.fromkeys((*FsdsSparkSubmitOperator.template_fields,
                                          "driver_memory", "executor_memory", "executor_cores")))


DIRECTORY = "{{ var.value.fsds_order_lifecycle_receipt_dir }}/{{ ts_nodash }}"
SOURCE_ROOT = "{{ params.source_root or var.value.fsds_order_lifecycle_source_root }}"
CUTOFF = "{{ params.cutoff or var.value.fsds_order_lifecycle_cutoff }}"
RUN_ID = "{{ run_id }}"
PACKAGES = "{{ var.value.fsds_spark_packages }}"
CONF = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
}


def write_receipt(path, document):
    """Lưu receipt có thời gian hoàn tất thật vào thư mục của lần chạy."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    document["completed_at"] = datetime.now(timezone.utc).isoformat()
    target.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n")


def check_registry(receipt, run_id):
    """Chạy CLI hợp đồng bằng Python sẵn có, lưu cả lỗi rồi truyền exit code."""
    result = subprocess.run(
        [sys.executable, "-m", "spark_jobs.lib.data_contracts", "check-registry",
         "--contracts-dir", "/opt/contracts"],
        cwd="/opt", capture_output=True, text=True, check=False,
    )
    write_receipt(receipt, {"run_id": run_id, "task_id": "check_contracts",
                            "status": "passed" if result.returncode == 0 else "failed",
                            "exit_code": result.returncode, "stdout": result.stdout,
                            "stderr": result.stderr})
    if result.returncode:
        raise RuntimeError(f"check-registry thất bại; xem receipt {receipt}")


class OrderLifecycleBronzeOperator(OrderLifecycleSparkSubmitOperator):
    """Bổ sung JDBC tùy chọn và receipt lỗi cho job 20."""

    template_fields = (*OrderLifecycleSparkSubmitOperator.template_fields, "receipt_path", "walmart_jdbc")

    def __init__(self, *, receipt_path, walmart_jdbc, **kwargs):
        """Nhận đường dẫn receipt và tên bảng JDBC đã được Airflow template."""
        self.receipt_path = receipt_path
        self.walmart_jdbc = walmart_jdbc
        super().__init__(**kwargs)

    def execute(self, context):
        """Giữ receipt Spark thành công; ghi receipt lỗi rồi truyền nguyên lỗi Spark."""
        original_args = self.application_args
        document = {"run_id": context["run_id"], "task_id": "ingest_bronze",
                    "status": "failed", "checks_available": False,
                    "tables": {"raw_source_records": "lakehouse.bronze.lifecycle_raw_records"},
                    "note": "Spark thất bại; xem log job 20, receipt lỗi không xác nhận metrics/schema."}
        try:
            self.application_args = list(original_args)
            if self.walmart_jdbc:
                self.application_args += ["--walmart-jdbc", self.walmart_jdbc]
            return super().execute(context)
        except Exception:
            write_receipt(self.receipt_path, document)
            raise
        finally:
            self.application_args = original_args


with DAG(
    dag_id="dp_order_lifecycle",
    description="Hợp đồng → Bronze v2 → Silver v2 → Gold v2 → Validate",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None, catchup=False, max_active_runs=1, max_active_tasks=1,
    is_paused_upon_creation=True,
    params={
        "source_root": Param(None, type=["null", "string"], minLength=1,
                             description="Thư mục fixture trong container; null dùng Variable"),
        "cutoff": Param(None, type=["null", "string"], format="date-time",
                        description="Cutoff UTC; null dùng Variable"),
        "walmart_jdbc": Param(None, type=["null", "string"],
                              pattern=r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$",
                              description="Ví dụ walmart_lifecycle.orders; null đọc bootstrap file"),
    },
    tags=["fsds", "v2", "contracts"],
) as dag:
    check_contracts = PythonOperator(
        task_id="check_contracts", python_callable=check_registry,
        op_kwargs={"receipt": DIRECTORY + "/check_contracts.json", "run_id": RUN_ID},
    )
    ingest_bronze = OrderLifecycleBronzeOperator(
        task_id="ingest_bronze", application="/opt/spark_jobs/bronze/ingest_order_lifecycle.py",
        application_args=["--root", SOURCE_ROOT, "--run-id", RUN_ID,
                          "--receipt", DIRECTORY + "/ingest_bronze.json"],
        walmart_jdbc="{{ params.walmart_jdbc or '' }}",
        receipt_path=DIRECTORY + "/ingest_bronze.json",
        runtime_groups=("iceberg", "postgres"),
        conn_id="spark_default", deploy_mode="client", packages=PACKAGES,
        driver_memory="{{ var.value.fsds_order_lifecycle_driver_memory }}",
        executor_memory="{{ var.value.fsds_order_lifecycle_executor_memory }}",
        executor_cores="{{ var.value.fsds_order_lifecycle_executor_cores }}", conf=CONF,
    )
    build_silver = OrderLifecycleSparkSubmitOperator(
        task_id="build_silver", application="/opt/spark_jobs/silver/build_order_lifecycle.py",
        application_args=["--run-id", RUN_ID, "--as-of", CUTOFF,
                          "--contracts-dir", "/opt/contracts", "--receipt", DIRECTORY + "/build_silver.json"],
        conn_id="spark_default", deploy_mode="client", packages=PACKAGES,
        driver_memory="{{ var.value.fsds_order_lifecycle_driver_memory }}",
        executor_memory="{{ var.value.fsds_order_lifecycle_executor_memory }}",
        executor_cores="{{ var.value.fsds_order_lifecycle_executor_cores }}", conf=CONF,
    )
    build_gold = OrderLifecycleSparkSubmitOperator(
        task_id="build_gold", application="/opt/spark_jobs/gold/build_order_lifecycle.py",
        application_args=["--run-id", RUN_ID, "--cutoff", CUTOFF, "--receipt", DIRECTORY + "/build_gold.json"],
        conn_id="spark_default", deploy_mode="client", packages=PACKAGES,
        driver_memory="{{ var.value.fsds_order_lifecycle_driver_memory }}",
        executor_memory="{{ var.value.fsds_order_lifecycle_executor_memory }}",
        executor_cores="{{ var.value.fsds_order_lifecycle_executor_cores }}", conf=CONF,
    )
    validate_gold = OrderLifecycleSparkSubmitOperator(
        task_id="validate_gold", application="/opt/spark_jobs/gold/validate_order_lifecycle.py",
        application_args=["--run-id", RUN_ID, "--cutoff", CUTOFF,
                          "--receipt", DIRECTORY + "/validate_gold.json"],
        conn_id="spark_default", deploy_mode="client", packages=PACKAGES,
        driver_memory="{{ var.value.fsds_order_lifecycle_driver_memory }}",
        executor_memory="{{ var.value.fsds_order_lifecycle_executor_memory }}",
        executor_cores="{{ var.value.fsds_order_lifecycle_executor_cores }}", conf=CONF,
    )
    check_contracts >> ingest_bronze >> build_silver >> build_gold >> validate_gold
