"""MLE: Gold → Parquet offline (feature SKU + tập train) → feast apply → materialize vào Redis online.

`end` là mốc thời gian GIẢ LẬP (mặc định = cutoff Gold), không phải giờ thật: mỗi lần chạy
Feast chỉ nạp ảnh chụp có event_timestamp trong (lần trước, end].
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from fsds_spark_operator import FsdsSparkSubmitOperator

FEAST = "/home/airflow/feast-venv/bin/feast"
REPO = "/opt/feature_repo"
CUTOFF = "{{ var.value.fsds_order_lifecycle_cutoff }}"
# Spark ghi qua s3a://, Feast đọc cùng chỗ qua s3://.
SPARK_ROOT = "{{ var.value.fsds_feast_root | replace('s3://', 's3a://') }}"
SPARK_OUTPUT = SPARK_ROOT + "/sku_daily"
CONF = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
}


def run_feast(*args):
    """Chạy CLI Feast trên bản sao repo (mount read-only), credential lấy lúc chạy."""
    minio = BaseHook.get_connection("fsds_minio")
    root = Variable.get("fsds_feast_root")
    env = {
        **os.environ,
        "AWS_ACCESS_KEY_ID": minio.login,
        "AWS_SECRET_ACCESS_KEY": minio.password,
        "FEAST_S3_ENDPOINT_URL": Variable.get("fsds_minio_internal_endpoint"),
        "FEAST_REGISTRY": f"{root}/registry.db",
        "FEAST_SKU_DAILY_PATH": f"{root}/sku_daily",
        "FEAST_SKU_RETURN_24H_PATH": f"{root}/sku_return_24h.parquet",
        "FEAST_REDIS": Variable.get("fsds_feast_redis"),
    }
    with tempfile.TemporaryDirectory() as tmp:
        repo = shutil.copytree(REPO, f"{tmp}/repo")
        result = subprocess.run(
            [FEAST, *args], cwd=repo, env=env, capture_output=True, text=True, check=False
        )
    print(result.stdout, result.stderr, sep="\n")
    result.check_returncode()


with DAG(
    dag_id="ml_feature_materialize",
    description="Feature SKU theo ngày: offline Parquet → Redis online (rubric mle 11)",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    params={"end": Param("", type="string", description="Mốc giả lập; trống = cutoff Gold")},
    tags=["fsds", "mle", "feature-store"],
) as dag:
    export = FsdsSparkSubmitOperator(
        task_id="export_sku_daily",
        application="/opt/spark_jobs/gold/export_sku_daily_features.py",
        application_args=["--cutoff", CUTOFF, "--output", SPARK_OUTPUT],
        conn_id="spark_default",
        packages="{{ var.value.fsds_spark_packages }},org.apache.hadoop:hadoop-aws:3.3.4",
        driver_memory="1g",
        conf=CONF,
    )
    # Tập train (entity df cho KFP): line có nhãn + thuộc tính lúc đặt, cùng generation Gold với feature.
    export_training = FsdsSparkSubmitOperator(
        task_id="export_training_lines",
        application="/opt/spark_jobs/gold/export_training_lines.py",
        application_args=["--output", SPARK_ROOT + "/training_lines"],
        conn_id="spark_default",
        packages="{{ var.value.fsds_spark_packages }},org.apache.hadoop:hadoop-aws:3.3.4",
        driver_memory="1g",
        conf=CONF,
    )
    apply = PythonOperator(task_id="feast_apply", python_callable=run_feast, op_args=["apply"])
    materialize = PythonOperator(
        task_id="materialize_incremental",
        python_callable=run_feast,
        op_args=[
            "materialize-incremental",
            # Chỉ view batch; view stream do pusher ghi thẳng vào Redis.
            "--views",
            "sku_return_daily",
            # Python 3.10 không đọc được đuôi "Z" của fromisoformat.
            "{{ (params.end or var.value.fsds_order_lifecycle_cutoff) | replace('Z', '+00:00') }}",
        ],
    )
    export >> export_training >> apply >> materialize
