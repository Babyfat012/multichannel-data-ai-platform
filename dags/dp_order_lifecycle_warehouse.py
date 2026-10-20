"""DAG chạy tay: validate Gold hiện tại rồi publish warehouse v2, không sửa DAG nguồn."""
from datetime import datetime, timezone

from airflow import DAG
from airflow.models.param import Param
from fsds_spark_operator import FsdsSparkSubmitOperator


class OrderLifecycleSparkSubmitOperator(FsdsSparkSubmitOperator):
    """Render tài nguyên v2 kể cả khi provider không template các trường này."""

    template_fields = tuple(dict.fromkeys((*FsdsSparkSubmitOperator.template_fields,
                                          "driver_memory", "executor_memory", "executor_cores")))


RUN_ID = '{{ params.run_id }}'
CUTOFF = '{{ params.cutoff }}'
DIRECTORY = '{{ var.value.fsds_order_lifecycle_receipt_dir }}/warehouse/{{ ts_nodash }}'
CONF = {'spark.driver.host': 'airflow', 'spark.driver.bindAddress': '0.0.0.0',
        'spark.jars.ivy': '/opt/airflow/.ivy2'}

with DAG(
    dag_id='dp_order_lifecycle_warehouse', schedule=None, catchup=False,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    max_active_runs=1, max_active_tasks=1, is_paused_upon_creation=True,
    params={'run_id': Param('', type='string', minLength=1,
                            description='generation_id Gold đã publish thành công'),
            'cutoff': Param('2026-06-01T00:00:00.000000Z', type='string', format='date-time')},
    tags=['fsds', 'v2', 'warehouse'],
) as dag:
    validate_gold = OrderLifecycleSparkSubmitOperator(
        task_id='validate_gold', application='/opt/spark_jobs/gold/validate_order_lifecycle.py',
        application_args=['--run-id', RUN_ID, '--cutoff', CUTOFF,
                          '--receipt', DIRECTORY + '/validate_gold.json'],
        conn_id='spark_default', deploy_mode='client', driver_memory="{{ var.value.fsds_order_lifecycle_driver_memory }}",
        executor_memory="{{ var.value.fsds_order_lifecycle_executor_memory }}",
        executor_cores="{{ var.value.fsds_order_lifecycle_executor_cores }}", conf=CONF,
        packages='{{ var.value.fsds_spark_packages }}',
    )
    export_warehouse = OrderLifecycleSparkSubmitOperator(
        task_id='export_warehouse', application='/opt/spark_jobs/warehouse/export_order_lifecycle.py',
        application_args=['--run-id', RUN_ID, '--cutoff', CUTOFF,
                          '--receipt', DIRECTORY + '/export_warehouse.json',
                          '--sql-dir', '{{ var.value.get("fsds_order_lifecycle_warehouse_sql_dir", "/opt/sql/warehouse_lifecycle") }}'],
        runtime_groups=('iceberg', 'postgres'),
        conn_id='spark_default', deploy_mode='client', driver_memory="{{ var.value.fsds_order_lifecycle_driver_memory }}",
        executor_memory="{{ var.value.fsds_order_lifecycle_executor_memory }}",
        executor_cores="{{ var.value.fsds_order_lifecycle_executor_cores }}", conf=CONF,
        packages='{{ var.value.fsds_spark_packages }}',
    )
    validate_gold >> export_warehouse
