"""Spark operator that fetches shared Airflow configuration only at task runtime."""

from __future__ import annotations

import os

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from platform_runtime import resolve_runtime_env


class PlatformSparkSubmitOperator(SparkSubmitOperator):
    """Inject selected connection groups into spark-submit without templating secrets."""

    template_fields = tuple(
        field for field in SparkSubmitOperator.template_fields if field != "env_vars"
    )

    def __init__(self, *, runtime_groups: tuple[str, ...] = ("iceberg",), **kwargs):
        """Create a Spark task with the Airflow connection groups it needs."""
        self.runtime_groups = runtime_groups
        super().__init__(**kwargs)

    def execute(self, context):
        """Resolve credentials at execution time and restore operator state afterward."""
        conn_ids = {
            "iceberg": "platform_minio",
            "postgres": "platform_postgres",
            "kafka": "platform_kafka",
        }
        storage = os.environ.get("PLATFORM_STORAGE", "minio")
        connections = {}
        for group in self.runtime_groups:
            if group == "iceberg" and storage != "minio":
                continue
            conn = BaseHook.get_connection(conn_ids[group])
            connections[conn_ids[group]] = {
                "host": conn.host,
                "port": conn.port,
                "schema": conn.schema,
                "login": conn.login,
                "password": conn.password,
            }
        variable_names = (
            ("platform_warehouse_name", "platform_lakekeeper_catalog_uri")
            if "iceberg" in self.runtime_groups
            else ()
        ) + (("platform_dp1_cdc_topic",) if "kafka" in self.runtime_groups else ())
        if "iceberg" in self.runtime_groups and storage == "minio":
            variable_names += ("platform_minio_internal_endpoint",)
        variables = {name: Variable.get(name) for name in variable_names}
        original_env = self.env_vars
        self.env_vars = {
            "PYTHONPATH": os.environ.get("PYTHONPATH", "/opt"),
            **(original_env or {}),
            **resolve_runtime_env(connections, variables, self.runtime_groups, storage=storage),
        }
        inherited_secrets = {
            key: os.environ.pop(key)
            for key in (
                "MINIO_ROOT_PASSWORD",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "POSTGRES_PASSWORD",
                "LAKEKEEPER_ENCRYPTION_KEY",
            )
            if key in os.environ
        }
        try:
            return super().execute(context)
        finally:
            self.env_vars = original_env
            os.environ.update(inherited_secrets)
