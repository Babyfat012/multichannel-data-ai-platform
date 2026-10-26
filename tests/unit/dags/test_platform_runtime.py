"""Tests for translating Airflow metadata into Spark runtime environment."""

import ast
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from dags.platform_runtime import resolve_runtime_env


def test_resolve_runtime_env_maps_requested_connection_groups_only():
    """Resolve only the requested integrations and never expose credentials elsewhere."""
    connections = {
        "platform_minio": {"host": "minio", "port": 9000, "login": "access", "password": "secret"},
        "platform_postgres": {
            "host": "postgres",
            "port": 5432,
            "schema": "sources",
            "login": "user",
            "password": "pg-secret",
        },
        "platform_kafka": {"host": "kafka", "port": 9092, "login": "", "password": ""},
    }
    variables = {
        "platform_minio_internal_endpoint": "http://minio:9000",
        "platform_warehouse_name": "lake",
        "platform_lakekeeper_catalog_uri": "http://lakekeeper:8181/catalog",
        "platform_dp1_cdc_topic": "orders.cdc",
    }

    assert resolve_runtime_env(connections, variables, ("iceberg", "postgres")) == {
        "AWS_ACCESS_KEY_ID": "access",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "MINIO_INTERNAL_ENDPOINT": "http://minio:9000",
        "WAREHOUSE_NAME": "lake",
        "LAKEKEEPER_CATALOG_URI": "http://lakekeeper:8181/catalog",
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "sources",
        "POSTGRES_USER": "user",
        "POSTGRES_PASSWORD": "pg-secret",
    }
    assert resolve_runtime_env(connections, variables, ("kafka",)) == {
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "KAFKA_TOPIC": "orders.cdc",
    }


def test_resolve_runtime_env_rejects_unknown_groups():
    """Reject typos in the integration group list instead of silently omitting config."""
    with pytest.raises(ValueError, match="unknown"):
        resolve_runtime_env({}, {}, ("unkown",))


@pytest.mark.parametrize("storage", ["s3"])
def test_cloud_runtime_uses_machine_identity_without_minio(storage):
    """Cloud không đọc Connection MinIO hoặc đưa placeholder key vào Spark."""
    variables = {
        "platform_warehouse_name": "cloud-lake",
        "platform_lakekeeper_catalog_uri": "http://lakekeeper:8181/catalog",
    }
    assert resolve_runtime_env({}, variables, ("iceberg",), storage=storage) == {
        "WAREHOUSE_NAME": "cloud-lake",
        "LAKEKEEPER_CATALOG_URI": "http://lakekeeper:8181/catalog",
    }


def test_runtime_rejects_unknown_storage():
    """Lỗi chính tả không được âm thầm đổi cách xác thực."""
    with pytest.raises(ValueError, match="storage"):
        resolve_runtime_env({}, {}, (), storage="typo")


@pytest.mark.parametrize("storage", ["s3"])
@pytest.mark.parametrize("fails", [False, True])
def test_cloud_operator_clears_inherited_keys_and_restores_state(monkeypatch, storage, fails):
    """Thực thi operator thật với biên Airflow giả; giữ credential ngoài process Spark."""
    keys = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
    for key in keys:
        monkeypatch.setenv(key, "stale-local-credential")
    monkeypatch.setenv("PLATFORM_STORAGE", storage)
    variables = {
        "platform_warehouse_name": "cloud-lake",
        "platform_lakekeeper_catalog_uri": "http://lakekeeper:8181/catalog",
    }

    class SparkStub:
        """Thay spark-submit bằng kiểm tra môi trường ngay tại biên execute."""

        template_fields = ("env_vars",)

        def __init__(self, **kwargs):
            """Giữ env_vars để kiểm việc phục hồi object sau khi chạy."""
            self.env_vars = kwargs.get("env_vars")

        def execute(self, context):
            """Đảm bảo không có key cũ trong env_vars lẫn môi trường kế thừa."""
            assert not set(keys).intersection(self.env_vars)
            assert not set(keys).intersection(os.environ)
            assert self.env_vars["WAREHOUSE_NAME"] == "cloud-lake"
            if fails:
                raise RuntimeError("spark failed")
            return "submitted"

    def unexpected_connection(name):
        """Không có dịch vụ MinIO trên Cloud nên không được tra connection đó."""
        raise AssertionError(f"Unexpected connection: {name}")

    source = Path(__file__).resolve().parents[3] / "dags/platform_spark_operator.py"
    tree = ast.parse(source.read_text())
    namespace = {
        "os": os,
        "SparkSubmitOperator": SparkStub,
        "BaseHook": SimpleNamespace(get_connection=unexpected_connection),
        "Variable": SimpleNamespace(get=variables.__getitem__),
        "resolve_runtime_env": resolve_runtime_env,
    }
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef)]
    exec(compile(ast.Module(body=classes, type_ignores=[]), str(source), "exec"), namespace)
    original = {"CUSTOM": "kept"}
    operator = namespace["PlatformSparkSubmitOperator"](env_vars=original)
    if fails:
        with pytest.raises(RuntimeError, match="spark failed"):
            operator.execute({})
    else:
        assert operator.execute({}) == "submitted"
    assert operator.env_vars is original
    assert all(os.environ[key] == "stale-local-credential" for key in keys)
