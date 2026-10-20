"""Tests for translating Airflow metadata into Spark runtime environment."""

import pytest

from dags.fsds_runtime import resolve_runtime_env


def test_resolve_runtime_env_maps_requested_connection_groups_only():
    """Resolve only the requested integrations and never expose credentials elsewhere."""
    connections = {
        "fsds_minio": {"host": "minio", "port": 9000, "login": "access", "password": "secret"},
        "fsds_postgres": {"host": "postgres", "port": 5432, "schema": "sources", "login": "user", "password": "pg-secret"},
        "fsds_kafka": {"host": "kafka", "port": 9092, "login": "", "password": ""},
    }
    variables = {
        "fsds_minio_internal_endpoint": "http://minio:9000",
        "fsds_warehouse_name": "lake",
        "fsds_lakekeeper_catalog_uri": "http://lakekeeper:8181/catalog",
        "fsds_dp1_cdc_topic": "orders.cdc",
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
