"""Create reusable Airflow Connections and Variables from Compose environment."""

from __future__ import annotations

import os

from airflow.models import Connection, Variable
from airflow.utils.session import create_session


def main() -> None:
    """Upsert shared source connections and runtime settings without CLI secrets."""
    connections = [
        Connection(conn_id="fsds_postgres", conn_type="postgres", host=os.environ.get("POSTGRES_HOST", "postgres"),
                   schema=os.environ["POSTGRES_DB"], login=os.environ["POSTGRES_USER"],
                   password=os.environ["POSTGRES_PASSWORD"], port=5432),
        Connection(conn_id="fsds_minio", conn_type="aws", host="minio", login=os.environ["MINIO_ROOT_USER"],
                   password=os.environ["MINIO_ROOT_PASSWORD"], port=9000),
        Connection(conn_id="fsds_kafka", conn_type="generic", host="kafka", port=9092),
    ]
    variables = {
        "fsds_maintenance_tables": '["lakehouse.demo.maintenance_demo"]',
        "fsds_maintenance_receipt_dir": "/opt/airflow/logs/iceberg-maintenance",
        "fsds_minio_internal_endpoint": "http://minio:9000",
        "fsds_warehouse_name": os.environ.get("WAREHOUSE_NAME", "fsds"),
        "fsds_lakekeeper_catalog_uri": os.environ.get("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog"),
        "fsds_dp1_cdc_topic": os.environ.get("KAFKA_TOPIC", "fsds_cdc.walmart.orders"),
    }
    with create_session() as session:
        for connection in connections:
            existing = session.query(Connection).filter_by(conn_id=connection.conn_id).one_or_none()
            if existing is None:
                session.add(connection)
                continue
            for field in ("conn_type", "host", "schema", "login", "password", "port", "extra"):
                setattr(existing, field, getattr(connection, field))
    for key, value in variables.items():
        Variable.set(key, value)


if __name__ == "__main__":
    main()
