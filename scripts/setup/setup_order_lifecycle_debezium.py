"""Thiết lập connector riêng cho Walmart v2, không thay đổi CDC v1."""

from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path

from scripts.setup.setup_order_snapshots_debezium import load_settings

CONNECTOR_NAME = "platform-walmart-order-lifecycle"
TOPIC = "platform_cdc_order_lifecycle.walmart_lifecycle.orders"
PUBLICATION = SLOT = "platform_walmart_order_lifecycle_orders"


def connector_config(settings):
    """Dựng cấu hình no_data vì bootstrap đã vào Bronze qua JDBC."""
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": settings.get("POSTGRES_CONNECT_HOST", "postgres"),
        "database.port": "5432",
        "database.user": settings["POSTGRES_USER"],
        "database.password": settings["POSTGRES_PASSWORD"],
        "database.dbname": settings["POSTGRES_DB"],
        "topic.prefix": "platform_cdc_order_lifecycle",
        "table.include.list": "walmart_lifecycle.orders",
        "plugin.name": "pgoutput",
        "slot.name": SLOT,
        "publication.name": PUBLICATION,
        "publication.autocreate.mode": "disabled",
        "snapshot.mode": "no_data",
        "tombstones.on.delete": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }


def configure(settings):
    """Kiểm khóa kép thật, tạo publication/topic và đăng ký connector riêng."""
    import psycopg
    from confluent_kafka.admin import AdminClient, NewTopic

    with psycopg.connect(
        host=settings.get("POSTGRES_HOST", "127.0.0.1"),
        port=int(settings.get("POSTGRES_PORT", "5432")),
        dbname=settings["POSTGRES_DB"],
        user=settings["POSTGRES_USER"],
        password=settings["POSTGRES_PASSWORD"],
        autocommit=True,
    ) as conn:
        if conn.execute("SHOW wal_level").fetchone()[0] != "logical":
            raise ValueError("Cần wal_level=logical")
        columns = conn.execute("""SELECT a.attname FROM pg_index i
            CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, n)
            JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
            WHERE i.indrelid='walmart_lifecycle.orders'::regclass AND i.indisprimary ORDER BY k.n""").fetchall()
        if [r[0] for r in columns] != ["dataset_version", "purchase_order_id"]:
            raise ValueError("Khóa chính khác DDL loader; từ chối thiết lập")
        conn.execute("ALTER TABLE walmart_lifecycle.orders REPLICA IDENTITY DEFAULT")
        if not conn.execute(
            "SELECT 1 FROM pg_publication WHERE pubname=%s", (PUBLICATION,)
        ).fetchone():
            conn.execute(f"CREATE PUBLICATION {PUBLICATION} FOR TABLE walmart_lifecycle.orders")
        tables = conn.execute(
            "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname=%s",
            (PUBLICATION,),
        ).fetchall()
        if tables != [("walmart_lifecycle", "orders")]:
            raise ValueError("Publication v2 phải chỉ chứa walmart_lifecycle.orders")
    admin = AdminClient(
        {"bootstrap.servers": settings.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9094")}
    )
    try:
        admin.create_topics([NewTopic(TOPIC, 1, 1)])[TOPIC].result(30)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise
    if len(admin.list_topics(TOPIC, timeout=30).topics[TOPIC].partitions) != 1:
        raise ValueError("Topic v2 cần đúng một partition để đối soát thứ tự")
    url = settings.get("DEBEZIUM_URL", "http://127.0.0.1:8084")
    request = urllib.request.Request(
        f"{url}/connectors/{CONNECTOR_NAME}/config",
        data=json.dumps(connector_config(settings)).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status not in (200, 201):
            raise RuntimeError(f"Kafka Connect HTTP {response.status}")
    print(f"Đã đăng ký {CONNECTOR_NAME}; chờ task RUNNING và slot active trước mutate")


def main():
    """Đọc cấu hình rồi thiết lập CDC v2."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args()
    configure(load_settings(args.env_file))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
