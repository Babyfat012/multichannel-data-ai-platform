"""Sao chép 30 ngày gần nhất của Gold fact năm Channel sang PostgreSQL warehouse, kèm index.

Warehouse giữ phần "nóng" để tra từng SKU nhanh; toàn bộ lịch sử ở Iceberg. Giữ ít cột
để vừa đĩa EBS. Sau khi ghi: đối soát số dòng theo Channel với Gold rồi mới tạo index.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import timedelta

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.label_channels import count_mismatches
from spark_jobs.lib.order_lifecycle_warehouse import connect, cutoff_utc

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def main(argv: list[str] | None = None) -> int:
    """Export the recent Gold window to PostgreSQL, reconcile per Channel, then index it."""
    parser = argparse.ArgumentParser(
        description="Gold năm Channel (30 ngày) → PostgreSQL warehouse"
    )
    parser.add_argument("--source-table", default="fact_order_lines_dp2")
    parser.add_argument("--table", default="fact_order_lines_30d_dp2")
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args(argv)
    for value in (args.source_table, args.table):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Tên bảng không hợp lệ: {value!r}")
    end = cutoff_utc(args.cutoff)
    start = end - timedelta(days=args.days)

    spark = build_spark("export-gold-warehouse", shuffle_partitions=32)
    try:
        rows = (
            spark.table(f"{CATALOG}.gold.{args.source_table}")
            .filter((F.col("created_at") >= F.lit(start)) & (F.col("created_at") < F.lit(end)))
            .select(
                "order_line_key",
                "channel",
                "channel_order_id",
                "created_at",
                "seller_sku",
                "line_net_amount",
                "currency",
            )
            .cache()
        )
        gold = {r["channel"]: r["count"] for r in rows.groupBy("channel").count().collect()}
        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = os.environ.get("POSTGRES_PORT", "5432")
        database = os.environ.get("POSTGRES_DB", "sources")
        target = f"warehouse.{args.table}"
        options = {
            "url": f"jdbc:postgresql://{host}:{port}/{database}",
            "dbtable": target,
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver",
            "batchsize": "5000",
        }
        # Postgres mới (AWS) chưa có schema: JDBC không tự tạo schema, chỉ tạo bảng.
        conn = connect()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
        conn.close()
        # Giới hạn: overwrite thẳng, lúc nạp lại bảng vắng/rỗng; cần đọc liên tục thì staging +
        # rename như spark_jobs/lib/order_lifecycle_warehouse.py.
        rows.repartition(4, "seller_sku").write.format("jdbc").options(**options).mode(
            "overwrite"
        ).save()
        conn = connect()
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT channel, count(*) FROM {target} GROUP BY channel")
                loaded = dict(cur.fetchall())
                bad = count_mismatches(gold, loaded)
                print(f"Warehouse {target} [{start} → {end}): gold={gold} postgres={loaded}")
                if bad:
                    raise ValueError(f"Warehouse lệch Gold hoặc kênh rỗng: {bad}")
                cur.execute(f"CREATE INDEX ON {target} (seller_sku)")
                cur.execute(f"ANALYZE {target}")
        finally:
            conn.close()
        print(f"Warehouse đã ghi + index seller_sku: {target}; rows={sum(loaded.values())}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
