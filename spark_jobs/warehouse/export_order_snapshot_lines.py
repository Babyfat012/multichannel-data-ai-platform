"""Sao chép fact Gold Walmart sang PostgreSQL để đo tác động của index.

Bảng warehouse là lát Walmart của fact năm Channel, cùng khóa và tiền gốc.
Giữ ít cột để phép đo index vừa dung lượng máy dev.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def main(argv: list[str] | None = None) -> int:
    """Export a selected Gold slice to the PostgreSQL warehouse through JDBC."""
    parser = argparse.ArgumentParser(description="Gold Walmart → PostgreSQL warehouse")
    parser.add_argument("--source-table", default="fact_order_lines_dp2")
    parser.add_argument("--table", default="fact_order_lines_walmart_dp2")
    args = parser.parse_args(argv)
    for value in (args.source_table, args.table):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Tên bảng không hợp lệ: {value!r}")

    spark = build_spark("export-gold-warehouse", shuffle_partitions=32)
    try:
        source = spark.table(f"{CATALOG}.gold.{args.source_table}")
        rows = source.filter(F.col("channel") == "walmart").select(
            "order_line_key",
            "channel_order_id",
            "created_at",
            "seller_sku",
            "line_net_amount",
            "currency",
        )
        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = os.environ.get("POSTGRES_PORT", "5432")
        database = os.environ.get("POSTGRES_DB", "sources")
        options = {
            "url": f"jdbc:postgresql://{host}:{port}/{database}",
            "dbtable": f"warehouse.{args.table}",
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver",
            "batchsize": "5000",
        }
        rows.repartition(4, "seller_sku").write.format("jdbc").options(**options).mode(
            "overwrite"
        ).save()
        print(f"Warehouse đã ghi: warehouse.{args.table}; channel=walmart")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
