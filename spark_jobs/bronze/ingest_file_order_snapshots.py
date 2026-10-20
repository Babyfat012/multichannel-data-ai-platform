"""Nạp nguồn JSONL của TikTok Shop, Shopify hoặc Shopee vào Bronze Iceberg.

Mỗi dòng nguồn là một response API mô phỏng. Bronze giữ nguyên chuỗi JSON,
không parse hoặc dedup; chỉ thêm đường dẫn file, Export Date, lúc nạp và run_id.
Amazon giữ job riêng vì nó là bằng chứng baseline/AQE đã đo của rubric.
"""

from __future__ import annotations

import argparse
import re
import sys
import uuid

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

CHANNELS = ("tiktok_shop", "shopify", "shopee")
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Đọc Channel, đường dẫn nguồn và bảng Bronze đích."""
    parser = argparse.ArgumentParser(description="Bronze JSONL cho ba Channel mới")
    parser.add_argument("--channel", choices=CHANNELS, required=True)
    parser.add_argument(
        "--source-root", required=True, help="gốc source-exports/ có thư mục channel=..."
    )
    parser.add_argument("--table", required=True)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.table):
        parser.error(f"Tên bảng không hợp lệ: {args.table!r}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Đọc JSONL và ghi Bronze; in số dòng của snapshot mới."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    source = f"{args.source_root.rstrip('/')}/channel={args.channel}/dataset=orders"
    target = f"{CATALOG}.bronze.{args.table}"
    spark = build_spark(f"bronze-{args.channel}")
    try:
        bronze = (
            spark.read.option("basePath", source)
            .text(source)
            .select(
                F.col("value").alias("payload"),
                F.input_file_name().alias("source_file"),
                F.col("export_date"),
            )
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id))
        )
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
        bronze.writeTo(target).partitionedBy("export_date").createOrReplace()
        rows = spark.table(target).count()
        print(f"nguồn       = {source}")
        print(f"bảng Bronze = {target}")
        print(f"run_id      = {run_id}")
        print(f"dòng đã nạp = {rows:,}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
