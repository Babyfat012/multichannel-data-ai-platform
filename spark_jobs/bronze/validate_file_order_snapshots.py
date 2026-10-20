"""Kiểm Bronze JSONL cho TikTok Shop, Shopify và Shopee sau khi nạp.

So số dòng với đúng thư mục JSONL nguồn và kiểm ID gốc/provenance trên từng
dòng. Bản trùng từ export chồng lấn là dữ liệu hợp lệ ở Bronze.
"""

from __future__ import annotations

import argparse
import re
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

ID_PATH = {
    "tiktok_shop": "$.data.orders[0].id",
    "shopify": "$.order.id",
    "shopee": "$.response.order_list[0].order_sn",
}
REQUIRED = {"payload", "source_file", "export_date", "ingested_at", "run_id"}
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Đọc Channel, nguồn JSONL, bảng Bronze và run_id từ Airflow."""
    parser = argparse.ArgumentParser(description="Validate Bronze JSONL")
    parser.add_argument("--channel", choices=ID_PATH, required=True)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.table):
        parser.error(f"Tên bảng không hợp lệ: {args.table!r}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Trả mã lỗi nếu bảng/cột, số dòng, ID hoặc provenance không đạt."""
    args = parse_args(argv)
    source = (f"{args.source_root.rstrip('/')}/channel={args.channel}"
              "/dataset=orders")
    target = f"{CATALOG}.bronze.{args.table}"
    spark = build_spark(f"validate-bronze-{args.channel}")
    try:
        source_count = spark.read.text(source).count()
        bronze = spark.table(target)
        missing = sorted(REQUIRED - set(bronze.columns))
        if missing:
            raise ValueError(f"{target} thiếu cột: {missing}")
        bad = (
            F.get_json_object(F.col("payload"), ID_PATH[args.channel]).isNull()
            | F.col("source_file").isNull()
            | ~F.col("source_file").contains(f"channel={args.channel}")
            | F.col("export_date").isNull()
            | F.col("ingested_at").isNull()
            | F.col("run_id").isNull()
            | (F.col("run_id") != args.run_id)
        )
        counts = bronze.agg(
            F.count("*").alias("rows"),
            F.sum(F.when(bad, 1).otherwise(0)).alias("invalid_rows"),
        ).first()
        target_count = counts["rows"]
        invalid_count = counts["invalid_rows"] or 0
        print(f"nguồn       = {source}")
        print(f"bảng Bronze = {target}")
        print(f"dòng nguồn  = {source_count:,}")
        print(f"dòng Bronze = {target_count:,}")
        print(f"dòng lỗi    = {invalid_count:,}")
        if source_count == 0 or target_count != source_count or invalid_count:
            raise ValueError("Bronze validate thất bại")
        print("Bronze validate: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
