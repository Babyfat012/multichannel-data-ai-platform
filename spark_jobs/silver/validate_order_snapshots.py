"""Kiểm đầu ra Silver của chặng DP2 sau khi Spark ghi bảng Iceberg.

Bronze là Order, Silver là Order Line, nên không so số dòng bằng nhau. Job kiểm
đầu ra không rỗng, số Order Line ít nhất bằng số Order nguồn, khoá không trùng,
các cột thiết yếu có giá trị, và mọi dòng thuộc đúng lần chạy Airflow.
"""

from __future__ import annotations

import argparse
import re
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
RULES = {
    "amazon": {
        "keys": ("order_id", "order_item_id"),
        "required": ("order_id", "order_item_id", "created_at", "order_status",
                     "seller_sku", "line_net_amount", "export_date", "source_file",
                     "schema_form", "processed_at", "run_id"),
    },
    "walmart": {
        "keys": ("purchase_order_id", "customer_order_id", "line_number"),
        "required": ("purchase_order_id", "customer_order_id", "line_number",
                     "created_at", "order_status", "sku", "line_net_amount",
                     "source_table", "fetched_at", "processed_at", "run_id"),
    },
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Đọc tên kênh, bảng nguồn/đích và mã lần chạy từ Airflow."""
    parser = argparse.ArgumentParser(description="Validate Silver Order Line")
    parser.add_argument("--channel", choices=RULES, required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    for name in (args.source_table, args.table):
        if not IDENTIFIER.fullmatch(name):
            parser.error(f"Tên bảng không hợp lệ: {name!r}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Đọc Bronze/Silver và trả mã lỗi khi bất kỳ điều kiện nào không đạt."""
    args = parse_args(argv)
    rule = RULES[args.channel]
    source = f"{CATALOG}.bronze.{args.source_table}"
    target = f"{CATALOG}.silver.{args.table}"
    spark = build_spark(f"validate-silver-{args.channel}")
    try:
        bronze = spark.table(source)
        silver = spark.table(target)
        missing = sorted(set(rule["required"]) - set(silver.columns))
        if missing:
            raise ValueError(f"{target} thiếu cột: {missing}")

        source_count = bronze.count()
        bad_value = F.lit(False)
        for column in rule["required"]:
            bad_value = bad_value | F.col(column).isNull()
        bad_value = bad_value | (F.col("run_id") != args.run_id)
        if args.channel == "amazon":
            bad_value = bad_value | ~F.col("schema_form").isin("v0", "2026")

        counts = silver.agg(
            F.count("*").alias("rows"),
            F.sum(F.when(bad_value, 1).otherwise(0)).alias("invalid_rows"),
        ).first()
        target_count = counts["rows"]
        invalid_count = counts["invalid_rows"] or 0
        duplicate_groups = (
            silver.groupBy(*rule["keys"])
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        print(f"nguồn Bronze      = {source}")
        print(f"đích Silver       = {target}")
        print(f"run_id            = {args.run_id}")
        print(f"Order nguồn       = {source_count:,}")
        print(f"Order Line Silver = {target_count:,}")
        print(f"dòng không hợp lệ = {invalid_count:,}")
        print(f"có khoá trùng     = {bool(duplicate_groups)}")

        errors = []
        if source_count == 0 or target_count < source_count:
            errors.append("số Order Line nhỏ hơn số Order nguồn hoặc nguồn rỗng")
        if invalid_count:
            errors.append(f"{invalid_count:,} dòng thiếu giá trị hoặc sai run_id/schema")
        if duplicate_groups:
            errors.append("khoá Order Line còn trùng")
        if errors:
            raise ValueError("Silver validate thất bại: " + "; ".join(errors))
        print("Silver validate: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
