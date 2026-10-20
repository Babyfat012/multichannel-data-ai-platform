"""Nối nhãn Walmart với Gold qua đủ ba mã gốc của Order Line.

Bridge chỉ dành cho dữ liệu mô phỏng: customer_order_id được tái tạo từ seed
và mã Order nội bộ trong ground truth bởi bước chuẩn bị khóa trên host.
Nhãn nằm ở bảng riêng, không vào fact hoặc feature dùng khi dự đoán.
"""

from __future__ import annotations

import argparse
import re
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def main(argv: list[str] | None = None) -> int:
    """Join source Walmart labels to Gold rows using the three-key bridge."""
    parser = argparse.ArgumentParser(description="Bridge nhãn Walmart chính xác")
    parser.add_argument("--fact-table", default="fact_order_lines_dp2")
    parser.add_argument("--table", default="fact_walmart_return_labels_dp3")
    parser.add_argument("--ground-truth", default="/data/ground-truth")
    parser.add_argument("--keys-path", default="/data/ground-truth-bridge/walmart-keys.parquet")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    for value in (args.fact_table, args.table):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Tên bảng không hợp lệ: {value!r}")

    spark = build_spark("walmart-return-label-bridge", shuffle_partitions=32)
    try:
        keys = spark.read.parquet(args.keys_path).select(
            "order_line_id",
            "channel_order_id",
            "channel_line_id",
            "channel_secondary_order_id",
        )
        fact = (
            spark.table(f"{CATALOG}.gold.{args.fact_table}")
            .filter(F.col("channel") == "walmart")
            .select(
                "order_line_key",
                "channel_order_id",
                "channel_secondary_order_id",
                "channel_line_id",
                "created_at",
            )
        )
        matched = fact.join(
            keys,
            ["channel_order_id", "channel_secondary_order_id", "channel_line_id"],
            "inner",
        )
        labels = spark.read.parquet(f"{args.ground_truth}/labels/*.parquet").select(
            "order_line_id", "label", "delivered_at", "label_available_at"
        )
        result = matched.join(labels, "order_line_id", "inner").select(
            "order_line_key",
            "order_line_id",
            "label",
            "delivered_at",
            "label_available_at",
            "created_at",
            F.lit(args.run_id).alias("run_id"),
        )
        target = f"{CATALOG}.gold.{args.table}"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
        result.writeTo(target).partitionedBy(F.days("created_at")).createOrReplace()
        saved = spark.table(target)
        duplicate = (
            saved.groupBy("order_line_key").count().filter(F.col("count") > 1).limit(1).count()
        )
        invalid = (
            saved.filter(
                F.col("order_line_key").isNull()
                | F.col("label").isNull()
                | ~F.col("label").isin(0, 1)
                | F.col("delivered_at").isNull()
                | F.col("label_available_at").isNull()
                | (F.col("created_at") > F.col("delivered_at"))
                | (F.col("delivered_at") > F.col("label_available_at"))
                | (F.col("run_id") != args.run_id)
            )
            .limit(1)
            .count()
        )
        rows = spark.table(f"{target}.files").agg(F.sum("record_count")).first()[0]
        print(f"Bridge Walmart: rows={rows}, duplicate={duplicate}, invalid={invalid}")
        if not rows or duplicate or invalid:
            raise ValueError("Bridge nhãn Walmart không hợp lệ")
        print(f"Bridge nhãn Walmart đã ghi: {target}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
