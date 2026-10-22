"""Gold v2 → Parquet tập train: mỗi Order Line đã có nhãn + thuộc tính biết lúc đặt đơn.

Đây là "entity dataframe" cho Feast: KFP ghép thêm feature SKU bằng PIT join tại `event_timestamp`
(= lúc đặt). Chỉ lấy line `state = labeled` (đã giao và đủ 30 ngày, đúng Q38). Không mang cột
biết sau khi đặt (giao, hủy, trả, hoàn) để tránh leakage.
"""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

# Thuộc tính lúc đặt đơn; mọi cột khác của fact (delivered_qty, approved_return_qty...) là tương lai.
ORDER_TIME_COLUMNS = (
    "channel",
    "account_id",
    "currency",
    "ordered_basis_qty",
    "ordered_product_net_minor",
)


def main(argv=None):
    """Ghép nhãn generation hiện tại với fact, ghi đè Parquet và in phân bố nhãn."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output", required=True, help="vd s3a://fsds/feature-store/training_lines"
    )
    args = parser.parse_args(argv)
    spark = build_spark("export-training-lines", shuffle_partitions=4)
    try:
        if args.output.startswith("s3a://"):
            hadoop = spark.sparkContext._jsc.hadoopConfiguration()
            hadoop.set("fs.s3a.endpoint", os.environ["MINIO_INTERNAL_ENDPOINT"])
            hadoop.set("fs.s3a.path.style.access", "true")
            hadoop.set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            hadoop.set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        fact = spark.table(f"{CATALOG}.gold.lifecycle_fact_order_lines")
        generation = fact.select("generation_id").distinct().collect()
        if len(generation) != 1:
            raise ValueError(f"cần đúng 1 generation trong fact, có {generation}")
        labels = (
            spark.table(f"{CATALOG}.gold.lifecycle_label_return")
            .filter(F.col("generation_id") == generation[0][0])
            .filter(F.col("state") == "labeled")
            .select("dataset_version", "line_id", "label", "cohort_date", "generation_id")
        )
        rows = (
            fact.select(
                "dataset_version", "line_id", "seller_sku", "created_at", *ORDER_TIME_COLUMNS
            )
            .join(labels, ["dataset_version", "line_id"])
            .withColumn("event_timestamp", F.col("created_at"))
        )
        rows.coalesce(1).write.mode("overwrite").parquet(args.output)
        written = spark.read.parquet(args.output)
        counts = {r["label"]: r["count"] for r in written.groupBy("label").count().collect()}
        print(f"wrote {written.count()} rows to {args.output}; label counts {counts}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
