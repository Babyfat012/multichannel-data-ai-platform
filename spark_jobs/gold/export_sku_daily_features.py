"""Gold v2 → Parquet ảnh chụp feature SKU theo ngày, làm kho offline cho Feast.

Đọc `lifecycle_fact_order_lines` + `lifecycle_label_segments` (Gold ghi đè hai bảng này mỗi run,
nên chỉ có một generation), tính `sku_daily_features` rồi ghi đè thư mục Parquet (`s3a://` trên
MinIO, `gs://` trên GCP). Chỉ nhận MỘT `dataset_version` để khoá Feast là `seller_sku` trơn.
"""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_lifecycle_gold import sku_daily_features


def main(argv=None):
    """Đọc Gold hiện tại, ghi Parquet và in số dòng."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--output", required=True, help="vd s3a://platform/feature-store/sku_daily")
    args = parser.parse_args(argv)
    spark = build_spark("export-sku-daily-features", shuffle_partitions=4)
    try:
        if args.output.startswith("s3a://"):
            hadoop = spark.sparkContext._jsc.hadoopConfiguration()
            hadoop.set("fs.s3a.endpoint", os.environ["MINIO_INTERNAL_ENDPOINT"])
            hadoop.set("fs.s3a.path.style.access", "true")
            hadoop.set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            hadoop.set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        fact = spark.table(f"{CATALOG}.gold.lifecycle_fact_order_lines")
        segments = spark.table(f"{CATALOG}.gold.lifecycle_label_segments")
        keys = fact.select("dataset_version", "generation_id").distinct().collect()
        if len(keys) != 1:
            raise ValueError(f"cần đúng 1 (dataset_version, generation_id) trong Gold, có {keys}")
        daily = sku_daily_features(fact, segments, args.cutoff).withColumn(
            "generation_id", F.lit(keys[0]["generation_id"])
        )
        daily.coalesce(1).write.mode("overwrite").parquet(args.output)
        print(f"wrote {spark.read.parquet(args.output).count()} rows to {args.output}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
