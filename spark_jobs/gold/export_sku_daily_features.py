"""Gold A → Parquet ảnh chụp feature SKU theo ngày, làm kho offline cho Feast.

Đọc `fact_order_lines_dp2` + `fact_return_labels_dp3` (năm Channel), tính tỷ lệ trả 90 ngày
mỗi 00:00 UTC chỉ từ nhãn đã có tại lúc chụp, rồi ghi đè thư mục Parquet (`s3a://`: MinIO
local, S3 thật qua IAM role trên AWS). Khoá Feast là `seller_sku` trơn.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_snapshot_ml import configure_output, sku_daily_features


def main(argv=None):
    """Đọc Gold hiện tại, ghi Parquet và in số dòng."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--output", required=True, help="vd s3a://platform/feature-store/sku_daily")
    parser.add_argument("--fact-table", default="fact_order_lines_dp2")
    parser.add_argument("--labels-table", default="fact_return_labels_dp3")
    args = parser.parse_args(argv)
    spark = build_spark("export-sku-daily-features", shuffle_partitions=64)
    try:
        configure_output(spark, args.output)
        labels = spark.table(f"{CATALOG}.gold.{args.labels_table}")
        runs = labels.select("run_id").distinct().collect()
        if len(runs) != 1:
            raise ValueError(f"cần đúng 1 run_id trong bảng nhãn, có {runs}")
        fact = spark.table(f"{CATALOG}.gold.{args.fact_table}")
        daily = sku_daily_features(fact, labels, args.cutoff).withColumn(
            "generation_id", F.lit(runs[0]["run_id"])
        )
        daily.coalesce(1).write.mode("overwrite").parquet(args.output)
        print(f"wrote {spark.read.parquet(args.output).count()} rows to {args.output}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
