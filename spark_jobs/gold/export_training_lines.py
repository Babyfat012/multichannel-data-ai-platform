"""Gold A → Parquet tập train: Order Line có nhãn (biết trước cutoff) + thuộc tính lúc đặt đơn.

Đây là "entity dataframe" cho Feast: KFP ghép thêm feature SKU bằng PIT join tại `event_timestamp`
(= lúc đặt). Nhãn lấy từ `fact_return_labels_dp3` (năm Channel); chỉ giữ nhãn có
`label_available_at <= cutoff`. Không mang cột biết sau khi đặt để tránh leakage. Lấy mẫu
`--sample-per-mille` theo hash `order_line_key`: chạy lại ra đúng tập cũ, rải đều theo thời gian.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_snapshot_ml import configure_output, training_rows


def main(argv=None):
    """Ghép nhãn với fact, ghi đè Parquet và in phân bố nhãn."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output", required=True, help="vd s3a://platform/feature-store/training_lines"
    )
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--fact-table", default="fact_order_lines_dp2")
    parser.add_argument("--labels-table", default="fact_return_labels_dp3")
    # 10‰ = 1%: năm Channel ~ trăm triệu line có nhãn, pod train trên EKS chỉ ~8 GB RAM.
    parser.add_argument("--sample-per-mille", type=int, default=10)
    args = parser.parse_args(argv)
    if not 0 < args.sample_per_mille <= 1000:
        parser.error("--sample-per-mille phải trong (0, 1000]")
    spark = build_spark("export-training-lines", shuffle_partitions=64)
    try:
        configure_output(spark, args.output)
        labels = spark.table(f"{CATALOG}.gold.{args.labels_table}")
        runs = labels.select("run_id").distinct().collect()
        if len(runs) != 1:
            raise ValueError(f"cần đúng 1 run_id trong bảng nhãn, có {runs}")
        fact = spark.table(f"{CATALOG}.gold.{args.fact_table}")
        rows = training_rows(fact, labels, args.cutoff, args.sample_per_mille)
        rows.coalesce(1).write.mode("overwrite").parquet(args.output)
        written = spark.read.parquet(args.output)
        counts = {
            (r["channel"], r["label"]): r["count"]
            for r in written.groupBy("channel", "label").count().collect()
        }
        bad = written.filter(F.col("ordered_product_net_minor").isNull()).limit(1).count()
        print(f"wrote {written.count()} rows to {args.output}; label counts {counts}")
        if not counts or bad:
            raise ValueError(f"Tập train rỗng hoặc có tiền NULL (tiền tệ lạ): bad={bad}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
