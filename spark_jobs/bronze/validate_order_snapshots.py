"""DP1 Validate — đối chiếu bảng Bronze với nguồn và kiểm provenance/payload.

Bronze phải giữ cả bản trùng và hai dạng schema Amazon. Vì vậy validation kiểm dữ liệu
được chép đủ và truy ngược được, không đòi khóa duy nhất hay một schema JSON duy nhất.
Job chạy sau mỗi lần Spark ingest; lỗi bất kỳ điều kiện nào làm task Airflow đỏ.
"""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs.lib.day_selection import pick_window_days
from spark_jobs.lib.iceberg_session import build_spark

AMAZON_SOURCE = "/data/raw/source-exports/channel=amazon/dataset=orders"
WALMART_SOURCE = "walmart.orders"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Đọc đúng nguồn, bảng đích và phạm vi ngày của lần ingest vừa chạy."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--channel", choices=("amazon", "walmart"), required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--window-days", type=int, default=0)
    parser.add_argument("--window-start", default=None)
    parser.add_argument("--source", default=AMAZON_SOURCE, help="Gốc export_date=*/ của Amazon.")
    return parser.parse_args(argv)


def amazon_source_rows(
    spark: SparkSession, window_days: int, window_start: str | None, source: str = AMAZON_SOURCE
) -> int:
    """Đếm chính các dòng JSONL mà job Amazon đã chọn để nạp."""
    if window_days <= 0:
        return spark.read.text(source).count()
    path = spark._jvm.org.apache.hadoop.fs.Path(source)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    days = sorted(
        status.getPath().getName().split("=", 1)[1]
        for status in fs.listStatus(path)
        if status.isDirectory() and status.getPath().getName().startswith("export_date=")
    )
    selected = pick_window_days(days, window_days, window_start)
    if not selected:
        raise ValueError("Không có ngày Amazon nào để validate")
    return spark.read.text([f"{source}/export_date={day}" for day in selected]).count()


def walmart_source_rows(spark: SparkSession) -> int:
    """Đếm bảng nguồn PostgreSQL qua JDBC, không đếm lại bảng Bronze."""
    query = f"(SELECT count(*) AS n FROM {WALMART_SOURCE}) AS source_count"
    jdbc = {
        "url": f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:"
        f"{os.environ.get('POSTGRES_PORT', '5432')}/{os.environ['POSTGRES_DB']}",
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "dbtable": query,
    }
    return int(spark.read.format("jdbc").options(**jdbc).load().first()["n"])


def validate(spark: SparkSession, channel: str, table: str, run_id: str, source_rows: int) -> None:
    """Kiểm bảng có dòng, đủ cột, đủ số dòng và không có provenance/payload hỏng."""
    full_name = f"lakehouse.bronze.{table}"
    df = spark.table(full_name)
    required = (
        {"payload", "source_file", "export_date", "ingested_at", "run_id"}
        if channel == "amazon"
        else {
            "purchase_order_id",
            "raw_payload",
            "source_table",
            "fetched_at",
            "ingested_at",
            "run_id",
        }
    )
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{full_name}: thiếu cột Bronze {sorted(missing)}")

    if channel == "amazon":
        source_id = F.coalesce(
            F.get_json_object("payload", "$.AmazonOrderId"),
            F.get_json_object("payload", "$.orderId"),
        )
        invalid = (
            F.col("payload").isNull()
            | (F.length("payload") == 0)
            | source_id.isNull()
            | F.col("source_file").isNull()
            | (F.length("source_file") == 0)
            | F.col("export_date").isNull()
        )
    else:
        source_id = F.get_json_object("raw_payload", "$.purchaseOrderId")
        invalid = (
            F.col("raw_payload").isNull()
            | (F.length("raw_payload") == 0)
            | source_id.isNull()
            | F.col("purchase_order_id").isNull()
            | (source_id != F.col("purchase_order_id"))
            | (F.col("source_table") != WALMART_SOURCE)
            | F.col("source_table").isNull()
            | F.col("fetched_at").isNull()
        )
    invalid = (
        invalid
        | F.col("ingested_at").isNull()
        | F.col("run_id").isNull()
        | (F.col("run_id") != run_id)
    )
    result = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(invalid, 1).otherwise(0)).alias("invalid_rows"),
    ).first()
    target_rows = int(result["rows"])
    invalid_rows = int(result["invalid_rows"] or 0)
    print(f"nguồn={source_rows:,} Bronze={target_rows:,} lỗi={invalid_rows:,} bảng={full_name}")
    if source_rows <= 0 or target_rows != source_rows or invalid_rows:
        raise ValueError("DP1 Validate thất bại: nguồn và Bronze lệch hoặc provenance/payload hỏng")
    print("DP1 Validate đạt")


def main(argv: list[str] | None = None) -> int:
    """Dựng SparkSession, đo nguồn và kiểm bảng Bronze của cùng Airflow run."""
    args = parse_args(argv)
    spark = build_spark(f"dp1-validate-{args.channel}-{args.table}")
    try:
        source_rows = (
            amazon_source_rows(spark, args.window_days, args.window_start, args.source)
            if args.channel == "amazon"
            else walmart_source_rows(spark)
        )
        validate(spark, args.channel, args.table, args.run_id, source_rows)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc
