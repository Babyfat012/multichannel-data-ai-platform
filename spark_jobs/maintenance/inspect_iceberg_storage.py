"""Inspect current Iceberg data files before choosing a compaction target."""

from __future__ import annotations

import argparse
import re

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import build_spark

TABLE_NAME = re.compile(r"lakehouse\.(bronze|silver)\.[A-Za-z_][A-Za-z0-9_]*\Z")


def main() -> None:
    """Print Iceberg file counts and partition metadata for storage inspection."""
    parser = argparse.ArgumentParser()
    parser.add_argument("tables", nargs="+", help="Fully qualified Iceberg table names")
    args = parser.parse_args()
    for table in args.tables:
        if not TABLE_NAME.fullmatch(table):
            parser.error(f"Invalid table name: {table}")

    spark = build_spark("platform-inspect-storage", shuffle_partitions=4)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        for table in args.tables:
            files = spark.table(f"{table}.files")
            summary = files.agg(
                F.count("*").alias("files"),
                F.sum("record_count").alias("rows"),
                F.sum("file_size_in_bytes").alias("bytes"),
                F.min("file_size_in_bytes").alias("smallest"),
                F.expr("percentile_approx(file_size_in_bytes, 0.5)").alias("median"),
                F.max("file_size_in_bytes").alias("largest"),
                F.sum(F.when(F.col("file_size_in_bytes") < 16 * 1024 * 1024, 1).otherwise(0)).alias(
                    "under_16_mib"
                ),
                F.sum(F.when(F.col("file_size_in_bytes") < 64 * 1024 * 1024, 1).otherwise(0)).alias(
                    "under_64_mib"
                ),
            ).first()
            if "partition" in files.columns:
                by_partition = files.groupBy("partition").count()
                partitions = (
                    by_partition.agg(
                        F.count("*").alias("partitions"),
                        F.max("count").alias("max_files_per_partition"),
                        F.sum(F.when(F.col("count") > 1, 1).otherwise(0)).alias(
                            "partitions_with_multiple_files"
                        ),
                    )
                    .first()
                    .asDict()
                )
            else:
                partitions = {
                    "partitions": 1,
                    "max_files_per_partition": summary["files"],
                    "partitions_with_multiple_files": int(summary["files"] > 1),
                }
            print(f"STORAGE_FILE_SUMMARY {table} {dict(**summary.asDict(), **partitions)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
