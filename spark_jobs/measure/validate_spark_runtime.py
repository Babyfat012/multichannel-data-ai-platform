"""Verify Airflow-resolved environment and Spark execution without reading or writing tables."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def main() -> None:
    """Check runtime configuration and execute a three-row JVM-only Spark query."""
    required = (
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "MINIO_INTERNAL_ENDPOINT",
        "WAREHOUSE_NAME", "LAKEKEEPER_CATALOG_URI", "POSTGRES_HOST", "POSTGRES_PORT",
        "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC",
    )
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise ValueError(f"Missing Airflow-resolved environment keys: {', '.join(missing)}")

    spark = SparkSession.builder.appName("fsds-runtime-smoke").getOrCreate()
    try:
        result = spark.range(3, numPartitions=1).selectExpr("sum(id) AS total").first()
        if result.total != 3:
            raise ValueError("Spark runtime smoke query returned an unexpected result")
        print(f"FSDS Spark runtime smoke: PASS; resolved_keys={len(required)}; "
              f"master={spark.sparkContext.master}; rows=3; sum={result.total}; table_writes=0")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
