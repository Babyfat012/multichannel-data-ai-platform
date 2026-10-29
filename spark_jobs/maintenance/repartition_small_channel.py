"""Gộp file nhỏ của bảng kênh nhỏ: đổi partition ngày → tháng (partition evolution) rồi viết lại.

Bảng Bronze kênh nhỏ chia theo ngày ra 1 file/ngày (~2,5 MB) nên `rewrite_data_files` thường không
gộp được gì (gap #16). Job đo trước, đổi spec, viết lại toàn bộ theo spec mới, đo sau và chặn nếu
dữ liệu đổi (fingerprint). Spec cũ vẫn nằm trong snapshot trước: lùi bằng `rollback_to_snapshot`.

Chạy: spark-submit .../repartition_small_channel.py --table lakehouse.bronze.raw_shopee_orders_dp1 \
  --column export_date --receipt /opt/airflow/logs/iceberg-maintenance/repartition.json
Module chỉ import pyspark khi dựng session; test được câu SQL không cần Spark.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time

from spark_jobs.maintenance.maintain_iceberg_tables import (
    column_name,
    fingerprint,
    identifier,
    literal,
    write_receipt,
)

RUNS = 3


def evolve_sql(table, column):
    """Đổi trường partition identity(ngày) sang months(ngày): dữ liệu cũ giữ spec cũ tới khi viết lại."""
    return (
        f"ALTER TABLE {identifier(table)} REPLACE PARTITION FIELD {column_name(column)} "
        f"WITH months({column_name(column)})"
    )


def rewrite_sql(table):
    """Viết lại MỌI file theo spec hiện hành (rewrite-all), không chỉ file dưới ngưỡng cỡ."""
    identifier(table)
    catalog, short = table.split(".", 1)
    return (
        f"CALL `{catalog}`.system.rewrite_data_files(table => {literal(short)}, "
        "options => map('rewrite-all', 'true'))"
    )


def probe_queries(table, column):
    """Hai câu đo: quét cả bảng và lọc 30 ngày cuối (theo cột partition)."""
    relation = identifier(table)
    col = column_name(column)
    return {
        "count_all": f"SELECT count(*) FROM {relation}",
        "last_30_days": (
            f"SELECT count(*) FROM {relation} "
            f"WHERE {col} > (SELECT date_sub(max({col}), 30) FROM {relation})"
        ),
    }


def check_same(before, after):
    """Chặn nếu số dòng hoặc fingerprint khác: viết lại file không được đổi dữ liệu."""
    if (before["rows"], before["fingerprint"]) != (after["rows"], after["fingerprint"]):
        raise ValueError(f"Dữ liệu đổi sau khi viết lại: {before} → {after}")


def measure(spark, table, column):
    """Số file, cỡ trung bình, số spec đang dùng và trung vị thời gian mỗi câu đo (3 lần)."""
    files = spark.sql(
        f"SELECT count(*) AS n, coalesce(avg(file_size_in_bytes), 0) AS avg, "
        f"count(DISTINCT spec_id) AS specs FROM {identifier(table)}.files"
    ).first()
    timings = {}
    for name, query in probe_queries(table, column).items():
        seconds = []
        for _ in range(RUNS):
            started = time.perf_counter()
            spark.sql(query).collect()
            seconds.append(time.perf_counter() - started)
        timings[name] = {"median_s": round(statistics.median(seconds), 3), "runs_s": seconds}
    return {
        "data_files": files["n"],
        "avg_file_mb": round(files["avg"] / 2**20, 2),
        "specs_in_use": files["specs"],
        "timings": timings,
        **fingerprint(spark, table),
    }


def main(argv=None):
    """Đo → đổi spec → viết lại → đo, ghi biên lai kể cả khi lỗi."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--table", required=True)
    parser.add_argument("--column", required=True)
    parser.add_argument("--receipt", required=True)
    args = parser.parse_args(argv)
    identifier(args.table)

    from spark_jobs.lib.iceberg_session import build_spark

    spark = build_spark("platform-repartition-small-channel", shuffle_partitions=8)
    receipt = {"table": args.table, "column": args.column, "status": "running"}
    try:
        receipt["before"] = measure(spark, args.table, args.column)
        receipt["snapshot_before"] = spark.sql(
            f"SELECT snapshot_id FROM {identifier(args.table)}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).first()[0]
        receipt["sql"] = [evolve_sql(args.table, args.column), rewrite_sql(args.table)]
        for statement in receipt["sql"]:
            spark.sql(statement).collect()
        receipt["after"] = measure(spark, args.table, args.column)
        check_same(receipt["before"], receipt["after"])
        receipt["status"] = "success"
        return 0
    except Exception as error:
        receipt.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        write_receipt(args.receipt, receipt)
        print(
            f"repartition {receipt['status']}: {receipt.get('before', {}).get('data_files')} → "
            f"{receipt.get('after', {}).get('data_files')} file; receipt {args.receipt}"
        )
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
