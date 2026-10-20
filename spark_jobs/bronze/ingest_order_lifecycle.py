"""Nguồn v2 (thư mục fixture) -> Iceberg `bronze.lifecycle_raw_records`, rồi kiểm đủ dòng.

Một bảng chung cho mọi feed, mỗi dòng JSONL một dòng, giữ nguyên `raw_json`; logic ở
`lib/order_lifecycle_lakehouse.py`. Số dòng mong đợi lấy từ `manifest.json` của bộ nguồn (chỉ file có
role `source`), không lấy từ chính dữ liệu đã đọc. Ghi đè toàn bảng (atomic) mỗi lần chạy.

Với `--walmart-jdbc`, đơn Walmart KHÔNG lấy từ file bootstrap mà đọc qua JDBC từ PostgreSQL
(DB phòng ban khác, đúng boundary manifest khai). Tập mã đơn đọc được phải đúng bằng
`native_sources.walmart.snapshot_scope.order_ids`; cột `ingestion_boundary` ghi file/jdbc.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_lifecycle_lakehouse import (
    bronze_from_jdbc,
    bronze_from_root,
    read_source_manifest,
    snapshot_scope_diff,
)
from spark_jobs.lib.receipts import completed_at_utc, write_receipt_schemas

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
DATASET_VERSION = re.compile(r"dataset-[0-9a-f]{32}\Z")
NAMESPACE = "bronze"


def expected_rows(root: Path | str, spark=None) -> tuple[int, str]:
    """Tổng dòng nguồn và dataset_version theo manifest của producer fixture."""
    manifest = read_source_manifest(spark, str(root))
    rows = sum(f["rows"] for f in manifest["files"] if f["role"] == "source")
    return rows, manifest["dataset_version"]


def read_walmart_jdbc(spark, table: str, dataset_version: str):
    """Đọc đơn Walmart của đúng dataset_version qua JDBC; trả (DataFrame, nhãn nguồn không mật khẩu).

    `raw_payload` lưu TEXT nên `::json` giữ nguyên chuỗi gốc (JSONB sẽ sắp lại khóa).
    `json_build_object` dựng lại đúng ba trường của dòng bootstrap.
    """
    if not DATASET_VERSION.fullmatch(dataset_version):
        raise ValueError(f"dataset_version không hợp lệ: {dataset_version!r}")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ["POSTGRES_DB"]
    url = f"jdbc:postgresql://{host}:{port}/{db}"
    query = f"""(SELECT dataset_version, purchase_order_id,
        json_build_object('fetched_at', fetched_at, 'purchase_order_id', purchase_order_id,
                          'raw_payload', raw_payload::json)::text AS raw_json
        FROM {table} WHERE dataset_version = '{dataset_version}') AS src"""
    rows = (
        spark.read.format("jdbc")
        .options(
            url=url,
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            driver="org.postgresql.Driver",
            dbtable=query,
            fetchsize="1000",
        )
        .load()
    )
    return rows, f"{url}#{table}"


def main(argv: list[str] | None = None) -> int:
    """Ghi Bronze v2 và dừng lỗi nếu số dòng/dataset_version không khớp manifest."""
    parser = argparse.ArgumentParser(description="Bronze nguồn v2")
    parser.add_argument(
        "--root", required=True, help="Thư mục một biến thể fixture (có manifest.json)"
    )
    parser.add_argument("--table", default="lifecycle_raw_records")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--receipt", type=Path, default=None)
    parser.add_argument(
        "--walmart-jdbc",
        default=None,
        metavar="SCHEMA.TABLE",
        help="Đọc đơn Walmart qua JDBC từ bảng này thay vì file bootstrap",
    )
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.table):
        parser.error(f"Tên bảng không hợp lệ: {args.table!r}")
    if args.walmart_jdbc and not all(IDENTIFIER.fullmatch(p) for p in args.walmart_jdbc.split(".")):
        parser.error(f"Tên bảng JDBC không hợp lệ: {args.walmart_jdbc!r}")
    root = args.root
    spark = build_spark("bronze-order-lifecycle", shuffle_partitions=4)
    try:
        want, dataset_version = expected_rows(root, spark)
        manifest = read_source_manifest(spark, root)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        target = f"{CATALOG}.{NAMESPACE}.{args.table}"
        bronze = bronze_from_root(spark, str(root), args.run_id)
        if args.walmart_jdbc:
            snapshot = manifest["native_sources"]["walmart"]
            rows, label = read_walmart_jdbc(spark, args.walmart_jdbc, dataset_version)
            rows = rows.cache()
            diff = snapshot_scope_diff(
                [r[0] for r in rows.select("purchase_order_id").collect()],
                snapshot["snapshot_scope"]["order_ids"],
            )
            print(
                f"Walmart JDBC: nguồn={label}, đơn={rows.count():,}, "
                f"thiếu={len(diff['missing'])}, thừa={len(diff['extra'])}, trùng={len(diff['duplicate'])}"
            )
            if any(diff.values()):
                raise ValueError(f"Snapshot Walmart JDBC lệch manifest: {diff}")
            bronze = bronze.filter(F.col("feed") != "walmart.orders").unionByName(
                bronze_from_jdbc(
                    rows, args.run_id, label, snapshot["snapshot_as_of"][:10], snapshot["schema_id"]
                )
            )
        bronze.writeTo(target).partitionedBy(F.col("feed")).createOrReplace()
        table = spark.table(target)
        got = table.count()
        versions = [r[0] for r in table.select("dataset_version").distinct().collect()]
        bad = table.filter(
            F.col("raw_json").isNull() | (F.col("feed") == "") | (F.col("run_id") != args.run_id)
        ).count()
        boundary = {r[0]: r[1] for r in table.groupBy("ingestion_boundary").count().collect()}
        print(
            f"Bronze v2: manifest={want:,}, Bronze={got:,}, lỗi={bad:,}, boundary={boundary}, "
            f"dataset_version={versions}, bảng={target}, run_id={args.run_id}"
        )
        if got != want or bad or versions != [dataset_version]:
            raise ValueError("Bronze v2 không khớp manifest nguồn")
        print("Validate Bronze v2: PASS")
        if args.receipt:
            receipt = {
                "run_id": args.run_id,
                "dataset_version": dataset_version,
                "tables": {"raw_source_records": target},
                "rows": got,
                "completed_at": completed_at_utc(),
                "schema_file": write_receipt_schemas(
                    {target: bronze.schema.jsonValue()}, args.receipt
                ),
            }
            args.receipt.write_text(
                json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
            )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
