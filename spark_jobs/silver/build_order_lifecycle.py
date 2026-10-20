"""Bronze v2 -> các bảng Silver theo thực thể (`silver.lifecycle_stg_*`), rồi đối soát.

Logic và các bất biến ở `lib/order_lifecycle_lakehouse.py`. Đối soát chạy trên bảng VỪA GHI (đọc lại
từ catalog), không trên DataFrame trong bộ nhớ. Bất kỳ check nào khác 0 thì job lỗi.
Số cách ly không làm fail job. Receipt JSON (checks/info/bảng/run) in ra log và ghi file nếu có `--receipt`.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.receipts import completed_at_utc, write_receipt_schemas
from spark_jobs.lib.order_lifecycle_lakehouse import build_silver, reconcile

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
NAMESPACE = "silver"
TABLES = (
    "line_commitments", "allocations", "deliveries", "return_events", "refunds",
    "listing_versions", "stock_operations", "stock_snapshots", "inbound_events",
    "uncertainties", "coverage", "line_lifecycle", "native_order_lines", "coverage_receipts", "quarantine_records",
    "observed_evidence", "coverage_validity",
)
# Lịch sử quan sát: Gold đọc theo thời điểm biết ⇒ partition theo ngày quan sát, không theo created_at.
PARTITION_BY_OBSERVED = ("observed_evidence",)


def main(argv: list[str] | None = None) -> int:
    """Ghi Silver v2, đối soát trên bảng đã ghi, dừng lỗi nếu có vi phạm."""
    parser = argparse.ArgumentParser(description="Silver nguồn v2")
    parser.add_argument("--bronze-table", default="lifecycle_raw_records")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of", default=None, help="Chỉ dùng dòng quan sát trước thời điểm này (UTC)")
    parser.add_argument("--contracts-dir", default=None)
    parser.add_argument("--receipt", type=Path, default=None)
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.bronze_table):
        parser.error(f"Tên bảng không hợp lệ: {args.bronze_table!r}")

    spark = build_spark("silver-order-lifecycle", shuffle_partitions=4)
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        bronze = spark.table(f"{CATALOG}.bronze.{args.bronze_table}")
        silver = build_silver(bronze, as_of=args.as_of, contracts_dir=args.contracts_dir)
        names = {}
        for name in TABLES:
            table_name = "lifecycle_quarantine_records" if name == "quarantine_records" else f"lifecycle_stg_{name}"
            target = f"{CATALOG}.{NAMESPACE}.{table_name}"
            writer = silver[name].writeTo(target)
            if name in PARTITION_BY_OBSERVED:
                writer = writer.partitionedBy(F.days("source_observed_at"))
            writer.createOrReplace()
            names[name] = target
        # Gold kiểm Silver được dựng đúng `as_of = cutoff` qua bảng này (một dòng mỗi lần dựng).
        info_target = f"{CATALOG}.{NAMESPACE}.lifecycle_stg_build_info"
        build_info = spark.createDataFrame([(args.run_id, args.as_of)], "run_id string, as_of string") \
            .withColumn("built_at", F.current_timestamp())
        build_info.writeTo(info_target).createOrReplace()
        names["build_info"] = info_target
        written = {name: spark.table(target) for name, target in names.items()}
        written["registry"] = silver["registry"]
        written["events"] = silver["events"]
        written["dedup"] = silver["dedup"]
        written["native_records"] = silver["native_records"]
        written["parsed_native_records"] = silver["parsed_native_records"]
        result = reconcile(written)
        receipt = {"run_id": args.run_id, "as_of": args.as_of, "bronze_table": f"{CATALOG}.bronze.{args.bronze_table}",
                   "tables": names, **result}
        if args.receipt:
            schemas = {names[name]: silver[name].schema.jsonValue() for name in TABLES}
            schemas[info_target] = build_info.schema.jsonValue()
            receipt.update(completed_at=completed_at_utc(),
                           schema_file=write_receipt_schemas(schemas, args.receipt))
        print("Silver v2 receipt: " + json.dumps(receipt, ensure_ascii=False, sort_keys=True))
        if args.receipt:
            args.receipt.parent.mkdir(parents=True, exist_ok=True)
            args.receipt.write_text(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        failed = {k: v for k, v in result["checks"].items() if v}
        if failed:
            raise ValueError(f"Đối soát Silver v2 thất bại: {failed}")
        print("Validate Silver v2: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
