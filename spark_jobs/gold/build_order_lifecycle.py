"""Materialize Gold v2 from Silver ONLY, preserve immutable label generations, write JSON receipt.

Silver must have been built by job 21 with `--as-of` equal to this cutoff (checked through
`silver.lifecycle_stg_build_info`); otherwise the job fails before writing. Evidence history
and coverage validity come from `lifecycle_stg_observed_evidence` / `lifecycle_stg_coverage_validity`.
No Bronze, v1 tables or hidden ground truth are read.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_lifecycle_gold import (
    GOLD_TABLES,
    build_gold,
    read_silver,
    validate_gold,
    write_receipt,
)
from spark_jobs.lib.receipts import write_receipt_schemas
from spark_jobs.lib.return_label import utc


def main(argv=None):
    """Validate before publication and again against written catalog tables."""
    parser = argparse.ArgumentParser(description="Gold Return v2 (fixture)")
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args(argv)
    cutoff = utc(args.cutoff)
    spark = build_spark("gold-order-lifecycle", shuffle_partitions=4)
    try:
        silver = read_silver(spark, cutoff)
        target_label = f"{CATALOG}.gold.lifecycle_label_return"
        if spark.catalog.tableExists(target_label) and (
            spark.table(target_label).filter(F.col("generation_id") == args.run_id).limit(1).count()
        ):
            raise ValueError(
                "generation_id already exists; immutable generations cannot be overwritten"
            )
        gold = build_gold(silver=silver, cutoff=cutoff, run_id=args.run_id)
        result = validate_gold(silver, gold, cutoff)
        if any(result["checks"].values()):
            write_receipt(
                args.receipt,
                {
                    "run_id": args.run_id,
                    "cutoff": cutoff.isoformat(),
                    "status": "failed_before_write",
                    **result,
                },
            )
            return 1
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
        names = {}
        for name in GOLD_TABLES:
            target = f"{CATALOG}.gold.lifecycle_{name.removesuffix('_v2')}"
            writer = gold[name].writeTo(target)
            if name == "label_return_v2" and spark.catalog.tableExists(target):
                writer.append()
            elif name in ("fact_order_lines", "feat_sku_return_rate_90d"):
                writer.partitionedBy(F.days("created_at")).createOrReplace()
            else:
                writer.createOrReplace()
            names[name] = target
        written = {name: spark.table(target) for name, target in names.items()}
        written["label_return_v2"] = written["label_return_v2"].filter(
            F.col("generation_id") == args.run_id
        )
        result = validate_gold(silver, written, cutoff)
        failed = any(result["checks"].values())
        schema_receipt = {}
        if args.receipt:
            schema_receipt["schema_file"] = write_receipt_schemas(
                {names[name]: gold[name].schema.jsonValue() for name in GOLD_TABLES}, args.receipt
            )
        write_receipt(
            args.receipt,
            {
                "run_id": args.run_id,
                "cutoff": cutoff.isoformat(),
                "status": "failed" if failed else "passed",
                "tables": names,
                **result,
                **schema_receipt,
            },
        )
        return int(failed)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
