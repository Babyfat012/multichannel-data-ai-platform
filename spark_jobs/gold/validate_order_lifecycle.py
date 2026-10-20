"""Validate persisted Gold v2 for one immutable generation; fail on any invariant violation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

from spark_jobs.lib import order_lifecycle_gold as job
from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_lifecycle_gold import validate_gold
from spark_jobs.lib.receipts import completed_at_utc
from spark_jobs.lib.return_label import utc


def main(argv=None):
    """Read actual catalog outputs, independently audit PIT lineage and pure/Spark parity."""
    parser = argparse.ArgumentParser(description="Validate Gold Return v2")
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args(argv)
    spark = build_spark("validate-gold-order-lifecycle", shuffle_partitions=4)
    try:
        silver = job.read_silver(spark, args.cutoff)
        gold = {
            name: spark.table(f"{CATALOG}.gold.lifecycle_{name.removesuffix('_v2')}")
            for name in job.GOLD_TABLES
        }
        gold["label_return_v2"] = gold["label_return_v2"].filter(
            F.col("generation_id") == args.run_id
        )
        result = validate_gold(silver, gold, utc(args.cutoff))
        failed = any(result["checks"].values())
        timing = {"completed_at": completed_at_utc()} if args.receipt else {}
        job.write_receipt(
            args.receipt,
            {
                "run_id": args.run_id,
                "cutoff": args.cutoff,
                "status": "failed" if failed else "passed",
                **result,
                **timing,
            },
        )
        return int(failed)
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
