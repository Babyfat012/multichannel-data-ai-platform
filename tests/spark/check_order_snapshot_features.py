"""Isolated Spark regression suite; run inside the cached Airflow image.

No Kafka/Iceberg services, catalog, warehouse or project volumes are used.
"""

import importlib.util
import os
import unittest
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spec = importlib.util.spec_from_file_location(
    "feature_validator",
    Path(__file__).resolve().parents[2]
    / "spark_jobs/gold/validate_order_snapshot_return_features.py",
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class FeatureValidationTests(unittest.TestCase):
    """Validate feature relations against the corresponding logical fact rows."""

    @classmethod
    def setUpClass(cls):
        """Start an isolated local Spark session for feature validation."""
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("validation-regression")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        """Stop the shared Spark session after validation tests."""
        cls.spark.stop()

    def frames(self):
        """Build matching fact and feature DataFrames with stable keys and timestamps."""
        fact = self.spark.createDataFrame(
            [("a", "2026-01-01"), ("b", "2026-01-02")], ["order_line_key", "created_at"]
        )
        fact = fact.withColumn("created_at", F.to_timestamp("created_at"))
        features = (
            fact.withColumnRenamed("created_at", "event_timestamp")
            .withColumn("created", F.to_timestamp(F.lit("2026-02-01")))
            .withColumn("quantity", F.lit(1))
            .withColumn("unit_net_amount", F.lit(10))
            .withColumn("run_id", F.lit("run-1"))
        )
        return fact, features

    def test_matching_logical_rows_pass(self):
        """Verify matching logical fact and feature rows pass validation."""
        fact, features = self.frames()
        self.assertEqual(module.validate_feature_frames(fact, features, "run-1")["feature_rows"], 2)

    def test_equal_count_with_substituted_key_fails(self):
        """Reject a substituted key even when the row count is unchanged."""
        fact, features = self.frames()
        features = features.withColumn(
            "order_line_key",
            F.when(F.col("order_line_key") == "b", "c").otherwise(F.col("order_line_key")),
        )
        with self.assertRaises(ValueError):
            module.validate_feature_frames(fact, features, "run-1")

    def test_wrong_prediction_timestamp_fails(self):
        """Reject features whose prediction timestamp differs from the fact."""
        fact, features = self.frames()
        features = features.withColumn("event_timestamp", F.to_timestamp(F.lit("2025-12-31")))
        with self.assertRaises(ValueError):
            module.validate_feature_frames(fact, features, "run-1")

    def test_logical_deletion_is_not_physical_file_row_count(self):
        """Verify validation uses logical rows after deletion rather than physical file counts."""
        fact, features = self.frames()
        # The validator receives a logical relation with one removed row.
        # A physical file could still contain both records until compaction.
        fact = fact.filter(F.col("order_line_key") == "a")
        features = features.filter(F.col("order_line_key") == "a")
        self.assertEqual(module.validate_feature_frames(fact, features, "run-1")["fact_rows"], 1)


if __name__ == "__main__":
    unittest.main()
