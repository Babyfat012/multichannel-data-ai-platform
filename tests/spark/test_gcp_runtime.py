"""Kiểm Spark thật local/file URI; planner bật FSDS_RUN_SPARK_TESTS=1 để chạy."""

import importlib.util
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("FSDS_RUN_SPARK_TESTS") != "1", reason="Planner chạy Spark riêng"
)


def test_file_uri_and_local_bronze(tmp_path):
    """Đọc cùng fixture qua pathlib/file URI và đối soát đầy đủ Bronze với manifest."""
    from pyspark.sql import SparkSession

    from generator.generate_order_lifecycle_fixture import generate_fixture
    from spark_jobs.lib.order_lifecycle_lakehouse import bronze_from_root, read_source_manifest

    root = tmp_path / "fixture"
    manifest = generate_fixture(root)
    spark = SparkSession.builder.master("local[2]").appName("gcp-manifest-uri-test").getOrCreate()
    job_path = Path(__file__).resolve().parents[2] / "spark_jobs/bronze/ingest_order_lifecycle.py"
    spec = importlib.util.spec_from_file_location("bronze_gcp_test", job_path)
    job = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job)
    try:
        expected = sum(f["rows"] for f in manifest["files"] if f["role"] == "source")
        for path in (str(root), root.as_uri()):
            assert read_source_manifest(spark, path) == manifest
            assert job.expected_rows(path, spark) == (expected, manifest["dataset_version"])
            bronze = bronze_from_root(spark, path, "uri-test")
            assert bronze.count() == expected
            assert bronze.filter(bronze.dataset_version != manifest["dataset_version"]).count() == 0
    finally:
        spark.stop()
