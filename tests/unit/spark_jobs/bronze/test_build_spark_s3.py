"""Nhánh S3 của hai job Bronze phải lấy cấu hình từ catalog_conf, không lọt endpoint MinIO."""

import sys
from types import ModuleType, SimpleNamespace

import pytest

# Venv test không cài pyspark: dựng module giả đủ để hai job import được.
_sql = ModuleType("pyspark.sql")
_sql.SparkSession = SimpleNamespace(builder=None)
_sql.DataFrame = object
_sql.functions = ModuleType("pyspark.sql.functions")
sys.modules.setdefault("pyspark", ModuleType("pyspark"))
sys.modules.setdefault("pyspark.sql", _sql)
sys.modules.setdefault("pyspark.sql.functions", _sql.functions)

from spark_jobs.bronze import ingest_amazon_order_snapshots as amazon  # noqa: E402
from spark_jobs.bronze import ingest_walmart_order_snapshots as walmart  # noqa: E402
from spark_jobs.bronze import validate_order_snapshots as validate  # noqa: E402


class FakeBuilder:
    """Ghi lại mọi config thay vì bật JVM."""

    def __init__(self):
        """Bắt đầu với cấu hình rỗng."""
        self.conf = {}

    def appName(self, _name):
        """Bỏ qua tên app."""
        return self

    def config(self, key, value):
        """Ghi một cặp config."""
        self.conf[key] = value
        return self

    def getOrCreate(self):
        """Trả dict config thay cho session."""
        return self.conf


@pytest.mark.parametrize(
    "build",
    [lambda: amazon.build_spark("on", 200, "t"), lambda: walmart.build_spark("on", "t")],
    ids=["amazon", "walmart"],
)
def test_s3_branch_uses_catalog_conf(monkeypatch, build):
    """PLATFORM_STORAGE=s3 => endpoint s3a theo region, không còn cấu hình MinIO."""
    monkeypatch.setenv("PLATFORM_STORAGE", "s3")
    monkeypatch.setenv("AWS_REGION", "ap-southeast-2")
    for mod in (amazon, walmart):
        monkeypatch.setattr(mod.SparkSession, "builder", FakeBuilder())
    conf = build()
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "s3.ap-southeast-2.amazonaws.com"
    assert conf["spark.sql.catalog.lakehouse.client.region"] == "ap-southeast-2"
    assert conf["spark.sql.adaptive.enabled"] == "true"
    assert not any("minio" in str(v) for v in conf.values())


def test_validate_reads_amazon_source_from_flag():
    """Validate Amazon đếm đúng nguồn được truyền (S3 trên AWS), mặc định vẫn là mount local."""
    assert validate.parse_args(["--channel", "amazon", "--table", "t", "--run-id", "r"]).source == (
        validate.AMAZON_SOURCE
    )
    s3 = "s3a://raw/order_snapshots/source-exports/channel=amazon/dataset=orders"
    args = validate.parse_args(
        ["--channel", "amazon", "--table", "t", "--run-id", "r", "--source", s3]
    )
    read = []
    spark = SimpleNamespace(
        read=SimpleNamespace(text=lambda p: read.append(p) or SimpleNamespace(count=lambda: 7))
    )
    assert validate.amazon_source_rows(spark, 0, None, args.source) == 7
    assert read == [s3]
