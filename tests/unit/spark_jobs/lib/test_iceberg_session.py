"""Kiểm offline cấu hình GCP, giữ nguyên MinIO và các chốt bảo vệ triển khai."""

import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from spark_jobs.lib.iceberg_session import catalog_conf

ROOT = Path(__file__).resolve().parents[4]


class ComposeLoader(yaml.SafeLoader):
    """Cho phép đọc tag Compose override trong kiểm tra tĩnh."""


ComposeLoader.add_constructor("!override", lambda loader, node: loader.construct_sequence(node))


def test_default_configuration_is_original():
    """So toàn bộ dict với cấu hình trước khi thêm GCS, không chỉ kiểm vài key."""
    p = "spark.sql.catalog.lakehouse"
    expected = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        p: "org.apache.iceberg.spark.SparkCatalog",
        p + ".type": "rest",
        p + ".uri": "http://lakekeeper:8181/catalog",
        p + ".warehouse": "fsds",
        p + ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        p + ".s3.endpoint": "http://minio:9000",
        p + ".s3.path-style-access": "true",
        p + ".s3.region": "local-01",
        "spark.sql.iceberg.compression-codec": "zstd",
        "spark.sql.session.timeZone": "UTC",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "file:///spark-events",
    }
    assert catalog_conf({}) == expected
    assert catalog_conf({"FSDS_STORAGE": "minio"}) == expected
    env = {
        "MINIO_INTERNAL_ENDPOINT": "http://custom:9000",
        "WAREHOUSE_NAME": "custom",
        "LAKEKEEPER_CATALOG_URI": "http://custom:8181/catalog",
    }
    expected.update(
        {
            p + ".s3.endpoint": env["MINIO_INTERNAL_ENDPOINT"],
            p + ".warehouse": "custom",
            p + ".uri": env["LAKEKEEPER_CATALOG_URI"],
        }
    )
    assert catalog_conf(env) == expected


def test_gcs_has_no_s3_or_key_configuration():
    """GCS dùng FileIO riêng và metadata VM, không chuyển access key sang Spark conf."""
    conf = catalog_conf({"FSDS_STORAGE": "gcs", "AWS_ACCESS_KEY_ID": "ignored"})
    assert conf["spark.sql.catalog.lakehouse.io-impl"] == "org.apache.iceberg.gcp.gcs.GCSFileIO"
    assert conf["spark.hadoop.fs.gs.impl"].endswith(".GoogleHadoopFileSystem")
    assert conf["spark.hadoop.fs.AbstractFileSystem.gs.impl"].endswith(".GoogleHadoopFS")
    assert conf["spark.hadoop.fs.gs.auth.service.account.enable"] == "true"
    assert not any("s3." in k or "key" in k.lower() for k in conf)
    with pytest.raises(ValueError):
        catalog_conf({"FSDS_STORAGE": "typo"})


def test_overlay_ports_services_secrets_and_budget():
    """Mọi port loopback, MinIO bị vô hiệu, tổng quota dưới giới hạn một VM."""
    text = (ROOT / "docker-compose.gcp.yml").read_text()
    services = yaml.load(text, Loader=ComposeLoader)["services"]
    for svc in services.values():
        assert all(port.startswith("127.0.0.1:") for port in svc.get("ports", []))
    for name in ("minio", "minio-init", "generator-batch", "generator-load"):
        assert services[name]["scale"] == 0
        assert services[name]["profiles"] == ["disabled-gcp"]
    assert "0.0.0.0:" not in text
    assert "PRIVATE KEY" not in text
    assert "AWS_ACCESS_KEY_ID" not in text
    assert sum(float(svc.get("cpus", 0)) for svc in services.values()) <= 8
    memory_gib = sum(
        float(str(svc.get("mem_limit", "0g"))[:-1])
        / (1024 if str(svc.get("mem_limit", "0g")).endswith("m") else 1)
        for svc in services.values()
    )
    assert memory_gib <= 32
    for name in ("postgres", "airflow", "spark-worker-1", "spark-worker-2"):
        assert any("/mnt/fsds-data/" in v for v in services[name]["volumes"])
    for name in ("airflow", "spark-master", "spark-worker-1", "spark-worker-2"):
        assert any(
            "gcs-connector-hadoop3-2.2.26-shaded.jar" in v for v in services[name]["volumes"]
        )
    variables = yaml.safe_load((ROOT / "infra/gcp/ansible/group_vars/all.example.yml").read_text())
    assert all(
        len(package.split(":")) == 3 for package in variables["spark_packages_gcp"].split(",")
    )


def test_disk_and_rsync_guards():
    """mkfs phụ thuộc kết quả blkid; rsync không chép env và Git."""
    disk = (ROOT / "infra/gcp/ansible/roles/data_disk/tasks/main.yml").read_text()
    tasks = yaml.safe_load(disk)
    fmt = next(t for t in tasks if "mkfs.ext4" in str(t.get("ansible.builtin.command", "")))
    assert fmt["when"] == "disk_blkid.rc == 2"
    assert disk.index("argv: [blkid") < disk.index("argv: [mkfs.ext4")
    assert "UUID={{ disk_uuid.stdout" in disk
    app = (ROOT / "infra/gcp/ansible/roles/app/tasks/main.yml").read_text()
    assert "--exclude=.env" in app and "--exclude=.git/" in app


def manifest_reader():
    """Trích hàm manifest để kiểm I/O mà không cần cài pyspark trong venv."""
    tree = ast.parse((ROOT / "spark_jobs/lib/order_lifecycle_lakehouse.py").read_text())
    function = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "read_source_manifest"
    )
    module = ast.Module(
        body=[
            ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
            function,
        ],
        type_ignores=[],
    )
    ns = {"json": json, "Path": Path, "re": __import__("re")}
    exec(compile(ast.fix_missing_locations(module), "<manifest-reader>", "exec"), ns)
    return ns["read_source_manifest"]


def test_manifest_local_uses_pathlib(tmp_path):
    """Nhánh local đọc pathlib, không gọi Spark."""
    (tmp_path / "manifest.json").write_text('{"dataset_version":"test"}')
    assert manifest_reader()(None, str(tmp_path)) == {"dataset_version": "test"}


@pytest.mark.parametrize("root", ["file:/tmp/fixture", "gs://raw/fixture", "s3a://raw/fixture"])
def test_manifest_uri_uses_spark(root):
    """URI giữ nguyên scheme và dùng wholetext trên session được truyền vào."""
    calls = []

    def read_text(path, **kwargs):
        """Giả Spark reader để chỉ ghi nhận lệnh đọc manifest."""
        calls.append((path, kwargs))
        return SimpleNamespace(collect=lambda: [{"value": '{"dataset_version":"test"}'}])

    spark = SimpleNamespace(read=SimpleNamespace(text=read_text))
    assert manifest_reader()(spark, root) == {"dataset_version": "test"}
    assert calls == [(root + "/manifest.json", {"wholetext": True})]
