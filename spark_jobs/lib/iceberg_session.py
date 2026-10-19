"""Dựng SparkSession đã nối vào catalog Iceberg trên Lakekeeper — dùng chung cho job Silver.

Vì sao có file này mà hai job Bronze KHÔNG dùng nó: `bronze/ingest_amazon_order_snapshots.py` và
`bronze/ingest_walmart_order_snapshots.py` là **bằng chứng đã đo** của rubric (`docs/spark.md` mục 5 và 6, kèm
ảnh Spark UI). Sửa chúng để bỏ phần trùng nghĩa là đổi file đã có số đo đi kèm, nên hai job
đó giữ nguyên bản `build_spark` của riêng chúng. File này là chỗ dùng chung cho mọi job
VIẾT TỪ ĐÂY TRỞ ĐI.

Khác biệt so với bản của Bronze, và vì sao:
  * GHIM `spark.sql.session.timeZone = UTC`. Bắt buộc với Silver, không phải tuỳ chọn:
    `docs/silver.md` mục 4.2 — hàm ngày của Spark đổi kết quả theo múi giờ phiên, và đích
    Return Label có cửa sổ 30 ngày, nên ranh giới ngày quyết định một dòng có được gán nhãn
    hay không. Không ghim thì cùng dữ liệu, máy khác múi giờ, ra nhãn khác.
  * KHÔNG có cờ `--aqe`. Bronze cần tắt AQE để có ảnh "trước" chứng minh skew; Silver không
    phải chứng minh lại điều đó, nên để Spark bật AQE theo mặc định của nó.
"""

from __future__ import annotations

import os

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

#: Tên catalog Iceberg, khớp `scripts/setup/setup_lakekeeper.sh` và hai job Bronze.
CATALOG = "lakehouse"


def build_spark(app_name: str, shuffle_partitions: int | None = None) -> SparkSession:
    """Dựng SparkSession nối vào Lakekeeper + MinIO, đã ghim múi giờ UTC.

    Lakekeeper chỉ giữ con trỏ tới metadata mới nhất của mỗi bảng; file metadata và file dữ
    liệu thật nằm trên MinIO. Spark mang sẵn access key của MinIO nên không xin credential
    tạm (vended credentials) từ Lakekeeper — warehouse đã tạo với `sts-enabled=false`.

    `shuffle_partitions` để `None` thì giữ mặc định của Spark (200). Tham số này có mặt vì
    job Silver CÓ shuffle thật (dedup theo khoá), khác hẳn job Bronze chỉ shuffle lúc ghi.
    """
    from pyspark.sql import SparkSession

    builder = (SparkSession.builder.appName(app_name)
               .config("spark.executorEnv.PYTHONPATH", os.environ.get("PYTHONPATH", "/opt")))
    for key, value in catalog_conf(os.environ).items():
        builder = builder.config(key, value)
    if shuffle_partitions is not None:
        builder = builder.config(
            "spark.sql.shuffle.partitions", str(shuffle_partitions)
        )
    return builder.getOrCreate()


def catalog_conf(env) -> dict[str, str]:
    """Trả cấu hình thuần; mặc định giữ nguyên từng cặp cấu hình MinIO hiện hữu."""
    prefix = f"spark.sql.catalog.{CATALOG}"
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": env.get("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog"),
        f"{prefix}.warehouse": env.get("WAREHOUSE_NAME", "fsds"),
        "spark.sql.iceberg.compression-codec": "zstd",
        "spark.sql.session.timeZone": "UTC",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "file:///spark-events",
    }
    storage = env.get("FSDS_STORAGE", "minio")
    if storage == "minio":
        conf.update({
            f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            f"{prefix}.s3.endpoint": env.get("MINIO_INTERNAL_ENDPOINT", "http://minio:9000"),
            f"{prefix}.s3.path-style-access": "true",
            f"{prefix}.s3.region": "local-01",
        })
    elif storage == "gcs":
        conf.update({
            f"{prefix}.io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "spark.hadoop.fs.gs.auth.service.account.enable": "true",
        })
    else:
        raise ValueError(f"FSDS_STORAGE không hỗ trợ: {storage!r}")
    return conf
