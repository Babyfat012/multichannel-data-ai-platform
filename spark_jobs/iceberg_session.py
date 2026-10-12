"""Dựng SparkSession đã nối vào catalog Iceberg trên Lakekeeper — dùng chung cho job Silver.

Vì sao có file này mà hai job Bronze KHÔNG dùng nó: `01_bronze_baseline.py` và
`01b_bronze_walmart.py` là **bằng chứng đã đo** của rubric (`docs/spark.md` mục 5 và 6, kèm
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

from pyspark.sql import SparkSession

#: Tên catalog Iceberg, khớp `scripts/setup_lakekeeper.sh` và hai job Bronze.
CATALOG = "lakehouse"


def build_spark(app_name: str, shuffle_partitions: int | None = None) -> SparkSession:
    """Dựng SparkSession nối vào Lakekeeper + MinIO, đã ghim múi giờ UTC.

    Lakekeeper chỉ giữ con trỏ tới metadata mới nhất của mỗi bảng; file metadata và file dữ
    liệu thật nằm trên MinIO. Spark mang sẵn access key của MinIO nên không xin credential
    tạm (vended credentials) từ Lakekeeper — warehouse đã tạo với `sts-enabled=false`.

    `shuffle_partitions` để `None` thì giữ mặc định của Spark (200). Tham số này có mặt vì
    job Silver CÓ shuffle thật (dedup theo khoá), khác hẳn job Bronze chỉ shuffle lúc ghi.
    """
    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]
    minio_endpoint = os.environ.get("MINIO_INTERNAL_ENDPOINT", "http://minio:9000")
    warehouse = os.environ.get("WAREHOUSE_NAME", "fsds")
    catalog_uri = os.environ.get(
        "LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog"
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", catalog_uri)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config(
            f"spark.sql.catalog.{CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", minio_endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", minio_user)
        .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", minio_password)
        .config(f"spark.sql.catalog.{CATALOG}.s3.region", "local-01")
        .config("spark.sql.iceberg.compression-codec", "zstd")
        # GHIM múi giờ — xem lý do ở docstring đầu file. Đặt MỘT chỗ, không rải trong
        # từng phép đổi, để không có chỗ nào quên.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///spark-events")
    )
    if shuffle_partitions is not None:
        builder = builder.config(
            "spark.sql.shuffle.partitions", str(shuffle_partitions)
        )
    return builder.getOrCreate()
