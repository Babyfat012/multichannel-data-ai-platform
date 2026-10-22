"""Kiểm ghi/đọc Iceberg S3 với bảng tạm riêng, rồi dọn đúng bảng vừa tạo."""

import sys
import uuid

sys.path.insert(0, "/opt")
from spark_jobs.lib.iceberg_session import build_spark


def main():
    """Đối soát dữ liệu và vị trí S3, dọn bảng trong finally kể cả khi lỗi."""
    spark = build_spark("s3-runtime-smoke", shuffle_partitions=2)
    table = "lakehouse.runtime_smoke.probe_" + uuid.uuid4().hex
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.runtime_smoke")
        spark.range(3).writeTo(table).create()
        assert [r.id for r in spark.table(table).orderBy("id").collect()] == [0, 1, 2]
        locations = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
        assert any(r.col_name == "Location" and r.data_type.startswith("s3://") for r in locations)
        print("S3 Iceberg smoke: PASS")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        spark.stop()


if __name__ == "__main__":
    main()
