"""Job Bronze 01b — nạp bảng `walmart.orders` của PostgreSQL vào Iceberg `bronze.raw_walmart_orders`.

Cặp với `bronze/ingest_amazon_order_snapshots.py`. Hai job vì Bronze có đúng HAI nguồn batch, và chúng khác
nhau ở chỗ dữ liệu *đến từ đâu*, không phải ở chỗ *làm gì với nó*:

  * Amazon giao FILE (JSONL trên đĩa/MinIO)  -> `bronze/ingest_amazon_order_snapshots.py`
  * Walmart nằm trong DB của PHÒNG BAN KHÁC   -> job này, đọc qua JDBC
  (TikTok Shop không có trong batch — nó chỉ đi Kafka, nên thuộc phần Flink.)

Vì sao hai BẢNG riêng chứ không một bảng gộp (Q2, chốt 2026-10-06): commit Iceberg là
nguyên khối — hoặc vào hết, hoặc không gì vào. Tách bảng thì một nguồn hỏng chỉ phải chạy
lại nguồn đó, không kéo theo nguồn kia.

Quyết định thiết kế đứng sau file này (chốt 2026-10-06, xem HANDOFF.bronze.md mục 1):
  * Q1 — `raw_payload` GIỮ NGUYÊN chuỗi JSON, không parse. Bronze là bảng đầu tiên của
    lakehouse: nhiệm vụ của nó là chép trung thực, việc parse và quy chuẩn thuộc Silver.
  * Q3 — cột provenance đặt tên riêng cho từng bảng, không gộp tên chung:
      `source_table`  thay cho `source_file` của Amazon. Vì sao không dùng
                      `input_file_name()`: qua JDBC hàm đó trả CHUỖI RỖNG, không phải lỗi,
                      nên dùng nó là âm thầm mất provenance.
      `fetched_at`    thời điểm hệ thống NGUỒN kéo đơn từ Walmart. Cố ý KHÔNG gộp với
                      `ingested_at` — một cái là đồng hồ của phòng ban khác, một cái là
                      đồng hồ của pipeline này. Gộp lại là mất khả năng đo độ trễ nguồn.
      `ingested_at`   lúc Spark ghi dòng này.
      `run_id`        mã lần chạy, để dò ngược log.
  * Q4 — PARTITIONED BY (days(fetched_at)). Hidden partitioning của Iceberg: khai báo theo
    cột thời gian thật, Iceberg tự dẫn ra giá trị ngày. An toàn dù nguồn đổi nhịp kéo, vì
    không phụ thuộc vào một cột ngày do người ghi tự điền.

Hai cái bẫy của JDBC đã xử lý trong file này, giải thích tại chỗ:
  * `raw_payload` là JSONB -> ép sang text ngay trong câu SQL, xem `source_query`.
  * Spark mặc định mở MỘT kết nối -> một task kéo hết bảng, xem `read_jdbc`.

Ví dụ chạy (cần cả driver PostgreSQL trong --packages, xem scripts/evidence/measure_order_snapshots_bronze.sh):
    spark-submit --master spark://spark-master:7077 \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,\
org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.postgresql:postgresql:42.7.4 \
      /opt/spark_jobs/bronze/ingest_walmart_order_snapshots.py --num-partitions 16
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

CATALOG = "lakehouse"
NAMESPACE = "bronze"

# Bảng nguồn trong DB `sources` — cố ý mang tên schema của Walmart để nhìn là biết nó mô
# phỏng hệ thống của phòng ban khác, không phải bảng do data team sở hữu.
SOURCE_TABLE = "walmart.orders"

# Cột dùng để CHIA VIỆC ĐỌC. `export_date` kiểu DATE, và Spark JDBC nhận partitionColumn
# kiểu số, DATE hoặc TIMESTAMP — KHÔNG bắt buộc phải là cột số. Nên không cần đi tìm `ctid`
# hay thêm cột id vào bảng nguồn (sửa bảng của "phòng ban khác" là việc không nên làm).
PARTITION_COLUMN = "export_date"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Đọc tham số dòng lệnh. Tách khỏi `main` để test được mà không cần SparkSession."""
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--run-id",
        default=None,
        help="Mã của lần chạy này, ghi vào mọi dòng. Mặc định sinh uuid4. Giai đoạn "
        "Airflow sẽ truyền run_id thật của DAG vào đây để dò ngược được log.",
    )
    p.add_argument(
        "--num-partitions",
        type=int,
        default=16,
        help="Số kết nối JDBC song song. 1 = hành vi mặc định của Spark (một task kéo cả "
        "bảng). Đặt 16 để 2 worker x 2 core có việc liên tục mà không mở quá nhiều "
        "kết nối tới PostgreSQL.",
    )
    p.add_argument(
        "--aqe",
        choices=("on", "off"),
        default="on",
        help="Adaptive Query Execution. Giữ cờ này để so được với job Amazon; lưu ý số đo "
        "ở job Amazon cho thấy AQE KHÔNG chữa được skew khi không có JOIN.",
    )
    p.add_argument(
        "--table",
        default="raw_walmart_orders",
        help="Tên bảng trong namespace bronze. Lần chạy thử nên dùng tên khác "
        "(ví dụ raw_walmart_orders_smoke) để không đè bảng thật.",
    )
    return p.parse_args(argv)


def jdbc_options() -> dict[str, str]:
    """Thông tin kết nối PostgreSQL, lấy từ biến môi trường (`env_file: .env` của compose).

    Dùng tên service `postgres` chứ không `localhost`: job chạy TRONG mạng Compose.
    """
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    return {
        "url": f"jdbc:postgresql://{host}:{port}/{db}",
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def source_query() -> str:
    """Câu SQL làm nguồn đọc, bọc trong ngoặc để Spark dùng làm bảng dẫn xuất.

    Vì sao không đọc thẳng `walmart.orders` mà phải qua câu SQL: cột `raw_payload` kiểu
    JSONB, và JDBC trả nó về dưới dạng `OTHER` (java.sql.Types 1111) — Spark không có
    kiểu Catalyst tương ứng và có thể gãy ngay lúc suy ra schema. Ép `::text` tại nguồn
    thì PostgreSQL trả về đúng kiểu `text`, Spark nhận thành StringType.

    Và ép sang text KHÔNG làm mất gì theo Q1: Bronze vốn phải giữ nguyên chuỗi JSON. Việc
    parse thuộc Silver. Nếu để JSONB thì chính PostgreSQL đã parse hộ — tức là nguồn đã
    "sửa" dữ liệu trước khi Bronze chép, đúng cái Bronze phải tránh.
    """
    return f"""(
        SELECT purchase_order_id,
               raw_payload::text AS raw_payload,
               fetched_at,
               {PARTITION_COLUMN}
        FROM {SOURCE_TABLE}
    ) AS src"""


def build_spark(aqe: str, app_name: str) -> SparkSession:
    """Dựng SparkSession đã nối vào catalog Iceberg trên Lakekeeper.

    Giống `bronze/ingest_amazon_order_snapshots.py`: Lakekeeper chỉ giữ con trỏ tới metadata mới nhất của mỗi
    bảng, còn file metadata và file dữ liệu thật nằm trên MinIO. Spark mang sẵn access key
    của MinIO nên không xin credential tạm — warehouse đã tạo với sts-enabled=false.
    """
    minio_endpoint = os.environ.get("MINIO_INTERNAL_ENDPOINT", "http://minio:9000")
    warehouse = os.environ.get("WAREHOUSE_NAME", "fsds")
    catalog_uri = os.environ.get("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")

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
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", minio_endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.region", "local-01")
        .config("spark.sql.iceberg.compression-codec", "zstd")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///spark-events")
        .config("spark.sql.adaptive.enabled", "true" if aqe == "on" else "false")
    )
    return builder.getOrCreate()


def read_bounds(spark: SparkSession) -> tuple[str, str, int]:
    """Hỏi PostgreSQL dải `export_date` thật và số dòng, TRƯỚC khi đọc dữ liệu.

    Vì sao phải hỏi thay vì đoán: `lowerBound`/`upperBound` của Spark không phải bộ lọc —
    chúng chỉ dùng để chia dải thành `numPartitions` khoảng. Đoán sai dải thì vẫn đủ dòng
    (dòng ngoài dải dồn vào khoảng đầu/cuối) nhưng việc chia hết ý nghĩa: một task ôm gần
    hết bảng. Nên đây là một lượt truy vấn rẻ để đổi lấy việc chia đúng.

    Trả về `upper` đã cộng thêm một ngày: Spark sinh các khoảng nửa mở, nếu `upper` đúng
    bằng ngày lớn nhất thì ngày đó rơi vào khoảng cuối cùng cùng với phần dư.
    """
    opts = jdbc_options()
    stats = f"""(
        SELECT min({PARTITION_COLUMN})            AS lo,
               max({PARTITION_COLUMN}) + 1        AS hi,
               count(*)                           AS n
        FROM {SOURCE_TABLE}
    ) AS bounds"""
    row = spark.read.format("jdbc").options(**opts, dbtable=stats).load().first()
    return str(row["lo"]), str(row["hi"]), int(row["n"])


def read_jdbc(spark: SparkSession, lower: str, upper: str, num_partitions: int) -> DataFrame:
    """Đọc bảng nguồn qua JDBC, chia thành `num_partitions` lượt đọc song song.

    Không có bốn tuỳ chọn partition thì Spark mở ĐÚNG MỘT kết nối và một task kéo toàn bộ
    bảng — 1,3 triệu dòng về một executor, hai worker còn lại ngồi không. Bốn tuỳ chọn phải
    đi cùng nhau; thiếu một cái là Spark lặng lẽ bỏ qua cả nhóm.

    `fetchsize` là số dòng driver kéo về mỗi lượt. Mặc định của PostgreSQL JDBC là 0 =
    "lấy hết kết quả vào RAM trước đã", mà mỗi dòng mang một payload JSON -> dễ OOM
    executor 2 GB. Đặt mức vừa phải để driver kéo theo từng mẻ.
    """
    opts = jdbc_options()
    return (
        spark.read.format("jdbc")
        .options(**opts)
        .option("dbtable", source_query())
        .option("partitionColumn", PARTITION_COLUMN)
        .option("lowerBound", lower)
        .option("upperBound", upper)
        .option("numPartitions", str(num_partitions))
        .option("fetchsize", "10000")
        .load()
    )


def add_provenance(df: DataFrame, run_id: str) -> DataFrame:
    """Thêm bộ cột provenance của riêng bảng Walmart (Q3).

    `source_table` đặt cứng chứ không gọi `input_file_name()`: qua JDBC hàm đó trả chuỗi
    rỗng mà không báo lỗi, nên dùng nó là âm thầm mất cột nguồn gốc.
    """
    return (
        df.withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
    )


def write_bronze(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Ghi (thay thế) bảng Iceberg, partition ẩn theo ngày của `fetched_at` (Q4).

    `days(fetched_at)` là hidden partitioning: khai báo một lần ở đây, và từ đó mọi câu
    truy vấn lọc theo `fetched_at` tự được bỏ qua partition không liên quan — người viết
    SQL không phải biết bảng chia theo gì.

    `createOrReplace` là một commit nguyên khối: hoặc bảng mới hiện ra đầy đủ, hoặc không
    gì đổi. Gãy nửa đường thì bảng cũ vẫn nguyên, chạy lại được.

    Phải dùng `F.days(...)`, KHÔNG phải `F.expr("days(fetched_at)")`. `partitionedBy` nhận
    một biểu thức phân hoạch (partition transform) chứ không phải một biểu thức SQL thường,
    nên `expr` bị từ chối với `AnalysisException: Invalid partition transformation`. PySpark
    có sẵn `days`, `months`, `years`, `hours`, `bucket` cho đúng việc này.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    full_name = f"{CATALOG}.{NAMESPACE}.{table}"
    df.writeTo(full_name).partitionedBy(F.days("fetched_at")).createOrReplace()


def main(argv: list[str] | None = None) -> int:
    """Chạy job và in số đo để đối chiếu với số dòng thật trong PostgreSQL."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    app_name = f"bronze-walmart-jdbc-{args.num_partitions}p-{args.table}"

    spark = build_spark(args.aqe, app_name)
    try:
        lower, upper, source_rows = read_bounds(spark)
        if source_rows == 0:
            print(
                f"{SOURCE_TABLE} đang RỖNG. Nạp trước bằng:\n"
                "  uv run python -m generator load-postgres --source /data/raw "
                "--sample-days 20 --dsn postgresql://<user>:<pass>@localhost:5432/sources",
                file=sys.stderr,
            )
            return 1

        print(f"run_id            = {run_id}")
        print(f"bảng nguồn        = {SOURCE_TABLE} ({source_rows:,} dòng)")
        print(f"bảng đích         = {CATALOG}.{NAMESPACE}.{args.table}")
        print(f"chia theo          = {PARTITION_COLUMN} [{lower} .. {upper})")
        print(f"số kết nối JDBC   = {args.num_partitions}")
        print(f"AQE               = {args.aqe}")

        started = time.monotonic()
        df = add_provenance(read_jdbc(spark, lower, upper, args.num_partitions), run_id)
        write_bronze(spark, df, args.table)
        elapsed = time.monotonic() - started

        written = spark.table(f"{CATALOG}.{NAMESPACE}.{args.table}")
        rows = written.count()
        days = written.select(F.to_date("fetched_at").alias("d")).distinct().count()
        print(f"XONG sau {elapsed:.1f}s — {rows:,} dòng, {days} ngày fetched_at")
        if rows != source_rows:
            print(
                f"CẢNH BÁO: lệch {source_rows - rows:,} dòng so với nguồn. Bronze phải "
                "chép trung thực, lệch là có lỗi — kiểm lại trước khi dùng bảng này.",
                file=sys.stderr,
            )
            return 1
        print("Số dòng khớp nguồn.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
