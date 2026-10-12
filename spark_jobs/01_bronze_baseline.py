"""Job Bronze 01 — nạp export gốc của Amazon vào bảng Iceberg `bronze.raw_amazon_orders`.

Bronze là bảng ĐẦU TIÊN của lakehouse: trước nó không có bảng nào, chỉ có file do Channel
giao. Vì vậy job này KHÔNG parse JSON và KHÔNG làm sạch gì — nó chỉ chuyển file thành bảng
và gắn thêm cột cho biết mỗi dòng từ đâu tới.

Quyết định thiết kế đứng sau file này (chốt 2026-10-06..10):
  * Giữ nguyên chuỗi JSON ở cột `payload`; việc parse để sang Silver. Lý do: tên khoá của
    Amazon đổi giữa kỳ (`AmazonOrderId` tháng 1 -> `orderId` tháng 5). Parse ở Bronze
    nghĩa là giải bài schema evolution ngay tại Bronze, trái với vai trò "chép nguyên bản".
  * Hệ quả đã chấp nhận: bảng này KHÔNG có cột mã đơn hàng. Muốn tra một đơn thì tra ở
    Silver. Bronze tồn tại để chạy lại khi Silver lỗi.
  * Cột provenance (nguồn gốc từng dòng): `source_file`, `ingested_at`, `run_id`.
  * PARTITIONED BY (export_date) — khớp đúng cách file nằm trên đĩa.

Vì sao có chế độ "baseline xấu": rubric cho điểm khi CHỨNG MINH tìm ra và sửa được vấn đề
hiệu năng. Dữ liệu lệch 224 lần giữa ngày to nhất (1,64 GB) và nhỏ nhất (7,3 MB) -> skew.
Bật AQE ngay từ đầu thì Spark tự sửa, không còn ảnh "trước" để chứng minh. Nên job này cho
tắt AQE bằng cờ, chạy một lần xấu để chụp Spark UI, rồi chạy lại với AQE bật.

Ví dụ ba lần chạy đã chốt:
    # 1. baseline xấu, 20 ngày rải đều (có cả ngày bé và ngày to) -> ảnh skew + số giây
    spark-submit ... 01_bronze_baseline.py --sample-days 20 --aqe off \
        --table raw_amazon_orders_sample
    # 2. đã sửa, CÙNG 20 ngày đó -> ảnh đẹp + số giây so được vì cùng quy mô
    spark-submit ... 01_bronze_baseline.py --sample-days 20 --aqe on \
        --table raw_amazon_orders_sample
    # 3. chạy thật, cả 180 ngày
    spark-submit ... 01_bronze_baseline.py --aqe on
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# spark-submit đưa thư mục của file script vào sys.path, nên import phẳng là đủ.
# day_selection.py cố tình KHÔNG import pyspark để test được trên máy host.
from day_selection import pick_sample_days, pick_window_days

# Thư mục export của Amazon bên trong container Spark (mount read-only từ /data/raw).
# Dưới nó là export_date=YYYY-MM-DD/part-*.jsonl -> Spark tự nhận `export_date` là cột
# partition nhờ quy ước tên thư mục `cột=giá trị`.
AMAZON_ROOT = "/data/raw/source-exports/channel=amazon/dataset=orders"

CATALOG = "lakehouse"
NAMESPACE = "bronze"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Đọc tham số dòng lệnh.

    Tách riêng khỏi `main` để test được mà không cần SparkSession.
    """
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--run-id",
        default=None,
        help="Mã của lần chạy này, ghi vào mọi dòng. Mặc định sinh uuid4. "
        "Giai đoạn Airflow sẽ truyền run_id thật của DAG vào đây để dò ngược được log.",
    )
    p.add_argument(
        "--sample-days",
        type=int,
        default=0,
        help="Chỉ nạp N ngày RẢI ĐỀU trên toàn dải thay vì cả 180 ngày. Rải đều (không "
        "phải N ngày cuối) vì các ngày cuối to gần bằng nhau, lấy liền nhau sẽ KHÔNG "
        "thấy skew. 0 = nạp hết.",
    )
    p.add_argument(
        "--window-days",
        type=int,
        default=0,
        help="Chỉ nạp N ngày LIỀN NHAU thay vì rải đều. Dùng khi cần chứng minh BẢN TRÙNG: "
        "generator xuất lại một Order trong khoảng overlap_days=3, nên ngày rải đều "
        "(cách nhau 9-10 ngày) không bao giờ chứa cùng một Order và dedup bắt được 0 bản. "
        "Đặt >= 7 để phủ cửa sổ ±3. Loại trừ với --sample-days. 0 = không dùng.",
    )
    p.add_argument(
        "--window-start",
        default=None,
        help="Ngày bắt đầu cửa sổ liền nhau (YYYY-MM-DD). Để trống thì lấy cửa sổ ở GIỮA "
        "dải. Chỉ có nghĩa khi đi cùng --window-days.",
    )
    p.add_argument(
        "--aqe",
        choices=("on", "off"),
        default="on",
        help="Adaptive Query Execution. `off` giữ lại skew để chụp ảnh làm bằng chứng.",
    )
    p.add_argument(
        "--shuffle-partitions",
        type=int,
        default=200,
        help="spark.sql.shuffle.partitions. Giữ 200 (mặc định của Spark) ở baseline.",
    )
    p.add_argument(
        "--table",
        default="raw_amazon_orders",
        help="Tên bảng trong namespace bronze. Lần chạy thử nên dùng tên khác "
        "(ví dụ raw_amazon_orders_sample) để không đè bảng thật.",
    )
    p.add_argument(
        "--source",
        default=AMAZON_ROOT,
        help="Thư mục gốc chứa export_date=*/ của Amazon.",
    )
    return p.parse_args(argv)


def build_spark(aqe: str, shuffle_partitions: int, app_name: str) -> SparkSession:
    """Dựng SparkSession đã nối vào catalog Iceberg trên Lakekeeper.

    Lakekeeper chỉ giữ con trỏ tới metadata mới nhất của mỗi bảng; file metadata và file
    dữ liệu thật nằm trên MinIO. Spark mang sẵn access key của MinIO nên không xin
    credential tạm (vended credentials) từ Lakekeeper — warehouse đã tạo với
    sts-enabled=false.
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
        # JSON chữ lặp tên khoá hàng trăm triệu lần -> zstd nén mạnh hơn snappy khá nhiều,
        # mà `/` chỉ còn ~37 GB.
        .config("spark.sql.iceberg.compression-codec", "zstd")
        # Ghi lại toàn bộ sự kiện của lần chạy ra /spark-events (bind mount tới
        # ./out/spark-events trên host). Spark UI ở cổng 4040 chết cùng job; có event log
        # thì mở History Server xem lại và chụp ảnh skew bất cứ lúc nào.
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///spark-events")
        .config("spark.sql.adaptive.enabled", "true" if aqe == "on" else "false")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    )
    return builder.getOrCreate()


def list_export_days(spark: SparkSession, source: str) -> list[str]:
    """Liệt kê giá trị export_date có thật dưới `source`, đọc từ HDFS API của Hadoop.

    Dùng Hadoop FileSystem thay vì `os.listdir` để code không phụ thuộc vào việc thư mục
    có nằm trên đĩa local hay trên s3a://.
    """
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(source)
    fs = path.getFileSystem(hadoop_conf)
    days = []
    for status in fs.listStatus(path):
        name = status.getPath().getName()
        if status.isDirectory() and name.startswith("export_date="):
            days.append(name.split("=", 1)[1])
    return sorted(days)


def read_raw(spark: SparkSession, source: str, days: list[str] | None) -> DataFrame:
    """Đọc JSONL thành MỘT cột chuỗi, kèm cột cho biết dòng đến từ file nào.

    `spark.read.text` cho mỗi dòng file thành một dòng bảng ở cột `value`, không phân tích
    JSON — đúng điều Bronze cần. `input_file_name()` trả đường dẫn file đang đọc; hàm này
    chỉ hoạt động với nguồn FILE (qua JDBC nó trả chuỗi rỗng).
    """
    reader = spark.read.option("basePath", source)
    if days is None:
        df = reader.text(source)
    else:
        # Trỏ thẳng vào từng thư mục ngày: Spark khỏi phải liệt kê 3.316 file của 180 ngày
        # rồi mới loại bỏ. `basePath` ở trên giữ cho `export_date` vẫn được nhận là cột.
        # Phải truyền MỘT danh sách: `text(path1, path2)` sẽ gán path2 vào tham số
        # `wholetext` và báo lỗi "For input string: <đường dẫn>".
        df = reader.text([f"{source}/export_date={d}" for d in days])
    return df.select(
        F.col("value").alias("payload"),
        F.input_file_name().alias("source_file"),
        F.col("export_date"),
    )


def add_provenance(df: DataFrame, run_id: str) -> DataFrame:
    """Thêm hai cột còn lại của bộ provenance: lúc Spark ghi, và mã lần chạy."""
    return df.withColumn("ingested_at", F.current_timestamp()).withColumn(
        "run_id", F.lit(run_id)
    )


def write_bronze(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Ghi (thay thế) bảng Iceberg, partition theo export_date.

    `createOrReplace` là một commit nguyên khối (atomic): hoặc bảng mới hiện ra đầy đủ,
    hoặc không có gì đổi. Nửa đường hỏng thì bảng cũ vẫn nguyên, chạy lại được.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    full_name = f"{CATALOG}.{NAMESPACE}.{table}"
    df.writeTo(full_name).partitionedBy("export_date").createOrReplace()


def main(argv: list[str] | None = None) -> int:
    """Chạy toàn bộ job và in ra số đo để đối chiếu với run_manifest.json của generator."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    app_name = f"bronze-amazon-aqe-{args.aqe}-{args.table}"

    spark = build_spark(args.aqe, args.shuffle_partitions, app_name)
    try:
        all_days = list_export_days(spark, args.source)
        if not all_days:
            print(f"KHÔNG tìm thấy export_date=* nào dưới {args.source}", file=sys.stderr)
            return 1
        if args.sample_days > 0 and args.window_days > 0:
            print(
                "--sample-days và --window-days loại trừ nhau: một cái rải đều để thấy "
                "skew, một cái liền nhau để thấy bản trùng. Chọn MỘT.",
                file=sys.stderr,
            )
            return 2
        if args.window_days > 0:
            days = pick_window_days(all_days, args.window_days, args.window_start)
        elif args.sample_days > 0:
            days = pick_sample_days(all_days, args.sample_days)
        else:
            days = None

        print(f"run_id          = {run_id}")
        print(f"bảng đích       = {CATALOG}.{NAMESPACE}.{args.table}")
        print(f"AQE             = {args.aqe}")
        print(f"ngày có trên đĩa= {len(all_days)}")
        print(f"ngày sẽ nạp     = {len(days) if days else len(all_days)}")
        if days:
            cach = "liền nhau" if args.window_days > 0 else "rải đều"
            print(f"  -> {days[0]} ... {days[-1]} ({cach})")

        started = time.monotonic()
        df = add_provenance(read_raw(spark, args.source, days), run_id)
        write_bronze(spark, df, args.table)
        elapsed = time.monotonic() - started

        rows = spark.table(f"{CATALOG}.{NAMESPACE}.{args.table}").count()
        print(f"XONG sau {elapsed:.1f}s — {rows:,} dòng đã ghi")
        print(f"Spark UI của lần chạy này đã kết thúc; xem history ở cổng 8080 nếu bật.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
