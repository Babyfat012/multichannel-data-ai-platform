"""Gold A (order snapshot) → tập train và feature SKU theo ngày cho ML, đúng thời điểm.

Một nhãn chỉ "biết được" từ `label_available_at`: ảnh chụp feature lúc 00:00 ngày S chỉ đếm
nhãn có `label_available_at <= S`, và tập train chỉ lấy nhãn có trước cutoff.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DATASET_VERSION = "order_snapshots"
# Cùng bảng với generator/core/storefronts.py CURRENCY_DECIMALS (Spark không mang theo generator).
CURRENCY_DECIMALS = {"USD": 2, "GBP": 2, "EUR": 2, "VND": 0}
WINDOW_DAYS = 90


def configure_output(spark, output: str) -> None:
    """MinIO local cần endpoint + key; AWS (PLATFORM_STORAGE=s3) dùng IAM role mặc định."""
    if not output.startswith("s3a://") or os.environ.get("PLATFORM_STORAGE", "minio") == "s3":
        return
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop.set("fs.s3a.endpoint", os.environ["MINIO_INTERNAL_ENDPOINT"])
    hadoop.set("fs.s3a.path.style.access", "true")
    hadoop.set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    hadoop.set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])


def minor_units(amount, currency):
    """Tiền decimal → số nguyên đơn vị nhỏ nhất theo tiền tệ; tiền tệ lạ ra NULL, không đoán."""
    scale = F.create_map(*[F.lit(v) for kv in CURRENCY_DECIMALS.items() for v in kv])[currency]
    return F.round(amount * F.pow(F.lit(10), scale)).cast("bigint")


def first_snapshot_day(available_at):
    """Ngày 00:00 đầu tiên mà nhãn đã có: làm tròn lên theo ngày (đúng 00:00 thì chính ngày đó)."""
    day = F.to_date(available_at)
    return F.when(F.to_timestamp(day) == available_at, day).otherwise(F.date_add(day, 1))


def training_rows(fact: DataFrame, labels: DataFrame, cutoff, per_mille: int) -> DataFrame:
    """Line có nhãn biết trước cutoff + thuộc tính lúc đặt; giữ `per_mille`/1000 theo hash khóa."""
    sampled = fact.filter(F.pmod(F.xxhash64("order_line_key"), F.lit(1000)) < per_mille)
    known = labels.filter(F.col("label_available_at") <= F.lit(cutoff).cast("timestamp"))
    return sampled.join(known.select("order_line_key", "label", "run_id"), "order_line_key").select(
        F.lit(DATASET_VERSION).alias("dataset_version"),
        F.col("order_line_key").alias("line_id"),
        "seller_sku",
        "created_at",
        F.col("created_at").alias("event_timestamp"),
        "channel",
        "country",
        "currency",
        F.col("quantity").alias("ordered_basis_qty"),
        minor_units(F.col("line_net_amount"), F.col("currency")).alias("ordered_product_net_minor"),
        "label",
        F.col("run_id").alias("generation_id"),
    )


def sku_daily_features(fact: DataFrame, labels: DataFrame, cutoff) -> DataFrame:
    """Ảnh chụp 00:00 mỗi ngày, từ ngày đặt đầu tiên của SKU tới cutoff: nhãn biết trong 90 ngày."""
    cutoff_day = F.to_date(F.lit(cutoff).cast("timestamp"))
    days = (
        fact.filter(F.col("created_at").isNotNull())
        .groupBy("seller_sku")
        .agg(F.to_date(F.min("created_at")).alias("first_day"))
        .select("seller_sku", F.explode(F.sequence("first_day", cutoff_day)).alias("day"))
    )
    known = (
        labels.join(fact.select("order_line_key", "seller_sku"), "order_line_key")
        .groupBy("seller_sku", first_snapshot_day(F.col("label_available_at")).alias("day"))
        .agg(F.count("*").alias("n"), F.sum("label").alias("p"))
    )
    # rangeBetween theo số ngày: nhãn có từ ngày S-89 tới S ⇔ label_available_at trong (S-90d, S].
    window = (
        Window.partitionBy("seller_sku")
        .orderBy(F.datediff("day", F.lit("1970-01-01")))
        .rangeBetween(-(WINDOW_DAYS - 1), 0)
    )
    # Ngày có nhãn nhưng nằm ngoài lưới (trước ngày đặt đầu tiên/sau cutoff) bị bỏ ở join trái.
    daily = days.join(known, ["seller_sku", "day"], "left").na.fill(0, ["n", "p"])
    labeled = F.sum("n").over(window)
    positive = F.sum("p").over(window)
    return daily.select(
        F.lit(DATASET_VERSION).alias("dataset_version"),
        "seller_sku",
        F.to_timestamp("day").alias("event_timestamp"),
        labeled.cast("bigint").alias("labeled_count_90d"),
        positive.cast("bigint").alias("positive_count_90d"),
        F.when(labeled > 0, positive / labeled).alias("return_or_refund_rate_90d"),
    )
