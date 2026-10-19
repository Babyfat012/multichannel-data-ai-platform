"""Job Silver 02 — `bronze.raw_walmart_orders` -> `silver.stg_walmart_order_lines`.

Cặp với `02_silver_amazon.py`. Hai bảng Silver RIÊNG, không gộp — chốt ở `docs/silver.md`
mục 2. Lý do: hai kênh có hình khoá dedup khác nhau (Amazon 2 cột, Walmart 3 cột), nên một
job dedup chung phải mang hai nhánh logic rồi vẫn ghi vào một bảng. Việc gộp hai kênh là
của **Gold** (`fact_` / `obt_`), nơi cả hai đã sạch và đã cùng snake_case.

Khác `02_silver_amazon.py` ở ba điểm, đều do NGUỒN khác chứ không do thiết kế:

  1. **Một dạng schema, không hai.** Walmart không có schema evolution, nên job này không
     có nhánh parse thứ hai. Phần việc "gộp hai dạng" nằm hết ở job Amazon.
  2. **Thời gian là epoch mili-giây**, không phải chuỗi ISO. Walmart ghi
     `epoch_millis(...)` (`generator/channels/walmart.py`), Amazon ghi `iso_z(...)`.
  3. **Có trạng thái `Delivered` thật**, Amazon không có. Nhưng nguồn ánh xạ CẢ `DELIVERED`
     lẫn `COMPLETED` thành cùng chữ `"Delivered"` (`walmart.py:34-39`), nên cột thời điểm
     mang tên `status_changed_at`, KHÔNG phải `delivered_at` — xem `docs/silver.md` mục 8.2.

Khoá dedup `(purchase_order_id, customer_order_id, line_number)`, tie-break
`status_changed_at` mới nhất, hoà thì `fetched_at` lớn nhất (`docs/silver.md` mục 3.1).
Vì sao PHẢI có `customer_order_id` trong khoá: `purchase_order_id` là `"1"` + 12 chữ số băm
(không gian 10^12) và ĐÃ ĐO ĐƯỢC một va chạm thật — giá trị `1678172477013` xuất hiện ở hai
`export_date` khác nhau là HAI đơn khác nhau (Houston vs Orlando). Thiếu `customer_order_id`
thì dedup gộp hai đơn thật thành một.

Bốn cột chỉ Walmart có: giữ `buyer_id` + `is_guest`, bỏ `customer_email_id` + `order_type`
(`docs/silver.md` mục 8.1). Bỏ KHÔNG mất dữ liệu — Bronze giữ `raw_payload` nguyên văn.

Bảng này KHÔNG có cột `fulfillment_method`: grep `fulfill` trong
`generator/channels/walmart.py` ra rỗng, kênh này không công bố gì (`docs/silver.md` 8.3).

Chạy:
    spark-submit ... 02_silver_walmart.py
    spark-submit ... 02_silver_walmart.py --source-table raw_walmart_orders_smoke \
        --table stg_walmart_order_lines_smoke
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# spark-submit đưa thư mục của file script vào sys.path, nên import phẳng là đủ.
from iceberg_session import CATALOG, build_spark
from silver_mapping import COUNTRY_ALIASES, WALMART_STATUS

BRONZE_NS = "bronze"
SILVER_NS = "silver"
MONEY = "decimal(14,2)"

# --- Schema của payload Walmart -----------------------------------------------------
# Khai tường minh, cùng lý do như job Amazon: để Spark tự suy thì hình dạng đổi theo lô dữ
# liệu, nghĩa là cùng job hai lần chạy ra hai schema.
#
# Chú ý ba kiểu KHÁC job Amazon:
#   * thời gian = `LongType` (epoch mili-giây), không phải chuỗi;
#   * tiền = `DoubleType` — Walmart ghi `round(...)` ra SỐ, còn Amazon ghi `money()` ra
#     CHUỖI. Cả hai đều ép sang `decimal(14,2)` ở bước sau, nhưng khai sai kiểu ở đây thì
#     `from_json` trả NULL mà không báo lỗi;
#   * `lineNumber` và số lượng = `StringType` — nguồn ghi `str(...)` (`walmart.py:98,106`).

_CHARGE = StructType([
    StructField("chargeType", StringType()),
    StructField("chargeName", StringType()),
    StructField("chargeAmount", StructType([
        StructField("currency", StringType()),
        StructField("amount", DoubleType()),
    ])),
])

_LINE_STATUS = StructType([
    StructField("status", StringType()),
    StructField("trackingInfo", StructType([
        StructField("shipDateTime", LongType()),
        StructField("carrierName", StructType([
            StructField("carrier", StringType()),
        ])),
        StructField("trackingNumber", StringType()),
    ])),
])

_ORDER_LINE = StructType([
    StructField("lineNumber", StringType()),
    StructField("item", StructType([
        StructField("productName", StringType()),
        StructField("sku", StringType()),
    ])),
    StructField("charges", StructType([
        StructField("charge", ArrayType(_CHARGE)),
    ])),
    StructField("orderLineQuantity", StructType([
        StructField("unitOfMeasurement", StringType()),
        StructField("amount", StringType()),
    ])),
    StructField("statusDate", LongType()),
    StructField("orderLineStatuses", StructType([
        StructField("orderLineStatus", ArrayType(_LINE_STATUS)),
    ])),
])

PAYLOAD_SCHEMA = StructType([
    StructField("purchaseOrderId", StringType()),
    StructField("customerOrderId", StringType()),
    StructField("orderDate", LongType()),
    StructField("buyerId", StringType()),
    StructField("isGuest", BooleanType()),
    StructField("shippingInfo", StructType([
        StructField("estimatedDeliveryDate", LongType()),
        StructField("postalAddress", StructType([
            StructField("city", StringType()),
            StructField("postalCode", StringType()),
            StructField("country", StringType()),
        ])),
    ])),
    StructField("orderLines", StructType([
        StructField("orderLine", ArrayType(_ORDER_LINE)),
    ])),
])
# KHÔNG khai `customerEmailId` và `orderType`: hai khoá đó đã quyết bỏ khỏi Silver
# (docs/silver.md mục 8.1). Không khai thì `from_json` bỏ qua luôn, khỏi parse rồi mới drop.

#: Thứ tự cột của bảng ra.
OUTPUT_COLUMNS = [
    # khoá + độ hạt
    "purchase_order_id", "customer_order_id", "line_number",
    # mức Order
    "created_at", "order_status", "channel_status",
    "buyer_id", "is_guest", "estimated_delivery_date",
    "city", "postal_code", "country",
    # mức Order Line
    "sku", "product_name", "quantity", "line_net_amount", "currency",
    "status_changed_at", "ship_date_time", "carrier", "tracking_number",
    # provenance mang từ Bronze (docs/silver.md mục 7)
    "source_table", "fetched_at",
    # cột của riêng Silver
    "processed_at", "run_id",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Đọc tham số dòng lệnh. Tách khỏi `main` để test được mà không cần SparkSession."""
    p = argparse.ArgumentParser(description="Silver: Walmart Order Line")
    p.add_argument("--source-table", default="raw_walmart_orders",
                   help="bảng Bronze nguồn, trong namespace `bronze`.")
    p.add_argument("--table", default="stg_walmart_order_lines",
                   help="bảng Silver đích, trong namespace `silver`.")
    p.add_argument("--shuffle-partitions", type=int, default=None,
                   help="spark.sql.shuffle.partitions. Để trống thì giữ mặc định 200.")
    p.add_argument("--run-id", default=None,
                   help="mã lần chạy ghi vào cột run_id. Để trống thì sinh uuid4.")
    return p.parse_args(argv)


def _ts_millis(col):
    """Đổi epoch **mili-giây** thành `timestamp`.

    Dùng `timestamp_millis` chứ không chia 1000 rồi cast: phép chia đi qua `double` nên mất
    chính xác ở phần mili-giây. Đích Return Label có cửa sổ 30 ngày tính từ mốc giao, nên
    sai một chút ở ranh giới ngày là sai NHÃN — không phải sai hiển thị.

    Múi giờ đã ghim UTC trong `iceberg_session.build_spark` (docs/silver.md mục 4.2).
    """
    return F.timestamp_millis(col)


def _country(col):
    """Quy mã quốc gia về ISO alpha-2 (`"USA"` -> `"US"`), mã lạ giữ nguyên văn.

    `country` là cột CHUẨN duy nhất, không có cột gốc đi kèm như trạng thái, nên giá trị lạ
    phải giữ lại chứ không thành null (docs/silver.md mục 4.4).
    """
    pairs = [F.lit(x) for kv in COUNTRY_ALIASES.items() for x in kv]
    return F.coalesce(F.create_map(*pairs)[col], col)


def _status(col):
    """Tra trạng thái gốc Walmart -> trạng thái chuẩn; chữ lạ thành NULL để đếm được."""
    pairs = [F.lit(x) for kv in WALMART_STATUS.items() for x in kv]
    return F.create_map(*pairs)[col]


def parse_order_lines(bronze: DataFrame) -> DataFrame:
    """Parse `raw_payload` và nổ mảng `orderLines.orderLine` thành từng Order Line.

    Hai chỗ lấy phần tử `[0]` của mảng, đều có lý do từ chính generator:
      * `charges.charge[0]` — nguồn luôn sinh ĐÚNG MỘT charge, `chargeType = "PRODUCT"`
        (`generator/channels/walmart.py:99-105`). Nếu sau này nguồn thêm charge thứ hai
        (phí vận chuyển, thuế) thì chỗ này phải đổi thành phép lọc theo `chargeType`, không
        phải lấy phần tử đầu.
      * `orderLineStatuses.orderLineStatus[0]` — nguồn cũng luôn sinh đúng một
        (`walmart.py:85-96`), vì mọi dòng của một đơn cùng trạng thái (đơn giản hoá đã khai).

    `chargeAmount` của Walmart là tiền **sau** giảm giá (`walmart.py:103`
    `(unit_price - unit_discount) * quantity`), nên nó vào `line_net_amount`. Bảng Walmart
    KHÔNG có `line_gross_amount` / `line_discount_amount` — nguồn không cho, và
    `docs/silver.md` mục 4.1 đã chốt không tạo cột rỗng cho kênh không có.
    """
    doc = F.from_json(F.col("raw_payload"), PAYLOAD_SCHEMA).alias("d")
    df = bronze.select("raw_payload", "source_table", "fetched_at", doc)
    line = F.col("line")
    charge = line["charges"]["charge"][0]
    line_status = line["orderLineStatuses"]["orderLineStatus"][0]
    return (
        df.select(
            "source_table", "fetched_at", "d",
            F.explode("d.orderLines.orderLine").alias("line"),
        )
        .select(
            F.col("d.purchaseOrderId").alias("purchase_order_id"),
            F.col("d.customerOrderId").alias("customer_order_id"),
            line["lineNumber"].cast("int").alias("line_number"),
            _ts_millis(F.col("d.orderDate")).alias("created_at"),
            _status(line_status["status"]).alias("order_status"),
            line_status["status"].alias("channel_status"),
            F.col("d.buyerId").alias("buyer_id"),
            F.col("d.isGuest").alias("is_guest"),
            _ts_millis(F.col("d.shippingInfo.estimatedDeliveryDate"))
            .alias("estimated_delivery_date"),
            F.col("d.shippingInfo.postalAddress.city").alias("city"),
            F.col("d.shippingInfo.postalAddress.postalCode").alias("postal_code"),
            _country(F.col("d.shippingInfo.postalAddress.country")).alias("country"),
            line["item"]["sku"].alias("sku"),
            line["item"]["productName"].alias("product_name"),
            line["orderLineQuantity"]["amount"].cast("int").alias("quantity"),
            charge["chargeAmount"]["amount"].cast(MONEY).alias("line_net_amount"),
            charge["chargeAmount"]["currency"].alias("currency"),
            _ts_millis(line["statusDate"]).alias("status_changed_at"),
            _ts_millis(line_status["trackingInfo"]["shipDateTime"]).alias("ship_date_time"),
            line_status["trackingInfo"]["carrierName"]["carrier"].alias("carrier"),
            line_status["trackingInfo"]["trackingNumber"].alias("tracking_number"),
            F.col("source_table"),
            F.col("fetched_at"),
        )
    )


def dedup(df: DataFrame) -> DataFrame:
    """Bỏ bản trùng theo `(purchase_order_id, customer_order_id, line_number)`.

    Tie-break, chốt ở `docs/silver.md` mục 3.1:
      1. `status_changed_at` mới nhất — cùng hướng với Amazon: "luôn lấy cái mới nhất";
      2. hoà thì `fetched_at` lớn nhất.

    Tie HOÀ là chuyện xảy ra thật, không phải giả định: bản `identical_copy` của generator
    có cùng trạng thái nên cùng `statusDate`. Khi đó hai dòng giống hệt nhau về nghiệp vụ,
    nên chọn bản nào cũng không làm sai tiền hay trạng thái — chỉ cột provenance khác.

    `line_number` PHẢI ở trong khoá: thiếu nó thì một đơn 3 món chỉ còn 1 dòng, tức xoá
    Order Line thật. Lỗi im lặng, job không báo gì.
    """
    newest = Window.partitionBy(
        "purchase_order_id", "customer_order_id", "line_number"
    ).orderBy(
        F.col("status_changed_at").desc_nulls_last(),
        F.col("fetched_at").desc_nulls_last(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(newest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def write_silver(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Ghi (thay thế) bảng Silver, partition theo `days(created_at)`.

    Cùng cách chọn partition với job Amazon, và cùng là ĐỀ XUẤT của planner chưa chốt trong
    `docs/silver.md`: chia theo ngày ĐẶT HÀNG, không theo ngày lấy dữ liệu. Lý do: sau dedup
    thì `fetched_at` chỉ còn nghĩa "lần lấy của bản thắng", nên partition theo nó làm một
    đơn nhảy partition mỗi lần có bản mới.

    Hai bảng Silver dùng cùng một cột partition để Gold union chúng không phải đọc hai cách
    chia khác nhau.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SILVER_NS}")
    full_name = f"{CATALOG}.{SILVER_NS}.{table}"
    df.writeTo(full_name).partitionedBy(F.days("created_at")).createOrReplace()


def main(argv: list[str] | None = None) -> int:
    """Chạy job và in số đo: số Order Line trước/sau dedup và tỉ lệ trùng."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    source = f"{CATALOG}.{BRONZE_NS}.{args.source_table}"
    spark = build_spark(f"silver-walmart-{args.table}", args.shuffle_partitions)
    try:
        bronze = spark.table(source)

        print(f"run_id          = {run_id}")
        print(f"bảng nguồn      = {source}")
        print(f"bảng đích       = {CATALOG}.{SILVER_NS}.{args.table}")

        started = time.monotonic()
        lines = (
            parse_order_lines(bronze)
            .withColumn("processed_at", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id))
            .select(*OUTPUT_COLUMNS)
        )
        before = lines.count()
        write_silver(spark, dedup(lines), args.table)
        elapsed = time.monotonic() - started

        after = spark.table(f"{CATALOG}.{SILVER_NS}.{args.table}").count()
        print(f"Order Line trước dedup = {before:,}")
        print(f"Order Line sau  dedup  = {after:,}")
        print(f"bản trùng đã bỏ        = {before - after:,}")
        if before:
            print(f"tỉ lệ trùng            = {(before - after) / before:.6f}")
        print(f"XONG sau               = {elapsed:.1f}s")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
