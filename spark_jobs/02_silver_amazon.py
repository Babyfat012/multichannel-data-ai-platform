"""Job Silver 02 — `bronze.raw_amazon_orders` -> `silver.stg_amazon_order_lines`.

Đây là job đầu tiên của tầng Silver. Nó làm đúng hai việc mà Bronze cố ý KHÔNG làm, theo
thiết kế đã chốt ở `docs/silver.md`:

  1. **Gộp hai dạng schema của Amazon** (rubric sheet `de`: handle schema evolution).
     Tháng 1 Amazon giao dạng v0 tên khoá PascalCase (`AmazonOrderId`, `OrderStatus`);
     từ 2026-01-01 đổi sang camelCase và lồng sâu hơn (`orderId`, `fulfillment.status`).
     Bronze giữ nguyên chuỗi JSON nên cả hai dạng nằm chung một bảng, một cột `payload`.
     Job này parse hai dạng bằng HAI schema riêng rồi `unionByName` về một bộ cột.

  2. **Bỏ bản trùng** (rubric sheet `de`: another offline problem — duplicate 2%).
     Khoá dedup `(order_id, order_item_id)`, giữ bản `last_updated_time` lớn nhất, hoà thì
     giữ `export_date` lớn nhất (`docs/silver.md` mục 3.1).

Thứ tự hai việc đó KHÔNG đổi được: **parse TRƯỚC, dedup SAU** (`docs/silver.md` mục 3.2).
Đã đo 23.487 khoá có một dòng dạng v0 và một dòng dạng 2026 — chưa quy tên khoá về một dạng
thì dedup không nhận ra đó là cùng một đơn.

Và dedup KHÔNG làm được trong từng partition: bản trùng luôn nằm ở `export_date` khác
(`overlap_days = 3`), nên dedup trong một partition bắt được 0 bản trùng. Vì vậy job dùng
window toàn cục, chấp nhận một lần shuffle thật.

Độ hạt bảng ra = **Order Line**, nên job nổ mảng `OrderItems` / `orderItems`. Lý do: nhãn
Return Label chấm ở mức Order Line.

Giới hạn của chính nguồn, ghi ở đây để không ai đi tìm:
  * Amazon KHÔNG công bố "đã giao" — v0 ghi `"Shipped"` cho cả shipped/delivered/completed
    (`generator/channels/amazon.py` bảng `STATUS_V0`). Nên `order_status` của bảng này
    không bao giờ ra `delivered` hay `completed`, và bảng không có cột thời điểm giao.
  * Dạng 2026 bỏ 5 khoá so với v0. Giữ 3, bỏ 2, phần thiếu để NULL chứ KHÔNG tính bù
    (`docs/silver.md` mục 5).

Chạy:
    spark-submit ... 02_silver_amazon.py
    spark-submit ... 02_silver_amazon.py --source-table raw_amazon_orders_sample \
        --table stg_amazon_order_lines_sample     # chạy thử trên 20 ngày
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
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# spark-submit đưa thư mục của file script vào sys.path, nên import phẳng là đủ.
from iceberg_session import CATALOG, build_spark
from silver_mapping import (
    AMAZON_2026_STATUS,
    AMAZON_FULFILLMENT,
    AMAZON_V0_STATUS,
    COUNTRY_ALIASES,
)

BRONZE_NS = "bronze"
SILVER_NS = "silver"

# --- Hai schema, một cho mỗi dạng ---------------------------------------------------
# Khai schema tường minh thay vì để Spark tự suy (`schema_of_json`): tự suy phải quét dữ
# liệu một lượt chỉ để biết hình dạng, và hình dạng suy ra đổi theo lô dữ liệu đưa vào —
# nghĩa là cùng job, hai lần chạy, hai schema. Khai tay thì bảng ra luôn cố định.
#
# Khoá nào không khai thì `from_json` bỏ qua; khoá nào khai mà payload không có thì ra NULL.
# Tiền khai `StringType` vì generator ghi tiền thành CHUỖI (`generator/channels/base.py:84`
# `money()` trả `f"{amount:.2f}"`), rồi job ép sang `decimal(14,2)` ở bước sau.

_V0_ITEM = StructType([
    StructField("OrderItemId", StringType()),
    StructField("ASIN", StringType()),
    StructField("SellerSKU", StringType()),
    StructField("QuantityOrdered", IntegerType()),
    StructField("QuantityShipped", IntegerType()),
    StructField("ItemPrice", StructType([
        StructField("CurrencyCode", StringType()),
        StructField("Amount", StringType()),
    ])),
    StructField("PromotionDiscount", StructType([
        StructField("CurrencyCode", StringType()),
        StructField("Amount", StringType()),
    ])),
])

V0_SCHEMA = StructType([
    StructField("AmazonOrderId", StringType()),
    StructField("PurchaseDate", StringType()),
    StructField("LastUpdateDate", StringType()),
    StructField("OrderStatus", StringType()),
    StructField("FulfillmentChannel", StringType()),
    StructField("SalesChannel", StringType()),
    StructField("MarketplaceId", StringType()),
    StructField("OrderTotal", StructType([
        StructField("CurrencyCode", StringType()),
        StructField("Amount", StringType()),
    ])),
    StructField("NumberOfItemsShipped", IntegerType()),
    StructField("NumberOfItemsUnshipped", IntegerType()),
    StructField("IsPrime", BooleanType()),
    StructField("EarliestDeliveryDate", StringType()),
    StructField("LatestDeliveryDate", StringType()),
    StructField("ShippingAddress", StructType([
        StructField("City", StringType()),
        StructField("PostalCode", StringType()),
        StructField("CountryCode", StringType()),
    ])),
    StructField("OrderItems", ArrayType(_V0_ITEM)),
])

_ITEM_2026 = StructType([
    StructField("orderItemId", StringType()),
    StructField("product", StructType([
        StructField("asin", StringType()),
        StructField("sellerSku", StringType()),
    ])),
    StructField("quantityOrdered", IntegerType()),
    StructField("price", StructType([
        StructField("currencyCode", StringType()),
        StructField("amount", StringType()),
    ])),
    StructField("promotion", StructType([
        StructField("currencyCode", StringType()),
        StructField("amount", StringType()),
    ])),
])

SCHEMA_2026 = StructType([
    StructField("orderId", StringType()),
    StructField("createdTime", StringType()),
    StructField("lastUpdatedTime", StringType()),
    StructField("programs", ArrayType(StringType())),
    StructField("fulfillment", StructType([
        StructField("status", StringType()),
        StructField("fulfilledBy", StringType()),
        StructField("deliverByWindow", StructType([
            StructField("startTime", StringType()),
            StructField("endTime", StringType()),
        ])),
    ])),
    StructField("salesChannel", StructType([
        StructField("marketplaceId", StringType()),
        StructField("channelName", StringType()),
    ])),
    StructField("proceeds", StructType([
        StructField("grandTotal", StructType([
            StructField("currencyCode", StringType()),
            StructField("amount", StringType()),
        ])),
    ])),
    StructField("shippingAddress", StructType([
        StructField("city", StringType()),
        StructField("postalCode", StringType()),
        StructField("countryCode", StringType()),
    ])),
    StructField("orderItems", ArrayType(_ITEM_2026)),
])

#: Thứ tự cột của bảng ra. Hai nhánh parse phải cùng đúng bộ này, nên để một chỗ.
OUTPUT_COLUMNS = [
    # khoá + độ hạt
    "order_id", "order_item_id",
    # mức Order
    "created_at", "last_updated_time", "order_status", "channel_status",
    "fulfillment_method", "sales_channel", "marketplace_id", "is_prime",
    "order_total_amount", "currency",
    "number_of_items_shipped", "number_of_items_unshipped",
    "earliest_delivery_date", "latest_delivery_date",
    "city", "postal_code", "country",
    # mức Order Line
    "asin", "seller_sku", "quantity_ordered", "quantity_shipped",
    "line_gross_amount", "line_discount_amount", "line_net_amount",
    # provenance mang từ Bronze (docs/silver.md mục 7)
    "export_date", "source_file",
    # cột của riêng Silver
    "schema_form", "processed_at", "run_id",
]

MONEY = "decimal(14,2)"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Đọc tham số dòng lệnh. Tách khỏi `main` để test được mà không cần SparkSession."""
    p = argparse.ArgumentParser(description="Silver: Amazon Order Line")
    p.add_argument("--source-table", default="raw_amazon_orders",
                   help="bảng Bronze nguồn, trong namespace `bronze`.")
    p.add_argument("--table", default="stg_amazon_order_lines",
                   help="bảng Silver đích, trong namespace `silver`.")
    p.add_argument("--shuffle-partitions", type=int, default=None,
                   help="spark.sql.shuffle.partitions. Để trống thì giữ mặc định 200.")
    p.add_argument("--run-id", default=None,
                   help="mã lần chạy ghi vào cột run_id. Để trống thì sinh uuid4.")
    return p.parse_args(argv)


def _lookup(table: dict[str, str], col):
    """Tra một bảng ánh xạ chữ -> chữ bằng map literal của Spark.

    Dùng `create_map` thay vì chuỗi `when(...)` dài: bảng ánh xạ đã nằm ở
    `silver_mapping.py` (có test), nên ở đây chỉ cần đổ nó vào Spark, không viết lại logic.
    Chữ không có trong bảng thì map trả NULL — đúng ý: một chữ lạ phải hiện ra thành null
    để đếm được, chứ không làm chết job.
    """
    pairs = [F.lit(x) for kv in table.items() for x in kv]
    return F.create_map(*pairs)[col]


def _ts(col):
    """Đổi chuỗi ISO-8601 có hậu tố `Z` thành `timestamp`.

    Múi giờ đã ghim UTC một chỗ trong `iceberg_session.build_spark`, nên ở đây không đặt
    lại — xem `docs/silver.md` mục 4.2 về vì sao việc ghim là bắt buộc, không phải tuỳ chọn.
    """
    return F.to_timestamp(col)


def parse_v0(bronze: DataFrame) -> DataFrame:
    """Parse nhánh dạng v0 (PascalCase) và nổ mảng `OrderItems` thành từng Order Line."""
    doc = F.from_json(F.col("payload"), V0_SCHEMA).alias("d")
    df = bronze.select("payload", "source_file", "export_date", doc)
    item = F.col("item")
    return (
        df.select("source_file", "export_date", "d", F.explode("d.OrderItems").alias("item"))
        .select(
            F.col("d.AmazonOrderId").alias("order_id"),
            item["OrderItemId"].alias("order_item_id"),
            _ts(F.col("d.PurchaseDate")).alias("created_at"),
            _ts(F.col("d.LastUpdateDate")).alias("last_updated_time"),
            _lookup(AMAZON_V0_STATUS, F.col("d.OrderStatus")).alias("order_status"),
            F.col("d.OrderStatus").alias("channel_status"),
            _lookup(AMAZON_FULFILLMENT, F.col("d.FulfillmentChannel")).alias("fulfillment_method"),
            F.col("d.SalesChannel").alias("sales_channel"),
            F.col("d.MarketplaceId").alias("marketplace_id"),
            F.col("d.IsPrime").alias("is_prime"),
            F.col("d.OrderTotal.Amount").cast(MONEY).alias("order_total_amount"),
            F.col("d.OrderTotal.CurrencyCode").alias("currency"),
            F.col("d.NumberOfItemsShipped").alias("number_of_items_shipped"),
            F.col("d.NumberOfItemsUnshipped").alias("number_of_items_unshipped"),
            _ts(F.col("d.EarliestDeliveryDate")).alias("earliest_delivery_date"),
            _ts(F.col("d.LatestDeliveryDate")).alias("latest_delivery_date"),
            F.col("d.ShippingAddress.City").alias("city"),
            F.col("d.ShippingAddress.PostalCode").alias("postal_code"),
            _country(F.col("d.ShippingAddress.CountryCode")).alias("country"),
            item["ASIN"].alias("asin"),
            item["SellerSKU"].alias("seller_sku"),
            item["QuantityOrdered"].alias("quantity_ordered"),
            item["QuantityShipped"].alias("quantity_shipped"),
            item["ItemPrice"]["Amount"].cast(MONEY).alias("line_gross_amount"),
            item["PromotionDiscount"]["Amount"].cast(MONEY).alias("line_discount_amount"),
            (item["ItemPrice"]["Amount"].cast(MONEY)
             - item["PromotionDiscount"]["Amount"].cast(MONEY))
            .cast(MONEY).alias("line_net_amount"),
            F.col("export_date"),
            F.col("source_file"),
            F.lit("v0").alias("schema_form"),
        )
    )


def parse_2026(bronze: DataFrame) -> DataFrame:
    """Parse nhánh dạng 2026 (camelCase, lồng sâu) về ĐÚNG bộ cột của nhánh v0.

    Ba cột `number_of_items_shipped`, `number_of_items_unshipped`, `quantity_shipped` để
    NULL tường minh: payload dạng 2026 không có khoá tương ứng, và `docs/silver.md` mục 5
    đã chốt KHÔNG tính bù từ trạng thái + số lượng. Lý do: số suy ra chỉ đúng tại ngày
    export đó, và nó suy TỪ trạng thái nên mô hình học trên đó là học lại chính trạng thái.
    """
    doc = F.from_json(F.col("payload"), SCHEMA_2026).alias("d")
    df = bronze.select("payload", "source_file", "export_date", doc)
    item = F.col("item")
    return (
        df.select("source_file", "export_date", "d", F.explode("d.orderItems").alias("item"))
        .select(
            F.col("d.orderId").alias("order_id"),
            item["orderItemId"].alias("order_item_id"),
            _ts(F.col("d.createdTime")).alias("created_at"),
            _ts(F.col("d.lastUpdatedTime")).alias("last_updated_time"),
            _lookup(AMAZON_2026_STATUS, F.col("d.fulfillment.status")).alias("order_status"),
            F.col("d.fulfillment.status").alias("channel_status"),
            _lookup(AMAZON_FULFILLMENT, F.col("d.fulfillment.fulfilledBy")).alias("fulfillment_method"),
            F.col("d.salesChannel.channelName").alias("sales_channel"),
            F.col("d.salesChannel.marketplaceId").alias("marketplace_id"),
            # `programs` là mảng chỉ có hai khả năng `["PRIME"]` hoặc `[]`
            # (`generator/channels/amazon.py:189`) — đúng bằng một bool, không hơn một bit
            # thông tin. Nên quy về `boolean` ngay ở Silver (docs/silver.md mục 4.3).
            F.array_contains(F.col("d.programs"), "PRIME").alias("is_prime"),
            F.col("d.proceeds.grandTotal.amount").cast(MONEY).alias("order_total_amount"),
            F.col("d.proceeds.grandTotal.currencyCode").alias("currency"),
            F.lit(None).cast("int").alias("number_of_items_shipped"),
            F.lit(None).cast("int").alias("number_of_items_unshipped"),
            _ts(F.col("d.fulfillment.deliverByWindow.startTime")).alias("earliest_delivery_date"),
            _ts(F.col("d.fulfillment.deliverByWindow.endTime")).alias("latest_delivery_date"),
            F.col("d.shippingAddress.city").alias("city"),
            F.col("d.shippingAddress.postalCode").alias("postal_code"),
            _country(F.col("d.shippingAddress.countryCode")).alias("country"),
            item["product"]["asin"].alias("asin"),
            item["product"]["sellerSku"].alias("seller_sku"),
            item["quantityOrdered"].alias("quantity_ordered"),
            F.lit(None).cast("int").alias("quantity_shipped"),
            item["price"]["amount"].cast(MONEY).alias("line_gross_amount"),
            item["promotion"]["amount"].cast(MONEY).alias("line_discount_amount"),
            (item["price"]["amount"].cast(MONEY)
             - item["promotion"]["amount"].cast(MONEY))
            .cast(MONEY).alias("line_net_amount"),
            F.col("export_date"),
            F.col("source_file"),
            F.lit("2026").alias("schema_form"),
        )
    )


def _country(col):
    """Quy mã quốc gia về ISO alpha-2; mã không có trong bảng alias thì giữ nguyên văn.

    Khác `_lookup`: ở đây giá trị lạ phải GIỮ LẠI chứ không thành null, vì `country` là cột
    chuẩn duy nhất (không có cột gốc đi kèm như trạng thái) — xem `silver_mapping.
    normalize_country`.
    """
    pairs = [F.lit(x) for kv in COUNTRY_ALIASES.items() for x in kv]
    return F.coalesce(F.create_map(*pairs)[col], col)


def split_by_form(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Chia dòng Bronze theo dạng schema, dựa trên khoá định danh ở gốc JSON.

    Dùng `get_json_object` chỉ để lấy MỘT khoá làm cờ phân biệt, rẻ hơn parse cả hai schema
    cho mọi dòng rồi mới lọc. `AmazonOrderId` chỉ có ở v0, `orderId` chỉ có ở dạng 2026.
    """
    is_v0 = F.get_json_object(F.col("payload"), "$.AmazonOrderId").isNotNull()
    is_2026 = F.get_json_object(F.col("payload"), "$.orderId").isNotNull()
    return bronze.filter(is_v0), bronze.filter(is_2026)


def dedup(df: DataFrame) -> DataFrame:
    """Bỏ bản trùng theo khoá `(order_id, order_item_id)`, giữ bản mới nhất.

    Thứ tự tie-break, chốt ở `docs/silver.md` mục 3.1:
      1. `last_updated_time` lớn nhất — "luôn lấy cái mới nhất";
      2. hoà thì `export_date` lớn nhất.

    Vì sao `order_id` MỘT MÌNH là sai: ở mức Order Line, một `order_id` nằm trên nhiều dòng
    THẬT (một đơn nhiều món), nên dedup chỉ theo `order_id` sẽ xoá Order Line thật. Đó là
    lỗi im lặng — job không báo gì và số dòng vẫn trông hợp lý.

    Window này là chỗ shuffle thật duy nhất của job. Không thu hẹp về từng partition được:
    bản trùng luôn nằm ở `export_date` khác (`overlap_days = 3`).
    """
    newest = Window.partitionBy("order_id", "order_item_id").orderBy(
        F.col("last_updated_time").desc_nulls_last(),
        F.col("export_date").desc_nulls_last(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(newest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def write_silver(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Ghi (thay thế) bảng Silver, partition theo `days(created_at)`.

    `createOrReplace` là một commit nguyên khối: hoặc bảng mới hiện ra đầy đủ, hoặc không
    có gì đổi. Nửa đường hỏng thì bảng cũ vẫn nguyên, chạy lại được.

    CHỌN PARTITION — khoản này là ĐỀ XUẤT của planner, `docs/silver.md` chưa chốt:
    partition theo ngày ĐẶT HÀNG (`days(created_at)`), không theo `export_date`.
      * `export_date` sau dedup chỉ còn nghĩa "ngày export của bản thắng", không còn là một
        lát dữ liệu đầy đủ — partition theo nó thì một đơn nhảy partition mỗi lần có bản mới.
      * `days(created_at)` là partition ẩn của Iceberg: cột vẫn là `timestamp`, Iceberg tự
        giữ lát theo ngày, nên query lọc theo khoảng ngày đặt hàng chỉ đọc đúng lát cần.
      * Giá: dedup ghi lại partition của mọi ngày có đơn được cập nhật, không chỉ ngày mới.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SILVER_NS}")
    full_name = f"{CATALOG}.{SILVER_NS}.{table}"
    df.writeTo(full_name).partitionedBy(F.days("created_at")).createOrReplace()


def main(argv: list[str] | None = None) -> int:
    """Chạy job và in số đo để đối chiếu: số dòng vào, số Order Line, số bị dedup bỏ."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    source = f"{CATALOG}.{BRONZE_NS}.{args.source_table}"
    spark = build_spark(f"silver-amazon-{args.table}", args.shuffle_partitions)
    try:
        bronze = spark.table(source)
        v0_rows, rows_2026 = split_by_form(bronze)

        print(f"run_id          = {run_id}")
        print(f"bảng nguồn      = {source}")
        print(f"bảng đích       = {CATALOG}.{SILVER_NS}.{args.table}")

        started = time.monotonic()
        lines = parse_v0(v0_rows).unionByName(parse_2026(rows_2026))
        lines = (
            lines.withColumn("processed_at", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id))
            .select(*OUTPUT_COLUMNS)
        )
        before = lines.count()
        deduped = dedup(lines)
        write_silver(spark, deduped, args.table)
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
