# Báo cáo chất lượng dữ liệu của Data Generator (Batch)

Báo cáo này đối chiếu từng dòng rubric "Implement Data Generator, Simulate offline data problems" với số đo thật. Phần Stream nằm ở [data-quality-report-stream.md](data-quality-report-stream.md). Bảng đầy đủ của lệnh `report` nằm ở [generator-design.md, mục Kết quả chạy](generator-design.md#kết-quả-chạy).

## Lần chạy

- Cấu hình: `configs/default.yaml` (seed 42, 2.000 SKU, 180 ngày từ 2026-01-01, 4.000 Order mỗi ngày).
- Nơi lưu: Amazon ở MinIO (bucket `fsds`), Walmart và `seller.skus` ở PostgreSQL (database `sources`). Cách dựng xem [Docker.md](Docker.md).
- Lệnh đo:

```bash
set -a; . ./.env; set +a
export AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD \
       FSSPEC_S3_ENDPOINT_URL=http://localhost:$MINIO_API_PORT
uv run python -m generator report --source s3://fsds \
    --dsn "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_PORT/$POSTGRES_DB"
```

Cột `_truth` là đáp án generator ghi trong `run_manifest.json`. Số đo đọc lại từ dữ liệu đã lưu, không lấy từ bộ nhớ của generator.

## Tóm tắt theo rubric

| Dòng rubric | Kết quả | Số đo chính |
|---|---|---|
| Simulate skew | Đạt | Thành phố lớn nhất chiếm 46 đến 49% Order mỗi Storefront. Apparel 17,7% Order Line, beauty 7,6%. SKU bán chạy nhất 5,4%. Hai ngày Prime Day gấp 3,7 lần trung vị |
| Simulate high cardinality | Đạt | `amazon.orders`: 498.156 mã Order, 721.099 mã Order Line |
| Simulate schema evolution | Đạt | `proceeds` null 100% trước export 2026-04-10 và 0% từ ngày đó. `IsPrime` ngược lại |
| Simulate another offline problem (duplicate 2%) | Đạt | Trùng 1,98% (Amazon) và 1,99% (Walmart), khớp đáp án. Sau dedup còn 0% |
| Using generator configuration | Đạt | Mọi tham số nằm trong `configs/*.yaml`, xem [mục 15](generator-design.md#các-khóa-cấu-hình-đang-dùng) |
| Store data (MinIO, PostgreSQL) | Đạt | 388 MB JSON Lines trong 180 file ở MinIO. Bảng `walmart.orders` 74 MB ở PostgreSQL |

## Chi tiết từng vấn đề

### Skew

```text
== storefront_skew ==
storefront_id   config_weight  orders_truth  pct_truth  orders_exported
--------------  -------------  ------------  ---------  ---------------
amazon_us       0.45           334046        45.38      334046
tiktok_shop_us  0.15           108042        14.68
amazon_uk       0.12           89332         12.14      89332
amazon_de       0.1            74778         10.16      74778
shopify         0.08           57673         7.83
walmart_us      0.07           50499         6.86       50499
shopee_vn       0.03           21759         2.96

== city_skew == (thành phố lớn nhất mỗi Storefront)
storefront_id  city      orders  pct
-------------  --------  ------  -----
amazon_de      Berlin    36723   49.11
amazon_uk      London    43798   49.03
amazon_us      New York  154019  46.11
walmart_us     Dallas    24843   49.2

== category_skew ==
category     order_lines  pct    return_pct_truth
-----------  -----------  -----  ----------------
apparel      140711       17.71  23.32
toys         115003       14.48  6.69
electronics  112797       14.2   8.1
accessories  105104       13.23  10.16
sports       90499        11.39  8.3
home         85100        10.71  4.57
footwear     84430        10.63  16.08
beauty       60717        7.64   5.83

== top_skus == (3 SKU đầu)
sku        order_lines  pct
---------  -----------  ----
ELE-01103  43073        5.42
TOY-01453  24485        3.08
SPT-01304  18065        2.27

== daily_orders == (Sale Event: 2026-04-30, 2026-05-01)
day         orders  x_median
----------  ------  --------
2026-05-01  10950   3.7
2026-04-30  10887   3.68
2026-03-13  3119    1.05
```

- **Storefront**: số Order đo được khớp đáp án ở cả 4 Storefront đã có renderer (ví dụ `amazon_us` 334.046). TikTok Shop, Shopify và Shopee chưa có renderer nên để trống.
- **Thành phố**: Berlin 49,1%, London 49,0%, New York 46,1%, Dallas 49,2%. Job group theo thành phố sẽ có một partition lớn gấp đôi các partition còn lại cộng lại.
- **Danh mục**: apparel 17,7% và tỷ lệ trả 23,3%, home 10,7% và tỷ lệ trả 4,6%. Danh mục chỉ có trong `seller.skus`, nên phải join với PostgreSQL mới đo được.
- **SKU**: `ELE-01103` chiếm 5,4% Order Line (43.073 dòng), gấp gần 2 lần SKU thứ hai.
- **Ngày**: 2026-04-30 và 2026-05-01 (Prime Day) có khoảng 10.900 Order, gấp 3,7 lần trung vị.

### High cardinality

```text
== cardinality ==
dataset         column           approx_distinct  exact_distinct  approx_err_pct  rows
--------------  ---------------  ---------------  --------------  --------------  ------
amazon.orders   order_id         385608           498156          -22.59          508227
amazon.orders   line_id          818537           721099          13.51           721099
amazon.orders   asin             1844             2000            -7.8            721099
amazon.orders   sku              1526             2000            -23.7           721099
walmart.orders  order_id         50400            50499           -0.2            51523
walmart.orders  buyerId          21549            21719           -0.78           51523
walmart.orders  line_id          68348            73262           -6.71           73262
walmart.orders  tracking_number  52956            48629           8.9             73262
```

| Cột | Số đếm chính xác | HyperLogLog của DuckDB | Lệch |
|---|---|---|---|
| `amazon.orders.order_id` | 498.156 | 385.608 | -22,6% |
| `amazon.orders.line_id` | 721.099 | 818.537 | +13,5% |
| `walmart.orders.order_id` | 50.499 | 50.400 | -0,2% |

`approx_count_distinct` của DuckDB lệch tới hơn 20% ở cột nhiều giá trị, nên báo cáo in thêm số đếm chính xác. Job Spark ở bước sau sẽ so với số chính xác này.

### Schema evolution

```text
== schema_evolution_daily ==
export_date  rows  pct_null_proceeds  pct_null_IsPrime
-----------  ----  -----------------  ----------------
2026-04-07   2786  100.0              0.0
2026-04-08   2632  100.0              0.0
2026-04-09   2819  100.0              0.0
2026-04-10   2688  0.0                100.0
2026-04-11   2676  0.0                100.0
2026-04-12   2707  0.0                100.0
```

| Giai đoạn | Số ngày export | Dòng | `proceeds` null | `IsPrime` null |
|---|---|---|---|---|
| Trước, Orders v0 | 99 | 247.213 | 100% | 0% |
| Sau, Orders 2026-01-01 | 81 | 261.014 | 0% | 100% |

Ranh giới rơi đúng ngày export 2026-04-10: ngày 2026-04-09 còn schema cũ, ngày 2026-04-10 đã là schema mới. `AmazonOrderId` cũng đổi tên, nên job Bronze phải gộp hai schema theo tên cột mới.

### Duplicate 2%

```text
== duplicates ==
dataset         rows_before  distinct_keys  dup_pct_before  identical_copies  older_versions  rows_after  dup_pct_after  dup_pct_truth
--------------  -----------  -------------  --------------  ----------------  --------------  ----------  -------------  -------------
amazon.orders   508227       498156         1.98            5108              4963            498156      0.0            1.98
walmart.orders  51523        50499          1.99            0                 1024            50499       0.0            1.99
```

| Dataset | Dòng | Khóa khác nhau | Trùng | Bản y hệt | Bản cũ hơn | Sau dedup | Đáp án |
|---|---|---|---|---|---|---|---|
| `amazon.orders` | 508.227 | 498.156 | 1,98% | 5.108 | 4.963 | 0% | 1,98% |
| `walmart.orders` | 51.523 | 50.499 | 1,99% | 0 | 1.024 | 0% | 1,99% |

Khoảng một nửa bản trùng của Amazon là bản cũ hơn, mang Order Status cũ. Vì vậy chỉ bỏ các dòng giống hệt nhau là không đủ, mà phải giữ bản cập nhật mới nhất theo khóa.

### Nơi lưu

![MinIO console](images/batch/minio-bucket.png)

![DBeaver](images/batch/dbeaver-walmart-orders.png)

![DBeaver seller.skus](images/batch/dbeaver-seller-skus.png)

| Dataset | Nơi lưu | Định dạng | File | Dung lượng |
|---|---|---|---|---|
| `amazon.orders` | MinIO | JSON Lines | 180 | 388 MB |
| `labels` | MinIO | Parquet | 1 | 13 MB |
| `walmart.orders` | PostgreSQL | bảng, cột JSONB | | 74 MB |
| `seller.skus` | PostgreSQL | bảng | | 0,3 MB |
