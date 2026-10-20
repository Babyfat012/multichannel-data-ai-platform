# Data Storage — compaction Iceberg

Cập nhật 2026-10-14. Phần này ghi bằng chứng thực chạy cho `rewrite_data_files`
trên bảng Amazon Bronze toàn kỳ. Code đo và chạy: `spark_jobs/maintenance/inspect_iceberg_storage.py`
và `spark_jobs/maintenance/compact_amazon_order_snapshots.py`. Tài liệu API tương ứng là
[Apache Iceberg 1.11 Spark procedures](https://iceberg.apache.org/docs/1.11.0/spark-procedures/)
và [metadata table `.files`](https://iceberg.apache.org/docs/1.11.0/spark-queries/).

## Chọn bảng bằng metadata, không suy từ số file raw

`/data/raw` có 5.774 JSONL, nhưng sau khi Spark ghi Bronze/Silver, các bảng
DP1/DP2 đang có **một file trên mỗi partition**. Vì vậy chỉ đếm file nhỏ rồi
chạy compaction sẽ không giảm số file: Iceberg giữ nguyên ranh giới partition.

| Bảng | File | Partition | Partition có nhiều file |
|---|---:|---:|---:|
| `bronze.raw_amazon_orders_dp1` | 10 | 10 | 0 |
| `bronze.raw_walmart_orders_dp1` | 20 | 20 | 0 |
| `silver.stg_amazon_order_lines_dp2` | 28 | 28 | 0 |
| `silver.stg_walmart_order_lines_dp2` | 180 | 180 | 0 |
| `bronze.raw_amazon_orders` toàn kỳ, trước compaction | 187 | 180 | 7 |

Bảy ngày Amazon có hai file là 2026-02-23, 05-07, 05-16, 05-18, 06-11,
06-23 và 06-28. Truy vấn đo chỉ quét bảy ngày này:

```sql
SELECT count(*) AS rows, sum(length(payload)) AS payload_chars
FROM lakehouse.bronze.raw_amazon_orders
WHERE export_date IN (
  DATE '2026-02-23', DATE '2026-05-07', DATE '2026-05-16',
  DATE '2026-05-18', DATE '2026-06-11', DATE '2026-06-23',
  DATE '2026-06-28'
);
```

`sum(length(payload))` buộc Spark đọc cột dữ liệu, thay vì trả `count(*)`
từ metadata. Mỗi phía có một lượt làm nóng rồi ba lượt đo cùng câu SQL.

## Kết quả thực chạy

Lệnh `CALL lakehouse.system.rewrite_data_files` dùng bin-pack,
`target-file-size-bytes=134217728`, `min-input-files=2`, và chỉ một nhóm
rewrite đồng thời để vừa RAM laptop. Lệnh báo **12 file được viết lại thành 6
file**, 311.825.996 byte đầu vào, 0 file thất bại, mất 16,088 giây.

| Chỉ số | Trước | Sau |
|---|---:|---:|
| File hiện hành toàn bảng | 187 | 181 |
| Dòng toàn bảng | 108.248.563 | 108.248.563 |
| Byte file hiện hành toàn bảng | 8.525.005.070 | 8.524.986.535 |
| Dòng của truy vấn bảy ngày | 5.757.639 | 5.757.639 |
| Tổng ký tự payload của truy vấn | 4.381.958.279 | 4.381.958.279 |
| Ba lượt quét, giây | 10,511 / 10,348 / 9,451 | 9,641 / 9,405 / 9,066 |
| Trung vị, giây | **10,348** | **9,405** |

Trung vị giảm 0,943 giây, khoảng **9,1%** trên máy này. Số dòng và tổng ký tự
payload trùng khớp trước/sau. Lượt đo sau phải chạy trong Spark session mới vì
phiên đầu bị ngắt sau khi Iceberg đã commit compaction; do đó số giây là quan
sát thực nghiệm, không tách riêng được tác động của cache, tải máy và startup.
Một partition (`2026-06-28`) vẫn có hai file, nên hiện có 181 file thay vì 180.

## PostgreSQL warehouse và index

`spark_jobs/warehouse/export_order_snapshot_lines.py` đã sao chép lát Walmart của
`lakehouse.gold.fact_order_lines_dp2` sang
`sources.warehouse.fact_order_lines_walmart_dp2`. Bảng có **1.875.607** dòng,
khoảng `created_at` từ 2026-01-01 đến 2026-06-29; trước index chiếm **253 MB**.
Chỉ giữ sáu cột cần cho truy vấn để vừa đĩa máy dev. Đây là bản sao của Gold,
không phải một fact đầy đủ năm Channel trong PostgreSQL.

Cùng truy vấn, SKU `FTW-00664` có 211 dòng và tổng tiền net 12.038,74 theo
đồng tiền nguồn:

```sql
SELECT count(*), sum(line_net_amount)
FROM warehouse.fact_order_lines_walmart_dp2
WHERE seller_sku = 'FTW-00664';
```

DDL tạo index dùng cho phép đo dưới đây:

```sql
CREATE INDEX idx_fact_order_lines_walmart_sku
  ON warehouse.fact_order_lines_walmart_dp2 (seller_sku);
```

| `EXPLAIN (ANALYZE, BUFFERS)` | Trước index | Sau index |
|---|---|---|
| Kế hoạch | `Parallel Seq Scan` | `Bitmap Index Scan` → `Bitmap Heap Scan` |
| Dòng lọc / đọc | 211 / 1.875.607 | 211 / 211 khóa index |
| Buffer | hit 15.704, read 16.635 | read 213 |
| `Execution Time` | **51,611 ms** | **1,134 ms** |

Index `idx_fact_order_lines_walmart_sku` trên `(seller_sku)` chiếm **13 MB**;
bảng cùng index là **266 MB**. Đây là hai lần `EXPLAIN ANALYZE` trên cùng máy,
không phải benchmark nhiều lượt; cache có thể khác nhau giữa hai lần. Bằng chứng
mạnh nhất là kế hoạch đã đổi từ quét toàn bảng sang dùng index, và số buffer
đọc giảm rõ rệt.

## Phạm vi phép đo

Tài liệu này ghi compaction Iceberg và chỉ mục PostgreSQL. Phép đối chiếu cùng
câu hỏi giữa Iceberg và warehouse bằng Trino được ghi riêng tại
[trino.md](trino.md); z-order chưa được đo trong bộ bằng chứng hiện có.
