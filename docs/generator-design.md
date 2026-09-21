# Thiết kế Data Generator

Tài liệu thiết kế của generator dữ liệu giả lập cho dự án. Trạng thái: **thiết kế, chưa có code**. Thuật ngữ (Channel, Storefront, Order Line, ...) xem [Domain.md](Domain.md). Nguồn và mức tin cậy của dạng dữ liệu từng Channel xem [channel-sources.md](channel-sources.md).

## Mục lục

1. [Mục tiêu và phạm vi](#1-mục-tiêu-và-phạm-vi)
2. [Kiến trúc ba lớp](#2-kiến-trúc-ba-lớp)
3. [Đường thời gian: Batch và Stream](#3-đường-thời-gian-batch-và-stream)
4. [Sinh nhãn Return/Refund](#4-sinh-nhãn-returnrefund)
5. [Vấn đề dữ liệu được chèn vào](#5-vấn-đề-dữ-liệu-được-chèn-vào)
6. [Nguồn dữ liệu từng Channel](#6-nguồn-dữ-liệu-từng-channel)
7. [Dữ liệu tham chiếu](#7-dữ-liệu-tham-chiếu)
8. [Bố cục lưu trữ](#8-bố-cục-lưu-trữ)
9. [Buyer, tiền tệ và thời gian](#9-buyer-tiền-tệ-và-thời-gian)
10. [Cấu hình](#10-cấu-hình)
11. [Bản đáp án và bằng chứng chất lượng](#11-bản-đáp-án-và-bằng-chứng-chất-lượng)
12. [Kỹ thuật và đóng gói](#12-kỹ-thuật-và-đóng-gói)
13. [Giả định và việc còn mở](#13-giả-định-và-việc-còn-mở)
14. [Đối chiếu với rubric](#14-đối-chiếu-với-rubric)

## 1. Mục tiêu và phạm vi

Generator sinh dữ liệu bán hàng của một Seller (ACME) trên 7 Storefront thuộc 5 Channel, để các thành phần phía sau (Bronze, Silver, Gold, Feature Store, giám sát drift) có dữ liệu thật sự có vấn đề để xử lý.

Mặc định: 2.000 SKU, 180 ngày lịch sử, khoảng 4.000 Order mỗi ngày (khoảng 720 nghìn Order, hơn 1 triệu Order Line). Mọi con số là cấu hình, xem [mục 10](#10-cấu-hình).

Ngoài phạm vi giai đoạn này: phí, settlement, quảng cáo, tồn kho FBA, clickstream.

## 2. Kiến trúc ba lớp

```
Lõi mô phỏng ──► Renderer theo Channel ──► Lớp giao hàng lỗi ──► MinIO / PostgreSQL / Kafka
 (sự thật)         (dạng gốc từng sàn)       (trùng, trễ, lô)
```

1. **Lõi mô phỏng** sinh sự thật chung, không mang hình dạng của Channel nào: SKU, Listing, Order, Order Line, Return, Refund, buyer, `Delivered At`, theo Order Status chuẩn.
2. **Renderer theo Channel** biến sự thật thành dạng gốc của Channel: tên field, tên trạng thái, định dạng file, chỉ thể hiện những gì Channel thật sự cho Seller thấy.
3. **Lớp giao hàng lỗi** chèn lỗi sau cùng: duplicate, late, theo lô, burst.

Vì sao chia lớp:
- Có đáp án chung để đối chiếu kết quả Silver (số Order chuẩn, nhãn thật).
- Quy luật sinh nhãn nằm ở một chỗ, không lặp cho 5 Channel.
- Lỗi chèn sau cùng nên biết chính xác đã chèn bao nhiêu, ghi vào `run_manifest.json`.

Đổi lại phải viết thêm một lớp renderer so với cách sinh thẳng từng Channel.

## 3. Đường thời gian: Batch và Stream

`cutoff` là mốc "hôm nay" trong thế giới giả lập. Lõi mô phỏng chạy **liền một mạch** qua `cutoff`:

```
ngày 1 ───────────── ngày 180 (cutoff) ───────────── ngày 181 → 210
       BATCH                                STREAM
```

- **Batch**: mọi thứ xảy ra trước `cutoff`, mỗi Channel ở dạng gốc, kèm ảnh chụp trạng thái của các Order đang mở tại `cutoff`.
- **Stream**: phần mô phỏng tiếp tục sau `cutoff`, gồm Order mới, đổi Order Status của Order đã có trong Batch, và Return đến muộn.
- Một Order thấy ở Batch được cập nhật tiếp ở Stream vì cùng một mô phỏng.
- Nhãn của Order giao trong 30 ngày cuối trước `cutoff` chưa biết tại `cutoff`, nên bị loại khỏi tập train. Nhãn đến dần trong Stream.
- Ngày mô phỏng ánh xạ sang lịch thật qua `start_date` trong cấu hình.
- Tốc độ phát Stream cấu hình được theo từng đoạn ngày (xem [mục 5](#streaming)), có chế độ phát nhanh để test.

Vì sao không phát lại chính dữ liệu Batch: không có Order mới, không có nhãn đến muộn, không có drift sau khi mô hình đã train.

## 4. Sinh nhãn Return/Refund

Nhãn: Order Line có bị Return hoặc Refund trong 30 ngày kể từ ngày giao không.

Dữ liệu giả không có hành vi thật, nên lõi dùng một quy luật xác suất có cấu trúc để mô hình học được thay vì đoán mò:

- Xác suất trả là hàm logistic của: danh mục, Storefront, Fulfillment Method, mức giảm giá, giá, số lượng, và **SKU Defect Propensity**.
- **SKU Defect Propensity** là "độ lỗi" ẩn riêng từng SKU (ví dụ `HOODIE-BLK-M` bị chật nên trả nhiều ở mọi Storefront). Nó không xuất hiện thành cột nào, chỉ suy ra được từ lịch sử. Đây chính là thứ Feature Store tra theo `sku_id` + `storefront_id`.
- Độ trễ từ lúc giao tới lúc trả rút từ phân phối riêng theo Channel, tối đa khoảng 30 ngày, có đuôi hiếm vượt 30 ngày.
- **Không leakage**: không cột nào ở thời điểm tạo Order lộ nhãn.
- Tỷ lệ mục tiêu: mức cơ sở khoảng 10 đến 12%, quần áo khoảng 22%, đồ gia dụng khoảng 4%.
- Lý do trả: mỗi Channel dùng lý do gốc của nó (TikTok Shop `return_reason`, Shopee `reason`, Walmart lý do trong `returnOrderLines`). Riêng Amazon dùng khoảng 10 mã (`WRONG_SIZE`, `DEFECTIVE`, `NOT_AS_DESCRIBED`, `CHANGED_MIND`, `ARRIVED_LATE`, `DAMAGED_IN_TRANSIT`, `WRONG_ITEM_SENT`, `BETTER_PRICE_FOUND`, `QUALITY_NOT_EXPECTED`, `ACCIDENTAL_ORDER`). Danh sách Amazon là **giả định** vì chưa có danh sách chính thức.

Quy luật này là giả định của generator. Mô hình tốt trên dữ liệu giả không chứng minh tốt ngoài đời.

### Ngày giao (`Delivered At`)

Nhãn tính theo `Delivered At` **thật** của lõi. Mỗi Channel chỉ để lộ phần nó thật sự cho Seller thấy:

| Channel | Seller thấy gì |
|---|---|
| TikTok Shop, Walmart | Trạng thái `DELIVERED` kèm thời điểm cập nhật |
| Shopify | Sự kiện fulfillment |
| Amazon MFN | Chỉ ngày giao dự kiến: `EarliestDeliveryDate`/`LatestDeliveryDate` trong Orders |
| Amazon FBA | Chỉ ngày giao dự kiến: `estimated-arrival-date` trong report FBA Shipments |
| Shopee | Chỉ `update_time` khi sang `COMPLETED` |

Pipeline và feature chỉ thấy ngày Channel cho, còn nhãn dùng ngày thật để có đáp án đúng. Amazon và Shopee là giả định, xem [channel-sources.md](channel-sources.md).

## 5. Vấn đề dữ liệu được chèn vào

### Offline (Batch)

| Vấn đề | Cách chèn |
|---|---|
| **Skew** | Order theo Storefront: Amazon US 45%, TikTok Shop US 15%, Amazon UK 12%, Amazon DE 10%, Shopify 8%, Walmart US 7%, Shopee VN 3%. SKU theo phân phối Zipf, tham số chọn sao cho SKU bán chạy nhất chiếm khoảng 5% Order Line để job Spark có task chạy chậm rõ rệt. 8 danh mục có tỷ lệ trả khác nhau. Địa lý lệch: vài thành phố lớn chiếm phần lớn Order của mỗi Storefront. Prime Day làm một số ngày có lượng Order gấp nhiều lần |
| **High cardinality** | Giữ 2.000 SKU. Cardinality cao đến từ `order_id` (khoảng 720 nghìn), `order_line_id` (hơn 1 triệu), `tracking_number` (khoảng 700 nghìn), mã buyer theo Channel (xem [mục 9](#9-buyer-tiền-tệ-và-thời-gian)) |
| **Schema evolution** | Từ một ngày cố định, dữ liệu đơn Amazon chuyển từ model Orders v0 sang Orders 2026-01-01. Cả hai là model chính thức. Có đủ ba kiểu thay đổi: đổi tên (`AmazonOrderId` thành `orderId`, `PurchaseDate` thành `createdTime`), đổi cấu trúc (`OrderStatus` thành `fulfillment.status`, giá trị `Shipped` thành `SHIPPED`), và field mới (`proceeds`) nên các partition cũ bị null. Ngược lại, field của v0 không còn ở bản mới (ví dụ `IsPrime`, chuyển thành `programs`) bị null ở các partition mới |
| **Duplicate 2%** | Export chồng lấn ngày (ví dụ lấy lại 3 ngày gần nhất để bắt cập nhật, như report `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL` của Amazon). Có bản trùng y hệt và **bản sửa lại** (cùng khóa, thời điểm cập nhật mới hơn, trạng thái mới hơn). Dedup phải giữ bản mới nhất, không chỉ bỏ bản giống hệt |

Cách đo tỷ lệ trùng: `1 - count(distinct khóa) / count(*)`. Khóa theo Channel: Amazon `AmazonOrderId` (từ khi đổi schema là `orderId`), TikTok Shop `id`, Shopify `id`, Walmart `purchaseOrderId`, Shopee `order_sn`. Cột thời điểm cập nhật dùng để chọn bản mới nhất: Amazon `LastUpdateDate`/`lastUpdatedTime`, TikTok Shop và Shopee `update_time`, Shopify `updated_at`, Walmart `fetched_at`.

### Streaming

| Vấn đề | Channel | Mức mặc định |
|---|---|---|
| **Duplicate** | TikTok Shop, Shopify (webhook) | 1.5%. Bản trùng giữ **cùng** mã thông báo (TikTok Shop `tts_notification_id`) để Flink dedup được |
| **Late** | Amazon (notification) | Trễ trung vị vài phút, 5% trễ hơn 1 giờ. Message trễ được gửi sau cả những message có `event_time` muộn hơn, tức là đến lệch thứ tự |
| **Theo lô** | Walmart, Shopee (polling, là lựa chọn thiết kế của dự án) | Gộp theo chu kỳ 5 đến 15 phút |
| **Burst** | Livestream TikTok (sau `cutoff`) | 5 đến 8 lần tốc độ thường trong vài giờ |

Prime Day cũng gây burst nhưng nằm trong Batch, nên chỉ tính cho skew theo ngày, không tính cho burst của stream.

**Tốc độ phát theo đoạn.** Burst phải kéo dài đủ lâu theo đồng hồ thật để thấy trên Flink UI (backpressure, throughput). Nếu phát đều 60 giây thật cho mỗi ngày mô phỏng thì buổi livestream 6 giờ chỉ kéo dài khoảng 15 giây. Vì vậy Stream được phát theo đoạn:
- Ngày bình thường: 60 giây thật cho mỗi ngày mô phỏng.
- Ngày có livestream: 600 giây thật cho mỗi ngày mô phỏng. Buổi livestream 6 giờ kéo dài khoảng 150 giây thật, với tốc độ message gấp 5 đến 8 lần bình thường.
- Một lần phát đủ 30 ngày Stream mất khoảng 40 phút.

Late arrival không bị ảnh hưởng bởi tốc độ phát, vì Flink đo độ trễ theo `event_time` trong payload.

### Drift

| Loại | Sự kiện | Nằm ở đâu |
|---|---|---|
| **Covariate** | Prime Day: giảm giá sâu và phân phối SKU đổi ở Amazon | Trong Batch, khoảng ngày 120. Tập train đã từng thấy một đợt sale |
| **Concept** | Livestream TikTok: tỷ lệ trả quần áo gần gấp đôi, feature không đổi | Chỉ trong Stream, sau `cutoff`. Mô hình chưa từng gặp, phải phát hiện được trước khi nhãn về sau 30 ngày |

Drift là thay đổi tham số của lõi theo lịch cấu hình, không phải sửa dữ liệu sau khi sinh.

## 6. Nguồn dữ liệu từng Channel

Mỗi nguồn khớp tài liệu chính thức. Field mức "Trung bình" hoặc "Thấp" ghi là giả định, xem [channel-sources.md](channel-sources.md).

| Channel | Nguồn Batch | Nơi lưu | Stream |
|---|---|---|---|
| Amazon | (1) Đơn hàng JSON, theo Orders v0 rồi Orders 2026-01-01. (2) Returns report TSV `GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE` cho đơn MFN. (3) Report FBA Shipments `GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL` (TSV). (4) Report FBA Returns `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` (TSV) | MinIO | Notification, hay đến trễ |
| TikTok Shop | (1) Đơn hàng JSON theo Get Order Detail 202309. (2) Return JSON theo Search Returns 202602 | MinIO | Webhook type 1 (đổi trạng thái đơn) và 12 (đổi trạng thái trả hàng), hay gửi trùng |
| Shopify | Đơn hàng JSONL theo cấu trúc REST Order, gồm `refunds` và `fulfillments` (REST là legacy, giữ vì đã xác minh field) | MinIO | Webhook, hay gửi trùng |
| Walmart | Đơn hàng và return JSON trong cột `raw_payload` JSONB, kèm `fetched_at` | PostgreSQL | Polling, đến theo lô |
| Shopee | Đơn hàng (`get_order_detail`) và return (`get_return_list`) JSON trong cột `raw_payload` JSONB, kèm `fetched_at` | PostgreSQL | Polling, đến theo lô |

Nhận xét:
- Amazon có 4 nguồn định dạng khác nhau, nên Silver phải ghép chúng theo `order-id`.
- Bronze có hai kiểu ingest: đọc file từ MinIO và đọc JDBC từ PostgreSQL.
- Chi tiết giữ nguyên từ tài liệu để buộc lớp ánh xạ trạng thái phải xử lý: TikTok Shop dùng `CANCELLED` ở API nhưng `CANCEL` ở webhook.

## 7. Dữ liệu tham chiếu

Các phần sau cần bảng dim có lịch sử (SCD2) và quan hệ dim–fact, nên generator xuất thêm:

| Dữ liệu | Nội dung | Nơi lưu | Dùng cho |
|---|---|---|---|
| Danh mục SKU | `sku_id`, tên, danh mục, giá vốn. Giả lập hệ thống ERP của Seller | PostgreSQL, `seller.skus` | `dim_sku` |
| Ảnh chụp Listing hàng ngày | Mỗi Listing mỗi ngày: Storefront, mã Listing của Channel (ASIN, variant ID, ...), giá, trạng thái. Giá đổi quanh các đợt sale, nên Prime Day tự sinh nhiều phiên bản mới | MinIO | `dim_listing` SCD2 |
| Tỷ giá ngày | Tỷ giá USD của GBP, EUR, VND theo ngày | MinIO | Quy đổi tiền ra USD |

Ảnh chụp Listing ở dạng chung do Seller tự xuất, không theo định dạng từng Channel. Đây là giả định để giữ phạm vi.

## 8. Bố cục lưu trữ

- **MinIO**, bucket `source-exports`: đường dẫn theo nguồn rồi theo **ngày export**, ví dụ `channel=amazon/dataset=orders/export_date=2026-03-14/`. Chia theo ngày export (không theo ngày đặt hàng) vì export chồng lấn và thay đổi schema đều gắn với ngày export, và pipeline DP1 kéo dữ liệu theo từng ngày. Mỗi ngày vài file cỡ vừa. Một số ngày cố ý sinh rất nhiều file nhỏ để tạo bài toán small-file cho phần Lakehouse.
- **PostgreSQL** (giả lập hệ thống của phòng ban khác): mỗi nguồn một schema, gồm `walmart.orders`, `walmart.returns`, `shopee.orders`, `shopee.returns`, `seller.skus`. Tiền tố `raw_` để dành cho tầng Bronze, không dùng ở hệ thống nguồn.
- **Kafka**: mỗi Channel một topic. Key là mã Order để các event của cùng một Order vào cùng partition. Value là payload gốc của Channel (ví dụ body webhook TikTok Shop).

## 9. Buyer, tiền tệ và thời gian

- Lõi biết buyer đầy đủ (mã, quốc gia, mã bưu chính). Renderer chỉ lộ những gì Channel lộ thật:

  | Channel | Mã buyer | Ghi chú |
  |---|---|---|
  | Walmart | `buyerId` | Có `isGuest` |
  | Shopify | `customer.id` | Null với khách không đăng nhập |
  | TikTok Shop | `user_id` | Chỉ có nghĩa trong TikTok Shop |
  | Shopee | `buyer_user_id` | Chỉ có nghĩa trong Shopee |
  | Amazon | Không có mã buyer ổn định | Chỉ có thông tin giao hàng bị hạn chế |

  Không mã nào dùng chung được giữa các Channel, nên feature vẫn tra theo `sku_id` + `storefront_id`.
- Tiền giữ nguyên số gốc kèm mã tiền tệ gốc (USD, GBP, EUR, VND theo Storefront), quy ra USD bằng bảng tỷ giá ngày ở [mục 7](#7-dữ-liệu-tham-chiếu). Tỷ giá sinh từ chuỗi giả lập dao động nhẹ quanh giá trị gốc. Shopify giữ cả tiền của store (`currency`) và tiền khách trả (`presentment_currency`).
- Thời gian lưu UTC.
  - `event_time` là thời điểm sự kiện xảy ra, nằm sẵn trong payload gốc (ví dụ `update_time` của TikTok Shop, `LastUpdateDate` của Amazon).
  - `ingested_at` là thời điểm hệ thống nhận. Với stream là timestamp của Kafka record lúc generator gửi. Với Batch là `export_date` hoặc `fetched_at`.
  - Chênh lệch giữa hai mốc là late arrival.

## 10. Cấu hình

Toàn bộ tham số nằm trong một file YAML. Tên khóa và giá trị dưới đây là **đề xuất**, sẽ chốt khi viết code. Tài liệu đầy đủ từng khóa sẽ nằm trong tài liệu của generator khi có code.

```yaml
seed: 42
scale:
  n_skus: 2000
  start_date: 2026-01-01     # ngày mô phỏng 1
  days_history: 180          # từ ngày 1 tới cutoff
  days_stream: 30            # sau cutoff
  orders_per_day: 4000
  sku_zipf_exponent: 0.8     # SKU bán chạy nhất khoảng 5% Order Line (tính với 2.000 SKU)
storefront_weights:          # skew theo Storefront
  amazon_us: 0.45
  tiktok_shop_us: 0.15
  amazon_uk: 0.12
  amazon_de: 0.10
  shopify: 0.08
  walmart_us: 0.07
  shopee_vn: 0.03
labels:
  base_return_rate: 0.11
  window_days: 30
offline_problems:
  duplicate_rate: 0.02
  overlap_days: 3
  amazon_orders_api_switch_day: 100    # v0 sang 2026-01-01
  small_file_days: [45, 46, 47]
streaming_problems:
  pacing:
    default_seconds_per_sim_day: 60
    segments:                          # phát chậm quanh sự kiện cần quan sát
      - {from_day: 195, to_day: 195, seconds_per_sim_day: 600}
  duplicate_rate: {tiktok_shop_us: 0.015, shopify: 0.015}
  amazon_late: {median_minutes: 5, share_over_1h: 0.05}
  polling_batch_minutes: [5, 15]
  burst_multiplier: [5, 8]
drift:
  prime_day: {day: 120, days: 2}
  tiktok_live: {day: 195, start_hour_utc: 12, duration_hours: 6}
storage:
  minio_bucket: source-exports
  postgres_channels: [walmart_us, shopee_vn]
```

Mọi con số có thể tăng khi chạy trên cloud (ví dụ `n_skus`, `orders_per_day`). `sku_zipf_exponent` cần chỉnh lại sau khi đo để đạt mục tiêu khoảng 5%.

## 11. Bản đáp án và bằng chứng chất lượng

### `run_manifest.json`

Sau mỗi lần chạy, generator ghi `run_manifest.json`: seed, cấu hình, số dòng theo nguồn, tỷ lệ lỗi thật đã chèn (duplicate, late, burst), ngày đổi schema, các đợt drift, dung lượng và số file theo nguồn. Đây là đáp án để đối chiếu.

### Báo cáo chất lượng dữ liệu

Rubric yêu cầu ảnh chụp output tóm tắt chất lượng dữ liệu. Phần này chia làm hai:

- **Module `generator/report.py`** chứa logic đo, có docstring và unit test, dùng DuckDB (nhẹ, có `approx_count_distinct`, đọc được cả MinIO lẫn PostgreSQL). Stream đo bằng một consumer Kafka nhỏ.
- **Notebook `notebooks/data_quality.ipynb`** chỉ gọi module đó rồi vẽ bảng và biểu đồ để chụp. Output được xóa trước khi commit để repo sạch.
- Ảnh chụp nằm trong `docs/images/generator/` và được nhúng vào mục "Kết quả chạy" sẽ thêm vào chính tài liệu này, mỗi ảnh kèm lời giải thích nó chứng minh điều gì. Như vậy phần generator có đúng một tài liệu, theo yêu cầu dòng Documentation của rubric.

| Rubric yêu cầu chụp | Báo cáo đo gì |
|---|---|
| Skew distribution (city/category %) | Phần trăm Order theo thành phố, theo danh mục, theo Storefront, theo SKU top, theo ngày |
| Cardinality | `approx_count_distinct` của `order_id`, `order_line_id`, `tracking_number`, mã buyer |
| Schema evolution | Tỷ lệ null của field mới (`proceeds`) theo từng `export_date`, trước và sau ngày đổi schema |
| Duplicate trước và sau dedup | Tỷ lệ trùng theo công thức ở mục 5, trước và sau khi áp quy tắc dedup (giữ bản mới nhất theo khóa). Báo cáo tự tính cả hai để phần generator không phụ thuộc Spark. Job Spark sau này phải ra cùng con số |
| Streaming burst/late/duplicate | Số message mỗi phút, phân phối `ingested_at - event_time`, tỷ lệ `tts_notification_id` lặp |
| Data volume, format sau lưu | Dung lượng và số file theo nguồn, định dạng từng nguồn |

Mỗi con số đo được đặt cạnh con số trong `run_manifest.json`.

## 12. Kỹ thuật và đóng gói

- Python, số ngẫu nhiên bằng `numpy` `Generator` với một `seed` duy nhất. Cùng seed và cùng cấu hình cho cùng dữ liệu. Riêng thời điểm Kafka nhận message phụ thuộc đồng hồ thật.
- Hai chế độ chạy: sinh Batch (job chạy một lần) và phát Stream (tiến trình chạy lâu, gửi vào Kafka).
- Ghi ra qua `fsspec`/S3 API vào MinIO hoặc bucket cloud, không qua đĩa máy cá nhân.
- Chia theo ngày để ít tốn RAM. Hơn 1 triệu Order Line chạy được trong một process, chưa cần Spark.
- Unit test local chỉ chạy cấu hình rất nhỏ (ví dụ 20 SKU, 3 ngày).
- Mỗi file có đoạn mô tả ở đầu file, mỗi hàm và class có docstring, theo yêu cầu chung của rubric.
- **Docker**: có Docker Compose (MinIO, PostgreSQL, Kafka) cho môi trường thử. Để có số liệu "giảm từ bao nhiêu xuống bao nhiêu", build trước một image đơn giản (một stage, base image đầy đủ) và ghi dung lượng, sau đó build bản tối ưu (multi-stage, base image slim, không giữ cache cài đặt) và ghi dung lượng. Kết quả ghi vào `docs/Docker.md`.
- Hạ tầng nặng chạy trên cloud, dựng bằng Terraform và `destroy` khi nghỉ.

## 13. Giả định và việc còn mở

**Giả định** (đã ghi rõ, cần xác minh nếu có nguồn):
- Quy luật sinh nhãn và các hệ số.
- Danh sách khoảng 10 mã lý do trả của Amazon.
- Ngày giao của Amazon và Shopee (chỉ thấy proxy).
- Report `GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE` chỉ chứa đơn MFN, hàng trả FBA nằm ở report FBA Returns.
- Ngày Amazon chuyển từ Orders v0 sang Orders 2026-01-01 là ngày tự chọn, không phải lịch thật của Amazon.
- Giá trị `ReturnStatus` của Shopee ngoài `REQUESTED` và `CANCELLED`.
- Danh sách trạng thái return của Walmart, danh sách cột đầy đủ của report FBA Shipments và FBA Returns.
- Notification của Amazon và webhook của Shopify: payload và cách Channel gửi trùng chưa đọc từ tài liệu chính thức.
- Ảnh chụp Listing dạng chung do Seller tự xuất.

**Việc còn mở:**
- Tình trạng REST và GraphQL của Shopify năm 2026.
- Có lưu bảng nhãn thật (ground truth) ở nơi riêng hay chỉ trong manifest.
- Ước lượng dung lượng dữ liệu: đo từ lần chạy nhỏ đầu tiên rồi ngoại suy, không đoán trước.
- Thiết kế bảng theo từng tầng (`raw_`, `stg_`, `dim_`, `fact_`, `feat_`, SCD2) thuộc phần schema design, chưa làm.

## 14. Đối chiếu với rubric

| Hạng mục rubric | Thiết kế đáp ứng ở | Bằng chứng sẽ nộp |
|---|---|---|
| Docker & Docker Compose | Mục 12 | `docs/Docker.md` |
| Optimize Dockerfile | Mục 12 | Dung lượng image trước và sau tối ưu |
| Simulate skew | Mục 5, offline | Ảnh chụp báo cáo, mục 11 |
| Simulate high cardinality | Mục 5, offline | Ảnh chụp `approx_count_distinct` |
| Simulate schema evolution | Mục 5, offline | Ảnh chụp tỷ lệ null theo `export_date` |
| Simulate another offline problem | Mục 5, duplicate 2% | Ảnh chụp tỷ lệ trùng trước và sau dedup |
| Using generator configuration | Mục 10 | File cấu hình và tài liệu từng khóa |
| Store data để ingest vào Bronze | Mục 6, 7, 8 | Ảnh chụp MinIO và PostgreSQL |
| Simulate burst | Mục 5, streaming | Ảnh chụp số message mỗi phút |
| Simulate late arrivals | Mục 5, streaming | Ảnh chụp phân phối độ trễ |
| Simulate another streaming problem | Mục 5, duplicate 1.5% | Ảnh chụp tỷ lệ mã thông báo lặp |
| Using generator configuration (streaming) | Mục 10, `streaming_problems` | File cấu hình |
| Document data characteristic | Mục 6, 8, 11 | Dung lượng, số file, định dạng từ báo cáo |
| Document config generator | Mục 10 | Tài liệu từng khóa |

Các phần sau dùng dữ liệu của generator: dim SCD2 và quan hệ dim–fact (mục 7), bài toán small-file của Lakehouse (mục 8), window của Flink (mục 5, streaming).
