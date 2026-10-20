# Tiến độ DE theo rubric hiện hành

> Ba việc phát hiện trong [rà soát ngày 2026-10-15](de-audit-20261015.md)
> đã được đóng: Airflow runtime 2/2 success, ảnh/REST Spark execution 3 và
> tài liệu công khai không dẫn tới file bị Git bỏ qua.

Cập nhật 2026-10-15. Đối chiếu sheet `de` trong
`edai-k11-cw/rubic/EDAI K11 - MLE.xlsx` (38 dòng chấm, 100 điểm). Bảng này theo
dõi **artifact và bằng chứng**, không tự gán điểm khi proof còn thiếu.

| Nhóm rubric | Điểm tối đa | Trạng thái kiểm được trong repo | Việc còn lại để đóng proof |
|---|---:|---|---|
| Docker | 5 | `Dockerfile`, `docker-compose.yml`, `docs/Docker.md` có số đo image | Đối chiếu ảnh và báo cáo khi nộp |
| Generator Batch/Stream | 12 | Code và hai báo cáo chất lượng dữ liệu đã viết lại theo phạm vi 100 GB (Amazon + Walmart) kèm mục 3-Channel nguồn nhỏ; `/data/raw/run_manifest.json` ghi 100.012.790.412 byte. Renderer Batch sinh nguồn nhỏ đủ năm Channel ở `out/channels-small` | Mở rộng ba nguồn nhỏ lên quy mô dự án nếu có tài nguyên; số đo 100 GB hiện chỉ thuộc Amazon + Walmart |
| Spark | 13 | Bronze/Silver năm Channel và Gold fact đã chạy. `docs/spark.md` có baseline, thí nghiệm skew, ảnh Spark UI và liên kết tới ba DAG Airflow đã chạy; `docs/silver.md` ghi schema evolution/dedup, `docs/gold.md` ghi kết quả Gold | Ảnh execution 3 và REST đã đối chiếu parse/dedup; benchmark 20/180 ngày không đồng phạm vi với DP1 Airflow 10 ngày |
| Flink | 10 | Flink 1.20.3 chạy baseline Amazon (399.410), watermark late (9.143 ở 1ms; 6.310 lần đo trước), dedup TikTok (93.554 → 92.157), tumbling 7.555 / sliding 37.870 cửa sổ. `docs/flink.md` có snippet `TUMBLE`/`HOP`, ảnh UI `flink-late-arrival.png` (job `b903a349…`) và `flink-windows.png` (job `c2bb95bb…`), đồ thị keyed Deduplicate kèm REST plan/metrics | Đối chiếu ảnh và số đo khi nộp; rà lại nếu chấm đòi thêm bằng chứng state dài hạn |
| Data Storage | 6 | Amazon Bronze toàn kỳ đã compaction 187 → 181 file, truy vấn bảy ngày có trung vị 10,348 → 9,405 giây. Lát Gold Walmart 1.875.607 dòng đã vào PostgreSQL warehouse; index `seller_sku` đổi kế hoạch `Parallel Seq Scan` → `Bitmap Index Scan`, một lượt EXPLAIN 51,611 → 1,134 ms; DDL `CREATE INDEX` đã ghi vào `docs/storage.md`. Trino đối chiếu Gold và warehouse cùng SKU: hai bên 211 dòng, net 12.038,74. | Rà pruning/z-order nếu rubric cần thêm ngoài compaction đã đo |
| DP1 Airflow | 4 | `dp1_bronze` chạy thành công 10/10 task của năm Channel; Amazon/Walmart đọc `/data/raw`, ba Channel mới đọc `out/channels-small`. Bảng mới được validate đúng số dòng 1.810 / 910 / 354. Nhánh `dp1_cdc` chạy 2/2 task thành công hai lần, Kafka 6 = Bronze 6. Ảnh Graph/Grid ở `docs/airflow.md` và `docs/cdc.md` | Runtime sau bootstrap đã kiểm bằng `fsds_runtime_config_check`, run `de_runtime_20261015`: 2/2 success; Spark standalone xử lý 3 dòng, không ghi bảng. Mở rộng phạm vi nguồn là phần bổ sung |
| DP2 Airflow | 4 | Run `manual__2026-10-14T03:53:52+00:00` hoàn tất 14/14 task Silver → Gold → Amazon SCD2 → Validate. Gold có 10.194.358 Order Line, 0 dòng lỗi/khóa trùng. Amazon dim có 5.779.561 phiên bản, mọi kiểm tra khóa fact và khoảng hiệu lực đều PASS. Graph/Grid và log ở `docs/airflow.md`, `docs/gold.md`. | Mở rộng ba Channel mới từ nguồn nhỏ lên phạm vi dự án khi có tài nguyên |
| DP3 Airflow | 4 | Run `manual__2026-10-14T04:11:15+00:00` đã chạy lại sau Gold SCD2, hoàn tất 2/2 task; feature offline có 10.194.358 dòng bằng Gold, 0 dòng lỗi và 0 khóa trùng. Bridge Walmart run `manual__2026-10-14T04:15:18+00:00` ghi riêng 1.319.345 dòng có nhãn, 0 lỗi/khóa trùng. Graph/Grid và log ở `docs/airflow.md`, `docs/gold.md`. | Mở rộng nguồn ba Channel mới; kiểm thêm độ phủ nhãn theo phạm vi toàn kỳ |
| DataHub | 12 | GMS `v1.7.0.1` đã nhận **21 dataset, 29 job, 15 assertion SUCCESS** trích từ log Airflow; nguồn file/PostgreSQL/Kafka nối vào Bronze, năm Channel qua Silver/Gold tới feature. UI DataHub hiển thị Lineage và `1 Passing` ở Quality cho bảng đại diện DP1/DP2/DP3; sáu ảnh và số liệu ở `docs/datahub.md`, manifest ở `docs/evidence/datahub-validation.json`. | Rà ảnh và manifest khi nộp; bản demo cục bộ dùng MySQL `8.0`, frontend `v1.6.0.2` do hai image quickstart hiện tại có file rỗng |
| Documentation/Schema | 10 | [thuật ngữ và tầng dữ liệu](terminology.md) có quy ước tên; Bronze/Silver/Gold và feature có tài liệu. Amazon SCD2 có 5.779.561 phiên bản, fact trỏ đúng một phiên bản; `feat_*` có `event_timestamp` và `created`. `docs/schema.md` có ảnh DBeaver nối Trino (`dbeaver-zones.png` ba zone, `dbeaver-erd-dim-fact.png` quan hệ fact–dim) bên cạnh sơ đồ logic. | Rà lại nếu chấm đòi thêm ảnh |
| Novel ideas | 20 | Trino 483 đã đối chiếu cùng SKU Gold Iceberg và warehouse PostgreSQL trong một SQL: mỗi bên 211 dòng/net 12.038,74; một lượt đo CLI 2,16 giây so với hai SQL riêng 2,67 giây (`docs/trino.md`). Debezium 3.4.3.Final đã đưa hai lượt thay đổi Walmart qua Kafka vào Bronze, Validate 6/6 offset, commit → Bronze tối đa 31,261–44,380 giây ở lượt service đã sẵn sàng; bảng nguồn Batch có 1.293.525 dòng. `kafka-ui` (profile `cdc`) xác nhận topic `fsds_cdc.walmart.orders` có 4 message offset 0–3 và connector/task `RUNNING`; ảnh ở `docs/images/cdc/`, trạng thái REST ở `docs/evidence/debezium-connector-status.json` (`docs/cdc.md`). | Đối chiếu chính sách lịch Batch với thời gian đo CDC; cân nhắc phép so sánh đa Channel theo tháng |

## Kết quả đóng ba việc ngày 2026-10-15

1. **Dòng 15 và 22–27, Airflow:** run `de_runtime_20261015` thành công 2/2
   task, không có DAG import error. Lookup ba Connection/bốn Variable,
   xác nhận mật khẩu mã hóa và chạy Spark standalone bằng operator mới;
   3 dòng, tổng bằng 3, không ghi bảng. [Bằng chứng](airflow.md).
2. **Dòng 13–14, Spark:** đã chụp execution 3, mở Details và lưu REST của
   app `app-20261014035356-0002`. Hai schema đi qua Union và window dedup,
   8.449.302 → 8.314.312 dòng. [Ảnh và số đo](spark.md).
3. **Ô A2, tài liệu:** đã có [thuật ngữ](terminology.md), [kiến trúc đã triển
   khai](architecture-decisions.md), quy ước tên trong [Schema](schema.md);
   các liên kết tới tài liệu nội bộ bị Git bỏ qua đã được thay. Trạng thái
   Gold/SCD2 và ba renderer Batch được cập nhật theo proof đã có.

Gold fact, SCD2, bridge nhãn Walmart, Trino, CDC, DataHub và các job Flink
đã có proof lịch sử. Các kiểm tra nhỏ ngày 2026-10-15 bổ sung proof runtime và
hồ sơ; không đại diện cho việc chạy lại pipeline năm Channel trên toàn bộ
100 GB. Các file mới vẫn cần được chủ dự án đưa vào bản commit/nộp; việc chấm
điểm thuộc người chấm, không tự gán 100/100.

## Số liệu nguồn 100 GB

`/data/raw/run_manifest.json` ghi `datasets[*].bytes`:

| Dataset | Byte |
|---|---:|
| `amazon.orders` | 86.728.943.600 |
| `walmart.orders` | 13.283.621.342 |
| `seller.skus` | 225.470 |
| **Tổng** | **100.012.790.412** |

Đây là dung lượng raw Batch trong manifest, chưa phải dung lượng bảng Iceberg.
