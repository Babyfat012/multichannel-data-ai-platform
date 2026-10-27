# Data Platform — evidence

Bằng chứng chạy trên AWS đứng trước; bằng chứng local giữ nguyên bên dưới làm đối chứng. Mỗi phép đo có phạm vi dữ liệu và môi trường riêng; không suy từ fixture sang full-scale hoặc từ local sang AWS.

## Trên AWS (cloud) — 2026-10-27

Sydney `ap-southeast-2`, EC2 `c6i.2xlarge` + S3, Spark 2 worker × 2 core, quyền S3 qua IAM role (không access key).
Receipt: [data-platform-aws-20261027.json](evidence/aws/data-platform-aws-20261027.json).

| Thành phần | Kết quả đã ghi nhận | Evidence | Giới hạn |
|---|---|---|---|
| Raw trên S3 | 17.393 object / 141.774.479.087 byte, khớp từng byte đĩa EC2; phần v1 (Amazon + Walmart + SKU) = 100.012.790.412 byte, khớp manifest local | [Receipt](evidence/aws/data-platform-aws-20261027.json) | Ba Channel thêm (TikTok Shop, Shopify, Shopee) chỉ có manifest EC2 đối chiếu |
| Nguồn Walmart (PostgreSQL) | `walmart.orders` 10.949.058 dòng, `seller.skus` 2.000, nạp 443 s | [Receipt](evidence/aws/data-platform-aws-20261027.json) | |
| Bronze Amazon 100 GB | 108.248.563 dòng, 180 ngày, đọc `s3a://`, 506,6 s | [Receipt](evidence/aws/data-platform-aws-20261027.json) | Chạy tay bằng `spark-submit`; bản trong DAG ở dòng dưới |
| Bronze Walmart | 10.949.058 dòng qua JDBC, tự so nguồn khớp, 267,2 s | [Receipt](evidence/aws/data-platform-aws-20261027.json) | Chạy tay bằng `spark-submit` |
| Airflow `dp1_bronze` | 10/10 task success (5 Channel ingest + validate, đủ 180 ngày), ~55 phút; `validate_amazon`: nguồn 108.248.563 = Bronze 108.248.563, lỗi 0 | [Graph](evidence/aws/dp1-bronze-graph-20261027.png), [Gantt](evidence/aws/dp1-bronze-gantt-20261027.png), [log validate](evidence/aws/dp1-validate-amazon-log-20261027.png), [Spark master 2×2 core](evidence/aws/spark-master-2x2core-20261027.png) | Ghi rồi mới kiểm (chưa phải branch WAP); `dp2_silver`, `dp3_features` đang chạy |
| EKS `platform-apps` | Kubernetes 1.37, 2 node `c6i.xlarge` Ready; EBS CSI + Pod Identity, StorageClass `gp3` mặc định, PVC ghi/đọc đạt; Redis Feast chạy, EC2 → Redis NodePort 30379 `PONG` từ cả hai node | [PVC smoke](evidence/aws/eks-ebs-gp3-pvc-smoke-20261027.png), [EC2 → Redis](evidence/aws/ec2-to-eks-redis-ping-20261028.png) | Chưa có workload ML/LLM ngoài Redis |

## Local — 2026-10-13 đến 2026-10-22

Receipt, log và ảnh gốc giữ nguyên nội dung, timestamp và định danh của môi trường đã chạy. Tên tài nguyên trong artifact lịch sử có thể khác cấu hình hiện hành dùng tiền tố `platform`; đổi tên cấu hình không phải là chạy lại phép đo.

### Nguồn và xử lý dữ liệu

| Thành phần | Kết quả đã ghi nhận | Evidence / mô tả phép đo | Giới hạn |
|---|---|---|---|
| Docker | So sánh cùng 27 package: naive 2,39 GB → optimized 698 MB | [So sánh image](evidence/docker-compare-20261019.json), [inventory local](evidence/docker-inventory-local-20261019.json), [phương pháp](Docker.md) | Inventory overlay GCP lịch sử cũng được thu local; không chứng minh deploy cloud |
| Generator batch v1 | 100.012.790.412 byte raw từ Amazon, Walmart và SKU; duplicate 1,98% / 1,97% | [Manifest](evidence/batch-100gb-manifest.json), [báo cáo batch](data-quality-report-batch.md) | Ba Channel còn lại dùng nguồn nhỏ; không phải 100 GB v2 |
| Generator stream v1 | 492.964 message; late arrival, out-of-order, duplicate và burst | [Báo cáo stream](data-quality-report-stream.md) | Phạm vi cấu hình nhỏ, tách khỏi batch 100 GB |
| Nguồn lifecycle v2 | 60.000 Order, 97.890 line, 1.546.066.142 byte; Spark reconcile trên 2.000 Order | [Scale](source-order-lifecycle-scale.md), [fixture](source-order-lifecycle-fixtures.md) | Chưa đạt 100 GB v2 |
| Bronze v2 | 550 bản ghi: 547 file + 3 JDBC, khớp manifest | [Receipt Bronze](evidence/bronze-v2-jdbc-20261018.json) | Fixture local |
| Silver v2 | 19/19 check có sai lệch bằng 0; giữ revision và chặn conflict | [Receipt Silver](evidence/silver-v2-receipt-20261018.json), [xử lý Silver](silver-order-lifecycle.md) | Receipt sạch có duplicate_rows_dropped=0; không thay phép đo duplicate v1 |
| Spark v1 | Baseline/AQE, skew, schema evolution, dedup; execution 3: 8.449.302 → 8.314.312 dòng | [Báo cáo Spark](spark.md), [event log và ảnh UI](evidence/spark-ui/README.md) | Các run có cửa sổ dữ liệu khác nhau; không suy thành benchmark v2 |
| Spark v2 | Có workload baseline/salted và kiểm bag equality | [Workload và protocol](spark-order-lifecycle.md) | Chưa có phép đo hiệu năng baseline/salted v2 |
| Flink v1 | Watermark, late arrival, keyed dedup, tumbling/sliding window | [Báo cáo Flink](flink.md), [state metrics](evidence/flink-dedup-state.json) | Kết quả gắn với job/offset ghi trong báo cáo |
| Stream lifecycle v2 | Hai epoch, restart/restore checkpoint; kept 15.261 khớp oracle | [Receipt stream](evidence/stream-v2-local-20261019.json), [protocol](stream-order-lifecycle.md) | Đếm sự kiện bounded; không phải HA hay số liệu kinh doanh chính thức |
| CDC lifecycle v2 | Insert/update/delete khớp oracle; ca extra bị chặn | [Receipt CDC](evidence/cdc-v2-local-20261019.json), [protocol](cdc-order-lifecycle.md) | Harness local, chưa chứng minh live DAG v2 |

### Storage, điều phối và governance

| Thành phần | Kết quả đã ghi nhận | Evidence / mô tả phép đo | Giới hạn |
|---|---|---|---|
| Iceberg v1 | Compaction 187 → 181 file; truy vấn bảy ngày có trung vị 10,348 → 9,405 s | [Storage](storage.md) | Theo bảng và lần chạy đã ghi |
| Warehouse v1 | 1.875.607 dòng Walmart; một lượt EXPLAIN 51,611 → 1,134 ms sau index | [Storage](storage.md) | Không phải phép đo trên toàn bộ Gold năm Channel |
| Federation | Gold và warehouse cùng SKU: 211 dòng, net 12.038,74 | [SQL và kết quả](trino.md), [đối chứng](data-integration.md) | Một SKU Walmart, không kết luận truy vấn liên nguồn luôn nhanh hơn |
| Airflow v1 | DP1/DP2/DP3 có Graph/Grid/log; runtime configuration smoke thành công 2/2 task | [Airflow](airflow.md), [receipt runtime](evidence/airflow-runtime-20261015.json) | Smoke cấu hình không ghi lại bảng nghiệp vụ |
| DataHub v1 | Lineage và assertion trích từ run Validate | [DataHub](datahub.md), [manifest](evidence/datahub-validation.json) | Assertion lịch sử, không xác nhận mọi run sau đều tự publish |
| Airflow / DataHub v2 | DAG và dry-run metadata có sẵn | [Airflow v2](airflow-order-lifecycle.md), [DataHub v2](datahub-order-lifecycle.md) | Chưa có receipt live DAG v2 và ảnh lineage/quality tương ứng |
| Data contracts | Infer/diff/gate bằng fixture Python; mô tả kiểm Spark 32/32 local | [Fixture receipt](evidence/de-n1-contracts-20261018.json), [data contracts](data-contracts.md) | Receipt Python không phải bằng chứng Spark |
| Iceberg maintenance | Spark 3/3 trên bảng demo; kiểm compact/zorder, dữ liệu và tag | [Receipt maintenance](evidence/de-n2-maintenance-local-20261018.json), [cơ chế](iceberg-maintenance.md) | Chưa có lịch maintenance và phép đo trên bảng nghiệp vụ thật |

Các kết quả Gold/e2e có log chỉ lưu trong `out/` được ghi riêng tại [trạng thái lifecycle](data-platform-order-lifecycle-status.md), [Gold](gold-order-lifecycle.md) và [e2e local](e2e-order-lifecycle-local.md). Những con số đó là ghi nhận lịch sử có giới hạn kiểm chứng công khai, không được xem như receipt đính kèm repo.

## Kiểm chứng còn thiếu

- E2e medium sau các bản sửa OOM; phép đo skew v2 với cùng input.
- Live DAG lifecycle, publish/capture DataHub cùng generation và schema inventory đã deploy.
- Dữ liệu v2 100 GB, EXPLAIN trên scale tương ứng và maintenance bảng nghiệp vụ thật.
- Runtime AWS: [runbook EC2 + S3](aws-deploy.md). Terraform validate, inventory local và hướng dẫn triển khai chưa phải bằng chứng chạy AWS.

[Mục lục tài liệu](README.md)
