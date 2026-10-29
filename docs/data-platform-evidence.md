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
| Airflow `dp1_bronze` | 10/10 task success (5 Channel ingest + validate, đủ 180 ngày), ~55 phút; `validate_amazon`: nguồn 108.248.563 = Bronze 108.248.563, lỗi 0 | [Graph](evidence/aws/dp1-bronze-graph-20261027.png), [Gantt](evidence/aws/dp1-bronze-gantt-20261027.png), [log validate](evidence/aws/dp1-validate-amazon-log-20261027.png), [Spark master 2×2 core](evidence/aws/spark-master-2x2core-20261027.png) | Ghi rồi mới kiểm (chưa phải branch WAP); `dp3_features` xem dòng dưới |
| Airflow `dp2_silver` | 14/14 task success, 1 giờ 58 phút. Silver = Gold theo Channel: amazon 153.849.901, tiktok_shop 33.338.110, shopify 17.782.786, walmart 15.562.675, shopee 6.666.831 (Order Line, nên nhiều hơn số dòng Bronze). Gold invalid 0, trùng khoá Order Line 0. Amazon SCD2: 106.109.637 bản hiện hành + 1.062.966 bản lịch sử, lỗi 0 | [Summary](evidence/aws/dp2-silver-summary-20261028.png), [Gantt](evidence/aws/dp2-silver-gantt-20261028.png), [log validate](evidence/aws/dp2-validate-logs-20261028.png) | `validate_walmart_silver` lần 1 hỏng vì ổ gốc đầy, chạy lại PASS; Summary chỉ ghi "success", lần hỏng chỉ thấy ở Gantt |
| Airflow `dp3_features` | 2/2 task success, 1 giờ 12 phút (build ~8 phút, validate ~64 phút). Feature 227.200.303 = Gold fact 227.200.303 (tổng 5 Channel dp2); missing 0, extra 0, trùng khoá 0, sai event time 0 | [Summary](evidence/aws/dp3-features-summary-20261028.png), [Gantt](evidence/aws/dp3-features-gantt-20261028.png), [log validate](evidence/aws/dp3-validate-log-20261028.png) | Validate so từng dòng với Gold nên lâu gấp ~8 lần build |
| Airflow `dp3_walmart_labels` | Sổ tra 15.562.675 dòng (= Silver/Gold Walmart) tạo từ ground truth seed 42, đọc từ S3; 12.165.007 Order Line có nhãn, trùng 0, lỗi 0 | log task (receipt `airflow_dp3_walmart_labels`) | Attempt 1 OOM exit 137 (worker hứa 3g > trần container), hạ `SPARK_WORKER_MEMORY` 2560m rồi PASS; dòng thiếu nhãn = chưa giao |
| Airflow `dp1_cdc` + Debezium trên EC2 | Debezium 3.4.3 đọc WAL Postgres (`snapshot.mode=no_data`) → Kafka `platform_cdc.walmart.orders` → Spark → `bronze.raw_walmart_cdc_v2`. Probe c/u/d: 731/452/295 ms commit→Kafka; Validate Kafka=3, Bronze=3, offset duy nhất 3, thiếu/thừa/lỗi 0 | [probe](evidence/aws/cdc-probe-20261028.json), [log Validate](evidence/aws/dp1-cdc-validate-20261028.png), [lineage](evidence/aws/datahub/dp1-cdc-lineage.png), [Quality](evidence/aws/datahub/dp1-cdc-validation.png), receipt `airflow_dp1_cdc` | Chỉ 3 sự kiện probe, không đo tải; nguồn thiếu index nên probe chậm (gap #15) |
| Kafka + Flink 1.20.3 trên EC2 | Producer 41 s: Amazon 399.410, TikTok 93.554 (1.397 trùng) = local. 5 job FINISHED: baseline 399.410; dedup 92.157 (= unique_keys, cạnh HASH → Deduplicate, 93.554 record vào); tumbling 7.555 / hopping 37.870 cửa sổ; tin muộn 10.166 | [Completed Jobs](evidence/aws/flink-completed-jobs-20261028.png), [dedup graph](evidence/aws/flink-dedup-graph-20261028.png), [manifest + số](evidence/aws/stream-manifest-20261028.json) | Stream cỡ `default.yaml`, không 100 GB; Kafka volume trên ổ gốc; số tin muộn không tất định (local 9.143) |
| DataHub 1.7.0.1 trên EC2 | Publish từ log Airflow AWS, đủ 5 DAG (`--dags dp1_bronze dp1_cdc dp2_silver dp3_features dp3_walmart_labels`, `--raw-root s3a://…/source-exports`): 21 dataset, 29 task, 15 assertion PASS. Lineage S3/Postgres/Kafka → Bronze → Silver → Gold → feature, nhãn | [lineage Bronze](evidence/aws/datahub/dp1-lineage.png), [Quality Bronze](evidence/aws/datahub/dp1-validation.png), [lineage Gold](evidence/aws/datahub/dp2-lineage.png), [Quality Gold](evidence/aws/datahub/dp2-validation.png), [lineage feature](evidence/aws/datahub/dp3-lineage.png), [Quality feature](evidence/aws/datahub/dp3-validation.png), [nguồn S3](evidence/aws/datahub/s3-sources.png) | Lineage khai tay (gap #3); UI lỗi menu/tìm kiếm (gap #14) |
| Airflow `maintenance_iceberg` | 3/3 task success (plan → execute → validate), bảng `bronze.raw_shopee_orders_dp1`. Plan `no_op`: chỉ kiểm kê orphan (dry-run, `prefix_listing` qua S3FileIO). Trước = sau: 4.689.957 dòng, 180 file, 461.051.515 byte, 1 snapshot, fingerprint trùng | [Receipt](evidence/aws/data-platform-aws-20261027.json) (`airflow_maintenance_iceberg`) | Bảng 1 file/partition ngày (~2,6 MB/file) nên compaction không có gì để gộp; partition theo ngày cho kênh nhỏ là gap riêng |
| Airflow `ml_feature_materialize` (Gold A → Feast) | 4/4 task success, run `manual__2026-10-29T00:30:43+00:00`: `export_sku_daily` + `export_training_lines` ghi Parquet `s3://<lakehouse>/feature-store/` (training_lines: mẫu 1% theo hash, nhãn có trước cutoff 2026-06-30); `feast_apply` dùng IAM role EC2 (log: `Found credentials from IAM Role: platform-data-platform-vm`); `materialize_incremental` nạp `sku_return_daily` vào Redis trên EKS (NodePort 30379) | [Log feast_apply](evidence/aws/ml-feature-materialize-feast-apply-log-20261029.png), [Graph](evidence/aws/ml-feature-materialize-graph-20261029.png), [Gantt](evidence/aws/ml-feature-materialize-gantt-20261029.png) | Gantt thấy 2 lần hỏng đã sửa: `export_training_lines` (cột `run_id` trùng giữa fact và nhãn), `feast_apply` (overlay AWS thiếu mount `feature_repo`). View `sku_return_24h` là file rỗng (stream pusher chưa chạy trên AWS) |
| ML trên EKS: Kubeflow → MLflow → KServe | KFP 2.17.2 trên EKS: run `train-aws-3` (`0485ebd9…`) 5/5 bước, 10:42; đọc `training_lines` S3 qua Pod Identity `kubeflow/pipeline-runner`, image ECR `platform-train:9d49807-m3b`. MLflow trên EC2 (artifact S3, gọi qua IP riêng :5050): `return-risk` v1 alias `production`, PR-AUC model ≈ 0,25 vs baseline ≈ 0,25 (`beats_baseline`=1, lift 2,25, prevalence 0,11, test 16.411 dòng). KServe v0.21.0 RawDeployment: `return-risk` READY, image ECR `platform-return-risk:v1-9d49807-m4a-55e51c6b`; gọi `/v1/models/return-risk:predict` SKU `APP-00042` → score 0,166, `abstain=false` (Feast online: registry S3 qua Pod Identity `ml-serving/return-risk` + Redis EKS) | [KFP runs](evidence/aws/kfp-runs-20261029.png), [KFP graph](evidence/aws/kfp-train-aws-3-graph-20261029.png), [MLflow runs](evidence/aws/mlflow-runs-20261029.png), [metrics](evidence/aws/mlflow-metrics-20261029.png), [registry](evidence/aws/mlflow-registry-production-20261029.png) | 4 run KFP hỏng trước (OOM với 1% mẫu; Feast file store nối mọi ảnh chụp ngày → gọi theo lô 20k; port-forward chưa sẵn). Mẫu train 2‰ (359.457 dòng). Model thắng baseline rất sát; view stream 24h rỗng trên AWS. Cache KFP tắt (EKS không ký CSR) |
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
