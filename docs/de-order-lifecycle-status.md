# Tiến độ DE scope mở rộng (v2) — bảng một trang

Cập nhật: **2026-10-20**, gồm các kết quả planner xác nhận trong brief codex-26 (ghi chú nội bộ, không nằm trong repo).
Gói theo [scope](scope-de-gap-20261018.md); ánh xạ đủ 38 dòng tại [ma trận rubric](de-rubric-matrix.md).
Đích triển khai: **GCP** (người học tự chạy mọi lệnh Cloud theo [checklist](gcp-checklist.md)).
"Local" = đã kiểm trên máy dev, fixture/harness/container tạm; **chưa** phải bằng chứng GCP.
Một gói có kiểm local không đồng nghĩa đã đóng đủ proof rubric hoặc full-scale.

| Gói | Nội dung | Tài liệu | Local | GCP |
|---|---|---|---|---|
| B | Nguồn v2 fixture 5 Channel | [silver-v2](silver-order-lifecycle.md) | ✅ fixture 15 Order / 30 line | ⏳ |
| B+ | Nguồn v2 scale (60k đơn ≈ 1,55 GB / 7 phút) | [source-v2-scale](source-order-lifecycle-scale.md) | ✅ sinh 60k; Spark 3/3 trên 2.000 đơn (200 × 10), reconcile = 0; chưa ≥100 GB | ⏳ |
| C | Bronze/Silver v2 + đối soát | [silver-v2](silver-order-lifecycle.md), [Bronze](evidence/bronze-v2-jdbc-20261018.json), [Silver](evidence/silver-v2-receipt-20261018.json) | ✅ fixture: Bronze 550 = manifest; Silver 19/19 check = 0 | ⏳ |
| G | DE-N1 data contract + cổng cách ly | [novel-de-n1](novel-de-n1-data-contracts.md) | ✅ Spark 32/32 sau sửa identity_path; registry file Git | ⏳ |
| H | DE-N2 Iceberg tự bảo trì | [novel-de-n2](novel-de-n2-iceberg-maintenance.md), [evidence](evidence/de-n2-maintenance-local-20261018.json) | ✅ Spark 3/3 trên bảng demo; chưa chạy lịch/bảng nghiệp vụ thật | ⏳ bảng thật |
| E | Nhãn Return v2 (L01–L17) + Gold + feature PIT | [gold-v2](gold-order-lifecycle.md) | ✅ Spark fixture 5/5; Gold chỉ đọc Silver history, đã sửa OOM keyless | ⏳ |
| E-eq | Gold-equivalence sau refactor | [gold-v2](gold-order-lifecycle.md) | ✅ diff fact/nhãn/feature = 0 fixture và nguồn scale 60 ngày, trước/sau đổi tên; feature PIT có tín hiệu sau khi generator phát coverage theo tuần ([ADR-0013](adr/0013-weekly-contiguous-coverage.md)): ca Gold 60 ngày có **131 line** `labeled_count_90d > 0`, Spark khớp Python thuần (2026-10-20) | ⏳ |
| F | Warehouse v2 PostgreSQL, publish an toàn | [warehouse-v2](warehouse-order-lifecycle.md) | ✅ PostgreSQL thật; warehouse-checks PASS 116 s trước đổi tên; chưa kết luận lợi ích index scale | ⏳ EXPLAIN scale |
| I-1 | DAG dp_order_lifecycle / dp_order_lifecycle_cdc / dp_order_lifecycle_warehouse + DataHub dry-run + ERD | [airflow-v2](airflow-order-lifecycle.md), [datahub-v2](datahub-order-lifecycle.md), [ERD](diagrams/schema-order-lifecycle.md) | ✅ DagBag 10 DAG / 0 lỗi; dry-run không phải publish thật | ⏳ |
| E2E-small | Nguồn → Bronze → Silver → Gold → PostgreSQL → EXPLAIN | [e2e-v2-local](e2e-order-lifecycle-local.md) | ✅ small 45 ngày × 50 đơn, 100 SKU, 3.631 fact: PASS 92 s sau sửa và đổi tên; mọi check = 0 | ⏳ |
| E2E-medium | Chuỗi đầu-cuối trên nguồn medium | [e2e-v2-local](e2e-order-lifecycle-local.md) | ⏳ chưa chạy lại; trước sửa: 500 × 60, 48.874 fact, Silver 2.090 s, gold_build 42 s, gold_validate OOM | Đo trên GCP: `bash tests/harness/run_order_lifecycle_scale.sh --profile medium --driver-memory 6g` |
| Skew v2 | Baseline / salted, cùng input và bag equality | [spark-v2](spark-order-lifecycle.md) | ⏳ chưa đo, chưa có kết luận giảm skew | Đo trên GCP: `python tests/spark/check_order_lifecycle_skew.py` trong runtime Spark; lệnh container tại [Spark v2](spark-order-lifecycle.md) |
| I-live | Chạy thật DAG v2 + publish/capture DataHub v2 | [airflow-v2](airflow-order-lifecycle.md), [datahub-v2](datahub-order-lifecycle.md) | ⏳ chưa chạy DAG/publish/capture; chưa có receipt/Graph/Grid/Lineage/Quality | Đo trên GCP: `bash scripts/run/run_order_lifecycle_live.sh --phase all`, host chuẩn bị theo [runbook](datahub-order-lifecycle.md#chạy-thật) và checklist GCP |
| K | Terraform + Ansible + overlay GCS | [gcp-deploy](gcp-deploy.md), [gcp-runtime](gcp-runtime.md), [checklist](gcp-checklist.md) | ✅ validate / syntax / jar; chưa triển khai Cloud | ⏳ người học |
| D | Stream v2 Kafka → Flink + oracle | [stream-v2](stream-order-lifecycle.md), [evidence](evidence/stream-v2-local-20261019.json) | ✅ harness 2/2 epoch + restart; không phải HA hay official sales/label | — |
| A | CDC v2 trên walmart_lifecycle.orders | [cdc-v2](cdc-order-lifecycle.md), [evidence](evidence/cdc-v2-local-20261019.json) | ✅ harness insert/update/delete + tiêm extra bị chặn | — |
| Docker | Inventory + so naive/opt cùng bộ thư viện | [Docker](Docker.md), [so sánh](evidence/docker-compare-20261019.json), [inventory local](evidence/docker-inventory-local-20261019.json), [overlay](evidence/docker-inventory-gcp-20261019.json) | ✅ 27 package giống, 2,39 GB → 698 MB; cả inventory overlay được thu local | — |

✅ đã kiểm trong phạm vi ghi · ⏳ chờ chạy · 🔨 đang chạy · — chưa lên kế hoạch GCP

## Số đo và giới hạn ngày 2026-10-19

Gold v2 từng **OOM ở small 3.631 line** vì coverage keyless bị nhân bản theo line.
Đã sửa; **e2e small PASS 92 s** sau đổi tên, `gold_build` **7,6 s**, mọi check **= 0**;
chi tiết và EXPLAIN tại [e2e local](e2e-order-lifecycle-local.md). Q3 giảm **104 → 4** buffer,
Q2 **583 → 583**; Q1 **173 → 2** nhưng **0 dòng**, không chứng minh lợi ích index.
Gold-equivalence fact/nhãn/feature **diff = 0** trên fixture và nguồn scale **60 ngày**,
cả trước và sau đổi tên; feature từng luôn **= 0**; sau [ADR-0013](adr/0013-weekly-contiguous-coverage.md) ca Gold 60 ngày có **131 line** feature > 0, khớp Python thuần (2026-10-20).
**warehouse-checks PASS 116 s trước đổi tên**; **DagBag 10 DAG / 0 lỗi** gồm
`dp_order_lifecycle`, `dp_order_lifecycle_cdc`, `dp_order_lifecycle_warehouse` và bảy DAG snapshots giữ tên,
chỉ là kiểm import. Pytest unit do planner xác nhận: **718 pass** (2026-10-20).
Medium chưa chạy lại sau ba bản sửa Gold keyless, oracle validate gom theo SKU và
Silver `collect_set`; kết quả OOM trong bảng là lần trước sửa.

Các số trên do planner xác nhận trong brief; receipt ở `out/` không commit,
chỉ ghi số, không dẫn link receipt. Không suy từ small/fixture sang medium,
≥100 GB hoặc GCP; không dùng thời lượng warehouse-checks làm số cải thiện index.

## Còn mở

- **Đo trên GCP:** chạy lại e2e medium sau sửa, đo skew v2, chạy thật DAG v2 +
  gửi/capture DataHub v2 bằng các lệnh trong bảng trên. Người học chọn bỏ các bước
  lâu ở local; chưa có số đo/ảnh.
- GCP fixture → scale, receipt pipeline/warehouse và EXPLAIN cùng input theo
  [checklist](gcp-checklist.md); DataHub chạy local cần receipt đúng generation GCP.
- Nguồn v2 **≥100 GB** trên GCP (dòng 5), sau nghiệm thu scale nhỏ; không đổi số
  100 GB nguồn v1 thành bằng chứng v2 hoặc cộng volume nhiều tầng.
- DE-N2 trên **bảng nghiệp vụ thật**, lịch maintenance/retention và kiểm sau chạy;
  Spark 3/3 hiện mới bao phủ bảng demo local.
- Ảnh/schema inventory bảng v2 đã deploy (dòng 34), ảnh Spark UI v2 và proof
  dimension SCD2/quan hệ dimension–fact v2 (dòng 35/37) còn thiếu. Inventory Docker
  local và overlay đã có; overlay không thay inventory runtime của VM GCP.
- DE-N1/DE-N2 là ứng viên bổ sung cho hồ sơ v1, chưa tự thay Novel 1/2 đã nộp;
  xem [Novel v2](de-rubric-matrix.md#novel-v2). Việc chọn cho scope mở rộng trong
  ledger được giữ nguyên.
