# Checklist sẵn sàng triển khai DE lên GCP — 2026-10-20

**Kết luận: sẵn sàng về bộ mã/cấu hình để chuẩn bị một đợt thử batch trên
GCP; chưa đủ điều kiện xác nhận triển khai toàn bộ DE hoặc chạy ≥100 GB.**
Chỉ bắt đầu fixture khi các mục trước triển khai bên dưới được đóng.
Medium, skew và các bằng chứng Cloud có thể đo trong đợt GCP, không bắt buộc
chạy lại workload lâu ở local trước khi thử fixture.

Nguồn rà soát: đọc trực tiếp sheet visible `de` trong
`edai-k11-cw/rubic/EDAI K11 - MLE.xlsx`: **38 dòng có điểm, tổng 100**,
cộng yêu cầu tài liệu ở A2. Đã đối chiếu các tiêu chí và điểm tương ứng
với sheet đầu của `EDAI K11 - DE.xlsx`. Phạm vi đánh giá là lifecycle v2;
bằng chứng v1 giữ giá trị trong phạm vi dataset/runtime cũ, không tự chuyển
thành bằng chứng v2/GCP. Đây là checklist kỹ thuật, không phải điểm tự chấm.

## 1. Trước khi bật VM

| Đóng? | Việc cần làm | Điều kiện đạt / hiện trạng |
|---|---|---|
| [x] | Ghi rõ phạm vi đợt đầu | Runbook đã ghi batch: GCS/JDBC → Bronze → Silver → Gold → PostgreSQL. Overlay tắt Kafka, Flink, Debezium và Trino; DataHub chạy riêng. |
| [ ] | Kiểm project, Billing, credit/hạn, quota, quyền và ADC | Người học xác nhận theo bước 0 của [checklist chạy](gcp-checklist.md). Lượt rà soát này không truy cập tài khoản Cloud. |
| [x] | Kiểm công cụ và cú pháp offline | Có Terraform, gcloud; Ansible core 2.21.5 chạy qua `uvx`, collection `ansible.posix` 2.2.2 tại `/tmp/fsds-ansible-collections`. Syntax-check đạt bằng inventory mẫu; chưa cài lệnh Ansible global trong PATH. Lệnh tái tạo ở [runtime](gcp-runtime.md#package-identity-và-các-điểm-planner-phải-kiểm). |
| [x] | Hoàn tất init/validate Terraform | Đã init `-backend=false -lockfile=readonly`, tải Google provider 6.49.0; fmt và validate đạt. Không cần credential để kiểm này. |
| [ ] | Đọc plan với project thực | Người học kiểm đúng 1 VM, 2 bucket và disk dự kiến; chưa chạy plan/apply. Validate offline không thay bước này. |
| [ ] | Điền cấu hình và bảo vệ secret | Điền tfvars/inventory/Vault theo outputs thực; PostgreSQL password, Lakekeeper key, OS Login/IAP đúng. Không cần gửi secret cho trợ lý. |
| [ ] | Chốt cấu hình tài nguyên theo từng bài chạy | Mặc định `e2-standard-8`, boot 50 GiB, data 200 GiB; worker 2 × 7g, Airflow 6g. Chạy một workload Spark mỗi lúc. Harness medium dùng driver 6g riêng; không tự đưa cấu hình đó vào container Airflow giới hạn 6g. |
| [ ] | Giữ bản mã triển khai có thể truy vết | Nhiều file đang modified/untracked, gồm overlay và `infra/`. Ghi commit cộng bản diff/manifest hoặc snapshot đầy đủ; bảo đảm bản bàn giao chứa các file mới. Ansible hiện đồng bộ working tree bằng rsync. |
| [ ] | Chốt dự toán, nơi giữ bằng chứng và backup | Dự toán cả VM, disk, GCS và thời gian giữ dữ liệu; kiểm dung lượng trống. Có cách backup DB/catalog, lấy receipt về, stop/start và kiểm `TERMINATED`. |

Runbook hiện có: [hạ tầng](gcp-deploy.md), [runtime](gcp-runtime.md),
[các bước thao tác](gcp-checklist.md). Không thực hiện thao tác Cloud trong
lượt rà soát này.

## 2. Đối chiếu đủ phạm vi rubric DE

“Có local” dưới đây là mức bằng chứng được ghi trong repo; không đồng nghĩa
đã chạy lại các workload đó trong lần rà soát này.

| Dòng / điểm | Nhóm cần kiểm | Đã có | Còn cần để đóng v2 / chứng minh GCP |
|---|---|---|---|
| 3–4 / 5đ | Docker/Compose và tối ưu image | Inventory local, multistage; so cùng 27 package: 2,39 GB → 698 MB | Inventory container/image thực trên VM và bản cấu hình đang chạy. Inventory overlay thu local chưa phải Cloud. |
| 5–8 / 8đ | Nguồn offline, skew, evolution, duplicate, lưu nguồn | Fixture 5 Channel; nguồn 60.000 đơn ~1,55 GB; ingest file/JDBC local | Raw v2 ≥100 GB sau các gate nhỏ; đo skew, cardinality, duplicate và schema trước/sau trên nguồn thật; manifest/path GCS. Không cộng Bronze/Silver/Gold để tính raw volume. |
| 9–10 / 4đ | Nguồn stream late/duplicate | Harness local có oracle | Nếu mục tiêu toàn bộ DE trên GCP: bổ sung kế hoạch runtime Kafka/Flink và bằng chứng nguồn thực; overlay hiện chưa chạy nhánh này. |
| 11–12 / 5đ | Spark baseline và xử lý skew | Có code benchmark/salting | Chưa đo v2. Cùng input/config, lưu event log, Spark UI, task/shuffle metrics, kiểm bag equality hai chiều = 0; kết quả inconclusive chưa chứng minh cải thiện. |
| 13–15 / 8đ | Spark evolution/dedup/tích hợp pipeline | Test/receipt local; DAG import được | Run DAG thật, ảnh/log diễn giải schema/duplicate, kết quả đối soát và shared Connection/Variable. |
| 16–19 / 10đ | Flink baseline, late, dedup, window | Local hai epoch/restart và oracle | Cần runtime, ảnh Flink UI, offsets/checkpoint, kết quả trước/sau nếu đưa nhánh stream lên GCP. Restart local không chứng minh HA. |
| 20–21 / 6đ | Lakehouse và warehouse optimization | Maintenance trên bảng demo; PostgreSQL local và EXPLAIN small | Maintenance bảng nghiệp vụ thật; EXPLAIN trước/sau cùng dữ liệu có kết quả. Small Q1 trả 0 dòng và Q2 không cải thiện nên chưa đủ kết luận index scale. |
| 22–27 / 12đ | DP1/DP2/DP3 ingest và validate | Có batch/CDC/warehouse DAG; e2e small pass theo tài liệu | Live Graph/Grid, receipts và validation từng bước. Rubric yêu cầu ảnh 3 pipeline DP1/DP2/DP3; lifecycle đang gộp feature vào `build_gold`. Cần chứng minh mapping và cấu trúc đáp ứng, hoặc tách pipeline; không mặc định warehouse DAG thay DP3. |
| 28–33 / 12đ | DataHub lineage và validation của DP1/2/3 | Publisher và MCP dry-run | Publish/capture thật từ receipt đúng generation GCP; đủ lineage/quality của các nhóm pipeline. DataHub có thể chạy local, nhưng metadata phải trỏ nguồn GCS/JDBC đúng. |
| 34 / 2đ | Bảng và vai trò ở mọi zone | ERD thiết kế | Inventory/schema và ảnh các bảng thực đã deploy, kèm vai trò/grain. |
| 35, 37 / 4đ | Dimension SCD2 và quan hệ dim–fact | Có proof lịch sử v1; ma trận chưa có proof v2 | Cần bảng dim v2, `valid_from_ts`, `valid_to_ts`, `is_current`, kiểm interval/current và quan hệ fact; không coi label history là proof SCD2 thay thế. |
| 36, 38 / 4đ | Feature clocks và naming | Local có `event_timestamp`, `created`; naming được mô tả | Đối chiếu schema thực sau deploy và ảnh/bằng chứng feature, tên bảng từng zone. DE yêu cầu feature table; không tự thêm điều kiện phải deploy Feast chỉ để đóng các dòng này. |
| 39 / 10đ | Novel 1 — DE-N1 scope mở rộng | Contract gate/quarantine, Spark 32/32 local theo tài liệu; registry file Git | Proof run thật, reject/quarantine và compatibility; mô tả đúng registry file, không gọi là service registry đã deploy. |
| 40 / 10đ | Novel 2 — DE-N2 scope mở rộng | Spark 3/3 local trên bảng demo | Bảng nghiệp vụ thật, lịch/trigger, retention, retry/failure và hậu kiểm. Chứng minh phần vượt compaction dòng 20; tránh dùng một ảnh để tự tính hai mục. |
| A2 / chung | README, diagram, tài liệu, ảnh, docstring | Có cấu trúc tài liệu | Cập nhật sơ đồ GCP đúng deployable units/luồng dữ liệu; link ảnh có giải thích; phân biệt local, Cloud, fixture, scale và lịch sử v1. |

Chi tiết từng dòng tại [ma trận](de-rubric-matrix.md), lựa chọn Novel và
quy cách proof tại [ledger](de-acceptance-ledger.md). Không tự đổi lựa chọn
Novel của hồ sơ v1 đã nộp.

## 3. Thứ tự kiểm chứng trên GCP

- [ ] **Runtime smoke:** Ansible failed/unreachable = 0; Spark đọc/ghi Iceberg
  trên GCS đúng; warehouse/catalog đúng bucket; Airflow nhận DAG, scheduler healthy.
- [ ] **Fixture qua DAG thật:** pipeline 5 task và warehouse 2 task xanh;
  receipt đúng run/cutoff, Bronze/JDBC khớp manifest, Silver/Gold mọi check = 0,
  quarantine = 0, warehouse checks = true. Số fixture chi tiết ở checklist chạy.
- [ ] **Lỗi và chạy lại:** kiểm publication bị chặn khi validation fail;
  dùng generation mới khi Gold đã append, giữ receipt lỗi. Run xanh đơn lẻ
  chưa chứng minh retry/recovery.
- [ ] **Medium sau sửa OOM:** chạy lại, kiểm dữ liệu và ghi RAM/thời gian thực.
  Trạng thái hiện hành chưa có medium PASS sau sửa; small PASS 92 giây không
  thay kết quả này. Dừng tăng scale nếu còn OOM hoặc reconcile lỗi.
- [ ] **Nguồn scale nhỏ:** giữ manifest/dataset/cutoff và đối soát count/key/money
  từ GCS/JDBC đến warehouse; đo EXPLAIN 5 lần trên cùng input, có dòng kết quả.
- [ ] **Skew:** chạy benchmark baseline/salted, lưu JSON, event log và ảnh;
  kiểm feature có tín hiệu, equality và stage metrics trước khi kết luận.
- [ ] **DE-N1/N2:** lưu proof gate/quarantine, maintenance bảng thật và hậu kiểm;
  không chạy maintenance song song writer.
- [ ] **DataHub và schema:** publish receipts/schema đúng run GCP, chụp lineage,
  quality và inventory thực; hoàn thiện SCD2/quan hệ dim–fact và mapping DP1/2/3.
- [ ] **Full-scale ≥100 GB:** chỉ sau các bước nhỏ đạt và đo lại nhu cầu disk,
  RAM, runtime, chi phí; nguồn 1,55 GB hiện có chưa đóng gate này.
- [ ] **Stream nếu triển khai toàn bộ DE:** chuẩn bị đợt riêng cho Kafka/Flink
  và CDC nếu giữ trong kiến trúc; kiểm late/dedup/window/replay theo oracle.
- [ ] **Lưu bằng chứng, backup, tắt VM:** archive mở được, không có secret;
  ghi giờ chạy/chi phí và xác nhận trạng thái VM `TERMINATED`.

## 4. Các điểm hướng dẫn đã xử lý và giới hạn còn lại

1. `scripts/run/run_order_lifecycle_live.sh --phase all` hiện sinh fixture local,
   bật MinIO và trigger với `/data/order_lifecycle`; publisher cũng hardcode
   source root local. **Đã bổ sung hướng dẫn riêng** tại [runtime mục 7](gcp-runtime.md#7-publish-datahub-từ-receipt-gcp):
   dùng trigger GCS/JDBC trong checklist, copy receipt/schema về local rồi
   gọi publisher với `--source-root gs://...` và `--walmart-jdbc` khớp run.
   Script local không được chuyển thành runner GCP trong đợt sửa này.
2. Bước DE-N2 ghi `/opt/airflow/receipts/order_lifecycle/de-n2-scale`, tương ứng
   `/mnt/fsds-data/airflow/receipts/order_lifecycle/de-n2-scale` trên host.
   **Đã sửa lệnh tar** để gom thêm plan/execute/validate DE-N2, kiểm danh sách
   archive và hướng dẫn giải nén ở máy cá nhân. Đã nhắc giữ `schemas.json`,
   thu event log/skew report và log Bronze riêng trước khi bàn giao.
3. Kho dữ liệu mặc định data disk 200 GiB chưa có số đo chứng minh chứa được
   nguồn local ≥100 GB đồng thời với DB, shuffle, log và các file phát sinh.
   Đây là điều kiện cần đo/dự toán trước full-scale, chưa phải lỗi fixture.

## 5. Kiểm tra thực hiện trong lượt rà soát

- Đọc workbook OOXML, rubric matrix, ledger, tiến độ, runbook, Terraform,
  Ansible, overlay và script live. Không đọc/xuất credential.
- `pytest -q tests/unit/infra/gcp/test_terraform.py
  tests/unit/spark_jobs/lib/test_iceberg_session.py`: **33 passed**.
- Terraform `init -backend=false -input=false -lockfile=readonly`,
  `fmt -check` và `validate`: **đạt**; provider Google 6.49.0.
- Ansible core 2.21.5 + `ansible.posix` 2.2.2: **syntax-check đạt** bằng
  inventory mẫu. Có cảnh báo `apt_repository` sẽ bỏ ở core 2.25; chưa chặn
  phiên bản đang kiểm. Chưa thực thi playbook trên VM.
- Compose overlay `config --quiet`: **đạt**, dùng `.env.example` và tên bucket
  giả cùng chuỗi package GCP để kiểm; không khởi động container.
- Hướng dẫn archive DE-N2, publish DataHub từ receipt GCP và link README đã sửa.
- Không chạy lại Spark/Flink/DAG, không kiểm Billing/quota/IAM thật và không
  tạo tài nguyên Cloud; chưa plan/apply. Các số local lịch sử dẫn từ tài liệu hiện hành tại
  [bảng tiến độ](de-order-lifecycle-status.md).
