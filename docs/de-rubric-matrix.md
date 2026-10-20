# Ma trận kiểm chứng rubric DE

> Đợt rà soát mới nhất: [2026-10-15](de-audit-20261015.md), có đối chiếu event
> log Spark, ảnh query plan, docstring và liên kết tới file bị Git bỏ qua.

Cập nhật bằng chứng v2: **2026-10-20**; đợt kiểm chứng v1 giữ ngày 2026-10-15. Nguồn tiêu chí là sheet `de` trong
`EDAI K11 - MLE.xlsx` do khóa học cung cấp (không phân phối trong repo). Ma trận
này tách đủ 38 dòng có điểm thành yêu cầu, artifact/bằng chứng v1, bằng chứng v2 và việc
còn thiếu. `Có proof` nghĩa là có bằng chứng được dẫn, chưa phải điểm do giảng
viên chấm.

Cột v1 giữ bằng chứng lịch sử đúng phạm vi từng run. Cột **Bằng chứng v2**
phân biệt receipt chạy thật local với tài liệu/code/dry-run; có liên kết không
đồng nghĩa đã đóng dòng rubric. `—` là chưa có bằng chứng v2 tương ứng.
**Chưa có bằng chứng chạy GCP** cho các dòng dưới đây; inventory overlay GCP
được dựng trên máy local cũng không chứng minh đã deploy Cloud. Fixture,
scale nhỏ và nguồn ≥100 GB là ba phạm vi khác nhau. Tiến độ chi tiết và
các run còn chờ tại [bảng v2](de-order-lifecycle-status.md).

## Engineering, generator và processing

| Dòng | Điểm | Tiêu chí | Artifact / bằng chứng v1 | Bằng chứng v2 | Trạng thái / gap (v1) |
|---:|---:|---|---|---|---|
| 3 | 2 | Docker và Compose; tài liệu tối ưu image, so sánh kích thước | [`Docker.md`](Docker.md), `Dockerfile`, `docker-compose.yml`, ảnh `images/docker/docker.png` | [Docker](Docker.md), [inventory local](evidence/docker-inventory-local-20261019.json), [inventory overlay GCP](evidence/docker-inventory-gcp-20261019.json): cấu hình/image trên máy local, chưa deploy GCP | Có proof; kiểm lại ảnh còn khớp số đo khi đóng hồ sơ |
| 4 | 3 | Tối ưu Dockerfile (ví dụ multistage) | `Dockerfile` hai stage, `docker/Dockerfile.naive`, so sánh trong [`Docker.md`](Docker.md) | [So image local](evidence/docker-compare-20261019.json): cùng 27 package, naive 2,39 GB → opt 698 MB; [Docker](Docker.md) | Có proof; kiểm lại cấu hình và số đo |
| 5 | 2 | Generator mô phỏng skew; mô tả cột, tỷ lệ và volume | [`generator-design.md`](generator-design.md), [`data-quality-report-batch.md`](data-quality-report-batch.md), manifest 100 GB | [Nguồn scale v2](source-order-lifecycle-scale.md): local 60.000 đơn, 97.890 line, 1.546.066.142 byte; có Zipf, chưa ≥100 GB v2 | Có proof; detailed skew theo các nguồn nhỏ, ghi đúng phạm vi |
| 6 | 2 | Generator mô phỏng schema evolution và mô tả schema trước/sau | [`generator-design.md`](generator-design.md), [`data-quality-report-batch.md`](data-quality-report-batch.md) | [Silver v2](silver-order-lifecycle.md): fixture Amazon hai schema, Spark parse/đối soát; [DE-N1](novel-de-n1-data-contracts.md): compatibility và cổng hợp đồng, Spark 32/32 local | Có proof; đối chiếu proof Spark xử lý schema ở dòng 13 |
| 7 | 2 | Generator mô phỏng vấn đề offline khác, ví dụ duplicate | [`data-quality-report-batch.md`](data-quality-report-batch.md), generator report | [Silver v2](silver-order-lifecycle.md): test retry/conflict/revision local; [receipt](evidence/silver-v2-receipt-20261018.json) sạch có duplicate_rows_dropped=0, chưa đo tỷ lệ duplicate trên scale batch v2 | Có proof; xác nhận tỷ lệ 1,98% Amazon và 1,97% Walmart |
| 8 | 2 | Lưu dữ liệu nguồn để ingest Bronze sau | [`Docker.md`](Docker.md), [`data-quality-report-batch.md`](data-quality-report-batch.md), ảnh MinIO/DBeaver | [Bronze JDBC local](evidence/bronze-v2-jdbc-20261018.json): nguồn fixture file + walmart_lifecycle.orders, manifest=Bronze=550 (547 file + 3 JDBC); [Silver v2](silver-order-lifecycle.md) | Có proof; 100 GB là Amazon/Walmart/SKU, ba Channel khác ở nguồn nhỏ |
| 9 | 2 | Generator mô phỏng late arrivals và mô tả dữ liệu/tỷ lệ | [`data-quality-report-stream.md`](data-quality-report-stream.md), [`generator-design.md`](generator-design.md) | [Stream v2](stream-order-lifecycle.md), [receipt local](evidence/stream-v2-local-20261019.json): ba feed delivery/return/refund, late đối chiếu oracle; không thay tỷ lệ stream v1 | Có proof; Stream dùng cấu hình nhỏ, tỷ lệ tương đương được ghi rõ |
| 10 | 2 | Generator mô phỏng vấn đề streaming khác, ví dụ duplicate | [`data-quality-report-stream.md`](data-quality-report-stream.md), manifest và Kafka offsets | [Stream v2](stream-order-lifecycle.md), [receipt local](evidence/stream-v2-local-20261019.json): đo retry theo bốn topic, giữ revision, hai epoch riêng | Có proof; xác nhận tỷ lệ và đối chiếu message thực |
| 11 | 2 | Spark baseline không tối ưu | [`spark.md`](spark.md), [`docs/evidence/spark-ui/README.md`](evidence/spark-ui/README.md), ảnh Spark UI | [Spark v2](spark-order-lifecycle.md): baseline chưa đo; Đo trên GCP theo lệnh trong tài liệu | Có proof; ảnh và số đo baseline được ghi theo run |
| 12 | 3 | Spark xử lý skew, giải thích vấn đề, thử nghiệm và cải thiện so baseline | [`spark.md`](spark.md), ảnh Spark UI 3 lần chạy | [Spark v2](spark-order-lifecycle.md): salted chưa đo; Đo trên GCP theo lệnh trong tài liệu, chưa kết luận giảm skew | Có proof; nêu kết quả đo và giới hạn của AQE |
| 13 | 3 | Spark xử lý schema evolution, giải thích cách xử lý | [`silver.md`](silver.md), [`spark.md`](spark.md), generator report | [Silver v2](silver-order-lifecycle.md), [DE-N1](novel-de-n1-data-contracts.md): parse Amazon hai schema, reject/quarantine local; chưa có ảnh Spark UI v2 | Có proof trực tiếp: ảnh execution 3, Details và REST parse hai schema trong `spark.md` |
| 14 | 3 | Spark xử lý vấn đề offline khác, ví dụ duplicate | [`silver.md`](silver.md), [`data-quality-report-batch.md`](data-quality-report-batch.md) | [Silver v2](silver-order-lifecycle.md): retry trùng y bị bỏ, conflict chặn, revision/lịch sử giữ lại; [receipt sạch local](evidence/silver-v2-receipt-20261018.json), không dùng latest snapshot thay lifecycle | Có proof execution 3: 8.449.302 → 8.314.312 dòng; chênh tỷ lệ với generator được giải thích theo cửa sổ |
| 15 | 2 | Spark job được tích hợp vào data pipelines | [`spark.md`](spark.md), [`airflow.md`](airflow.md), Graph/Grid DP1–DP3 | [Airflow v2](airflow-order-lifecycle.md): Spark task trong DAG v2, DagBag local 10 DAG / 0 lỗi; chạy thật DAG chưa chạy; Đo trên GCP theo runbook, xem [trạng thái](de-order-lifecycle-status.md) | Có proof; runtime sau bootstrap đã kiểm 2/2 task success ngày 2026-10-15 |
| 16 | 2 | Flink baseline không tối ưu | [`flink.md`](flink.md), ảnh overview | [Stream v2](stream-order-lifecycle.md), [receipt local](evidence/stream-v2-local-20261019.json): baseline bounded theo topic/epoch/partition/offset, chưa có ảnh UI v2 | Có proof; kiểm lại ảnh và kết quả theo run |
| 17 | 3 | Flink xử lý late arrival, giải thích và có ảnh UI | [`flink.md`](flink.md), `images/flink/flink-late-arrival.png` | [Stream v2](stream-order-lifecycle.md), [receipt local](evidence/stream-v2-local-20261019.json): main + late = input; late proof một partition khớp oracle, không chứng minh official label được sửa | Có proof; kiểm tra ảnh/ID job còn ghi đúng |
| 18 | 3 | Flink xử lý vấn đề streaming khác, giải thích và có bằng chứng | [`flink.md`](flink.md), ảnh dedup/state và REST plan/metrics | [Stream v2](stream-order-lifecycle.md), [receipt local](evidence/stream-v2-local-20261019.json): dedup giữ revision, restart TaskManager/restore checkpoint, kept 15.261 = oracle; không phải HA | Có proof; kiểm tra bằng chứng state và số đo |
| 19 | 2 | Flink window processing, code và giải thích | [`flink.md`](flink.md), `images/flink/flink-windows.png` | [Stream v2](stream-order-lifecycle.md), [receipt local](evidence/stream-v2-local-20261019.json): TUMBLE ngày UTC khớp oracle; đây là đếm sự kiện, không phải headline sales | Có proof; có TUMBLE/HOP và kết quả cửa sổ |

## Storage và Airflow

| Dòng | Điểm | Tiêu chí | Artifact / bằng chứng v1 | Bằng chứng v2 | Trạng thái / gap (v1) |
|---:|---:|---|---|---|---|
| 20 | 3 | Tối ưu lakehouse bằng compaction, z-order hoặc partitioning; phân tích trước/sau | [`storage.md`](storage.md), Iceberg compaction 187 → 181 file và truy vấn 7 ngày | [DE-N2](novel-de-n2-iceberg-maintenance.md), [receipt local](evidence/de-n2-maintenance-local-20261018.json): Spark 3/3 trên bảng demo, compact/zorder giữ dữ liệu/tag; chưa chạy bảng nghiệp vụ thật/GCP | Có proof compaction; z-order chưa đo, không cần nếu proof compaction đóng đủ tiêu chí |
| 21 | 3 | Tối ưu warehouse, ví dụ indexing | [`storage.md`](storage.md), DDL và EXPLAIN trước/sau index | [Warehouse v2](warehouse-order-lifecycle.md): publish/rollback/index; warehouse-checks local PASS 116 s; [e2e small](e2e-order-lifecycle-local.md) PASS, chưa kết luận lợi ích index scale/GCP (xem [số planner](#kết-quả-local-do-planner-xác-nhận-ngày-2026-10-19)) | Có proof; kiểm lại số đo và phạm vi |
| 22 | 2 | DP1 ingest vào Bronze | [`airflow.md`](airflow.md), ảnh Graph/Grid DP1 10/10 task, DAG | [Bronze local](evidence/bronze-v2-jdbc-20261018.json), [CDC v2 local](evidence/cdc-v2-local-20261019.json): ingest thật trong harness; [DAG v2](airflow-order-lifecycle.md) mới kiểm import, live chưa chạy; Đo trên GCP theo runbook | Có proof lịch sử; lookup Connection/Variable và Spark operator đã kiểm trong run `de_runtime_20261015` |
| 23 | 2 | DP1 validate | [`airflow.md`](airflow.md), log Validate và ảnh Grid | [CDC v2](cdc-order-lifecycle.md), [receipt local](evidence/cdc-v2-local-20261019.json): oracle insert/update/delete 3=3; tiêm extra bị chặn đúng kỳ vọng; chưa là validate qua DAG v2 | Có proof lịch sử; runtime dùng chung đã kiểm 2/2 success, không ghi bảng |
| 24 | 2 | DP2 ingest Bronze → Silver/Gold | [`airflow.md`](airflow.md), ảnh Graph/Grid DP2 14/14 task, DAG | [Silver](silver-order-lifecycle.md), [Gold](gold-order-lifecycle.md), [e2e local](e2e-order-lifecycle-local.md): small PASS sau sửa OOM; chưa phải run Airflow v2 (xem [số planner](#kết-quả-local-do-planner-xác-nhận-ngày-2026-10-19)) | Có proof lịch sử; runtime dùng chung đã kiểm 2/2 success, không ghi bảng |
| 25 | 2 | DP2 validate | [`airflow.md`](airflow.md), log Validate Silver/Gold/SCD2 và ảnh Grid | [Silver receipt local](evidence/silver-v2-receipt-20261018.json): 19/19 check=0; [Gold](gold-order-lifecycle.md)/[e2e small](e2e-order-lifecycle-local.md): mọi check=0; chưa có receipt live DAG v2 | Có proof lịch sử; runtime dùng chung đã kiểm 2/2 success, không ghi bảng |
| 26 | 2 | DP3 ingest offline feature | [`airflow.md`](airflow.md), ảnh Graph/Grid DP3, DAG | [Gold v2](gold-order-lifecycle.md): label observable, feature PIT từ Silver history/segments; equivalence fact/nhãn/feature diff=0 fixture và nguồn scale 60 ngày trước/sau đổi tên; feature PIT có tín hiệu sau khi generator phát coverage theo tuần ([ADR-0013](adr/0013-weekly-contiguous-coverage.md)): ca Gold 60 ngày có **131 line** `labeled_count_90d > 0`, Spark khớp Python thuần (2026-10-20) | Có proof lịch sử; runtime dùng chung đã kiểm 2/2 success, không ghi bảng |
| 27 | 2 | DP3 validate | [`airflow.md`](airflow.md), feature row count và validation log | [Gold v2](gold-order-lifecycle.md): L01–L17, Spark fixture 5/5; validate PIT độc lập và e2e small check=0 local; live DAG chưa có kết quả | Có proof lịch sử; runtime dùng chung đã kiểm 2/2 success, không ghi bảng |

## DataHub, schema, tài liệu và novel ideas

| Dòng | Điểm | Tiêu chí | Artifact / bằng chứng v1 | Bằng chứng v2 | Trạng thái / gap (v1) |
|---:|---:|---|---|---|---|
| 28 | 2 | DP1 lineage giữa pipeline và bảng trên DataHub | [`datahub.md`](datahub.md), ảnh DP1 Lineage, manifest validation | [DataHub v2](datahub-order-lifecycle.md): mapping nguồn/Bronze/Silver và dry-run MCP; chưa có proof lineage live v2, Đo trên GCP theo runbook | Có proof; rà ảnh/manifest trước khi chốt |
| 29 | 2 | DP1 data validation trên DataHub | [`datahub.md`](datahub.md), ảnh DP1 Quality, assertion SUCCESS | [DataHub v2](datahub-order-lifecycle.md): tạo assertion từ check Silver có receipt, dry-run; chưa có proof Quality live pipeline v2, Đo trên GCP theo runbook | Có proof; rà ảnh/manifest trước khi chốt |
| 30 | 2 | DP2 lineage giữa pipeline và bảng trên DataHub | [`datahub.md`](datahub.md), ảnh DP2 Lineage, manifest validation | [DataHub v2](datahub-order-lifecycle.md): mapping Silver → Gold, dry-run; chưa có proof lineage live pipeline v2, Đo trên GCP theo runbook | Có proof; rà ảnh/manifest trước khi chốt |
| 31 | 2 | DP2 data validation trên DataHub | [`datahub.md`](datahub.md), ảnh DP2 Quality, assertion SUCCESS | [DataHub v2](datahub-order-lifecycle.md): receipt Gold/Validate → assertion, dry-run; chưa có proof Quality live pipeline v2, Đo trên GCP theo runbook | Có proof; rà ảnh/manifest trước khi chốt |
| 32 | 2 | DP3 lineage giữa pipeline và bảng trên DataHub | [`datahub.md`](datahub.md), ảnh DP3 Lineage, manifest validation | [DataHub v2](datahub-order-lifecycle.md): lineage feature/label nằm trong build_gold của dp_order_lifecycle, dry-run; chưa có proof live, không phải DP3 Feast | Có proof; rà ảnh/manifest trước khi chốt |
| 33 | 2 | DP3 data validation trên DataHub | [`datahub.md`](datahub.md), ảnh DP3 Quality, assertion SUCCESS | [DataHub v2](datahub-order-lifecycle.md): check PIT/nhãn từ receipt → assertion, dry-run; chưa có proof live, không dùng assertion DP3 cũ thay thế | Có proof; rà ảnh/manifest trước khi chốt |
| 34 | 2 | Hình hóa các bảng trong mọi zone, mô tả vai trò | [`schema.md`](schema.md), ảnh schema zones và DBeaver | [ERD v2](diagrams/schema-order-lifecycle.md), [DataHub v2](datahub-order-lifecycle.md): tài liệu grain/quan hệ và schema từ receipt; chưa có inventory/ảnh toàn bộ bảng đã deploy v2 | Có proof; kiểm tra ảnh/quan hệ bao phủ đúng các bảng nộp |
| 35 | 2 | Dimension SCD2 có valid_from_ts, valid_to_ts, is_current | [`schema.md`](schema.md), [`gold.md`](gold.md), DESCRIBE và validation | — | Có proof; rà cấu trúc/schema bằng chứng |
| 36 | 2 | Feature tables có event_timestamp và created | [`schema.md`](schema.md), DESCRIBE feature | [Gold v2](gold-order-lifecycle.md), [Warehouse v2](warehouse-order-lifecycle.md): feature có event_timestamp và created, đã kiểm fixture/e2e small local; chưa deploy Feast/GCP | Có proof; rà cấu trúc/schema bằng chứng |
| 37 | 2 | Quan hệ dimension và fact | [`schema.md`](schema.md), ảnh ER DBeaver, kiểm tra khóa fact/SCD2 | — | Có proof; rà ảnh thể hiện hai bảng đúng |
| 38 | 2 | Naming convention cho Bronze/Silver/Gold | [Thuật ngữ](terminology.md), [`schema.md`](schema.md) | [Silver v2](silver-order-lifecycle.md), [Gold v2](gold-order-lifecycle.md), [ERD v2](diagrams/schema-order-lifecycle.md): namespace bronze/silver/gold với tiền tố lifecycle_, raw_/stg_/fact_/feat_; chưa chứng minh dimension SCD2 v2 | Có proof; đối chiếu tên bảng thực tế và quy ước |
| 39 | 10 | Novel idea 1, tài liệu và proof chạy được | [`novel-ideas.md`](novel-ideas.md), [`trino.md`](trino.md), ảnh và kết quả federation | — | Có proof; tài liệu Novel dẫn trực tiếp tới cấu hình và số đo công khai |
| 40 | 10 | Novel idea 2, tài liệu và proof chạy được | [`novel-ideas.md`](novel-ideas.md), [`cdc.md`](cdc.md), Kafka UI, Airflow và REST evidence | [CDC v2](cdc-order-lifecycle.md), [receipt local](evidence/cdc-v2-local-20261019.json): Walmart v2 insert/update/delete + ca lỗi; bổ sung nhánh CDC, chưa tự thay Novel 2 đã nộp bằng DE-N2 | Có proof; rà chính sách lịch Batch, giới hạn đối chứng đã đo |

## Kết quả local do planner xác nhận ngày 2026-10-19

Các số dưới đây do planner xác nhận trong brief codex-26 (ghi chú nội bộ, không nằm trong repo). Receipt/log nằm
trong `out/`, không commit nên chỉ ghi số, không tạo liên kết tới receipt đó.
Tài liệu cơ chế và cách chạy: [Gold v2](gold-order-lifecycle.md), [e2e local](e2e-order-lifecycle-local.md),
[Warehouse v2](warehouse-order-lifecycle.md), [Spark v2](spark-order-lifecycle.md), [Airflow v2](airflow-order-lifecycle.md).

- Gold v2 từng OOM trên nguồn small **3.631 line** do nhân bản coverage keyless
  theo line. Đã sửa; **e2e small PASS 92 s** (45 ngày × 50 đơn, 100 SKU), mọi check **= 0**.
  Chi tiết số dòng/thời gian và EXPLAIN tại [e2e local](e2e-order-lifecycle-local.md).
  Kết quả small không thay bằng chứng ≥100 GB hay GCP.
- Gold-equivalence: diff fact/nhãn/feature **= 0** trên fixture và nguồn scale **60 ngày**,
  cả trước và sau đổi tên. Feature từng luôn **= 0** vì generator chỉ phát coverage một lần cuối kỳ;
  sau ADR-0013 (coverage theo tuần) ca Gold 60 ngày có **131 line** feature > 0, khớp Python thuần (2026-10-20).
- **warehouse-checks PASS, 116 s**: kiểm local publish/rollback/index theo harness **trước đổi tên**;
  không dùng thời lượng này làm số cải thiện EXPLAIN.
- **DagBag: 10 DAG / 0 lỗi**: kiểm import, chưa chứng minh chạy thành công DAG v2.
- **E2e medium chưa chạy lại sau sửa**: nguồn 500 × 60, 48.874 fact; lần trước sửa có
  Silver 2.090 s, gold_build 42 s, gold_validate OOM. Đã sửa Gold keyless, oracle
  validate gom theo SKU và Silver `collect_set`; chưa chứng minh medium PASS.
  Đo trên GCP: `bash tests/harness/run_order_lifecycle_scale.sh --profile medium --driver-memory 6g`;
  xem [e2e local](e2e-order-lifecycle-local.md) để giữ đúng phạm vi số lịch sử.
- **Skew v2 chưa đo**, chưa có so baseline/salted để đóng proof v2 dòng 11–12.
  Đo trên GCP: `python tests/spark/check_order_lifecycle_skew.py` trong runtime Spark
  đã chuẩn bị; lệnh container và lưu JSON/event log tại [Spark v2](spark-order-lifecycle.md).
- **Chạy thật DAG v2 + DataHub v2 chưa chạy**, chưa có receipt/Graph/Grid/Lineage/Quality.
  Đo trên GCP: `bash scripts/run/run_order_lifecycle_live.sh --phase all` trên host đã
  chuẩn bị theo [runbook](datahub-order-lifecycle.md#chạy-thật) và [checklist GCP](gcp-checklist.md);
  chưa có run_id, thời gian hay ảnh thực.

`datahub-assertions-v2-20261018.json` là bản assertion của nhánh CDC/DP3 cũ
(`fsds-dp1_cdc`, `fsds-dp3_features`), không phải proof chạy thật
`dp_order_lifecycle` hoặc các bảng `build_gold`; hậu tố v2 của file không đổi phạm vi đó.

## Novel v2

[Ledger](de-acceptance-ledger.md) chọn DE-N1/DE-N2 cho scope mở rộng. Trong ma trận
hồ sơ đã nộp v1, đây là **ứng viên bổ sung**, không tự thay Novel 1 (Trino
federation) / Novel 2 (CDC) ở dòng 39–40 trừ khi learner quyết định.

- **DE-N1 — data contract + cổng cách ly**: [tài liệu](novel-de-n1-data-contracts.md),
  [evidence Python fixture](evidence/de-n1-contracts-20261018.json); Spark sau sửa
  `identity_path` **32/32 local** được ghi trong tài liệu ngày 2026-10-19.
  Registry là file Git, chưa phải service schema registry hay bằng chứng scale/GCP.
- **DE-N2 — Iceberg tự bảo trì**: [tài liệu](novel-de-n2-iceberg-maintenance.md),
  [evidence local](evidence/de-n2-maintenance-local-20261018.json), Spark **3/3**;
  có trigger chính sách, retry, retention/tag và oracle sau maintenance trên bảng
  demo. Chưa chạy lịch/bảng nghiệp vụ thật/GCP. Phần Novel phải vượt yêu cầu
  compaction dòng 20, không dùng cùng ảnh compaction để tự tính hai lần điểm.

## Điều kiện chung và phạm vi

Ô A2 của sheet yêu cầu README có giới thiệu domain, mục lục, cấu trúc repo và
sơ đồ deployable units với luồng dữ liệu được gắn nhãn; tài liệu chi tiết và ảnh
được đặt/liên kết trong `docs/`; các hàm/class có docstring và file có mô tả.
Đây là yêu cầu chung, không có điểm riêng trong tổng 100. README có domain,
mục lục, repo structure và sơ đồ; docstring và liên kết tài liệu được kiểm
lại sau khi đóng ba việc ngày 2026-10-15.

Manifest `/data/raw/run_manifest.json` ghi 100.012.790.412 byte. Con số này là
raw Batch Amazon/Walmart/SKU, không phải lượng từng DAG đã xử lý. Mỗi proof phải
giữ đúng phạm vi ghi trong tài liệu của lần chạy tương ứng.

## Trạng thái v1 sau khi đóng các phát hiện

Ba việc trong [rà soát ngày 2026-10-15](de-audit-20261015.md) đã được bổ sung
proof: ảnh/REST Spark, Airflow runtime và tài liệu công khai. Không còn gap
bắt buộc nào được xác định trong lượt rà soát v1 ngày 2026-10-15; các gap v2
vẫn theo cột v2 và [tiến độ](de-order-lifecycle-status.md); kết luận không phải điểm
100/100 do người chấm xác nhận. Bản nộp phải bao gồm các DAG, job, tài liệu và
ảnh mới hiện còn untracked; không kèm dữ liệu nguồn 100 GB hoặc credential.

Giới hạn giữ nguyên: pipeline Airflow chưa xử lý hết 100 GB, ba Channel dùng
nguồn Batch nhỏ, DataHub publish là thao tác riêng. Những giới hạn đó được
nêu tại [tiến độ](de-progress.md) và tài liệu thành phần, không bị xóa khỏi
hồ sơ khi đóng proof.
