# Gap phần DE theo rubric và scope đã chốt — 2026-10-18

Đối chiếu **chỉ đọc** tại `/home/phattl/eonsr/FSDS`; chỉ tạo báo cáo này và
[rubric-links.md](rubric-links.md). Không sửa code/config/workbook, không
khởi động service, chạy pipeline/test/build, thao tác Cloud hoặc commit.
HEAD tham chiếu: `60604c3c1aabaa42b35e48d92e8261c0facfd589`; working tree
có thay đổi chưa commit, nên kết luận áp dụng cho files hiện có, không chỉ HEAD.

Nguồn tiêu chí: `edai-k11-cw/rubic/EDAI K11 - MLE.xlsx`, sheet **visible `de`**,
dòng Excel **3–40**, cột **E**. Đã đọc trực tiếp OOXML/shared strings/merged
cells bằng thư viện chuẩn Python: **38 dòng, tổng 100 điểm**. Workbook SHA-256:
`ef5a69e8621b2959c998dd061616f6ce69a06adc2867519751fb4f2a5845683f`.
Các yêu cầu Proof ở cột D được áp dụng cho toàn vùng ô gộp tương ứng;
ô D trống bên trong vùng gộp không có nghĩa dòng đó không cần proof.

## Cách đọc và cơ sở ước lượng

Hai điều cần phân biệt: **đáp ứng rubric DE gốc** và **đáp ứng scope dự án
hiện hành** theo [ledger](de-acceptance-ledger.md), [plan](high-level-architecture-plan.md),
[scope Q52–Q61](expanded-scope-design.md), [nguồn v2](source-contract-order-lifecycle.md),
[nhãn v2](return-label-contract.md) và [publication](run-publication-contract.md).
Ví dụ rubric yêu cầu ≥100 GB offline, không yêu cầu riêng từng Channel phải
có100 GB; mục tiêu bộ nguồn chung năm Channel là scope dự án đã chọn.

- **K**: code còn thể hiện cơ chế của proof, không tìm thấy mâu thuẫn tĩnh
  trong phạm vi đã đọc. Không có nghĩa xác nhận byte-identical với code lúc
  chạy hoặc runtime hiện tại: phần lớn proof cũ không pin code/image digest.
- **P**: proof chỉ khớp một phần hoặc một lát dữ liệu; cần bổ sung cho claim hiện tại.
- **L**: proof trước thay đổi code/interface; giữ làm lịch sử, chưa chứng minh code mới.
- **N**: chưa có proof cho cách đáp ứng đã chốt trong ledger.

Cột Q dùng **≠** khi không được tiếp tục dùng claim/ý nghĩa cũ cho scope mới;
**Δ** khi proof cũ vẫn đúng trong phạm vi ghi nhận nhưng không bao phủ phần
mở rộng; **—** khi quyết định mới không tự làm sai tiêu chí/proof đó. Đây
không phải tuyên bố ảnh/log lịch sử bị sai hoặc điểm đã bị người chấm trừ.

Ước giờ là **giờ công kỹ thuật** của một người quen repo, gồm sửa phần thiếu,
kiểm nhỏ và lưu proof; tính cho mục tiêu scope đã chốt. Các dòng phụ thuộc
nguồn/schema mới giả định đầu vào nhỏ đã sẵn sàng; công sinh nguồn v2 nằm
trong gói dùng chung ở cuối. Các dòng chia sẻ DAG/run/source/ảnh nên **không
cộng máy móc các giờ từng dòng**. Chưa bao gồm thời gian người học học lại,
chờ full-scale100 GB, Cloud/quota, MLE/UI/agent hoặc triển khai production.
Đây là ước lượng từ độ rộng thay đổi, không phải benchmark hay cam kết lịch.

## Bảng đủ 38 dòng

| Dòng | Điểm | Mục rubric | Proof hiện có, đường dẫn | Còn khớp code hiện tại? | Q52–Q61 | Việc thiếu để dùng cho scope hiện hành | Ước giờ |
|---:|---:|---|---|---|---|---|---:|
| 3 | 2 | Docker & Compose | [Docker](Docker.md), [ảnh size](images/docker/docker.png), [Compose](../docker-compose.yml) | **K/P**: có Compose và Dockerfile; số677 MB là phép đo image cũ, không có digest gắn source hiện nay | **—**: thêm feed không phủ định việc dùng Docker | Inventory image/service/profile đang nộp, pin build/input; đo lại size nếu trình bày là image hiện tại. Không cần optimize mọi service để chứng minh dòng3 | 1–2 |
| 4 | 3 | Optimize Dockerfile | [Dockerfile](../Dockerfile), [naive](../docker/Dockerfile.naive), [so sánh](Docker.md#2-tối-ưu-dockerfile) | **K/P**: multistage vẫn có; naive dùng pip không lock, optimized dùng uv.lock, nên claim “cùng thư viện” chưa được chứng minh | **—**: không tự mất proof multistage | So cùng dependency set/code/runtime, ghi image digest/size trước–sau; cập nhật phép đo cũ2,35 GB→677 MB cho build được chọn | 2–4 |
| 5 | 2 | Simulate skew; nhóm offline ≥100 GB | [Batch DQ](data-quality-report-batch.md), [manifest100 GB](evidence/batch-100gb-manifest.json), [generator](generator-design.md) | **P/L**: số100.012.790.412 byte thuộc source cũ Amazon/Walmart/SKU; generator hiện thêm renderers và identity guard | **Δ Q52/54/61**: chưa đo distribution/volume nguồn lifecycle/stock mới | Dataset/version chung, skew/cardinality/count/bytes từng feed và5 Channel; không gắn số cũ cho source v2. Giữ proof ≥100 GB cũ đúng phạm vi | 4–8 |
| 6 | 2 | Simulate schema evolution | [Batch DQ](data-quality-report-batch.md#schema-evolution), [Amazon renderer](../generator/channels/amazon.py), [Silver](silver.md) | **K/P**: hai dạng Amazon vẫn có; chưa có receipt schema/compatibility của feeds mới | **Δ Q52/53/61** | Fixture trước/sau schema, emitted payload/digest và expected map/reject cho nguồn mới; không bắt native Orders có fields operational giả | 3–6 |
| 7 | 2 | Offline problem khác: duplicate | [Batch DQ](data-quality-report-batch.md#duplicate-2), [exports](../generator/delivery/batch_exports.py), [resume tests](../tests/unit/generator/test_generate_order_snapshots_resume.py) | **K/P**: placement duplicate còn; test resume không phải proof duplicate rate của dataset mới | **Δ Q52/61**: operation/revision identity mới chưa được phủ | Đo duplicate theo native key/event ID, retry/correction/hash conflict; giữ event hợp lệ khác revision, không dedup theo SKU/status | 3–5 |
| 8 | 2 | Lưu nguồn sau generate | [Docker](Docker.md), [Batch DQ](data-quality-report-batch.md), [source boundary](channel-sources.md) | **K/P**: exports/MinIO/PG tồn tại trong proof; không xác minh service/storage hiện còn dữ liệu; root cũ thiếu identity không resume bằng code mới | **Δ Q61** | Feed manifests/checksums/coverage, immutable roots và lưu source mới để replay; giữ Walmart JDBC khác bootstrap JSONL; không restart root evidence cũ | 4–8 |
| 9 | 2 | Simulate late arrivals | [Stream DQ](data-quality-report-stream.md), [stream](../generator/publish_order_snapshot_events.py), [evidence script](../scripts/evidence/measure_order_snapshots_stream.py) | **K/P**: median5,27 phút/4,98%>1h thuộc Amazon notifications; code/manifest chỉ phủ hai stream chính | **Δ Q52/56/57/61** | Late observation/correction/delivery/approval/refund với event time và known-time riêng, tail/closeout; receipt theo feed. Stream5 Channel còn là scope Q40 từ trước | 4–8 |
| 10 | 2 | Streaming problem khác: duplicate | [Stream DQ](data-quality-report-stream.md#duplicate), [Flink state](evidence/flink-dedup-state.json) | **K/P**:93.554→92.157 webhook là proof TikTok cũ, không phải các operational events mới | **Δ Q52/61** | Stable event identity xuyên retry, không nuốt status transitions/revisions; đo rate/offsets từng stream, reset/replay có epoch | 3–6 |
| 11 | 2 | Spark baseline | [Spark](spark.md), [UI index](evidence/spark-ui/README.md), [baseline job](../spark_jobs/bronze/ingest_amazon_order_snapshots.py) | **K/P**: baseline/flags và ảnh có; run20/180 ngày khác run DP1/nguồn mới, không có full source-image attestation | **Δ Q61**: workload mới cần baseline riêng, baseline cũ vẫn dùng được cho rubric | Pin input/runtime/config, lưu query plan và logical output oracle cho baseline mới; giữ nhãn scope lịch sử | 3–6 |
| 12 | 3 | Spark handle skew + explanation | [Spark §5f](spark.md#f-ba-phương-pháp-đã-thử-và-bảng-trướcsau-đo-2026-10-12), [UI index](evidence/spark-ui/README.md) | **K/P**: có thử hash/none/range và task-max giảm; runtime tổng có nhiễu, không đủ claim nhanh hơn chung | **Δ Q52/54/61** | Same-input so task rows/shuffle/files và correctness với workload v2; nêu trade-off file nhỏ, repeated timings nếu claim speedup. Không coi AQE bật là đã chữa write skew | 5–10 |
| 13 | 3 | Spark handle schema evolution | [execution3](evidence/spark-ui/silver-amazon-execution-3.json), [ảnh parse](evidence/spark-ui/13-silver-amazon-schema-details.png), [Amazon Silver](../spark_jobs/silver/build_amazon_order_snapshots.py) | **K/P**: parse hai schema+union còn khớp cơ chế; proof không bao gồm schemas operational mới | **Δ Q52/53/61** | Compatibility/parse/reject/quarantine fixtures trên schema v2, lưu UI/expected data; cùng source manifest với build/validate | 4–8 |
| 14 | 3 | Spark handle offline problem khác | [dedup plan](evidence/spark-ui/12-silver-amazon-dedup-plan.png), [execution3](evidence/spark-ui/silver-amazon-execution-3.json), [Silver](silver.md) | **K/P**:8.449.302→8.314.312 đúng snapshot/dedup cũ; latest-per-line không đủ dựng lifecycle causal history | **≠ Q52/53/61** nếu coi dedup latest là giữ đủ operational history | Stable line/event/revision keys, causal pending/conflict, partial lifecycle và mapping history; duplicate/revision khác nhau; đối soát không mất sự kiện | 5–10 |
| 15 | 2 | Spark tích hợp pipeline | [Airflow](airflow.md), [runtime smoke](evidence/airflow-runtime-20261015.json), [DP1](../dags/dp1_bronze.py), [DP2](../dags/dp2_silver.py), [DP3](../dags/dp3_features.py) | **K/L**: core DAG có proof; smoke3 rows/table_writes0 không chứng minh task CDC/validator mới; CDC CLI đã đổi | **Δ Q61**; code CDC đổi độc lập Q | DAG import+Spark submit của tasks mới, run receipts/input IDs và task graph; không dùng smoke20261015 thay end-to-end hiện tại | 2–4 |
| 16 | 2 | Flink baseline | [Flink](flink.md), [overview](images/flink/flink-overview.png), [`flink_jobs/order_snapshots/baseline.sql`](../flink_jobs/order_snapshots/baseline.sql) | **K/P**: bounded Amazon→filesystem còn khớp cơ chế; không phải sink lakehouse/feature store | **Δ Q61** | Baseline của events mới khi thêm nguồn; exact offset/epoch/partition vector, count theo range. Dual-sink Feast thuộc scope phụ thuộc, không tự là rubric16 gốc | 2–4 |
| 17 | 3 | Flink handle late arrival | [late UI](images/flink/flink-late-arrival.png), [`flink_jobs/order_snapshots/late_arrival.sql`](../flink_jobs/order_snapshots/late_arrival.sql), [giải thích](flink.md#bắt-tin-đến-muộn-bằng-watermark-rubric-c17) | **K/P**: hiện chỉ tách late ra file bằng CURRENT_WATERMARK;9.143 phụ thuộc lịch watermark, không proof reconciliation metrics | **≠ Q52/56/57/61** nếu dùng “đã bắt late” làm proof official label/sales được sửa đúng | Boundary/late correction và đối soát output chính với side output; không bỏ refund/delivery muộn rồi gán0; recovery/checkpoint trên bộ nhỏ | 4–8 |
| 18 | 3 | Flink handle streaming problem khác | [state receipt](evidence/flink-dedup-state.json), [state UI](images/flink/flink-dedup-state.png), [`flink_jobs/order_snapshots/dedup_webhooks.sql`](../flink_jobs/order_snapshots/dedup_webhooks.sql), [`flink_jobs/order_snapshots/dedup_state_proof.sql`](../flink_jobs/order_snapshots/dedup_state_proof.sql) | **K/P**: keyed notification dedup có proof; bounded vài giây không chứng minh state lâu dài/restart | **Δ Q52/61** | Dedupe operation identity nhưng giữ revisions, replay/reset/retention/checkpoint và quantity balance; oracle riêng cho vận hành mới | 4–8 |
| 19 | 2 | Flink window processing | [`flink_jobs/order_snapshots/windows.sql`](../flink_jobs/order_snapshots/windows.sql), [window UI](images/flink/flink-windows.png), [Flink](flink.md) | **K** cho TUMBLE/HOP; chưa chứng minh daily official product sales | **≠ Q52/60** nếu gọi count UNPAID là headline sales; **Δ Q61** | Giữ tên phép đo “new UNPAID events”; nếu làm sales window cần cancellation basis/quantity/currency/coverage và event revision. Không dùng cửa sổ realtime thay official generation | 2–4 |
| 20 | 3 | Optimize lakehouse | [Storage](storage.md), [`spark_jobs/maintenance/inspect_iceberg_storage.py`](../spark_jobs/maintenance/inspect_iceberg_storage.py), [`spark_jobs/maintenance/compact_amazon_order_snapshots.py`](../spark_jobs/maintenance/compact_amazon_order_snapshots.py) | **K/P**: manual compaction187→181 và query logical equality có; `.files` counts không bảo đảm logical rows khi có delete files | **Δ Q52/61**: workload/tables mới chưa benchmark; generation retention còn từ Q45 | Delete-aware oracle, fixed snapshots và benchmarks workload mới; không bắt buộc z-order riêng bởi rubric ghi ví dụ lựa chọn. Không tính manual compaction là DE-N2 đã xong | 3–6 |
| 21 | 3 | Optimize warehouse/indexing | [Storage/index](storage.md#postgresql-warehouse-và-index), [`spark_jobs/warehouse/export_order_snapshot_lines.py`](../spark_jobs/warehouse/export_order_snapshot_lines.py), [Trino đối soát](trino.md) | **K/P**: index trên lát Walmart6 cột/1.875.607 dòng; export overwrite không tự tái tạo index; chưa có warehouse5 Channel/marts | **≠ Q60** nếu tổng line_net cũ được gọi headline mới; **Δ Q52/53/54/61** | Warehouse canonical theo currency/cutoff, quantity/cancel/refund riêng; index DDL áp lại sau load, explain same-input và đối soát5 Channel/phần mới | 4–8 |
| 22 | 2 | DP1 ingest | [Airflow DP1](airflow.md), [CDC lịch sử](cdc.md), [DAG CDC](../dags/dp1_cdc.py), [setup](../scripts/setup/setup_dp1_cdc.sh) | **K/L**: core10/10 task có proof; CDC6 rows chỉ bảng v1, code/setup hiện chọn v2/source_epoch/boundary | **Δ Q61**: operational feeds chưa ingest | Run nhỏ CDCv2 đúng shared boundary dir+run_id, source UUID/range/retry; Bronze riêng feeds mới. Không tự suy Airflow Variable runtime đã đổi vì setup file đã đổi | 6–12 |
| 23 | 2 | DP1 validate | [DataHub receipts](evidence/datahub-validation.json), [CDC `spark_jobs/cdc/validate_walmart_order_changes.py`](../spark_jobs/cdc/validate_walmart_order_changes.py), [boundary unit proof](evidence/audit-remediation-20261018.json) | **L/P**: CDC v2 chưa integration; unit metadata helper không chứng minh Kafka read/MERGE/validate | **Δ Q61** | Missing/extra/payload/epoch/producer growth/retention/reset fixtures; frozen file/JDBC inputs và source coverage. Core chỉ count/provenance chưa đủ mới | 6–12 |
| 24 | 2 | DP2 ingest Silver/Gold | [DP2 run14/14](airflow.md#dp2-sau-khi-thêm-scd2-amazon), [Gold](gold.md), [job `spark_jobs/gold/build_order_snapshot_lines.py`](../spark_jobs/gold/build_order_snapshot_lines.py) | **K/P**: latest snapshot Order Line facts; chưa có allocation/return/refund/stock/listing facts | **≠ Q52/53/54/58/60/61** nếu dùng fact cũ làm đủ data model hiện hành | Canonical facts độc lập chống fan-out, immutable binding/mapping history, cancel/net measures và warehouse outputs; account namespace và pinned input còn là gap nền trước Q52 | 10–18 |
| 25 | 2 | DP2 validate | [run receipts](evidence/datahub-validation.json), [Gold `spark_jobs/gold/validate_order_snapshot_lines.py`](../spark_jobs/gold/validate_order_snapshot_lines.py), [dim `spark_jobs/gold/validate_amazon_order_status_scd2.py`](../spark_jobs/gold/validate_amazon_order_status_scd2.py) | **K/P**: Gold validator vẫn SUM(record_count) từ `.files`; checks cũ không chứng minh quantity/money/causal/mapping đúng | **≠ Q52/53/54/58/60/61** đối với assertions scope mới | Logical key/value oracle, pinned snapshots, delete/correction, quantity/money/stock balance, header uncertainty, no-fan-out; fail trước publication | 8–16 |
| 26 | 2 | DP3 build offline feature | [DP3 proof](gold.md#xác-nhận-runtime), [feature job `spark_jobs/gold/build_order_snapshot_return_features.py`](../spark_jobs/gold/build_order_snapshot_return_features.py), [`spark_jobs/gold/build_walmart_order_snapshot_labels.py`](../spark_jobs/gold/build_walmart_order_snapshot_labels.py) | **K/P** cho bảng feat_; latest quantity/price có thể là sửa sau Order. `spark_jobs/gold/build_walmart_order_snapshot_labels.py` đọc ground truth v1, không là observable label v2 | **≠ Q52/53/56/57/61** nếu claim feature/nhãn hiện hành đã đúng PIT | Historical SKU+Storefront/known-time, nguồn labelv2 và revisions/maturity, giữ truth chỉ làm oracle; không coi target “request trong30d” cũ là approved/refund-success | 8–16 |
| 27 | 2 | DP3 validate | [DP3 lịch sử](evidence/datahub-validation.json), [`spark_jobs/gold/validate_order_snapshot_return_features.py`](../spark_jobs/gold/validate_order_snapshot_return_features.py), [166 Python+4 Spark record](evidence/audit-remediation-20261018.json) | **L/P**: validator đã đổi logical count/key/time;4 Spark cases là isolated DataFrames, chưa run DAG/Iceberg mới | **≠ Q56/57/61** với label/PIT claim mới; **Δ Q52/53** | Run pipeline nhỏ hiện tại, availability/value oracle, correction/maturity/unknown, delete files; clock train/feature/eval tách biệt. Đồng bộ parser log DataHub | 6–12 |
| 28 | 2 | DP1 lineage trên DataHub | [DataHub](datahub.md), [DP1 lineage UI](images/datahub/dp1-lineage.png), [publisher](../scripts/run/publish_order_snapshots_datahub.py) | **L/P**: publisher CDC constant vẫn `raw_walmart_cdc`; không theo setupv2 | **Δ Q61**: chưa graph operational feeds | Mapping/versioned dataset URNs theo actual receipts/run, graph v2 và nguồn mới; giữ graph v1 như lịch sử, không đổi tên evidence cũ thànhv2 | 3–5 |
| 29 | 2 | DP1 validation trên DataHub | [Quality DP1](images/datahub/dp1-validation.png), [15 checks manifest](evidence/datahub-validation.json), [evidence builder](../scripts/evidence/build_order_snapshots_datahub_validation.py) | **L/P**: builder hard-code v1/runs cũ; CDC parser bỏ extra/payload mismatch/epoch mới | **Δ Q61** | Receipt/schema parserv2, assert missing/extra/payload +coverage và giữ run/input identity; publish proof mới, không coi SUCCESS cũ là validationv2 | 3–5 |
| 30 | 2 | DP2 lineage trên DataHub | [DP2 lineage UI](images/datahub/dp2-lineage.png), [DataHub](datahub.md), [publisher map](../scripts/run/publish_order_snapshots_datahub.py) | **K/P** cho graph5 Silver→fact+Amazon dim; chưa graph entity/feed mới | **Δ Q52/53/54/61** | Operation/listing/inventory facts và frozen-input lineage; upstream/downstream receipts đúng dataset/version | 3–5 |
| 31 | 2 | DP2 validation trên DataHub | [DP2 Quality](images/datahub/dp2-validation.png), [checks](evidence/datahub-validation.json) | **K/P**: assertions cũ là count/basic key rules, không quantity/stock/header reconciliation | **≠ Q52/53/54/58/60/61** nếu claim assertions đủ nghiệp vụ mới | Đưa các invariant đã pass vào DataHub, parse richer receipts; ảnh/rule/expected/actual/run mới, không chỉ tăng số SUCCESS | 3–5 |
| 32 | 2 | DP3 lineage trên DataHub | [DP3 lineage UI](images/datahub/dp3-lineage.png), [publisher](../scripts/run/publish_order_snapshots_datahub.py) | **P**: map feature←fact có; bridge label map chỉ fact→labels, không hiện đủ keys/truth artifacts job `spark_jobs/gold/build_walmart_order_snapshot_labels.py` thực đọc | **≠ Q56/57/61** đối với nguồn labelv2; **Δ Q53** | Source-observable label history/coverage và feature dependencies; nếu giữ bridge demo thì khai đủ lineage truth/keys và đánh legacy, không gọi là source v2 | 3–5 |
| 33 | 2 | DP3 validation trên DataHub | [DP3 Quality](images/datahub/dp3-validation.png), [checks](evidence/datahub-validation.json), [parser](../scripts/evidence/build_order_snapshots_datahub_validation.py) | **L**: parser còn chờ `Gold fact=`, `feature invalid=`, `duplicate_order_line_key=`; `spark_jobs/gold/validate_order_snapshot_return_features.py` nay in dict field names khác | **≠ Q56/57/61** với nhãn/PIT mới, ngoài ra code drift độc lập Q | Sửa parser/receipt schema, kiểm SUCCESS lấy từ task/version mới và các lỗi key/value/availability; ảnh/log mới sau integration | 3–5 |
| 34 | 2 | Visualize tables mọi zone | [Schema](schema.md), [zones](images/schema-zones.png), [DBeaver](images/schema/dbeaver-zones.png) | **K/P**: ảnh20261014 cho core cũ; văn bản vẫn nhắc CDCv1, chưa có tablesv2 runtime | **Δ Q52/53/54/61** | ER/schema inventory của actual implemented tables mới, grain/role và linkage; đánh planned khác measured, không vẽ contract như bảng đã deploy | 3–6 |
| 35 | 2 | Dim SCD2 | [Gold](gold.md#bảng-lakehousegolddim_amazon_order_dp2), [job `spark_jobs/gold/build_amazon_order_status_scd2.py`](../spark_jobs/gold/build_amazon_order_status_scd2.py), [dim validate](evidence/datahub-validation.json) | **K/P**: Amazon Order status SCD2 có5.779.561 versions; không phải Listing→SKU/pool history | **≠ Q53** nếu dùng Order SCD2 thay Listing/mapping history; **Δ Q52/61** | Implement Listing/attributes/binding histories và effective/observed corrections. Ledger còn đích SKU/Storefront dim rộng hơn Amazon Order dim hiện có | 6–12 |
| 36 | 2 | feat_ có event_timestamp và created | [Schema DESCRIBE](schema.md), [job `spark_jobs/gold/build_order_snapshot_return_features.py`](../spark_jobs/gold/build_order_snapshot_return_features.py), [DP3](gold.md) | **K** cho hai cột rubric; tên cột không tự chứng minh PIT | **—** đối với hình thức2 cột; **Δ Q53/56/57/61** về meaning/clock | Ghi mapping clocks/units/nullability trong schema mới và DESCRIBE của run mới; PIT/label proof nằm dòng26–27, không tính lặp | 1–2 |
| 37 | 2 | Quan hệ dim–fact | [DBeaver ER](images/schema/dbeaver-erd-dim-fact.png), [Schema](schema.md#quan-hệ-fact-và-scd2), [order version key](../spark_jobs/lib/order_version_key.py) | **K/P**: fact Amazon→Order dim có logical key check;4 Channel khác null; không có FK cưỡng chế Iceberg | **Δ Q52/53/54/61** | Listing/SKU/pool bindings theo sale/operation-time, no orphan/no fan-out/correction fixtures, ER và reference assertions mới | 3–6 |
| 38 | 2 | Naming conventions | [Terminology](terminology.md), [Schema naming](schema.md#quy-ước-đặt-tên), [setupv2](../scripts/setup/setup_dp1_cdc.sh) | **K**: raw_/stg_/fact_/dim_/feat_ còn đúng; không bắt buộc có obt_ nếu chưa có vai trò đó | **—**: tênv2 vẫn raw_, scope không tự phá convention | Inventory names actual/planned; cập nhật chỉ dẫn CDCv1/v2 và tên facts mới khi triển khai, không đổi tên chỉ để thêm loại prefix | 1–2 |
| 39 | 10 | Novel idea1: ledger chọn DE-N1 | [Novel cũ](novel-ideas.md), [Trino proof](trino.md), [DE-N1 đã chốt](de-acceptance-ledger.md#novel-ideas--đã-chốt-q51) | **N cho DE-N1**: Trino federation có proof nhưng không là data contract/schema registry; source contract Markdown chưa là registry thực thi | **Δ Q61**, nhưng thay idea từ **Q51 trước Q52** | Registry/artifact schema version, compatibility check tự động, gate/reject/quarantine và demo schema before/after; lưu proof chạy được, không đổi idea về Trino ngầm | 16–28 |
| 40 | 10 | Novel idea2: ledger chọn DE-N2 | [CDC cũ](cdc.md), [Novel cũ](novel-ideas.md), [manual compaction](storage.md), [DE-N2](de-acceptance-ledger.md#novel-ideas--đã-chốt-q51) | **N cho DE-N2**: CDC không là automated maintenance; job `spark_jobs/maintenance/compact_amazon_order_snapshots.py` là manual, chưa scheduler/trigger/ref-safe expiry/failure-retry | **Δ Q61**; đổi idea từ **Q51**, pin retention từ Q45, không đổ cho Q52–Q61 | Automated trigger+compaction/sort/expiry policy, retention bảo vệ versions/readers/model, crash/retry và correctness/latency proofs. Không đếm riêng proof dòng20 hai lần | 24–40 |

## Những chỗ proof không còn chứng minh đúng đối tượng hiện tại

### CDC v1 → v2: không chỉ đổi tên bảng

[Job `spark_jobs/cdc/ingest_walmart_order_changes.py`](../spark_jobs/cdc/ingest_walmart_order_changes.py), [job `spark_jobs/cdc/validate_walmart_order_changes.py`](../spark_jobs/cdc/validate_walmart_order_changes.py)
và [DAG](../dags/dp1_cdc.py) yêu cầu cùng `run_id`/POSIX boundary directory,
frozen partition/offset range, cluster/topic UUID và `source_epoch` trong merge
identity. [Setup](../scripts/setup/setup_dp1_cdc.sh) chọn `raw_walmart_cdc_v2`.
Đây là **default trong code/setup**, chưa đọc Airflow DB nên chưa biết Variable
runtime đã đổi hoặc bảngv2 đã được tạo chưa.

[Proof DataHub](evidence/datahub-validation.json) và [CDC UI/log](cdc.md)
ghi `raw_walmart_cdc`, run `manual__2026-10-13T22:34:22+00:00`, Kafka6 =
Bronze6. Không chứng minh UUID lookup/Py4J/committed range/MERGE/POSIX volume
hoặc reset/fencing của codev2. [Remediation evidence](evidence/audit-remediation-20261018.json)
ghi12 boundary unit cases và4 feature Spark cases; không có CDC integrationv2.
Trong lượt chỉ đọc này, SHA-256 của **12 code/test files** trong remediation
vẫn khớp artifact; không chạy lại166 tests hoặc4 Spark cases.

Các đường cần cập nhật khi được phép implement, chưa sửa ở lượt này:

1. `scripts/run/publish_order_snapshots_datahub.py`: `CDC` constant và job map vẫn v1.
2. `scripts/evidence/build_order_snapshots_datahub_validation.py`: selected run IDs và target
   CDC vẫn v1; parser CDC chưa ghi `extra`/payload mismatches/source epoch.
3. Cùng script, nhánh feature parser còn chờ output cũ; `spark_jobs/gold/validate_order_snapshot_return_features.py` hiện in
   dictionary `fact_rows`, `feature_rows`, `invalid`, `duplicate_keys`,
   `missing`, `extra`, `wrong_event_time`… nên parser không có bằng chứng
   đọc được log mới. Đây là đối chiếu tĩnh, không gọi parser để tái hiện.
4. `docs/schema.md`, `docs/airflow.md`, `docs/novel-ideas.md` còn mô tả proofv1;
   cần gắn lịch sử và dẫn sang v2 nếu dùng làm hồ sơ code hiện hành.

Tác động trực tiếp: dòng **15,22,23,28,29,34**; feature log drift thêm
**27,33**. CDCv1 là nhánh bổ sung; core DP1 Batch có proof riêng, vì vậy
không kết luận toàn bộ dòng22–23 mất hết proof. Không rename artifactv1 hoặc
chạy lại publisher với receipts cũ để làm như đã có validationv2.

### Novel ideas: rubric mở nhưng ledger đã chọn cách đáp ứng

Workbook dòng39/40 chỉ yêu cầu “Document idea + proof it worked!” và10 điểm
mỗi dòng. [Ledger Q51](de-acceptance-ledger.md) hiện chọn DE-N1/schema registry
và DE-N2/maintenance tự động. [Novel docs cũ](novel-ideas.md) vẫn chọn Trino
federation và Debezium CDC; hai proof đó không bị xóa giá trị lịch sử, nhưng
**không thể xác nhận hai novel mới đã hoàn thành**. Không tự coi registry là
đã có vì có hai schema parse thủ công, hoặc maintenance đã có vì có job
compaction. Không tìm thấy implementation registry hoặc DAG maintenance/
expiry trong các thư mục code `generator/`, `spark_jobs/`, `dags/`, `scripts/`
đã quét. Muốn đổi lại idea phải sửa decision/impact assessment với người dùng,
không tự đổi mapping để giữ20 điểm. Quyền chấm “novel” thuộc người chấm.

### Q52–Q61 đổi ý nghĩa, khác với chỉ thiếu ảnh

| Nhóm Q | Claim cũ không được tiếp tục dùng | Dòng ảnh hưởng chính |
|---|---|---|
| Q52 partial lifecycle | Một trạng thái/current quantity hoặc latest line là đủ lịch sử allocation/Return/Refund | 14,24–27,31,34,37; mở coverage ở5–10,13,17–18,22–23,28–30,32–33 |
| Q53 Listing history | Amazon Order-status SCD2 chính là Listing→SKU/pool mapping history | 24–27,30–32,34–35,37 |
| Q54/Q58 inventory | Đã có Gold sales nghĩa là đã có stock ledger/reservations/quarantine; cộng tồn theo Storefront | 21,24–25,30–31,34,37 |
| Q55/Q59 queue | DE xong tự chứng minh UI/agent queue và decomposition | Chưa có dòng DE riêng cho UI; cần DE facts/coverage đầu vào, không gán proof UI vào38 dòng này |
| Q56/Q57 labelv2 | Request-time30d/ground-truth bridgev1 là approved Return/product Refund thành công theo allocation windows | 26–27,32–33; late/revision proof liên quan9,17,23–25 |
| Q60 headline sales | SUM(line_net_amount) cũ hoặc count UNPAID là sales còn lại sau hủy trước ship | 19,21,24–25,31; feature source quantity/price cần kiểm ở26–27 |
| Q61 observable feeds | Native Orders và truth-only bridge chứng minh nguồn delivery/approval/refund/stock đều quan sát được | 5–10,13–18,20–35,37,39–40 tùy capability; không phải mọi dòng đều invalid |

Proof cũ vẫn đúng cho rubric hẹp về **Docker/multistage, TUMBLE/HOP, hai cột
feature, prefix naming hoặc Order SCD2** khi nói đúng đối tượng. Ký hiệu≠
trong bảng đánh vào claim mở rộng, không sửa lại lịch sử lần chạy cũ.

## Ước lượng theo gói để tránh cộng trùng

Một đường xử lý dùng chung các dòng được chia như sau. Giờ là ước lượng mới
trong báo cáo, không lấy từ rubric hoặc log. Mỗi gói có deliverable riêng;
baseline/validator/ảnh chia cùng run thì không tính thêm cùng thời gian đó.

| Gói | Deliverable chưa có hoặc cần cập nhật | Dòng được phục vụ | Giờ |
|---|---|---|---:|
| A | Sửa proof plumbing code hiện tại: CDCv2 POC, parser/URN DataHub, receipts version, build/proof attribution | 3–4,15,22–23,27–29,33–34 | 12–22 |
| B | Sourcev2 world/envelope/authority/capability/horizons, stable IDs và emitted fixtures nhỏ | 5–10,13–14,22–27; nền Q52–Q61 | 40–72 |
| C | Bronze/Silver canonical lifecycle/Listing/stock/quantity/money facts, corrections/reconciliation và SCD2/bindings | 13–15,22–25,35,37 | 60–100 |
| D | Stream scope mới: bounded receipts/late/dedup/window correctness và recovery nhỏ | 9–10,16–19 | 16–30 |
| E | Feature/labelv2 availability/revisions và independent oracle/validation | 26–27,32–33,36 | 24–44 |
| F | Warehouse canonical/indexing, fixed-input Spark/storage measurements | 11–12,20–21 | 14–26 |
| G | DE-N1 registry/compatibility gate và demo riêng | 39 | 16–28 |
| H | DE-N2 automated maintenance/retention/retry và demo riêng | 40 | 24–40 |
| I | Governance nguồn mới, schema/ER/ảnh/docs/A2 và kiểm bản nộp | 28–38 và A2 | 12–22 |
| J | Frozen input/stage receipts, publish/fencing/retention POC tối thiểu cho đầu ra DE | 22–33; prerequisite dùng chung Q45/IR02/IR19 | 24–44 |
| **Tổng gói, chưa dự phòng** | **Bộ nhỏ DE theo scope đã chọn, không phải toàn platform/Cloud/full-scale** | | **242–428** |

Nếu chỉ làm **A** để evidence khớp code hiện tại thì vẫn chưa có DE-N1/N2
và chưa đạt source/scope mới; không gọi12–22 giờ là thời gian hoàn tất DE.
Nền tảng dùng chung của góiJ có thể chia với P2 nhưng vẫn phải có owner và
proof, không biến thành chi phí0. J chỉ là POC DE tối thiểu, chưa gồm toàn
Feast/model/agent/restore stack. Không suy tổng này thành ngày Cloud phải bật;
thời gian sinh/scan100 GB, tải máy, dependency failures và thao tác Cloud
đều cần dự toán riêng trước chạy lớn.

## Điều kiện ngoài 38 dòng và thứ tự đóng

**A2** là điều kiện chung không có điểm riêng: README domain/mục lục/repo
structure/deployable-unit diagram, hướng/mô tả/thứ tự arrows, docs/ảnh có giải
thích, docstrings. [Closure20261015](evidence/de-closure-20261015.json) chỉ
chứng minh đợt kiểm cũ; diagram hiện chưa là deployment proof các feeds/ML/
agent tương lai. Cần inventory/hash/run manifest của bản nộp để reviewer biết
artifact nào là implemented/planned/legacy. Các files untracked phải được
chủ repo đưa vào hồ sơ sau; lượt này không commit hoặc sửa tài liệu cũ.

Thứ tự: A để chặn attribution sai → B/J và fixtures trước nguồn lớn →
C/D/E/F theo dependency → G/H có proof riêng → I/A2. POC GCS/Feast và ngân
sách còn theo [G0–G4](implementation-readiness.md), không được bỏ chỉ vì
bảng này tập trung DE. Không chạy pipeline hoặc khẳng định hiện service nào
đang healthy trong đợt chỉ đọc này.

[Ma trận20261015](de-rubric-matrix.md) kết luận “không còn gap bắt buộc” cho
lượt audit/scope cũ; nó vẫn ghi Trino/CDC ở39/40. Báo cáo này chỉ ra chênh
lệch với code hiện tại và ledger đã mở rộng, **không tự chấm đạt100/100 hay
trừ20 điểm**. Danh sách link/hướng dẫn gốc được trích riêng ở
[rubric-links.md](rubric-links.md); không truy cập các website trong lượt này.
